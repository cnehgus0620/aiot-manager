#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite → AWS IoT Core 퍼블리셔 (Glue 학습용 스키마 호환)
- 파티션 키: date(yyyy-MM-dd) / hour(HH) / room_p
- temp/hum/gas/pm* 표준편차(std) 포함
- SQLite ts는 "YYYY-MM-DD HH:MM:SS" (KST 문자열)
- PM은 pm1_0 / pm2_5 / pm10 3종 모두 집계 + 호환용 단일 pm_*는 pm2_5로 매핑
"""

import os, sys, time, json, sqlite3, datetime, re
from zoneinfo import ZoneInfo
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

# ===== 설정 =====
DB_PATH        = os.getenv("DB_PATH", "/aiot/dataset/sensor_data.db")
THING_NAME     = os.getenv("THING_NAME", "RaspberryPi5_IoT_Thing_Gateway_1")
IOT_ENDPOINT   = os.getenv("IOT_ENDPOINT", "a9jkapbuh4ma7-ats.iot.ap-northeast-2.amazonaws.com")
CA_PATH        = os.getenv("CA_PATH", "/iotcert/AmazonRootCA1.pem")
CERT_PATH      = os.getenv("CERT_PATH", "/iotcert/RaspberryPi5_IoT_Thing_Gateway_1.cert.pem")
KEY_PATH       = os.getenv("KEY_PATH", "/iotcert/RaspberryPi5_IoT_Thing_Gateway_1.private.key")
TOPIC          = os.getenv("TOPIC", "iot/sensor/minute")
ROOM           = os.getenv("ROOM", "room-306")
QOS            = int(os.getenv("QOS", "1"))  # AWS IoT는 QoS 0/1 지원

KST = ZoneInfo("Asia/Seoul")
UTC = ZoneInfo("UTC")

def log(msg): print(msg, flush=True)
def epoch_to_utc_text(e): return datetime.datetime.fromtimestamp(e, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
def epoch_to_kst_text(e): return datetime.datetime.fromtimestamp(e, tz=KST).strftime("%Y-%m-%d %H:%M:%S")

# ===== 표준편차 계산 =====
def calc_std(avg, sumsq, n):
    if avg is None or sumsq is None or n is None or n < 2:
        return None
    var = (sumsq - n * (avg ** 2)) / (n - 1)
    return round(var ** 0.5, 4) if var >= 0 else 0.0

# ===== 로컬 체크포인트 =====
def ensure_checkpoint_table(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS iot_checkpoint (last_end_utc INTEGER)")
    conn.commit()

def get_local_checkpoint(conn):
    row = conn.execute("SELECT last_end_utc FROM iot_checkpoint ORDER BY ROWID DESC LIMIT 1").fetchone()
    return int(row[0]) if row and row[0] else 0

def update_local_checkpoint(conn, value):
    conn.execute("DELETE FROM iot_checkpoint")
    conn.execute("INSERT INTO iot_checkpoint (last_end_utc) VALUES (?)", (int(value),))
    conn.commit()

# ===== MQTT 연결 =====
def connect_mqtt():
    log(f"[MQTT] Connecting to {IOT_ENDPOINT} as {THING_NAME} ...")
    c = AWSIoTMQTTClient(THING_NAME)
    c.configureEndpoint(IOT_ENDPOINT, 8883)
    c.configureCredentials(CA_PATH, KEY_PATH, CERT_PATH)
    c.configureAutoReconnectBackoffTime(1, 32, 20)
    c.configureOfflinePublishQueueing(-1)
    c.configureDrainingFrequency(2)
    c.configureConnectDisconnectTimeout(10)
    c.configureMQTTOperationTimeout(10)
    c.connect()
    log("[MQTT] Connected.")
    return c

# ===== SQLite 집계 =====
def fetch_utc_window_aggregate(conn, start_utc, end_utc):
    # DB에는 KST 문자열로 저장되어 있으므로 UTC 윈도우를 KST 문자열 범위로 변환
    start_kst = datetime.datetime.fromtimestamp(start_utc, tz=UTC).astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")
    end_kst   = datetime.datetime.fromtimestamp(end_utc,   tz=UTC).astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")

    sql = """
    SELECT dev, COUNT(*) AS n,
           AVG(t)  AS temp_avg, MIN(t)  AS temp_min, MAX(t)  AS temp_max, SUM(t*t)  AS t_sumsq,
           AVG(h)  AS hum_avg,  MIN(h)  AS hum_min,  MAX(h)  AS hum_max,  SUM(h*h)  AS h_sumsq,
           AVG(lx) AS lux_avg,
           AVG(g)  AS gas_avg,  MIN(g)  AS gas_min,  MAX(g)  AS gas_max,  SUM(g*g)  AS g_sumsq,

           -- PM 3종 집계
           AVG(pm1_0) AS pm1_0_avg, MIN(pm1_0) AS pm1_0_min, MAX(pm1_0) AS pm1_0_max, SUM(pm1_0*pm1_0) AS pm1_0_sumsq,
           AVG(pm2_5) AS pm2_5_avg, MIN(pm2_5) AS pm2_5_min, MAX(pm2_5) AS pm2_5_max, SUM(pm2_5*pm2_5) AS pm2_5_sumsq,
           AVG(pm10)  AS pm10_avg,  MIN(pm10)  AS pm10_min,  MAX(pm10)  AS pm10_max,  SUM(pm10*pm10)  AS pm10_sumsq

    FROM metrics
    WHERE ts >= ? AND ts < ?
    GROUP BY dev;
    """
    cur = conn.execute(sql, (start_kst, end_kst))
    return [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

# ===== 발행 =====
def publish_window(client, start_utc, end_utc, conn_ckpt):
    start_s, end_s = epoch_to_utc_text(start_utc), epoch_to_utc_text(end_utc)
    log(f"[WINDOW][UTC] {start_s} ~ {end_s} : aggregating ...")
    with sqlite3.connect(DB_PATH) as conn:
        rows = fetch_utc_window_aggregate(conn, start_utc, end_utc)
    if not rows:
        log(f"[INFO] No data in window (UTC {start_s}~{end_s}). Skipped.")
        return 0

    # room_p 추출 (숫자만)
    room_digits = re.search(r"(\d+)$", ROOM)
    room_p = room_digits.group(1) if room_digits else "unknown"

    for r in rows:
        # 표준편차 계산
        r["temp_std"] = calc_std(r["temp_avg"], r["t_sumsq"], r["n"])
        r["hum_std"]  = calc_std(r["hum_avg"],  r["h_sumsq"], r["n"])
        r["gas_std"]  = calc_std(r["gas_avg"],  r["g_sumsq"], r["n"])
        r["pm1_0_std"] = calc_std(r["pm1_0_avg"], r["pm1_0_sumsq"], r["n"])
        r["pm2_5_std"] = calc_std(r["pm2_5_avg"], r["pm2_5_sumsq"], r["n"])
        r["pm10_std"]  = calc_std(r["pm10_avg"],  r["pm10_sumsq"],  r["n"])

        # 호환용 단일 pm_* (pm2_5로 매핑) — 기존 Glue 스키마가 pm_*만 읽어도 깨지지 않게
        pm_avg = r["pm2_5_avg"]; pm_min = r["pm2_5_min"]; pm_max = r["pm2_5_max"]; pm_std = r["pm2_5_std"]

        payload = {
            "device": r["dev"], "room": ROOM,

            "window_start": epoch_to_utc_text(start_utc),
            "window_end":   epoch_to_utc_text(end_utc),
            "window_start_kst": epoch_to_kst_text(start_utc),
            "window_end_kst":   epoch_to_kst_text(end_utc),

            "count": r["n"],

            "temp_avg": r["temp_avg"], "temp_max": r["temp_max"], "temp_min": r["temp_min"], "temp_std": r["temp_std"],
            "hum_avg":  r["hum_avg"],  "hum_max":  r["hum_max"],  "hum_min":  r["hum_min"],  "hum_std":  r["hum_std"],
            "lux_avg":  r["lux_avg"],
            "gas_avg":  r["gas_avg"],  "gas_max":  r["gas_max"],  "gas_min":  r["gas_min"],  "gas_std":  r["gas_std"],

            # --- PM 3종 ---
            "pm1_0_avg": r["pm1_0_avg"], "pm1_0_max": r["pm1_0_max"], "pm1_0_min": r["pm1_0_min"], "pm1_0_std": r["pm1_0_std"],
            "pm2_5_avg": r["pm2_5_avg"], "pm2_5_max": r["pm2_5_max"], "pm2_5_min": r["pm2_5_min"], "pm2_5_std": r["pm2_5_std"],
            "pm10_avg":  r["pm10_avg"],  "pm10_max":  r["pm10_max"],  "pm10_min":  r["pm10_min"],  "pm10_std":  r["pm10_std"],

            # --- 호환용 단일 pm_* (== pm2_5_*) ---
            "pm_avg": pm_avg, "pm_max": pm_max, "pm_min": pm_min, "pm_std": pm_std,

            # 파티션 키
            "room_p": room_p
        }

        msg = json.dumps(payload, ensure_ascii=False)
        client.publish(TOPIC, msg, QOS)
        log(f"[PUBLISH] {r['dev']} room_p={room_p} ({start_s}~{end_s})")

    update_local_checkpoint(conn_ckpt, end_utc)
    return len(rows)

# ===== 메인 루프 =====
def run_incremental(run_forever):
    client = connect_mqtt()
    try:
        with sqlite3.connect(DB_PATH) as conn_ckpt:
            ensure_checkpoint_table(conn_ckpt)
            last_utc_end = get_local_checkpoint(conn_ckpt)
            if not last_utc_end:
                last_utc_end = int(time.time()) - 300  # 최근 5분 윈도우 기준
            while True:
                now = int(time.time())
                target_end = now - (now % 300)
                next_end = last_utc_end + 300
                if next_end > target_end:
                    log(f"[IDLE] Up-to-date. ({epoch_to_utc_text(last_utc_end)} UTC)")
                    if not run_forever: break
                    time.sleep(5)
                    continue
                publish_window(client, next_end - 300, next_end, conn_ckpt)
                last_utc_end = next_end
    finally:
        try:
            client.disconnect()
        except Exception:
            pass

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["once", "drain"], default="drain")
    args = ap.parse_args()
    run_incremental(run_forever=(args.mode == "drain"))
