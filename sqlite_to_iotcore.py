#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite → AWS IoT Core 퍼블리셔 (로컬 체크포인트 + 안정적 room 파티션)
- Glue 평면 컬럼 스키마 호환 (year, month, day, hour, min5, room_p)
- temp/hum/gas/pm 표준편차(std) 포함
- SQLite는 "YYYY-MM-DD HH:MM:SS" (KST 문자열)
- S3는 예외 복구 시에만 참조
"""

import os, sys, time, json, sqlite3, datetime, boto3, re
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from zoneinfo import ZoneInfo

# ===== 설정 =====
DB_PATH        = os.getenv("DB_PATH", "/aiot/dataset/sensor_data.db")
THING_NAME     = os.getenv("THING_NAME", "RaspberryPi5_IoT_Thing_Gateway_1")
IOT_ENDPOINT   = os.getenv("IOT_ENDPOINT", "a9jkapbuh4ma7-ats.iot.ap-northeast-2.amazonaws.com")
CA_PATH        = os.getenv("CA_PATH", "/iotcert/AmazonRootCA1.pem")
CERT_PATH      = os.getenv("CERT_PATH", "/iotcert/RaspberryPi5_IoT_Thing_Gateway_1.cert.pem")
KEY_PATH       = os.getenv("KEY_PATH", "/iotcert/RaspberryPi5_IoT_Thing_Gateway_1.private.key")
TOPIC          = os.getenv("TOPIC", "iot/sensor/minute")
S3_BUCKET      = os.getenv("S3_BUCKET", "iot-school-env")
S3_PREFIX_BASE = os.getenv("S3_PREFIX_BASE", "data")
S3_PARTITION_TZ= os.getenv("S3_PARTITION_TZ", "UTC").upper()
ROOM           = os.getenv("ROOM", "room-306")
QOS            = int(os.getenv("QOS", "1"))

KST = ZoneInfo("Asia/Seoul")
UTC = ZoneInfo("UTC")
s3  = boto3.client("s3")

def log(msg): print(msg, flush=True)

def epoch_to_utc_text(e): return datetime.datetime.fromtimestamp(e, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
def epoch_to_kst_text(e): return datetime.datetime.fromtimestamp(e, tz=KST).strftime("%Y-%m-%d %H:%M:%S")

def floor_to_five_minutes_utc(epoch_sec):
    dt = datetime.datetime.fromtimestamp(epoch_sec, tz=UTC)
    m5 = (dt.minute // 5) * 5
    return int(dt.replace(minute=m5, second=0, microsecond=0).timestamp())

# ===== 표준편차 계산 =====
def calc_std(avg, sumsq, n):
    if avg is None or sumsq is None or n is None or n < 2:
        return None
    var = (sumsq - n * avg**2) / (n - 1)
    if var < 0: var = 0
    return round(var ** 0.5, 4)

# ===== 로컬 체크포인트 =====
def ensure_checkpoint_table(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS iot_checkpoint (last_end_utc INTEGER)")
    conn.commit()

def get_local_checkpoint(conn):
    row = conn.execute("SELECT last_end_utc FROM iot_checkpoint ORDER BY ROWID DESC LIMIT 1").fetchone()
    return int(row[0]) if row and row[0] is not None else 0

def update_local_checkpoint(conn, value):
    conn.execute("DELETE FROM iot_checkpoint")
    conn.execute("INSERT INTO iot_checkpoint (last_end_utc) VALUES (?)", (int(value),))
    conn.commit()

# ===== S3 최신 파티션 (예외 복구용) =====
def s3_part_to_end_utc(y, mo, d, h, m5):
    base_tz = UTC if S3_PARTITION_TZ == "UTC" else KST
    dt = datetime.datetime(y, mo, d, h, m5, tzinfo=base_tz) + datetime.timedelta(minutes=5)
    return int(dt.astimezone(UTC).timestamp())

def get_latest_s3_window_utc(bucket, prefix_base):
    try:
        paginator = s3.get_paginator("list_objects_v2")
        latest = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix_base):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not any(key.endswith(ext) for ext in [".json", ".parquet", ".gz", ".gzip"]):
                    continue
                parts = [p.split("=")[-1] for p in key.split("/") if "=" in p]
                if len(parts) >= 5:
                    try:
                        y, mo, d, h, m5 = map(int, parts[:5])
                        end_utc = s3_part_to_end_utc(y, mo, d, h, m5)
                        latest = max(latest, end_utc)
                    except Exception:
                        continue
        if latest:
            log(f"[S3] Latest object partition end: {epoch_to_utc_text(latest)} UTC")
            return latest
    except Exception as e:
        log(f"[WARN] S3 list failed: {e}")
    return 0

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
    start_kst = datetime.datetime.fromtimestamp(start_utc, tz=UTC).astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")
    end_kst   = datetime.datetime.fromtimestamp(end_utc, tz=UTC).astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")

    log(f"[DEBUG] Query window (KST) {start_kst} ~ {end_kst}")
    sql = """
    SELECT dev, COUNT(*) AS n,
           AVG(t) AS temp_avg, MIN(t) AS temp_min, MAX(t) AS temp_max, SUM(t*t) AS t_sumsq,
           AVG(h) AS hum_avg,  MIN(h) AS hum_min,  MAX(h) AS hum_max,  SUM(h*h) AS h_sumsq,
           AVG(lx) AS lux_avg,
           AVG(g) AS gas_avg,  MIN(g) AS gas_min,  MAX(g) AS gas_max,  SUM(g*g) AS g_sumsq,
           AVG(pm) AS pm_avg,  MIN(pm) AS pm_min,  MAX(pm) AS pm_max,  SUM(pm*pm) AS pm_sumsq
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

    # ROOM에서 숫자만 추출
    room_digits = re.search(r"(\d+)$", ROOM)
    room_p = room_digits.group(1) if room_digits else "unknown"

    for r in rows:
        r["temp_std"] = calc_std(r["temp_avg"], r["t_sumsq"], r["n"])
        r["hum_std"]  = calc_std(r["hum_avg"],  r["h_sumsq"], r["n"])
        r["gas_std"]  = calc_std(r["gas_avg"],  r["g_sumsq"], r["n"])
        r["pm_std"]   = calc_std(r["pm_avg"],   r["pm_sumsq"], r["n"])

        kst_dt = datetime.datetime.fromtimestamp(start_utc, tz=UTC).astimezone(KST)
        year, month, day, hour = kst_dt.strftime("%Y"), kst_dt.strftime("%m"), kst_dt.strftime("%d"), kst_dt.strftime("%H")
        min5 = f"{(kst_dt.minute // 5) * 5:02d}"

        payload = {
            "device": r["dev"], "room": ROOM,
            "window_start": epoch_to_utc_text(start_utc),
            "window_end": epoch_to_utc_text(end_utc),
            "window_start_kst": epoch_to_kst_text(start_utc),
            "window_end_kst": epoch_to_kst_text(end_utc),
            "count": r["n"],
            "temp_avg": r["temp_avg"], "temp_max": r["temp_max"], "temp_min": r["temp_min"], "temp_std": r["temp_std"],
            "hum_avg": r["hum_avg"], "hum_max": r["hum_max"], "hum_min": r["hum_min"], "hum_std": r["hum_std"],
            "lux_avg": r["lux_avg"],
            "gas_avg": r["gas_avg"], "gas_max": r["gas_max"], "gas_min": r["gas_min"], "gas_std": r["gas_std"],
            "pm_avg": r["pm_avg"], "pm_max": r["pm_max"], "pm_min": r["pm_min"], "pm_std": r["pm_std"],
            "year": year, "month": month, "day": day, "hour": hour, "min5": min5, "room_p": room_p
        }

        msg = json.dumps(payload, ensure_ascii=False)
        client.publish(TOPIC, msg, QOS)
        log(f"[PUBLISH] {r['dev']} ({year}-{month}-{day} {hour}:{min5}) -> {TOPIC}")

    # 윈도우 처리 완료 후 로컬 체크포인트 갱신
    update_local_checkpoint(conn_ckpt, end_utc)
    return len(rows)

# ===== 실행 루프 =====

def run_incremental(run_forever):
    client = connect_mqtt()
    try:
        # === 로컬 체크포인트 커넥션을 루프 전부터 유지 ===
        with sqlite3.connect(DB_PATH) as conn_ckpt:
            conn_ckpt.execute("PRAGMA journal_mode=WAL")
            conn_ckpt.execute("PRAGMA busy_timeout=3000")
            ensure_checkpoint_table(conn_ckpt)

            local_ckpt = get_local_checkpoint(conn_ckpt)
            if local_ckpt:
                last_utc_end = local_ckpt
                log(f"[BOOT] Local checkpoint found: {epoch_to_utc_text(last_utc_end)} UTC")
            else:
                last_utc_end = get_latest_s3_window_utc(S3_BUCKET, S3_PREFIX_BASE)
                log(f"[BOOT] S3 anchor fallback: {epoch_to_utc_text(last_utc_end)} UTC")

            # === 메인 루프 ===
            while True:
                now = int(time.time())
                target_end = floor_to_five_minutes_utc(now)
                next_end = last_utc_end + 300

                if next_end > target_end:
                    log(f"[IDLE] Up-to-date. last_end={epoch_to_utc_text(last_utc_end)} UTC (sleep 5s)")
                    if not run_forever: break
                    time.sleep(5)
                    continue

                start_utc = next_end - 300
                # === 읽기 전용 커넥션 (락 최소화) ===
                with sqlite3.connect(DB_PATH) as conn_read:
                    conn_read.execute("PRAGMA journal_mode=WAL")
                    conn_read.execute("PRAGMA busy_timeout=3000")
                    rows = fetch_utc_window_aggregate(conn_read, start_utc, next_end)

                if not rows:
                    log(f"[INFO] No data in window (UTC {epoch_to_utc_text(start_utc)}~{epoch_to_utc_text(next_end)}). Skipped.")
                    update_local_checkpoint(conn_ckpt, next_end)
                    last_utc_end = next_end
                    continue

                # === 집계 및 MQTT 발행 ===
                for r in rows:
                    r["temp_std"] = calc_std(r["temp_avg"], r["t_sumsq"], r["n"])
                    r["hum_std"]  = calc_std(r["hum_avg"],  r["h_sumsq"], r["n"])
                    r["gas_std"]  = calc_std(r["gas_avg"],  r["g_sumsq"], r["n"])
                    r["pm_std"]   = calc_std(r["pm_avg"],   r["pm_sumsq"], r["n"])

                    kst_dt = datetime.datetime.fromtimestamp(start_utc, tz=UTC).astimezone(KST)
                    year, month, day, hour = kst_dt.strftime("%Y"), kst_dt.strftime("%m"), kst_dt.strftime("%d"), kst_dt.strftime("%H")
                    min5 = f"{(kst_dt.minute // 5) * 5:02d}"
                    room_digits = re.search(r"(\d+)$", ROOM)
                    room_p = room_digits.group(1) if room_digits else "unknown"

                    payload = {
                        "device": r["dev"], "room": ROOM,
                        "window_start": epoch_to_utc_text(start_utc),
                        "window_end": epoch_to_utc_text(next_end),
                        "window_start_kst": epoch_to_kst_text(start_utc),
                        "window_end_kst": epoch_to_kst_text(next_end),
                        "count": r["n"],
                        "temp_avg": r["temp_avg"], "temp_max": r["temp_max"], "temp_min": r["temp_min"], "temp_std": r["temp_std"],
                        "hum_avg": r["hum_avg"], "hum_max": r["hum_max"], "hum_min": r["hum_min"], "hum_std": r["hum_std"],
                        "lux_avg": r["lux_avg"],
                        "gas_avg": r["gas_avg"], "gas_max": r["gas_max"], "gas_min": r["gas_min"], "gas_std": r["gas_std"],
                        "pm_avg": r["pm_avg"], "pm_max": r["pm_max"], "pm_min": r["pm_min"], "pm_std": r["pm_std"],
                        "year": year, "month": month, "day": day, "hour": hour, "min5": min5, "room_p": room_p
                    }

                    msg = json.dumps(payload, ensure_ascii=False)
                    client.publish(TOPIC, msg, QOS)
                    log(f"[PUBLISH] {r['dev']} ({year}-{month}-{day} {hour}:{min5}) -> {TOPIC}")

                update_local_checkpoint(conn_ckpt, next_end)
                last_utc_end = next_end

    finally:
        try:
            client.disconnect()
            log("[MQTT] Disconnected.")
        except Exception:
            pass

# ===== Main =====
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["once", "drain"], default="drain")
    args = ap.parse_args()
    run_incremental(run_forever=(args.mode == "drain"))
