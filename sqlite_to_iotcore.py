#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite → AWS IoT Core 퍼블리셔 (S3 객체 기반 최신 동기화)
- Glue 평면 컬럼 스키마 호환 (year, month, day, hour, min5, room_p)
- temp/hum/gas/pm 표준편차(std) 포함
- SQLite는 "YYYY-MM-DD HH:MM:SS" (KST 문자열)
- S3 최신 파티션(UTC/KST) 비교 후 자동 진행
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
ROOM           = os.getenv("ROOM", "lecture-306")
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

# ===== S3 최신 파티션 탐색 =====
def s3_part_to_end_utc(y, mo, d, h, m5):
    base_tz = UTC if S3_PARTITION_TZ == "UTC" else KST
    dt = datetime.datetime(y, mo, d, h, m5, tzinfo=base_tz) + datetime.timedelta(minutes=5)
    return int(dt.astimezone(UTC).timestamp())

def get_latest_s3_window_utc(bucket, prefix_base):
    prefixes = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix_base):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not (key.endswith(".json") or key.endswith(".parquet") or key.endswith(".gz") or key.endswith(".gzip")):
                    continue
                parts = [p.split("=")[-1] for p in key.split("/") if "=" in p]
                if len(parts) >= 5:
                    try:
                        y, mo, d, h, m5 = map(int, parts[:5])
                        prefixes.append(s3_part_to_end_utc(y, mo, d, h, m5))
                    except Exception:
                        continue
        if prefixes:
            latest = max(prefixes)
            log(f"[S3] Latest object partition end: {epoch_to_utc_text(latest)} UTC")
            return latest
        else:
            log("[S3] No data objects found (only folders?).")
    except Exception as e:
        log(f"[WARN] S3 list failed: {e}")
    return int(time.time()) - 300

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

# ===== room_p 추출 =====
def extract_room_p(dev_name):
    m = re.search(r"(\d+)$", dev_name or "")
    return m.group(1) if m else "unknown"

# ===== 발행 =====
def publish_window(client, start_utc, end_utc):
    start_s, end_s = epoch_to_utc_text(start_utc), epoch_to_utc_text(end_utc)
    log(f"[WINDOW][UTC] {start_s} ~ {end_s} : aggregating ...")
    with sqlite3.connect(DB_PATH) as conn:
        rows = fetch_utc_window_aggregate(conn, start_utc, end_utc)
    if not rows:
        log(f"[INFO] No data in window (UTC {start_s}~{end_s}). Skipped.")
        return 0

    for r in rows:
        # 표준편차 계산
        r["temp_std"] = calc_std(r["temp_avg"], r["t_sumsq"], r["n"])
        r["hum_std"]  = calc_std(r["hum_avg"],  r["h_sumsq"], r["n"])
        r["gas_std"]  = calc_std(r["gas_avg"],  r["g_sumsq"], r["n"])
        r["pm_std"]   = calc_std(r["pm_avg"],   r["pm_sumsq"], r["n"])

        # Glue 파티션용 시간 필드
        kst_dt = datetime.datetime.fromtimestamp(start_utc, tz=UTC).astimezone(KST)
        year, month, day, hour = kst_dt.strftime("%Y"), kst_dt.strftime("%m"), kst_dt.strftime("%d"), kst_dt.strftime("%H")
        min5 = f"{(kst_dt.minute // 5) * 5:02d}"
        room_p = extract_room_p(r["dev"])

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

    return len(rows)

# ===== 실행 루프 =====
def run_incremental(run_forever):
    client = connect_mqtt()
    try:
        last_utc_end = get_latest_s3_window_utc(S3_BUCKET, S3_PREFIX_BASE)
        log(f"[BOOT] latest S3 end: {epoch_to_utc_text(last_utc_end)} UTC / {epoch_to_kst_text(last_utc_end)} KST")

        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("SELECT MAX(strftime('%s', ts, '-9 hours')) FROM metrics").fetchone()
            db_max_utc = int(row[0]) if row and row[0] is not None else 0

        if db_max_utc:
            log(f"[BOOT] DB  max end: {epoch_to_utc_text(db_max_utc)} UTC / {epoch_to_kst_text(db_max_utc)} KST")

        if db_max_utc and last_utc_end - db_max_utc > 6 * 3600:
            log("[WARN] S3 anchor >> DB by >6h. Rolling back to DB max.")
            last_utc_end = db_max_utc

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
            publish_window(client, start_utc, next_end)
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
    ap.add_argument("--mode", choices=["once","drain"], default="drain")
    args = ap.parse_args()
    run_incremental(run_forever=(args.mode == "drain"))
