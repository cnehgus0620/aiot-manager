#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MQTT -> SQLite 수집기 (KST 문자열 타임스탬프 저장)
- Topic: sensor/metrics
- DB: /aiot/dataset/sensor_data.db
- 입력 형식(업데이트):
  1) JSON 예:
     {"dev":"esp8266-306","ts":"2025-11-08 00:07:51","t":19.61,"h":69.92,"lx":0.0,"g":0.679,"pm1_0":6.0,"pm2_5":12.5,"pm10":18.3}
  2) PIPE 예:
     id|dev|ts|t|h|lx|g|pm1_0|pm2_5|pm10|
     14774|esp8266-306|2025-11-08 00:07:51|19.61|69.92|0.0|0.679|6|12|18|
"""

import os
import json
import sqlite3
import signal
import sys
from datetime import datetime, timezone, timedelta
import paho.mqtt.client as mqtt

# =======================
# 환경설정
# =======================
DB_PATH   = os.getenv("DB_PATH", "/aiot/dataset/sensor_data.db")
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC     = os.getenv("TOPIC", "sensor/metrics")

# 필터링 옵션
STRICT_PM_ALL_ZERO = os.getenv("STRICT_PM_ALL_ZERO", "1") == "1"  # PM 세 값 모두 0이면 버림
MAX_PM_UGM3        = float(os.getenv("MAX_PM_UGM3", "1000"))      # PM 최대 유효 범위 µg/m³
MAX_LUX            = float(os.getenv("MAX_LUX", "65535"))         # 조도 상한치
DROP_NEGATIVE      = os.getenv("DROP_NEGATIVE", "1") == "1"       # 음수인 경우 버림
KEEP_REJECTS       = os.getenv("KEEP_REJECTS", "1") == "1"        # 거절 데이터 별도 저장 여부

# 한국 시간대
KST = timezone(timedelta(hours=9))

# =======================
# DB 초기화
# =======================
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;")
cur  = conn.cursor()

# 메인 테이블
cur.execute("""
CREATE TABLE IF NOT EXISTS metrics (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    dev    TEXT    NOT NULL,
    ts     TEXT    NOT NULL,   -- "YYYY-MM-DD HH:MM:SS" (KST)
    t      REAL,
    h      REAL,
    lx     REAL,
    g      REAL,
    pm1_0  REAL,
    pm2_5  REAL,
    pm10   REAL
)
""")
conn.commit()
cur.execute("CREATE INDEX IF NOT EXISTS idx_metrics_dev_ts ON metrics(dev, ts)")
conn.commit()

# 거절(reject) 데이터 테이블
cur.execute("""
CREATE TABLE IF NOT EXISTS rejects (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    dev     TEXT,
    ts      TEXT,
    reason  TEXT    NOT NULL,
    payload TEXT    NOT NULL,
    created TEXT    NOT NULL
)
""")
conn.commit()

# =======================
# 유틸: ts 문자열 보정
# =======================
def to_kst_str_from_any(ts_val):
    try:
        if isinstance(ts_val, (int, float)):
            if ts_val > 10**11:
                ts_val = ts_val / 1000.0
            dt = datetime.fromtimestamp(ts_val, tz=KST)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(ts_val, str):
            s = ts_val.strip().replace("Z", "")
            try:
                dt = datetime.fromisoformat(s)
            except ValueError:
                dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            else:
                dt = dt.astimezone(KST)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")

# =======================
# 파서: JSON 또는 PIPE
# =======================
def parse_payload(payload: str):
    try:
        d = json.loads(payload)
        dev   = d.get("dev") or d.get("device") or "unknown"
        ts    = to_kst_str_from_any(d.get("ts"))
        pm1   = d.get("pm1_0")
        pm25  = d.get("pm2_5")
        pm10  = d.get("pm10")
        return {
            "dev":  dev,
            "ts":   ts,
            "t":    d.get("t",  d.get("temp")),
            "h":    d.get("h",  d.get("hum")),
            "lx":   d.get("lx", d.get("lux")),
            "g":    d.get("g",  d.get("gas")),
            "pm1_0": pm1,
            "pm2_5": pm25,
            "pm10":  pm10
        }
    except Exception:
        pass

    parts = payload.split("|")
    if len(parts) >= 10:
        dev   = (parts[1] or "unknown").strip()
        ts    = to_kst_str_from_any(parts[2].strip())
        t     = float(parts[3]) if parts[3] else None
        h     = float(parts[4]) if parts[4] else None
        lx    = float(parts[5]) if parts[5] else None
        g     = float(parts[6]) if parts[6] else None
        pm1   = float(parts[7]) if parts[7] else None
        pm25  = float(parts[8]) if parts[8] else None
        pm10  = float(parts[9]) if parts[9] else None
        return {
            "dev": dev,
            "ts": ts,
            "t": t,
            "h": h,
            "lx": lx,
            "g": g,
            "pm1_0": pm1,
            "pm2_5": pm25,
            "pm10": pm10
        }

    raise ValueError("Unsupported payload format")

# =======================
# 값 검증기
# =======================
def _is_num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)

def validate_row(d: dict):
    """
    정상 데이터면 (True, None)
    버릴 데이터면 (False, '이유1|이유2|...')
    """
    reasons = []

    # 음수 체크
    if DROP_NEGATIVE:
        for k in ("t", "h", "lx", "g", "pm1_0", "pm2_5", "pm10"):
            v = d.get(k)
            if _is_num(v) and v < 0:
                reasons.append(f"neg_{k}")

    # 습도 범위 체크
    h = d.get("h")
    if _is_num(h) and not (0 <= h <= 100):
        reasons.append("humidity_out_of_range")

    # 조도 상한 체크
    lx = d.get("lx")
    if _is_num(lx) and lx > MAX_LUX:
        reasons.append("lux_out_of_range")

    # PM 최대치 체크
    for k in ("pm1_0", "pm2_5", "pm10"):
        v = d.get(k)
        if _is_num(v) and v > MAX_PM_UGM3:
            reasons.append(f"{k}_too_high")

    # PM 모두가 0이면 이상 가능성
    pm1 = d.get("pm1_0"); pm25 = d.get("pm2_5"); pm10 = d.get("pm10")
    def _zero_or_none(x):
        return (x is None) or (_is_num(x) and x == 0)
    if STRICT_PM_ALL_ZERO and _zero_or_none(pm1) and _zero_or_none(pm25) and _zero_or_none(pm10):
        reasons.append("pm_all_zero")

    return (len(reasons) == 0, "|".join(reasons) if reasons else None)

# =======================
# MQTT 핸들러
# =======================
def on_connect(client, userdata, flags, rc):
    print(f"[INFO] MQTT connected rc={rc}; subscribing '{TOPIC}'")
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace").strip()
    try:
        data = parse_payload(payload)
        ok, why = validate_row(data)
        if not ok:
            print(f"[DROP] dev={data.get('dev')} ts='{data.get('ts')}' reason={why}")
            if KEEP_REJECTS:
                cur.execute(
                    "INSERT INTO rejects (dev, ts, reason, payload, created) VALUES (?, ?, ?, ?, datetime('now','localtime'))",
                    (data.get("dev"), data.get("ts"), why, payload)
                )
                conn.commit()
            return

        cur.execute(
            "INSERT INTO metrics (dev, ts, t, h, lx, g, pm1_0, pm2_5, pm10) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (data["dev"], data["ts"], data["t"], data["h"], data["lx"], data["g"],
             data["pm1_0"], data["pm2_5"], data["pm10"])
        )
        conn.commit()
        print(f"[OK] dev={data['dev']} ts='{data['ts']}' t={data['t']} h={data['h']} lx={data['lx']} g={data['g']} "
              f"pm1_0={data['pm1_0']} pm2_5={data['pm2_5']} pm10={data['pm10']}")
    except Exception as e:
        print(f"[ERROR] {e} | payload={payload}")

# =======================
# 종료 처리
# =======================
def _graceful_exit(signum, frame):
    print(f"\n[INFO] Caught signal {signum}. Closing DB...")
    conn.commit()
    conn.close()
    sys.exit(0)

signal.signal(signal.SIGINT, _graceful_exit)
signal.signal(signal.SIGTERM, _graceful_exit)

# =======================
# Main
# =======================
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    print(f"[INFO] Listening MQTT '{TOPIC}' -> {DB_PATH} (KST TEXT ts mode)")
    client.loop_forever()

if __name__ == "__main__":
    main()
