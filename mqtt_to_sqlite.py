#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MQTT -> SQLite 수집기 (KST 문자열 타임스탬프 저장)
- Topic: sensor/metrics
- DB: /aiot/dataset/sensor_data.db
- 입력 형식:
  1) JSON 예: {"dev":"esp8266-306","ts":"2025-11-08 00:07:51","t":19.61,"h":69.92,"lx":0.0,"g":0.679,"pm":null}
  2) PIPE 예: id|dev|ts|t|h|lx|g|pm|
              14774|esp8266-306|2025-11-08 00:07:51|19.61|69.92|0.0|0.679|
"""

import os, json, sqlite3, signal, sys
from datetime import datetime, timezone, timedelta
import paho.mqtt.client as mqtt

# =======================
# 환경설정
# =======================
DB_PATH   = os.getenv("DB_PATH", "/aiot/dataset/sensor_data.db")
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC     = os.getenv("TOPIC", "sensor/metrics")

# 한국 시간대
KST = timezone(timedelta(hours=9))

# =======================
# DB 초기화
# =======================
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;")  # 동시 읽기/쓰기 안정화에 유리. :contentReference[oaicite:1]{index=1}
cur  = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS metrics (
    id  INTEGER PRIMARY KEY AUTOINCREMENT,
    dev TEXT    NOT NULL,
    ts  TEXT    NOT NULL,   -- "YYYY-MM-DD HH:MM:SS" (KST)
    t   REAL,
    h   REAL,
    lx  REAL,
    g   REAL,
    pm  REAL
)
""")
conn.commit()

cur.execute("CREATE INDEX IF NOT EXISTS idx_metrics_dev_ts ON metrics(dev, ts)")
conn.commit()

# =======================
# 유틸: ts 문자열 보정
# =======================
def to_kst_str_from_any(ts_val):
    """
    ts_val이 숫자(epoch 초/밀리초) 또는 문자열(ISO/스페이스 분리)이 올 수 있음.
    반환: KST 기준 "YYYY-MM-DD HH:MM:SS" 문자열
    """
    try:
        # 숫자형: epoch 로 보고 KST 문자열로
        if isinstance(ts_val, (int, float)):
            # 밀리초 추정
            if ts_val > 10**11:
                ts_val = ts_val / 1000.0
            dt = datetime.fromtimestamp(ts_val, tz=KST)
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        # 문자열: ISO or "YYYY-MM-DD HH:MM:SS" → KST 문자열 통일
        if isinstance(ts_val, str):
            s = ts_val.strip().replace("Z", "")
            # ISO 포맷 or 공백 구분 직접 파싱
            try:
                dt = datetime.fromisoformat(s)
            except ValueError:
                # 공백 구분 시도: "YYYY-MM-DD HH:MM:SS"
                dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            # TZ 없으면 KST 부여, 있으면 KST로 변환
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            else:
                dt = dt.astimezone(KST)
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        # 그 외: 현재 시각
        return datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")

    except Exception:
        return datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")

# =======================
# 파서: JSON 또는 PIPE
# =======================
def parse_payload(payload: str):
    """
    입력 payload를 dict로 변환. 우선 JSON 시도, 실패하면 PIPE('|' ) 파싱.
    결과 키: dev, ts(str), t, h, lx, g, pm
    """
    # 1) JSON 우선
    try:
        d = json.loads(payload)
        dev = d.get("dev") or d.get("device") or "unknown"
        ts  = to_kst_str_from_any(d.get("ts"))
        return {
            "dev": dev,
            "ts":  ts,
            "t":   d.get("t",  d.get("temp")),
            "h":   d.get("h",  d.get("hum")),
            "lx":  d.get("lx", d.get("lux")),
            "g":   d.get("g",  d.get("gas")),
            "pm":  d.get("pm")
        }
    except Exception:
        pass

    # 2) PIPE 파싱: id|dev|ts|t|h|lx|g|pm|
    parts = payload.split("|")
    # 최소 7개는 있어야 t,h,lx,g 까지 읽힘
    if len(parts) >= 7:
        # parts[0]=id(버림), parts[1]=dev, parts[2]=ts
        dev = (parts[1] or "unknown").strip()
        ts_raw = parts[2].strip() if len(parts) > 2 else None
        t  = float(parts[3]) if len(parts) > 3 and parts[3] else None
        h  = float(parts[4]) if len(parts) > 4 and parts[4] else None
        lx = float(parts[5]) if len(parts) > 5 and parts[5] else None
        g  = float(parts[6]) if len(parts) > 6 and parts[6] else None
        pm = float(parts[7]) if len(parts) > 7 and parts[7] else None
        ts = to_kst_str_from_any(ts_raw)
        return {"dev": dev, "ts": ts, "t": t, "h": h, "lx": lx, "g": g, "pm": pm}

    # 파싱 실패
    raise ValueError("Unsupported payload format")

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
        cur.execute(
            "INSERT INTO metrics (dev, ts, t, h, lx, g, pm) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (data["dev"], data["ts"], data["t"], data["h"], data["lx"], data["g"], data["pm"])
        )
        conn.commit()
        print(f"[OK] dev={data['dev']} ts='{data['ts']}' t={data['t']} h={data['h']} lx={data['lx']} g={data['g']} pm={data['pm']}")
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
