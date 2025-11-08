#!/usr/bin/env bash
set -euo pipefail

REPO_URL="https://raw.githubusercontent.com/cnehgus0620/aiot-manager/main"  # 수정
APP_DIR="/opt/aiot"
BIN_PATH="/usr/local/bin/aiot-manager"

echo "=== AIoT Gateway Initializer ==="

# 1️⃣  필수 디렉토리 준비
echo "[1/6] Preparing directories..."
sudo mkdir -p /opt/aiot /var/log/aiot /iotcert
sudo chown -R aiot:aiot /opt/aiot /var/log/aiot /iotcert || true
echo "[OK] 디렉토리 구조 준비 완료"

# 2️⃣  aiot-manager 다운로드 및 배치
echo "[2/6] Downloading aiot-manager..."
sudo curl -fsSL -o "$BIN_PATH" "$REPO_URL/aiot-manager"
sudo chmod +x "$BIN_PATH"
echo "[OK] /usr/local/bin/aiot-manager 등록 완료"

# 3️⃣  Collector/Publisher 코드 다운로드
echo "[3/6] Downloading collector & publisher scripts..."
sudo curl -fsSL -o "$APP_DIR/mqtt_to_sqlite.py" "$REPO_URL/mqtt_to_sqlite.py"
sudo curl -fsSL -o "$APP_DIR/sqlite_to_iotcore.py" "$REPO_URL/sqlite_to_iotcore.py"
sudo chown aiot:aiot "$APP_DIR"/*.py
echo "[OK] /opt/aiot 코드 배치 완료"

# 4️⃣  systemd 유닛 등록
echo "[4/6] Registering systemd units..."
sudo tee /etc/systemd/system/aiot-collector.service >/dev/null <<'EOD'
[Unit]
Description=AIoT MQTT→SQLite Collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=aiot
Group=aiot
WorkingDirectory=/opt/aiot
EnvironmentFile=-/etc/default/aiot
ExecStart=/opt/aiot/venv/bin/python -u /opt/aiot/mqtt_to_sqlite.py
Restart=always
RestartSec=5
SyslogIdentifier=aiot-collector
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOD

sudo tee /etc/systemd/system/aiot-publisher@.service >/dev/null <<'EOD'
[Unit]
Description=AIoT SQLite→AWS IoT Publisher (room %i)
After=network-online.target aiot-collector.service
Wants=network-online.target

[Service]
Type=simple
User=aiot
Group=aiot
WorkingDirectory=/opt/aiot
EnvironmentFile=-/etc/default/aiot
Environment=ROOM=room-%i
ExecStart=/opt/aiot/venv/bin/python -u /opt/aiot/sqlite_to_iotcore.py --mode drain
Restart=always
RestartSec=5
SyslogIdentifier=aiot-publisher-%i
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOD

sudo systemctl daemon-reload
echo "[OK] systemd 등록 완료"

# 5️⃣  환경 설정 마법사 실행
echo "[5/6] Running configuration wizard..."
aiot-manager configure

# 6️⃣  자동 시작 등록 및 즉시 실행
if [ -f /etc/aiot/room ]; then
  room_num=$(grep -o '[0-9]\+' /etc/aiot/room || echo "000")
  sudo systemctl enable aiot-collector
  sudo systemctl enable aiot-publisher@${room_num}
  sudo systemctl start aiot-collector
  sudo systemctl start aiot-publisher@${room_num}
  echo "[OK] systemd enable/start 완료 (room ${room_num})"
fi

echo
echo "=== AIoT Gateway Setup Complete! ==="
echo "→ /usr/local/bin/aiot-manager 로 관리 가능"
echo "→ 재부팅 후 자동 실행 유지됨"

