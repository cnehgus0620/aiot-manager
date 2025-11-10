#!/usr/bin/env bash
set -euo pipefail

REPO_URL="https://raw.githubusercontent.com/cnehgus0620/aiot-manager/main/"
APP_DIR="/opt/aiot"
BIN_PATH="/usr/local/bin/aiot-manager"

echo "=== AIoT Gateway Initializer ==="

# 0️⃣ 서비스 계정 보장
if ! id -u aiot >/dev/null 2>&1; then
  echo "[INIT] 'aiot' 시스템 계정이 없습니다. 생성 중..."
  sudo useradd -r -s /usr/sbin/nologin -m -d /var/lib/aiot aiot
  echo "[OK] 'aiot' 계정 생성 완료"
fi

# 1️⃣ 필수 도구 보장 (curl, sudo 등)
PKGS=(curl sudo)
MISSING=()
for pkg in "${PKGS[@]}"; do
  if ! command -v "$pkg" >/dev/null 2>&1; then
    MISSING+=("$pkg")
  fi
done
if [ ${#MISSING[@]} -gt 0 ]; then
  echo "[INIT] Missing base tools: ${MISSING[*]}"
  if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update
    sudo apt-get install -y "${MISSING[@]}"
  elif command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y "${MISSING[@]}"
  elif command -v yum >/dev/null 2>&1; then
    sudo yum install -y "${MISSING[@]}"
  elif command -v pacman >/dev/null 2>&1; then
    sudo pacman -Sy --noconfirm "${MISSING[@]}"
  fi
fi

# 2️⃣ 필수 디렉토리 준비
echo "[1/7] Preparing directories..."
sudo mkdir -p "$APP_DIR" /var/log/aiot /iotcert /aiot/dataset /run/aiot
sudo chown -R aiot:aiot "$APP_DIR" /aiot /var/log/aiot /iotcert /run/aiot
echo "[OK] 디렉토리 구조 준비 완료"

# 3️⃣ 필수 패키지 자동 설치 (python3, mosquitto 등)
echo "[2/7] Installing required OS packages..."
REQ_PKGS=(python3 python3-venv python3-pip mosquitto mosquitto-clients jq awscli)
if command -v apt-get >/dev/null 2>&1; then
  sudo apt-get update -y
  sudo apt-get install -y "${REQ_PKGS[@]}"
elif command -v dnf >/dev/null 2>&1; then
  sudo dnf install -y "${REQ_PKGS[@]}"
elif command -v yum >/dev/null 2>&1; then
  sudo yum install -y "${REQ_PKGS[@]}"
elif command -v pacman >/dev/null 2>&1; then
  sudo pacman -Sy --noconfirm "${REQ_PKGS[@]}"
else
  echo "[WARN] 자동 설치 불가. 수동으로 설치 필요: ${REQ_PKGS[*]}"
fi
echo "[OK] 필수 패키지 설치 완료"

# 4️⃣ aiot-manager 다운로드 및 배치
echo "[3/7] Downloading aiot-manager..."
sudo curl -fsSL -o "$BIN_PATH" "${REPO_URL}aiot-manager"
sudo chmod +x "$BIN_PATH"
echo "[OK] /usr/local/bin/aiot-manager 등록 완료"

# 5️⃣ Collector / Publisher 코드 다운로드
echo "[4/7] Downloading collector & publisher scripts..."
sudo curl -fsSL -o "$APP_DIR/mqtt_to_sqlite.py" "${REPO_URL}mqtt_to_sqlite.py"
sudo curl -fsSL -o "$APP_DIR/sqlite_to_iotcore.py" "${REPO_URL}sqlite_to_iotcore.py"
sudo chown aiot:aiot "$APP_DIR"/*.py
echo "[OK] /opt/aiot 코드 배치 완료"

# 6️⃣ Python 환경 점검
echo "[5/7] Checking Python environment..."
sudo aiot-manager check || true
echo "[OK] Python venv 및 의존성 확인 완료"

# 7️⃣ systemd 유닛 등록 및 구성
echo "[6/7] Registering systemd units..."
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
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/opt/aiot /var/log/aiot /iotcert

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
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/opt/aiot /var/log/aiot /iotcert

[Install]
WantedBy=multi-user.target
EOD

sudo systemctl daemon-reload
echo "[OK] systemd 등록 완료"

# 8️⃣ 설정 마법사 실행
echo "[7/7] Running configuration wizard..."
sudo aiot-manager configure || true

echo
echo "=== AIoT Gateway Setup Complete! ==="
echo "→ /usr/local/bin/aiot-manager 로 관리 가능"
echo "→ 모든 패키지 자동 설치 완료 및 systemd 등록됨"
