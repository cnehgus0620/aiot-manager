# 🧠 AIoT Manager – Gateway Installer & Automation Suite

이 프로젝트는 **라즈베리파이 / 젯슨 나노 기반 IoT 게이트웨이**에서  
센서 데이터를 수집(SQLite 저장)하고,  
5분 단위로 AWS IoT Core를 통해 S3로 전송하는  
**완전 자동화형 AIoT 관리 시스템**입니다.

---

## 🧩 주요 구성

| 구성요소 | 설명 |
|-----------|------|
| `aiot-manager` | 게이트웨이 런처 및 관리 CLI (`/usr/local/bin/`에 설치됨) |
| `aiot_init.sh` | 게이트웨이 초기화 스크립트 (GitHub에서 자동 배포) |
| `mqtt_to_sqlite.py` | MQTT → SQLite 실시간 수집기 |
| `sqlite_to_iotcore.py` | SQLite → AWS IoT Core 퍼블리셔 (5분 단위 집계 및 전송) |

---

## ⚙️ 설치 방법

### 1️⃣ 설치 스크립트 실행

게이트웨이(라즈베리파이 또는 젯슨 나노)에서 다음 한 줄로 설치:

```bash
curl -fsSL https://raw.githubusercontent.com/cnehgus0620/aiot-manager/main/aiot_init.sh | bash
```

---

### 2️⃣ 자동 설치되는 항목

| 항목 | 설치 경로 | 설명 |
|------|-------------|------|
| aiot-manager | `/usr/local/bin/aiot-manager` | 관리용 명령어 |
| mqtt_to_sqlite.py | `/opt/aiot/mqtt_to_sqlite.py` | 데이터 수집기 |
| sqlite_to_iotcore.py | `/opt/aiot/sqlite_to_iotcore.py` | 퍼블리셔 |
| systemd 유닛 | `/etc/systemd/system/aiot-collector.service`, `/etc/systemd/system/aiot-publisher@.service` | 부팅 시 자동 실행 |

---

## 🧙 설치 마법사 (configure 단계)

설치 중 실행되는 **환경 설정 마법사**는 아래 항목들을 순서대로 설정합니다.

1. **AWS CLI 로그인 확인** (`aws configure` 필요)
2. **디바이스 유형 선택**  
   - Raspberry Pi  
   - Jetson Nano
3. **게이트웨이 번호 입력**  
   → Thing 이름 자동 생성 (`RaspberryPi5_IoT_Thing_Gateway_<번호>`)
4. **방 번호 입력**  
   → 자동으로 `room-<번호>` 형식 적용
5. **AWS IoT 엔드포인트 자동 감지**  
   (`aws iot describe-endpoint` 명령으로 자동 획득)

---

## 🚀 실행 및 관리

### 1. 서비스 시작
```bash
aiot-manager collector start
aiot-manager publisher start
```

### 2. 서비스 상태 확인
```bash
aiot-manager status
```

### 3. 점검 및 패키지 검사
```bash
aiot-manager check
```

### 4. 설정 재구성 (마법사 재실행)
```bash
aiot-manager configure
```

---

## 🔁 부팅 자동 실행

설치 후 자동으로 systemd에 등록되어  
재부팅 시 자동으로 collector/publisher가 실행됩니다.

```bash
sudo systemctl enable aiot-collector
sudo systemctl enable aiot-publisher@306
sudo systemctl start aiot-collector
sudo systemctl start aiot-publisher@306
```

---

## 🧰 디렉터리 구조

```plaintext
/opt/aiot/
 ├── mqtt_to_sqlite.py       # 수집기
 ├── sqlite_to_iotcore.py    # 퍼블리셔
/usr/local/bin/
 └── aiot-manager            # CLI 관리 도구
/etc/aiot/
 ├── room                    # ex) room-306
 ├── endpoint                # AWS IoT endpoint
/etc/systemd/system/
 ├── aiot-collector.service
 └── aiot-publisher@.service
```

---

## 📦 필수 조건

| 구성요소 | 설명 |
|----------|------|
| AWS CLI (`aws`) | 로그인 필요 (`aws configure`) |
| Python ≥ 3.8 | 라즈베리파이 OS / Jetson Ubuntu 기본 지원 |
| mosquitto | 로컬 MQTT 브로커 |
| boto3, paho-mqtt, AWSIoTPythonSDK | Python 모듈 자동 설치됨 |

---

## 💬 예시 동작 로그

```bash
[BOOT] DB  max end: 2025-11-08 08:35:00 UTC / 2025-11-08 17:35:00 KST
[WINDOW][UTC] 2025-11-08 08:35:00 ~ 2025-11-08 08:40:00 : aggregating ...
[PUBLISH] esp8266-306 (2025-11-08 17:35) -> iot/sensor/minute
```

---

## 🧑‍💻 주요 명령 요약

| 명령 | 설명 |
|------|------|
| `aiot-manager check` | 설치 점검 및 패키지 검사 |
| `aiot-manager configure` | 환경 설정 마법사 실행 |
| `aiot-manager status` | collector/publisher 상태 확인 |
| `aiot-manager collector start/stop/restart` | 수집기 제어 |
| `aiot-manager publisher start/stop/restart` | 퍼블리셔 제어 |

---

## ⚠️ 주의사항

- `aws configure` 는 **aiot_init.sh 실행 전에 완료되어야 함**  
- `/iotcert/` 폴더에 인증서(`.pem` 3종)가 존재해야 함  
- `/opt/aiot/` 의 소유자는 `aiot` 사용자여야 함  
- `aiot-manager` 실행은 `sudo` 권한으로 권장  

---

## 📄 License
MIT License © 2025 [cnehgus0620](https://github.com/cnehgus0620)
