# Apartment Price Tracker

작게 시작하는 개인용 아파트 가격 추적 웹앱입니다.

## 로컬 실행

```powershell
pip install -r requirements.txt
$env:REAL_ESTATE_ENTRY_PASSWORD = "원하는-비밀번호"
python run_real_estate_server.py
```

브라우저에서 `http://127.0.0.1:5000`을 엽니다.


## 웹에서 가격 직접 입력

메인 화면의 **가격 직접 입력** 영역에서 단지명, 기준일, 가격, 입력 비밀번호를 입력하고 **가격 저장**을 누릅니다. 전용면적, 층, 비고는 선택 항목입니다. 가격은 원 단위 숫자로 입력하며, 저장 직후 해당 단지의 차트와 표가 갱신됩니다.

## 데이터 넣기

`data/apartment_prices.csv`에 아래 형식으로 행을 추가한 뒤 API로 가져올 수 있습니다.

```csv
source,apartment_name,observed_date,price_krw,area_m2,trade_type,floor,note
csv,래미안 원베일리,2026-06-01,4380000000,84.95,sale,중층,사용자 CSV 예시
```

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:5000/api/import-csv -Body @{ apartment_name = "래미안 원베일리"; path = "data/apartment_prices.csv"; password = "원하는-비밀번호" }
```

## 매일 오전 10시 수집

수집 대상 설정 파일을 만듭니다.

```bash
cp data/apartment_targets.example.json data/apartment_targets.json
```

`data/apartment_targets.json`의 `name`은 CSV의 `apartment_name`과 같아야 합니다.

수동 실행:

```bash
python -m real_estate_app.collect_prices --source csv
```

서버에서 systemd timer 등록:

```bash
sudo cp deploy/stock-test-collector.service /etc/systemd/system/
sudo cp deploy/stock-test-collector.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now stock-test-collector.timer
systemctl list-timers stock-test-collector.timer
```

로그 확인:

```bash
tail -f logs/collect_prices.log
sudo journalctl -u stock-test-collector.service -n 100
```

## 배포 후보

무료로 시작하기 쉬운 곳은 Render의 free web service입니다. 다만 무료 인스턴스는 일정 시간 미사용 시 sleep 될 수 있습니다. 운영 데이터가 쌓이면 SQLite 파일이 사라지지 않도록 Postgres 같은 외부 저장소를 붙이는 편이 좋습니다.

호갱노노는 `robots.txt`에서 일반 크롤링 금지를 명시하므로 자동 크롤러를 기본으로 활성화하지 않았습니다. 네이버부동산/아실도 실제 연결 전 최신 약관, 요청 제한, 인증 필요 여부를 확인해야 합니다. 장기적으로는 국토교통부 실거래가 API나 직접 내려받은 CSV를 1차 데이터로 쓰는 구성이 더 안정적입니다.
