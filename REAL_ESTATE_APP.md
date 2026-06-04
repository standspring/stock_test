# Apartment Price Tracker

작게 시작하는 개인용 아파트 가격 추적 웹앱입니다.

## 로컬 실행

```powershell
pip install -r requirements.txt
python run_real_estate_server.py
```

브라우저에서 `http://127.0.0.1:5000`을 엽니다.

## 데이터 넣기

`data/apartment_prices.csv`에 아래 형식으로 행을 추가한 뒤 API로 가져올 수 있습니다.

```csv
source,apartment_name,observed_date,price_krw,area_m2,trade_type,floor,note
csv,래미안 원베일리,2026-06-01,4380000000,84.95,sale,중층,사용자 CSV 예시
```

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:5000/api/import-csv -Body @{ apartment_name = "래미안 원베일리"; path = "data/apartment_prices.csv" }
```

## 배포 후보

무료로 시작하기 쉬운 곳은 Render의 free web service입니다. 다만 무료 인스턴스는 일정 시간 미사용 시 sleep 될 수 있습니다. 운영 데이터가 쌓이면 SQLite 파일이 사라지지 않도록 Postgres 같은 외부 저장소를 붙이는 편이 좋습니다.

호갱노노는 `robots.txt`에서 일반 크롤링 금지를 명시하므로 자동 크롤러를 기본으로 활성화하지 않았습니다. 네이버부동산/아실도 실제 연결 전 최신 약관, 요청 제한, 인증 필요 여부를 확인해야 합니다. 장기적으로는 국토교통부 실거래가 API나 직접 내려받은 CSV를 1차 데이터로 쓰는 구성이 더 안정적입니다.
