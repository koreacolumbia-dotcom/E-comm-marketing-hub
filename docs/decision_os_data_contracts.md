# Decision OS Data Contracts

Decision OS는 아래 경로가 생성되면 다음 실행부터 자동으로 `waiting`에서 `live`로 전환됩니다. 실제 데이터가 없는 빈 파일은 생성하지 마십시오.

## 1. 실시간 GA4

경로: `reports/realtime_alerts/alerts.json`

필수 키:

```json
{
  "observed_hour": "2026-07-17T22:00:00+09:00",
  "alerts": [],
  "status": "healthy",
  "alert_count": 0
}
```

## 2. Canonical commerce

경로: `reports/canonical/snapshot.json`

```json
{
  "metrics": {
    "sessions": 0,
    "users": 0,
    "orders": 0,
    "revenue": 0,
    "cvr": 0,
    "aov": 0
  }
}
```

## 3. 재고·가격·옵션

경로: `reports/commerce_ops/inventory.json`

```json
{
  "generated_at": "2026-07-17T23:00:00+09:00",
  "products": [
    {
      "product_code": "C72FM7047",
      "price": 129000,
      "stock": 42,
      "sold_out_option_rate": 0.25
    }
  ]
}
```

## 4. 어드민 시간대별 주문

경로: `reports/commerce_ops/orders_hourly.json`

```json
{
  "generated_at": "2026-07-17T23:00:00+09:00",
  "hours": [
    {
      "hour_kst": "2026-07-17 22:00:00",
      "orders": 12,
      "revenue": 1450000
    }
  ]
}
```

이 데이터가 연결되면 GA4 태깅 장애와 실제 주문 장애를 구분하는 근거로 사용합니다.

## 5. PG 승인 데이터

경로: `reports/commerce_ops/pg_hourly.json`

```json
{
  "generated_at": "2026-07-17T23:00:00+09:00",
  "hours": [
    {
      "hour_kst": "2026-07-17 22:00:00",
      "approved_transactions": 12,
      "approved_amount": 1450000,
      "failed_transactions": 2
    }
  ]
}
```

## 6. 상품 마진

경로: `reports/commerce_ops/margin.json`

전체 기본 마진율만 연결하는 경우:

```json
{
  "gross_margin_rate": 0.58
}
```

상품 단위 확장:

```json
{
  "gross_margin_rate": 0.58,
  "products": [
    {
      "product_code": "C72FM7047",
      "gross_margin_rate": 0.61
    }
  ]
}
```

## 7. Paid media

경로: `reports/paid_media/summary.json`

```json
{
  "spend": 10000000,
  "revenue": 42000000,
  "roas": 420,
  "platforms": [
    {
      "name": "Meta",
      "spend": 4000000,
      "revenue": 13000000,
      "roas": 325,
      "orders": 120,
      "new_customers": 54
    }
  ]
}
```

## 8. 프로모션 캘린더

경로: `reports/commerce_ops/promotion_calendar.json`

```json
{
  "promotions": [
    {
      "start": "2026-07-17T00:00:00+09:00",
      "end": "2026-07-20T23:59:59+09:00",
      "name": "Summer Promotion",
      "channels": ["all"],
      "expected_traffic_lift": 0.35
    }
  ]
}
```

## 9. 날씨·외부 신호

경로: `reports/external/weather.json`

```json
{
  "generated_at": "2026-07-17T23:00:00+09:00",
  "location": "Seoul",
  "temperature": 29,
  "rain_mm": 12,
  "condition": "rain"
}
```

## 연결 원칙

- 숫자가 없는 빈 파일을 생성하지 않습니다.
- 생성 시각과 데이터 관측 시각을 구분합니다.
- 금액은 원 단위 숫자로 저장합니다.
- 비율은 `0.25` 형식으로 저장합니다.
- 개인정보, 이메일, 전화번호, 주문자명은 산출물에 포함하지 않습니다.
- 운영 원천은 집계 데이터만 저장합니다.
