# CSK Marketing Operations 3.0 — 2026-11-25

**보고 상태: READY** · 원천 복구 우선

## Executive Summary
복구 및 원천 정합성 확인이 최우선. Paid 예산 재배분이나 상품 예상 매출액은 데이터 검증 전 실행하지 않음.

### 원천 신뢰도
- paid_cost: stale
- order_mart: stale
- crm: stale
- inventory: connection_error

## 7일 KPI (전주 완결 기간 비교)
상태: **ready**
기간: 2026-11-19 ~ 2026-11-25 / 비교: 2026-11-12 ~ 2026-11-18
- 세션 182,419 · WoW +57.1%
- 주문 1,714 · WoW +179.2%
- 매출 ₩284,524,125 · WoW +209.5%
- 구매 CVR 0.94% · 상대 WoW +77.7%

## 마케팅 캘린더 (기간과 출처 확인)
올해 2026-11-19 ~ 2026-11-25: records_present
전년 동요일 대응 기간 2025-11-20 ~ 2025-11-26: available
### 전년 확인 활동
- 2025-11-21 [event] Winter Festival 본행사 · W47 · 4~7p
- 2025-11-21 [message] 카카오 · Winter Festival · W47 · 3p
### 전년 계획만 확인된 활동 (실행 미확인)
- 2025-11-21 윈터페스티벌 1차
- 2025-11-21 윈터페스티벌 2차
- 2025-11-24 LMS · Winter Festival
- 2025-11-24 윈터페스티벌 2차

## 상품/PDP 개선 검토
- [MEASUREMENT_QA] Amaze Puff Hooded Jacket (C74WR9847309) · PDP 704 · Cart 0.14% · 구매 이벤트와 어드민 상품코드 일치 여부 검증 필요
- [CRO_HYPOTHESIS] PFG PRO SPORT BOOT (C75BM1935063) · PDP 379 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전
- [CRO_HYPOTHESIS] Sauvie Sky Quad Parka IC Jacket (C74WE4490235) · PDP 249 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전
- [CRO_HYPOTHESIS] ROC Wide Leg Pant (C73AR3742278) · PDP 238 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전
- [CRO_HYPOTHESIS] Birdree Lite Drawstring Backpack (C76PU7257125) · PDP 219 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전
- [CRO_HYPOTHESIS] Loneridge Legging (C73AR7506010) · PDP 163 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전
- [CRO_HYPOTHESIS] W Saudan Pro 3L Shell (C71WR6042027) · PDP 153 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전
- [CRO_HYPOTHESIS] RUDDY LEAF SHORT II (C75YU8771010) · PDP 146 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전

## 경쟁사 가격 참고 (동일 SKU 비교 불가)
수집 상태: reference_fresh
브랜드 평균 가격은 상품 구성·시즌·판매처가 달라 동일 SKU의 가격 차이를 증명하지 않음
- DISCOVERY: 수집상품 5,702, 표기 평균가 ₩60,795, 할인 상품 비율 46.1% (수집일 2026-11-25, recent)
- NEW_BALANCE: 수집상품 5,308, 표기 평균가 ₩113,699, 할인 상품 비율 1.8% (수집일 2026-11-25, recent)
- THE_NORTH_FACE: 수집상품 3,631, 표기 평균가 ₩144,729, 할인 상품 비율 0.0% (수집일 2026-11-25, recent)
- NEPA: 수집상품 3,208, 표기 평균가 ₩89,818, 할인 상품 비율 0.0% (수집일 2026-11-25, recent)
- MILLET: 수집상품 2,283, 표기 평균가 ₩114,996, 할인 상품 비율 92.2% (수집일 2026-11-25, recent)
- COLUMBIA: 수집상품 1,614, 표기 평균가 ₩129,884, 할인 상품 비율 0.0% (수집일 2026-11-25, recent)
- ARCTERYX: 수집상품 668, 표기 평균가 ₩220,000, 할인 상품 비율 0.0% (수집일 2026-11-25, recent)

## 신상품 7·14·30일
상태: launch_registry_missing
승인된 신상품 출시일·상품코드 원본 미등록

## 광고 소재 피로도
상태: not_connected
Meta/Google 소재별 14일 노출·클릭 원본 미연결

## 크리에이터 크루 릴스/피드
상태: not_connected
크리에이터별 원본 인사이트 비공개/미연결. 실제와 추정 성과를 섞지 않음

## 데이터 복구·담당 부서 액션
- **PAID_COST_FRESHNESS** [매체 담당/대행사]: 광고비 상태 stale, 최종 비용일 2026-04-06. 광고 매체별 원천 export/API 인증·적재 스케줄 복구, 일별 합계·통화/세금/캠페인 코드 대조
- **WAREHOUSE_ETL_FRESHNESS** [데이터 엔지니어]: 주문 마트 stale, CRM stale. 원천/대상 수정시각·ETL 로그·삭제/덮어쓰기 모드 검토 후 증분 동기화 및 어드민 주문건수 대조
- **MSSQL_STOCK_ACCESS** [인프라/DBA]: MSSQL 재고 접근 상태 connection_error. GitHub Actions 러너에서 허용 IP/VPN/DB 포트 1433과 읽기 전용 연결 진단

## CRM/캠페인 증분효과
상태: awaiting_private_aggregates
A/B 홀드아웃 원천 미연결. 순증분 매출·유의성 수치 생성하지 않음

## 데이터 제약
- 2025 confirmed activity is not evidence of incremental sales
- no current 2026 calendar records does not mean zero actual activity
- current stock is not historical inventory
- campaign control-group incremental results require protected source
