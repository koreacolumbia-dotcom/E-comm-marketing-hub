# CSK Marketing Operations 3.0 — 2026-11-14

**보고 상태: PARTIAL** · 원천 복구 우선

## Executive Summary
복구 및 원천 정합성 확인이 최우선. Paid 예산 재배분이나 상품 예상 매출액은 데이터 검증 전 실행하지 않음.

### 원천 신뢰도
- paid_cost: stale
- order_mart: stale
- crm: stale
- inventory: connection_error

## 7일 KPI (전주 완결 기간 비교)
상태: **ready**
기간: 2026-11-08 ~ 2026-11-14 / 비교: 2026-11-01 ~ 2026-11-07
- 세션 120,752 · WoW +2.7%
- 주문 757 · WoW -0.4%
- 매출 ₩133,278,030 · WoW +12.2%
- 구매 CVR 0.63% · 상대 WoW -3.0%

## 마케팅 캘린더 (기간과 출처 확인)
올해 2026-11-08 ~ 2026-11-14: not_connected
전년 동요일 대응 기간 2025-11-09 ~ 2025-11-15: available
### 전년 확인 활동
- 2025-11-03 [event] 겨울 아우터 옥외광고 · W45 · 8p
- 2025-11-10 [event] 3차 래플 이벤트 · W46 · 4p
- 2025-11-10 [message] 래플 이벤트 3차 · 트래픽현황 2026 · 2025 비교기준 52행
- 2025-11-11 [event] 무신사 라이브 커머스 · W46 · 4p, 11p
- 2025-11-13 [event] Winter Boots 콘텐츠 · W46 · 8p
- 2025-11-13 [event] Winter Down 콘텐츠 · W46 · 4p, 7p
- 2025-11-14 [message] (광고) [컬럼비아] 일상과 아웃도어 어디에서도 스타일과 보온성이 뛰어난 윈터부츠 · 트래픽현황 2026 · 2025 비교기준 70행

## 상품/PDP 개선 검토
- [MEASUREMENT_QA] M`s MT.Hood Down Jacket (C74YM3674342) · PDP 13,569 · Cart 0.10% · 구매 이벤트와 어드민 상품코드 일치 여부 검증 필요
- [MEASUREMENT_QA] CSC1938 Dually Mountain Outerchange (C71XM4875265) · PDP 799 · Cart 0.13% · 구매 이벤트와 어드민 상품코드 일치 여부 검증 필요
- [CRO_HYPOTHESIS] Amaze Puff Hooded Jacket (C74WR9847309) · PDP 341 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전
- [CRO_HYPOTHESIS] SNOWTROT PEAK ODX (C76BL5658010) · PDP 191 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전
- [CRO_HYPOTHESIS] Birdree Lite Sacoche (C76PU7259461) · PDP 166 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전
- [CRO_HYPOTHESIS] W Saudan Pro 3L Shell (C71WR6042027) · PDP 161 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전
- [CRO_HYPOTHESIS] KONOS SPEED TRAIL ATR (C76BL0303550) · PDP 146 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전
- [CRO_HYPOTHESIS] Amaze Puff Vest (C74XR1616256) · PDP 144 · Cart 0.00% · PDP→Cart 이탈 후보. 유입 매체·재고·상세페이지 요인 검증 전

## 데이터 복구·담당 부서 액션
- **PAID_COST_FRESHNESS** [매체 담당/대행사]: 광고비 상태 stale, 최종 비용일 2026-04-06. 광고 매체별 원천 export/API 인증·적재 스케줄 복구, 일별 합계·통화/세금/캠페인 코드 대조
- **WAREHOUSE_ETL_FRESHNESS** [데이터 엔지니어]: 주문 마트 stale, CRM stale. 원천/대상 수정시각·ETL 로그·삭제/덮어쓰기 모드 검토 후 증분 동기화 및 어드민 주문건수 대조
- **MSSQL_STOCK_ACCESS** [인프라/DBA]: MSSQL 재고 접근 상태 connection_error. GitHub Actions 러너에서 허용 IP/VPN/DB 포트 1433과 읽기 전용 연결 진단
- **CHANNEL_QA** [GA4/BigQuery 담당]: 채널 합산과 전체 세션 불일치, ETC 귀속 오류. 동일 세션 키/기간/귀속 규칙으로 재집계 후 ETC·Paid·Organic 중복 제거 확인
- **PDP_QA** [GA4/BigQuery 담당]: PDP 최신일 수집/상품 이벤트 집계 검증 실패. view_item와 purchase.item_id/상품 SKU 매핑, 최신일 지연과 중복 이벤트 확인

## CRM/캠페인 증분효과
상태: awaiting_private_aggregates
A/B 홀드아웃 원천 미연결. 순증분 매출·유의성 수치 생성하지 않음

## 데이터 제약
- 2025 confirmed activity is not evidence of incremental sales
- no current 2026 calendar records does not mean zero actual activity
- current stock is not historical inventory
- campaign control-group incremental results require protected source
