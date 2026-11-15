# CSK Marketing Intelligence — 2026-11-14
검증 상태: PARTIAL

## KPI (전주 동요일 / 전년 동요일)
- 세션: 17,973 | WoW +6.6% | YoY +0.7%
- 주문: 72 | WoW -31.4% | YoY +22.0%
- 매출: 10,764,610원 | WoW -29.4% | YoY +30.3%
- 구매 CVR: 0.40% | WoW -35.6% | YoY +21.2%

## 데이터 품질
- [BLOCK] 채널 합계 19,850 vs 전체 17,973 (차이 +10.4%) — 채널별 판단 중단
- [WARNING] ETC 11.7% — 미귀속 세션 상세 및 GA4 (not set)/(other) 점검 필요
- [BLOCK] PDP 최신일 수집 누락 또는 카테고리 매핑 오류 — 0을 실제 조회수로 해석 금지
- [INFO] Paid 상세 성과와 매체별 ROAS 판단을 보류합니다.
- [INFO] 어드민 실적이 0으로 내려왔습니다. 실제 매출 0으로 단정하지 않습니다.

## 원인 가설 및 업무 액션
### P0 PDP 일별 조회 데이터 복구
- 관측: 최신일 카테고리 조회수가 누락 또는 0으로 기록됨
- 가설: GA4 BigQuery events_* 일별 export 지연, intraday 적재 또는 상품 카테고리 매핑 문제
- 검증: events_*와 events_intraday_*의 최근 3일 view_item 날짜별 건수 및 item_category 원본 대조
- 담당: 데이터/BigQuery 담당

### P0 채널별 성과 분석 보류
- 관측: 채널 합계 또는 ETC 귀속 상태가 기준을 충족하지 않음
- 가설: GA4 집계 시점 지연, session 차원 중복, 귀속 분류 미확정 가능성
- 검증: 동일 날짜 GA4 전체 sessions vs sessionDefaultChannelGroup 합계 비교, (not set)/(other) 상세, 조회 API 샘플링·쿼리 차원 확인
- 담당: 데이터/GA4 담당

### P1 유입 대비 구매 전환 급락
- 관측: 전주 동요일 대비 세션 +6.6%, 주문 -31.4%, CVR -35.6%
- 가설: 랜딩·상품 구성·가격·재고·결제 경험 중 하나 이상이 영향을 줬을 가능성
- 검증: 채널별 신규/재방문 구매율, PDP→Cart→Checkout→Purchase, 상품별 재고·가격을 비교
- 담당: 이커머스/분석 담당


데이터 미검증 영역의 원인 설명은 가설이며 광고 예산 조정은 자동 수행하지 않습니다. 매체 ROAS는 신뢰 가능한 비용 데이터 부재 시 계산하지 않습니다.

원본: reports/daily_digest/data/daily/2026-11-14.json
