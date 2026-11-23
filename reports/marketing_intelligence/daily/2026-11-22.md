# CSK Marketing Intelligence — 2026-11-22
검증 상태: PARTIAL

## KPI (전주 동요일 / 전년 동요일)
- 세션: 26,861 | WoW +71.2% | YoY -0.6%
- 주문: 325 | WoW +301.2% | YoY +47.1%
- 매출: 48,544,799원 | WoW +312.6% | YoY +56.3%
- 구매 CVR: 1.21% | WoW +134.3% | YoY +47.9%

## 데이터 품질
- [BLOCK] ETC 15.4% — 미귀속 세션 상세 및 GA4 (not set)/(other) 점검 필요
- [INFO] Paid 상세 성과와 매체별 ROAS 판단을 보류합니다.
- [INFO] 어드민 실적이 0으로 내려왔습니다. 실제 매출 0으로 단정하지 않습니다.

## 원인 가설 및 업무 액션
### P0 채널별 성과 분석 보류
- 관측: 채널 합계 또는 ETC 귀속 상태가 기준을 충족하지 않음
- 가설: GA4 집계 시점 지연, session 차원 중복, 귀속 분류 미확정 가능성
- 검증: 동일 날짜 GA4 전체 sessions vs sessionDefaultChannelGroup 합계 비교, (not set)/(other) 상세, 조회 API 샘플링·쿼리 차원 확인
- 담당: 데이터/GA4 담당


데이터 미검증 영역의 원인 설명은 가설이며 광고 예산 조정은 자동 수행하지 않습니다. 매체 ROAS는 신뢰 가능한 비용 데이터 부재 시 계산하지 않습니다.

원본: reports/daily_digest/data/daily/2026-11-22.json
