# CSK Marketing Intelligence 2.0 — 2026-11-27

데이터 상태: **PARTIAL**

## 주요 지표
- 세션: 14,861 | 전주 -59.4% | 전년 -25.0%
- 주문: 166 | 전주 -54.8% | 전년 -7.8%
- 매출: 28,761,608 | 전주 -50.4% | 전년 -17.2%
- 구매 CVR: 0 | 전주 +11.5% | 전년 +22.9%

## QA
- UNASSIGNED_TRAFFIC (block): ETC 21.6% — 미귀속 세션 상세 및 GA4 (not set)/(other) 점검 필요
- PAID_NOT_VERIFIED (info): Paid 상세 성과와 매체별 ROAS 판단을 보류합니다.
- ADMIN_UNAVAILABLE (info): 어드민 실적이 0으로 내려왔습니다. 실제 매출 0으로 단정하지 않습니다.

## PDP 상품 분석 (ready)
- PDP 상품 분석 종료일 2026-11-28은 Daily Digest 기준일 2026-11-27과 다름. 별도 30일 관측으로만 사용하며 당일 매출 감소의 원인으로 단정하지 않음
- 상품 퍼널 지표는 30일 GA4 상품별 세션 기준. 재고/광고 원인 및 매출 하락 기여도를 직접 증명하지 않음
- C74WR9847309 Amaze Puff Hooded Jacket: 718 세션 · ATC 0.1% · 구매 0.0%
- C75BM1935063 PFG PRO SPORT BOOT: 375 세션 · ATC 0.0% · 구매 0.0%
- C71WE8104491 Cassiar Pro ODX Shell: 264 세션 · ATC 0.0% · 구매 0.0%
- C74WE4490235 Sauvie Sky Quad Parka IC Jacket: 254 세션 · ATC 0.0% · 구매 0.0%
- C73AR3742278 ROC Wide Leg Pant: 232 세션 · ATC 0.0% · 구매 0.0%
- C76PU7257125 Birdree Lite Drawstring Backpack: 217 세션 · ATC 0.0% · 구매 0.0%
- C73AR7506010 Loneridge Legging: 161 세션 · ATC 0.0% · 구매 0.0%
- C71WR6042027 W Saudan Pro 3L Shell: 154 세션 · ATC 0.0% · 구매 0.0%

## 광고 대행사 점검 (blocked)
- GA4 공통: ETC/(not set)/(other) 비중과 Paid Ad/전체 세션 합계부터 복구해 주세요. 검증 전에는 매체별 ROAS로 예산 조정하지 않겠습니다.
- PMax: 최근 7일 캠페인별 비용·전환가치·주문·상품군·검색어/브랜드 유입 비중을 확인하고, Demand Gen/GDN과 별도 집계되는지 점검해 주세요.
- Criteo: 리타겟팅 대상·빈도·클릭 후 구매·뷰스루 기여와 GA4 세션/주문 차이를 구분해 공유해 주세요.
- Meta: 신규회원 확보 캠페인의 가입 이후 장바구니·구매 전환을 캠페인/타깃별로 구분해 점검해 주세요.

## CRM (not_connected)
- 가입 후 30일 미구매: 가입일이 30일 이상 경과했으며 유효 주문 0건
- 첫 구매 후 60일 재구매 없음: 유효 구매 정확히 1회, 구매 이후 60일 이상
- 최근 관심 상품 반복 조회·미구매: 최근 30일 동일 상품 PDP 반복 조회 및 장바구니, 해당 상품 구매 없음

## 원천 연결과 신선도
- 광고 비용: stale / 마지막 적재 2026-04-06
- 상품 어드민: stale
- 실재고: connection_error (완전한 재고 원본인지 별도 확인)
- CRM 원본: stale

## 실행 전후 관측 (인과효과 아님)
- 등록된 개선 실험 없음

## 미해결 액션
- [OPEN] P0 · PAID · 대행사 채널 귀속 데이터 우선 점검 (최근 관측 2026-11-27; 담당 데이터/퍼포먼스 대행사)
- [OPEN] P0 · DATA · 채널별 성과 분석 보류 (최근 관측 2026-11-27; 담당 데이터/GA4 담당)
- [OPEN] P0 · DATA · MSSQL 실재고 조회 접근 문제 해결 (최근 관측 2026-11-27; 담당 인프라/DBA 담당)
- [OPEN] P0 · PAID · 광고비 원본 자동 수집 재개 (최근 관측 2026-11-27; 담당 퍼포먼스 대행사/매체 담당)
- [OPEN] P0 · DATA · Amaze Puff Hooded Jacket 상품별 이벤트 정합성 검증 (최근 관측 2026-11-27; 담당 데이터/이커머스)
- [OPEN] P0 · DATA · MSSQL → BigQuery 주문·CRM 적재 갱신 (최근 관측 2026-11-27; 담당 데이터 엔지니어/BigQuery 담당)
- [OPEN] P1 · PRODUCT · Cassiar Pro ODX Shell 상세페이지 개선 검토 (최근 관측 2026-11-27; 담당 이커머스/MD)
- [OPEN] P1 · PRODUCT · Sauvie Sky Quad Parka IC Jacket 상세페이지 개선 검토 (최근 관측 2026-11-27; 담당 이커머스/MD)
- [OPEN] P1 · PRODUCT · PFG PRO SPORT BOOT 상세페이지 개선 검토 (최근 관측 2026-11-27; 담당 이커머스/MD)
- [OPEN] P2 · CRM · CRM 세그먼트 집계 연동 (최근 관측 2026-11-27; 담당 CRM/데이터 담당)
- [OPEN] P0 · DATA · PDP 일별 조회 데이터 복구 (최근 관측 2026-11-14; 담당 데이터/BigQuery 담당)
- [OPEN] P0 · DATA · CSC1938 Dually Mountain Outerchange 상품별 이벤트 정합성 검증 (최근 관측 2026-11-18; 담당 데이터/이커머스)
- [OPEN] P0 · DATA · M`s MT.Hood Down Jacket 상품별 이벤트 정합성 검증 (최근 관측 2026-11-21; 담당 데이터/이커머스)
- [OPEN] P1 · COMMERCE · 유입 대비 구매 전환 급락 (최근 관측 2026-11-14; 담당 이커머스/분석 담당)
- [OPEN] P1 · PRODUCT · ROC Wide Leg Pant 상세페이지 개선 검토 (최근 관측 2026-11-25; 담당 이커머스/MD)
- [OPEN] P1 · PRODUCT · SNOWTROT PEAK ODX 상세페이지 개선 검토 (최근 관측 2026-11-15; 담당 이커머스/MD)
- [OPEN] P1 · PRODUCT · Birdree Lite Drawstring Backpack 상세페이지 개선 검토 (최근 관측 2026-11-22; 담당 이커머스/MD)
- [OPEN] P1 · PRODUCT · Birdree Lite Drawstring Backpack 상세페이지 개선 검토 (최근 관측 2026-11-24; 담당 이커머스/MD)
- [OPEN] P2 · COMMERCE · 주문당 매출 감소 (최근 관측 2026-11-18; 담당 이커머스/MD)

원천과 제약: 인과 설명은 가설, 기간이 다른 상품 데이터는 당일 증감 원인에 사용하지 않음
