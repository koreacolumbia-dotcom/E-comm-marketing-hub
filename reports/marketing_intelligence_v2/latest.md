# CSK Marketing Intelligence 2.0 — 2026-11-15

데이터 상태: **BLOCKED**

## 주요 지표

## QA
- MISSING_SOURCE (block): 기준일 Daily Digest JSON이 없거나 읽을 수 없습니다.

## PDP 상품 분석 (ready)
- PDP 상품 분석 종료일 2026-11-16은 Daily Digest 기준일 2026-11-15과 다름. 별도 30일 관측으로만 사용하며 당일 매출 감소의 원인으로 단정하지 않음
- 상품 퍼널 지표는 30일 GA4 상품별 세션 기준. 재고/광고 원인 및 매출 하락 기여도를 직접 증명하지 않음
- C74YM3674342 M`s MT.Hood Down Jacket: 13,824 세션 · ATC 0.1% · 구매 0.0%
- C71XM4875265 CSC1938 Dually Mountain Outerchange: 791 세션 · ATC 0.1% · 구매 0.0%
- C74WR9847309 Amaze Puff Hooded Jacket: 348 세션 · ATC 0.0% · 구매 0.0%
- C76BL5658010 SNOWTROT PEAK ODX: 201 세션 · ATC 0.0% · 구매 0.0%
- C76PU7259461 Birdree Lite Sacoche: 164 세션 · ATC 0.0% · 구매 0.0%
- C71WR6042027 W Saudan Pro 3L Shell: 159 세션 · ATC 0.0% · 구매 0.0%
- C74XR1616256 Amaze Puff Vest: 144 세션 · ATC 0.0% · 구매 0.0%
- C76BL0303550 KONOS SPEED TRAIL ATR: 144 세션 · ATC 0.0% · 구매 0.0%

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
- [OPEN] P0 · PAID · 대행사 채널 귀속 데이터 우선 점검 (최근 관측 2026-11-15; 담당 데이터/퍼포먼스 대행사)
- [OPEN] P0 · DATA · MSSQL 실재고 조회 접근 문제 해결 (최근 관측 2026-11-15; 담당 인프라/DBA 담당)
- [OPEN] P0 · PAID · 광고비 원본 자동 수집 재개 (최근 관측 2026-11-15; 담당 퍼포먼스 대행사/매체 담당)
- [OPEN] P0 · DATA · CSC1938 Dually Mountain Outerchange 상품별 이벤트 정합성 검증 (최근 관측 2026-11-15; 담당 데이터/이커머스)
- [OPEN] P0 · DATA · M`s MT.Hood Down Jacket 상품별 이벤트 정합성 검증 (최근 관측 2026-11-15; 담당 데이터/이커머스)
- [OPEN] P0 · DATA · MSSQL → BigQuery 주문·CRM 적재 갱신 (최근 관측 2026-11-15; 담당 데이터 엔지니어/BigQuery 담당)
- [OPEN] P1 · PRODUCT · Amaze Puff Hooded Jacket 상세페이지 개선 검토 (최근 관측 2026-11-15; 담당 이커머스/MD)
- [OPEN] P1 · PRODUCT · SNOWTROT PEAK ODX 상세페이지 개선 검토 (최근 관측 2026-11-15; 담당 이커머스/MD)
- [OPEN] P2 · CRM · CRM 세그먼트 집계 연동 (최근 관측 2026-11-15; 담당 CRM/데이터 담당)
- [OPEN] P0 · DATA · 채널별 성과 분석 보류 (최근 관측 2026-11-14; 담당 데이터/GA4 담당)
- [OPEN] P0 · DATA · PDP 일별 조회 데이터 복구 (최근 관측 2026-11-14; 담당 데이터/BigQuery 담당)
- [OPEN] P1 · COMMERCE · 유입 대비 구매 전환 급락 (최근 관측 2026-11-14; 담당 이커머스/분석 담당)

원천과 제약: 인과 설명은 가설, 기간이 다른 상품 데이터는 당일 증감 원인에 사용하지 않음
