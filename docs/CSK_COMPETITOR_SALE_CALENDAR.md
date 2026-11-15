# CSK 경쟁사 세일 캘린더 운영 안내

## 진입 경로

- CSK Hub 사이드바 **경쟁사 세일 캘린더**, 모바일 **세일** 탭.
- 단독 대시보드: `reports/competitor_sale_calendar/index.html` (데이터 `latest.json`).
- 전월/다음월, 기준월, 브랜드, 확인 유형(기간 확인/배너 관측), 행사명 검색, 날짜 클릭 상세 및 근거 링크.

## 원천과 판정

1. 기존 `reports/hero_main_banners_YYYYMMDD_HHMMSS.csv`를 최근 65일 범위에서 날짜별 최신 1건만 읽습니다. 동일 배너가 여러 날 관측되면 `observed_on` 배열에 **해당 날짜만** 보관합니다. 중간 미수집일을 연속된 실제 행사로 칠하지 않습니다.
2. CSV의 랜딩 안내문(`landing_title`, `landing_summary`, `landing_text_status=ok`)에서 공식몰 도메인에 속하고 세일 표현 및 명확한 일자 범위가 있을 때만 `verified_period`로 분류합니다. 배너 CSV의 `plan_start/plan_end`는 실제 행사 일정과 불일치할 수 있어 사용하지 않습니다.
3. 이벤트 목록 또는 공식 판매채널 약관에 기간이 적힌 행사는 `config/competitor_sale_events_verified.json`에 출처 URL, 시작/종료일, 혜택을 수동 검증해 등록합니다. 종료일 미확인/2999년 종료일은 `end_date: null`로 처리하고 시작일에만 표시합니다.
4. 다른 플랫폼으로 이동하는 광고 리다이렉트 URL은 링크로 사용하지 않습니다. 공식몰/공식 판매처 도메인에서만 링크를 열 수 있습니다.
5. 세일 홍보 문구에 나온 최대 할인율이나 쿠폰은 **모든 상품에 동일 적용된 할인율이 아닙니다**. 쿠폰과 즉시할인도 합산 계산하지 않습니다.

## 선등록한 출처 확인 행사

- 스노우피크 다운 컬렉션 최대 9만원 할인: 공식 이벤트 목록에서 **2026-10-22 시작**, 종료일 불명 (`https://www.snowpeakstore.co.kr/board/list.php?bdId=event`).
- 노스페이스 SSF SHOP 26 F/W 블랙프라이데이: **2026-11-12 10:00 ~ 2026-12-04 09:59**, 최대 30% 추가 할인 (`https://www.ssfshop.com/special/125470/view`). 이것은 **SSF SHOP 판매채널 행사**로 구분합니다.
- 블랙야크 9월 쇼핑지원금: **2026-09-01 ~ 2026-10-11**, 최대 15% (`https://m.byn.kr/_container/exh_list.php?hta_brand=blackyak`). 과거 기록입니다.

## 자동 갱신

- `.github/workflows/competitor-sale-calendar.yml`: 매일 **08:40 및 17:40 KST**, 변경 푸시 또는 수동 실행.
- 표본은 기존 기획전 배너 수집 작업이 성공적으로 만든 CSV입니다. **이 워크플로는 경쟁사 사이트를 새로 크롤링하지 않습니다.** 원천 수집이 중단되면 `status=stale/no_snapshots`로 표시하고 임의로 행사를 생성하지 않습니다.
- 원본과 출력은 공개 저장소의 비개인정보 마케팅 경쟁 정보입니다. 광고 예산, 이메일 발송, CRM 및 운영 DB를 변경하지 않습니다.
- 실제 데이터 갱신: `python scripts/build_competitor_sale_calendar.py --as-of YYYY-MM-DD --lookback 65`
- QA: `python -m unittest discover -s tests -p test_competitor_sale_calendar.py -v`

## 확장

더 많은 실시간 세일을 정확한 종료일까지 표시하려면 각 경쟁사 공식 이벤트 목록 URL을 별도 읽기 전용 커넥터로 추가하고 행사 기간/약관을 직접 파싱·검증해야 합니다. 브랜드 상품 전체 평균 세일 비중은 기간형 프로모션의 증거가 아니므로 달력 이벤트로 쓰지 않습니다.
