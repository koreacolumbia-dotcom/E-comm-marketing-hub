# CSK Marketing Intelligence 2.0 — 운영 가이드

## 동작
- 매일 한국시간 10:05 GitHub Actions를 실행하며, 처음 병합 시에도 시험 빌드를 실행합니다.
- 결과: reports/marketing_intelligence_v2/index.html, latest.json, latest.md, state.json.
- Daily Digest 이메일·기존 레이아웃·기존 워크플로는 변경하지 않습니다.
- 광고 예산 변경, CRM 발송, 대행사 이메일 전송은 하지 않습니다.

## 데이터 연결과 판단 규칙
| 기능 | 원본 | 검증 기준 | 제한 |
|---|---|---|---|
| 상품 | reports/pdp_opportunity/data.json + meta.json | 데이터 종료일 일치, 최근 3일, 상품별 PDP 세션/장바구니/구매 수 일치 | 재고·가격·광고 원인은 미검증 가설 |
| Paid | Daily Digest JSON | 채널 세션 합계 차이 1% 이하, ETC 5% 미만, Paid 상세 합계 일치 | 광고비 없으면 ROAS 금지 |
| CRM | 비공개 합산 그룹 입력(기본 미연결) | 30명 이상, 그룹 수치만, 최근 7일 | 고객 식별자와 그룹 인원 공개 금지 |
| 액션 | v1 진단+상품+대행사+CRM 신호 | 동일 액션 ID 반복 감지 | 자동으로 DONE 변경 금지 |

## 처리 상태 수동 변경
config/marketing_intelligence_actions.json 파일의 overrides 객체에 실제 액션 ID로 상태를 입력합니다.
허용 상태는 OPEN, IN_PROGRESS, BLOCKED, DONE이며 담당자(owner)와 메모(note)를 추가할 수 있습니다.
예를 들어 PDP_QA에 IN_PROGRESS 상태와 데이터팀 담당 부서를 입력한 후 커밋하고 Actions 수동 실행을 하면 다음 집계부터 적용합니다.
DONE은 담당자가 실제로 검증을 완료한 뒤에만 지정해야 합니다. 데이터에서 액션이 사라지는 것은 해결의 증거가 아닙니다.

## CRM 연결
현재 신뢰할 만한 고객별 구매·가입 원천이 공개 보고서에 연결돼 있지 않아 가입 미구매, 첫구매 후 60일 이탈, PDP 반복 조회 미구매 세그먼트의 정의만 표시합니다.
원천 결합과 타깃 개인 식별 작업은 별도 권한을 가진 비공개 시스템에서만 수행해야 합니다.
저장소는 공개이므로 고객명·이메일·전화번호·주소·회원번호·주문별 상세정보를 커밋하지 마세요.
30명 미만 그룹은 차단하며, 유효한 그룹 집계가 있어도 공개 페이지에는 구체적인 회원수를 표시하지 않습니다.

## 검증
- GitHub Actions 실행 성공, 결과 파일 갱신, 실제 GitHub Pages 게시 완료를 구분해서 확인합니다.
- 마지막 Daily Digest 자체가 PARTIAL 또는 BLOCKED면 Paid 원인을 사업성과로 단정하지 않습니다.
- PDP 30일 데이터는 Daily KPI와 종료일이 다를 수 있으며 매출 하락 기여를 증명하지 않습니다.
- 실제로 실행한 개선 실험은 추후 별도 기준선과 대조해 평가하되 상관관계를 인과관계로 취급하지 않습니다.

## 확장 조건
상품별 어드민 주문·가격·실재고, 광고 매체별 검증된 비용, 개인정보 보호된 CRM 합계가 연결되면 각 엔진의 판단 정밀도를 확장할 수 있습니다.

## 2026-11-15 추가: Paid 비용 신선도·BigQuery 연결·실험 효과 검증

**Paid 비용**: reports/daily_digest/data/paid_media_spend_history.csv를 검사하며, 현재 파일 마지막 날짜는 2026-04-06입니다. 2026-11-14 보고서 기준 오래된 자료여서 ROAS를 산출하지 않습니다. CSV 형식은 date,channel,sub_channel,spend이고 같은 날짜·채널·매체 중복과 음수·미래 날짜를 차단합니다. 파일이 최신이어도 실제 광고 플랫폼 비용 원본과 매체 매핑을 대조하지 않았다면 최종 ROAS를 자동 확정하지 않습니다.

**BigQuery 조회 전용 진단**: 매일 10:05 GitHub Actions에서 scripts/build_secure_csk_signals.py를 실행합니다. 이미 등록된 GOOGLE_SA_JSON_B64와 BQ_PROJECT/BQ_RAW_DATASET/BQ_MART_DATASET 등을 사용합니다. 350MiB 쿼리 청구 제한과 스키마·최근 적재일 조건을 적용합니다. 쿼리 실패 시 0으로 대체하지 않고 query_error/missing/stale 상태로 분리합니다. 원본 권한이나 테이블이 없는 경우 담당자의 접근 권한 확인이 필요합니다.

**상품 실재고 주의**: BigQuery의 주문상품 마트는 주문된 상품·사이즈에만 재고 신호가 붙어 있어 전체 상품에 대한 최신 실재고 증거가 아닙니다. 완전한 실재고 연결은 별도 TB_ProductSizeStock 실시간 승인과 시점 검증이 필요하며, 그전까지 품절을 판매 하락의 확정 원인으로 주장하지 않습니다.

**CRM 비공개 집계**: TB_Member·TB_Order에 대응하는 BigQuery staging tables가 접근 가능하고 최신인 경우에만 두 개의 회원 그룹을 비공개 임시 경로 reports/private_aggregate_crm/segments.json에 작성합니다. 그룹별 30명 미만이면 억제하며, 공개 보고서에는 인원 자체를 게재하지 않습니다. 회원별 PII는 결과 파일에 기록하지 않습니다. ETL 회원 가입일 적재 범위가 전체 회원보다 짧을 수 있으므로 전체 회원 모수로 쓰지 마세요.

**성과 검증**: config/marketing_intelligence_experiments.json에 실제 개선 실행일과 추적 과제를 등록하세요. 예를 들면 experiments 배열 원소에 id, title, start_date, window_days:7(또는 14)을 입력합니다. 시작일 이전 7일과 이후 7일의 동일 요일을 비교하고 전일부가 모두 존재해야 결과를 생성합니다. 주문·매출·구매율 전후 결과는 observational_only이며, 실험 통제군 없는 환경에서는 인과효과로 인정하지 않습니다. 유효한 실행일이 없다면 기본 배열은 비워 둡니다.

**실패 시 처리**: CRM/광고/재고 연동 오류가 생겨도 완성되지 않은 지표를 0으로 출력하거나 기존 액션을 자동 완료하지 않습니다. 2.0 공개 화면에는 원천 신선도, 권한, 부분 연동 상태를 함께 보여줍니다. 기존 데일리 리포트 발송·광고 예산·CRM 캠페인에는 영향을 주지 않습니다.
