# CSK Marketing Operations 3.0

## 목적과 운영
- Marketing Intelligence 2.0 원천 결과에 마케팅 일정·상품 검증·안전한 복구 액션·홀드아웃 실험·Executive Summary를 연결합니다.
- 2.0 완료 후 자동 실행하고 별도 예비 스케줄로 매일 오전 10:20 KST 점검합니다. 기존 데일리 메일과 광고/CRM 설정은 바꾸지 않습니다.
- 산출물: `reports/marketing_operations/index.html`, `latest.md`, `latest.json`.
- 원본 보고일과 2.0 보고일 불일치 시 실행을 중단하고 가짜 합계를 출력하지 않습니다.

## 마케팅 캘린더
- 전년 일정 `reports/daily_digest/data/marketing_calendar_2025.json` (CSK Sales Meeting W44~W52), Owned 발송 `reports/daily_digest/data/owned_message_2025.json`을 사용합니다.
- 비교 날짜는 전년 같은 날짜를 계산하지 않고 **Daily Digest `meta.yoy_start`의 전년 동요일 앵커**를 사용합니다. 최근 7일은 해당 앵커의 7일 범위로 연결합니다.
- 확인된 활동과 계획만 기록된 활동을 분리합니다. 수치가 들어 있더라도 메시지 발송 매출을 증분/기여 매출로 더하지 않습니다.
- 올해 일정은 `config/marketing_calendar_2026.json`에 실제 원본을 확인한 항목만 추가하세요. 등록 형식 예시: `{"date":"2026-11-16","kind":"promotion","title":"프로모션명","status":"planned","source":"승인된 일정 파일"}`. 미확인 실제 발송을 confirmed로 기록하지 않습니다.
- 빈 일정은 **미연결**이지 실행된 활동이 0건이라는 의미가 아닙니다.

## 상품 분석
- `reports/pdp_opportunity/data.json`을 검증한 Intelligence 2.0의 상품 후보를 사용합니다. PDP 세션이 높으면서 구매가 0이면 상품 실적 부진보다 `MEASUREMENT_QA`를 우선합니다.
- 주문·재고·가격·마진 원본이 신선하고 상품코드/기간 정합성이 입증되기 전까지 예상 판매 증가액, 광고 집행액 증액, 품절 원인을 추정하지 않습니다.
- `CRO_HYPOTHESIS`는 PDP 구성·유입 채널·재고 영향에 대한 검증 후 개선 후보이지 확정 과제가 아닙니다.

## GitHub 이슈 자동 등록
- P0 원천 복구 5종과 조회수 높은 상품의 측정 QA 최대 2건을 **최대 7개** 자동 등록합니다. 고유 마커 `CSKOPS-ID`로 중복 방지합니다.
- 이슈 소유자는 업무 조직으로만 작성합니다. 실제 GitHub 담당자 ID를 임의로 할당하지 않습니다.
- 사용자 판단으로 종료된 이슈를 자동으로 재오픈하지 않고, 사람이 작성한 코멘트/설명도 덮어쓰지 않습니다.
- 기존 문제는 `reports/marketing_intelligence_v2/state.json`에서 계속 관찰합니다.

## 복구 기준 및 제약
- 광고비 CSV 최종 수집일이 늦으면 원천 매체 API/수집 파일/일별 통화·세금 기준과 채널 매핑부터 점검합니다.
- BigQuery ETL이 stale이면 기존 ETL 실행 모드, Modified Time, 마지막 Run 로그, 원본 행수/기간/회원 범위부터 확인합니다.
- 기존 `ai_agent_full_daily.yml`의 ETL 경로는 `run_etl=false`로 실행될 수 있으며, WRITE_TRUNCATE 옵션은 자동 활성화하지 않습니다. 검증 없는 재실행은 기존 데이터를 손실시킬 수 있습니다.
- MSSQL이 HYT00이면 네트워크·VPN·포트·허용 IP 등 인프라 점검 대상입니다. GitHub Actions에서 DB 쓰기/관리자 권한은 요구하지 않습니다.

## KPI 및 증분 실험
- Weekly는 현재 7일과 이전 7일 Daily KPI가 **각각 모두 7일 완결**된 경우에만 세션·주문·매출·CVR을 집계합니다. 결측일은 0으로 보정하지 않습니다.
- 이미 구축된 실험 실행일의 전후 비교(`marketing_intelligence_experiments.json`)는 관측 수치이며 인과효과가 아닙니다.
- `private_data/csk_signals/incrementality_aggregate.json`에 개인 식별정보를 제외한 무작위 배정 2그룹의 집계 결과를 안전한 비공개 실행 환경에서 주입하면 홀드아웃 구매율 차이·정규근사 구간·1인당 수익 차이를 계산합니다.
- 개인 데이터나 그룹별 세그먼트 원본을 GitHub에 커밋하지 마세요. 그룹별 최소 100명 이상이며 `randomization_verified`와 `assignment_audit_passed`는 실제 검증을 마친 경우에만 true로 설정합니다.
- 샘플 데이터 형태: `{"experiments":[{"id":"CRM-TEST-1","title":"캠페인명","end_date":"2026-11-30","randomization_verified":true,"assignment_audit_passed":true,"treatment":{"eligible":1000,"orders":100,"revenue":12300000},"control":{"eligible":1000,"orders":90,"revenue":11000000}}]}`. **위 값은 스키마 설명을 위한 가상 데이터이며 실제 업무 성과에 사용하지 않습니다.**

## 검증 후 실행 원칙
- 이슈 담당자가 수정한 뒤 기존 Daily/2.0/3.0 소스를 다시 조회하여 일치하면 수동으로 완료 처리합니다.
- 광고 예산, 고객 메시지 발송, ETL 삭제/쓰기, 데이터 파일 덮어쓰기를 자동화하지 않습니다.
- GitHub Pages 게시 완료는 별도 확인합니다. Actions에서 보고서 생성 성공을 확인했다고 외부 웹 공개까지 주장하지 않습니다.


## 3.0 확장: 경쟁사 가격/신상품/광고 소재/크리에이터 크루

**경쟁사 가격**: `reports/competitor_intel/brand_summary.csv`와 `checkpoint_<brand>_status.json`을 연결합니다. 체크포인트 최신일, 완료 여부, 브랜드별 수집 상품 수 일치 여부를 검증합니다. 표시되는 브랜드별 평균가는 **상품 카테고리·시즌·가격대가 다르므로** 동종 SKU의 가격 격차나 가격 대응안을 증명하지 않습니다. 현재는 참고용 브랜드 집계만 공개하고, 개별 상품별 시계열은 미래 별도 승인된 구조화 소스가 필요합니다.

**신상품 추적**: `config/product_launches_2026.json`의 `launches` 배열에 실제 출시일·SKU·상품명을 출처 검증 후 추가합니다. 예: `{"sku":"C74YM3674342","name":"상품명","launch_date":"2026-11-01"}`. 실제 일별 상품 KPI는 비공개 `private_data/csk_signals/sku_daily_aggregate.json`의 rows(date,sku,pdp_sessions,orders,revenue)에서 계산합니다. 7/14/30일 *모든 날짜가 완결*되어야 각 기간 성과를 표시하며 SKU·날짜 중복이나 누락은 차단합니다.

**광고 소재 피로도**: 비공개 `private_data/csk_signals/creative_daily_aggregate.json`에 rows(date,platform,creative_id,impressions,clicks)를 수집합니다. 앞 7일과 뒤 7일 모두 해당 소재·날짜의 연속 집계가 있으며 각 기간 2천회 이상 노출돼야 CTR 전후를 검토합니다. CTR -25% 이상이면 검증 후보로 표시할 뿐, 노출 위치·빈도·타깃·입찰·계절성 확인 전에는 소재 문제나 광고비 변경을 확정하지 않습니다.

**크리에이터 크루**: 비공개 `private_data/csk_signals/creator_crew_aggregate.json`의 rows(mission,format,creator_count,posts,views,engagements,cash_cost_krw,estimated)를 미션·릴스/피드 그룹별로 연결합니다. 최소 3명 이상 그룹만 공개하고, estimated=true는 실제 성과 합계에서 제외합니다. `engagements_per_view_pct`는 조회수 기준 참여/조회 비율로, Reach 기반 참여율과 혼동하지 않습니다. `cash_only_cpv_krw`는 현금성 활동비만 계산하므로 제품 제공·운영 인건비를 포함한 전체 CPV가 아닙니다. **개별 크루 이름, 계정, 연락처, URL, 팔로워 개인정보를 공개하지 마세요.**

네 개 확장 기능은 관련 원천이 없을 때 숫자를 생성하지 않고 `not_connected`, `launch_registry_missing` 등으로 표시합니다.
