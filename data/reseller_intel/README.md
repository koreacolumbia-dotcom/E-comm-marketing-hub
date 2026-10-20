# 컬럼비아 리셀러 모니터링

## 현 상태 (2026-10-20)

**네이버 쇼핑 검색 OpenAPI는 2026-07-31에 종료되었습니다.** 기존 저장소의 네이버 쇼핑 최저가 수집 프로그램은 더 이상 최신 데이터를 정상 수집할 수 없습니다. 과거 캐시와 기존 HTML의 가격은 현재가가 아닙니다.

네이버 공지:
- https://developers.naver.com/notice/article/32564
- https://developers.naver.com/notice/article/32530 (쇼핑 검색 API 대체 제공 안 함)

## 수집 경로

이 디렉터리의 CSV는 **승인된 데이터 공급업체, 정당하게 확보한 검색 결과 내보내기 또는 사람이 공개 판매자 페이지를 확인해 작성한 자료**를 받습니다. 종료된 API 호출이나 네이버 로그인/자동화 차단 우회를 하지 않습니다.

1. listings.csv: 네이버 검색 결과나 판매 페이지에서 관측한 상품 단위 내역. 상품코드/판매자명/채널/상품명/판매가/URL/관측일을 기록합니다. 타 브랜드 취급 확인은 같은 판매자의 다른 브랜드 상품 행을 추가하여 근거로 남깁니다.
2. sellers.csv: 실제 판매자명/사업자명/사업자등록번호/사업자주소/반품지주소/확인 URL/검증일. 네이버쇼핑 가격비교의 mallName=네이버는 개별 판매자가 아니므로 판매자 식별 완료로 기록하지 않습니다.
3. 원문 출처 URL, 조회일, 검증 상태가 없는 주소는 리포트에서 미확인 처리합니다. 개인 주거지/비공개 개인정보를 조사해 넣지 마세요.
4. 상품 중심도는 확인한 상품코드 빈도에 따른 노출 분포입니다. 판매량이나 판매 매출 규모가 아닙니다.

## 필요한 열

- listings.csv: observed_at (KST ISO 8601), seller_name, seller_id, channel, store_url, brand, product_code, product_name, category, price_krw, list_price_krw, shipping_fee_krw, product_url, source_url, source_type, verification_status
- sellers.csv: seller_name, seller_id, channel, business_name, business_registration_number, business_address, business_info_url, return_address, return_info_url, store_url, verified_at, verification_status, memo

source_type 예: manual_public_store, licensed_export, seller_profile_public.
verification_status 예: verified_public, unverified, needs_manual_review.
- 같은 판매자는 가능하면 seller_id를 채워야 채널 또는 표기 차이와 무관하게 합쳐집니다.
- 브랜드는 확인된 상품 브랜드 그대로 입력합니다. 타 브랜드 취급 여부를 추측하지 마세요.
- 가격은 상품판매가(원)이며 배송비/쿠폰가와 분리해 기록합니다.
- 가격비교 페이지의 최저가는 실제 개별 판매자 가격/판매자명으로 확정되지 않습니다. 실제 판매 판매자 단위의 링크를 기록하세요.

## 재생성

python scripts/build_reseller_intel.py --listings data/reseller_intel/listings.csv --sellers data/reseller_intel/sellers.csv --out reports/reseller_intel --max-age-days 3

결과:
- reports/reseller_intel/index.html : 판매자별 대시보드
- reports/reseller_intel/seller_master.csv : 판매자/주소/컬럼비아 집중 품목/타 브랜드 요약
- reports/reseller_intel/listings_normalized.csv : 상품명/상품코드/가격/채널/링크/수집 시점
- reports/reseller_intel/meta.json : 최신 데이터 존재 여부

**주의**: 빈 CSV로 실행하면 상태가 REQUIRES_CURRENT_SOURCE로 표시됩니다. 이는 정상적인 경고이며 실제 판매자 조사를 완료했다는 의미가 아닙니다.
