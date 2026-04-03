-- member_funnel_master 채널 보강 최종 SQL
-- 프로젝트명: columbia-ga4
-- workflow에서 source_table / target_table 문자열 치환

DECLARE source_table STRING DEFAULT 'columbia-ga4.crm_mart.member_funnel_master_staging';
DECLARE target_table STRING DEFAULT 'columbia-ga4.crm_mart.member_funnel_master';

EXECUTE IMMEDIATE FORMAT('''
CREATE OR REPLACE TABLE `%s` AS
WITH base AS (
  SELECT
    t.*,
    NULLIF(TRIM(CAST(first_source AS STRING)), '') AS first_source_raw,
    NULLIF(TRIM(CAST(first_medium AS STRING)), '') AS first_medium_raw,
    NULLIF(TRIM(CAST(first_campaign AS STRING)), '') AS first_campaign_raw,
    NULLIF(TRIM(CAST(latest_source AS STRING)), '') AS latest_source_raw,
    NULLIF(TRIM(CAST(latest_medium AS STRING)), '') AS latest_medium_raw,
    NULLIF(TRIM(CAST(latest_campaign AS STRING)), '') AS latest_campaign_raw
  FROM `%s` t
),
channel_seed AS (
  SELECT
    *,
    COALESCE(first_source_raw, latest_source_raw) AS source_fallback,
    COALESCE(first_medium_raw, latest_medium_raw) AS medium_fallback,
    COALESCE(first_campaign_raw, latest_campaign_raw) AS campaign_fallback,
    CASE WHEN first_source_raw IS NULL AND latest_source_raw IS NOT NULL THEN 1 ELSE 0 END AS used_latest_source_fallback,
    CASE
      WHEN first_source_raw IS NULL
       AND latest_source_raw IS NULL
       AND first_medium_raw IS NULL
       AND latest_medium_raw IS NULL
      THEN 1 ELSE 0
    END AS channel_unknown_flag
  FROM base
),
normalized AS (
  SELECT
    *,
    COALESCE(source_fallback, '(not set)') AS channel_source,
    COALESCE(medium_fallback, '(not set)') AS channel_medium,
    COALESCE(campaign_fallback, '(not set)') AS channel_campaign,
    LOWER(COALESCE(source_fallback, '')) AS src_l,
    LOWER(COALESCE(medium_fallback, '')) AS med_l,
    LOWER(COALESCE(campaign_fallback, '')) AS camp_l,
    LOWER(CONCAT(COALESCE(source_fallback, '(not set)'), ' / ', COALESCE(medium_fallback, '(not set)'))) AS source_medium_l
  FROM channel_seed
),
classified AS (
  SELECT
    *,
    CASE
      WHEN
        (src_l = '' OR src_l = '(not set)' OR src_l = 'not set')
        AND (med_l = '' OR med_l = '(not set)' OR med_l = 'not set')
        AND (camp_l = '' OR camp_l = '(not set)' OR camp_l = 'not set')
      THEN 'Unknown'
      WHEN
        REGEXP_CONTAINS(source_medium_l, r'[(]direct[)]\s*/\s*[(]none[)]')
        OR (src_l IN ('direct', '(direct)') AND med_l IN ('(none)', 'none', 'direct', ''))
      THEN 'Direct'
      WHEN REGEXP_CONTAINS(source_medium_l, r'instagram') AND REGEXP_CONTAINS(source_medium_l, r'story') THEN 'Official SNS'
      WHEN REGEXP_CONTAINS(source_medium_l, r'igshopping') THEN 'Official SNS'
      WHEN REGEXP_CONTAINS(source_medium_l, r'instagram') AND REGEXP_CONTAINS(source_medium_l, r'referral') THEN 'Official SNS'
      WHEN REGEXP_CONTAINS(source_medium_l, r'lms') OR REGEXP_CONTAINS(camp_l, r'lms') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(source_medium_l, r'email|edm') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(source_medium_l, r'kakao_fridnstalk') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(source_medium_l, r'kakao_ch') OR REGEXP_CONTAINS(camp_l, r'kakao_ch') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(source_medium_l, r'kakao_alimtalk') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(source_medium_l, r'kakao_coupon') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(source_medium_l, r'kakao_chatbot') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(source_medium_l, r'mkt|_bd') OR REGEXP_CONTAINS(camp_l, r'mkt|\[bd') THEN 'Awareness'
      WHEN REGEXP_CONTAINS(source_medium_l, r'google\s*/\s*cpc') AND REGEXP_CONTAINS(camp_l, r'디멘드젠|디멘드잰|디맨드젠|디맨드잰|dg|demandgen') THEN 'Awareness'
      WHEN REGEXP_CONTAINS(source_medium_l, r'google\s*/\s*cpc') AND REGEXP_CONTAINS(camp_l, r'유튜브|yt|youtube|instream|vac|vvc') THEN 'Awareness'
      WHEN REGEXP_CONTAINS(source_medium_l, r'google\s*/\s*cpc') AND REGEXP_CONTAINS(camp_l, r'discovery') THEN 'Awareness'
      WHEN REGEXP_CONTAINS(source_medium_l, r'nap') AND REGEXP_CONTAINS(source_medium_l, r'da') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'toss') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'blind') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'kakaobs') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'meta|facebook|instagram|ig|fb') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'google\s*/\s*cpc') AND REGEXP_CONTAINS(camp_l, r'pmax') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'google\s*/\s*cpc') AND REGEXP_CONTAINS(camp_l, r'sa|ss|검색') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'google\s*/\s*cpc') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'naver') AND REGEXP_CONTAINS(source_medium_l, r'da') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'gfa') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'naverbs') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'naver') AND REGEXP_CONTAINS(source_medium_l, r'cpc') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'shopping_ad') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'kakao') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'signalplay|signal play|signal_play|sg_|signal|manplus') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'buzzvill|criteo|mobon|snow|smr|tg|t_cafe') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'cpc') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'banner|da') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(source_medium_l, r'benz') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'inhouse') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'facebook') AND REGEXP_CONTAINS(source_medium_l, r'referral') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'google\s*/\s*organic') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'google') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'youtube') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'naver') AND REGEXP_CONTAINS(source_medium_l, r'shopping') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'naver') AND REGEXP_CONTAINS(source_medium_l, r'organic') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'naver') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'daum\s*/\s*organic') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'daum') AND REGEXP_CONTAINS(source_medium_l, r'referral') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'organic') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'referral') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'shopping') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(source_medium_l, r'social') THEN 'Organic Traffic'
      ELSE 'Unknown'
    END AS channel_group_enhanced
  FROM normalized
)
SELECT
  * EXCEPT(channel_group),
  channel_group_enhanced AS channel_group,
  channel_source AS first_source_enhanced,
  channel_medium AS first_medium_enhanced,
  channel_campaign AS first_campaign_enhanced
FROM classified
''', target_table, source_table);
