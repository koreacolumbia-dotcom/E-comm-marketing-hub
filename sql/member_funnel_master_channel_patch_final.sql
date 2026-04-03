-- member_funnel_master 채널 보강 최종 SQL (enhanced 컬럼 우선 사용, 중간 컬럼명 충돌 방지)
-- 프로젝트명: columbia-ga4
-- workflow에서 source_table / target_table 문자열 치환

DECLARE source_table STRING DEFAULT 'columbia-ga4.crm_mart.member_funnel_master_staging';
DECLARE target_table STRING DEFAULT 'columbia-ga4.crm_mart.member_funnel_master';

EXECUTE IMMEDIATE FORMAT('''
CREATE OR REPLACE TABLE `%s` AS
WITH base AS (
  SELECT
    t.*,
    NULLIF(TRIM(CAST(COALESCE(t.first_source_enhanced, t.first_source) AS STRING)), '') AS __mf_first_source_raw,
    NULLIF(TRIM(CAST(COALESCE(t.first_medium_enhanced, t.first_medium) AS STRING)), '') AS __mf_first_medium_raw,
    NULLIF(TRIM(CAST(COALESCE(t.first_campaign_enhanced, t.first_campaign) AS STRING)), '') AS __mf_first_campaign_raw,
    NULLIF(TRIM(CAST(t.latest_source AS STRING)), '') AS __mf_latest_source_raw,
    NULLIF(TRIM(CAST(t.latest_medium AS STRING)), '') AS __mf_latest_medium_raw,
    NULLIF(TRIM(CAST(t.latest_campaign AS STRING)), '') AS __mf_latest_campaign_raw,
    NULLIF(TRIM(CAST(COALESCE(t.channel_group_enhanced, t.channel_group) AS STRING)), '') AS __mf_existing_channel_group_raw
  FROM `%s` t
),
channel_seed AS (
  SELECT
    *,
    COALESCE(__mf_first_source_raw, __mf_latest_source_raw) AS __mf_source_fallback,
    COALESCE(__mf_first_medium_raw, __mf_latest_medium_raw) AS __mf_medium_fallback,
    COALESCE(__mf_first_campaign_raw, __mf_latest_campaign_raw) AS __mf_campaign_fallback,
    CASE WHEN __mf_first_source_raw IS NULL AND __mf_latest_source_raw IS NOT NULL THEN 1 ELSE 0 END AS __mf_used_latest_source_fallback,
    CASE
      WHEN __mf_first_source_raw IS NULL
       AND __mf_latest_source_raw IS NULL
       AND __mf_first_medium_raw IS NULL
       AND __mf_latest_medium_raw IS NULL
      THEN 1 ELSE 0
    END AS __mf_channel_unknown_flag
  FROM base
),
normalized AS (
  SELECT
    *,
    COALESCE(__mf_source_fallback, '(not set)') AS __mf_channel_source,
    COALESCE(__mf_medium_fallback, '(not set)') AS __mf_channel_medium,
    COALESCE(__mf_campaign_fallback, '(not set)') AS __mf_channel_campaign,
    LOWER(COALESCE(__mf_source_fallback, '')) AS __mf_src_l,
    LOWER(COALESCE(__mf_medium_fallback, '')) AS __mf_med_l,
    LOWER(COALESCE(__mf_campaign_fallback, '')) AS __mf_camp_l,
    LOWER(CONCAT(COALESCE(__mf_source_fallback, '(not set)'), ' / ', COALESCE(__mf_medium_fallback, '(not set)'))) AS __mf_source_medium_l,
    LOWER(COALESCE(__mf_existing_channel_group_raw, '')) AS __mf_existing_channel_group_l
  FROM channel_seed
),
classified AS (
  SELECT
    *,
    CASE
      WHEN __mf_existing_channel_group_l IN (
        'awareness', 'paid ad', 'organic traffic', 'official sns', 'owned channel', 'direct'
      ) THEN __mf_existing_channel_group_raw
      WHEN
        (__mf_src_l = '' OR __mf_src_l = '(not set)' OR __mf_src_l = 'not set')
        AND (__mf_med_l = '' OR __mf_med_l = '(not set)' OR __mf_med_l = 'not set')
        AND (__mf_camp_l = '' OR __mf_camp_l = '(not set)' OR __mf_camp_l = 'not set')
      THEN 'Unknown'
      WHEN
        REGEXP_CONTAINS(__mf_source_medium_l, r'[(]direct[)] */ *[(]none[)]')
        OR (__mf_src_l IN ('direct', '(direct)') AND __mf_med_l IN ('(none)', 'none', 'direct', ''))
      THEN 'Direct'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'instagram') AND REGEXP_CONTAINS(__mf_source_medium_l, r'story') THEN 'Official SNS'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'igshopping') THEN 'Official SNS'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'instagram') AND REGEXP_CONTAINS(__mf_source_medium_l, r'referral') THEN 'Official SNS'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'lms') OR REGEXP_CONTAINS(__mf_camp_l, r'lms') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'email|edm') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'kakao_fridnstalk') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'kakao_ch') OR REGEXP_CONTAINS(__mf_camp_l, r'kakao_ch') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'kakao_alimtalk') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'kakao_coupon') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'kakao_chatbot') THEN 'Owned Channel'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'mkt|_bd') OR REGEXP_CONTAINS(__mf_camp_l, r'mkt|[[]bd') THEN 'Awareness'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'google */ *cpc') AND REGEXP_CONTAINS(__mf_camp_l, r'디멘드젠|디멘드잰|디맨드젠|디맨드잰|dg|demandgen') THEN 'Awareness'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'google */ *cpc') AND REGEXP_CONTAINS(__mf_camp_l, r'유튜브|yt|youtube|instream|vac|vvc') THEN 'Awareness'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'google */ *cpc') AND REGEXP_CONTAINS(__mf_camp_l, r'discovery') THEN 'Awareness'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'nap') AND REGEXP_CONTAINS(__mf_source_medium_l, r'da') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'toss') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'blind') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'kakaobs') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'meta|facebook|instagram|ig|fb') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'google */ *cpc') AND REGEXP_CONTAINS(__mf_camp_l, r'pmax') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'google */ *cpc') AND REGEXP_CONTAINS(__mf_camp_l, r'sa|ss|검색') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'google */ *cpc') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'naver') AND REGEXP_CONTAINS(__mf_source_medium_l, r'da') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'gfa') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'naverbs') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'naver') AND REGEXP_CONTAINS(__mf_source_medium_l, r'cpc') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'shopping_ad') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'kakao') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'signalplay|signal play|signal_play|sg_|signal|manplus') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'buzzvill|criteo|mobon|snow|smr|tg|t_cafe') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'cpc') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'banner|da') THEN 'Paid Ad'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'benz') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'inhouse') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'facebook') AND REGEXP_CONTAINS(__mf_source_medium_l, r'referral') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'google */ *organic') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'google') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'youtube') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'naver') AND REGEXP_CONTAINS(__mf_source_medium_l, r'shopping') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'naver') AND REGEXP_CONTAINS(__mf_source_medium_l, r'organic') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'naver') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'daum */ *organic') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'daum') AND REGEXP_CONTAINS(__mf_source_medium_l, r'referral') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'organic') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'referral') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'shopping') THEN 'Organic Traffic'
      WHEN REGEXP_CONTAINS(__mf_source_medium_l, r'social') THEN 'Organic Traffic'
      ELSE 'Unknown'
    END AS __mf_final_channel_group
  FROM normalized
)
SELECT
  * EXCEPT(
    channel_group,
    channel_group_enhanced,
    first_source_enhanced,
    first_medium_enhanced,
    first_campaign_enhanced,
    __mf_first_source_raw,
    __mf_first_medium_raw,
    __mf_first_campaign_raw,
    __mf_latest_source_raw,
    __mf_latest_medium_raw,
    __mf_latest_campaign_raw,
    __mf_existing_channel_group_raw,
    __mf_source_fallback,
    __mf_medium_fallback,
    __mf_campaign_fallback,
    __mf_used_latest_source_fallback,
    __mf_channel_unknown_flag,
    __mf_channel_source,
    __mf_channel_medium,
    __mf_channel_campaign,
    __mf_src_l,
    __mf_med_l,
    __mf_camp_l,
    __mf_source_medium_l,
    __mf_existing_channel_group_l,
    __mf_final_channel_group
  ),
  __mf_final_channel_group AS channel_group,
  __mf_final_channel_group AS channel_group_enhanced,
  __mf_channel_source AS first_source_enhanced,
  __mf_channel_medium AS first_medium_enhanced,
  __mf_channel_campaign AS first_campaign_enhanced
FROM classified
''', target_table, source_table);
