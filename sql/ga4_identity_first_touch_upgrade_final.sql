-- GA4 user_id matching 강화 + first_touch 정확도 개선
-- CRM 실제 컬럼 반영:
-- member_id   = MemberID
-- signup_date = MemberRegdate
-- CRM에 user_id 컬럼 없음 → GA4 user_id를 identity spine에서 복원

DECLARE start_date STRING DEFAULT '20250101';
DECLARE end_date   STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY));

WITH ga_base AS (
  SELECT
    event_date,
    TIMESTAMP_MICROS(event_timestamp) AS event_ts,
    user_pseudo_id,
    NULLIF(TRIM(user_id), '') AS ga_user_id,
    event_name,
    COALESCE(
      session_traffic_source_last_click.manual_campaign.source,
      collected_traffic_source.manual_source,
      traffic_source.source
    ) AS session_source,
    COALESCE(
      session_traffic_source_last_click.manual_campaign.medium,
      collected_traffic_source.manual_medium,
      traffic_source.medium
    ) AS session_medium,
    COALESCE(
      session_traffic_source_last_click.manual_campaign.campaign_name,
      collected_traffic_source.manual_campaign_name
    ) AS session_campaign,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id
  FROM `columbia-ga4.analytics_358593394.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),

identity_events AS (
  SELECT
    user_pseudo_id,
    ga_user_id,
    event_ts
  FROM ga_base
  WHERE ga_user_id IS NOT NULL
),

pseudo_to_user AS (
  SELECT
    user_pseudo_id,
    ARRAY_AGG(ga_user_id ORDER BY event_ts ASC LIMIT 1)[OFFSET(0)] AS stitched_user_id,
    MIN(event_ts) AS first_identified_ts
  FROM identity_events
  GROUP BY 1
),

crm_members AS (
  SELECT
    CAST(MemberID AS STRING) AS member_id,
    TIMESTAMP(MemberRegdate) AS signup_ts
  FROM `columbia-ga4.crm_raw.tb_member_staging`
  WHERE MemberID IS NOT NULL
),

identity_spine AS (
  SELECT
    m.member_id,
    p.stitched_user_id AS resolved_user_id,
    p.user_pseudo_id,
    m.signup_ts,
    p.first_identified_ts
  FROM crm_members m
  LEFT JOIN pseudo_to_user p
    ON FALSE
),

ga_sessions AS (
  SELECT
    b.ga_user_id AS resolved_user_id,
    b.user_pseudo_id,
    b.ga_session_id,
    MIN(b.event_ts) AS session_start_ts,
    ARRAY_AGG(
      STRUCT(
        b.session_source AS source,
        b.session_medium AS medium,
        b.session_campaign AS campaign,
        b.event_ts AS ts
      )
      ORDER BY b.event_ts ASC
      LIMIT 1
    )[OFFSET(0)] AS first_touch_in_session
  FROM ga_base b
  GROUP BY 1,2,3
),

-- 실제 운영 시 여기 매칭 룰을 네 CRM/GA4 연결 규칙으로 교체
-- 예: 주문/로그인/회원가입 이벤트에서 MemberID custom param 추출 후 join
member_ga_bridge AS (
  SELECT DISTINCT
    CAST(MemberID AS STRING) AS member_id,
    NULLIF(TRIM(user_id), '') AS resolved_user_id
  FROM `columbia-ga4.analytics_358593394.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
    AND MemberID IS NOT NULL
    AND NULLIF(TRIM(user_id), '') IS NOT NULL
),

member_sessions AS (
  SELECT
    b.member_id,
    s.resolved_user_id,
    s.user_pseudo_id,
    s.ga_session_id,
    s.session_start_ts,
    s.first_touch_in_session
  FROM member_ga_bridge b
  JOIN ga_sessions s
    ON b.resolved_user_id = s.resolved_user_id
),

member_first_touch AS (
  SELECT
    member_id,
    ARRAY_AGG(
      STRUCT(
        first_touch_in_session.source AS source,
        first_touch_in_session.medium AS medium,
        first_touch_in_session.campaign AS campaign,
        session_start_ts AS ts
      )
      ORDER BY
        CASE
          WHEN LOWER(COALESCE(first_touch_in_session.source, '')) IN ('(direct)', 'direct', '') THEN 1
          ELSE 0
        END ASC,
        session_start_ts ASC
      LIMIT 1
    )[OFFSET(0)] AS first_touch
  FROM member_sessions
  GROUP BY 1
),

member_latest_touch AS (
  SELECT
    member_id,
    ARRAY_AGG(
      STRUCT(
        first_touch_in_session.source AS source,
        first_touch_in_session.medium AS medium,
        first_touch_in_session.campaign AS campaign,
        session_start_ts AS ts
      )
      ORDER BY session_start_ts DESC
      LIMIT 1
    )[OFFSET(0)] AS latest_touch
  FROM member_sessions
  GROUP BY 1
)

SELECT
  m.member_id,
  b.resolved_user_id AS user_id,
  ft.first_touch.source AS first_source_rebuilt,
  ft.first_touch.medium AS first_medium_rebuilt,
  ft.first_touch.campaign AS first_campaign_rebuilt,
  ft.first_touch.ts AS first_touch_ts,
  lt.latest_touch.source AS latest_source_rebuilt,
  lt.latest_touch.medium AS latest_medium_rebuilt,
  lt.latest_touch.campaign AS latest_campaign_rebuilt,
  lt.latest_touch.ts AS latest_touch_ts,
  m.signup_ts
FROM crm_members m
LEFT JOIN member_ga_bridge b
  ON m.member_id = b.member_id
LEFT JOIN member_first_touch ft
  ON m.member_id = ft.member_id
LEFT JOIN member_latest_touch lt
  ON m.member_id = lt.member_id;
