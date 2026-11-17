"""Daily Digest channel source: one GA4 export session key, one attribution.
No first-user attribution, GA API proportional allocation, or fabricated direct.
Daily tables take precedence over intraday for each calendar partition.
"""
import datetime as dt
import re
import pandas as pd

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

METRICS = ["sessions", "transactions", "purchaseRevenue"]
DIMENSIONS = ["sessionSourceMedium", "sessionCampaignName", "sessionDefaultChannelGroup"]


def select_partitions(client, wildcard, start, end):
    if not re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_]+\.events_\*", wildcard):
        raise ValueError("Expected project.dataset.events_*")
    prefix = wildcard[:-1]
    # Prior day supplies attribution for sessions spanning midnight.
    day = start - dt.timedelta(days=1)
    parts = []
    while day <= end:
        suffix = day.strftime("%Y%m%d")
        try:
            table = client.get_table(prefix + suffix)
            kind = "daily"
        except NotFound:
            table = client.get_table(prefix + "intraday_" + suffix)
            kind = "intraday"
        parts.append((day, kind, table))
        day += dt.timedelta(days=1)
    return parts


def build_sql(parts, start, end):
    tick = chr(96)
    selects = []
    for day, kind, table in parts:
        columns = {f.name for f in table.schema}
        st = "TO_JSON_STRING(session_traffic_source_last_click)" if "session_traffic_source_last_click" in columns else "CAST(NULL AS STRING)"
        ct = "TO_JSON_STRING(collected_traffic_source)" if "collected_traffic_source" in columns else "CAST(NULL AS STRING)"
        seq = "CAST(event_bundle_sequence_id AS STRING)" if "event_bundle_sequence_id" in columns else "CAST(NULL AS STRING)"
        identifier = f"{table.project}.{table.dataset_id}.{table.table_id}"
        selects.append(f"""
        SELECT PARSE_DATE('%Y%m%d', event_date) AS event_day,
          event_timestamp, event_name, user_pseudo_id,
          (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id' LIMIT 1) AS sid,
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location' LIMIT 1) AS page_url,
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key='source' LIMIT 1) AS param_source,
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key='medium' LIMIT 1) AS param_medium,
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key='campaign' LIMIT 1) AS param_campaign,
          {st} AS st, {ct} AS ct, {seq} AS seq,
          COALESCE(NULLIF(CAST(ecommerce.transaction_id AS STRING), ''),
            (SELECT NULLIF(value.string_value,'') FROM UNNEST(event_params) WHERE key='transaction_id' LIMIT 1)) AS transaction_id,
          SAFE_CAST(ecommerce.purchase_revenue AS FLOAT64) AS purchase_revenue
        FROM {tick}{identifier}{tick}""")
    union_sql = "\nUNION ALL\n".join(selects)
    return f"""
CREATE TEMP FUNCTION clean(s STRING) AS (
  IF(LOWER(TRIM(IFNULL(s,''))) IN ('','(not set)','(not provided)','(other)'), NULL, TRIM(s))
);
CREATE TEMP FUNCTION url_decode(s STRING) RETURNS STRING LANGUAGE js AS r'''
  if (s === null) return null;
  try {{ return decodeURIComponent(s.replace(/\\+/g, ' ')); }} catch(e) {{ return s; }}
''';
WITH raw AS ({union_sql}),
keyed AS (
  SELECT *,
    IF(user_pseudo_id IS NOT NULL AND user_pseudo_id != '' AND sid BETWEEN 946684800 AND 4102444800,
       TO_JSON_STRING(STRUCT(user_pseudo_id, sid)), NULL) AS session_key,
    IF(sid BETWEEN 946684800 AND 4102444800,
       DATE(TIMESTAMP_SECONDS(sid), 'Asia/Seoul'), NULL) AS session_day
  FROM raw
),
-- Keep the acquisition fields together from a single candidate/event.
-- Prefer GA4 session-level attribution; use collected/URL UTM only when missing.
candidates AS (
  SELECT session_key, event_timestamp, event_name, c.*,
    clean(JSON_VALUE(st,'$.cross_channel_campaign.default_channel_group')) AS default_group
  FROM keyed
  CROSS JOIN UNNEST([
    STRUCT(1 AS priority,
      clean(JSON_VALUE(st,'$.manual_campaign.source')) AS source,
      clean(JSON_VALUE(st,'$.manual_campaign.medium')) AS medium,
      clean(JSON_VALUE(st,'$.manual_campaign.campaign_name')) AS campaign),
    STRUCT(2 AS priority,
      clean(JSON_VALUE(st,'$.cross_channel_campaign.source')) AS source,
      clean(JSON_VALUE(st,'$.cross_channel_campaign.medium')) AS medium,
      clean(JSON_VALUE(st,'$.cross_channel_campaign.campaign_name')) AS campaign),
    STRUCT(3 AS priority,
      IF(clean(JSON_VALUE(st,'$.google_ads_campaign.campaign_name')) IS NOT NULL,'google',NULL) AS source,
      IF(clean(JSON_VALUE(st,'$.google_ads_campaign.campaign_name')) IS NOT NULL,'cpc',NULL) AS medium,
      clean(JSON_VALUE(st,'$.google_ads_campaign.campaign_name')) AS campaign),
    STRUCT(4 AS priority,
      clean(JSON_VALUE(ct,'$.manual_source')) AS source,
      clean(JSON_VALUE(ct,'$.manual_medium')) AS medium,
      clean(JSON_VALUE(ct,'$.manual_campaign_name')) AS campaign),
    STRUCT(5 AS priority,
      clean(url_decode(REGEXP_EXTRACT(page_url,r'(?i)[?&]utm_source=([^&#]+)'))) AS source,
      clean(url_decode(REGEXP_EXTRACT(page_url,r'(?i)[?&]utm_medium=([^&#]+)'))) AS medium,
      clean(url_decode(REGEXP_EXTRACT(page_url,r'(?i)[?&]utm_campaign=([^&#]+)'))) AS campaign),
    STRUCT(6 AS priority, clean(param_source) AS source, clean(param_medium) AS medium, clean(param_campaign) AS campaign),
    STRUCT(7 AS priority,
      IF(clean(JSON_VALUE(ct,'$.gclid')) IS NOT NULL OR REGEXP_CONTAINS(IFNULL(page_url,''),r'(?i)[?&]gclid='),'google',NULL) AS source,
      IF(clean(JSON_VALUE(ct,'$.gclid')) IS NOT NULL OR REGEXP_CONTAINS(IFNULL(page_url,''),r'(?i)[?&]gclid='),'cpc',NULL) AS medium,
      CAST(NULL AS STRING) AS campaign)
  ]) AS c
  WHERE session_key IS NOT NULL
),
attribution AS (
  SELECT session_key, source, medium, campaign, default_group
  FROM candidates
  WHERE source IS NOT NULL OR medium IS NOT NULL OR campaign IS NOT NULL OR default_group IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY session_key ORDER BY
      CASE WHEN source IS NULL AND medium IS NULL AND campaign IS NULL THEN 100
           WHEN source IS NULL OR medium IS NULL THEN 50 + priority
           ELSE priority END,
      IF(event_name='session_start',0,1), event_timestamp,
      IFNULL(source,''), IFNULL(medium,''), IFNULL(campaign,''), IFNULL(default_group,'')
  ) = 1
),
session_keys AS (
  SELECT session_key, MIN(session_day) AS session_day
  FROM keyed WHERE session_key IS NOT NULL GROUP BY session_key
),
purchases AS (
  SELECT session_key, purchase_revenue
  FROM keyed
  WHERE event_name='purchase' AND event_day BETWEEN DATE('{start}') AND DATE('{end}')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COALESCE(
      IF(LOWER(IFNULL(transaction_id,'')) IN ('','(not set)','(not provided)'),NULL,CONCAT('id:',transaction_id)),
      CONCAT('event:',IFNULL(user_pseudo_id,''),':',IFNULL(CAST(sid AS STRING),''),':',CAST(event_timestamp AS STRING),':',IFNULL(seq,'')))
    ORDER BY IF(session_key IS NULL,1,0), event_timestamp, IFNULL(session_key,''), IFNULL(purchase_revenue,0) DESC
  )=1
),
purchase_metrics AS (
  SELECT session_key, COUNT(*) AS transactions, SUM(IFNULL(purchase_revenue,0)) AS purchaseRevenue
  FROM purchases GROUP BY session_key
),
session_metrics AS (
  SELECT
    CONCAT(IFNULL(a.source,'(not set)'), ' / ', IFNULL(a.medium,'(not set)')) AS sessionSourceMedium,
    IFNULL(a.campaign,'(not set)') AS sessionCampaignName,
    IFNULL(a.default_group,'') AS sessionDefaultChannelGroup,
    IF(s.session_day BETWEEN DATE('{start}') AND DATE('{end}'),1,0) AS sessions,
    IFNULL(p.transactions,0) AS transactions, IFNULL(p.purchaseRevenue,0) AS purchaseRevenue
  FROM session_keys s LEFT JOIN attribution a USING(session_key)
  LEFT JOIN purchase_metrics p USING(session_key)
  WHERE s.session_day BETWEEN DATE('{start}') AND DATE('{end}') OR p.transactions > 0
  UNION ALL
  SELECT '(not set) / (not set)', '(not set)', '', 0, transactions, purchaseRevenue
  FROM purchase_metrics WHERE session_key IS NULL
),
independent AS (
  SELECT COUNTIF(session_day BETWEEN DATE('{start}') AND DATE('{end}')) AS expected_sessions
  FROM session_keys
)
SELECT sessionSourceMedium, sessionCampaignName, sessionDefaultChannelGroup,
  SUM(sessions) AS sessions, SUM(transactions) AS transactions, SUM(purchaseRevenue) AS purchaseRevenue,
  (SELECT expected_sessions FROM independent) AS expected_sessions
FROM session_metrics
GROUP BY 1,2,3
"""


def load_session_frame(wildcard, location, start, end):
    project = wildcard.split(".")[0]
    client = bigquery.Client(project=project, location=location or None)
    parts = select_partitions(client, wildcard, start, end)
    sql = build_sql(parts, start.isoformat(), end.isoformat())
    df = client.query(sql, location=location or None).result().to_dataframe(create_bqstorage_client=False)
    if df.empty:
        raise RuntimeError(f"Missing session data: {start}..{end}")
    for col in METRICS:
        df[col] = pd.to_numeric(df[col], errors="raise").fillna(0)
    expected = int(df["expected_sessions"].iloc[0])
    actual = int(df["sessions"].sum())
    if expected <= 0 or expected != actual:
        raise RuntimeError(f"Session key reconciliation failed: {actual} vs {expected}")
    df = df.drop(columns=["expected_sessions"])
    status = {d.isoformat(): k for d,k,_ in parts}
    df.attrs["partitions"] = status
    print(f"[SESSION_BQ] {start}..{end}: unique={actual:,}, transactions={df['transactions'].sum():,.0f}, partitions={status}", flush=True)
    return df
