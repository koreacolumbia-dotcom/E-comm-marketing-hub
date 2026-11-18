"""Read-only GA4 query. Return aggregate paths; never export identifiers."""
from datetime import date,timedelta
import json

def build_path_sql(parts,start,end,lookback=30):
    from daily_digest_session_source import build_sql
    if not isinstance(start,date) or not isinstance(end,date) or start>end or (end-start).days>31:
        raise ValueError("Invalid purchase interval")
    if not isinstance(lookback,int) or not 1<=lookback<=90:
        raise ValueError("Invalid lookback")
    prefix=build_sql(parts,start.isoformat(),end.isoformat()).split("session_keys AS (",1)[0]
    return prefix+f"""
orders_dedup AS (
 SELECT session_key, user_pseudo_id, event_timestamp, IFNULL(purchase_revenue,0) AS revenue,
   COALESCE(
     IF(LOWER(IFNULL(transaction_id,'')) IN ('','(not set)','(not provided)'),NULL,CONCAT('id:',transaction_id)),
     CONCAT('event:',IFNULL(user_pseudo_id,''),':',IFNULL(CAST(sid AS STRING),''),':',CAST(event_timestamp AS STRING),':',IFNULL(seq,''))) AS order_key
 FROM keyed
 WHERE event_name='purchase' AND event_day BETWEEN DATE('{start}') AND DATE('{end}')
 QUALIFY ROW_NUMBER() OVER(PARTITION BY order_key
   ORDER BY IF(session_key IS NULL,1,0),event_timestamp,IFNULL(session_key,''),IFNULL(purchase_revenue,0) DESC)=1
),
sessions AS (
 SELECT session_key,user_pseudo_id,MIN(sid)*1000000 AS started
 FROM keyed WHERE session_key IS NOT NULL
 GROUP BY 1,2
),
touches AS (
 SELECT s.session_key,s.user_pseudo_id,s.started,
   CONCAT(IFNULL(a.source,'(not set)'), ' / ',IFNULL(a.medium,'(not set)')) AS source_medium,
   IFNULL(a.campaign,'(not set)') AS campaign,IFNULL(a.default_group,'') AS default_group
 FROM sessions s LEFT JOIN attribution a USING(session_key)
),
order_paths AS (
 SELECT p.order_key,p.revenue,TO_JSON_STRING(ARRAY_AGG(
   STRUCT(t.source_medium,t.campaign,t.default_group) ORDER BY t.started,t.session_key)) AS path_json
 FROM orders_dedup p JOIN touches t
 ON t.user_pseudo_id=p.user_pseudo_id
 AND t.started BETWEEN p.event_timestamp-{lookback}*86400000000 AND p.event_timestamp
 WHERE p.session_key IS NOT NULL
 GROUP BY p.order_key,p.revenue,p.session_key
 HAVING COUNTIF(t.session_key=p.session_key)>0
),
agg AS (SELECT path_json,COUNT(*) AS orders,SUM(revenue) AS revenue FROM order_paths GROUP BY 1),
totals AS (SELECT COUNT(*) AS expected_orders,IFNULL(SUM(revenue),0) AS expected_revenue FROM orders_dedup)
SELECT path_json,orders,revenue,expected_orders,expected_revenue
FROM totals LEFT JOIN agg ON TRUE
"""

def load(wildcard,location,start,end,classify,lookback=30,maximum_gib=15):
    from google.cloud import bigquery
    from daily_digest_session_source import select_partitions
    from multi_touch import summarize
    if not isinstance(maximum_gib,int) or not 1<=maximum_gib<=15:
        raise ValueError("Query limit must be 1–15 GiB")
    ceiling=maximum_gib*1024**3
    client=bigquery.Client(project=wildcard.split(".")[0],location=location or None)
    parts=select_partitions(client,wildcard,start-timedelta(days=lookback),end)
    # Strictly reject intraday or incomplete lookback; no silent coverage inflation.
    if any(kind!="daily" for _,kind,_ in parts):
        raise RuntimeError("Complete daily export partitions required")
    sql=build_path_sql(parts,start,end,lookback)
    # A multi-statement dry run may return incomplete estimates. Never execute
    # a query if its measured estimate is missing or zero.
    dry=client.query(sql,location=location or None,
                     job_config=bigquery.QueryJobConfig(dry_run=True,use_query_cache=False))
    estimate=dry.total_bytes_processed
    if estimate is None or estimate<=0:
        raise RuntimeError("No usable dry-run estimate; execution skipped")
    if estimate>ceiling:
        raise RuntimeError("Dry-run estimate exceeds configured limit; execution skipped")
    job=client.query(sql,location=location or None,
                     job_config=bigquery.QueryJobConfig(maximum_bytes_billed=ceiling,use_query_cache=True))
    output=[]
    totals=None
    for row in job.result():
        totals=(int(row["expected_orders"]),float(row["expected_revenue"]))
        if row["path_json"] is None: continue
        # Reuse the caller's existing channel classifier; source/campaign tuples stay in memory.
        path=[classify(t["source_medium"],t["campaign"],t["default_group"]) for t in json.loads(row["path_json"])]
        output.append({"path":path,"orders":int(row["orders"]),"revenue":float(row["revenue"])})
    if totals is None: raise RuntimeError("No query totals")
    result=summarize(output,*totals,start.isoformat(),end.isoformat(),lookback)
    result["partition_status"]="daily_complete"
    result["cost"]={"estimated_bytes":int(estimate),"billed_bytes":job.total_bytes_billed,"maximum_bytes_billed":ceiling,"cache_hit":job.cache_hit}
    return result
