#!/usr/bin/env python3
"""GA4 same-session ordered product funnel. Aggregate only, one 2-GiB cost-capped query."""
import base64
import datetime as dt
import json
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports/decision_health/product_funnel_evidence.json"
CAP = 2147483648
TABLE = "columbia-ga4.analytics_358593394.events_*"
LOCATION = "asia-northeast3"
COUNTS = ("view_sessions","cart_sessions","purchase_sessions","cart_after_view_sessions",
          "purchase_after_cart_sessions","view_events","cart_events","purchase_units")


def read(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save(x):
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(x, ensure_ascii=False, indent=2, allow_nan=False)+"\n", encoding="utf-8")


def targets(focus, purchase):
    if not focus.get("date") or focus["date"] != purchase.get("report_date") or purchase.get("status") != "ga4_measured":
        return {}
    valid = {x.get("name"): x for x in purchase.get("product_sales", [])}
    found = {}
    for p in (focus.get("cards") or [])[:8]:
        x = valid.get(p.get("name"))
        if not x or p.get("sales_7d") != x.get("current_units") or p.get("sales_previous_7d") != x.get("previous_units"):
            continue
        codes = sorted({s for s in x.get("codes", []) if isinstance(s,str) and re.fullmatch(r"[A-Za-z0-9_-]{5,40}",s)})
        if codes:
            found[p["name"]] = codes
    return found


def query_for(end, by_style):
    start = end-dt.timedelta(days=13)
    split = end-dt.timedelta(days=6)
    quote = lambda x: "'" + x.replace("'","''") + "'"
    selectors = ",\n      ".join("STRUCT("+quote(code)+" AS item_id, "+quote(style)+" AS style)"
        for style,codes in by_style.items() for code in codes)
    table = chr(96)+TABLE+chr(96)
    return f"""
    WITH targets AS (SELECT item_id,style FROM UNNEST([{selectors}])),
    events AS (
      SELECT event_date,event_timestamp,event_name,user_pseudo_id,
        (SELECT ep.value.int_value FROM UNNEST(event_params) ep
         WHERE ep.key='ga_session_id' LIMIT 1) AS ga_session_id,
        NULLIF(TRIM(ecommerce.transaction_id),'') AS txn,
        COALESCE(NULLIF(traffic_source.source,''),'(not set)') AS first_source,
        items
      FROM {table}
      WHERE _TABLE_SUFFIX BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}'
        AND event_name IN ('view_item','add_to_cart','purchase')
    ),
    matched AS (
      SELECT IF(PARSE_DATE('%Y%m%d',e.event_date)>=DATE '{split}','current','previous') AS period,
        t.style,t.item_id,e.event_name,e.event_timestamp,e.first_source,
        IF(e.user_pseudo_id IS NOT NULL AND e.ga_session_id IS NOT NULL,
          CONCAT(e.user_pseudo_id,'_',CAST(e.ga_session_id AS STRING)),NULL) AS session_key,
        SAFE_CAST(i.quantity AS FLOAT64) AS units,
        ROW_NUMBER() OVER (
          PARTITION BY e.event_date,e.event_name,t.item_id,
            IF(e.event_name='purchase' AND e.txn IS NOT NULL,
              e.txn,CAST(e.event_timestamp AS STRING))
          ORDER BY e.event_timestamp
        ) AS rn
      FROM events AS e CROSS JOIN UNNEST(e.items) AS i
      INNER JOIN targets AS t ON i.item_id=t.item_id
    ),
    sessions AS (
      SELECT period,style,session_key,ANY_VALUE(first_source) AS first_source,
        MIN(IF(event_name='view_item',event_timestamp,NULL)) AS view_ts,
        MIN(IF(event_name='add_to_cart',event_timestamp,NULL)) AS cart_ts,
        MIN(IF(event_name='purchase',event_timestamp,NULL)) AS buy_ts,
        COUNTIF(event_name='view_item') AS view_events,
        COUNTIF(event_name='add_to_cart') AS cart_events,
        SUM(IF(event_name='purchase',COALESCE(units,1),0)) AS purchase_units
      FROM matched
      WHERE session_key IS NOT NULL AND (event_name!='purchase' OR rn=1)
      GROUP BY period,style,session_key
    )
    SELECT period,style,first_source,
      COUNTIF(view_ts IS NOT NULL) AS view_sessions,
      COUNTIF(cart_ts IS NOT NULL) AS cart_sessions,
      COUNTIF(buy_ts IS NOT NULL) AS purchase_sessions,
      COUNTIF(view_ts IS NOT NULL AND cart_ts>=view_ts) AS cart_after_view_sessions,
      COUNTIF(view_ts IS NOT NULL AND cart_ts>=view_ts AND buy_ts>=cart_ts) AS purchase_after_cart_sessions,
      SUM(view_events) AS view_events,SUM(cart_events) AS cart_events,
      SUM(purchase_units) AS purchase_units
    FROM sessions GROUP BY period,style,first_source
    """


def summarize(rows, by_style, focus):
    products = []
    for name,codes in by_style.items():
        result, sources = {}, {}
        for period in ("previous","current"):
            sub = [r for r in rows if r.get("style")==name and r.get("period")==period]
            metrics = {k:sum(float(r.get(k) or 0) for r in sub) for k in COUNTS}
            metrics["view_to_cart_pct"] = round(100*metrics["cart_after_view_sessions"]/metrics["view_sessions"],2) if metrics["view_sessions"] else None
            metrics["view_to_purchase_pct"] = round(100*metrics["purchase_after_cart_sessions"]/metrics["view_sessions"],2) if metrics["view_sessions"] else None
            result[period] = metrics
            sources[period] = {str(r.get("first_source") or "(not set)")[:80]:int(r.get("view_sessions") or 0) for r in sub}
        prev,now = result["previous"],result["current"]
        top = [dict(source=s, previous_view_sessions=sources["previous"].get(s,0),current_view_sessions=v)
                for s,v in sorted(sources["current"].items(),key=lambda z:-z[1])[:4]]
        target = next((x for x in (focus.get("cards") or []) if x.get("name")==name), {})
        total_purchase = target.get("sales_7d")
        coverage = round(100*now["purchase_units"]/total_purchase,1) if isinstance(total_purchase,(int,float)) and total_purchase>0 else None
        delta = round(now["view_to_cart_pct"]-prev["view_to_cart_pct"],2) if now["view_to_cart_pct"] is not None and prev["view_to_cart_pct"] is not None else None
        products.append(dict(name=name,item_code_count=len(codes),previous=prev,current=now,
            view_to_cart_delta_pp=delta,first_touch_sources=top,
            matched_purchase_coverage_pct=coverage,coverage_reliable=coverage is not None and 80<=coverage<=105,
            note="동일 GA4 세션 내 시간순 view_item→add_to_cart→purchase. 세션 식별 결측은 제외. "
                 "source는 최초 유입 출처로 현재 세션/캠페인 귀속이 아님."))
    return products


def main():
    focus = (read(ROOT/"reports/daily_intelligence/latest.json").get("product_focus") or {})
    purchase = read(ROOT/"reports/decision_health/ga4_purchase_diagnostics.json")
    by_style=targets(focus,purchase)
    status={"schema":1,"report_date":focus.get("date"),"status":"source_not_aligned",
        "products":[],"cost":{"maximum_bytes_billed":CAP,"dry_run_required":True},
        "scope":"14-day exact item IDs; ordered same-session GA4 funnel; aggregates only"}
    if not by_style:
        save(status)
        return
    end=dt.date.fromisoformat(focus["date"])
    if end>dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).date():
        save(status)
        return
    before=read(OUT)
    if before.get("report_date")==focus["date"] and before.get("status")=="ga4_measured" and os.getenv("CSK_FUNNEL_FORCE")!="1":
        print("[FUNNEL] Reusing matching aggregate; no query")
        return
    token=os.getenv("GOOGLE_SA_JSON_B64","").strip()
    if not token:
        save({**status,"status":"blocked_credentials"})
        return
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
        info=json.loads(base64.b64decode(token,validate=True))
        client=bigquery.Client(project=info["project_id"],
            credentials=service_account.Credentials.from_service_account_info(info),location=LOCATION)
        q=query_for(end,by_style)
        dry=client.query(q,location=LOCATION,job_config=bigquery.QueryJobConfig(dry_run=True,use_query_cache=False))
        estimate=int(dry.total_bytes_processed or 0)
        if estimate<=0:
            save({**status,"status":"blocked_estimate"})
            return
        if estimate>CAP:
            save({**status,"status":"blocked_budget","cost":{**status["cost"],"dry_run_bytes":estimate}})
            print("[FUNNEL] Budget guard: scan larger than 2 GiB, execution skipped")
            return
        job=client.query(q,location=LOCATION,job_config=bigquery.QueryJobConfig(maximum_bytes_billed=CAP,use_query_cache=True))
        rows=[dict(r) for r in job.result(timeout=180)]
        save({**status,"status":"ga4_measured" if rows else "no_matching_events",
            "products":summarize(rows,by_style,focus),
            "cost":{**status["cost"],"dry_run_bytes":estimate,"billed_bytes":int(job.total_bytes_billed or 0)}})
        print("[FUNNEL] status",("ga4_measured" if rows else "no_matching_events"),
              "report_date",end,"aggregate_rows",len(rows),"billed_bytes",int(job.total_bytes_billed or 0))
    except Exception as exc:
        save({**status,"status":"query_error","error_type":type(exc).__name__})
        print("[FUNNEL] Query unavailable:",type(exc).__name__)


if __name__=="__main__":
    main()
