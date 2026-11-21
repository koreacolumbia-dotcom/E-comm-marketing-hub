"""Read-only, aggregate-only anomaly diagnostic. No production report writes."""
import datetime as dt
import html
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import pandas as pd
from google.cloud import bigquery
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from daily_digest_session_source import select_partitions, build_sql
from daily_digest_live_final import classify_looker_channel, PDP_CATEGORY_MAP
from send_daily_email import send_email_mixed_with_inline_image

PROJECT = "columbia-ga4"
TABLE = PROJECT + ".analytics_358593394.events_*"
LOCATION = "asia-northeast3"
RECIPIENT = "hugh.kang@columbia.com"
request = json.loads(Path("diagnostics/csk-anomaly-request.json").read_text(encoding="utf-8"))
day = dt.date.fromisoformat(request["date"])
today = dt.datetime.now(ZoneInfo("Asia/Seoul")).date()
if not today - dt.timedelta(days=62) <= day < today:
    raise ValueError("Date must be a completed day within the last 62 days")
reason = str(request.get("reason", ""))[:1500]
client = bigquery.Client(project=PROJECT, location=LOCATION)
queries, problems, query_results = {}, [], {}
def execute(label, sql):
    queries[label] = sql
    config = bigquery.QueryJobConfig(maximum_bytes_billed=50 * 1024**3)
    job = client.query(sql, location=LOCATION, job_config=config)
    df = job.result(timeout=240).to_dataframe(create_bqstorage_client=False)
    print(f"[BQ] {label}: job={job.job_id} rows={len(df)} bytes={job.total_bytes_processed}", flush=True)
    query_results[label] = {"job_id": job.job_id, "bytes_processed": job.total_bytes_processed, "rows": len(df)}
    return df

def source_day(d):
    parts = select_partitions(client, TABLE, d, d)
    label = "session_" + d.isoformat()
    f = execute(label, build_sql(parts, d.isoformat(), d.isoformat()))
    if f.empty:
        raise RuntimeError("No session rows")
    f["bucket"] = f.apply(lambda r: classify_looker_channel(str(r["sessionSourceMedium"]), str(r["sessionCampaignName"]), str(r["sessionDefaultChannelGroup"])), axis=1)
    for metric in ("sessions", "transactions", "purchaseRevenue"):
        f[metric] = pd.to_numeric(f[metric], errors="raise").fillna(0)
    totals = {m: float(f[m].sum()) for m in ("sessions","transactions","purchaseRevenue")}
    assert totals["sessions"] == float(f["expected_sessions"].iloc[0]), "Independent session reconciliation failed"
    totals["cvr"] = totals["transactions"] / totals["sessions"] if totals["sessions"] else None
    channels = f.groupby("bucket")[["sessions","transactions","purchaseRevenue"]].sum().reset_index()
    etc = f[f["bucket"] == "etc"].groupby("sessionSourceMedium", as_index=False)["sessions"].sum().sort_values("sessions",ascending=False).head(8)
    return {"totals":totals,"channels":channels.to_dict("records"),"unassigned_sources":etc.to_dict("records"),"partitions":{x.isoformat():k for x,k,_ in parts}}, parts

results = {"date":day.isoformat(),"request_id":request.get("request_id"),"reason":reason,"source":TABLE,"comparisons":{}}
parts = None
for d in (day, day-dt.timedelta(days=1), day-dt.timedelta(days=7)):
    try:
        values, selected = source_day(d)
        results["comparisons"][d.isoformat()] = values
        if d == day:
            parts = selected
    except Exception as exc:
        problems.append(f"세션/채널 쿼리 {d}: {type(exc).__name__}: {str(exc)[:350]}")

if parts:
    try:
        _, partition_kind, table = next(p for p in parts if p[0] == day)
        identifier = f"{table.project}.{table.dataset_id}.{table.table_id}"
        tick = chr(96)
        sql = f"""
WITH e AS (
  SELECT event_name, user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id' LIMIT 1) AS sid,
    COALESCE(NULLIF(CAST(ecommerce.transaction_id AS STRING),''),(SELECT NULLIF(value.string_value,'') FROM UNNEST(event_params) WHERE key='transaction_id' LIMIT 1)) AS tid,
    ARRAY_LENGTH(items) AS item_count
  FROM {tick}{identifier}{tick}
)
SELECT COUNT(*) AS events,
  COUNTIF(event_name='session_start') AS session_start_events,
  COUNTIF(user_pseudo_id IS NULL OR sid IS NULL) AS events_missing_session_key,
  COUNTIF(event_name='purchase') AS purchase_events,
  COUNTIF(event_name='purchase' AND (tid IS NULL OR LOWER(tid) IN ('(not set)','(not provided)'))) AS purchase_missing_transaction_id,
  COUNTIF(event_name='purchase' AND tid IS NOT NULL AND LOWER(tid) NOT IN ('(not set)','(not provided)')) -
    COUNT(DISTINCT IF(event_name='purchase' AND LOWER(tid) NOT IN ('(not set)','(not provided)'),tid,NULL)) AS repeated_purchase_id_events,
  COUNTIF(event_name='view_item') AS pdp_events,
  SUM(IF(event_name='view_item',IFNULL(item_count,0),0)) AS pdp_item_views
FROM e
"""
        raw = execute("raw_event_quality", sql).iloc[0].to_dict()
        results["raw_event_quality"] = {k:int(v or 0) for k,v in raw.items()}
        results["raw_event_partition"] = partition_kind
    except Exception as exc:
        problems.append(f"이벤트 품질 쿼리: {type(exc).__name__}: {str(exc)[:350]}")

    try:
        f = execute("pdp_source_medium", build_sql(parts,day.isoformat(),day.isoformat(),pdp_lookup_table=TABLE))
        f["c1"] = f["c1"].astype(str).str.strip().str.upper()
        f["c2"] = f["c2"].astype(str).str.strip()
        f["views"] = pd.to_numeric(f["views"],errors="raise")
        cats = []
        for c1, subs in PDP_CATEGORY_MAP.items():
            for label, names in subs.items():
                subset=f[(f["c1"]==c1)&f["c2"].isin(names or [label])]
                top=subset.groupby("sessionSourceMedium",as_index=False)["views"].sum().sort_values(["views","sessionSourceMedium"],ascending=[False,True]).head(2)
                cats.append({"category":f"{c1} · {label}","views":int(subset["views"].sum()),"top_sources":top.to_dict("records")})
        results["pdp"] = cats
    except Exception as exc:
        problems.append(f"PDP 소스/매체 쿼리: {type(exc).__name__}: {str(exc)[:350]}")

report_path = Path(f"reports/daily_digest/data/daily/{day.isoformat()}.json")
report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}
current = results["comparisons"].get(day.isoformat(), {})
findings=[]
if current:
    totals=current["totals"]
    kpis=report.get("kpis",{})
    for metric,key in (("sessions","sessions"),("transactions","orders"),("purchaseRevenue","revenue")):
        if key in kpis:
            a,b=totals[metric],float(kpis[key] or 0)
            if a != b:
                findings.append(f"{metric}: 원천 {a:,.2f} / 리포트 {b:,.2f} / 차이 {a-b:+,.2f}")
    etc = next((c["sessions"] for c in current["channels"] if c["bucket"]=="etc"),0)
    if totals["sessions"] and etc/totals["sessions"]>=0.05:
        findings.append(f"ETC 원천 비중 {etc/totals['sessions']:.2%} (5% 이상)")
    week = results["comparisons"].get((day-dt.timedelta(days=7)).isoformat(),{}).get("totals",{})
    for metric in ("sessions","transactions","purchaseRevenue","cvr"):
        a,b=totals.get(metric),week.get(metric)
        if a is not None and b:
            delta=a/b-1
            if abs(delta)>=0.30 or (metric!="sessions" and delta<=-0.25):
                findings.append(f"{metric}: 전주 동요일 {delta:+.1%} (프로모션·집계 지연 등 추가 확인 필요)")
if not report:
    findings.append("현재 보고 대상일 리포트 JSON 미도착")
if report and results.get("pdp"):
    reported={r.get("itemCategory"):r.get("views_d1") for r in report.get("pdp_series",{}).get("rows",[])}
    for r in results["pdp"]:
        if r["category"] in reported and r["views"]!=reported[r["category"]]:
            findings.append(f"PDP {r['category']}: 원천 {r['views']:,} / 리포트 {reported[r['category']]:,}")

# Winter Sale pre-notification cohort: aggregate-only read-only BigQuery.
if request.get("analysis_mode") == "winter_sale_cohort":
    try:
        cohort_sql = """
WITH base AS (
 SELECT PARSE_DATE('%Y%m%d',event_date) AS d, event_name, user_pseudo_id,
   (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id' LIMIT 1) AS sid,
   COALESCE(ecommerce.purchase_revenue,0) AS revenue,
   COALESCE(NULLIF(CAST(ecommerce.transaction_id AS STRING),''),
     (SELECT NULLIF(value.string_value,'') FROM UNNEST(event_params) WHERE key='transaction_id' LIMIT 1)) AS tid
 FROM `columbia-ga4.analytics_358593394.events_*`
 WHERE _TABLE_SUFFIX BETWEEN '20261111' AND '20261120'
out = Path("anomaly-output")
out.mkdir(exist_ok=True)
payload=json.dumps(results,ensure_ascii=False,indent=2,default=str)
(out/"diagnosis.json").write_text(payload,encoding="utf-8")
sql_bundle="\n\n".join("-- "+name+"\n"+sql for name,sql in queries.items())
(out/"executed_queries.sql").write_text(sql_bundle,encoding="utf-8")
print("[DIAGNOSTIC_RESULT] "+json.dumps(results,ensure_ascii=False,default=str),flush=True)

lines=[f"CSK E-COMM 데이터 원천 재검증 | {day}",f"감지 사유: {reason}",f"리포트 생성: {report.get('built_at_kst','미도착')}", ""]
if current:
    for d,r in results["comparisons"].items():
        t=r["totals"]
        cvr=f"{t['cvr']:.2%}" if t.get("cvr") is not None else "N/A"
        lines.append(f"{d}: 세션 {t['sessions']:,.0f} / 주문 {t['transactions']:,.0f} / 매출 {t['purchaseRevenue']:,.0f}원 / CVR {cvr}")
lines += ["","[확인된 편차]",*(findings or ["원천 재조회에서 설정 기준 이상의 편차가 추가 확인되지 않았습니다."]), "", "[조회 오류/제약]",*(problems or ["없음"]), "", "판단: 원천과 리포트 차이는 데이터 확정 지연·집계 정의 변경 가능성이 있으므로 실제 성과 이상과 구분합니다.", "권장 조치: 확정 파티션 확인, 불일치 지표 재집계, 동일 기준 비교. Paid 광고비/매체 ROAS는 별도 매체 원본 확인이 필요합니다.", "첨부: 실제 쿼리와 집계 결과(JSON). 사용자 개인 식별자는 결과에 포함하지 않았습니다."]
body="\n".join(lines)
if request.get("analysis_mode") != "winter_sale_cohort":
    send_email_mixed_with_inline_image(host=os.environ["SMTP_HOST"],port=int(os.environ.get("SMTP_PORT") or "587"),user=os.environ["SMTP_USER"],password=os.environ["SMTP_PASS"],to_list=[RECIPIENT],subject=f"[CSK DATA ALERT] {day} 원천 재검증",text_fallback=body,html_body="<pre style='font:14px Arial;white-space:pre-wrap'>"+html.escape(body)+"</pre>",inline_png_cid=None,inline_png_bytes=None,attachments=[("diagnosis.json","json",payload.encode()),("executed_queries.sql","octet-stream",sql_bundle.encode())])
if request.get("analysis_mode") != "winter_sale_cohort":
    print("[DIAGNOSTIC_EMAIL_OK] SMTP accepted aggregate diagnosis for Hugh only",flush=True)
else:
    print("[COHORT_ANALYSIS_ONLY] No duplicate email sent",flush=True)
)
 AND user_pseudo_id IS NOT NULL
),
visitors AS (
 SELECT user_pseudo_id,
   MAX(IF(d BETWEEN DATE '2026-11-18' AND DATE '2026-11-19',1,0)) AS previsit,
   MAX(IF(d BETWEEN DATE '2026-11-11' AND DATE '2026-11-17',1,0)) AS baseline_visit,
   MAX(IF(d=DATE '2026-11-20',1,0)) AS salevisit
 FROM base WHERE event_name='session_start'
 GROUP BY 1
),
sales AS (
 SELECT user_pseudo_id,COUNT(DISTINCT tid) AS orders,SUM(revenue) AS revenue
 FROM base WHERE d=DATE '2026-11-20' AND event_name='purchase' AND tid IS NOT NULL
 GROUP BY 1
)
SELECT CASE WHEN v.previsit=1 THEN 'pre_notification_visitor'
 WHEN v.baseline_visit=1 THEN 'earlier_visitor_no_prenotification'
 ELSE 'no_recorded_visit_11_11_to_11_19' END AS cohort,
 COUNT(*) AS visitors_in_cohort,
 COUNTIF(v.salevisit=1) AS sale_day_return_visitors,
 COUNTIF(s.orders>0) AS sale_day_buyers,
 SUM(IFNULL(s.orders,0)) AS sale_day_orders,
 ROUND(SUM(IFNULL(s.revenue,0)),0) AS sale_day_revenue
FROM visitors v LEFT JOIN sales s USING(user_pseudo_id)
WHERE v.salevisit=1 OR v.previsit=1 OR v.baseline_visit=1
GROUP BY 1 ORDER BY 1
"""
        cohort = execute("winter_sale_cohort", cohort_sql)
        results["winter_sale_cohort"] = cohort.to_dict("records")
        results["winter_sale_cohort_definition"] = "GA4 user_pseudo_id, session_start visitor cohort; sale-day purchase events; observational association, not causal attribution. Device/cookie changes can undercount returning visitors."
    except Exception as exc:
        problems.append(f"윈터세일 코호트 쿼리: {type(exc).__name__}: {str(exc)[:350]}")

results.update({"findings":findings,"errors":problems,"query_jobs":query_results,"report_built_at":report.get("built_at_kst")})
out = Path("anomaly-output")
out.mkdir(exist_ok=True)
payload=json.dumps(results,ensure_ascii=False,indent=2,default=str)
(out/"diagnosis.json").write_text(payload,encoding="utf-8")
sql_bundle="\n\n".join("-- "+name+"\n"+sql for name,sql in queries.items())
(out/"executed_queries.sql").write_text(sql_bundle,encoding="utf-8")
print("[DIAGNOSTIC_RESULT] "+json.dumps(results,ensure_ascii=False,default=str),flush=True)

lines=[f"CSK E-COMM 데이터 원천 재검증 | {day}",f"감지 사유: {reason}",f"리포트 생성: {report.get('built_at_kst','미도착')}", ""]
if current:
    for d,r in results["comparisons"].items():
        t=r["totals"]
        cvr=f"{t['cvr']:.2%}" if t.get("cvr") is not None else "N/A"
        lines.append(f"{d}: 세션 {t['sessions']:,.0f} / 주문 {t['transactions']:,.0f} / 매출 {t['purchaseRevenue']:,.0f}원 / CVR {cvr}")
lines += ["","[확인된 편차]",*(findings or ["원천 재조회에서 설정 기준 이상의 편차가 추가 확인되지 않았습니다."]), "", "[조회 오류/제약]",*(problems or ["없음"]), "", "판단: 원천과 리포트 차이는 데이터 확정 지연·집계 정의 변경 가능성이 있으므로 실제 성과 이상과 구분합니다.", "권장 조치: 확정 파티션 확인, 불일치 지표 재집계, 동일 기준 비교. Paid 광고비/매체 ROAS는 별도 매체 원본 확인이 필요합니다.", "첨부: 실제 쿼리와 집계 결과(JSON). 사용자 개인 식별자는 결과에 포함하지 않았습니다."]
body="\n".join(lines)
send_email_mixed_with_inline_image(host=os.environ["SMTP_HOST"],port=int(os.environ.get("SMTP_PORT") or "587"),user=os.environ["SMTP_USER"],password=os.environ["SMTP_PASS"],to_list=[RECIPIENT],subject=f"[CSK DATA ALERT] {day} 원천 재검증",text_fallback=body,html_body="<pre style='font:14px Arial;white-space:pre-wrap'>"+html.escape(body)+"</pre>",inline_png_cid=None,inline_png_bytes=None,attachments=[("diagnosis.json","json",payload.encode()),("executed_queries.sql","octet-stream",sql_bundle.encode())])
print("[DIAGNOSTIC_EMAIL_OK] SMTP accepted aggregate diagnosis for Hugh only",flush=True)
