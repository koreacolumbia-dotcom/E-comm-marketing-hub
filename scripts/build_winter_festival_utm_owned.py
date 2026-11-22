#!/usr/bin/env python3
"""Budget-capped event D1-D2 owned UTM campaign/term comparison, 2025 vs 2026.

Reuses Daily Digest canonical session attribution and transaction de-duplication.
Only aggregate non-PII labels; never publish raw event/user/session records.
"""
from __future__ import annotations
import base64
import datetime as dt
import json
import math
import os
import re
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUTPUT=ROOT/"reports/decision_health/winter_festival_utm_owned_2026.json"
TABLE="columbia-ga4.analytics_358593394.events_*"
LOCATION="asia-northeast3"
PER_QUERY_CAP=3*1024**3
TOTAL_CAP=6*1024**3
WINDOWS={"2025":("2025-11-21","2025-11-22"),"2026":("2026-11-20","2026-11-21")}
OWNED_MARKERS=re.compile(r"owned|on[-_ ]?ed",re.I)
FESTIVAL_MARKERS=re.compile(r"winter[_ -]?festival|윈터페스티벌",re.I)
SEASONOFF_MARKERS=re.compile(r"winter[_ -]?seasonoff|윈터시즌오프",re.I)
OWNED_PREFIX=re.compile(r"^(?:KAKAO_CH|LMS_ECOM|EDM|KAKAO_FridnsTalk|KAKAO_alimtalk)\s*/",re.I)
SAFE_SUFFIXES={"store","main","coupon","banner","special_price","logo","men","women","product1","product2","shirts","pants","hat","jacket"}
SOURCE_HINT=re.compile(r"kakao|카카오|lms|sms|edm|e-?mail|friendtalk|alimtalk|newsletter|kakaotalk",re.I)
UNSAFE=re.compile(r"@|https?://|[?&](?:token|email|uid|user_id|phone)=|(?:\+?\d[\d\s().-]{9,}\d)",re.I)

def clean_label(value,limit=84):
    """Publish only harmless UTM names; no customer identifiers or URLs."""
    s=str(value or "(not set)").strip()
    if not s or len(s)>limit or UNSAFE.search(s):
        return "(restricted)"
    return s

def metric(value):
    if type(value) not in (int,float) or not math.isfinite(value) or value<0:
        raise ValueError("Invalid nonnegative GA4 aggregate")
    return float(value)

def sql_with_utm_term(base):
    """Retain canonical GA4 source/campaign attribution; attach first nonempty session UTM term.

    A session_start without term must not overwrite a later tagged pageview.
    Event-level page-location term is fallback, not a substitute for source attribution.
    """
    marker="session_keys AS ("
    if base.count(marker)!=1:
        raise ValueError("Unexpected canonical query layout (session_keys)")
    term_cte=r"""
utm_term_candidates AS (
  SELECT session_key, event_name, event_timestamp,
    COALESCE(
      clean(JSON_VALUE(st,'$.manual_campaign.term')),
      clean(JSON_VALUE(st,'$.manual_campaign.manual_term')),
      clean(JSON_VALUE(st,'$.cross_channel_campaign.term')),
      clean(JSON_VALUE(ct,'$.manual_term')),
      clean(url_decode(REGEXP_EXTRACT(page_url,r'(?i)[?&]utm_term=([^&#]+)')))
    ) AS utm_term
  FROM keyed WHERE session_key IS NOT NULL
),
utm_term_by_session AS (
  SELECT session_key, utm_term FROM utm_term_candidates
  WHERE utm_term IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY session_key
    ORDER BY IF(event_name='session_start',0,1),event_timestamp,utm_term
  )=1
),
"""
    out=base.replace(marker,term_cte+marker)
    required=[
        ("IFNULL(a.default_group,'') AS sessionDefaultChannelGroup,",
         "IFNULL(a.default_group,'') AS sessionDefaultChannelGroup,\n    IFNULL(t.utm_term,'(not set)') AS sessionManualTerm,"),
        ("FROM session_keys s LEFT JOIN attribution a USING(session_key)",
         "FROM session_keys s LEFT JOIN attribution a USING(session_key)\n  LEFT JOIN utm_term_by_session t USING(session_key)"),
        ("SELECT '(not set) / (not set)', '(not set)', '', 0, transactions, purchaseRevenue",
         "SELECT '(not set) / (not set)', '(not set)', '', '(not set)', 0, transactions, purchaseRevenue"),
        ("SELECT sessionSourceMedium, sessionCampaignName, sessionDefaultChannelGroup,\n",
         "SELECT sessionSourceMedium, sessionCampaignName, sessionDefaultChannelGroup, sessionManualTerm,\n"),
        ("GROUP BY 1,2,3\n","GROUP BY 1,2,3,4\n")
    ]
    for before,after in required:
        if out.count(before)!=1:
            raise ValueError("Unexpected canonical query layout: "+before[:50])
        out=out.replace(before,after)
    return out

def summarize(source):
    """Keep confirmed UTM campaign/term and contextual owned-source rows separate.

    Source hints alone are NOT proof of an owned festival campaign.
    """
    data={}
    for year,rows in source.items():
        matched={}
        for r in rows:
            src=clean_label(r.get("sessionSourceMedium"),160)
            camp=clean_label(r.get("sessionCampaignName"))
            term=clean_label(r.get("sessionManualTerm"))
            if "(restricted)" in (src,camp,term):
                continue
            media=bool(SOURCE_HINT.search(src))
            tagged=camp+" "+term
            festival=bool(FESTIVAL_MARKERS.search(tagged))
            seasonoff=bool(SEASONOFF_MARKERS.search(tagged))
            is_owned=bool(OWNED_MARKERS.search(tagged))
            # Festival word alone in paid search/DA does not establish an Owned UTM.
            strong=is_owned or ((festival or seasonoff) and bool(OWNED_PREFIX.search(src)))
            if not strong and not media:continue
            kind="utm_campaign_or_term" if strong else "media_context_only"
            family="winter_festival" if festival else "winter_seasonoff" if seasonoff else "other_owned"
            # Distinct appended sub-ID fragments may be personalized; keep campaign aggregate
            # and mask that UTM term rather than disclosing per-link suffixes.
            if (term.lower().startswith(camp.lower()) and len(term)>len(camp)+1 and
                term[len(camp):].strip("_- ").lower() not in SAFE_SUFFIXES):
                term="(individualized suffix masked)"
            key=(kind,family,src,camp,term)
            val=matched.setdefault(key,{"sessions":0.,"orders":0.,"revenue":0.})
            val["sessions"]+=metric(r.get("sessions"))
            val["orders"]+=metric(r.get("transactions"))
            val["revenue"]+=metric(r.get("purchaseRevenue"))
        grouped=[]
        suppressed={"sessions":0.,"orders":0.,"revenue":0.}
        for (kind,family,src,camp,term),v in matched.items():
            if v["sessions"]<5:
                for k in suppressed:suppressed[k]+=v[k]
                continue
            grouped.append({"classification":kind,"campaign_family":family,"source_medium":src,
                "utm_campaign":camp,"utm_term":term,
                "sessions":int(v["sessions"]),"orders":int(v["orders"]),
                "revenue":round(v["revenue"]),"cvr_pct":round(100*v["orders"]/v["sessions"],2) if v["sessions"] else None})
        grouped.sort(key=lambda x:(x["classification"]!="utm_campaign_or_term",-x["revenue"],x["source_medium"]))
        a={"utm_campaign_or_term":{"sessions":0,"orders":0,"revenue":0},
           "media_context_only":{"sessions":0,"orders":0,"revenue":0}}
        for g in grouped:
            for m in ("sessions","orders","revenue"):a[g["classification"]][m]+=g[m]
        data[year]={"rows":grouped,"grouped_totals":a,
                    "suppressed_small_groups":{"count":sum(v["sessions"]<5 for v in matched.values()),
                        "sessions":round(suppressed["sessions"]),"orders":round(suppressed["orders"]),
                        "revenue":round(suppressed["revenue"])},
                    "raw_aggregate_rows":len(rows),"visible_rows":len(grouped)}
    return data

def write(payload):
    OUTPUT.parent.mkdir(parents=True,exist_ok=True)
    OUTPUT.write_text(json.dumps(payload,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")

def main():
    base={"schema":1,"report_date":"2026-11-21","period":WINDOWS,
       "status":"not_measured","data":{},"causal":False,
       "definition":"GA4 BigQuery purchase-event revenue and session-key attribution. campaign is sessionCampaignName; term is session UTM manual_term or landing URL utm_term.",
       "evidence_separation":"utm_campaign_or_term denotes keyword match in campaign or term; media_context_only is only a source/medium hint, NOT verified Winter Festival.",
       "cost":{"per_query_cap":PER_QUERY_CAP,"total_dry_cap":TOTAL_CAP,"dry_run_required":True},
       "caveats":["UTM tag labels need not match named campaign; overlapping campaign tags can be misclassified.",
                  "Revenue from tagged sessions is attributed revenue, NOT incremental CRM lift.",
                  "Small tag groups suppressed. Untagged or differently tagged sessions excluded from relevant buckets.",
                  "2025 and 2026 tag naming may differ; compare category coverage, not exact term names."]}
    if not os.getenv("GOOGLE_SA_JSON_B64"):
        write({**base,"status":"blocked_credentials"})
        return
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
        sys.path.insert(0,str(ROOT))
        from daily_digest_session_source import build_sql,select_partitions
        info=json.loads(base64.b64decode(os.environ["GOOGLE_SA_JSON_B64"],validate=True))
        creds=service_account.Credentials.from_service_account_info(info)
        client=bigquery.Client(project=info["project_id"],credentials=creds,location=LOCATION)
        prepared={}
        estimates={}
        for year,(start,end) in WINDOWS.items():
            parts=select_partitions(client,TABLE,dt.date.fromisoformat(start),dt.date.fromisoformat(end))
            sql=sql_with_utm_term(build_sql(parts,start,end))
            dry=client.query(sql,location=LOCATION,job_config=bigquery.QueryJobConfig(dry_run=True,use_query_cache=False))
            estimate=int(dry.total_bytes_processed or 0)
            estimates[year]=estimate
            prepared[year]=sql
        cost={**base["cost"],"dry_run_bytes":estimates,
            "dry_run_total_bytes":sum(estimates.values())}
        if any(x<=0 or x>PER_QUERY_CAP for x in estimates.values()) or sum(estimates.values())>TOTAL_CAP:
            write({**base,"status":"blocked_budget","cost":cost})
            return
        rows={}
        billed={}
        for year in ("2025","2026"):
            job=client.query(prepared[year],location=LOCATION,job_config=bigquery.QueryJobConfig(maximum_bytes_billed=PER_QUERY_CAP,use_query_cache=True))
            rows[year]=[dict(r) for r in job.result(timeout=190)]
            billed[year]=int(job.total_bytes_billed or 0)
        data=summarize(rows)
        write({**base,"status":"ga4_measured","data":data,
               "cost":{**cost,"billed_bytes":billed}})
        print("[FESTIVAL UTM] measured", {y:v["visible_rows"] for y,v in data.items()})
    except Exception as exc:
        # No raw SQL, UTM identifiers, event properties, or secrets in error file.
        write({**base,"status":"query_error","error_type":type(exc).__name__})
        print("[FESTIVAL UTM] unavailable",type(exc).__name__)

if __name__=="__main__":
    main()
