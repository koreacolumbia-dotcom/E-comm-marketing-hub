#!/usr/bin/env python3
"""Winter Festival report-only evidence bundle. Standard library; NO SQL/PII/API calls.

Reads already-published aggregate GA4 sources. Never presents GA4 attributed
revenue as ad-platform ROAS or incremental campaign effects.
"""
from __future__ import annotations
import datetime as dt
import json
import math
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
BASE=ROOT/"reports"
OUT=BASE/"decision_health/winter_festival_integrated_latest.json"
OPS=BASE/"marketing_operations/latest.json"
SOURCES=BASE/"decision_health/source_medium_weekly.json"
UTM=BASE/"decision_health/winter_festival_utm_owned_2026.json"
INTEL=BASE/"marketing_intelligence_v2/latest.json"
DAY_KEYS=[("2026-11-20","2025-11-21"),("2026-11-21","2025-11-22"),
          ("2026-11-22","2025-11-23")]
HIGHLIGHT={
    "meta / banner":("meta_banner","Meta / banner","paid"),
    "META_BD / META_BD":("meta_bd","META_BD / META_BD","paid"),
    "Facebook / banner":("facebook_banner","Facebook / banner","paid"),
    "naverbs / cpc":("naver_brand","네이버 브랜드검색","paid"),
    "naver / cpc":("naver_cpc","네이버 일반검색","paid"),
    "criteo / banner":("criteo","Criteo / banner","paid"),
    "kakao / da":("kakao_da","카카오 DA","paid"),
    "KAKAO_CH / KAKAO_CH":("kakao_owned","카카오 채널","crm"),
    "LMS_ECOM / LMS_ECOM":("lms_owned","LMS","crm"),
    "EDM / EDM":("edm_owned","EDM","crm"),
    "google / organic":("google_organic","Google 자연검색","organic"),
    "naver / organic":("naver_organic","Naver 자연검색","organic"),
}
def read(path):
    try:
        d=json.loads(path.read_text(encoding="utf-8"))
        return d if isinstance(d,dict) else {}
    except (OSError,ValueError,UnicodeError):
        return {}

def valid(v):
    return type(v) in (int,float) and math.isfinite(v) and v>=0

def metrics(v):
    if not isinstance(v,dict):return None
    sessions=v.get("sessions")
    orders=v.get("transactions",v.get("orders"))
    revenue=v.get("revenue")
    if not all(valid(x) for x in (sessions,orders,revenue)):return None
    s,o,r=int(sessions),int(orders),round(revenue)
    return {"sessions":s,"orders":o,"revenue":r,
            "cvr_pct":round(100*o/s,3) if s else None}

def event_days(ops,old):
    year=((ops.get("weekly") or {}).get("yoy") or {})
    rows=year.get("daily_comparison") or []
    index={(r.get("date"),r.get("ly_date")):r for r in rows
           if isinstance(r,dict)}
    matched=[]
    for current,previous in DAY_KEYS:
        row=index.get((current,previous),{})
        now=metrics(row.get("current"))
        ly=metrics(row.get("last_year"))
        if now is None or ly is None or str(ops.get("report_date",""))<current:
            break
        matched.append({"day":len(matched)+1,"current_date":current,"previous_date":previous,
                        "current":now,"previous":ly})
    if len(matched)>=2 and year.get("status")=="ready":
        sums={}
        for period in ("current","previous"):
            raw={k:sum(day[period][k] for day in matched)
                 for k in ("sessions","orders","revenue")}
            sums[period]=metrics(raw)
        return {"status":"verified_ga4","days":len(matched),"daily":matched,
                "totals":sums,"comparison":"same_event_day_same_weekday_364_offset",
                "source_report_date":ops.get("report_date"),"causal":False}
    historic=old.get("event") or {}
    if historic.get("status")=="verified_ga4" and historic.get("days",0)>=2:
        return dict(historic, retention_reason="Last verified event-day comparison retained after rolling weekly window changed.")
    return {"status":"not_ready","days":0,"daily":[],"totals":{},"causal":False}

def weekly_channels(source,ops):
    period=source.get("period") or {}
    rows=source.get("rows") or []
    out=[]
    for row in rows:
        name=row.get("source_medium")
        if name not in HIGHLIGHT:continue
        identity,label,kind=HIGHLIGHT[name]
        previous=metrics(row.get("previous"))
        current=metrics(row.get("current"))
        if previous is None or current is None:continue
        out.append({"id":identity,"source_medium":name,"label":label,"kind":kind,
                    "previous":previous,"current":current,
                    "revenue_delta":current["revenue"]-previous["revenue"]})
    out.sort(key=lambda x:(x["kind"]!="paid",-x["current"]["revenue"],x["label"]))
    is_fresh=(source.get("report_date")==ops.get("report_date"))
    is_measured=(source.get("status")=="ga4_measured")
    return {"status":"ga4_measured" if is_measured else "unavailable",
            "report_date":source.get("report_date"),
            "period":period,"rows":out,
            "date_aligned_with_daily_kpi":is_fresh,
            "source_type":"ga4_session_source_medium",
            "comparison":"current_7d_vs_previous_7d_NOT_year_over_year",
            "caveat":"GA4 session-attributed metrics. This 7-day period is not a festival-only D1-D3 cohort."}

def media_event(utm,old):
    media=utm.get("media_rollup")
    period=utm.get("period")
    data={}
    if (utm.get("status")=="ga4_measured"
            and isinstance(media,dict) and all(y in media for y in ("2025","2026"))
            and isinstance(period,dict)):
        return {"status":"ga4_measured","period":period,"years":media,
                "source_report_date":utm.get("report_date"),
                "causal":False,"scope":"All session source/medium grouped by paid, owned, organic, referral and direct; NOT Meta Ads spend or incremental lift.",
                "cost":utm.get("cost")}
    prior=old.get("event_media") or {}
    if prior.get("status")=="ga4_measured":
        return dict(prior, retention_reason="Previous verified media view retained until bounded GA4 query succeeds.")
    return {"status":"not_ready","period":utm.get("period"),"years":{},
            "source_report_date":utm.get("report_date"),"causal":False}

def owned_comparison(utm):
    data=utm.get("data") or {}
    if utm.get("status")!="ga4_measured":return {"status":"not_ready"}
    out={}
    for year in ("2025","2026"):
        record=(data.get(year) or {}).get("grouped_totals") or {}
        out[year]=record
    return {"status":"ga4_measured","period":utm.get("period"),"grouped_totals":out,
            "tag_naming_not_equivalent":True,"causal":False}


def shapley_event(totals):
    """Exact 3-factor 6-permutation decomposition: sessions x CVR x AOV."""
    a,b=totals["previous"],totals["current"]
    if not all(a.get(k,0)>0 and b.get(k,0)>0 for k in ("sessions","orders","revenue")):
        return {"status":"not_ready","terms":{}}
    old=[a["sessions"],a["orders"]/a["sessions"],a["revenue"]/a["orders"]]
    new=[b["sessions"],b["orders"]/b["sessions"],b["revenue"]/b["orders"]]
    impact=[0.,0.,0.]
    permutations=((0,1,2),(0,2,1),(1,0,2),(1,2,0),(2,0,1),(2,1,0))
    def multiply(v):return v[0]*v[1]*v[2]
    for perm in permutations:
        vals=old[:]
        for i in perm:
            before=multiply(vals)
            vals[i]=new[i]
            impact[i]+=(multiply(vals)-before)/len(permutations)
    if abs(sum(impact)-(b["revenue"]-a["revenue"]))>0.1:
        return {"status":"unreconciled","terms":{}}
    return {"status":"reconciled","terms":{
        name:{"amount_krw":round(value),"value_before":old[i],
              "value_after":new[i]} for i,(name,value) in enumerate(
              zip(("sessions","cvr","aov"),impact))},
            "note":"Shapley 6-order arithmetic decomposition, not causal effects."}

def diagnose_drivers(event, media, paid):
    """Rank why performance changed using two SEPARATE, not additive lenses.
    Funnel Shapley factors and GA4 channel attribution overlap. Never sum them.
    2026 event D3 is not combined with a D2-only media analysis.
    """
    if event.get("status")!="verified_ga4" or not event.get("totals"):
        return {"status":"not_ready","causal":False}
    totals=event["totals"]
    a,b=totals["previous"],totals["current"]
    if a["revenue"]<=0:
        return {"status":"not_ready","causal":False}
    funnel=shapley_event(totals)
    net=b["revenue"]-a["revenue"]
    event_days_count=event.get("days",0)
    out={"schema":1,"status":"funnel_only","causal":False,
         "event_days":event_days_count,
         "compared_dates":{"current":[event["daily"][0]["current_date"],
                                      event["daily"][-1]["current_date"]],
                           "previous":[event["daily"][0]["previous_date"],
                                       event["daily"][-1]["previous_date"]]},
         "net_revenue_change_krw":net,
         "net_revenue_yoy_pct":round((b["revenue"]/a["revenue"]-1)*100,2),
         "funnel":funnel,
         "event_media_aligned":False,
         "strengths":[],"weaknesses":[],
         "caveats":["전환율·세션·객단가 분해와 채널 매출 증감은 서로 겹치는 설명 관점입니다. 둘을 합산하지 않습니다.",
                    "관측된 GA4 귀속 매출은 프로모션·광고의 순증 인과 효과가 아닙니다.",
                    "상품 믹스·할인·광고 소재·실제 광고비 원본이 검증되지 않으면 원인으로 확정하지 않습니다."]}
    def card(key,axis,title,headline,sub,evidence,interpretation,verify):
        return {"id":key,"axis":axis,"title":title,"headline":headline,
                "subtitle":sub,"evidence":evidence,
                "interpretation":interpretation,"verify_next":verify,
                "confidence":"measured_descriptive","causal":False}
    if funnel["status"]=="reconciled":
        ft=funnel["terms"]
        cv=a["orders"]/a["sessions"]*100
        cv2=b["orders"]/b["sessions"]*100
        out["strengths"].append(card(
            "conversion","funnel","구매전환 효율 변화",
            ft["cvr"]["amount_krw"],"CVR 변화의 매출 산술 배분액 (원)",
            f"CVR {cv:.2f}% → {cv2:.2f}%; 주문 {a['orders']} → {b['orders']}; 세션 {a['sessions']:,} → {b['sessions']:,}",
            "유입 증가만으로 설명되지 않는 전환율 변화가 관측됐습니다. 판촉 자체의 인과 기여로 단정할 수 없습니다.",
            "제품상세→장바구니→결제 퍼널, 신규·기존 구매자, 가격/쿠폰 및 디바이스별 전환을 전년 동일 구간으로 대조"))
        out["basket_watch"]=card(
            "aov","funnel","객단가·구매 구성 점검",
            ft["aov"]["amount_krw"],"객단가 변화의 매출 산술 배분액 (원)",
            f"AOV {a['revenue']/a['orders']:,.0f}원 → {b['revenue']/b['orders']:,.0f}원",
            "주문수 증가와 별개로 객단가가 하락했습니다. 할인율·SKU 믹스·수량 구성이 원인인지는 아직 미검증입니다.",
            "스타일·카테고리·할인율별 구매수량, 주문당 상품수와 객단가 비교")
    if (media.get("status")!="ga4_measured" or
            not all((media.get("years") or {}).get(y) for y in ("2025","2026"))):
        out["media_unavailable_reason"]="2025·2026 채널 귀속 원본 미확보"
        return out
    periods=media.get("period") or {}
    expect={"2025":out["compared_dates"]["previous"],
            "2026":out["compared_dates"]["current"]}
    if any(periods.get(y)!=dates for y,dates in expect.items()):
        out["media_unavailable_reason"]="행사 채널 원본 기간과 누적 KPI 집계 기간 불일치"
        out["media_period"]=periods
        return out
    years=media["years"]
    # Bridge only if full grouped channel totals reconcile with official event sums.
    for year,key in (("2025","previous"),("2026","current")):
        media_totals=years[year].get("totals") or {}
        if (media_totals.get("revenue")!=totals[key]["revenue"] or
                media_totals.get("orders")!=totals[key]["orders"] or
                abs(media_totals.get("sessions",0)-totals[key]["sessions"])>2):
            out["media_unavailable_reason"]="채널 합계가 GA4 행사 KPI와 일치하지 않음"
            return out
    ly={r["id"]:r for r in years["2025"].get("groups",[]) if "id" in r}
    ty={r["id"]:r for r in years["2026"].get("groups",[]) if "id" in r}
    rows=[]
    for key in sorted(set(ly)|set(ty)):
        prior=ly.get(key,{})
        current=ty.get(key,{})
        row={"id":key,"label":current.get("label") or prior.get("label") or key,
             "previous":{"revenue":prior.get("revenue",0),
                         "orders":prior.get("orders",0),"sessions":prior.get("sessions",0)},
             "current":{"revenue":current.get("revenue",0),
                        "orders":current.get("orders",0),"sessions":current.get("sessions",0)},
             "delta_krw":current.get("revenue",0)-prior.get("revenue",0)}
        for year in ("previous","current"):
            p=row[year]
            p["cvr_pct"]=round(100*p["orders"]/p["sessions"],3) if p["sessions"] else None
        rows.append(row)
    if sum(r["delta_krw"] for r in rows)!=net:
        out["media_unavailable_reason"]="채널별 증감액 합계가 총매출 증감과 불일치"
        return out
    out.update({"status":"reconciled","event_media_aligned":True,
                "media_period":periods,"channel_bridge":rows,
                "channel_total_change_krw":sum(r["delta_krw"] for r in rows),
                "channel_gains":sorted([r for r in rows if r["delta_krw"]>0],key=lambda x:-x["delta_krw"]),
                "channel_losses":sorted([r for r in rows if r["delta_krw"]<0],key=lambda x:x["delta_krw"]),
                "unclassified":{"current":ty.get("other",{}).get("revenue",0),
                                "previous":ly.get("other",{}).get("revenue",0)},
                "paid_roas_eligible":paid.get("roas_eligible") is True})
    brand=next((r for r in rows if r["id"]=="naver_brand_search"),None)
    meta=next((r for r in rows if r["id"]=="meta_conversion_tagged"),None)
    referral=next((r for r in rows if r["id"]=="referral"),None)
    search=next((r for r in rows if r["id"]=="naver_search_other"),None)
    crm=next((r for r in rows if r["id"]=="owned_crm_tagged"),None)
    bd=next((r for r in rows if r["id"]=="meta_bd_tagged"),None)
    if brand:
        b0,b1=brand["previous"],brand["current"]
        out["strengths"].append(card(
            "brand_search","media","네이버 브랜드검색 성장",
            brand["delta_krw"],"동일 행사일 GA4 브랜드검색 매출 증감 (원)",
            f"세션 {b0['sessions']:,} → {b1['sessions']:,}; 구매 {b0['orders']} → {b1['orders']}; CVR {b0['cvr_pct']:.2f}% → {b1['cvr_pct']:.2f}%",
            "브랜드를 찾는 유입의 구매·전환이 동시에 증가했습니다. 검색 수요 증가인지 검색광고 개선인지는 구분되지 않습니다.",
            "검색어·브랜드검색 노출/클릭·점유율·랜딩과 프로모션 노출 확인"))
    if meta:
        m0,m1=meta["previous"],meta["current"]
        out["strengths"].append(card(
            "meta_banner","media","Meta 배너 구매 확대",
            meta["delta_krw"],"동일 행사일 Meta/Facebook 배너 귀속 매출 증감 (원)",
            f"세션 {m0['sessions']:,} → {m1['sessions']:,}; 구매 {m0['orders']} → {m1['orders']}; CVR {m0['cvr_pct']:.2f}% → {m1['cvr_pct']:.2f}%",
            "Meta 배너 태그로 들어온 세션의 구매가 증가했습니다. 광고 집행비·캠페인 목적 데이터 없이 ROAS 개선으로 판단하지 않습니다.",
            "Meta 광고관리자 캠페인별 실제 지출·목적·소재/세트·구매 전환 데이터 대조"))
    if referral:
        r0,r1=referral["previous"],referral["current"]
        out["weaknesses"].append(card(
            "referral","media","레퍼럴 전환 약화",
            referral["delta_krw"],"동일 행사일 레퍼럴 귀속 매출 증감 (원)",
            f"세션 {r0['sessions']:,} → {r1['sessions']:,}; 구매 {r0['orders']} → {r1['orders']}; CVR {r0['cvr_pct']:.2f}% → {r1['cvr_pct']:.2f}%",
            "레퍼럴 유입은 늘었지만 구매 효율이 악화됐습니다. 어떤 추천 사이트 또는 랜딩에서 이탈하는지는 추가 분석이 필요합니다.",
            "추천 도메인/랜딩/PDP/주문 세그먼트별 전년 차이 확인"))
    if search:
        x0,x1=search["previous"],search["current"]
        out["weaknesses"].append(card(
            "nonbrand_search","media","네이버 일반검색 유입 감소",
            search["delta_krw"],"동일 행사일 일반검색 귀속 매출 증감 (원)",
            f"세션 {x0['sessions']:,} → {x1['sessions']:,}; 구매 {x0['orders']} → {x1['orders']}; CVR {x0['cvr_pct']:.2f}% → {x1['cvr_pct']:.2f}%",
            "CVR은 개선됐지만 방문 규모가 줄어 매출이 감소했습니다. 검색 수요·노출·입찰의 어느 요인 때문인지는 미확인입니다.",
            "일반검색 키워드 노출수/클릭수, 품절/랜딩 품질, 예산·입찰 변동 확인"))
    if crm:
        x0,x1=crm["previous"],crm["current"]
        out["weaknesses"].append(card(
            "crm","media","CRM·Owned 구매 유입 약세",
            crm["delta_krw"],"동일 행사일 카카오·LMS·EDM 귀속 매출 증감 (원)",
            f"세션 {x0['sessions']:,} → {x1['sessions']:,}; 구매 {x0['orders']} → {x1['orders']}; CVR {x0['cvr_pct']:.2f}% → {x1['cvr_pct']:.2f}%",
            "유입·구매건수가 소폭 감소했습니다. 전년/올해 UTM 명명체계가 달라 캠페인 태그 한 종류만으로 CRM 효과를 판단하지 않습니다.",
            "LMS·EDM·카카오 발송수/수신모수, 미구매자/기존 구매자, 발송 시점별 세션 확인"))
    if bd:
        x=bd["current"]
        out["traffic_watch"]={"title":"META_BD 대량 유입·구매 분리",
             "sessions":x["sessions"],"orders":x["orders"],"revenue":x["revenue"],
             "cvr_pct":x["cvr_pct"],
             "note":"인지·트래픽 목적 태그는 판매 목적 Meta 배너와 분리. 인지도 목적의 실패를 구매 지표만으로 판정하지 않음.",
             "verify_next":"노출·도달·CPM·빈도·랜딩 이후 전환 경로"}
    return out

def build(ops,source,utm,intel,previous=None):
    previous=previous or {}
    integration=((intel.get("integrations") or {}).get("paid_cost") or {})
    paid={"status":integration.get("status","unavailable"),
          "latest_date":integration.get("latest_date"),
          "roas_eligible":integration.get("roas_eligible") is True and
            integration.get("latest_date")==ops.get("report_date"),
          "source":integration.get("source"),
          "note":"No verified daily Meta/Google/Naver ad spend for event dates. Do not infer ROAS or CPA."}
    e=event_days(ops,previous)
    w=weekly_channels(source,ops)
    u=media_event(utm,previous)
    diag=diagnose_drivers(e,u,paid)
    now=dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds")
    return {"schema":2,"generated_at_kst":now,"report_date":ops.get("report_date"),
            "event":e,"weekly_media":w,"event_media":u,"diagnosis":diag,
            "owned_utm":owned_comparison(utm),"paid_cost":paid,
            "updated_from":["reports/marketing_operations/latest.json",
                            "reports/decision_health/source_medium_weekly.json",
                            "reports/decision_health/winter_festival_utm_owned_2026.json",
                            "reports/marketing_intelligence_v2/latest.json"],
            "quality":{"reliable_event_days":e.get("days",0),
                       "event_media_measured":u.get("status")=="ga4_measured",
                       "weekly_media_date_aligned":w["date_aligned_with_daily_kpi"],
                       "roas_eligible":paid["roas_eligible"],
                       "caution":"GA4 attribution is descriptive, not causal. Media costs are separately verified. No ad-platform campaign spend is implied."}}


# Read the canonical daily GA4 aggregates directly so an earlier Marketing Operations
# publication time cannot block yesterday's festival KPI. Strict same-weekday YoY.
FESTIVAL_START=dt.date(2026,11,20)
FESTIVAL_DAILY=BASE/"daily_digest/data/daily"

def daily_festival_event(previous_event=None, daily_root=FESTIVAL_DAILY):
    """Rebuild the complete event D1..Dn only from matched current/last-year daily rows."""
    rows=[]
    kst=dt.timezone(dt.timedelta(hours=9))
    yesterday=dt.datetime.now(kst).date()-dt.timedelta(days=1)
    for offset in range(45):
        day=FESTIVAL_START+dt.timedelta(days=offset)
        if day>yesterday:break
        ly=day-dt.timedelta(days=364)
        d=read(daily_root/(day.isoformat()+".json"))
        current=(d.get("overall") or {}).get("current") or {}
        previous=(d.get("overall") or {}).get("yoy") or {}
        anchor=d.get("yoy") or {}
        if (d.get("end_date")!=day.isoformat()
            or anchor.get("start")!=ly.isoformat() or anchor.get("end")!=ly.isoformat()
            or current.get("source")!="ga4_bigquery_session_key_v1"
            or previous.get("source")!="ga4_bigquery_session_key_v1"):
            break
        curr=metrics({"sessions":current.get("sessions"),
                      "transactions":current.get("transactions"),
                      "revenue":current.get("purchaseRevenue")})
        prev=metrics({"sessions":previous.get("sessions"),
                      "transactions":previous.get("transactions"),
                      "revenue":previous.get("purchaseRevenue")})
        kpi=d.get("kpis") or {}
        if (not curr or not prev or prev["sessions"]<=0
            or curr["orders"]>curr["sessions"]
            or prev["orders"]>prev["sessions"]
            or any(not valid(kpi.get(x)) or abs(kpi[x]-curr[y])>1
                   for x,y in (("sessions","sessions"),("orders","orders"),("revenue","revenue")))):
            break
        partitions=d.get("traffic_partition_status") or {}
        rows.append({"day":offset+1,"current_date":day.isoformat(),
                     "previous_date":ly.isoformat(),"current":curr,"previous":prev,
                     "data_finality":"provisional_intraday" if partitions.get(day.isoformat())=="intraday" else "daily_partition"})
    if len(rows)<2:return None
    prior=previous_event or {}
    if prior.get("status")=="verified_ga4" and prior.get("days",0)>len(rows):
        return None  # Never regress to a shorter verified event report.
    totals={}
    for key in ("current","previous"):
        raw={name:sum(row[key][name] for row in rows)
             for name in ("sessions","orders","revenue")}
        totals[key]=metrics(raw)
    return {"status":"verified_ga4","days":len(rows),"daily":rows,"totals":totals,
            "comparison":"same_event_day_same_weekday_364_offset",
            "source_report_date":rows[-1]["current_date"],
            "source":"daily_digest_ga4_bigquery_session_key_v1",
            "last_day_finality":rows[-1]["data_finality"],
            "causal":False}

def extend_festival_from_daily(report, old=None):
    """Override the fixed D3 ops window only when complete daily proof exists."""
    measured=daily_festival_event((old or {}).get("event"))
    if not measured:return report
    current=report.get("event") or {}
    if measured["days"]<current.get("days",0):return report
    report["event"]=measured
    report["report_date"]=max(str(report.get("report_date") or ""),measured["source_report_date"])
    report["diagnosis"]=diagnose_drivers(measured,report["event_media"],report["paid_cost"])
    report["quality"]["reliable_event_days"]=measured["days"]
    report["quality"]["source_report_date"]=measured["source_report_date"]
    report["quality"]["latest_day_finality"]=measured["last_day_finality"]
    report["quality"]["marketing_ops_date"]=report["weekly_media"].get("report_date")
    report["quality"]["weekly_media_date_aligned"]=(report["weekly_media"].get("report_date")==report["report_date"])
    report["updated_from"].append("reports/daily_digest/data/daily/YYYY-MM-DD.json")
    return report

def main():
    current=extend_festival_from_daily(build(read(OPS),read(SOURCES),read(UTM),read(INTEL),read(OUT)),read(OUT))
    OUT.parent.mkdir(parents=True,exist_ok=True)
    serialized=json.dumps(current,ensure_ascii=False,allow_nan=False,indent=2)+"\n"
    # Avoid unnecessary commits if only the clock changed and source data did not.
    prior=read(OUT)
    prior.pop("generated_at_kst",None)
    comparable=dict(current)
    comparable.pop("generated_at_kst",None)
    if prior==comparable:
        print("[FESTIVAL] unchanged",current["report_date"])
        return
    OUT.write_text(serialized,encoding="utf-8")
    print("[FESTIVAL] event D",current["event"]["days"],
          "media",current["event_media"]["status"],
          "weekly",current["weekly_media"]["report_date"],
          "roas_eligible",current["paid_cost"]["roas_eligible"])

if __name__=="__main__":
    main()
