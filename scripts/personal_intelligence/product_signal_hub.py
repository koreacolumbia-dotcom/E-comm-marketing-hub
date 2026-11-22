"""Product-first evidence joins. No causal labels, no top-N absence treated as zero."""
from datetime import date,timedelta
from math import isfinite
from statistics import median
import re

def namekey(s):return " ".join(str(s or "").replace("’","'").replace("`","'").split()).casefold()
def number(v):return float(v) if isinstance(v,(int,float)) and not isinstance(v,bool) and isfinite(v) and v>=0 else None

def official_product_image(row):
    """One actual image/color from canonical PDP scope, never an inferred SKU image."""
    url=str((row or {}).get("image_url") or "").strip()
    if re.fullmatch(r"https://assetscdn\d+\.styleship\.com/assets/columbia/data/productimages/a/[124]/[A-Za-z0-9_-]+\.jpg",url,re.I):
        return url
    return None
def idx(rows,field="itemName"):
    out={};dups=set()
    for r in rows or []:
        k=namekey(r.get(field))
        if not k:continue
        if k in out:dups.add(k)
        out[k]=r
    return {k:v for k,v in out.items() if k not in dups}
def family(s):
    s=str(s).casefold()
    for k,p in (("다운",r"down|다운"),("플리스",r"fleece|플리스|후리스|헬베시아|helvetia"),("백팩",r"backpack|rucksack|백팩|배낭"),("신발",r"footwear|shoes|boots?|waterproof|yellowtail|snowtrot|등산화|트레킹화|신발"),("아우터",r"jacket|아우터|재킷|자켓|패딩|바람막이|outer")):
        if re.search(p,s):return k
    return "기타"
def change(a,b):return round((a/b-1)*100,1) if a is not None and b is not None and b>0 else None
def valid_temp(r):
    if not isinstance(r,dict) or r.get("verified") is not True or r.get("kind")!="observed":return None
    if str(r.get("station"))!="108" and not (str(r.get("station"))=="47108099999" and r.get("source")=="NOAA GSOD Seoul WMO 47108"):return None
    v=r.get("mean_temp");return float(v) if isinstance(v,(int,float)) and isfinite(v) and -60<=v<=60 else None


def assess_weather_association(temperatures, quantities, source=None):
    """Conservative 14-day descriptive weather/sales screen, never a causal estimate.

    Requires complete, actual GA4 item units on the same calendar dates as observed
    temperatures. A missing bestseller entry is NOT a zero-sale observation.
    Cold/warm buckets are descriptive, not an experimental control group.
    """
    result={"status":"insufficient","label":"판단 불가","correlation":None,
            "paired_days":0,"cold_days":0,"warm_days":0,"cold_avg_units":None,
            "warm_avg_units":None,"total_units":None,"sales_source":source,
            "method":"14일 실측 기온과 GA4 상품 일별 판매수량의 피어슨 상관관계",
            "causal":False,"reason":"기온·일별 판매 원본이 충분하지 않음"}
    if source!="ga4_purchase_items" or not isinstance(temperatures,list) or not isinstance(quantities,list) or len(temperatures)!=14 or len(quantities)!=14:
        return result
    pairs=[(float(t),float(q)) for t,q in zip(temperatures,quantities)
           if isinstance(t,(int,float)) and not isinstance(t,bool) and isfinite(t)
           and isinstance(q,(int,float)) and not isinstance(q,bool) and isfinite(q) and q>=0]
    result["paired_days"]=len(pairs)
    result["total_units"]=round(sum(q for _,q in pairs),1)
    cold=[q for t,q in pairs if t<=5]
    warm=[q for t,q in pairs if t>5]
    result["cold_days"]=len(cold);result["warm_days"]=len(warm)
    result["cold_avg_units"]=round(sum(cold)/len(cold),2) if cold else None
    result["warm_avg_units"]=round(sum(warm)/len(warm),2) if warm else None
    if len(pairs)<12 or len(cold)<3 or len(warm)<5 or (max(t for t,_ in pairs)-min(t for t,_ in pairs))<6:
        result["reason"]="관측일·저온/비저온 비교일 부족"
        return result
    if sum(q>0 for _,q in pairs)<6 or result["total_units"]<10:
        result["reason"]="일별 판매 표본 부족 (14일 판매 10개 미만 또는 판매 발생일 6일 미만)"
        return result
    mt=sum(t for t,_ in pairs)/len(pairs);mq=sum(q for _,q in pairs)/len(pairs)
    cov=sum((t-mt)*(q-mq) for t,q in pairs)
    tt=sum((t-mt)**2 for t,_ in pairs)
    qq=sum((q-mq)**2 for _,q in pairs)
    if tt<=0 or qq<=0:
        result["reason"]="기온 또는 판매량 변동이 없어 상관 비교 불가"
        return result
    corr=cov/(tt*qq)**0.5
    result["correlation"]=round(corr,2)
    cold_avg=result["cold_avg_units"];warm_avg=result["warm_avg_units"]
    if corr<=-0.6 and cold_avg>=1.35*warm_avg and cold_avg-warm_avg>=0.5:
        result.update(status="signal",label="날씨 연관 신호 있음",
                      reason="추운 날 판매가 높은 패턴 관측 · 행사/재고 등 다른 요인 통제 전")
    else:
        result.update(status="weak",label="뚜렷한 연관 신호 없음",
                      reason="최근 14일에 설정된 강한 역상관·저온 판매 증가 기준 미충족")
    return result

def build_signals(d,history=(),events=None,discounts=None,weather=None,limit=3):
    end=date.fromisoformat(d["end_date"]); day=end.isoformat()
    dates=[(end-timedelta(days=i)).isoformat() for i in range(13,-1,-1)]
    scopes=d.get("product_scopes") or {};current=idx(scopes.get("current_name"));previous=idx(scopes.get("previous_name"))
    docs={x.get("end_date"):x for x in history if isinstance(x,dict)};docs[day]=d
    sales={dt:idx((docs.get(dt,{}).get("product_scopes") or {}).get("best_name")) for dt in dates}
    # The producer's current_name / previous_name are 7-day ranked PDP samples.
    cards={};groups={}
    def card(k,r):
        if k not in cards:cards[k]={"name":r.get("itemName"),"family":family(r.get("itemName")),"signals":[],"causal":False}
        return cards[k]
    for k,r in current.items():
        a=number(r.get("views"));b=number(previous.get(k,{}).get("views"))
        if a is None or b is None:continue
        delta=a-b; pct=change(a,b)
        if abs(delta)>=100 and pct is not None and abs(pct)>=20:
            lab="pdp_up" if delta>0 else "pdp_down"
            c=card(k,r);c.update(pdp_7d=a,pdp_prior_7d=b,pdp_change_pct=pct,pdp_delta=delta)
            groups.setdefault(lab,[]).append((abs(delta),k))
    # Selected name-based bestseller rows: prior days only when same name actually appeared.
    for k,r in sales[day].items():
        a=number(r.get("qty"));earlier=[number(sales[dt].get(k,{}).get("qty")) for dt in dates[-8:-1]]
        if a is None or any(v is None for v in earlier):continue
        b=median(earlier);pct=change(a,b);delta=a-b
        if abs(delta)>=2 and (b==0 or pct is not None and abs(pct)>=30):
            c=card(k,r);c.update(sales_day=a,sales_baseline=b,sales_change_pct=pct,sales_delta=delta)
            groups.setdefault("sales_up" if delta>0 else "sales_down",[]).append((abs(delta),k))
    selected={}
    for label, arr in groups.items():
        for _,k in sorted(arr,key=lambda x:(-x[0],x[1]))[:limit]:
            selected.setdefault(k,[]).append(label)
    # Include large matched movements even when none cross threshold: clearly review only.
    if not selected:
        for k,r in list(current.items())[:limit]:
            card(k,r);selected[k]=["review_only"]
    w=weather or {};temp=[valid_temp(w.get(dt)) for dt in dates]
    ct=temp[-7:];pt=temp[:7]
    sources=sorted({str(w[dt].get("source","KMA ASOS")) for dt in dates if valid_temp(w.get(dt)) is not None})
    weather_label=" / ".join(sources) if sources else "미확보"
    shift=round(sum(ct)/7-sum(pt)/7,1) if all(v is not None for v in temp) else None
    efeed=events or {};event_rows=efeed.get("events") or []
    df=discounts or {};prices=df.get("styles") or [];checked=df.get("checked_at")
    def related(c):
        family_name=c["family"]; found=[]
        for e in event_rows:
            if str(e.get("brand","")).casefold()=="columbia":continue
            title=str(e.get("title") or "");kind=e.get("date_status")
            if kind in ("official_period","first_party_landing_text") and e.get("source_url") and e.get("start_date") and e.get("end_date"):
                try:
                    a,b=date.fromisoformat(e["start_date"]),date.fromisoformat(e["end_date"])
                    overlap=(min(end,b)-max(end-timedelta(days=6),a)).days+1
                except (ValueError,TypeError):continue
                if overlap<=0:continue
            elif kind=="observed_only":
                overlap=len(set(e.get("observed_on") or [])&set(dates[-7:]))
                if not overlap:continue
            else:continue
            cat=family(title)
            # General brand event is contextual only; exclude explicitly unrelated categories.
            if cat!="기타" and cat!=family_name and not (cat=="아우터" and family_name in ("다운","플리스")):continue
            found.append({"brand":e.get("brand_name"),"title":title,"start":e.get("start_date"),"end":e.get("end_date"),"date_status":kind,"url":e.get("source_url"),"overlap_days":overlap,"match":"상품군 일치 후보" if cat!="기타" else "브랜드 행사 참고"})
        found.sort(key=lambda x:(x["match"]!="상품군 일치 후보",x["date_status"]=="observed_only",-x["overlap_days"]))
        return found[:5]
    out=[]
    for k,labs in selected.items():
        c=cards[k];c["signals"]=labs
        c["pdp_7d"]=number(current.get(k,{}).get("views"));c["pdp_prior_7d"]=number(previous.get(k,{}).get("views"))
        c["pdp_change_pct"]=change(c["pdp_7d"],c["pdp_prior_7d"])
        # Preferred displayed color = first observed current PDP SKU; fallback prior.
        # Official images are already attached to the canonical Daily Digest data.
        c["image_url"]=official_product_image(current.get(k)) or official_product_image(previous.get(k))
        c["sales_series"]=[number(sales[dt].get(k,{}).get("qty")) for dt in dates]
        c["sales_day"]=c["sales_series"][-1]
        c["temperature_change_c"]=shift
        c["weather_assessment"]="기온 하락·방한 상품 수요 동행 여부 확인" if shift is not None and shift<=-2 and c["family"] in ("다운","플리스","아우터") else "기온 변동과 상품 추이 비교" if shift is not None else "실측 기온 미확보"
        c["competitor_events"]=related(c)
        matches=[{"brand":p.get("brand"),"name":p.get("name"),"discount_pct":p.get("discount_pct_median"),"regular":p.get("regular_price_median"),"sale":p.get("sale_price_median"),"url":p.get("url")} for p in prices if p.get("brand")!="COLUMBIA" and c["family"]!="기타" and family(p.get("name"))==c["family"]]
        c["discount_samples"]=sorted(matches,key=lambda p:-(number(p["discount_pct"]) or 0))[:3]
        c["discount_checked_at"]=checked
        c["interpretation"]="동일 상품군 행사·할인 후보와 시기 확인; 직접 영향은 미검증"
        out.append(c)
    # Independent daily SKU sample, never passed off as style totals.
    skus=[]
    for r in d.get("best_sellers") or []:
        k=namekey(r.get("itemId"))
        if not k:continue
        ser=idx((d.get("best_sellers_series") or {}).get("items"),"itemId").get(k,{})
        a=ser.get("ys") or []; qty=number(r.get("qty"))
        expected=[(end-timedelta(days=i)).strftime("%m/%d") for i in range(6,-1,-1)]
        if (d.get("best_sellers_series") or {}).get("x")!=expected or len(a)!=7 or any(number(v) is None for v in a) or qty!=a[-1]:continue
        b=median(a[:-1]);delta=qty-b
        if abs(delta)>=2 and (b==0 or abs(delta)/b>=.3):
            skus.append({"name":r.get("itemName"),"sku":r.get("itemId"),"qty":qty,"baseline":b,"delta":delta,"series":a,"scope":"개별 SKU 상위 판매 표본"})
    skus.sort(key=lambda x:-abs(x["delta"]))
    return {"date":day,"status":"partial","cards":out,"daily_sku_alerts":skus[:6],"dates":dates,"temperature":temp,"weather_source":weather_label,"temperature_change_c":shift,"weather_days":sum(v is not None for v in temp),"matched_pdp_names":len(set(current)&set(previous)),"calendar_as_of":efeed.get("as_of"),"note":"상품명 PDP는 최근 7일 대 직전 7일 상위 표본 비교입니다. 판매는 동일 상품명이 두 기간에 실제 등장한 날만 사용하며, 미등장=0이 아닙니다. 할인 후보는 수집일 가격으로 과거 적용 여부 미확인. 행사 동시 발생과 기온 변화만으로 인과관계를 확정하지 않습니다."}
