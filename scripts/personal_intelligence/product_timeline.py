"""Shared date-axis product and market timeline, from existing aggregate snapshots."""
from datetime import date,timedelta
from collections import defaultdict
from xml.etree import ElementTree
import re
from product_signal_hub import namekey,number,idx,valid_temp

BRANDS={"tnf":"노스페이스","k2":"K2","discovery":"디스커버리","blackyak":"블랙야크","nepa":"네파","kolonsport":"코오롱스포츠","eider":"아이더","national_geographic":"내셔널지오그래픽","snowpeak":"스노우피크","millet":"밀레","salomon":"살로몬","arcteryx":"아크테릭스","columbia":"컬럼비아"}
def days(end,n):return [(end-timedelta(days=i)).isoformat() for i in range(n-1,-1,-1)]
def points(row,dates,metric,stroke="#0055a5"):
    # Read exact point labels, never reconstruct numbers from rendered pixel coordinates.
    raw=row.get("trend_svg")
    if not isinstance(raw,str) or len(raw)>40000 or "<!" in raw:return None
    try:root=ElementTree.fromstring(raw)
    except ElementTree.ParseError:return None
    labels=[]
    unit="PDP 조회수" if metric=="views" else "판매수량"
    for node in root.iter():
        if node.tag.split("}")[-1]!="circle" or node.get("fill")!=stroke:continue
        title=next((x.text for x in node if x.tag.split("}")[-1]=="title"),None)
        m=re.fullmatch(r"(\d{2}/\d{2}): ([\d,]+) "+unit,title or "")
        if not m:return None
        labels.append((m[1],int(m[2].replace(",",""))))
    if [a for a,b in labels]!=[x[5:].replace("-","/") for x in dates]:return None
    return [b for a,b in labels]

def build_timeline(d,history,calendar,weather,focus):
    end=date.fromisoformat(d["end_date"]); dates=days(end,14); data=defaultdict(lambda:{"pdp":{},"sales":{}})
    names={};docs={x.get("end_date"):x for x in history if isinstance(x,dict)};docs[d["end_date"]]=d
    for dt,doc in sorted((k,v) for k,v in docs.items() if k and k<=d["end_date"]):
        if (doc.get("meta") or {}).get("cur_end")!=dt:continue
        e=date.fromisoformat(dt);cur=days(e,7);prev=days(e-timedelta(days=7),7)
        s=doc.get("product_scopes") or {}; prior=idx(s.get("previous_name"))
        for k,r in idx(s.get("current_name")).items():
            names[k]=r["itemName"]; a=points(r,cur,"views")
            if a is not None and sum(a)==number(r.get("views")):data[k]["pdp"].update(zip(cur,a))
            b=points(r,prev,"views","#64748b")
            if b is not None and sum(b)==number(prior.get(k,{}).get("views")):
                for kdate,v in zip(prev,b):data[k]["pdp"].setdefault(kdate,v)
        for k,r in idx(s.get("best_name")).items():
            names[k]=r["itemName"]; a=points(r,cur,"qty")
            if a is not None and a[-1]==number(r.get("qty")):data[k]["sales"].update(zip(cur,a))
            elif number(r.get("qty")) is not None:data[k]["sales"][dt]=number(r["qty"])
    # Prefer fully-scanned GA4 purchase items over Top-N snippet inference when
    # the same dated product card has a verified 14-day series.
    if focus.get("ga4_purchase_status")=="ga4_measured":
        for card in focus.get("cards",[]):
            if card.get("sales_source")!="ga4_purchase_items":continue
            series=card.get("sales_series")
            if not isinstance(series,list) or len(series)!=14:continue
            if any(number(v) is None for v in series):continue
            key=namekey(card.get("name"))
            if not key:continue
            names[key]=card.get("name")
            data[key]["sales"].update(zip(dates,series))
    selected=[namekey(r["name"]) for r in focus.get("cards",[])]
    # Rank additional complete same-weekday pairs on observed absolute movement.
    ranking={"pdp_up":[],"pdp_down":[],"sales_up":[],"sales_down":[]}
    for k,metrics in data.items():
        for metric,vals in metrics.items():
            a,b=vals.get(dates[-1]),vals.get((end-timedelta(days=7)).isoformat())
            floor=50 if metric=="pdp" else 2
            if a is None or b is None or abs(a-b)<floor or (b>0 and abs(a-b)/b<.3):continue
            ranking[metric+("_up" if a>b else "_down")].append((abs(a-b),k))
    for group in ranking.values():
        for _,k in sorted(group,reverse=True)[:3]:
            if k not in selected:selected.append(k)
    # If the names overlap across PDP and sales rankings, each metric gets a separate lane.
    rows=[]
    for k in selected:
        if k not in data:continue
        for metric,vals in data[k].items():
            values=[vals.get(t) for t in dates]
            if not any(v is not None for v in values):continue
            baseline=[vals.get((date.fromisoformat(t)-timedelta(days=7)).isoformat()) for t in dates]
            delta=[a-b if a is not None and b is not None else None for a,b in zip(values,baseline)]
            rows.append({"name":names[k],"key":k,"metric":metric,"values":values,"baseline":baseline,"delta":delta})
    event_rows=defaultdict(list);seen=set()
    for e in (calendar or {}).get("events",[]):
        k=str(e.get("brand") or "").lower()
        if not k:continue
        title=str(e.get("title") or "");status=e.get("date_status")
        if not title:continue
        if status in ("official_period","first_party_landing_text") and e.get("source_url"):
            try:a=date.fromisoformat(e["start_date"]);b=date.fromisoformat(e["end_date"])
            except (ValueError,KeyError,TypeError):continue
            active=[t for t in dates if a<=date.fromisoformat(t)<=b]
        elif status=="observed_only":active=sorted(set(dates)&set(e.get("observed_on") or []))
        elif status=="official_start_only" and e.get("source_url"):active=[t for t in dates if t==e.get("start_date")]
        else:continue
        if not active:continue
        key=(k,title,status,tuple(active))
        if key in seen:continue
        seen.add(key)
        event_rows[k].append({"title":title,"status":status,"dates":active,"start":e.get("start_date"),"end":e.get("end_date"),"url":e.get("source_url"),"benefit":e.get("benefit")})
    branded=[{"brand":BRANDS.get(k,k),"events":event_rows.get(k,[])} for k in list(BRANDS)+[k for k in event_rows if k not in BRANDS]]
    return {"dates":dates,"weather":[valid_temp((weather or {}).get(t)) for t in dates],
            "weather_sources":[((weather or {}).get(t) or {}).get("source") if valid_temp((weather or {}).get(t)) is not None else None for t in dates],
            "products":rows,"brands":branded,
            "note":"상품명 단위 일별 값은 원본 그래프의 정확한 데이터 라벨과 합계가 일치한 상위 표본입니다. 막대 증감은 7일 전 같은 상품과 비교한 수량입니다. 미수집은 빈칸. 행사 관측일과 공식 기간을 구분하며 동시 발생은 인과관계를 의미하지 않습니다."}
