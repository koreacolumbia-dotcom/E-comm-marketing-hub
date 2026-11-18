"""Read existing public-post caches and anonymized review aggregates, no new scraping."""
from collections import Counter,defaultdict
from datetime import date,datetime,timedelta
from pathlib import Path
from urllib.parse import urlsplit,urlunsplit,parse_qsl,urlencode
from zoneinfo import ZoneInfo
import json,re

ROOT=Path(__file__).resolve().parents[2]
SOURCES=("dcinside","eomisae","naver_cafe","ppomppu")
ALLOWED={"dcinside":("dcinside.com",),"eomisae":("eomisae.co.kr",),"naver_cafe":("cafe.naver.com",),"ppomppu":("ppomppu.co.kr",)}
BRAND=re.compile(r"컬럼비아|콜럼비아|\bcolumbia\b",re.I)
TOPICS={"price/promotion":r"할인|세일|특가|가성비|가격|쿠폰|득템|대란|저렴",
        "fit/size":r"사이즈|크게|작게|정사이즈|핏",
        "product/function":r"다운|패딩|바람막이|바막|플리스|후리스|방수|옴니|타이타늄|티타늄|등산화|신발|보온|하드쉘",
        "delivery/service":r"배송|교환|환불|취소|불량|as\b|수선",
        "usage/recommendation":r"추천|등산|트레킹|착용|코디|입기|산행"}
def day(v):
    try:
        dt=datetime.fromisoformat(str(v).replace("Z","+00:00"))
        return dt.astimezone(ZoneInfo("Asia/Seoul")).date() if dt.tzinfo else dt.date()
    except (ValueError,TypeError):return None
def read(p):
    try:return json.loads(p.read_text(encoding="utf-8"))
    except (OSError,ValueError):return None
def url(source,v):
    try:
        p=urlsplit(str(v))
        host=(p.hostname or "").lower()
        if p.scheme!="https" or p.username or p.password or not any(host==h or host.endswith("."+h) for h in ALLOWED[source]):return None
        q=[(k,v) for k,v in parse_qsl(p.query) if k.lower() in ("id","no","articleid","clubid","cafeid")]
        return urlunsplit((p.scheme,host,p.path,urlencode(sorted(q)),""))
    except ValueError:return None
def topics(s):
    return [k for k,p in TOPICS.items() if re.search(p,s,re.I)] or ["other"]
def build_social(report_day,root=ROOT):
    end=date.fromisoformat(str(report_day))
    start=end-timedelta(days=6)
    days=[start+timedelta(days=i) for i in range(7)]
    mentions=[];seen=set();sources=[];counts=Counter();daily=Counter()
    for source in SOURCES:
        data=read(root/"reports/external_signal_cache"/(source+".json"))
        if not isinstance(data,list):
            sources.append({"source":source,"status":"missing","observed_matches":None});continue
        observed=[];n=0
        for r in data:
            if not isinstance(r,dict):continue
            dt=day(r.get("created_at"))
            if dt and dt<=end:observed.append(dt)
            if not dt or not start<=dt<=end:continue
            u=url(source,r.get("url"))
            # Count only explicit brand text; query labels or comments alone do not establish a mention.
            t=str(r.get("title") or "")+" "+str(r.get("content") or "")
            if not u or not BRAND.search(t) or u in seen:continue
            seen.add(u);n+=1;daily[dt.isoformat()]+=1
            labels=topics(t);counts.update(labels)
            # No author, raw snippet, review text or identity fields in output.
            mentions.append({"source":source,"recorded_date":dt.isoformat(),"url":u,"topics":labels,
                             "date_quality":"cache_timestamp_unverified" if source=="naver_cafe" else "cache_recorded"})
        sources.append({"source":source,"status":"cached_sample" if observed else "empty_or_no_dated_rows",
                        "latest_recorded_date":max(observed).isoformat() if observed else None,"observed_matches":n if observed else None})
    for source in ("instagram","naver_blog","youtube"):
        sources.append({"source":source,"status":"not_connected","observed_matches":None})
    # Official product VOC stays separate from community mentions.
    health=read(root/"reports/crema_raw/health/latest_health.json") or {}
    ratings=Counter();review_topics=Counter();products=defaultdict(lambda:{"reviews":0,"low_rating":0,"size_mentions":0})
    review_seen=set();review_days=[];review_recorded=0
    for dt in days:
        doc=read(root/"reports/crema_raw/daily"/(dt.isoformat()+".json"))
        if not isinstance(doc,dict) or not isinstance(doc.get("reviews"),list):continue
        review_days.append(dt.isoformat())
        for r in doc["reviews"]:
            if not isinstance(r,dict) or r.get("source")!="Official" or not r.get("id") or day(r.get("created_at"))!=dt:continue
            k=str(r["id"])
            if k in review_seen:continue
            review_seen.add(k);review_recorded+=1
            rating=r.get("rating")
            if isinstance(rating,int) and not isinstance(rating,bool) and 1<=rating<=5:ratings[rating]+=1
            tags=r.get("tags") or []
            tagset=set(topics(str(r.get("text") or "")))
            if "size" in tags:tagset.add("fit/size")
            if "req" in tags:tagset.add("request")
            review_topics.update(tagset)
            name=str(r.get("product_name") or "").strip()
            if name:
                item=products[name];item["reviews"]+=1
                item["low_rating"]+=int(isinstance(rating,int) and 1<=rating<=2)
                item["size_mentions"]+=int("fit/size" in tagset)
    rated=sum(ratings.values())
    top=sorted([{"product":k,**v} for k,v in products.items() if v["reviews"]>=3],key=lambda x:(-x["low_rating"],-x["size_mentions"],-x["reviews"],x["product"]))[:10]
    return {"date":end.isoformat(),"period_start":start.isoformat(),"status":"partial" if any(s["status"]=="cached_sample" for s in sources) or review_days else "source_required",
            "observed_community_mentions":len(mentions) if any(s["status"]=="cached_sample" for s in sources) else None,
            "community_source_status":sources,"community_daily":[{"date":x.isoformat(),"observed_mentions":daily[x.isoformat()]} for x in days],
            "community_topics":[{"topic":k,"mentions":n} for k,n in counts.most_common()],
            "community_evidence":sorted(mentions,key=lambda x:x["recorded_date"],reverse=True)[:30],
            "voc":{"files_present_days":len(review_days),"days":review_days,"reviews":review_recorded if review_days else None,
                   "rated_reviews":rated,"average_rating":sum(k*v for k,v in ratings.items())/rated if rated else None,
                   "low_rating_count":ratings[1]+ratings[2] if rated else None,"rating_distribution":dict(ratings),
                   "topics":[{"topic":k,"mentions":n} for k,n in review_topics.most_common()],
                   "product_voc":top,"product_threshold":3,
                   "latest_collection_health":{k:health.get(k) for k in ("updated_at","start","end","incomplete","failed_chunks")}},
            "keyword_sentiment":None,"share_of_voice":None,"wow":None,
            "note":"Observed cache sample only; missing source is not zero. Naver cache timestamps may be retrieval time. Source completeness and prior-period coverage are not verified. Topic labels overlap; keyword tags are not sentiment. Reviews and social posts are separate."}
