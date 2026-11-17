"""Official Columbia price snapshot collector. Run with verified PDP URLs as input."""
import json,re,sys,time
from datetime import datetime,timezone,timedelta
from urllib.parse import urlparse
from urllib.request import Request,urlopen
from urllib.robotparser import RobotFileParser
from pathlib import Path

UA="CSKMarketResearchBot"
HOSTS={"www.columbiakorea.co.kr","columbiakorea.co.kr"}
def allowed_url(url):
    p=urlparse(url)
    return p.scheme=="https" and p.hostname in HOSTS and not p.username and not p.password
def fetch(url):
    if not allowed_url(url): raise ValueError("Not official PDP host")
    with urlopen(Request(url,headers={"User-Agent":UA}),timeout=20) as res:
        if not allowed_url(res.geturl()): raise ValueError("Unexpected redirect")
        return res.read(3000000).decode("utf-8",errors="replace")
def products(obj):
    if isinstance(obj,list):
        for v in obj: yield from products(v)
    elif isinstance(obj,dict):
        t=obj.get("@type",[])
        if t=="Product" or isinstance(t,list) and "Product" in t: yield obj
        for k in ("@graph","mainEntity"):
            if k in obj: yield from products(obj[k])
def extract(html,sku,url,day):
    for match in re.finditer(r'<script\b[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',html,re.I|re.S):
        try: obj=json.loads(match.group(1))
        except ValueError: continue
        for p in products(obj):
            if str(p.get("sku","")).strip().upper()!=sku.upper(): continue
            offers=p.get("offers")
            if isinstance(offers,list) and len(offers)==1: offers=offers[0]
            if not isinstance(offers,dict) or offers.get("priceCurrency")!="KRW": continue
            try: price=int(str(offers["price"]).replace(",",""))
            except (ValueError,KeyError,TypeError): continue
            if price<=0: continue
            return {"sku":sku,"date":day,"source_url":url,"sale_price":price,"regular_price":None,"coupon_price":None,"verified":True,"evidence":"exact SKU JSON-LD Offer KRW","image":p.get("image")}
    return {"sku":sku,"date":day,"source_url":url,"verified":False,"sale_price":None,"reason":"exact SKU offer not found"}
def collect(items):
    day=datetime.now(timezone(timedelta(hours=9))).date().isoformat()
    robots={}; rows=[]
    for item in items:
        url=item["url"];sku=item["sku"]
        try:
            if not allowed_url(url): raise ValueError("invalid official URL")
            host=urlparse(url).hostname
            if host not in robots:
                rp=RobotFileParser();rp.parse(fetch("https://"+host+"/robots.txt").splitlines());robots[host]=rp
            if not robots[host].can_fetch(UA,url): raise ValueError("robots disallowed")
            time.sleep(max(2,robots[host].crawl_delay(UA) or 0))
            rows.append(extract(fetch(url),sku,url,day))
        except Exception as exc:
            rows.append({"sku":sku,"date":day,"source_url":url,"verified":False,"sale_price":None,"reason":str(exc)})
    return rows
if __name__=="__main__":
    items=json.loads(Path(sys.argv[1]).read_text())
    rows=collect(items)
    out=Path(sys.argv[2]);out.parent.mkdir(parents=True,exist_ok=True)
    old=json.loads(out.read_text()) if out.exists() else []
    merged={(x["sku"],x["date"]):x for x in old}
    for x in rows:
        k=(x["sku"],x["date"])
        if x.get("verified") or not merged.get(k,{}).get("verified"): merged[k]=x
    out.write_text(json.dumps(list(merged.values()),ensure_ascii=False,indent=2))
