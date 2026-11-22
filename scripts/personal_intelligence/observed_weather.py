"""KMA ASOS Seoul station 108 daily observations; never substitutes forecasts."""
import csv,os,re,json,sys
from urllib.error import HTTPError,URLError
from datetime import date,timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen
API="https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php"
LAST_STATUS="not_requested"
BLOCKED_HTTP=None
BLOCKED_NETWORK=False
def parse(raw,start,end):
    fields=None;result={}
    for line in raw.splitlines():
        line=line.strip()
        if not line:continue
        body=line.lstrip("#").strip()
        chunks=[v.strip() for v in next(csv.reader([body]))] if "," in body else body.split()
        while chunks and not chunks[-1]:chunks.pop()
        if "TM" in chunks and "STN" in chunks and "TA_AVG" in chunks:
            fields=chunks;continue
        if line.startswith("#") or not fields or not re.fullmatch(r"\d{8}",chunks[0]) or len(chunks)!=len(fields):continue
        try:
            r=dict(zip(fields,chunks));dt=date.fromisoformat(r["TM"][:4]+"-"+r["TM"][4:6]+"-"+r["TM"][6:8])
            if r["STN"]!="108" or not start<=dt<=end:continue
            mean,lo,hi=(float(r[k]) for k in ("TA_AVG","TA_MIN","TA_MAX"))
            if not -60<=lo<=mean<=hi<=60:continue
            try:rain=float(r.get("RN_DAY","-99"))
            except (ValueError,TypeError):rain=-99
            result[dt.isoformat()]={"mean_temp":mean,"min_temp":lo,"max_temp":hi,"rain_mm":rain if 0<=rain<=1000 else None,"station":"108","kind":"observed","verified":True,"source":"KMA ASOS"}
        except (ValueError,KeyError):continue
    return result
def _request(api,params,key,start,end):
    global BLOCKED_HTTP,BLOCKED_NETWORK,LAST_STATUS
    if BLOCKED_HTTP in (403,429) or BLOCKED_NETWORK:return {}
    query=urlencode({**params,"stn":"108","help":"1","authKey":key})
    try:
        with urlopen(api+"?"+query,timeout=8) as r:
            raw=r.read(500000).decode("utf-8-sig",errors="replace")
        result=parse(raw,start,end)
        if result:LAST_STATUS="observed"
        elif LAST_STATUS!="observed":LAST_STATUS="empty_response"
        if not result:print("KMA ASOS: no valid observation rows")
        return result
    except HTTPError as exc:
        if exc.code in (403,429):
            BLOCKED_HTTP=exc.code
            LAST_STATUS="access_denied_403" if exc.code==403 else "rate_limited_429"
        print("KMA ASOS HTTP status:",exc.code)
    except URLError:
        BLOCKED_NETWORK=True
        if LAST_STATUS!="observed": LAST_STATUS="request_error_URLError"
        print("KMA ASOS: transport error; stop repeated per-day requests")
    except Exception as exc:
        BLOCKED_NETWORK=True
        if LAST_STATUS not in ("observed","access_denied_403","rate_limited_429"):
            LAST_STATUS="request_error_"+type(exc).__name__
        print("KMA ASOS request error:",type(exc).__name__)
    return {}


NOAA_GSOD="https://www.ncei.noaa.gov/data/global-summary-of-the-day/access"
NOAA_SEOUL="47108099999"

def parse_noaa(raw,start,end):
    """NOAA daily station observations; Fahrenheit values, not KMA daily TA_AVG."""
    out={}
    for r in csv.DictReader(raw.splitlines()):
        if r.get("STATION")!=NOAA_SEOUL:continue
        try:
            day=date.fromisoformat(r["DATE"])
            if not start<=day<=end:continue
            value=float(r["TEMP"])
            n=float(r.get("TEMP_ATTRIBUTES") or 0)
            mean=(value-32)*5/9
            if n<4 or not -55<=mean<=55:continue
            mn=mx=None
            for name in ("MIN","MAX"):
                v=r.get(name)
                if v:
                    x=(float(v)-32)*5/9
                    if -55<=x<=55:
                        if name=="MIN":mn=round(x,1)
                        else:mx=round(x,1)
            out[day.isoformat()]={
                "mean_temp":round(mean,1),"min_temp":mn,"max_temp":mx,
                "station":NOAA_SEOUL,"kind":"observed","verified":True,
                "source":"NOAA GSOD Seoul WMO 47108",
                "note":"NOAA daily synoptic summary; aggregation may differ from KMA local-day ASOS"
            }
        except (ValueError,KeyError,TypeError):continue
    return out

def fetch_noaa(start,end):
    """Public NOAA GSOD fallback; one CSV per year, no API key or inferred temperatures."""
    result={}
    for year in range(start.year,end.year+1):
        api=f"{NOAA_GSOD}/{year}/{NOAA_SEOUL}.csv"
        try:
            req=__import__("urllib.request",fromlist=["Request"]).Request(api,headers={"User-Agent":"CSK-Ecommerce-Weather/1.0"})
            with urlopen(req,timeout=12) as r:
                raw=r.read(1500000).decode("utf-8-sig",errors="replace")
            result.update(parse_noaa(raw,start,end))
        except (HTTPError,URLError,ValueError,TimeoutError,OSError) as exc:
            print("NOAA GSOD retrieval unavailable:",year,type(exc).__name__)
    return result

def fetch(start,end):
    global LAST_STATUS,BLOCKED_HTTP,BLOCKED_NETWORK
    LAST_STATUS="not_requested"
    BLOCKED_HTTP=None
    BLOCKED_NETWORK=False
    if end<start:return {}
    key=os.environ.get("DAILY_DIGEST_KMA_SERVICE_KEY","").strip()
    result={}
    if key:
        result=_request(API,{"tm1":start.strftime("%Y%m%d"),"tm2":end.strftime("%Y%m%d"),
                             "obs":"TA","mode":"0"},key,start,end)
        # A transport failure applies to the endpoint, not each of 14 days.
        if BLOCKED_HTTP not in (403,429) and not BLOCKED_NETWORK:
            day=start
            while day<=end:
                if BLOCKED_HTTP in (403,429) or BLOCKED_NETWORK:break
                if day.isoformat() not in result:
                    result.update(_request(API.replace("kma_sfcdd3.php","kma_sfcdd.php"),
                        {"tm":day.strftime("%Y%m%d")},key,day,day))
                day+=timedelta(days=1)
    else:
        LAST_STATUS="credential_missing"
    kma_count=len(result)
    if len(result)<(end-start).days+1:
        backup=fetch_noaa(start,end)
        for day,val in backup.items():
            result.setdefault(day,val)
        if len(result)>kma_count:
            LAST_STATUS="mixed_kma_noaa" if kma_count else "noaa_gsod_fallback"
    if not result and LAST_STATUS=="not_requested":LAST_STATUS="no_data"
    print("Seoul daily observations:",len(result),"/",(end-start).days+1,
          "kma",kma_count,"noaa",len(result)-kma_count,"status",LAST_STATUS)
    return result

def main():
    end=date.fromisoformat(sys.argv[1]);start=end-timedelta(days=13)
    out=fetch(start,end);p=Path(sys.argv[2]);p.write_text(json.dumps(out,ensure_ascii=False),encoding="utf-8")
    print("KMA observed days:",len(out),"/14")
if __name__=="__main__":main()
