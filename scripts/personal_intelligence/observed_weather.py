"""KMA ASOS Seoul station 108 daily observations; never substitutes forecasts."""
import csv,os,re,json,sys
from urllib.error import HTTPError
from datetime import date,timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen
API="https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php"
LAST_STATUS="not_requested"
BLOCKED_HTTP=None
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
    global BLOCKED_HTTP,LAST_STATUS
    if BLOCKED_HTTP in (403,429):return {}
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
    except Exception as exc:
        if LAST_STATUS not in ("observed","access_denied_403","rate_limited_429"):
            LAST_STATUS="request_error_"+type(exc).__name__
        print("KMA ASOS request error:",type(exc).__name__)
    return {}

def fetch(start,end):
    global LAST_STATUS,BLOCKED_HTTP
    LAST_STATUS="not_requested"
    BLOCKED_HTTP=None
    key=os.environ.get("DAILY_DIGEST_KMA_SERVICE_KEY","").strip()
    if not key:
        LAST_STATUS="credential_missing"
        print("KMA ASOS: credential not configured")
        return {}
    if end<start:return {}
    result=_request(API,{"tm1":start.strftime("%Y%m%d"),"tm2":end.strftime("%Y%m%d"),"obs":"TA","mode":"0"},key,start,end)
    # Official single-day endpoint is a separate API product; only fill missing days.
    if BLOCKED_HTTP in (403,429):
        print("KMA ASOS: access denied; stopped per-day retries")
        return result
    day=start
    while day<=end:
        if BLOCKED_HTTP in (403,429):break
        if day.isoformat() not in result:
            fallback=_request(API.replace("kma_sfcdd3.php","kma_sfcdd.php"),{"tm":day.strftime("%Y%m%d")},key,day,day)
            result.update(fallback)
        day+=timedelta(days=1)
    if not result and LAST_STATUS=="not_requested":LAST_STATUS="no_data"
    print("KMA ASOS observed coverage:",len(result),"/",(end-start).days+1,"status",LAST_STATUS)
    return result

def main():
    end=date.fromisoformat(sys.argv[1]);start=end-timedelta(days=13)
    out=fetch(start,end);p=Path(sys.argv[2]);p.write_text(json.dumps(out,ensure_ascii=False),encoding="utf-8")
    print("KMA observed days:",len(out),"/14")
if __name__=="__main__":main()
