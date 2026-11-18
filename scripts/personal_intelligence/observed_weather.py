"""KMA ASOS Seoul station 108 daily observations; never substitutes forecasts."""
import csv,os,re,json,sys
from datetime import date,timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen
API="https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php"
def parse(raw,start,end):
    fields=None;result={}
    for line in raw.splitlines():
        line=line.strip()
        if not line:continue
        chunks=re.split(r"[\s,]+",line.lstrip("#").strip())
        if "TM" in chunks and "STN" in chunks and "TA_AVG" in chunks:
            fields=chunks;continue
        if line.startswith("#") or not fields or not re.fullmatch(r"\d{8}",chunks[0]) or len(chunks)!=len(fields):continue
        try:
            r=dict(zip(fields,chunks));dt=date.fromisoformat(r["TM"][:4]+"-"+r["TM"][4:6]+"-"+r["TM"][6:8])
            if r["STN"]!="108" or not start<=dt<=end:continue
            mean,lo,hi=(float(r[k]) for k in ("TA_AVG","TA_MIN","TA_MAX"))
            if not -60<=lo<=mean<=hi<=60:continue
            rain=float(r.get("RN_DAY","-99"))
            result[dt.isoformat()]={"mean_temp":mean,"min_temp":lo,"max_temp":hi,"rain_mm":rain if 0<=rain<=1000 else None,"station":"108","kind":"observed","verified":True,"source":"KMA ASOS"}
        except (ValueError,KeyError):continue
    return result
def fetch(start,end):
    key=os.environ.get("DAILY_DIGEST_KMA_SERVICE_KEY","").strip()
    if not key:return {}
    query=urlencode({"tm1":start.strftime("%Y%m%d"),"tm2":end.strftime("%Y%m%d"),"stn":"108","help":"1","authKey":key})
    try:
        with urlopen(API+"?"+query,timeout=25) as r:
            raw=r.read(500000).decode("utf-8",errors="replace")
        return parse(raw,start,end)
    except Exception:
        # Never print a URL containing credentials.
        return {}
def main():
    end=date.fromisoformat(sys.argv[1]);start=end-timedelta(days=13)
    out=fetch(start,end);p=Path(sys.argv[2]);p.write_text(json.dumps(out,ensure_ascii=False),encoding="utf-8")
    print("KMA observed days:",len(out),"/14")
if __name__=="__main__":main()
