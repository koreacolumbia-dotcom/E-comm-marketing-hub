#!/usr/bin/env python3
"""Evidence-backed competitor sale calendar from existing daily hero CSV snapshots.

Calendar semantics:
- verified_period: actual first-party/official retailer source explicitly gives dates
- verified_start: first-party source gives start only; never infer end
- observed_banner: on each day a sale-oriented banner was actually captured;
  capture dates are NOT a campaign's true start/end dates.
No fabricated discount periods, no personal data, no write to external websites.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlsplit

KST = dt.timezone(dt.timedelta(hours=9))
ROOT = Path("reports")
DEST = ROOT / "competitor_sale_calendar" / "latest.json"
REGISTRY = Path("config/competitor_sale_events_verified.json")
MAX_SNAPSHOT_DAYS = 65
SALE_PATTERN = re.compile(
    r"(?:세일|할인|특가|블랙\s*프라이데이|블랙\s*페스타|BLACK\s*(?:FRIDAY|FESTA)|"
    r"(?:WINTER|SUMMER|SEASON)\s*SALE|K[- ]?SALE|시즌\s*오프|"
    r"\bSALE\b|사은품\s*증정|쿠폰(?:팩)?\s*\d+%|\d+%\s*쿠폰|즉시\s*\d+\s*만\s*원\s*할인)",
    re.I
)
GENERIC = {"메인 배너","메인","image","main visual section","main banner"}
BRANDS = {
    "tnf": "The North Face", "blackyak":"Black Yak",
    "snowpeak":"Snow Peak", "discovery":"Discovery Expedition",
    "kolonsport":"Kolon Sport", "k2":"K2", "salomon":"Salomon",
    "nepa":"NEPA","millet":"MILLET","arcteryx":"Arc'teryx",
    "patagonia":"Patagonia","newbalance":"New Balance",
    "blackyak_kids":"Black Yak Kids","montbell":"Montbell",
    "eider":"Eider", "national_geographic":"National Geographic"
}
HOSTS = {
    "tnf": ("thenorthfacekorea.co.kr",),
    "blackyak":("byn.kr",),
    "snowpeak":("snowpeakstore.co.kr",),
    "discovery":("discovery-expedition.com","discovery-expedition.co.kr"),
    "kolonsport":("kolonsport.com","kolonsport.co.kr","kolonmall.com"),
    "k2":("k2.co.kr","k-village.co.kr"), "salomon":("salomon.co.kr",),
    "nepa":("nepa.co.kr","nplus.co.kr"), "millet":("millet.co.kr",),
    "arcteryx":("arcteryx.co.kr",),
    "patagonia":("patagonia.co.kr",),
    "newbalance":("nbkorea.com",), "eider":("eider.co.kr","k-village.co.kr"),"national_geographic":("naturestore.co.kr","nstationmall.com")
}
OBS_RE = re.compile(r"^hero_main_banners_(\d{8})_\d{6}\.csv$")
DATED_PERIOD = re.compile(
    r"(?:(?:20\d\d)\s*[년.]\s*)?(\d{1,2})\s*월\s*(\d{1,2})\s*일?\s*"
    r"(?:~|～|[-–—]|부터)\s*(?:(?:20\d\d)\s*[년.]\s*)?"
    r"(\d{1,2})\s*월\s*(\d{1,2})\s*일?"
)
DISCOUNT = re.compile(r"(?:최대|추가|단독)?\s*(\d{1,2}(?:\.\d+)?)\s*%\s*(?:할인|OFF|세일|SALE)", re.I)
DISCOUNT_ALT = re.compile(r"(?:할인|OFF)\s*(?:최대|추가)?\s*(\d{1,2}(?:\.\d+)?)\s*%", re.I)
WON = re.compile(r"(?:최대|즉시)\s*(\d{1,3})\s*만\s*원\s*(?:즉시\s*)?할인")


def date(value: object) -> dt.date | None:
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except (ValueError,TypeError):
        return None


def clean(value: object, limit: int = 160) -> str:
    text = re.sub(r"[\x00-\x1f]+"," ",str(value or "")).strip()
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}","[redacted]",text)
    return text[:limit]


def safe_url(value: str, brand: str | None = None, registry: bool = False) -> str | None:
    try:
        parts = urlsplit(str(value or ""))
        if parts.scheme != "https" or parts.username or parts.password or parts.port not in (None,443):
            return None
        host = (parts.hostname or "").lower()
        if not host or host == "localhost" or host.endswith(".local"):
            return None
        if registry:
            # Registry is curated from official retailers (SSF SHOP etc).
            allowed = ("ssfshop.com","kream.co.kr","snowpeakstore.co.kr",
                       "byn.kr","thenorthfacekorea.co.kr","nplus.co.kr","kolonmall.com",
                       "k-village.co.kr","nstationmall.com","discovery-expedition.com","11st.co.kr")
        else:
            allowed = HOSTS.get(brand or "",())
        if registry and host in ("nplus.co.kr","www.nplus.co.kr") and brand != "nepa":
            return None
        if registry and (host == "kolonmall.com" or host.endswith(".kolonmall.com")) and brand != "kolonsport":
            return None
        if registry:
            scoped = {"k-village.co.kr": {"k2","eider"}, "nstationmall.com": {"national_geographic"},
                      "discovery-expedition.com": {"discovery"}, "11st.co.kr": {"k2","eider"}}
            for domain, brands in scoped.items():
                if (host == domain or host.endswith("."+domain)) and brand not in brands:
                    return None
        if not any(host == d or host.endswith("."+d) for d in allowed):
            return None
        return parts.geturl()
    except (ValueError,TypeError):
        return None


def benefit(text: str) -> str | None:
    found = DISCOUNT.search(text) or DISCOUNT_ALT.search(text)
    if found:
        try:
            n = float(found.group(1))
            if 0 < n <= 95:
                return f"최대 {n:g}% 할인" if "최대" in text else f"{n:g}% 할인"
        except (ValueError,TypeError):
            pass
    m = WON.search(text)
    if m and int(m.group(1)) <= 100:
        return f"최대 {int(m.group(1))}만원 할인"
    return None


def extract_period(text: str, year: int) -> tuple[str,str] | None:
    match = DATED_PERIOD.search(text)
    if not match:
        return None
    a,b,c,d = map(int,match.groups())
    try:
        start=dt.date(year,a,b)
        end=dt.date(year,c,d)
        if end < start:
            end=dt.date(year+1,c,d)
        if not (0 <= (end-start).days <= 95):
            return None
        return start.isoformat(),end.isoformat()
    except ValueError:
        return None


def read_snapshots(root: Path, as_of: dt.date, lookback: int = MAX_SNAPSHOT_DAYS) -> tuple[list[dict],list[str]]:
    """Read the latest capture per day only. No interpolation for missing captures."""
    candidates={}
    for path in root.glob("hero_main_banners_*.csv"):
        m=OBS_RE.match(path.name)
        if not m:
            continue
        try:
            day=dt.datetime.strptime(m.group(1),"%Y%m%d").date()
        except ValueError:
            continue
        if not (0 <= (as_of-day).days <= lookback):
            continue
        if day not in candidates or path.name>candidates[day].name:
            candidates[day]=path
    rows=[]
    files=[]
    for day,path in sorted(candidates.items()):
        try:
            with path.open("r",encoding="utf-8-sig",newline="") as stream:
                rdr=csv.DictReader(stream)
                if not {"date","brand_key","title","href_clean"}.issubset(rdr.fieldnames or []):
                    continue
                for item in rdr:
                    if date(item.get("date")) != day:
                        continue
                    key=str(item.get("brand_key") or "").strip().lower()
                    if key not in BRANDS or key=="columbia":
                        continue
                    title=clean(item.get("title"),120)
                    landing=clean(item.get("landing_title"),170)
                    summary=clean(item.get("landing_summary"),420)
                    candidate = landing if title.lower() in GENERIC and landing else title
                    if not SALE_PATTERN.search(candidate) and not SALE_PATTERN.search(title):
                        continue
                    if re.search(r"(?:멤버십|첫구매|로그인|회원가입)",candidate) and not re.search(r"(?:블랙|페스타|세일|특가)",candidate):
                        continue
                    url = safe_url(item.get("href_clean") or item.get("href"),key)
                    if not url:
                        # Never send users to ad-tracking open redirects.
                        url = None
                    offer=benefit(" ".join([candidate,summary]))
                    rows.append({
                        "brand":key,"brand_name":BRANDS[key],"title":candidate,
                        "observed":day.isoformat(),"url":url,
                        "benefit":offer,"landing_summary":summary,
                        "landing_status":str(item.get("landing_text_status") or ""),
                        "fallback":str(item.get("is_fallback") or "")=="1"
                    })
            files.append(path.name)
        except (UnicodeError,OSError,csv.Error):
            continue
    return rows,files


def read_registry(path: Path, as_of: dt.date) -> list[dict]:
    try:
        data=json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeError,OSError,ValueError):
        return []
    raw=data.get("events") if isinstance(data,dict) else None
    if not isinstance(raw,list):
        return []
    events=[]
    for item in raw:
        if not isinstance(item,dict):
            continue
        brand=str(item.get("brand") or "").lower()
        start=date(item.get("start_date"))
        end=date(item.get("end_date")) if item.get("end_date") else None
        if (brand not in BRANDS or brand=="columbia" or not start or
            start>as_of+dt.timedelta(days=190) or start.year<2025 or
            (end is not None and (end<start or (end-start).days>160)) or
            item.get("verification") not in ("official_event_page","official_retailer_page")):
            continue
        link=safe_url(item.get("source_url"),brand,registry=True)
        if not link:
            continue
        title=clean(item.get("title"),140)
        if not title or not (SALE_PATTERN.search(title) or re.search(r"쿠폰팩|BLACK\s*WEEK|래플|얼리버드",title,re.I)):
            continue
        ident=clean(item.get("id"),90)
        if not re.fullmatch(r"[a-z0-9_-]{5,90}",ident):
            continue
        events.append({
            "id":ident,"brand":brand,"brand_name":BRANDS[brand],"title":title,
            "kind":"verified_period" if end else "verified_start",
            "start_date":start.isoformat(),"end_date":end.isoformat() if end else None,
            "date_status":"official_period" if end else "official_start_only",
            "observed_on":[],
            "benefit":clean(item.get("benefit"),60) or benefit(title),
            "source_url":link,"source_type":item["verification"],
            "channel":clean(item.get("channel") or "공식몰",55),
            "note":clean(item.get("note"),240)
        })
    return events


def observed_events(rows: list[dict]) -> list[dict]:
    groups:dict[tuple,dict]={}
    for row in rows:
        # Title+brand is stable even if ad tracking URLs change daily.
        key=(row["brand"],re.sub(r"\s+","",row["title"]).lower()[:130])
        if key not in groups:
            digest=hashlib.sha256("|".join(key).encode("utf-8")).hexdigest()[:12]
            groups[key]={
                "id":"observed-"+digest,"brand":row["brand"],"brand_name":row["brand_name"],
                "title":row["title"],"kind":"observed_banner","start_date":None,
                "end_date":None,"date_status":"observed_only","observed_on":[],
                "benefit":row["benefit"],"source_url":row["url"],"source_type":"hero_banner_csv",
                "channel":"공식몰 배너","note":"해당 수집일에 할인 문구가 포착됨. 실제 행사 시작/종료일은 확인되지 않음"
            }
        group=groups[key]
        if row["observed"] not in group["observed_on"]:
            group["observed_on"].append(row["observed"])
        if not group["source_url"] and row["url"]:
            group["source_url"]=row["url"]
        if not group["benefit"] and row["benefit"]:
            group["benefit"]=row["benefit"]
        # First-party landing text can override junk HTML plan_start/plan_end.
        if row["url"] and row["landing_status"]=="ok":
            text=" ".join([row["title"],row["landing_summary"]])
            # Requires an explicit sale-oriented landing-title and a tight
            # actual day-month range, not fallback captured image dates.
            period=extract_period(text,int(row["observed"][:4]))
            if period and (SALE_PATTERN.search(text) and not row["fallback"]):
                group["kind"]="verified_period"
                group["start_date"],group["end_date"]=period
                group["date_status"]="first_party_landing_text"
                group["source_type"]="first_party_landing_text"
                group["note"]="공식몰 랜딩 설명에 명시된 기간을 추출. 실제 종료/변경은 원문 재확인 필요"
    out=list(groups.values())
    for x in out:
        x["observed_on"].sort()
        x["first_seen"]=x["observed_on"][0]
        x["last_seen"]=x["observed_on"][-1]
    return out


def build(as_of:dt.date, root:Path=ROOT, registry:Path=REGISTRY, lookback:int=MAX_SNAPSHOT_DAYS) -> dict:
    rows,files=read_snapshots(root,as_of,lookback)
    documented=read_registry(registry,as_of)
    events=documented+observed_events(rows)
    events.sort(key=lambda x:(x["brand"],x["kind"],x.get("start_date") or x.get("first_seen") or "",x["title"]))
    captures=sorted({dt.datetime.strptime(re.match(OBS_RE, f).group(1),"%Y%m%d").date().isoformat()
                    for f in files if OBS_RE.match(f)})
    most_recent=captures[-1] if captures else None
    lag=(as_of-date(most_recent)).days if most_recent else None
    sale_observed=[x for x in events if x["kind"]=="observed_banner"]
    confirmed=[x for x in events if x["kind"]=="verified_period"]
    return {
        "schema_version":1,"as_of":as_of.isoformat(),
        "generated_at_kst":dt.datetime.now(KST).isoformat(timespec="seconds"),
        "status":"fresh" if lag is not None and lag<=1 else ("stale" if captures else "no_snapshots"),
        "freshness":{"last_snapshot":most_recent,"lag_days":lag,
                     "snapshot_days":len(captures),"lookback_days":lookback},
        "counts":{"events":len(events),"dated_periods":len(confirmed),
                  "observed_only":len(sale_observed),"verified_starts":sum(x["kind"]=="verified_start" for x in events)},
        "brands":sorted({x["brand"] for x in events}),
        "events":events,
        "provenance":{
            "snapshots":len(files),"registry_events":len(documented),
            "disclaimer":"관측일≠실제 세일 시작일. 종료일 확인이 안 된 세일은 미래일까지 칠하지 않습니다.",
            "discounts":"상품/브랜드별 할인율이 아닌 프로모션 홍보 문구만 반영할 수 있습니다.",
            "source_data":"reports/hero_main_banners_YYYYMMDD_HHMMSS.csv"
        }
    }


def save(data:dict, path:Path=DEST) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print("[SALE CALENDAR]",data["as_of"],data["status"],"events",data["counts"]["events"],
          "periods",data["counts"]["dated_periods"],"observed",data["counts"]["observed_only"])


def main() -> int:
    ap=argparse.ArgumentParser()
    ap.add_argument("--root",default=str(ROOT))
    ap.add_argument("--registry",default=str(REGISTRY))
    ap.add_argument("--out",default=str(DEST))
    ap.add_argument("--as-of",default=dt.datetime.now(KST).date().isoformat())
    ap.add_argument("--lookback",type=int,default=MAX_SNAPSHOT_DAYS)
    x=ap.parse_args()
    target=date(x.as_of)
    if target is None or not (1<=x.lookback<=180):
        ap.error("invalid dates")
    out=build(target,Path(x.root),Path(x.registry),x.lookback)
    save(out,Path(x.out))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
