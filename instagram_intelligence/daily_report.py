"""Conservative daily report: unknown dates and unverified tags never become confirmed mentions."""
import argparse, datetime as dt, json, re
from collections import Counter
from pathlib import Path
from zoneinfo import ZoneInfo

KST=ZoneInfo("Asia/Seoul")
OFFICIAL={"columbia_kr","columbia_creator_crew"}
NEGATIVE=("불량","하자","환불","불만","실망","찢어","누수","고장")
POSITIVE=("추천","좋아요","만족","편해","따뜻","예뻐")

def parse_date(value):
    if not value or not isinstance(value,str): return None
    try:
        date=dt.datetime.fromisoformat(value.replace("Z","+00:00"))
        return date.astimezone(KST) if date.tzinfo else None
    except ValueError: return None

def classify(post):
    evidence=post.get("evidence",[])
    blob=" ".join(str(e.get(k,"")) for e in evidence for k in ("title","description")).lower()
    account=next((str(e.get("account","")).lower().lstrip("@") for e in evidence if e.get("account")), "")
    if account in OFFICIAL: kind="official"
    elif any("크루" in q or "crew" in q.lower() for q in post.get("matched_queries",[])): kind="crew_candidate"
    elif any(w in blob for w in ("협찬","광고","제공받","sponsored","#ad")): kind="sponsored_candidate"
    else: kind="ugc_candidate"
    sentiment="negative" if any(w in blob for w in NEGATIVE) else "positive" if any(w in blob for w in POSITIVE) else "neutral"
    return kind,sentiment

def summarize(report,day):
    start=dt.datetime.combine(day,dt.time.min,KST)
    end=start+dt.timedelta(days=1)
    confirmed=[]; pending=[]; types=Counter(); sentiment=Counter(); accounts=set()
    for post in report.get("candidates",[]):
        dates=[parse_date(e.get("published_at")) for e in post.get("evidence",[])]
        dates=[d for d in dates if d is not None]
        kind,mood=classify(post)
        if kind=="official": continue
        # Search query matches alone do not prove tags or original posting time.
        verified=any(e.get("original_post_verified") is True and e.get("korean_relevance_verified") is True and e.get("brand_reference_verified") is True and (parsed:=parse_date(e.get("published_at"))) is not None and start<=parsed<end for e in post.get("evidence",[]))
        if verified:
            confirmed.append(post);types[kind]+=1;sentiment[mood]+=1
            accounts.update(str(e["account"]).strip().lower().lstrip("@") for e in post["evidence"] if e.get("account") and str(e["account"]).strip().lstrip("@"))
        else: pending.append(post)
    coverage=report.get("coverage",[])
    operational=bool(coverage) and any(c.get("status")=="ok" for c in coverage)
    return {"day":day.isoformat(),"verified_posts":len(confirmed) if operational else None,"verified_accounts":len(accounts) if operational else None,"type_counts":dict(types),"sentiment_counts":dict(sentiment),"candidate_posts_pending_verification":len(pending),"coverage":coverage,"data_status":"partial" if operational else "unavailable","representative_urls":[p["url"] for p in confirmed[:10]],"limitations":report.get("limitations",[])}

def main():
    p=argparse.ArgumentParser()
    p.add_argument("input",type=Path)
    p.add_argument("--day",type=dt.date.fromisoformat,default=(dt.datetime.now(KST)-dt.timedelta(days=1)).date())
    p.add_argument("--output",type=Path,default=Path("instagram_daily.json"))
    a=p.parse_args()
    result=summarize(json.loads(a.input.read_text(encoding="utf-8")),a.day)
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
if __name__=="__main__": main()
