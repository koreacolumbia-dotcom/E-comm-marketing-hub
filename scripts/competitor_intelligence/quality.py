import json, os, re
from pathlib import Path
p=Path("competitor-output")
f=p/"snapshot.json"
data=json.loads(f.read_text()) if f.exists() else {"brands":[]}
rows=[]
for b in data["brands"]:
    pages=b.get("pages",[])
    promo=[x for x in pages if x.get("evidence") and re.search(r"event|promotion|special|exh|기획전|이벤트|페스타|black friday",x.get("url","")+" "+x.get("title",""),re.I)]
    priced=[x for x in pages if x.get("price_mentions")]
    usable=bool(promo and priced)
    rows.append({"brand":b["brand"],"pages":len(pages),"promotion_pages":len(promo),"price_pages":len(priced),"candidate_usable":usable,"errors":b.get("errors",[])})
n=sum(r["candidate_usable"] for r in rows)
quality={"checked_at_kst":data.get("checked_at_kst"),"target":9,"denominator":10,"candidate_usable":n,"candidate_coverage_pct":n*10,"target_met_candidate_only":n>=9,"brands":rows,"note":"Candidate coverage only; human verification of dates and paired prices still required."}
p.mkdir(exist_ok=True)
(p/"quality.json").write_text(json.dumps(quality,ensure_ascii=False,indent=2))
summary=["# Competitor collection quality",f"Candidate evidence: {n}/10 (target 9/10)","| Brand | Pages | Promo | Price |","|---|---:|---:|---:|"]
summary += [f"| {r['brand']} | {r['pages']} | {r['promotion_pages']} | {r['price_pages']} |" for r in rows]
if os.getenv("GITHUB_STEP_SUMMARY"):
    with open(os.environ["GITHUB_STEP_SUMMARY"],"a") as out: out.write("\n".join(summary)+"\n")
print(json.dumps(quality,ensure_ascii=False))
if n<9: raise SystemExit(1)
