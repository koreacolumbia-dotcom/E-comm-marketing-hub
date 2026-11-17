"""Public Instagram discovery via configured Brave Search API and optional local VOC JSON.
No login bypass. Search results are candidates, not a complete Instagram census."""
import argparse, datetime as dt, json, os, re, urllib.request, urllib.parse
from pathlib import Path
from zoneinfo import ZoneInfo

KST=ZoneInfo("Asia/Seoul")
TERMS=["@columbia_kr","@columbia_creator_crew","#columbia_kr","#columbia_creator_crew","#컬럼비아","#컬럼비아스포츠웨어","#columbiakorea","#컬럼비아크리에이터크루"]
RX=re.compile(r"https?://(?:www\.)?instagram\.com/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)",re.I)

def search(term,key):
    q='site:instagram.com (inurl:reel OR inurl:p) "'+term+'"'
    url="https://api.search.brave.com/res/v1/web/search?"+urllib.parse.urlencode({"q":q,"count":20,"freshness":"pw"})
    req=urllib.request.Request(url,headers={"X-Subscription-Token":key,"Accept":"application/json"})
    with urllib.request.urlopen(req,timeout=25) as response:
        return json.load(response).get("web",{}).get("results",[])

def collect(rows,term,source,posts):
    for row in rows:
        m=RX.search(str(row.get("url","")))
        if not m: continue
        code=m.group(1)
        p=posts.setdefault(code,{"shortcode":code,"url":m.group(0),"matched_queries":[],"sources":[],"evidence":[]})
        if term not in p["matched_queries"]: p["matched_queries"].append(term)
        if source not in p["sources"]: p["sources"].append(source)
        p["evidence"].append({k:row[k] for k in ("title","description","published_at","account","page_age","url") if k in row})

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--input",type=Path,action="append",default=[],help="Optional JSON array of public post candidates with URL and evidence")
    parser.add_argument("--accounts",type=Path,help="Optional JSON array of verified public creator account handles")
    parser.add_argument("--output",type=Path,default=Path("instagram_candidates.json"))
    args=parser.parse_args()
    terms=list(TERMS)
    if args.accounts:
        terms.extend("@"+a.lstrip("@") for a in json.loads(args.accounts.read_text()) if isinstance(a,str))
    posts={}; coverage=[]
    key=os.environ.get("BRAVE_SEARCH_API_KEY")
    for term in dict.fromkeys(terms):
        if not key:
            coverage.append({"query":term,"status":"not_configured"}); continue
        try:
            rows=search(term,key); collect(rows,term,"brave",posts)
            coverage.append({"query":term,"status":"ok","results":len(rows)})
        except Exception as exc:
            coverage.append({"query":term,"status":"error","error":type(exc).__name__})
    for path in args.input:
        try:
            rows=json.loads(path.read_text())
            if not isinstance(rows,list): raise ValueError("Expected JSON array")
            collect(rows,"import","local_voc",posts)
            coverage.append({"source":str(path),"status":"ok","results":len(rows)})
        except Exception as exc:
            coverage.append({"source":str(path),"status":"error","error":type(exc).__name__})
    report={"collected_at":dt.datetime.now(KST).isoformat(),"candidate_count":len(posts),"verified_daily_count":None,"coverage":coverage,"candidates":list(posts.values()),"limitations":["Search index is incomplete; candidates require original post date, Korean relevance and non-official author verification.","Matched query does not prove a photo tag or mention.","Stories and private posts are excluded."]}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
if __name__=="__main__":
    main()
