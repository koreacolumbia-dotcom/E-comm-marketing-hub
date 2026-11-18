import json
from pathlib import Path
from market_signals import publish_signals
p=Path('competitor-output')
now=json.loads((p/'snapshot.json').read_text())
oldfile=Path('previous-snapshot/snapshot.json')
old=json.loads(oldfile.read_text()) if oldfile.exists() else None
# The strict coverage gate can fail even when useful evidence was published.
# Compare with the newest available prior real evidence, never a future snapshot.
prior_feed=Path("reports/competitor_intelligence/latest-report-feed.json")
if prior_feed.exists():
    try:
        f=json.loads(prior_feed.read_text())
        stamp=f.get("checked_at_kst") or ""
        if stamp and stamp<now["checked_at_kst"] and (not old or stamp>(old.get("checked_at_kst") or "")):
            old={"checked_at_kst":stamp,"brands":[
                {"brand":b["brand"],"pages":b.get("evidence",[])}
                for b in f.get("brands",[])+([f["owned"]] if isinstance(f.get("owned"),dict) else [])
                if isinstance(b,dict) and b.get("brand")
            ]}
    except (OSError,ValueError,TypeError,KeyError):
        pass
previous={b['brand']:b for b in old.get('brands',[])} if old else {}
rows=[]
for b in now['brands']:
    prev={x['url']:x for x in previous.get(b['brand'],{}).get('pages',[])}
    pages=[]
    for x in b['pages']:
        y=prev.get(x['url'])
        pages.append({**x,'change':'baseline' if not old else 'newly_observed' if not y else 'changed' if y['fingerprint']!=x['fingerprint'] else 'unchanged'})
    rows.append({'brand':b['brand'],'status':b['status'],'pages_checked':len(pages),'promotion_evidence_pages':b.get('promotion_evidence_pages',0),'errors':b['errors'],'evidence':pages})
owned=next((r for r in rows if r['brand']=='COLUMBIA'),None)
rows=[r for r in rows if r['brand']!='COLUMBIA']
feed={'checked_at_kst':now['checked_at_kst'],'comparison_at':old['checked_at_kst'] if old else None,'coverage':{'brands':len(rows),'accessible':sum(r['pages_checked']>0 for r in rows),'promotion_evidence':sum(r['promotion_evidence_pages']>0 for r in rows)},'brands':rows,'owned':owned,'warning':'Evidence candidates only. Newly observed is not necessarily newly launched. Unobserved is not ended. Verify event dates, prices and conditions before timeline placement.'}
(p/'report-feed.json').write_text(json.dumps(feed,ensure_ascii=False,indent=2))
print(json.dumps(feed['coverage']))

# Compact daily archive, intentionally separate from competitor counts.
if owned is not None:
    checked=now["checked_at_kst"]
    day=checked[:10]
    archive=Path("reports/competitor_intelligence/owned")
    archive.mkdir(parents=True,exist_ok=True)
    assets={}
    promotions={}
    for page in owned.get("evidence",[]):
        for asset in page.get("hero_assets",[]):
            if asset.get("local_image") and Path(asset["local_image"]).name==asset["local_image"] and (archive/"assets"/asset["local_image"]).is_file():
                assets[asset["local_image"]]={k:asset.get(k) for k in ("local_image","image","url","title")}
        if "/event/" in page.get("url","").lower():
            promotions[page["url"]]={"title":page.get("title",""),"url":page["url"],"evidence":page.get("evidence",[])[:12],"date_mentions":page.get("date_mentions",[])[:15]}
    payload={"date":day,"checked_at_kst":checked,"brand":"COLUMBIA","status":owned["status"],"pages_checked":owned["pages_checked"],"assets":list(assets.values()),"promotions":list(promotions.values()),"errors":owned.get("errors",[])}
    # Preserve evidence collected earlier the same day, even if a later crawl fails.
    daily=archive/(day+".json")
    if daily.exists():
        try:
            prior=json.loads(daily.read_text(encoding="utf-8"))
            if prior.get("date")==day and prior.get("brand")=="COLUMBIA":
                by_asset={x["local_image"]:x for x in prior.get("assets",[]) if x.get("local_image")}
                by_asset.update(assets)
                by_promo={x["url"]:x for x in prior.get("promotions",[]) if x.get("url")}
                by_promo.update(promotions)
                payload["assets"]=list(by_asset.values())
                payload["promotions"]=list(by_promo.values())
                payload["retained_prior_evidence"]=bool(set(by_asset)-set(assets) or set(by_promo)-set(promotions))
        except (OSError,ValueError,TypeError,KeyError):
            pass
    payload["latest_attempt_pages"]=owned["pages_checked"]
    daily.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8")
    (archive/"latest.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8")

# Publish a small evidence feed independently of the large product report.
publish_signals(now)
