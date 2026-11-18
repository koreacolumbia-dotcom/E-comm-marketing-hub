import json
from pathlib import Path
p=Path('competitor-output')
now=json.loads((p/'snapshot.json').read_text())
oldfile=Path('previous-snapshot/snapshot.json')
old=json.loads(oldfile.read_text()) if oldfile.exists() else None
previous={b['brand']:b for b in old['brands']} if old else {}
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
