#!/usr/bin/env python3
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
V2=ROOT/'reports'/'v2'/'data.json';TODAY=ROOT/'reports'/'today_kpi'/'data.json';PDP=ROOT/'reports'/'pdp_opportunity'/'data.json'
def load(p):return json.loads(p.read_text(encoding='utf-8')) if p.exists() else {}
def main():
    if not V2.exists():raise SystemExit('[ERROR] V2 data missing')
    v=load(V2);t=load(TODAY);p=load(PDP)
    if t.get('status')=='live' and t.get('metrics'):
        v['metrics']=t['metrics'];v['today_kpi']=t;v['period']=f"오늘 누적 {t.get('date')} KST";v['period_type']='today_intraday';v['provenance']={k:'reports/today_kpi/data.json::metrics' for k in t['metrics']}
    products=p.get('products') or []
    if products:
        v['opportunities']=[{'name':x.get('product_name'),'code':x.get('product_code'),'score':x.get('opportunity_score'),'sessions':x.get('pdp_sessions'),'cvr':x.get('cvr'),'revenue':x.get('revenue'),'expected_revenue':x.get('expected_revenue'),'reason':x.get('reason'),'pdp_abandonment_rate':x.get('pdp_abandonment_rate'),'atc_rate':x.get('atc_rate')} for x in products]
        v['pdp_opportunity']={'ready':True,'row_count':len(products),'data_start':p.get('data_start'),'data_end':p.get('data_end'),'source':'reports/pdp_opportunity/data.json','top':products[:20]}
    else:
        v['pdp_opportunity']={'ready':False,'row_count':0,'diagnostics':p.get('diagnostics',{}),'source':'reports/pdp_opportunity/data.json'}
    V2.write_text(json.dumps(v,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(f"[CONNECTED_GA4] today={bool(v.get('today_kpi'))} pdp={len(products)}")
if __name__=='__main__':main()
