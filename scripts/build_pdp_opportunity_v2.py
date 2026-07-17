#!/usr/bin/env python3
from __future__ import annotations
import base64, datetime as dt, json, math, os
from pathlib import Path
from typing import Any
from google.cloud import bigquery

KST=dt.timezone(dt.timedelta(hours=9)); ROOT=Path(__file__).resolve().parents[1]; OUT=ROOT/'reports'/'pdp_opportunity'
def env(k,d=''): return os.getenv(k,d).strip()
def cred():
    p=env('GOOGLE_APPLICATION_CREDENTIALS')
    if p and Path(p).exists(): return
    b=env('GOOGLE_SA_JSON_B64')
    if not b: raise SystemExit('GOOGLE_SA_JSON_B64 is empty')
    p=ROOT/'gcp_service_account.json'; p.write_bytes(base64.b64decode(b)); os.environ['GOOGLE_APPLICATION_CREDENTIALS']=str(p)
def n(v):
    try:return float(v or 0)
    except:return 0.0

def fetch(client,start,end):
    table=env('GA4_EVENTS_TABLE','columbia-ga4.analytics_358593394.events_*')
    sql=f'''DECLARE s DATE DEFAULT @s; DECLARE e DATE DEFAULT @e;
    WITH b AS (
      SELECT event_name,user_pseudo_id,
        CONCAT(user_pseudo_id,'-',CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS STRING)) session_key,
        device.category device,
        CONCAT(COALESCE(traffic_source.source,'(direct)'),' / ',COALESCE(traffic_source.medium,'(none)')) source_medium,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location') page_location,
        i.item_id,i.item_name,i.item_category,i.price,i.quantity,ecommerce.purchase_revenue
      FROM `{table}` ev LEFT JOIN UNNEST(ev.items) i ON TRUE
      WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d',s) AND FORMAT_DATE('%Y%m%d',e)
        AND event_name IN ('view_item','add_to_cart','begin_checkout','purchase')
    ), x AS (
      SELECT *,UPPER(TRIM(COALESCE(NULLIF(item_id,''),REGEXP_EXTRACT(page_location,r'(?i)(?:product|goods|item)[^A-Za-z0-9]*([A-Za-z0-9_-]{{5,}})')))) code FROM b
    ), sp AS (
      SELECT session_key,code,ANY_VALUE(NULLIF(item_name,'')) name,ANY_VALUE(NULLIF(item_category,'')) category,
        ANY_VALUE(device) device,ANY_VALUE(source_medium) source_medium,
        MAX(IF(event_name='view_item',1,0)) viewed,MAX(IF(event_name='add_to_cart',1,0)) added,
        MAX(IF(event_name='begin_checkout',1,0)) checkout,MAX(IF(event_name='purchase',1,0)) purchased,
        SUM(IF(event_name='purchase',COALESCE(price,0)*COALESCE(quantity,1),0)) item_revenue,
        MAX(IF(event_name='purchase',COALESCE(purchase_revenue,0),0)) tx_revenue
      FROM x WHERE code IS NOT NULL GROUP BY 1,2
    )
    SELECT code product_code,ANY_VALUE(name HAVING MAX LENGTH(COALESCE(name,''))) product_name,
      ANY_VALUE(category HAVING MAX LENGTH(COALESCE(category,''))) category,
      COUNTIF(viewed=1) pdp_sessions,COUNTIF(added=1) add_to_cart_sessions,
      COUNTIF(checkout=1) checkout_sessions,COUNTIF(purchased=1) purchase_sessions,
      SUM(GREATEST(item_revenue,tx_revenue)) revenue,
      APPROX_TOP_COUNT(device,1)[SAFE_OFFSET(0)].value top_device,
      APPROX_TOP_COUNT(source_medium,1)[SAFE_OFFSET(0)].value top_source_medium
    FROM sp GROUP BY 1 HAVING pdp_sessions>=@minimum ORDER BY pdp_sessions DESC'''
    cfg=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('s','DATE',start),bigquery.ScalarQueryParameter('e','DATE',end),bigquery.ScalarQueryParameter('minimum','INT64',int(env('PDP_MIN_SESSIONS','10')))])
    return [dict(r.items()) for r in client.query(sql,job_config=cfg,location=env('BQ_LOCATION','asia-northeast3')).result()]

def score(rows):
    if not rows:return []
    mx=max(n(r['pdp_sessions']) for r in rows) or 1
    aovs=sorted(n(r['revenue'])/max(n(r['purchase_sessions']),1) for r in rows if n(r['revenue'])>0); fallback=aovs[len(aovs)//2] if aovs else 100000
    out=[]
    for r in rows:
        v=n(r['pdp_sessions']); a=n(r['add_to_cart_sessions']); c=n(r['checkout_sessions']); p=n(r['purchase_sessions']); rev=n(r['revenue'])
        atc=a/v*100 if v else 0; cr=p/v*100 if v else 0; cc=p/c*100 if c else 0
        traffic=math.log1p(v)/math.log1p(mx); friction=max(0,min(1,(100-atc-55)/45)); gap=max(0,min(1,(2-cr)/2)); checkout_gap=max(0,min(1,(45-cc)/45)) if c else 1
        s=round(100*(.38*traffic+.27*friction+.23*gap+.12*checkout_gap),1)
        inc=max(v*(max(atc,8)-atc)/100,0); downstream=max(p/a if a else .12,.08); eo=inc*downstream; aov=rev/p if p else fallback
        reasons=[]
        if v>=100:reasons.append('트래픽 큼')
        if atc<5:reasons.append('장바구니 전환 낮음')
        if cr<1:reasons.append('구매전환 낮음')
        if c and cc<35:reasons.append('체크아웃 이탈')
        out.append({'product_code':r.get('product_code') or '','product_name':r.get('product_name') or r.get('product_code') or '상품명 미수집','category':r.get('category') or '','pdp_sessions':round(v),'add_to_cart_sessions':round(a),'checkout_sessions':round(c),'purchase_sessions':round(p),'atc_rate':round(atc,2),'checkout_rate':round(c/v*100 if v else 0,2),'cvr':round(cr,2),'pdp_abandonment_rate':round(100-atc,2),'checkout_completion_rate':round(cc,2),'revenue':round(rev),'opportunity_score':s,'expected_orders':round(eo,1),'expected_revenue':round(eo*aov),'top_device':r.get('top_device') or '','top_source_medium':r.get('top_source_medium') or '','reason':' · '.join(reasons) or '상세 페이지 점검'})
    return sorted(out,key=lambda x:(x['opportunity_score'],x['expected_revenue']),reverse=True)

def html_page(d):
    j=json.dumps(d,ensure_ascii=False).replace('</','<\\/')
    return '''<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>PDP Opportunity</title><style>*{box-sizing:border-box}body{margin:0;background:#f5f7fb;color:#172033;font-family:-apple-system,BlinkMacSystemFont,"Noto Sans KR",sans-serif}main{max-width:1360px;margin:auto;padding:20px}.sub{font-size:11px;color:#6b778c}.k{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0}.card{background:#fff;border:1px solid #e7ebf1;border-radius:15px;padding:15px}.v{font-size:24px;font-weight:800}.r{display:grid;grid-template-columns:48px minmax(220px,1fr) repeat(4,minmax(90px,.5fr));gap:10px;align-items:center;padding:11px 0;border-bottom:1px solid #e7ebf1}.s{width:40px;height:40px;display:grid;place-items:center;border-radius:11px;background:#eaf4ff;color:#0874e8;font-weight:800}@media(max-width:700px){main{padding:9px}h1{font-size:20px;margin:4px 0}.k{grid-template-columns:repeat(2,1fr);gap:6px}.card{padding:9px;border-radius:11px}.v{font-size:17px}.r{grid-template-columns:34px 1fr;gap:7px;padding:8px 0}.r>*:nth-child(n+3){display:none}.s{width:32px;height:32px;font-size:11px}}</style></head><body><main><h1>PDP Opportunity Center</h1><div class="sub">GA4 view_item → add_to_cart → begin_checkout → purchase</div><div class="k" id="k"></div><div class="card" id="r"></div></main><script>const D='''+j+''',R=D.products||[],f=n=>new Intl.NumberFormat('ko-KR',{notation:'compact',maximumFractionDigits:1}).format(n||0),p=n=>(n||0).toFixed(1)+'%',t=k=>R.reduce((a,x)=>a+(+x[k]||0),0);k.innerHTML=[['PDP 세션',f(t('pdp_sessions'))],['구매',f(t('purchase_sessions'))],['CVR',p(t('purchase_sessions')/Math.max(t('pdp_sessions'),1)*100)],['예상 개선매출',f(R.slice(0,20).reduce((a,x)=>a+x.expected_revenue,0))+'원']].map(x=>'<div class="card"><div class="sub">'+x[0]+'</div><div class="v">'+x[1]+'</div></div>').join('');r.innerHTML=R.slice(0,100).map((x,i)=>'<div class="r"><span class="s">'+Math.round(x.opportunity_score)+'</span><span><b>'+(i+1)+'. '+x.product_name+'</b><div class="sub">'+x.product_code+' · '+x.reason+'</div></span><span>'+f(x.pdp_sessions)+' 세션</span><span>ATC '+p(x.atc_rate)+'</span><span>CVR '+p(x.cvr)+'</span><span>'+f(x.expected_revenue)+'원</span></div>').join('')||'<p>데이터 없음</p>';</script></body></html>'''

def main():
    cred(); days=int(env('PDP_LOOKBACK_DAYS','30')); end=dt.datetime.now(KST).date()-dt.timedelta(days=1); start=end-dt.timedelta(days=days-1)
    rows=score(fetch(bigquery.Client(project=env('GCP_PROJECT_ID') or None),start,end)); now=dt.datetime.now(KST).isoformat()
    payload={'generated_at':now,'data_start':start.isoformat(),'data_end':end.isoformat(),'definition':{'pdp_sessions':'상품별 view_item GA4 세션','pdp_abandonment_rate':'1 - add_to_cart 세션 / view_item 세션','cvr':'purchase 세션 / view_item 세션','expected_revenue':'ATC 8% 목표를 적용한 보수적 추정'},'products':rows,'rows':rows}
    OUT.mkdir(parents=True,exist_ok=True); (OUT/'data.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8'); (OUT/'index.html').write_text(html_page(payload),encoding='utf-8'); (OUT/'meta.json').write_text(json.dumps({'generated_at':now,'data_start':start.isoformat(),'data_end':end.isoformat(),'status':'fresh' if rows else 'empty','row_count':len(rows)},ensure_ascii=False,indent=2)+'\n',encoding='utf-8'); print('[OK] PDP products',len(rows))
if __name__=='__main__':main()
