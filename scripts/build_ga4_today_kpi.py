#!/usr/bin/env python3
from __future__ import annotations
import base64, datetime as dt, json, math, os
from pathlib import Path
from typing import Any
from google.cloud import bigquery

ROOT=Path(__file__).resolve().parents[1]
KST=dt.timezone(dt.timedelta(hours=9))
TODAY_OUT=ROOT/'reports'/'today_kpi'
PDP_OUT=ROOT/'reports'/'pdp_opportunity'

def env(k:str,d:str='')->str:return os.getenv(k,d).strip()
def setup_credentials()->None:
    p=env('GOOGLE_APPLICATION_CREDENTIALS')
    if p and Path(p).exists():return
    b=env('GOOGLE_SA_JSON_B64')
    if not b:raise SystemExit('[ERROR] GOOGLE_SA_JSON_B64 missing')
    p=Path('/tmp/ga4_operational_sa.json');p.write_bytes(base64.b64decode(b));os.environ['GOOGLE_APPLICATION_CREDENTIALS']=str(p)
def f(v:Any)->float:
    try:
        x=float(v or 0);return x if math.isfinite(x) else 0.0
    except:return 0.0

def run_query(client:bigquery.Client,sql:str,params:list[bigquery.ScalarQueryParameter]|None=None):
    cfg=bigquery.QueryJobConfig(query_parameters=params or [],use_query_cache=True)
    return [dict(r.items()) for r in client.query(sql,job_config=cfg,location=env('BQ_LOCATION','asia-northeast3')).result(timeout=600)]

def build_today(client:bigquery.Client)->dict[str,Any]:
    table=env('GA4_EVENTS_TABLE','columbia-ga4.analytics_358593394.events_*')
    sql=f'''WITH b AS (
      SELECT event_name,user_pseudo_id,TIMESTAMP_MICROS(event_timestamp) event_ts,
        CONCAT(user_pseudo_id,'-',COALESCE(CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS STRING),'0')) session_key,
        NULLIF(COALESCE(ecommerce.transaction_id,(SELECT value.string_value FROM UNNEST(event_params) WHERE key='transaction_id')),'') transaction_id,
        COALESCE(ecommerce.purchase_revenue,(SELECT value.double_value FROM UNNEST(event_params) WHERE key='value'),CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key='value') AS FLOAT64),0) revenue
      FROM `{table}`
      WHERE REGEXP_CONTAINS(_TABLE_SUFFIX,r'^(?:intraday_)?\\d{{8}}$')
        AND DATE(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul')=CURRENT_DATE('Asia/Seoul')
        AND event_name IN ('session_start','page_view','view_item','add_to_cart','begin_checkout','purchase','sign_up')
    )
    SELECT COUNT(DISTINCT IF(event_name='session_start',session_key,NULL)) sessions,
      COUNT(DISTINCT user_pseudo_id) users,
      COUNT(DISTINCT IF(event_name='purchase',transaction_id,NULL)) orders,
      SUM(IF(event_name='purchase',revenue,0)) revenue,
      COUNTIF(event_name='sign_up') signups,
      COUNTIF(event_name='view_item') view_item_events,
      COUNTIF(event_name='add_to_cart') add_to_cart_events,
      COUNTIF(event_name='begin_checkout') checkout_events,
      COUNTIF(event_name='purchase') purchase_events,
      MAX(event_ts) latest_event_ts
    FROM b'''
    row=(run_query(client,sql) or [{}])[0]
    sessions=f(row.get('sessions'));orders=f(row.get('orders'));revenue=f(row.get('revenue'))
    metrics={'sessions':round(sessions),'users':round(f(row.get('users'))),'orders':round(orders),'revenue':round(revenue),'cvr':orders/sessions if sessions else 0,'aov':revenue/orders if orders else 0,'signups':round(f(row.get('signups')))}
    return {'generated_at':dt.datetime.now(KST).isoformat(),'date':dt.datetime.now(KST).date().isoformat(),'timezone':'Asia/Seoul','source':'GA4 BigQuery Export including intraday','latest_event_ts':str(row.get('latest_event_ts') or ''),'metrics':metrics,'funnel':{k:round(f(row.get(k))) for k in ['view_item_events','add_to_cart_events','checkout_events','purchase_events']},'status':'live' if sessions>0 else 'empty'}

def build_pdp(client:bigquery.Client)->dict[str,Any]:
    table=env('GA4_EVENTS_TABLE','columbia-ga4.analytics_358593394.events_*');days=int(env('PDP_LOOKBACK_DAYS','30'));minimum=int(env('PDP_MIN_SESSIONS','5'))
    sql=f'''DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE('Asia/Seoul'),INTERVAL @lookback_minus_one DAY);
    WITH b AS (
      SELECT event_name,user_pseudo_id,TIMESTAMP_MICROS(event_timestamp) event_ts,
        CONCAT(user_pseudo_id,'-',COALESCE(CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS STRING),'0')) session_key,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location') page_location,
        NULLIF(item.item_id,'') item_id,NULLIF(item.item_name,'') item_name,
        COALESCE(item.item_revenue,item.price*COALESCE(item.quantity,1),0) item_revenue
      FROM `{table}` event
      LEFT JOIN UNNEST(event.items) item ON TRUE
      WHERE REGEXP_CONTAINS(_TABLE_SUFFIX,r'^(?:intraday_)?\\d{{8}}$')
        AND DATE(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') BETWEEN start_date AND CURRENT_DATE('Asia/Seoul')
        AND event_name IN ('view_item','add_to_cart','begin_checkout','purchase')
    ),x AS (
      SELECT *,UPPER(TRIM(COALESCE(item_id,REGEXP_EXTRACT(page_location,r'(?i)(?:product|goods|item)[^A-Za-z0-9]*([A-Za-z0-9_-]{{5,}})')))) product_code FROM b
    ),sp0 AS (
      SELECT product_code,session_key,ANY_VALUE(item_name) product_name,
        MIN(IF(event_name='view_item',event_ts,NULL)) view_ts,
        MIN(IF(event_name='add_to_cart',event_ts,NULL)) cart_ts,
        MIN(IF(event_name='begin_checkout',event_ts,NULL)) checkout_ts,
        MIN(IF(event_name='purchase',event_ts,NULL)) purchase_ts,
        SUM(IF(event_name='purchase',item_revenue,0)) revenue
      FROM x WHERE product_code IS NOT NULL GROUP BY 1,2
    ),sp AS (
      SELECT *,view_ts IS NOT NULL viewed,
        cart_ts IS NOT NULL AND cart_ts>=view_ts added,
        checkout_ts IS NOT NULL AND cart_ts IS NOT NULL AND checkout_ts>=cart_ts AND cart_ts>=view_ts checkout,
        purchase_ts IS NOT NULL AND checkout_ts IS NOT NULL AND purchase_ts>=checkout_ts purchased
      FROM sp0
    )
    SELECT product_code,ANY_VALUE(product_name) product_name,COUNTIF(viewed) pdp_sessions,COUNTIF(added) add_to_cart_sessions,
      COUNTIF(checkout) checkout_sessions,COUNTIF(purchased) purchase_sessions,SUM(revenue) revenue
    FROM sp GROUP BY 1 HAVING pdp_sessions>=@minimum ORDER BY pdp_sessions DESC'''
    rows=run_query(client,sql,[bigquery.ScalarQueryParameter('lookback_minus_one','INT64',max(days-1,0)),bigquery.ScalarQueryParameter('minimum','INT64',minimum)])
    products=[]
    for r in rows:
        v=f(r.get('pdp_sessions'));a=f(r.get('add_to_cart_sessions'));c=f(r.get('checkout_sessions'));p=f(r.get('purchase_sessions'));rev=f(r.get('revenue'))
        atc=a/v*100 if v else 0;cart_checkout=c/a*100 if a else 0;cvr=p/v*100 if v else 0;pdp_drop=100-atc;cart_drop=100-cart_checkout if a else 0;score=round(min(100,35*math.log1p(v)/math.log1p(max(v,10))+35*pdp_drop/100+30*max(0,2-cvr)/2),1)
        aov=rev/p if p else 100000;expected=max(v*(max(8,atc)-atc)/100*.12,0)
        products.append({'product_code':r.get('product_code') or '','product_name':r.get('product_name') or r.get('product_code') or '상품명 미수집','pdp_sessions':round(v),'add_to_cart_sessions':round(a),'checkout_sessions':round(c),'purchase_sessions':round(p),'atc_rate':round(atc,2),'cart_to_checkout_rate':round(cart_checkout,2),'cvr':round(cvr,2),'pdp_to_cart_abandonment_rate':round(pdp_drop,2),'cart_to_checkout_abandonment_rate':round(cart_drop,2),'pdp_abandonment_rate':round(pdp_drop,2),'revenue':round(rev),'opportunity_score':score,'score':score,'expected_orders':round(expected,1),'expected_revenue':round(expected*aov),'reason':'PDP 조회 대비 장바구니 미진입' if atc<8 else '구매전환 개선 기회'})
    products.sort(key=lambda x:(x['opportunity_score'],x['expected_revenue']),reverse=True)
    def pct_rank(values:list[float],value:float)->float:
        return sum(1 for item in values if item<=value)/len(values) if values else 0
    revenues=[f(x['revenue']) for x in products];views=[f(x['pdp_sessions']) for x in products];drops=[f(x['pdp_to_cart_abandonment_rate']) for x in products]
    for product in products:
        rp=pct_rank(revenues,f(product['revenue']));vp=pct_rank(views,f(product['pdp_sessions']));dp=pct_rank(drops,f(product['pdp_to_cart_abandonment_rate']))
        product['dashboard_priority']=round(100*(.4*max(rp,vp)+.4*((rp+vp)/2)+.2*dp))
    trend_products=sorted(products,key=lambda x:(x['dashboard_priority'],x['revenue'],x['pdp_sessions']),reverse=True)[:10]
    top_codes=[x['product_code'] for x in trend_products]
    if top_codes:
        history_sql=f'''DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE('Asia/Seoul'),INTERVAL 29 DAY);
        WITH b AS (
          SELECT DATE(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') event_date,event_name,
            CONCAT(user_pseudo_id,'-',COALESCE(CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS STRING),'0')) session_key,
            TIMESTAMP_MICROS(event_timestamp) event_ts,NULLIF(item.item_id,'') item_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location') page_location
          FROM `{table}` event LEFT JOIN UNNEST(event.items) item ON TRUE
          WHERE REGEXP_CONTAINS(_TABLE_SUFFIX,r'^(?:intraday_)?\\d{{8}}$')
            AND DATE(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') BETWEEN start_date AND CURRENT_DATE('Asia/Seoul')
            AND event_name IN ('view_item','add_to_cart','begin_checkout')
        ),x AS (
          SELECT *,UPPER(TRIM(COALESCE(item_id,REGEXP_EXTRACT(page_location,r'(?i)(?:product|goods|item)[^A-Za-z0-9]*([A-Za-z0-9_-]{{5,}})')))) product_code FROM b
        ),sp0 AS (
          SELECT event_date,product_code,session_key,MIN(IF(event_name='view_item',event_ts,NULL)) view_ts,
            MIN(IF(event_name='add_to_cart',event_ts,NULL)) cart_ts,MIN(IF(event_name='begin_checkout',event_ts,NULL)) checkout_ts
          FROM x WHERE product_code IN UNNEST(@top_codes) GROUP BY 1,2,3
        ),daily AS (
          SELECT event_date,product_code,COUNTIF(view_ts IS NOT NULL) pdp_sessions,
            COUNTIF(cart_ts IS NOT NULL AND cart_ts>=view_ts) cart_sessions,
            COUNTIF(checkout_ts IS NOT NULL AND cart_ts IS NOT NULL AND checkout_ts>=cart_ts AND cart_ts>=view_ts) checkout_sessions
          FROM sp0 GROUP BY 1,2
        ) SELECT * FROM daily ORDER BY product_code,event_date'''
        history_rows=run_query(client,history_sql,[bigquery.ArrayQueryParameter('top_codes','STRING',top_codes)])
        history_by_code={code:{} for code in top_codes}
        for r in history_rows:
            v=f(r.get('pdp_sessions'));a=f(r.get('cart_sessions'));c=f(r.get('checkout_sessions'))
            history_by_code[r['product_code']][str(r['event_date'])]={'date':str(r['event_date']),'pdp_sessions':round(v),'cart_sessions':round(a),'checkout_sessions':round(c),'pdp_to_cart_abandonment_rate':round(100-a/v*100,2) if v else None,'cart_to_checkout_abandonment_rate':round(100-c/a*100,2) if a else None}
        history_dates=[(dt.datetime.now(KST).date()-dt.timedelta(days=offset)).isoformat() for offset in range(29,-1,-1)]
        for product in trend_products:
            product['daily_history']=[history_by_code.get(product['product_code'],{}).get(day,{'date':day,'pdp_sessions':0,'cart_sessions':0,'checkout_sessions':0,'pdp_to_cart_abandonment_rate':None,'cart_to_checkout_abandonment_rate':None}) for day in history_dates]
    now=dt.datetime.now(KST)
    return {'generated_at':now.isoformat(),'data_start':(now.date()-dt.timedelta(days=days-1)).isoformat(),'data_end':now.date().isoformat(),'source':'GA4 daily + intraday tables','status':'live' if products else 'empty','diagnostics':{'lookback_days':days,'minimum_sessions':minimum,'raw_product_rows':len(rows),'message':'No product rows: verify view_item items.item_id or PDP URL product code' if not products else 'OK'},'products':products,'rows':products}

def main()->int:
    setup_credentials();client=bigquery.Client(project=env('BQ_PROJECT','columbia-ga4'))
    today=build_today(client);pdp=build_pdp(client)
    TODAY_OUT.mkdir(parents=True,exist_ok=True);PDP_OUT.mkdir(parents=True,exist_ok=True)
    (TODAY_OUT/'data.json').write_text(json.dumps(today,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (TODAY_OUT/'meta.json').write_text(json.dumps({k:today.get(k) for k in ['generated_at','date','status','latest_event_ts']},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (PDP_OUT/'data.json').write_text(json.dumps(pdp,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (PDP_OUT/'meta.json').write_text(json.dumps({'generated_at':pdp['generated_at'],'data_start':pdp['data_start'],'data_end':pdp['data_end'],'status':pdp['status'],'row_count':len(pdp['products']),'diagnostics':pdp['diagnostics']},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(f"[GA4_OPS] today={today['status']} sessions={today['metrics']['sessions']} pdp={len(pdp['products'])}")
    return 0
if __name__=='__main__':raise SystemExit(main())
