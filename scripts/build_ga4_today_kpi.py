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
    sql=f'''DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE('Asia/Seoul'),INTERVAL @days-1 DAY);
    WITH b AS (
      SELECT event_name,user_pseudo_id,
        CONCAT(user_pseudo_id,'-',COALESCE(CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS STRING),'0')) session_key,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location') page_location,
        (SELECT ANY_VALUE(NULLIF(item_id,'')) FROM UNNEST(items) WHERE NULLIF(item_id,'') IS NOT NULL) item_id,
        (SELECT ANY_VALUE(NULLIF(item_name,'')) FROM UNNEST(items) WHERE NULLIF(item_name,'') IS NOT NULL) item_name,
        ecommerce.transaction_id,COALESCE(ecommerce.purchase_revenue,0) purchase_revenue
      FROM `{table}`
      WHERE REGEXP_CONTAINS(_TABLE_SUFFIX,r'^(?:intraday_)?\\d{{8}}$')
        AND DATE(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') BETWEEN start_date AND CURRENT_DATE('Asia/Seoul')
        AND event_name IN ('view_item','add_to_cart','begin_checkout','purchase')
    ),x AS (
      SELECT *,UPPER(TRIM(COALESCE(item_id,REGEXP_EXTRACT(page_location,r'(?i)(?:product|goods|item)[^A-Za-z0-9]*([A-Za-z0-9_-]{{5,}})')))) product_code FROM b
    ),sp AS (
      SELECT product_code,session_key,ANY_VALUE(item_name) product_name,
        MAX(event_name='view_item') viewed,MAX(event_name='add_to_cart') added,MAX(event_name='begin_checkout') checkout,MAX(event_name='purchase') purchased,
        MAX(IF(event_name='purchase',purchase_revenue,0)) revenue
      FROM x WHERE product_code IS NOT NULL GROUP BY 1,2
    )
    SELECT product_code,ANY_VALUE(product_name) product_name,COUNTIF(viewed) pdp_sessions,COUNTIF(added) add_to_cart_sessions,
      COUNTIF(checkout) checkout_sessions,COUNTIF(purchased) purchase_sessions,SUM(revenue) revenue
    FROM sp GROUP BY 1 HAVING pdp_sessions>=@minimum ORDER BY pdp_sessions DESC'''
    rows=run_query(client,sql,[bigquery.ScalarQueryParameter('days','INT64',days),bigquery.ScalarQueryParameter('minimum','INT64',minimum)])
    products=[]
    for r in rows:
        v=f(r.get('pdp_sessions'));a=f(r.get('add_to_cart_sessions'));c=f(r.get('checkout_sessions'));p=f(r.get('purchase_sessions'));rev=f(r.get('revenue'))
        atc=a/v*100 if v else 0;cvr=p/v*100 if v else 0;drop=100-atc;score=round(min(100,35*math.log1p(v)/math.log1p(max(v,10))+35*drop/100+30*max(0,2-cvr)/2),1)
        aov=rev/p if p else 100000;expected=max(v*(max(8,atc)-atc)/100*.12,0)
        products.append({'product_code':r.get('product_code') or '','product_name':r.get('product_name') or r.get('product_code') or '상품명 미수집','pdp_sessions':round(v),'add_to_cart_sessions':round(a),'checkout_sessions':round(c),'purchase_sessions':round(p),'atc_rate':round(atc,2),'checkout_rate':round(c/v*100 if v else 0,2),'cvr':round(cvr,2),'pdp_abandonment_rate':round(drop,2),'revenue':round(rev),'opportunity_score':score,'score':score,'expected_orders':round(expected,1),'expected_revenue':round(expected*aov),'reason':'PDP 조회 대비 장바구니 미진입' if atc<8 else '구매전환 개선 기회'})
    products.sort(key=lambda x:(x['opportunity_score'],x['expected_revenue']),reverse=True)
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
