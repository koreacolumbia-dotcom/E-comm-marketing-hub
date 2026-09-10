#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build the CSK performance dashboard from GA4 BigQuery + Columbia CRM MSSQL."""
import os, re, json
from pathlib import Path
import numpy as np
import pandas as pd

OUT = Path(os.getenv("OUT_DIR", "reports")); OUT.mkdir(parents=True, exist_ok=True)
PROJECT = os.getenv("GCP_PROJECT", "columbia-ga4")
DATASET = os.getenv("GA4_DATASET", "analytics_358593394")
SERVER = os.getenv("MSSQL_SERVER", "211.239.167.185")
DB = os.getenv("MSSQL_DATABASE", "columbia_crm")
USER = os.getenv("MSSQL_USER", "crmdata")
PWD = os.getenv("MSSQL_PASSWORD", "")
TODAY = pd.Timestamp.today().normalize()
END = pd.Timestamp(os.getenv("REPORT_END_DATE", (TODAY-pd.Timedelta(days=1)).strftime("%Y-%m-%d")))
START = pd.Timestamp(os.getenv("REPORT_START_DATE", (END-pd.Timedelta(days=30)).strftime("%Y-%m-%d")))
LY_START, LY_END = START-pd.DateOffset(years=1), END-pd.DateOffset(years=1)

def rx(t,p): return bool(re.search(p, t or "", re.I))
def classify(s,m,c=""):
    sm=f"{s or ''} / {m or ''}"; cp=c or ""
    if rx(sm,r"youtube\s*/\s*live"): return ("4. Official SNS","YouTube Referral","유튜브 라이브")
    if rx(sm,r"lighthouse"): return ("3. Organic Traffic","Referral","라이트하우스 팝 디스플레이 존")
    if rx(sm,r"instagram.*story"): return ("4. Official SNS","Instagram Story","인스타그램 스토리")
    if rx(sm,r"instagram.*feed"): return ("4. Official SNS","Instagram Feed","인스타그램 피드")
    if rx(sm,r"benz"): return ("3. Organic Traffic","Referral","벤츠 러닝 프로그램")
    if rx(sm,r"nap.*da|toss|blind"): return ("2. Paid Ad","Paid Display","배너/리워드 광고")
    if rx(sm,r"kakaobs"): return ("2. Paid Ad","Paid Search","카카오 브랜드검색광고")
    if rx(sm,r"inhouse"): return ("3. Organic Traffic","Inhouse Purchase","Inhouse Purchase")
    if rx(sm,r"lms") or rx(cp,r"lms"): return ("5. Owned Channel","LMS","문자메시지유입")
    if rx(sm,r"email|edm"): return ("5. Owned Channel","Email","이메일유입")
    if rx(sm,r"kakao_fridnstalk"): return ("5. Owned Channel","Kakao Friendstalk","카카오톡친구톡")
    if rx(sm,r"mkt|_bd") or rx(cp,r"mkt|\[bd"): return ("1. Awareness","Awareness","Awareness")
    if rx(sm,r"igshopping"): return ("4. Official SNS","Instagram Official Shop","인스타그램 샵")
    if rx(sm,r"facebook.*referral"): return ("3. Organic Traffic","Social","페이스북자연유입")
    if rx(sm,r"instagram.*referral"): return ("4. Official SNS","Instagram Official Account","인스타그램 공식계정")
    if rx(sm,r"meta|facebook|instagram|\big\b|\bfb\b"): return ("2. Paid Ad","Paid Social","메타광고")
    if rx(sm,r"google") and rx(cp,r"demand|demend|디멘드|gdn"): return ("2. Paid Ad","Paid Display","구글 디스플레이 광고")
    if rx(sm,r"google\s*/\s*cpc") and rx(cp,r"pmax"): return ("2. Paid Ad","Paid Omni Channel","구글피맥스광고")
    if rx(sm,r"google\s*/\s*cpc") and rx(cp,r"youtube|yt|video|instream|vac|vvc|유튜브"): return ("1. Awareness","Paid Video","구글동영상광고")
    if rx(sm,r"google\s*/\s*cpc") and rx(cp,r"discovery"): return ("1. Awareness","Paid Display","구글디스커버리광고")
    if rx(sm,r"google\s*/\s*cpc"): return ("2. Paid Ad","Paid Search","구글검색광고")
    if rx(sm,r"google\s*/\s*organic"): return ("3. Organic Traffic","Organic Search","구글자연검색")
    if rx(sm,r"google"): return ("3. Organic Traffic","Referral","구글기타유입")
    if rx(sm,r"youtube"): return ("3. Organic Traffic","YouTube","유튜브자연유입")
    if rx(sm,r"naver.*(da|gfa)|gfa"): return ("2. Paid Ad","Paid Display","네이버배너광고")
    if rx(sm,r"naverbs"): return ("2. Paid Ad","Paid Search","네이버브랜드검색광고")
    if rx(sm,r"naver.*shopping_ad"): return ("2. Paid Ad","Paid Search","네이버쇼핑검색광고")
    if rx(sm,r"naver.*cpc"): return ("2. Paid Ad","Paid Search","네이버파워링크광고")
    if rx(sm,r"naver.*shopping"): return ("3. Organic Traffic","Organic Search","네이버쇼핑자연검색")
    if rx(sm,r"naver.*organic"): return ("3. Organic Traffic","Organic Search","네이버사이트자연검색")
    if rx(sm,r"naver"): return ("3. Organic Traffic","Referral","네이버기타유입")
    if rx(sm,r"daum.*organic"): return ("3. Organic Traffic","Organic Search","다음자연검색")
    if rx(sm,r"daum"): return ("3. Organic Traffic","Referral","다음기타유입")
    if rx(sm,r"kakao_ch"): return ("5. Owned Channel","Kakao Channel","카카오톡채널메시지")
    if rx(sm,r"kakao_alimtalk"): return ("5. Owned Channel","Kakao Alimtalk","카카오톡알림톡")
    if rx(sm,r"kakao_coupon"): return ("5. Owned Channel","Kakao Coupon","카카오톡쿠폰")
    if rx(sm,r"kakao_chatbot"): return ("5. Owned Channel","Kakao Chatbot","카카오톡챗봇")
    if rx(sm,r"kakao"): return ("2. Paid Ad","Paid Display","카카오광고")
    if rx(sm,r"\(direct\).*\(none\)"): return ("3. Organic Traffic","Direct","직접유입")
    if rx(sm,r"signal|buzzvill|criteo|mobon|snow|smr|tg|t_cafe|banner|\bda\b"): return ("2. Paid Ad","Paid Display","기타배너광고")
    if rx(sm,r"cpc"): return ("2. Paid Ad","Paid Search","기타검색광고")
    if rx(sm,r"organic"): return ("3. Organic Traffic","Organic Search","기타자연검색")
    if rx(sm,r"referral"): return ("3. Organic Traffic","Referral","기타추천유입")
    if rx(sm,r"shopping"): return ("3. Organic Traffic","Organic Search","기타쇼핑유입")
    if rx(sm,r"social"): return ("3. Organic Traffic","Social","기타소셜유입")
    return ("6. etc","미분류","미분류")

def ga4_query(a,b):
    a,b=a.strftime("%Y%m%d"),b.strftime("%Y%m%d")
    return f'''WITH base AS (SELECT PARSE_DATE('%Y%m%d',event_date) event_date,TIMESTAMP_MICROS(event_timestamp) event_ts,user_pseudo_id,event_name,(SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') ga_session_id,COALESCE(session_traffic_source_last_click.manual_campaign.source,collected_traffic_source.manual_source,traffic_source.source,'(direct)') source,COALESCE(session_traffic_source_last_click.manual_campaign.medium,collected_traffic_source.manual_medium,traffic_source.medium,'(none)') medium,COALESCE(session_traffic_source_last_click.manual_campaign.campaign_name,collected_traffic_source.manual_campaign_name,'(not set)') campaign,ecommerce.transaction_id transaction_id,COALESCE(ecommerce.purchase_revenue,0) revenue FROM `{PROJECT}.{DATASET}.events_*` WHERE _TABLE_SUFFIX BETWEEN '{a}' AND '{b}'), s AS (SELECT event_date,CONCAT(user_pseudo_id,'-',CAST(ga_session_id AS STRING)) session_key,ARRAY_AGG(source IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] source,ARRAY_AGG(medium IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] medium,ARRAY_AGG(campaign IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] campaign,COUNT(DISTINCT IF(event_name='purchase',transaction_id,NULL)) conversions,SUM(IF(event_name='purchase',revenue,0)) revenue FROM base WHERE ga_session_id IS NOT NULL GROUP BY 1,2) SELECT event_date,source,medium,campaign,COUNT(DISTINCT session_key) sessions,SUM(conversions) conversions,SUM(revenue) revenue FROM s GROUP BY 1,2,3,4'''

def load_bq(a,b):
    from google.cloud import bigquery
    d=bigquery.Client(project=PROJECT).query(ga4_query(a,b)).to_dataframe(); d["event_date"]=pd.to_datetime(d["event_date"])
    cls=[classify(s,m,c) for s,m,c in zip(d.source,d.medium,d.campaign)]
    d[["channel_major","channel_middle","channel_minor"]]=pd.DataFrame(cls,index=d.index)
    return d

def product_query(a,b):
    a=a.strftime("%Y-%m-%d"); b=(b+pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    return f'''SELECT CAST(o.OrderRegdate AS date) order_date,CAST(op.ProductCode AS varchar(100)) product_code,COALESCE(NULLIF(LTRIM(RTRIM(p.ProductName)),''),op.ProductCode) product_name,SUM(CAST(ISNULL(op.ProductQuantity,0) AS float)) qty,SUM(CAST(ISNULL(op.OrderProductPrice,0) AS float)) revenue,CASE WHEN SUM(CAST(ISNULL(op.ProductPrice,0) AS float)*CAST(ISNULL(op.ProductQuantity,0) AS float))>0 THEN 1-SUM(CAST(ISNULL(op.OrderProductPrice,0) AS float))/NULLIF(SUM(CAST(ISNULL(op.ProductPrice,0) AS float)*CAST(ISNULL(op.ProductQuantity,0) AS float)),0) END discount_rate,COUNT(DISTINCT o.OrderNo) orders FROM dbo.TB_Order o JOIN dbo.TB_OrderProduct op ON o.OrderNo=op.OrderNo LEFT JOIN dbo.TB_Product p ON op.ProductNo=p.ProductNo WHERE o.OrderRegdate>='{a}' AND o.OrderRegdate<'{b}' AND ISNULL(op.ProductQuantity,0)>0 AND ISNULL(op.OrderRefundStatus,0)=0 GROUP BY CAST(o.OrderRegdate AS date),CAST(op.ProductCode AS varchar(100)),COALESCE(NULLIF(LTRIM(RTRIM(p.ProductName)),''),op.ProductCode)'''

def load_sql(a,b):
    import pyodbc
    if not PWD: raise RuntimeError("MSSQL_PASSWORD is missing")
    drivers=[x for x in pyodbc.drivers() if "SQL Server" in x]
    driver="ODBC Driver 18 for SQL Server" if "ODBC Driver 18 for SQL Server" in drivers else "ODBC Driver 17 for SQL Server"
    cs=f"DRIVER={{{driver}}};SERVER={SERVER};DATABASE={DB};UID={USER};PWD={PWD};Encrypt=yes;TrustServerCertificate=yes;"
    with pyodbc.connect(cs,timeout=30) as cn: d=pd.read_sql(product_query(a,b),cn)
    d["order_date"]=pd.to_datetime(d["order_date"]); return d

def pkey(d):
    n=d.product_name.fillna("").astype(str).str.lower().str.replace(r"\s+"," ",regex=True).str.replace(r"[^0-9a-z가-힣 ]+","",regex=True).str.strip()
    return np.where(n!="","NAME:"+n,"CODE:"+d.product_code.fillna("").astype(str))

def build_channel(c,l):
    dims=["channel_major","channel_middle","channel_minor"]
    C=c.groupby(dims,dropna=False).agg(sessions_ty=("sessions","sum"),conversions_ty=("conversions","sum"),revenue_ty=("revenue","sum")).reset_index()
    L=l.groupby(dims,dropna=False).agg(sessions_ly=("sessions","sum"),conversions_ly=("conversions","sum"),revenue_ly=("revenue","sum")).reset_index()
    x=C.merge(L,on=dims,how="outer").fillna(0); x["channel"]=x.channel_minor
    x["cvr_ty"]=np.where(x.sessions_ty>0,x.conversions_ty/x.sessions_ty,0); x["cvr_ly"]=np.where(x.sessions_ly>0,x.conversions_ly/x.sessions_ly,0)
    for m in ["sessions","conversions","revenue"]: x[m+"_yoy"]=np.where(x[m+"_ly"]!=0,x[m+"_ty"]/x[m+"_ly"]-1,np.nan)
    x["cvr_yoy_pp"]=(x.cvr_ty-x.cvr_ly)*100
    return x.sort_values("revenue_ty",ascending=False)

def build_products(c,l):
    C=c.copy(); L=l.copy(); C["key"]=pkey(C); L["key"]=pkey(L)
    C=C.groupby("key").agg(product_code=("product_code","first"),product_name=("product_name","first"),revenue_ty=("revenue","sum"),qty_ty=("qty","sum"),orders_ty=("orders","sum"),discount_ty=("discount_rate","mean")).reset_index()
    L=L.groupby("key").agg(product_code_ly=("product_code","first"),product_name_ly=("product_name","first"),revenue_ly=("revenue","sum"),qty_ly=("qty","sum"),orders_ly=("orders","sum"),discount_ly=("discount_rate","mean")).reset_index()
    x=C.merge(L,on="key",how="outer"); x["product_name"]=x.product_name.fillna(x.product_name_ly); x["product_code"]=x.product_code.fillna(x.product_code_ly)
    for z in ["revenue_ty","revenue_ly","qty_ty","qty_ly","orders_ty","orders_ly"]: x[z]=pd.to_numeric(x[z],errors="coerce").fillna(0)
    x["revenue_yoy"]=np.where(x.revenue_ly!=0,x.revenue_ty/x.revenue_ly-1,np.nan); x["qty_yoy"]=np.where(x.qty_ly!=0,x.qty_ty/x.qty_ly-1,np.nan); x["discount_delta_pp"]=(x.discount_ty-x.discount_ly)*100
    return x.sort_values("revenue_ty",ascending=False)

def spikes(d):
    g=d.groupby(["order_date","product_code","product_name"],dropna=False).agg(revenue=("revenue","sum"),qty=("qty","sum"),discount_rate=("discount_rate","mean")).reset_index()
    out=[]
    for (code,name),z in g.groupby(["product_code","product_name"],dropna=False):
        z=z.set_index("order_date").reindex(pd.date_range(START,END,freq="D")).rename_axis("order_date").reset_index(); z["product_code"]=code; z["product_name"]=name; z["revenue"]=z.revenue.fillna(0); z["qty"]=z.qty.fillna(0)
        z["revenue_7d_avg"]=z.revenue.shift(1).rolling(7,min_periods=3).mean(); z["spike_vs_7d"]=np.where(z.revenue_7d_avg>0,z.revenue/z.revenue_7d_avg-1,np.nan); out.append(z)
    x=pd.concat(out,ignore_index=True) if out else pd.DataFrame();
    return x[(x.spike_vs_7d>=1)&(x.revenue>=1000000)].sort_values(["order_date","spike_vs_7d"],ascending=[False,False]) if len(x) else x

def clean_records(df):
    y=df.copy()
    for c in y.columns:
        if pd.api.types.is_datetime64_any_dtype(y[c]): y[c]=y[c].dt.strftime("%Y-%m-%d")
    y=y.replace({np.nan:None,np.inf:None,-np.inf:None})
    return y.to_dict("records")

def write_excel(ch,pr,sp,bc,bl,pc,pl):
    p=OUT/"performance_export.xlsx"
    with pd.ExcelWriter(p,engine="openpyxl") as w:
        ch.to_excel(w,"Channel_YoY",index=False); pr.to_excel(w,"Product_YoY",index=False); sp.to_excel(w,"Spikes",index=False); bc.to_excel(w,"GA4_TY_Raw",index=False); bl.to_excel(w,"GA4_LY_Raw",index=False); pc.to_excel(w,"MSSQL_TY_Raw",index=False); pl.to_excel(w,"MSSQL_LY_Raw",index=False)
    return p

def write_html(ch,pr,sp):
    summary={"sessions":float(ch.sessions_ty.sum()),"revenue":float(ch.revenue_ty.sum()),"conversions":float(ch.conversions_ty.sum()),"cvr":float(ch.conversions_ty.sum()/ch.sessions_ty.sum()) if ch.sessions_ty.sum() else 0}
    data=json.dumps({"summary":summary,"channels":clean_records(ch.head(100)),"products":clean_records(pr.head(100)),"spikes":clean_records(sp.head(100))},ensure_ascii=False)
    html=f'''<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Performance Dashboard</title><style>body{{margin:0;background:#06090d;color:#f5f7fa;font-family:Inter,Arial,sans-serif}}.wrap{{max-width:1500px;margin:auto;padding:32px}}h1{{font-size:34px;margin:0 0 6px}}.sub{{color:#8f9baa;margin-bottom:24px}}.kpis{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}}.card{{background:#0f141b;border:1px solid #222c38;border-radius:18px;padding:20px}}.label{{font-size:12px;color:#8f9baa;text-transform:uppercase}}.value{{font-size:28px;font-weight:750;margin-top:8px}}h2{{margin-top:34px}}table{{width:100%;border-collapse:collapse;background:#0f141b;border-radius:16px;overflow:hidden}}th,td{{padding:11px 12px;border-bottom:1px solid #1c2631;text-align:right;font-size:13px}}th:first-child,td:first-child{{text-align:left}}th{{color:#8f9baa}}.pos{{color:#56d89a}}.neg{{color:#ff758a}}a{{color:#8fd7ff}}@media(max-width:800px){{.kpis{{grid-template-columns:1fr 1fr}}.wrap{{padding:18px}}}}</style></head><body><div class="wrap"><a href="../index.html">← Dashboard Home</a><h1>Performance Dashboard</h1><div class="sub">{START.date()} ~ {END.date()} · GA4 attribution + MSSQL product sales</div><div class="kpis" id="k"></div><h2>Channel Health</h2><div style="overflow:auto"><table id="ct"></table></div><h2>Product Movers</h2><div style="overflow:auto"><table id="pt"></table></div><h2>Spike Finder</h2><div style="overflow:auto"><table id="st"></table></div></div><script>const D={data};const won=v=>'₩'+Math.round(v||0).toLocaleString();const pct=v=>(v==null||Number.isNaN(v))?'-':(v*100).toFixed(1)+'%';const n=v=>Math.round(v||0).toLocaleString();k.innerHTML=[['Sessions',n(D.summary.sessions)],['Revenue',won(D.summary.revenue)],['Conversions',n(D.summary.conversions)],['CVR',pct(D.summary.cvr)]].map(x=>`<div class="card"><div class="label">${{x[0]}}</div><div class="value">${{x[1]}}</div></div>`).join('');ct.innerHTML='<tr><th>Channel</th><th>TY Revenue</th><th>LY Revenue</th><th>Revenue YoY</th><th>Sessions YoY</th><th>CVR</th></tr>'+D.channels.map(r=>`<tr><td>${{r.channel}}</td><td>${{won(r.revenue_ty)}}</td><td>${{won(r.revenue_ly)}}</td><td class="${{(r.revenue_yoy||0)>=0?'pos':'neg'}}">${{pct(r.revenue_yoy)}}</td><td>${{pct(r.sessions_yoy)}}</td><td>${{pct(r.cvr_ty)}}</td></tr>`).join('');pt.innerHTML='<tr><th>Product</th><th>TY Revenue</th><th>LY Revenue</th><th>Revenue YoY</th><th>TY Qty</th><th>Discount Δ</th></tr>'+D.products.map(r=>`<tr><td>${{r.product_name}}</td><td>${{won(r.revenue_ty)}}</td><td>${{won(r.revenue_ly)}}</td><td class="${{(r.revenue_yoy||0)>=0?'pos':'neg'}}">${{pct(r.revenue_yoy)}}</td><td>${{n(r.qty_ty)}}</td><td>${{r.discount_delta_pp==null?'-':Number(r.discount_delta_pp).toFixed(1)+'%p'}}</td></tr>`).join('');st.innerHTML='<tr><th>Date</th><th>Product</th><th>Revenue</th><th>7D Avg</th><th>Spike</th></tr>'+D.spikes.map(r=>`<tr><td>${{r.order_date}}</td><td>${{r.product_name}}</td><td>${{won(r.revenue)}}</td><td>${{won(r.revenue_7d_avg)}}</td><td class="pos">${{pct(r.spike_vs_7d)}}</td></tr>`).join('');</script></body></html>'''
    p=OUT/"performance_dashboard.html"; p.write_text(html,encoding="utf-8"); return p

def main():
    print(f"[1/6] BigQuery TY {START.date()} ~ {END.date()}"); bc=load_bq(START,END)
    print(f"[2/6] BigQuery LY {LY_START.date()} ~ {LY_END.date()}"); bl=load_bq(LY_START,LY_END)
    print(f"[3/6] MSSQL TY {START.date()} ~ {END.date()}"); pc=load_sql(START,END)
    print(f"[4/6] MSSQL LY {LY_START.date()} ~ {LY_END.date()}"); pl=load_sql(LY_START,LY_END)
    print("[5/6] YoY / spike calculation"); ch=build_channel(bc,bl); pr=build_products(pc,pl); sp=spikes(pc)
    print("[6/6] Build HTML + Excel"); print(write_html(ch,pr,sp)); print(write_excel(ch,pr,sp,bc,bl,pc,pl))
if __name__=="__main__": main()
