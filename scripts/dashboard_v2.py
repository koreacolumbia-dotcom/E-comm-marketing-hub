#!/usr/bin/env python3
from __future__ import annotations
import json, math, re
from calendar import monthrange
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

KST=timezone(timedelta(hours=9)); ROOT=Path(__file__).resolve().parents[1]
REPORTS=ROOT/'reports'; OUT=REPORTS/'v2'; CONFIG=ROOT/'config'/'dashboard_v2.json'

def load(path:Path, default:Any):
    try:return json.loads(path.read_text(encoding='utf-8'))
    except Exception:return default

def num(v):
    if isinstance(v,(int,float)) and math.isfinite(v):return float(v)
    if isinstance(v,str):
        try:return float(re.sub(r'[^0-9+-.]','',v))
        except:return None
    return None

def deep(obj,names):
    if isinstance(obj,dict):
        for k,v in obj.items():
            if k.lower() in names:return v
        for v in obj.values():
            x=deep(v,names)
            if x is not None:return x
    elif isinstance(obj,list):
        for v in obj:
            x=deep(v,names)
            if x is not None:return x
    return None

def metric(data,*keys):return num(deep(data,{k.lower() for k in keys}))

def decompose(summary):
    revenue=metric(summary,'revenue','current_revenue','sales')
    prev_revenue=metric(summary,'previous_revenue','prev_revenue','revenue_prev')
    sessions=metric(summary,'sessions','current_sessions')
    prev_sessions=metric(summary,'previous_sessions','prev_sessions')
    orders=metric(summary,'orders','purchases','current_orders')
    prev_orders=metric(summary,'previous_orders','prev_orders')
    if not all(v is not None and v>0 for v in [revenue,prev_revenue,sessions,prev_sessions,orders,prev_orders]):
        return {'ready':False,'message':'현재 Summary JSON에 전기 매출·세션·주문 값이 없어 자동 분해를 대기 중입니다.'}
    cvr=orders/sessions; prev_cvr=prev_orders/prev_sessions; aov=revenue/orders; prev_aov=prev_revenue/prev_orders
    session_effect=(sessions-prev_sessions)*prev_cvr*prev_aov
    cvr_effect=sessions*(cvr-prev_cvr)*prev_aov
    aov_effect=sessions*cvr*(aov-prev_aov)
    total=revenue-prev_revenue
    return {'ready':True,'current':revenue,'previous':prev_revenue,'change':total,'effects':[{'name':'세션','value':session_effect},{'name':'CVR','value':cvr_effect},{'name':'AOV','value':aov_effect}]}

def forecast(summary,config):
    now=datetime.now(KST); target=num(config.get('monthly_revenue_target'))
    current=metric(summary,'month_revenue','mtd_revenue','monthly_revenue','revenue')
    if current is None:return {'ready':False,'message':'월 누적 매출 필드가 확인되지 않습니다.'}
    elapsed=max(now.day,1); days=monthrange(now.year,now.month)[1]
    pace=current/elapsed; predicted=pace*days
    out={'ready':True,'current':current,'elapsed_days':elapsed,'days':days,'predicted':predicted,'daily_pace':pace,'target':target}
    if target:
        out['attainment']=current/target*100; out['forecast_attainment']=predicted/target*100; out['gap']=predicted-target; out['required_daily']=max(target-current,0)/max(days-elapsed,1)
    return out

def find_rows(obj):
    candidates=[]
    def walk(x):
        if isinstance(x,list) and x and all(isinstance(i,dict) for i in x[:10]):candidates.append(x)
        elif isinstance(x,dict):
            for v in x.values():walk(v)
        elif isinstance(x,list):
            for v in x:walk(v)
    walk(obj); return max(candidates,key=len) if candidates else []

def first(row,names):
    for k,v in row.items():
        if k.lower() in names:return v
    return None

def opportunities():
    paths=[REPORTS/'product_keyword'/'data.json',REPORTS/'product_keyword'/'summary.json',REPORTS/'product_keyword'/'data'/'summary.json']
    data={}
    for p in paths:
        if p.exists():data=load(p,{});break
    rows=find_rows(data); out=[]
    for r in rows[:5000]:
        name=first(r,{'product_name','item_name','상품명','name'}); code=first(r,{'product_code','item_id','상품코드','sku'})
        sessions=num(first(r,{'sessions','pdp_sessions','views','pageviews','상품조회수'})) or 0
        bounce=num(first(r,{'bounce_rate','exit_rate','pdp_exit_rate','이탈률'}))
        cvr=num(first(r,{'cvr','conversion_rate','구매전환율'}))
        revenue=num(first(r,{'revenue','sales','매출'})) or 0
        stock=num(first(r,{'stock','inventory','재고'}))
        price=num(first(r,{'price','selling_price','판매가'})) or 0
        if bounce is not None and bounce<=1:bounce*=100
        if cvr is not None and cvr<=1:cvr*=100
        if not name and not code:continue
        traffic=min(math.log1p(max(sessions,0))/10,1)
        friction=min(max((bounce or 50)/100,0),1)
        conversion_gap=min(max((2.0-(cvr or 0))/2.0,0),1)
        value=min(math.log1p(max(revenue,price*sessions*.01,0))/18,1)
        availability=0.25 if stock==0 else 1
        score=round(100*(.35*traffic+.25*friction+.25*conversion_gap+.15*value)*availability,1)
        expected_orders=round(max(sessions,0)*max(0,(2.0-(cvr or 0))/100)*.25,1)
        expected_revenue=round(expected_orders*(price or (revenue/max(num(first(r,{'orders','purchases','구매수'})) or 1,1))),0)
        reason=[]
        if sessions>0:reason.append('트래픽 보유')
        if bounce and bounce>=60:reason.append('높은 이탈')
        if cvr is not None and cvr<1.5:reason.append('낮은 CVR')
        if stock==0:reason.append('재고 없음')
        out.append({'name':str(name or code),'code':str(code or ''),'score':score,'sessions':sessions,'bounce':bounce,'cvr':cvr,'revenue':revenue,'stock':stock,'expected_orders':expected_orders,'expected_revenue':expected_revenue,'reason':' · '.join(reason) or '상품 상세 점검'})
    return sorted(out,key=lambda x:x['score'],reverse=True)[:50]

def alerts(meta,summary,opp,ads):
    out=[]
    for key,item in (meta.get('reports') or {}).items():
        if isinstance(item,dict) and item.get('status') in {'stale','missing'}:
            out.append({'level':'critical','title':f'{key} 데이터 {"지연" if item.get("status")=="stale" else "누락"}','impact':f'기준일 {item.get("data_end") or "확인 불가"}','cause':'원천 적재 또는 리포트 생성 작업 확인','link':'../index.html'})
    for label,keys,limit in [('매출',('revenue_wow','wow_revenue'),-10),('주문',('orders_wow','wow_orders'),-10),('CVR',('cvr_wow','wow_cvr'),-.2)]:
        v=metric(summary,*keys)
        if v is not None and v<=limit:out.append({'level':'high','title':f'{label} 급락 감지','impact':f'전주 대비 {v:.1f}{"%p" if label=="CVR" else "%"}','cause':'채널·상품·퍼널 기여도 확인','link':'../index.html'})
    if opp and opp[0]['score']>=70:out.append({'level':'medium','title':'PDP 개선 기회 발견','impact':f'{opp[0]["name"]} 점수 {opp[0]["score"]}','cause':opp[0]['reason'],'link':'#opportunity'})
    if not ads.get('connected'):out.append({'level':'info','title':'광고비 데이터 미연결','impact':'ROAS·CAC 통합 분석 대기','cause':'광고 플랫폼 집계 파일 또는 BigQuery View 설정 필요','link':'#media'})
    return out[:20]

def inject_nav():
    p=ROOT/'index.html'
    if not p.exists():return
    text=p.read_text(encoding='utf-8')
    if 'data-key="hub_v2"' in text:return
    marker='<div class="nav-item active" data-key="summary"'
    i=text.find(marker)
    if i<0:return
    end=text.find('</div>',text.find('</div>',i)+6)+6
    item='\n        <div class="nav-item" data-key="hub_v2" data-target="reports/v2/index.html" data-label="Marketing Hub v2"><i class="fa-solid fa-wand-magic-sparkles"></i><span>Marketing Hub v2</span></div>'
    text=text[:end]+item+text[end:]
    p.write_text(text,encoding='utf-8')

def html_page(payload):
    data=json.dumps(payload,ensure_ascii=False).replace('</','<\\/')
    return '''<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="robots" content="noindex,nofollow"><title>Marketing Hub v2</title><link rel="manifest" href="manifest.webmanifest"><link rel="stylesheet" href="../../assets/dashboard_redesign.css"><style>
:root{color-scheme:light}.dark{color-scheme:dark;--csk-bg:#0b1220;--csk-card:#111a2b;--csk-text:#e8eef9;--csk-muted:#93a4bd;--csk-line:#22304a}.v2{max-width:1500px;margin:auto;padding:28px}.top{display:flex;justify-content:space-between;gap:16px;align-items:center}.eyebrow{font-size:11px;color:#0874e8;font-weight:800;letter-spacing:.13em}.top h1{margin:6px 0;font-size:34px}.tools{display:flex;gap:8px}.btn{border:1px solid var(--csk-line);background:var(--csk-card);padding:10px 13px;border-radius:12px;font-weight:700}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:14px;margin-top:18px}.card{background:var(--csk-card);border:1px solid var(--csk-line);border-radius:18px;padding:20px;box-shadow:var(--csk-shadow)}.span12{grid-column:span 12}.span8{grid-column:span 8}.span6{grid-column:span 6}.span4{grid-column:span 4}.metric{font-size:30px;font-weight:800;letter-spacing:-.05em}.sub{font-size:12px;color:var(--csk-muted)}.effects{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.effect{padding:13px;background:var(--csk-bg);border-radius:13px}.alert{display:grid;grid-template-columns:90px 1fr auto;gap:12px;padding:13px 0;border-bottom:1px solid var(--csk-line)}.level{font-size:11px;font-weight:800}.critical,.high{color:#dc2626}.medium{color:#d97706}.info{color:#0874e8}.opp{display:grid;grid-template-columns:48px minmax(180px,1fr) repeat(4,minmax(90px,.5fr));gap:12px;align-items:center;padding:12px 0;border-bottom:1px solid var(--csk-line)}.score{width:42px;height:42px;border-radius:12px;background:#eaf4ff;color:#0874e8;display:grid;place-items:center;font-weight:800}.palette{position:fixed;inset:0;background:rgba(5,12,24,.5);display:none;place-items:start center;padding-top:12vh;z-index:99}.palette.open{display:grid}.palette-box{width:min(620px,92vw);background:var(--csk-card);border-radius:18px;padding:14px}.palette input{width:100%;padding:13px}.palette a{display:block;padding:12px;border-radius:10px;color:inherit;text-decoration:none}.palette a:hover{background:var(--csk-bg)}@media(max-width:850px){.v2{padding:16px 14px 90px}.top{align-items:flex-start}.top h1{font-size:27px}.span8,.span6,.span4{grid-column:span 12}.effects{grid-template-columns:1fr}.opp{grid-template-columns:44px 1fr}.opp>*:nth-child(n+3){display:none}.alert{grid-template-columns:70px 1fr}.alert a{display:none}}
</style></head><body><main class="v2"><header class="top"><div><div class="eyebrow">E-COMMERCE MARKETING HUB V2</div><h1>Decision Intelligence</h1><div class="sub" id="updated"></div></div><div class="tools"><button class="btn" id="cmd">⌘ K</button><button class="btn" id="theme">◐</button></div></header><section class="grid"><article class="card span8"><h2>Executive Brief 2.0</h2><div id="brief"></div></article><article class="card span4"><h2>Target & Forecast</h2><div id="forecast"></div></article><article class="card span6"><h2>매출 변화 원인 분해</h2><div id="decomp"></div></article><article class="card span6" id="media"><h2>Paid Media 통합</h2><div id="ads"></div></article><article class="card span12"><h2>Alert Center</h2><div id="alerts"></div></article><article class="card span12" id="opportunity"><h2>PDP Opportunity Center</h2><div class="sub">상품을 클릭하면 기존 상품 리포트로 연결됩니다.</div><div id="opps"></div></article></section></main><div class="palette" id="palette"><div class="palette-box"><input id="search" placeholder="메뉴 또는 기능 검색"><div id="commands"></div></div></div><script>const D='''+data+''';const won=n=>n==null?'-':new Intl.NumberFormat('ko-KR',{notation:'compact',maximumFractionDigits:1}).format(n)+'원';const pct=n=>n==null?'-':Number(n).toFixed(1)+'%';document.querySelector('#updated').textContent='Updated '+D.generated_at;let brief=[];if(D.alerts.filter(x=>['critical','high'].includes(x.level)).length)brief.push('핵심 경고 '+D.alerts.filter(x=>['critical','high'].includes(x.level)).length+'건이 있습니다.');else brief.push('핵심 운영 지표는 설정된 경고 범위 안에 있습니다.');if(D.decomposition.ready){const e=[...D.decomposition.effects].sort((a,b)=>Math.abs(b.value)-Math.abs(a.value))[0];brief.push('매출 변화의 가장 큰 기여 요인은 '+e.name+'이며 영향액은 '+won(e.value)+'입니다.')}if(D.forecast.ready)brief.push('현재 페이스 기준 월말 예상 매출은 '+won(D.forecast.predicted)+'입니다.');if(D.opportunities.length)brief.push('PDP 최우선 개선 상품은 '+D.opportunities[0].name+'입니다.');document.querySelector('#brief').innerHTML=brief.map(x=>'<p>• '+x+'</p>').join('');const F=D.forecast;document.querySelector('#forecast').innerHTML=F.ready?'<div class="metric">'+won(F.predicted)+'</div><div class="sub">월말 예상</div><p>현재 '+won(F.current)+'</p>'+(F.target?'<p>목표 '+won(F.target)+' · 예상 달성률 '+pct(F.forecast_attainment)+'</p><p>필요 일평균 '+won(F.required_daily)+'</p>':'<p class="sub">config/dashboard_v2.json에 월 목표를 설정하세요.</p>'):'<p>'+F.message+'</p>';const C=D.decomposition;document.querySelector('#decomp').innerHTML=C.ready?'<div class="metric">'+won(C.change)+'</div><div class="effects">'+C.effects.map(x=>'<div class="effect"><b>'+x.name+'</b><div>'+won(x.value)+'</div></div>').join('')+'</div>':'<p>'+C.message+'</p>';document.querySelector('#ads').innerHTML=D.ads.connected?'<div class="metric">'+pct(D.ads.roas)+'</div><div class="sub">통합 ROAS</div>':'<p>광고비 집계 데이터가 아직 연결되지 않았습니다.</p><div class="sub">Google·Meta·Naver·Kakao 집계 View를 연결하면 Spend, ROAS, CAC가 표시됩니다.</div>';document.querySelector('#alerts').innerHTML=D.alerts.map(x=>'<div class="alert"><span class="level '+x.level+'">'+x.level.toUpperCase()+'</span><div><b>'+x.title+'</b><div class="sub">'+x.impact+' · '+x.cause+'</div></div><a href="'+x.link+'">열기</a></div>').join('')||'<p>현재 활성 경고가 없습니다.</p>';document.querySelector('#opps').innerHTML=D.opportunities.slice(0,20).map((x,i)=>'<a class="opp" href="../product_keyword/index.html"><span class="score">'+Math.round(x.score)+'</span><span><b>'+(i+1)+'. '+x.name+'</b><div class="sub">'+x.reason+'</div></span><span>'+Math.round(x.sessions).toLocaleString()+' 세션</span><span>'+pct(x.cvr)+'</span><span>'+won(x.expected_revenue)+'</span><span>'+won(x.revenue)+'</span></a>').join('')||'<p>상품 JSON 컬럼 매핑 후 우선순위가 표시됩니다.</p>';const commands=[['Summary','../index.html'],['상품 성과','../product_keyword/index.html'],['Funnel','../daily_digest/owned_funnel_tab.html'],['소스/매체','../utm_channel/index.html'],['PDP Opportunity','#opportunity'],['Alert Center','#alerts']];const pal=document.querySelector('#palette'),render=q=>document.querySelector('#commands').innerHTML=commands.filter(x=>x[0].toLowerCase().includes(q.toLowerCase())).map(x=>'<a href="'+x[1]+'">'+x[0]+'</a>').join('');render('');document.querySelector('#cmd').onclick=()=>{pal.classList.add('open');document.querySelector('#search').focus()};document.querySelector('#search').oninput=e=>render(e.target.value);pal.onclick=e=>{if(e.target===pal)pal.classList.remove('open')};document.onkeydown=e=>{if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'){e.preventDefault();document.querySelector('#cmd').click()}if(e.key==='Escape')pal.classList.remove('open')};document.querySelector('#theme').onclick=()=>{document.documentElement.classList.toggle('dark');localStorage.setItem('v2theme',document.documentElement.classList.contains('dark')?'dark':'light')};if(localStorage.getItem('v2theme')==='dark')document.documentElement.classList.add('dark');if('serviceWorker'in navigator)navigator.serviceWorker.register('sw.js');</script></body></html>'''

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    summary=load(REPORTS/'summary.json',{}); meta=load(REPORTS/'meta.json',{}); config=load(CONFIG,{})
    ads=load(REPORTS/'paid_media'/'summary.json',{'connected':False}); ads['connected']=bool(ads and any(k in ads for k in ['spend','roas','platforms']))
    opp=opportunities(); payload={'generated_at':datetime.now(KST).strftime('%Y-%m-%d %H:%M KST'),'decomposition':decompose(summary),'forecast':forecast(summary,config),'opportunities':opp,'ads':ads}
    payload['alerts']=alerts(meta,summary,opp,ads)
    (OUT/'data.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (OUT/'index.html').write_text(html_page(payload),encoding='utf-8')
    (OUT/'manifest.webmanifest').write_text(json.dumps({'name':'CSK Marketing Hub v2','short_name':'CSK Hub','start_url':'./','display':'standalone','background_color':'#f5f7fb','theme_color':'#0874e8'},ensure_ascii=False),encoding='utf-8')
    (OUT/'sw.js').write_text("const C='csk-v2-1';self.addEventListener('install',e=>e.waitUntil(caches.open(C).then(c=>c.addAll(['./','index.html','data.json']))));self.addEventListener('fetch',e=>e.respondWith(fetch(e.request).catch(()=>caches.match(e.request))));",encoding='utf-8')
    inject_nav(); print('[OK] generated Marketing Hub v2')
if __name__=='__main__':main()
