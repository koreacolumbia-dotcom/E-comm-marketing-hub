#!/usr/bin/env python3
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
DATA=ROOT/'reports'/'v2'/'data.json'
HTML=ROOT/'reports'/'v2'/'index.html'

def main():
    d=json.loads(DATA.read_text(encoding='utf-8'))
    data=json.dumps(d,ensure_ascii=False).replace('</','<\\/')
    html='''<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>Marketing Hub v2</title><style>
body{margin:0;background:#f4f7fb;color:#142033;font-family:system-ui,sans-serif}main{max-width:1400px;margin:auto;padding:18px}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:10px}.card{background:#fff;border:1px solid #e7edf5;border-radius:14px;padding:15px}.s12{grid-column:span 12}.s6{grid-column:span 6}.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:7px}.kpi{background:#f4f7fb;border-radius:10px;padding:10px}.kpi b{display:block;font-size:18px}.sub{font-size:11px;color:#718096}.alert{padding:9px 0;border-bottom:1px solid #e7edf5}.critical{color:#dc2626}.warning{color:#d97706}@media(max-width:760px){main{padding:10px 8px 72px}.s6{grid-column:span 12}.kpis{grid-template-columns:repeat(2,1fr)}}
</style></head><body><main><h1>Marketing Hub v2</h1><div class="sub" id="updated"></div><section class="grid"><article class="card s12"><h2>오늘의 핵심 KPI</h2><div id="kpis"></div></article><article class="card s6"><h2>Executive Brief</h2><div id="brief"></div></article><article class="card s6"><h2>Target & Forecast</h2><div id="forecast"></div></article><article class="card s6"><h2>Decision Actions</h2><div id="actions"></div></article><article class="card s6"><h2>Realtime BigQuery Monitoring</h2><div id="realtime"></div></article><article class="card s12"><h2>Alert Center</h2><div id="alerts"></div></article><article class="card s6"><h2>데이터 연결 상태</h2><div id="sources"></div></article><article class="card s6"><h2>PDP Opportunity Center</h2><div id="opps"></div></article></section></main><script>
const D=__DATA__;
const el=id=>document.getElementById(id);const list=x=>Array.isArray(x)?x:[];const obj=x=>x&&typeof x==='object'?x:{};const nf=n=>Number.isFinite(Number(n))?new Intl.NumberFormat('ko-KR',{notation:'compact',maximumFractionDigits:1}).format(Number(n)):'-';const won=n=>nf(n)+(nf(n)==='-'?'':'원');const pct=n=>Number.isFinite(Number(n))?((Math.abs(Number(n))<=1?Number(n)*100:Number(n)).toFixed(1)+'%'):'-';
function render(id,fn){try{el(id).innerHTML=fn()}catch(e){el(id).innerHTML='<p class="critical">표시 오류: '+e.message+'</p>';console.error(id,e)}}
el('updated').textContent=(D.generated_at||'-')+' · '+(D.source_count||0)+'개 집계 소스';
render('kpis',()=>{const m=obj(D.metrics);return '<div class="kpis">'+[['매출',won(m.revenue)],['주문',nf(m.orders)],['세션',nf(m.sessions)],['CVR',pct(m.cvr)],['사용자',nf(m.users)],['AOV',won(m.aov)],['가입',nf(m.signups)]].map(x=>'<div class="kpi"><span class="sub">'+x[0]+'</span><b>'+x[1]+'</b></div>').join('')+'</div>'});
render('brief',()=>{const a=list(D.alerts),r=obj(D.realtime_alerts);return '<p>• '+(a.some(x=>x.level==='critical')?'Critical 경고 '+a.filter(x=>x.level==='critical').length+'건':'현재 Critical 경고 없음')+'</p>'+(r.observed_hour?'<p>• 최근 관측 '+r.observed_hour+'</p>':'')});
render('forecast',()=>{const f=obj(D.forecast);return f.ready?'<b>'+won(f.predicted)+'</b>':'<p class="sub">'+(f.message||'월 누적 데이터 연결 대기')+'</p>'});
render('actions',()=>list(D.actions).map(x=>'<div class="alert"><b>'+x.title+'</b><div class="sub">'+(x.reason||'')+'</div></div>').join('')||'<p class="sub">추천 액션이 없습니다.</p>');
render('realtime',()=>{const r=obj(D.realtime_alerts);return r.status?'<div class="kpis">'+[['상태',String(r.status).toUpperCase()],['알림',nf(r.alert_count)],['Critical',nf(r.critical_count)],['관측',r.observed_hour||'-']].map(x=>'<div class="kpi"><span class="sub">'+x[0]+'</span><b>'+x[1]+'</b></div>').join('')+'</div>':'<p class="sub">실시간 감시 데이터가 없습니다.</p>'});
render('alerts',()=>list(D.alerts).map(x=>'<div class="alert"><b class="'+(x.level||'')+'">'+String(x.level||'info').toUpperCase()+'</b> · <b>'+x.title+'</b><div class="sub">'+(x.impact||'')+'<br>'+(x.cause||'')+'<br>'+(x.action||'')+'</div></div>').join('')||'<p class="sub">활성 경고가 없습니다.</p>');
render('sources',()=>list(D.source_status).slice(0,10).map(x=>'<div class="alert"><b>'+String(x.status||'-').toUpperCase()+'</b><div class="sub">'+(x.path||'-')+' · '+(x.age_hours??'-')+'h</div></div>').join('')||'<p class="sub">연결 소스가 없습니다.</p>');
render('opps',()=>list(D.opportunities).slice(0,15).map((x,i)=>'<div class="alert"><b>'+(i+1)+'. '+(x.name||x.code||'-')+'</b><div class="sub">점수 '+nf(x.score)+' · 예상 추가매출 '+won(x.expected_revenue)+'</div></div>').join('')||'<p class="sub">상품 단위 집계 데이터가 없습니다.</p>');
</script></body></html>'''.replace('__DATA__',data)
    HTML.write_text(html,encoding='utf-8')
    print('[OK] safe V2 renderer completed')

if __name__=='__main__': main()
