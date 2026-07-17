#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
ALERTS=ROOT/'reports'/'realtime_alerts'/'alerts.json'
V2_DATA=ROOT/'reports'/'v2'/'data.json'
V2_HTML=ROOT/'reports'/'v2'/'index.html'
INDEX=ROOT/'index.html'


def main() -> int:
    if not ALERTS.exists() or not V2_DATA.exists() or not V2_HTML.exists():
        raise SystemExit('[ERROR] realtime alerts or V2 output missing')
    realtime=json.loads(ALERTS.read_text(encoding='utf-8'))
    v2=json.loads(V2_DATA.read_text(encoding='utf-8'))
    existing=[x for x in (v2.get('alerts') or []) if x.get('category')!='realtime']
    injected=[]
    for a in realtime.get('alerts',[])[:40]:
        injected.append({
            'level':a.get('level','warning'),
            'category':'realtime',
            'title':a.get('title','실시간 이상 징후'),
            'impact':a.get('impact',''),
            'cause':a.get('cause',''),
            'action':a.get('action','세부 리포트 확인'),
            'link':a.get('link','../realtime_alerts/index.html'),
            'detected_at':a.get('detected_at'),
            'metric':a.get('metric'),
            'dimension_type':a.get('dimension_type'),
            'dimension_value':a.get('dimension_value'),
        })
    rank={'critical':0,'warning':1,'info':2}
    v2['alerts']=sorted(injected+existing,key=lambda x:rank.get(x.get('level'),9))[:50]
    v2['realtime_alerts']={
        'status':realtime.get('status'),
        'observed_hour':realtime.get('observed_hour'),
        'alert_count':realtime.get('alert_count',0),
        'critical_count':realtime.get('critical_count',0),
        'warning_count':realtime.get('warning_count',0),
        'top_alerts':realtime.get('alerts',[])[:10],
    }
    actions=[x for x in (v2.get('actions') or []) if x.get('reason')!='Realtime anomaly']
    for a in realtime.get('alerts',[])[:3]:
        actions.insert(0,{'priority':a.get('level','warning'),'title':a.get('action','실시간 이상 확인'),'reason':'Realtime anomaly','link':a.get('link','../realtime_alerts/index.html')})
    v2['actions']=actions[:10]
    V2_DATA.write_text(json.dumps(v2,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')

    text=V2_HTML.read_text(encoding='utf-8')
    embedded=json.dumps(v2,ensure_ascii=False).replace('</','<\\/')
    text,count=re.subn(r'const D=.*?;const won=', 'const D='+embedded+';const won=', text, count=1, flags=re.S)
    if count!=1:
        raise SystemExit('[ERROR] Could not replace V2 embedded payload')
    if 'id="realtimeStatus"' not in text:
        block='<article class="card s12" id="realtime"><h2>Realtime BigQuery Monitoring</h2><div id="realtimeStatus"></div></article>'
        text=text.replace('<article class="card s12" id="alerts">',block+'<article class="card s12" id="alerts">',1)
        marker="document.querySelector('#actions').innerHTML="
        js="document.querySelector('#realtimeStatus').innerHTML=D.realtime_alerts?'<div class=\"kpis\"><div class=\"kpi\"><span class=\"sub\">Status</span><b>'+String(D.realtime_alerts.status||'-').toUpperCase()+'</b></div><div class=\"kpi\"><span class=\"sub\">Alerts</span><b>'+D.realtime_alerts.alert_count+'</b></div><div class=\"kpi\"><span class=\"sub\">Critical</span><b>'+D.realtime_alerts.critical_count+'</b></div><div class=\"kpi\"><span class=\"sub\">Observed</span><b style=\"font-size:12px\">'+(D.realtime_alerts.observed_hour||'-')+'</b></div></div>':'<p class=\"sub\">실시간 감시 데이터가 없습니다.</p>';"
        text=text.replace(marker,js+marker,1)
    text=text.replace("['Alert Center','#alerts']", "['Alert Center','#alerts'],['Realtime Alerts','../realtime_alerts/index.html']")
    V2_HTML.write_text(text,encoding='utf-8')

    if INDEX.exists():
        root=INDEX.read_text(encoding='utf-8')
        if 'data-key="realtime_alerts"' not in root:
            needle='<div class="nav-item" data-key="alert_center"'
            start=root.find(needle)
            if start<0:
                needle='<div class="nav-item" data-key="hub_v2"'
                start=root.find(needle)
            if start>=0:
                end=root.find('</div>',root.find('</div>',start)+6)+6
                item='\n        <div class="nav-item" data-key="realtime_alerts" data-target="reports/realtime_alerts/index.html" data-label="Realtime Alerts"><i class="fa-solid fa-bolt"></i><span>Realtime Alerts</span></div>'
                root=root[:end]+item+root[end:]
                INDEX.write_text(root,encoding='utf-8')
    print(f"[OK] Connected {len(injected)} realtime alerts to V2")
    return 0

if __name__=='__main__':
    raise SystemExit(main())
