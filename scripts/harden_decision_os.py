#!/usr/bin/env python3
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
DATA=ROOT/'reports'/'decision_os'/'data.json'
STATE=ROOT/'reports'/'decision_os'/'state.json'
V2=ROOT/'reports'/'v2'/'data.json'


def load(path):
    return json.loads(path.read_text(encoding='utf-8'))


def num(v):
    try: return float(v or 0)
    except Exception: return 0.0


def is_loss_alert(a):
    cur=a.get('current'); base=a.get('baseline'); delta=a.get('delta_pct')
    if cur is not None and base is not None:
        return num(cur) < num(base)
    if delta is not None:
        return num(delta) < 0
    title=str(a.get('title',''))
    return any(x in title for x in ('급락','하락','저하','누락','지연','이상')) and '급증' not in title


def family(a):
    metric=str(a.get('metric','')).lower()
    category=str(a.get('category','')).lower()
    if category=='data_quality' or any(x in metric for x in ('integrity','duplicate','latency','missing','zero_revenue')):
        return 'data_quality'
    if any(x in metric for x in ('revenue','orders','cvr','purchase')): return 'commerce'
    if any(x in metric for x in ('view_item','add_to_cart','checkout','page_view')): return 'funnel'
    if any(x in metric for x in ('session','users','event_count')): return 'traffic'
    return 'other'


def main():
    d=load(DATA)
    incidents=d.get('incidents',[])
    for inc in incidents:
        inc['family']=family((inc.get('alerts') or [{}])[0])
        if not any(is_loss_alert(a) for a in inc.get('alerts',[])):
            inc['revenue_at_risk']={'hourly':0,'today':0,'seven_day':0,'recoverable':0}
        inc['risk_basis']='negative anomalies only'
    incidents.sort(key=lambda x:(x.get('level')!='critical',-num((x.get('revenue_at_risk') or {}).get('today'))))
    d['incidents']=incidents
    d['revenue_risk']={
        'today':sum(num((x.get('revenue_at_risk') or {}).get('today')) for x in incidents),
        'seven_day':sum(num((x.get('revenue_at_risk') or {}).get('seven_day')) for x in incidents),
        'recoverable':sum(num((x.get('revenue_at_risk') or {}).get('recoverable')) for x in incidents),
    }
    cc=d.get('command_center',{})
    cc['revenue_at_risk_today']=d['revenue_risk']['today']
    cc['headline']=incidents[0]['title'] if incidents else '현재 중대한 이상 징후 없음'
    d['command_center']=cc
    DATA.write_text(json.dumps(d,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    state=load(STATE) if STATE.exists() else {}
    state['incidents']=incidents
    STATE.write_text(json.dumps(state,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    v2=load(V2)
    v2['decision_os']=d
    V2.write_text(json.dumps(v2,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(f"[DECISION_OS_HARDENED] incidents={len(incidents)} loss_risk={d['revenue_risk']['today']:,.0f}")

if __name__=='__main__': main()
