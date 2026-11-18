"""Approved monthly calendar SVG layout. Supply evidence-backed events only."""
from calendar import monthrange
from datetime import date
from html import escape

ROWS=("공식몰 프로모션","LMS","KAKAO","EDM","노스페이스","K2","디스커버리","블랙야크","네파","코오롱스포츠","아이더","내셔널지오그래픽","스노우피크","밀레")
def calendar_svg(year,month,events,today=None):
    days=monthrange(year,month)[1]
    first,last=date(year,month,1),date(year,month,days)
    left,width=150,1000
    step=(width-left-12)/days
    esc=lambda s:escape(str(s),quote=True)
    grouped={r:[] for r in ROWS}
    unknown=[]
    for e in events:
        if e.get("row") not in grouped or not e.get("title"):continue
        try:
            a=date.fromisoformat(e["start"])
            b=date.fromisoformat(e["end"]) if e.get("end") else a if e.get("single_day") else None
        except (KeyError,TypeError,ValueError): b=None
        if b is None or e.get("status") not in ("confirmed","planned","예정","verified"):
            unknown.append(e);continue
        if b<a or b<first or a>last:continue
        grouped[e["row"]].append((max(a,first),min(b,last),e))
    lanes={}
    for row,items in grouped.items():
        merged=[]
        for a,b,e in sorted(items,key=lambda x:(x[0],x[1])):
            match=next((i for i,(aa,bb,ee) in enumerate(merged) if ee["title"]==e["title"] and ee.get("status")==e.get("status") and a.toordinal()<=bb.toordinal()+1),None)
            if match is not None:
                aa,bb,ee=merged[match];merged[match]=(aa,max(bb,b),ee)
            else:merged.append((a,b,e))
        tracks=[]
        for a,b,e in sorted(merged,key=lambda x:x[0]):
            t=next((t for t in tracks if t[-1][1]<a),None)
            if t is None:t=[];tracks.append(t)
            t.append((a,b,e))
        lanes[row]=tracks or [[]]
    height=100+sum(26*len(t)+6 for t in lanes.values())+23*len(unknown)
    out=[f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%" font-family="Pretendard, Inter, sans-serif" color="#17334e">']
    add=out.append
    for d in range(1,days+1):
        x=left+(d-1)*step
        if date(year,month,d).weekday()>=5:add(f'<rect x="{x}" y="32" width="{step}" height="{height-65}" fill="#98a5b5" opacity=".09"/>')
        add(f'<line x1="{x}" x2="{x}" y1="32" y2="{height-33}" stroke="#98a5b5" opacity=".2"/><text x="{x+step/2}" y="22" font-size="11" text-anchor="middle" fill="currentColor">{d}</text>')
    y=36
    for row,tracks in lanes.items():
        add(f'<text x="8" y="{y+17}" font-size="13" fill="currentColor">{esc(row)}</text>')
        for t in tracks:
            for a,b,e in t:
                x=left+(a.day-1)*step;w=(b.day-a.day+1)*step-2
                planned=e.get("status") in ("planned","예정")
                color="#28557e" if row in ROWS[:4] else "#6587a5"
                title=esc(e["title"]);clip=f"e{len(out)}"
                opacity=.2 if planned else 1
                dash="4 3" if planned else "none"
                ink=color if planned else "white"
                add(f'<g><title>{title}</title><rect x="{x}" y="{y+3}" width="{w}" height="20" rx="4" fill="{color}" fill-opacity="{opacity}" stroke="{color}" stroke-dasharray="{dash}"/><clipPath id="{clip}"><rect x="{x}" y="{y+3}" width="{w}" height="20"/></clipPath><text x="{x+5}" y="{y+17}" font-size="11" fill="{ink}" clip-path="url(#{clip})">{title}</text></g>')
            y+=26
        add(f'<line x1="0" x2="{width-12}" y1="{y}" y2="{y}" stroke="#e0e7ee"/>');y+=6
    if today and first<=today<=last:
        x=left+(today.day-.5)*step
        add(f'<line x1="{x}" x2="{x}" y1="30" y2="{y}" stroke="#e99537" stroke-width="2"/><text x="{x}" y="{y+14}" text-anchor="middle" font-size="10" fill="#e99537">TODAY</text>')
    y+=32
    for e in unknown:
        add(f'<text x="8" y="{y}" font-size="12" fill="#697887">{esc(e["row"])} · {esc(e["title"])} — 기간/검증 미확인</text>');y+=23
    return "".join(out)+"</svg>"
