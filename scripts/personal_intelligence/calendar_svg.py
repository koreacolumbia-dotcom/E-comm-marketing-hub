"""Approved monthly calendar SVG layout. Supply evidence-backed events only."""
from calendar import monthrange
from datetime import date
from html import escape

ROWS=("공식몰 프로모션","LMS","KAKAO","EDM","노스페이스","K2","디스커버리","블랙야크","네파","코오롱스포츠","아이더","내셔널지오그래픽","스노우피크","밀레")
def calendar_svg(year,month,events,today=None):
    days=monthrange(year,month)[1]
    left=89; step=261/days
    esc=lambda s:escape(str(s),quote=True)
    out=['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 360 370" width="100%" font-family="Inter, Pretendard, sans-serif" color="#17334e">']
    def add(s):out.append(s)
    for d in range(1,days+1):
        x=left+(d-1)*step
        if date(year,month,d).weekday()>=5:add(f'<rect x="{x}" y="33" width="{step}" height="292" fill="#98a5b5" opacity=".09"/>')
        add(f'<line x1="{x}" x2="{x}" y1="33" y2="325" stroke="#98a5b5" opacity=".13"/>')
        if d==1 or d%5==0 or d==days:add(f'<text x="{x+step/2}" y="24" font-size="9" text-anchor="middle" fill="currentColor">{d}</text>')
    add('<rect x="0" y="34" width="350" height="20" fill="#28557e" opacity=".09"/><text x="6" y="47" font-size="10" font-weight="bold" fill="currentColor">COLUMBIA</text>')
    for i,name in enumerate(ROWS):
        add(f'<text x="6" y="{67+i*18}" font-size="8.5" fill="currentColor">{esc(name)}</text><line x1="0" x2="350" y1="{73+i*18}" y2="{73+i*18}" stroke="#98a5b5" opacity=".18"/>')
    first,last=date(year,month,1),date(year,month,days)
    for e in events:
        if e.get("row") not in ROWS or not e.get("title"):continue
        try:
            a=date.fromisoformat(e["start"]);b=date.fromisoformat(e.get("end") or e["start"])
        except (KeyError, TypeError, ValueError):
            continue
        if not e.get("end") and e.get("row") not in ("LMS","KAKAO","EDM") and not e.get("single_day"):
            continue
        if b<a or b<first or a>last:continue
        a,b=max(a,first),min(b,last);i=ROWS.index(e["row"])
        x=left+(a.day-1)*step;y=56+i*18;w=(b.day-a.day+1)*step-1
        title=esc(e["title"]);status=esc(e.get("status","unverified"))
        add(f'<g><title>{title} | {status}</title>')
        color="#28557e" if i==0 else "#819db6"
        add(f'<rect x="{x}" y="{y}" width="{w}" height="13" rx="2" fill="{color}" fill-opacity="{0.25 if status in ('planned','예정') else 1}" stroke="{color}" stroke-dasharray="{'3 2' if status in ('planned','예정') else 'none'}"/>')
        clip=f"event-{len(out)}"
        add(f'<clipPath id="{clip}"><rect x="{x}" y="{y}" width="{w}" height="13"/></clipPath><text x="{x+4}" y="{y+9}" font-size="7.5" fill="{color if status in ('planned','예정') else 'white'}" clip-path="url(#{clip})">{title}</text>')
        add('</g>')
    if today and first<=today<=last:
        x=left+(today.day-.5)*step
        add(f'<line x1="{x}" x2="{x}" y1="32" y2="332" stroke="#e99537" stroke-width="1.4"/><text x="{x}" y="345" font-size="9" text-anchor="middle" fill="#e99537">TODAY</text>')
    return "".join(out)+"</svg>"
