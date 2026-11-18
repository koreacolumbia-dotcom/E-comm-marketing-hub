"""Approved monthly calendar SVG layout. Supply evidence-backed events only."""
from calendar import monthrange
from datetime import date
from html import escape
from urllib.parse import urlparse

ROWS=("공식몰 프로모션","LMS","KAKAO","EDM","상품 출시","Paid Media","Meta Creative","노스페이스","K2","디스커버리","블랙야크","네파","코오롱스포츠","아이더","내셔널지오그래픽","스노우피크","밀레")
BRANDS={"blackyak":"블랙야크","discovery":"디스커버리","eider":"아이더","k2":"K2","kolonsport":"코오롱스포츠","millet":"밀레","national_geographic":"내셔널지오그래픽","nepa":"네파","snowpeak":"스노우피크","tnf":"노스페이스"}
def calendar_events(feed, internal=()):
    result=list(internal)
    for e in feed.get("events",[]):
        row=BRANDS.get(e.get("brand"))
        if not row:continue
        exact=e.get("date_status") in ("official_period","first_party_landing_text") and e.get("start_date") and e.get("end_date")
        result.append(dict(row=row,title=e["title"]+(" · "+e["channel"] if e.get("source_type")=="official_retailer_page" else ""),start=e.get("start_date"),end=e.get("end_date"),status="confirmed" if exact else "unverified",url=e.get("source_url"),channel=e.get("channel"),observed_on=e.get("observed_on",[])))
    return result

def calendar_svg(year,month,events,today=None):
    days=monthrange(year,month)[1]
    first,last=date(year,month,1),date(year,month,days)
    left,width=170,1200
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
            match=next((i for i,(aa,bb,ee) in enumerate(merged) if ee["title"]==e["title"] and ee.get("status")==e.get("status") and ee.get("url")==e.get("url") and ee.get("channel")==e.get("channel") and a.toordinal()<=bb.toordinal()+1),None)
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
                color="#28557e" if row in ROWS[:7] else "#6587a5"
                title=esc(e["title"]);clip=f"e{len(out)}"
                opacity=.2 if planned else 1
                dash="4 3" if planned else "none"
                ink=color if planned else "white"
                url=e.get("url") or ""
                link=urlparse(url).scheme=="https" and bool(urlparse(url).hostname)
                if link:add(f'<a href="{esc(url)}" target="_blank" rel="noopener noreferrer">')
                add(f'<g><title>{title}</title><rect x="{x}" y="{y+3}" width="{w}" height="20" rx="4" fill="{color}" fill-opacity="{opacity}" stroke="{color}" stroke-dasharray="{dash}"/><clipPath id="{clip}"><rect x="{x}" y="{y+3}" width="{w}" height="20"/></clipPath><text x="{x+w/2}" y="{y+17}" text-anchor="middle" font-size="11" fill="{ink}" clip-path="url(#{clip})">{title}</text></g>')
                if link:add("</a>")
            y+=26
        add(f'<line x1="0" x2="{width-12}" y1="{y}" y2="{y}" stroke="#e0e7ee"/>');y+=6
    if today and first<=today<=last:
        x=left+(today.day-.5)*step
        add(f'<line x1="{x}" x2="{x}" y1="30" y2="{y}" stroke="#e99537" stroke-width="2"/><text x="{x}" y="{y+14}" text-anchor="middle" font-size="10" fill="#e99537">TODAY</text>')
    y+=32
    for e in unknown:
        add(f'<text x="8" y="{y}" font-size="12" fill="#697887">{esc(e["row"])} · {esc(e["title"])} — 기간/검증 미확인</text>');y+=23
    return "".join(out)+"</svg>"

def calendar_model(end_date, events=None, competitor=None):
    """Explicit event dates only. Page-wide mentions never establish a promotion period."""
    day=date.fromisoformat(end_date)
    entries=list(events or [])
    if day.year==2026 and day.month==11 and not any(e.get("row")=="공식몰 프로모션" and e.get("title")=="컬럼비아 윈터 페스티벌" for e in entries):
        entries.append(dict(row="공식몰 프로모션",title="컬럼비아 윈터 페스티벌",start="2026-11-20",end="2026-11-29",status="planned",source="user supplied schedule"))
    aliases=dict(zip(("TNF","K2","DISCOVERY","BLACKYAK","NEPA","KOLON SPORT","EIDER","NATIONAL GEOGRAPHIC","SNOW PEAK","MILLET"),ROWS[7:]))
    coverage=[]
    for brand in (competitor or {}).get("brands",[]):
        row=aliases.get(brand.get("brand"))
        if not row: continue
        coverage.append(dict(row=row,status=brand.get("status"),pages_checked=brand.get("pages_checked"),errors=brand.get("errors",[])))
        for e in brand.get("verified_events",[]):
            if e.get("verified") is True and e.get("source_url") and e.get("title") and e.get("start") and e.get("end"):
                entries.append(dict(row=row,title=e["title"],start=e["start"],end=e["end"],status="verified",source=e["source_url"]))
    return dict(events=entries,coverage=coverage,svg=calendar_svg(day.year,day.month,entries,day),unknown_is_no_event=False)
