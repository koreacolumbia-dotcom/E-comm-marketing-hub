#!/usr/bin/env python3
"""Render the freshest verified Winter Festival daily KPI into the existing page.
Retain prior D1-D3 chart panels with explicit legacy labelling.
No API calls, no extra queries, no user-level data.
"""
from __future__ import annotations
import html
import json
import re
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
P=ROOT/"reports/decision_health/winter_festival_comparison_2026.html"
SRC=ROOT/"reports/decision_health/winter_festival_integrated_latest.json"
START="<!-- CSK_FESTIVAL_LIVE_START -->"
END="<!-- CSK_FESTIVAL_LIVE_END -->"

def money(x):
    return format(round(float(x)),",") + "원"

def comma(x):
    return format(round(float(x)),",")

def change(now,old, suffix="%"):
    if not isinstance(old,(int,float)) or old<=0:return "비교 불가"
    value=(float(now)/float(old)-1)*100
    return ("+" if value>=0 else "")+format(value,".1f")+suffix

def box(label,now,old,formatter=comma):
    return (
      '<div class="kpi"><span>'+html.escape(label)+'</span>'
      '<strong>'+html.escape(formatter(now))+'</strong>'
      '<small>2025년 '+html.escape(formatter(old))
      +' · YoY '+html.escape(change(now,old))+'</small></div>'
    )

def render(event):
    days=event["daily"]
    current=event["totals"]["current"]
    prior=event["totals"]["previous"]
    first=days[0]["current_date"]
    last=days[-1]["current_date"]
    ly_start=days[0]["previous_date"]
    ly_last=days[-1]["previous_date"]
    label="D1–D"+str(len(days))
    finality=days[-1].get("data_finality",event.get("last_day_finality"))
    provisional=(finality=="provisional_intraday")
    lines=[]
    for row in days:
        cur=row["current"];old=row["previous"]
        suffix=" (잠정)" if row.get("data_finality")=="provisional_intraday" else ""
        lines.append('<tr><th>'+html.escape(row["current_date"]+suffix)
          +'</th><td>'+money(cur["revenue"])+'</td><td>'+money(old["revenue"])
          +'</td><td>'+change(cur["revenue"],old["revenue"])
          +'</td><td>'+comma(cur["orders"])+' / '+comma(old["orders"])
          +'</td><td>'+comma(cur["sessions"])+' / '+comma(old["sessions"])+'</td></tr>')
    rate_new=100*current["orders"]/current["sessions"] if current["sessions"] else 0
    rate_old=100*prior["orders"]/prior["sessions"] if prior["sessions"] else 0
    provisional_note=("마지막 일자는 GA4 BigQuery intraday 파티션이 포함된 잠정치입니다. 최종 적재 후 수치가 바뀔 수 있습니다."
       if provisional else "마지막 일자는 GA4 BigQuery 일별 적재 검증치입니다.")
    body=(
       START+'\n'
       '<section class="panel" id="fm-live-daily" data-live-date="'+html.escape(last)+'" '
       'style="border:2px solid #8fc0d9;padding:20px;margin-top:18px">'
       '<div class="viz-eyebrow">LATEST VERIFIED EVENT KPI · '+html.escape(last)+'</div>'
       '<h2 style="margin:7px 0 10px">윈터페스티벌 최신 누적 성과 · '+html.escape(label)+'</h2>'
       '<p class="meta" style="font-size:12px">2026년 '+html.escape(first)+' ~ '+html.escape(last)
       +' vs 2025년 '+html.escape(ly_start)+' ~ '+html.escape(ly_last)
       +' · 동일 요일 / 행사 경과일 비교</p>'
       '<div class="kpis" style="margin-top:13px">'
       +box("누적 매출",current["revenue"],prior["revenue"],money)
       +box("누적 구매",current["orders"],prior["orders"])
       +box("누적 세션",current["sessions"],prior["sessions"])
       +'<div class="kpi"><span>누적 구매 CVR</span><strong>'
       +format(rate_new,'.2f')+'%</strong><small>2025년 '+format(rate_old,'.2f')
       +'% · 차이 '+('+' if rate_new>=rate_old else '')+format(rate_new-rate_old,'.2f')+'%p</small></div>'
       +'</div>'
       '<div class="scroll"><table><thead><tr><th>2026 행사일</th><th>2026 매출</th><th>2025 매출</th>'
       '<th>매출 YoY</th><th>구매 (26/25)</th><th>세션 (26/25)</th></tr></thead><tbody>'
       +''.join(lines)+'</tbody></table></div>'
       '<p class="meta" style="margin:10px 0 0">'
       +html.escape(provisional_note)
       +' · 출처: 날짜별 GA4 BigQuery 검증 데이터. 광고비·상품·채널 상세는 별도 기준일을 확인하세요.'
       +'</p></section>\n'+END
    )
    if len(days)>3:
        body+=('\n<p id="fm-legacy-note" class="meta" style="padding:8px 3px">'
           '아래 상세 차트와 4개 요약 카드는 기존 D1–D3 스냅샷입니다. '
           '최신 '+html.escape(label)+' 누적값은 바로 위 표를 기준으로 확인하세요.</p>\n')
    return body

def main():
    data=json.loads(SRC.read_text(encoding="utf-8"))
    event=data.get("event") or {}
    if event.get("status")!="verified_ga4" or len(event.get("daily") or [])<2:
        print("[FESTIVAL PAGE] no fresh verified daily summary; preserved")
        return
    source=P.read_text(encoding="utf-8")
    widget=render(event)
    if START in source and END in source:
        start=source.index(START)
        # Consume any previously injected D1-D3 legacy badge as well.
        end=source.index(END,start)+len(END)
        trailing=re.match(r'\\s*<p id="fm-legacy-note"[^>]*>.*?</p>\\s*',source[end:],re.S)
        if trailing:end+=trailing.end()
        result=source[:start]+widget+source[end:]
    else:
        assert "</header>" in source
        result=source.replace("</header>","</header>\n"+widget,1)
    result=result.replace("<title>CSK | 2026 vs 2025 윈터페스티벌 D1–D3 성과</title>",
                          "<title>CSK | 2026 vs 2025 윈터페스티벌 최신 누적 성과</title>")
    if result!=source:
        P.write_text(result,encoding="utf-8")
        print("[FESTIVAL PAGE] updated",event["days"],event["daily"][-1]["current_date"])
    else:print("[FESTIVAL PAGE] unchanged",event["days"])

if __name__=="__main__":
    main()
