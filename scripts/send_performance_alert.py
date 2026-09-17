#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Send a conditional HTML alert for Performance Dashboard anomalies."""
import hashlib
import html
import json
import os
import smtplib
import ssl
from collections import defaultdict
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path

OUT = Path(os.getenv("OUT_DIR", "reports"))
DATA = OUT / "performance_data.json"
STATE = OUT / "performance_alert_state.json"
RECIPIENT = os.getenv("PERFORMANCE_ALERT_TO", "hugh.kang@columbia.com")
DASHBOARD_URL = os.getenv("PERFORMANCE_DASHBOARD_URL", "https://koreacolumbia-dotcom.github.io/E-comm-marketing-hub/#performance")
DRY_RUN = os.getenv("PERFORMANCE_ALERT_DRY_RUN", "").lower() in {"1", "true", "yes"}
TODAY = date.today()
EXPECTED = TODAY - timedelta(days=1)


def n(v): return f"{round(float(v or 0)):,}"
def won(v): return "₩" + n(v)
def pct(v): return f"{float(v or 0):,.0f}%"
def esc(v): return html.escape(str(v))


def aggregate(rows):
    out = {"sessions": 0, "purchases": 0, "revenue": 0.0}
    for r in rows:
        out["sessions"] += float(r.get("sessions", 0) or 0)
        out["purchases"] += float(r.get("purchases", 0) or 0)
        out["revenue"] += float(r.get("revenue", 0) or 0)
    return out


def deviation(value, baseline):
    return None if not baseline else (value / baseline - 1) * 100


def add_alert(alerts, severity, title, actual, expected, reason):
    alerts.append({"severity": severity, "title": title, "actual": actual, "expected": expected, "reason": reason})


def build_alerts(payload):
    alerts = []
    ty = payload.get("ty", [])
    ads = [r for r in payload.get("ads", []) if (r.get("period") or "ty") == "ty"]
    ga_dates = sorted({r.get("date") for r in ty if r.get("date")})
    ad_dates = sorted({r.get("date") for r in ads if r.get("date")})
    ga_last = ga_dates[-1] if ga_dates else None
    ad_last = ad_dates[-1] if ad_dates else None
    expected_s = EXPECTED.isoformat()

    if ga_last != expected_s:
        add_alert(alerts, "critical", "GA4 데이터 최신성", ga_last or "데이터 없음", expected_s, "GA4 원천 데이터가 어제까지 갱신되지 않았습니다.")
    if ad_last != expected_s:
        add_alert(alerts, "critical", "광고 데이터 최신성", ad_last or "데이터 없음", expected_s, "광고비·ROAS 원천 데이터가 어제까지 갱신되지 않았습니다.")

    if ga_last:
        latest = aggregate([r for r in ty if r.get("date") == ga_last])
        prev_dates = [d for d in ga_dates if d < ga_last][-7:]
        prev = aggregate([r for r in ty if r.get("date") in prev_dates])
        divisor = max(len(prev_dates), 1)
        avg = {k: prev[k] / divisor for k in prev}
        labels = {"sessions": "Sessions", "purchases": "Purchases", "revenue": "Revenue"}
        formats = {"sessions": n, "purchases": n, "revenue": won}
        limits = {"sessions": 50, "purchases": 60, "revenue": 50}
        for key in labels:
            if latest[key] == 0:
                add_alert(alerts, "critical", f"{labels[key]} 0건", formats[key](latest[key]), f"7일 평균 {formats[key](avg[key])}", "수집 누락 또는 사이트 이벤트 이상 가능성이 있습니다.")
            else:
                delta = deviation(latest[key], avg[key])
                if delta is not None and abs(delta) >= limits[key]:
                    add_alert(alerts, "warning", f"{labels[key]} 급변", formats[key](latest[key]), f"7일 평균 {formats[key](avg[key])}", f"직전 7일 평균 대비 {delta:+.1f}% 변동했습니다.")

    if ad_last:
        day_ads = [r for r in ads if r.get("date") == ad_last]
        spend = sum(float(r.get("spend", 0) or 0) for r in day_ads)
        ga_rev = sum(float(r.get("report_ga_revenue", 0) or 0) for r in day_ads)
        roas = ga_rev / spend * 100 if spend else None
        if spend and (roas < 30 or roas > 1500):
            add_alert(alerts, "warning", "전체 GA ROAS 이상 범위", pct(roas), "30%~1,500%", f"광고비 {won(spend)}, GA 매출 {won(ga_rev)} 기준입니다.")
        for r in day_ads:
            ch_spend = float(r.get("spend", 0) or 0)
            ch_ga = float(r.get("report_ga_revenue", 0) or 0)
            if ch_spend >= 100000 and ch_ga == 0:
                add_alert(alerts, "critical", f"{r.get('channel')} GA 매출 0원", won(ch_ga), f"광고비 {won(ch_spend)}", "광고비는 집행됐지만 GA 전환매출이 없습니다.")
    return alerts, ga_last, ad_last


def render_html(alerts, ga_last, ad_last):
    critical = sum(a["severity"] == "critical" for a in alerts)
    warning = len(alerts) - critical
    rows = []
    for a in alerts:
        color = "#dc2626" if a["severity"] == "critical" else "#d97706"
        label = "긴급 확인" if a["severity"] == "critical" else "확인 권장"
        rows.append(f"""<tr>
<td style="padding:18px;border-bottom:1px solid #e5e7eb;vertical-align:top"><span style="display:inline-block;padding:5px 9px;border-radius:999px;background:{color}15;color:{color};font-size:11px;font-weight:800">{label}</span><div style="margin-top:8px;font-size:15px;font-weight:800;color:#111827">{esc(a['title'])}</div><div style="margin-top:5px;font-size:12px;line-height:1.6;color:#6b7280">{esc(a['reason'])}</div></td>
<td style="padding:18px;border-bottom:1px solid #e5e7eb;vertical-align:top;text-align:right"><div style="font-size:16px;font-weight:800;color:{color}">{esc(a['actual'])}</div><div style="margin-top:6px;font-size:11px;color:#9ca3af">기준: {esc(a['expected'])}</div></td>
</tr>""")
    return f"""<!doctype html><html><body style="margin:0;background:#f3f6fb;font-family:Arial,'Noto Sans KR',sans-serif;color:#111827">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f3f6fb"><tr><td align="center" style="padding:28px 12px">
<table role="presentation" width="680" cellspacing="0" cellpadding="0" style="max-width:680px;width:100%;background:#fff;border-radius:22px;overflow:hidden;box-shadow:0 18px 45px rgba(15,23,42,.10)">
<tr><td style="padding:30px;background:linear-gradient(135deg,#002d72,#1455a0);color:#fff"><div style="font-size:12px;font-weight:800;letter-spacing:1.4px;opacity:.75">CSK E-COMM · PERFORMANCE ALERT</div><div style="margin-top:8px;font-size:26px;font-weight:900">데이터 확인이 필요합니다</div><div style="margin-top:9px;font-size:13px;line-height:1.6;opacity:.82">자동 점검에서 {len(alerts)}개 항목이 기준을 벗어났습니다. 원천 데이터와 캠페인 상태를 확인해주세요.</div></td></tr>
<tr><td style="padding:22px 28px"><table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr><td style="padding:14px;background:#fff1f2;border-radius:14px;text-align:center"><div style="font-size:11px;color:#9f1239">긴급 확인</div><div style="font-size:24px;font-weight:900;color:#be123c">{critical}</div></td><td width="12"></td><td style="padding:14px;background:#fff7ed;border-radius:14px;text-align:center"><div style="font-size:11px;color:#9a3412">확인 권장</div><div style="font-size:24px;font-weight:900;color:#c2410c">{warning}</div></td><td width="12"></td><td style="padding:14px;background:#eff6ff;border-radius:14px;text-align:center"><div style="font-size:11px;color:#1d4ed8">GA4 / 광고 최신일</div><div style="font-size:13px;font-weight:900;color:#1e3a8a">{esc(ga_last or '-')}<br>{esc(ad_last or '-')}</div></td></tr></table></td></tr>
<tr><td style="padding:0 28px 8px"><table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border:1px solid #e5e7eb;border-radius:16px;overflow:hidden">{''.join(rows)}</table></td></tr>
<tr><td align="center" style="padding:24px 28px 30px"><a href="{esc(DASHBOARD_URL)}" style="display:inline-block;padding:14px 24px;border-radius:12px;background:#002d72;color:#fff;text-decoration:none;font-size:14px;font-weight:800">Performance Dashboard 확인</a><div style="margin-top:18px;font-size:11px;color:#9ca3af">동일한 이상 상태는 중복 발송하지 않습니다 · {datetime.now().strftime('%Y-%m-%d %H:%M')}</div></td></tr>
</table></td></tr></table></body></html>"""


def main():
    payload = json.loads(DATA.read_text(encoding="utf-8"))
    alerts, ga_last, ad_last = build_alerts(payload)
    if not alerts:
        print("Performance alert: healthy, no email")
        return
    signature = hashlib.sha256(json.dumps(alerts, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    previous = {}
    if STATE.exists():
        try: previous = json.loads(STATE.read_text(encoding="utf-8"))
        except Exception: pass
    if previous.get("signature") == signature:
        print("Performance alert: duplicate state, no email")
        return
    html_body = render_html(alerts, ga_last, ad_last)
    if DRY_RUN:
        (OUT / "performance_alert_preview.html").write_text(html_body, encoding="utf-8")
        print(f"Performance alert dry-run: {len(alerts)} alerts")
        return
    host = os.getenv("SMTP_HOST", "smtp.office365.com")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER") or os.getenv("MAIL_USERNAME")
    password = os.getenv("SMTP_PASSWORD") or os.getenv("MAIL_PASSWORD")
    sender = os.getenv("SMTP_FROM") or user
    if not user or not password or not sender:
        raise SystemExit("SMTP credentials missing: SMTP_USER/SMTP_PASSWORD (or MAIL_USERNAME/MAIL_PASSWORD)")
    msg = EmailMessage()
    msg["Subject"] = f"[Performance 확인 필요] {EXPECTED.isoformat()} · {len(alerts)}개 이상 징후"
    msg["From"] = sender
    msg["To"] = RECIPIENT
    msg.set_content("Performance Dashboard 데이터 확인이 필요합니다. HTML 메일을 지원하는 환경에서 확인해주세요.")
    msg.add_alternative(html_body, subtype="html")
    with smtplib.SMTP(host, port, timeout=30) as smtp:
        smtp.starttls(context=ssl.create_default_context())
        smtp.login(user, password)
        smtp.send_message(msg)
    STATE.write_text(json.dumps({"signature": signature, "sent_at": datetime.now().isoformat(), "alerts": alerts}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Performance alert sent to {RECIPIENT}: {len(alerts)} alerts")


if __name__ == "__main__":
    main()
