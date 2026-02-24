#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate


def env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or default or "").strip()


def split_recipients(s: str) -> list[str]:
    if not s:
        return []
    parts = re.split(r"[,\s;]+", s.strip())
    return [p for p in (x.strip() for x in parts) if p]


def kst_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=9)))


def kst_yesterday_ymd() -> str:
    return (kst_now() - timedelta(days=1)).strftime("%Y-%m-%d")


def read_file(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def build_subject(kst_stamp: str, mode_daily: bool, mode_voc: bool, mode_blog: bool) -> str:
    mode = "FULL"
    if mode_daily:
        mode = "DAILY ONLY"
    elif mode_voc:
        mode = "VOC ONLY"
    elif mode_blog:
        mode = "BLOG ONLY"
    return f"[CSK E-COMM] Daily Update ({mode}) - {kst_stamp}".strip()


def send_email_html(
    host: str,
    port: int,
    user: str,
    password: str,
    to_list: list[str],
    subject: str,
    html_body: str,
    text_fallback: str = "",
):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ", ".join(to_list)
    msg["Date"] = formatdate(localtime=True)

    if text_fallback:
        msg.attach(MIMEText(text_fallback, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(host, port, timeout=30) as s:
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(user, password)
        s.sendmail(user, to_list, msg.as_string())


# ---------------------------
# Email-friendly formatting
# ---------------------------
def html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )


def strip_tags(html: str) -> str:
    """Very simple HTML -> text. (메일 요약 추출용)"""
    if not html:
        return ""
    # remove script/style
    html = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", " ", html)
    # replace <br>/<p>/<div>/<li> with newlines
    html = re.sub(r"(?i)<br\s*/?>", "\n", html)
    html = re.sub(r"(?i)</(p|div|li|tr|h\d)>", "\n", html)
    # remove tags
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    # unescape minimal entities
    html = html.replace("&nbsp;", " ")
    html = re.sub(r"\s+\n", "\n", html)
    html = re.sub(r"\n\s+", "\n", html)
    html = re.sub(r"[ \t]{2,}", " ", html)
    return html.strip()


def find_kpi(text: str, label: str) -> tuple[str, str]:
    """
    Extract KPI value and delta from text blob like:
      Sessions
      7,691
      전일 대비 -30.2%
    Returns (value, delta) else ("-", "-")
    """
    # label line then value line then optional delta line
    # value could be ₩9,398,769 or 1.03% or 57 etc.
    pat = re.compile(
        rf"(?im)^{re.escape(label)}\s*$\s*^([^\n]+)\s*$\s*(?:^전일\s*대비\s*([^\n]+)\s*$)?",
        re.MULTILINE
    )
    m = pat.search(text)
    if not m:
        return "-", "-"
    val = (m.group(1) or "").strip()
    delta = (m.group(2) or "").strip() if m.group(2) else "-"
    return val, delta


def build_email_html_summary(
    report_date: str,
    hub_url: str,
    daily_url: str,
    weekly_url: str,
    kpis: list[tuple[str, str, str]],
) -> str:
    # inline CSS only
    def badge(text: str) -> str:
        return f"""
        <span style="display:inline-block;padding:6px 10px;border:1px solid #e2e8f0;border-radius:999px;
                     font-size:12px;font-weight:700;color:#0f172a;background:#ffffff;margin-right:6px;">
          {html_escape(text)}
        </span>
        """

    def btn(text: str, url: str, primary: bool = False) -> str:
        bg = "#002d72" if primary else "#ffffff"
        fg = "#ffffff" if primary else "#0f172a"
        bd = "#002d72" if primary else "#e2e8f0"
        return f"""
        <a href="{html_escape(url)}"
           style="display:inline-block;text-decoration:none;border-radius:12px;padding:10px 14px;
                  font-size:12px;font-weight:800;border:1px solid {bd};background:{bg};color:{fg};margin-left:6px;">
          {html_escape(text)}
        </a>
        """

    rows = ""
    for name, val, delta in kpis:
        delta_html = html_escape(delta)
        rows += f"""
        <tr>
          <td style="padding:12px 10px;border-bottom:1px solid #eef2f7;font-weight:800;color:#334155;white-space:nowrap;">
            {html_escape(name)}
          </td>
          <td style="padding:12px 10px;border-bottom:1px solid #eef2f7;font-weight:900;color:#0f172a;">
            {html_escape(val)}
          </td>
          <td style="padding:12px 10px;border-bottom:1px solid #eef2f7;font-weight:800;color:#64748b;white-space:nowrap;">
            {delta_html}
          </td>
        </tr>
        """

    # fallback links if empty
    hub_line = hub_url.rstrip("/") if hub_url else ""
    return f"""\
<!doctype html>
<html>
  <body style="margin:0;background:#f6f8fb;font-family:Arial,Helvetica,sans-serif;color:#0f172a;">
    <div style="max-width:860px;margin:0 auto;padding:22px;">
      <div style="background:#ffffff;border:1px solid #eef2f7;border-radius:18px;padding:18px 18px 14px;">
        <div style="font-size:18px;font-weight:900;margin-bottom:6px;">CSK E-COMM · Daily Digest</div>
        <div style="font-size:13px;color:#64748b;font-weight:700;">
          기준일: <b style="color:#0f172a;">{html_escape(report_date)}</b>
        </div>

        <div style="margin-top:12px;">
          {badge("Top KPIs")}
          {badge("Daily")}
          {badge("KST")}
          <span style="float:right;">
            {btn("Hub", hub_line or "#", primary=False)}
            {btn("Daily", daily_url or "#", primary=True)}
            {btn("Weekly", weekly_url or "#", primary=False)}
          </span>
          <div style="clear:both;"></div>
        </div>
      </div>

      <div style="height:12px;"></div>

      <div style="background:#ffffff;border:1px solid #eef2f7;border-radius:18px;padding:16px;">
        <div style="font-size:14px;font-weight:900;margin-bottom:10px;">Top 5 KPIs</div>
        <table style="width:100%;border-collapse:collapse;">
          <thead>
            <tr>
              <th style="text-align:left;padding:10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#64748b;">KPI</th>
              <th style="text-align:left;padding:10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#64748b;">Value</th>
              <th style="text-align:left;padding:10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#64748b;">DoD</th>
            </tr>
          </thead>
          <tbody>
            {rows}
          </tbody>
        </table>

        <div style="margin-top:12px;font-size:12px;color:#64748b;line-height:1.6;">
          * 메일은 클라이언트 제약(스크립트/CSS 차단) 때문에 “요약형”으로 전송됩니다.<br/>
          * 자세한 리포트는 위 버튼(Daily/Weekly/Hub)에서 확인하세요.
        </div>
      </div>
    </div>
  </body>
</html>
"""


def main():
    smtp_host = env("SMTP_HOST")
    smtp_port = int(env("SMTP_PORT", "587") or "587")
    smtp_user = env("SMTP_USER")
    smtp_pass = env("SMTP_PASS")

    to_list = split_recipients(env("DAILY_RECIPIENTS"))
    if not smtp_host:
        raise RuntimeError("Missing SMTP_HOST")
    if not smtp_user:
        raise RuntimeError("Missing SMTP_USER")
    if not smtp_pass:
        raise RuntimeError("Missing SMTP_PASS")
    if not to_list:
        raise RuntimeError("Missing DAILY_RECIPIENTS")

    hub_url = env("HUB_URL")  # ex) https://xxxx.github.io/repo
    kst_stamp = env("KST_STAMP") or kst_now().strftime("%Y.%m.%d (%a) %H:%M KST")

    mode_daily = env("MODE_DAILY_ONLY").lower() == "true"
    mode_voc = env("MODE_VOC_ONLY").lower() == "true"
    mode_blog = env("MODE_BLOG_ONLY").lower() == "true"

    subject = build_subject(kst_stamp, mode_daily, mode_voc, mode_blog)

    # 기준 리포트 날짜(전일)
    report_date = env("REPORT_DATE") or kst_yesterday_ymd()

    # 리포트 파일(로컬) — 여기서 KPI 텍스트만 뽑아 메일을 만들 것
    default_path = f"reports/daily_digest/daily/{report_date}.html"
    html_path = env("DAILY_DIGEST_HTML_PATH") or default_path
    html_report = read_file(html_path)

    # 링크(메일 CTA)
    base = hub_url.rstrip("/") if hub_url else ""
    daily_url = f"{base}/{default_path}" if base else ""
    weekly_url = f"{base}/reports/daily_digest/weekly/END_{report_date}.html" if base else ""
    hub_link = f"{base}/reports/daily_digest/index.html" if base else ""

    if not html_report:
        # fallback only links
        html_body = build_email_html_summary(
            report_date=report_date,
            hub_url=hub_link,
            daily_url=daily_url,
            weekly_url=weekly_url,
            kpis=[
                ("Sessions", "-", "-"),
                ("Orders", "-", "-"),
                ("Revenue", "-", "-"),
                ("CVR", "-", "-"),
                ("Sign-up Users", "-", "-"),
            ],
        )
        text_fallback = f"Daily Digest {report_date}\nDaily: {daily_url}\nWeekly: {weekly_url}\nHub: {hub_link}"
        print(f"[WARN] Daily report not found: {html_path}")
    else:
        txt = strip_tags(html_report)

        # KPI 5개 추출
        sessions_v, sessions_d = find_kpi(txt, "Sessions")
        orders_v, orders_d = find_kpi(txt, "Orders")
        revenue_v, revenue_d = find_kpi(txt, "Revenue")
        cvr_v, cvr_d = find_kpi(txt, "CVR")
        su_v, su_d = find_kpi(txt, "Sign-up Users")

        kpis = [
            ("Sessions", sessions_v, sessions_d),
            ("Orders", orders_v, orders_d),
            ("Revenue", revenue_v, revenue_d),
            ("CVR", cvr_v, cvr_d),
            ("Sign-up Users", su_v, su_d),
        ]

        html_body = build_email_html_summary(
            report_date=report_date,
            hub_url=hub_link,
            daily_url=daily_url,
            weekly_url=weekly_url,
            kpis=kpis,
        )
        text_fallback = f"CSK E-COMM Daily Digest {report_date}\nDaily: {daily_url}\nWeekly: {weekly_url}\nHub: {hub_link}"
        print(f"[OK] Built email summary from report: {html_path}")

    send_email_html(
        host=smtp_host,
        port=smtp_port,
        user=smtp_user,
        password=smtp_pass,
        to_list=to_list,
        subject=subject,
        html_body=html_body,
        text_fallback=text_fallback,
    )
    print(f"[OK] Sent email to: {', '.join(to_list)}")


if __name__ == "__main__":
    main()
