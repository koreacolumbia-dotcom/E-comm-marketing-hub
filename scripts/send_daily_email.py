#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import smtplib
from urllib.parse import urljoin
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate


def env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def split_recipients(s: str) -> list[str]:
    if not s:
        return []
    parts = re.split(r"[,;\s]+", s.strip())
    return [p for p in (x.strip() for x in parts) if p]


def read_file(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


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


def build_subject(kst_stamp: str, mode_daily: bool, mode_voc: bool, mode_blog: bool) -> str:
    mode = "FULL"
    if mode_daily:
        mode = "DAILY ONLY"
    elif mode_voc:
        mode = "VOC ONLY"
    elif mode_blog:
        mode = "BLOG ONLY"
    return f"[CSK E-COMM] Daily Update ({mode}) - {kst_stamp}".strip()


def _extract_after_label(html: str, label_patterns: list[str], max_window: int = 700) -> str:
    if not html:
        return ""
    s = re.sub(r"\s+", " ", html)
    for pat in label_patterns:
        m = re.search(pat, s, flags=re.IGNORECASE)
        if not m:
            continue
        win = s[m.end(): m.end() + max_window]

        # currency
        m2 = re.search(r"(₩\s?[\d,]+(?:\.\d+)?)", win)
        if m2:
            return m2.group(1).replace(" ", "")

        # percent
        m2 = re.search(r"([\d]+(?:\.[\d]+)?\s?%)", win)
        if m2:
            return m2.group(1).replace(" ", "")

        # plain number (with commas)
        m2 = re.search(r"(\d[\d,]*)(?![\d,])", win)
        if m2:
            return m2.group(1)

    return ""


def extract_kpis_from_daily_html(html: str) -> dict:
    return {
        "Sessions": _extract_after_label(html, [r"\bSessions\b"]),
        "Orders": _extract_after_label(html, [r"\bOrders\b"]),
        "Revenue": _extract_after_label(html, [r"\bRevenue\b", r"\bSales\b"]),
        "CVR": _extract_after_label(html, [r"\bCVR\b", r"\bConversion\s*Rate\b"]),
        "Sign-up Users": _extract_after_label(html, [r"Sign[- ]?up\s*Users", r"Sign[- ]?ups?"]),
    }


def build_email_summary_html(
    *,
    kst_stamp: str,
    report_date: str,
    hub_url: str,
    daily_url: str,
    weekly_url: str,
    kpis: dict,
) -> str:
    def k(v: str) -> str:
        return (v or "-").strip()

    def btn(label: str, url: str, primary: bool = False) -> str:
        if not url:
            return (
                '<span style="display:inline-block;padding:12px 14px;border-radius:14px;'
                'border:1px solid #e5e7eb;background:#f3f4f6;color:#9ca3af;'
                'font-weight:800;font-size:13px;">'
                f"{label}</span>"
            )
        bg = "#002d72" if primary else "#ffffff"
        bd = "#002d72" if primary else "#e5e7eb"
        fg = "#ffffff" if primary else "#0f172a"
        return (
            f'<a href="{url}" style="display:inline-block;padding:12px 14px;border-radius:14px;'
            f'border:1px solid {bd};background:{bg};color:{fg};text-decoration:none;'
            'font-weight:900;font-size:13px;">'
            f"{label}</a>"
        )

    return f"""
<div style="margin:0;padding:0;background:#f6f8fb;">
  <div style="max-width:760px;margin:0 auto;padding:18px 14px;">
    <div style="background:#ffffff;border:1px solid #eef2f7;border-radius:20px;overflow:hidden;
                box-shadow:0 18px 45px rgba(0,45,114,0.06);">
      <div style="padding:18px 18px 14px 18px;background:linear-gradient(180deg,#ffffff,#f8fafc);">
        <div style="font-weight:900;font-size:18px;color:#0f172a;margin:0;">
          CSK E-COMM Daily Update
        </div>
        <div style="margin-top:6px;color:#64748b;font-size:13px;font-weight:700;">
          Report date: <span style="color:#0f172a;font-weight:900;">{report_date}</span>
          <span style="color:#94a3b8;font-weight:800;"> · </span>
          Sent: {kst_stamp}
        </div>
      </div>

      <div style="padding:16px 18px;">
        <div style="display:flex;flex-wrap:wrap;gap:10px;margin-bottom:14px;">
          {btn("Open Daily", daily_url, True)}
          {btn("Open Weekly (7D)", weekly_url, False)}
          {btn("Open Hub", hub_url, False)}
        </div>

        <div style="border:1px solid #eef2f7;border-radius:18px;background:#ffffff;padding:14px;">
          <div style="font-size:12px;font-weight:900;letter-spacing:.18em;text-transform:uppercase;color:#94a3b8;">
            Top KPIs
          </div>

          <table role="presentation" cellpadding="0" cellspacing="0"
                 style="width:100%;margin-top:10px;border-collapse:separate;border-spacing:0 10px;">
            <tr>
              <td style="width:50%;padding-right:10px;">
                <div style="border:1px solid #eef2f7;border-radius:16px;padding:12px;background:#ffffff;">
                  <div style="color:#64748b;font-size:11px;font-weight:900;letter-spacing:.14em;text-transform:uppercase;">Sessions</div>
                  <div style="margin-top:6px;font-size:18px;font-weight:900;color:#0f172a;">{k(kpis.get("Sessions"))}</div>
                </div>
              </td>
              <td style="width:50%;padding-left:10px;">
                <div style="border:1px solid #eef2f7;border-radius:16px;padding:12px;background:#ffffff;">
                  <div style="color:#64748b;font-size:11px;font-weight:900;letter-spacing:.14em;text-transform:uppercase;">Orders</div>
                  <div style="margin-top:6px;font-size:18px;font-weight:900;color:#0f172a;">{k(kpis.get("Orders"))}</div>
                </div>
              </td>
            </tr>

            <tr>
              <td style="width:50%;padding-right:10px;">
                <div style="border:1px solid #eef2f7;border-radius:16px;padding:12px;background:#ffffff;">
                  <div style="color:#64748b;font-size:11px;font-weight:900;letter-spacing:.14em;text-transform:uppercase;">Revenue</div>
                  <div style="margin-top:6px;font-size:18px;font-weight:900;color:#0f172a;">{k(kpis.get("Revenue"))}</div>
                </div>
              </td>
              <td style="width:50%;padding-left:10px;">
                <div style="border:1px solid #eef2f7;border-radius:16px;padding:12px;background:#ffffff;">
                  <div style="color:#64748b;font-size:11px;font-weight:900;letter-spacing:.14em;text-transform:uppercase;">CVR</div>
                  <div style="margin-top:6px;font-size:18px;font-weight:900;color:#0f172a;">{k(kpis.get("CVR"))}</div>
                </div>
              </td>
            </tr>

            <tr>
              <td style="width:50%;padding-right:10px;">
                <div style="border:1px solid #eef2f7;border-radius:16px;padding:12px;background:#ffffff;">
                  <div style="color:#64748b;font-size:11px;font-weight:900;letter-spacing:.14em;text-transform:uppercase;">Sign-up Users</div>
                  <div style="margin-top:6px;font-size:18px;font-weight:900;color:#0f172a;">{k(kpis.get("Sign-up Users"))}</div>
                </div>
              </td>
              <td style="width:50%;padding-left:10px;">
                <div style="border:1px solid #eef2f7;border-radius:16px;padding:12px;background:#f8fafc;color:#64748b;">
                  <div style="font-size:11px;font-weight:900;letter-spacing:.14em;text-transform:uppercase;">Notes</div>
                  <div style="margin-top:6px;font-size:12px;font-weight:800;line-height:1.5;">
                    이메일은 요약만 제공합니다.<br/>상세는 버튼으로 리포트에서 확인하세요.
                  </div>
                </div>
              </td>
            </tr>
          </table>
        </div>

        <div style="margin-top:14px;color:#94a3b8;font-size:11px;font-weight:700;">
          This email was sent automatically by GitHub Actions.
        </div>
      </div>
    </div>
  </div>
</div>
""".strip()


def main():
    smtp_host = env("SMTP_HOST")
    smtp_port = int(env("SMTP_PORT", "587") or "587")
    smtp_user = env("SMTP_USER")
    smtp_pass = env("SMTP_PASS")

    to_list = split_recipients(env("DAILY_RECIPIENTS"))

    if not smtp_host:
        raise RuntimeError("Missing SMTP_HOST (vars.SMTP_HOST)")
    if not smtp_user:
        raise RuntimeError("Missing SMTP_USER (secrets.SMTP_USER)")
    if not smtp_pass:
        raise RuntimeError("Missing SMTP_PASS (secrets.SMTP_PASS)")
    if not to_list:
        raise RuntimeError("Missing DAILY_RECIPIENTS (vars.DAILY_RECIPIENTS)")

    hub_url = env("HUB_URL")
    kst_stamp = env("KST_STAMP")

    mode_daily = env("MODE_DAILY_ONLY").lower() == "true"
    mode_voc = env("MODE_VOC_ONLY").lower() == "true"
    mode_blog = env("MODE_BLOG_ONLY").lower() == "true"

    subject = build_subject(kst_stamp, mode_daily, mode_voc, mode_blog)

    report_date = env("REPORT_DATE")  # 2026-02-23
    daily_html_path = env("DAILY_DIGEST_HTML_PATH")  # reports/daily_digest/daily/2026-02-23.html

    daily_html = read_file(daily_html_path) if daily_html_path else ""
    kpis = extract_kpis_from_daily_html(daily_html)

    daily_rel = env("DAILY_DIGEST_REL_URL") or (f"reports/daily_digest/daily/{report_date}.html" if report_date else "")
    weekly_rel = env("WEEKLY_DIGEST_REL_URL") or (f"reports/daily_digest/weekly/END_{report_date}.html" if report_date else "")

    base = hub_url + ("" if hub_url.endswith("/") else "/") if hub_url else ""
    daily_url = urljoin(base, daily_rel) if base and daily_rel else ""
    weekly_url = urljoin(base, weekly_rel) if base and weekly_rel else ""

    html_body = build_email_summary_html(
        kst_stamp=kst_stamp,
        report_date=report_date or "-",
        hub_url=hub_url,
        daily_url=daily_url,
        weekly_url=weekly_url,
        kpis=kpis,
    )

    text_fallback = "\n".join(
        [
            f"CSK E-COMM Daily Update - {kst_stamp}",
            f"Report date: {report_date or '-'}",
            f"Daily: {daily_url or '(no link)'}",
            f"Weekly: {weekly_url or '(no link)'}",
            f"Hub: {hub_url or '(no link)'}",
        ]
    ).strip()

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
    print(f"[OK] Sent email to: {', '.join(to_list)} | report_date={report_date}")


if __name__ == "__main__":
    main()
