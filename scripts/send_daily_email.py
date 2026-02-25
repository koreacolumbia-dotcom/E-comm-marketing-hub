#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
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


def inject_base_href(html: str, base_href: str) -> str:
    """
    리포트 내부 상대경로(assets/..., data/...)가 조금이라도 동작하게 <base href="..."> 주입.
    - 이미 <base> 있으면 그대로 둠
    - <head> 없으면 그대로 반환
    """
    if not html or not base_href:
        return html

    if re.search(r"(?is)<base\s+[^>]*href=", html):
        return html

    m = re.search(r"(?is)<head[^>]*>", html)
    if not m:
        return html

    insert_at = m.end()
    base_tag = f'\n  <base href="{base_href.rstrip("/") + "/"}">\n'
    return html[:insert_at] + base_tag + html[insert_at:]


def send_email_html_full_report(
    host: str,
    port: int,
    user: str,
    password: str,
    to_list: list[str],
    subject: str,
    html_body: str,
    text_fallback: str,
    attach_name: str | None = None,
    attach_bytes: bytes | None = None,
):
    """
    - multipart/mixed
      - multipart/alternative (plain + html)
      - attachment (optional): 원본 리포트 html
    """
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ", ".join(to_list)
    msg["Date"] = formatdate(localtime=True)

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(text_fallback or "", "plain", "utf-8"))
    alt.attach(MIMEText(html_body or "", "html", "utf-8"))
    msg.attach(alt)

    if attach_name and attach_bytes:
        part = MIMEApplication(attach_bytes, _subtype="html")
        part.add_header("Content-Disposition", "attachment", filename=attach_name)
        msg.attach(part)

    with smtplib.SMTP(host, port, timeout=30) as s:
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(user, password)
        s.sendmail(user, to_list, msg.as_string())


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

    # ✅ 기준 리포트 날짜(전일)
    report_date = env("REPORT_DATE") or kst_yesterday_ymd()

    # ✅ 어제 리포트 파일(로컬)
    default_path = f"reports/daily_digest/daily/{report_date}.html"
    html_path = env("DAILY_DIGEST_HTML_PATH") or default_path
    html_report = read_file(html_path)

    base = hub_url.rstrip("/") if hub_url else ""
    daily_url = f"{base}/{default_path}" if base else ""
    hub_link = f"{base}/reports/daily_digest/index.html" if base else ""

    if not html_report:
        # 파일 없으면 링크만 보내기
        text_fallback = f"[WARN] Daily report not found: {html_path}\nDaily: {daily_url}\nHub: {hub_link}"
        html_body = f"""
        <html><body style="font-family:Arial,Helvetica,sans-serif">
          <h3>Daily Digest Report Not Found</h3>
          <p><b>Date:</b> {report_date}</p>
          <p><a href="{daily_url}">Open Daily Report</a></p>
          <p><a href="{hub_link}">Open Hub</a></p>
          <p style="color:#64748b;font-size:12px">Expected file: {html_path}</p>
        </body></html>
        """.strip()
        attach_name = None
        attach_bytes = None
        print(f"[WARN] Daily report not found: {html_path}")
    else:
        # ✅ 메일 본문 = 전체 HTML 리포트
        # 상대경로가 조금이라도 동작하도록 base href 주입
        # daily 리포트 기준 상위: .../reports/daily_digest/
        base_href = f"{base}/reports/daily_digest/" if base else ""
        html_body = inject_base_href(html_report, base_href)

        text_fallback = f"CSK E-COMM Daily Digest {report_date}\nDaily: {daily_url}\nHub: {hub_link}"

        # ✅ 원본도 첨부 (메일 클라이언트가 스크립트/스타일을 제거하더라도 안전하게 “그대로” 전달)
        attach_name = f"DailyDigest_{report_date}.html"
        attach_bytes = html_report.encode("utf-8")

        print(f"[OK] Using full HTML report as email body: {html_path}")

    send_email_html_full_report(
        host=smtp_host,
        port=smtp_port,
        user=smtp_user,
        password=smtp_pass,
        to_list=to_list,
        subject=subject,
        html_body=html_body,
        text_fallback=text_fallback,
        attach_name=attach_name,
        attach_bytes=attach_bytes,
    )
    print(f"[OK] Sent email to: {', '.join(to_list)}")


if __name__ == "__main__":
    main()
