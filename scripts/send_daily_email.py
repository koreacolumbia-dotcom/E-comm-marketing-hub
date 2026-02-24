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


def absolutize_urls(html: str, base_url: str) -> str:
    """
    Keep HTML as-is, but convert relative href/src to absolute based on base_url.
    This keeps the report 'whole' while making links/images work in email.
    """
    if not html or not base_url:
        return html

    base_url = base_url.rstrip("/")

    def repl(m):
        pre, url, post = m.group(1), (m.group(2) or "").strip(), m.group(3)
        if not url:
            return m.group(0)

        # keep already-absolute / anchors / mailto / tel
        if url.startswith("#") or url.startswith("mailto:") or url.startswith("tel:"):
            return m.group(0)
        if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", url):
            return m.group(0)
        if url.startswith("//"):
            return pre + "https:" + url + post

        # absolute-path from domain root
        if url.startswith("/"):
            return pre + base_url + url + post

        # relative path
        return pre + base_url + "/" + url + post

    html = re.sub(r'(href\s*=\s*["\'])([^"\']+)(["\'])', repl, html, flags=re.IGNORECASE)
    html = re.sub(r'(src\s*=\s*["\'])([^"\']+)(["\'])', repl, html, flags=re.IGNORECASE)
    return html


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

    # ✅ 전일자 리포트 “통째” 본문에 넣기
    report_date = env("REPORT_DATE") or kst_yesterday_ymd()
    default_path = f"reports/daily_digest/daily/{report_date}.html"
    html_path = env("DAILY_DIGEST_HTML_PATH") or default_path

    html_report = read_file(html_path)
    if not html_report:
        # 최소 fallback (그래도 '전일자' 링크는 줘야 함)
        report_url = ""
        if hub_url:
            report_url = hub_url.rstrip("/") + "/" + default_path
        html_report = f"""
        <div style="font-family:Arial,sans-serif; line-height:1.6;">
          <h2>Daily Digest</h2>
          <p>전일자 리포트 파일을 찾지 못했습니다.</p>
          <p><b>Expected:</b> <code>{default_path}</code></p>
          <p><b>Looked for:</b> <code>{html_path}</code></p>
          <p><b>Open:</b> <a href="{report_url}">{report_url}</a></p>
        </div>
        """.strip()
        text_fallback = f"Daily Digest missing: {default_path}"
        print(f"[WARN] Daily report not found: {html_path}")
    else:
        # ✅ 통째 유지 + 상대경로만 절대경로화
        html_report = absolutize_urls(html_report, hub_url)
        text_fallback = f"CSK E-COMM Daily Update - {report_date}"
        print(f"[OK] Embedding full report HTML: {html_path}")

    send_email_html(
        host=smtp_host,
        port=smtp_port,
        user=smtp_user,
        password=smtp_pass,
        to_list=to_list,
        subject=subject,
        html_body=html_report,
        text_fallback=text_fallback,
    )
    print(f"[OK] Sent email to: {', '.join(to_list)}")


if __name__ == "__main__":
    main()
