#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate

def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

def send_email(host: str, port: int, user: str, password: str, to_list: list[str], subject: str, html_body: str):
    msg = MIMEText(html_body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ", ".join(to_list)
    msg["Date"] = formatdate(localtime=True)

    with smtplib.SMTP(host, port) as s:
        s.ehlo()
        s.starttls()
        s.login(user, password)
        s.sendmail(user, to_list, msg.as_string())

def main():
    smtp_host = env("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(env("SMTP_PORT", "587") or "587")
    smtp_user = env("SMTP_USER")
    smtp_pass = env("SMTP_PASS")
    alert_recipient = env("ALERT_RECIPIENT")

    if not alert_recipient:
        # 알림 수신자 미설정이면 조용히 종료(실패시키지 않음)
        return

    if not smtp_user:
        raise RuntimeError("Missing SMTP_USER env")
    if not smtp_pass:
        raise RuntimeError("Missing SMTP_PASS env")

    kst_stamp = env("KST_STAMP", "")
    run_url = env("GH_RUN_URL", "")
    hub_url = env("HUB_URL", "")

    subject = f"[ALERT] CSK E-COMM Daily Update FAILED ({kst_stamp})"

    body = f"""
    <div style="font-family: Arial, sans-serif; line-height:1.6;">
      <h2 style="margin:0 0 8px 0;color:#b91c1c;">Workflow Failed</h2>
      <div><b>Time:</b> {kst_stamp}</div>
      <div style="margin-top:10px;">
        <b>Run:</b> <a href="{run_url}">{run_url}</a>
      </div>
      <div style="margin-top:10px;">
        <b>Hub:</b> {f'<a href="{hub_url}">{hub_url}</a>' if hub_url else '(HUB_URL not set)'}
      </div>
      <hr style="border:none;border-top:1px solid #e5e7eb;margin:16px 0;" />
      <div style="color:#6b7280;font-size:12px;">
        Please open the run link above and check the first failing step logs.
      </div>
    </div>
    """

    send_email(
        host=smtp_host,
        port=smtp_port,
        user=smtp_user,
        password=smtp_pass,
        to_list=[alert_recipient],
        subject=subject,
        html_body=body,
    )

if __name__ == "__main__":
    main()
