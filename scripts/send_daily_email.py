#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate

def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

def parse_one_recipient(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    # allow accidental "a@x.com, b@y.com" -> first one
    parts = re.split(r"[,\s;]+", raw)
    for p in parts:
        p = p.strip()
        if p:
            return p
    return ""

def send_email(host: str, port: int, user: str, password: str, to_list: list[str], subject: str, html_body: str):
    msg = MIMEText(html_body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ", ".join(to_list)
    msg["Date"] = formatdate(localtime=True)

    if int(port) == 465:
        with smtplib.SMTP_SSL(host, port) as s:
            s.ehlo()
            s.login(user, password)
            s.sendmail(user, to_list, msg.as_string())
        return

    with smtplib.SMTP(host, port) as s:
        s.ehlo()
        try:
            s.starttls()
            s.ehlo()
        except smtplib.SMTPException:
            pass
        s.login(user, password)
        s.sendmail(user, to_list, msg.as_string())

def main():
    smtp_host = env("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(env("SMTP_PORT", "587") or "587")
    smtp_user = env("SMTP_USER")
    smtp_pass = env("SMTP_PASS")

    alert_recipient = parse_one_recipient(env("ALERT_RECIPIENT"))
    if not alert_recipient:
        print("[WARN] ALERT_RECIPIENT not set. Skipping failure email.")
        return

    if not smtp_user:
        raise RuntimeError("Missing SMTP_USER env")
    if not smtp_pass:
        raise RuntimeError("Missing SMTP_PASS env")

    kst_stamp = env("KST_STAMP", "")
    run_url = env("GH_RUN_URL", "")
    hub_url = env("HUB_URL", "")

    subject = f"[ALERT] CSK E-COMM Daily Update FAILED ({kst_stamp})".strip()

    body = f"""
    <div style="font-family: Arial, sans-serif; line-height:1.6;">
      <h2 style="margin:0 0 8px 0;color:#b91c1c;">Workflow Failed</h2>
      <div><b>Time:</b> {kst_stamp or "(KST_STAMP not set)"}</div>
      <div style="margin-top:10px;">
        <b>Run:</b> {f'<a href="{run_url}">{run_url}</a>' if run_url else '(GH_RUN_URL not set)'}
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

    print(f"[INFO] SMTP={smtp_host}:{smtp_port}")
    print(f"[INFO] TO={[alert_recipient]}")
    print(f"[INFO] SUBJECT={subject}")

    send_email(
        host=smtp_host,
        port=smtp_port,
        user=smtp_user,
        password=smtp_pass,
        to_list=[alert_recipient],
        subject=subject,
        html_body=body,
    )

    print("[OK] Failure alert email sent.")

if __name__ == "__main__":
    main()
