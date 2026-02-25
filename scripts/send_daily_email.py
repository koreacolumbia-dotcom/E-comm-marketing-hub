#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from email.utils import formatdate, make_msgid


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


def read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def read_bytes(path: str) -> bytes:
    try:
        with open(path, "rb") as f:
            return f.read()
    except Exception:
        return b""


def build_subject(kst_stamp: str, mode_daily: bool, mode_voc: bool, mode_blog: bool) -> str:
    mode = "FULL"
    if mode_daily:
        mode = "DAILY ONLY"
    elif mode_voc:
        mode = "VOC ONLY"
    elif mode_blog:
        mode = "BLOG ONLY"
    return f"[CSK E-COMM] Daily Update ({mode}) - {kst_stamp}".strip()


def html_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def inject_base_href(html: str, base_href: str) -> str:
    """
    첨부 HTML을 브라우저에서 열 때 상대경로(asset/data)가 조금이라도 먹게 base href 주입.
    메일 본문은 Gmail이 스크립트/CSS를 막기 때문에 이걸로 해결 불가(첨부용만).
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


def build_email_cover_html(
    report_date: str,
    daily_url: str,
    hub_url: str,
    inline_img_cid: str | None,
    missing_reason: str | None = None,
) -> str:
    """
    Gmail에서도 안 깨지는 “커버/프리뷰” 본문 (인라인 CSS + table 레이아웃)
    - 리포트 자체를 본문에 넣는 건 불가능(스크립트/외부CSS 차단)
    - 대신: 링크 + (가능 시) PNG 프리뷰 인라인 표시
    """
    btn_primary = (
        f'<a href="{html_escape(daily_url)}" '
        f'style="display:inline-block;background:#002d72;color:#fff;text-decoration:none;'
        f'font-weight:800;font-size:13px;padding:10px 14px;border-radius:12px;">Open Daily Report</a>'
        if daily_url else
        '<span style="display:inline-block;background:#94a3b8;color:#fff;font-weight:800;'
        'font-size:13px;padding:10px 14px;border-radius:12px;">Daily link unavailable</span>'
    )
    btn_secondary = (
        f'<a href="{html_escape(hub_url)}" '
        f'style="display:inline-block;background:#ffffff;color:#0f172a;text-decoration:none;'
        f'font-weight:800;font-size:13px;padding:10px 14px;border-radius:12px;'
        f'border:1px solid #e2e8f0;margin-left:8px;">Open Hub</a>'
        if hub_url else
        ''
    )

    warn = ""
    if missing_reason:
        warn = f"""
          <tr>
            <td style="padding:12px 18px;background:#fff7ed;border:1px solid #fed7aa;border-radius:14px;">
              <div style="font-weight:900;color:#9a3412;font-size:13px;">주의</div>
              <div style="margin-top:6px;color:#9a3412;font-size:12px;line-height:1.6;">{html_escape(missing_reason)}</div>
            </td>
          </tr>
          <tr><td style="height:12px"></td></tr>
        """

    preview_block = ""
    if inline_img_cid:
        preview_block = f"""
          <tr>
            <td style="padding:0 0 6px 0;color:#64748b;font-size:12px;font-weight:700;">
              Preview (static)
            </td>
          </tr>
          <tr>
            <td style="border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;background:#ffffff;">
              <img src="cid:{inline_img_cid}" alt="Daily Digest Preview"
                   style="display:block;width:100%;max-width:900px;height:auto;border:0;margin:0;" />
            </td>
          </tr>
          <tr><td style="height:14px"></td></tr>
        """
    else:
        preview_block = """
          <tr>
            <td style="padding:12px 18px;border:1px dashed #cbd5e1;border-radius:14px;color:#64748b;font-size:12px;line-height:1.6;">
              프리뷰 이미지(PNG)가 없어서 메일 본문에는 링크/첨부로만 전달됩니다.<br/>
              (가능하면 CI에서 리포트를 PNG/PDF로 렌더링해서 같이 첨부하면 “그대로” 보입니다.)
            </td>
          </tr>
          <tr><td style="height:14px"></td></tr>
        """

    return f"""<!doctype html>
<html>
  <body style="margin:0;background:#f6f8fb;font-family:Arial,Helvetica,sans-serif;color:#0f172a;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f6f8fb;">
      <tr>
        <td align="center" style="padding:22px 10px;">
          <table role="presentation" width="900" cellspacing="0" cellpadding="0"
                 style="max-width:900px;width:100%;background:#ffffff;border:1px solid #eef2f7;border-radius:18px;">
            <tr>
              <td style="padding:18px 18px 14px;">
                <div style="font-size:18px;font-weight:900;">CSK E-COMM · Daily Digest</div>
                <div style="margin-top:6px;font-size:13px;color:#64748b;font-weight:700;">
                  기준일: <span style="color:#0f172a;font-weight:900;">{html_escape(report_date)}</span>
                </div>
                <div style="margin-top:14px;">
                  {btn_primary}
                  {btn_secondary}
                </div>
                <div style="margin-top:10px;font-size:12px;color:#64748b;line-height:1.6;">
                  * Gmail/Outlook는 보안상 스크립트·외부CSS(Tailwind 등)를 차단해서 “웹 리포트 HTML”을 본문에 그대로 렌더링할 수 없습니다.<br/>
                  * 아래 프리뷰는 정적 이미지이며, 전체 리포트는 링크 또는 첨부파일(HTML/PNG)을 확인해주세요.
                </div>
              </td>
            </tr>
          </table>

          <table role="presentation" width="900" cellspacing="0" cellpadding="0"
                 style="max-width:900px;width:100%;margin-top:14px;">
            {warn}
            {preview_block}
            <tr>
              <td style="font-size:11px;color:#94a3b8;line-height:1.6;">
                첨부: DailyDigest_{html_escape(report_date)}.html (원본) / (옵션) DailyDigest_{html_escape(report_date)}.png
              </td>
            </tr>
          </table>

        </td>
      </tr>
    </table>
  </body>
</html>
"""


def send_email_mixed_with_inline_image(
    host: str,
    port: int,
    user: str,
    password: str,
    to_list: list[str],
    subject: str,
    text_fallback: str,
    html_body: str,
    inline_png_cid: str | None,
    inline_png_bytes: bytes | None,
    attachments: list[tuple[str, str, bytes]],  # (filename, mime_subtype, bytes)
):
    """
    MIME 구조:
    multipart/mixed
      - multipart/related
          - multipart/alternative
              - text/plain
              - text/html
          - inline image (optional, Content-ID)
      - attachments...
    """
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ", ".join(to_list)
    msg["Date"] = formatdate(localtime=True)

    related = MIMEMultipart("related")
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(text_fallback or "", "plain", "utf-8"))
    alt.attach(MIMEText(html_body or "", "html", "utf-8"))
    related.attach(alt)

    if inline_png_cid and inline_png_bytes:
        img = MIMEImage(inline_png_bytes, _subtype="png")
        img.add_header("Content-ID", f"<{inline_png_cid}>")
        img.add_header("Content-Disposition", "inline", filename="preview.png")
        related.attach(img)

    msg.attach(related)

    for filename, subtype, b in attachments:
        if not b:
            continue
        if subtype == "html":
            # HTML 첨부는 text/html이 더 호환 좋음
            part = MIMEText(b.decode("utf-8", errors="ignore"), "html", "utf-8")
            part.add_header("Content-Disposition", "attachment", filename=filename)
        else:
            part = MIMEApplication(b, _subtype=subtype)
            part.add_header("Content-Disposition", "attachment", filename=filename)
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

    report_date = env("REPORT_DATE") or kst_yesterday_ymd()

    # 로컬 리포트 HTML
    default_html_path = f"reports/daily_digest/daily/{report_date}.html"
    html_path = env("DAILY_DIGEST_HTML_PATH") or default_html_path
    html_report = read_text(html_path)

    # (옵션) PNG 프리뷰: CI에서 생성해두면 메일에서 “그대로” 보임
    default_png_path = f"reports/daily_digest/daily/{report_date}.png"
    png_path = env("DAILY_DIGEST_PNG_PATH") or default_png_path
    png_bytes = read_bytes(png_path)

    base = hub_url.rstrip("/") if hub_url else ""
    daily_url = f"{base}/{default_html_path}" if base else ""
    hub_link = f"{base}/reports/daily_digest/index.html" if base else ""

    attachments: list[tuple[str, str, bytes]] = []

    missing_reason = None
    if not html_report:
        missing_reason = f"Daily report HTML 파일을 찾지 못했습니다. (expected: {html_path})"
    else:
        # 첨부용 HTML에 base href 주입 (브라우저로 열 때 상대경로 조금 도움)
        base_href = f"{base}/reports/daily_digest/" if base else ""
        html_for_attach = inject_base_href(html_report, base_href)
        attachments.append((f"DailyDigest_{report_date}.html", "html", html_for_attach.encode("utf-8")))

    inline_cid = None
    if png_bytes:
        # 인라인 CID는 <> 없이 id만 사용
        inline_cid = make_msgid(domain="csk.local")[1:-1]
        attachments.append((f"DailyDigest_{report_date}.png", "png", png_bytes))

    html_body = build_email_cover_html(
        report_date=report_date,
        daily_url=daily_url,
        hub_url=hub_link,
        inline_img_cid=inline_cid,
        missing_reason=missing_reason,
    )

    text_fallback = f"CSK E-COMM Daily Digest {report_date}\nDaily: {daily_url}\nHub: {hub_link}"

    send_email_mixed_with_inline_image(
        host=smtp_host,
        port=smtp_port,
        user=smtp_user,
        password=smtp_pass,
        to_list=to_list,
        subject=subject,
        text_fallback=text_fallback,
        html_body=html_body,
        inline_png_cid=inline_cid,
        inline_png_bytes=png_bytes if png_bytes else None,
        attachments=attachments,
    )
    print(f"[OK] Sent email to: {', '.join(to_list)}")
    if png_bytes:
        print(f"[OK] Attached/inline PNG preview: {png_path}")
    else:
        print(f"[WARN] PNG preview not found (optional): {png_path}")
    if html_report:
        print(f"[OK] Attached HTML report: {html_path}")


if __name__ == "__main__":
    main()
