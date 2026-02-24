#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import smtplib
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate


def looks_like_full_html(doc: str) -> bool:
    if not doc:
        return False
    head = doc[:2000].lower()
    return "<html" in head or "<!doctype" in head


def derive_report_base_url(hub_url: str, html_path: str) -> str:
    """Best-effort base URL for rewriting relative asset links."""
    hub_url = (hub_url or "").strip()
    if not hub_url:
        return ""

    # Common case: HUB_URL points to .../reports/daily_digest/index.html
    if "/reports/" in hub_url:
        prefix = hub_url.split("/reports/")[0] + "/reports/"
        rel = (html_path or "").lstrip("/")
        if rel.startswith("reports/"):
            rel = rel[len("reports/"):]
        return urljoin(prefix, rel)

    # Fallback: directory of HUB_URL
    if hub_url.endswith("/"):
        return hub_url
    return hub_url.rsplit("/", 1)[0] + "/"


def rewrite_relative_urls(html: str, base_url: str) -> str:
    if not html or not base_url:
        return html
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(True):
        for attr in ("href", "src", "poster"):
            if not tag.has_attr(attr):
                continue
            v = (tag.get(attr) or "").strip()
            if not v:
                continue
            low = v.lower()
            if low.startswith(("http://", "https://", "mailto:", "tel:", "data:", "#")):
                continue
            if v.startswith("//"):
                continue
            tag[attr] = urljoin(base_url, v)
    return str(soup)


def strip_unsafe_email_nodes(html: str) -> str:
    if not html:
        return html
    soup = BeautifulSoup(html, "html.parser")
    for t in soup.find_all(["script", "noscript"]):
        t.decompose()
    for t in soup.find_all("link"):
        rel = " ".join(t.get("rel", [])).lower()
        if "stylesheet" in rel or t.get("as") == "style":
            t.decompose()
    return str(soup)


def inline_computed_styles_via_playwright(html_path: str, timeout_ms: int = 20000) -> str:
    """Render local HTML in Chromium and inline computed styles onto each element."""
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return ""

    p = Path(html_path)
    if not p.exists():
        return ""
    file_url = p.resolve().as_uri()

    js_inline = r"""
(() => {
  const SAFE = [
    'display','position','top','right','bottom','left','z-index',
    'box-sizing','width','max-width','min-width','height','max-height','min-height',
    'margin','margin-top','margin-right','margin-bottom','margin-left',
    'padding','padding-top','padding-right','padding-bottom','padding-left',
    'border','border-top','border-right','border-bottom','border-left',
    'border-color','border-width','border-style','border-radius',
    'background','background-color','background-image','background-size','background-position','background-repeat',
    'color','opacity',
    'font','font-family','font-size','font-weight','font-style','line-height','letter-spacing','text-transform',
    'text-align','text-decoration','white-space',
    'box-shadow',
    'overflow','overflow-x','overflow-y',
    'gap','row-gap','column-gap',
    'flex','flex-direction','flex-wrap','justify-content','align-items','align-content',
  ];

  document.querySelectorAll('script,noscript').forEach(n => n.remove());
  const nodes = Array.from(document.querySelectorAll('*'));
  for (const el of nodes) {
    const cs = window.getComputedStyle(el);
    let out = '';
    for (const prop of SAFE) {
      const v = cs.getPropertyValue(prop);
      if (!v) continue;
      if (v === 'initial' || v === 'normal' || v === 'none' || v === 'auto') continue;
      out += `${prop}:${v};`;
    }
    if (out) {
      const existing = el.getAttribute('style') || '';
      el.setAttribute('style', existing + (existing && !existing.trim().endsWith(';') ? ';' : '') + out);
    }
    el.removeAttribute('class');
  }

  document.body.setAttribute('style', (document.body.getAttribute('style')||'') + ';margin:0;padding:0;');
  return '<!doctype html>' + document.documentElement.outerHTML;
})();
"""

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={"width": 1200, "height": 900})
        page.goto(file_url, wait_until="networkidle", timeout=timeout_ms)
        page.wait_for_timeout(500)
        html = page.evaluate(js_inline)
        browser.close()
        return html or ""


def env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def split_recipients(s: str) -> list[str]:
    if not s:
        return []
    parts = re.split(r"[,\s;]+", s.strip())
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


def build_fallback_body(kst_stamp: str, hub_url: str, mode: str) -> str:
    hub_line = f'<a href="{hub_url}">{hub_url}</a>' if hub_url else "(HUB_URL not set)"
    return f"""
    <div style="font-family: Arial, sans-serif; line-height:1.6;">
      <h2 style="margin:0 0 8px 0;">CSK E-COMM Daily Update</h2>
      <div><b>Time:</b> {kst_stamp}</div>
      <div><b>Hub:</b> {hub_line}</div>
      <div><b>Mode:</b> {mode}</div>
      <hr style="border:none;border-top:1px solid #e5e7eb;margin:16px 0;" />
      <div style="color:#6b7280;font-size:12px;">
        This email was sent automatically by GitHub Actions.
      </div>
    </div>
    """.strip()


def _join_url(base: str, path: str) -> str:
    base = (base or "").rstrip("/")
    path = (path or "").lstrip("/")
    if not base:
        return path
    return f"{base}/{path}"


def _looks_like_full_html(s: str) -> bool:
    if not s:
        return False
    low = s.lower()
    return ("<html" in low) or ("<body" in low) or ("<!doctype html" in low)


def _escape_html(s: str) -> str:
    # minimal escape for text-hub -> safe HTML
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _convert_hub_text_to_html(text: str, hub_url: str, title: str, subtitle: str) -> str:
    """
    Converts a plain-text hub like:
      Daily Reports
      [daily/2026-02-23.html]2026-02-23 ...
    into a clickable HTML list.
    """
    lines = [ln.rstrip() for ln in (text or "").splitlines()]
    # remove empty leading/trailing
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()

    daily_items = []
    weekly_items = []
    section = None

    # pattern: [path]LABEL  or [path]LABEL(without space)
    pat = re.compile(r"\[([^\]]+)\]\s*([^\[]+)?")

    for ln in lines:
        raw = ln.strip()
        if not raw:
            continue
        low = raw.lower()

        if "daily reports" in low:
            section = "daily"
            continue
        if "weekly" in low:
            section = "weekly"
            continue

        # extract multiple [..].. pairs from one line
        matches = pat.findall(raw)
        if matches:
            for path, label in matches:
                path = (path or "").strip()
                label = (label or "").strip()
                href = _join_url(hub_url, path) if hub_url else path
                label2 = label if label else path
                item_html = f'<a href="{_escape_html(href)}" style="color:#0b5bd3;text-decoration:none;font-weight:700;">{_escape_html(label2)}</a>'
                if section == "weekly":
                    weekly_items.append(item_html)
                else:
                    daily_items.append(item_html)
        else:
            # treat as plain line
            if section == "weekly":
                weekly_items.append(_escape_html(raw))
            else:
                daily_items.append(_escape_html(raw))

    def pills(items: list[str]) -> str:
        if not items:
            return "<div style='color:#94a3b8;font-size:12px;'>—</div>"
        # show as wrapping pills
        return "".join(
            f"<span style='display:inline-block;margin:6px 8px 0 0;padding:7px 10px;border:1px solid #e2e8f0;border-radius:999px;background:#ffffff;'>"
            f"{it}</span>"
            for it in items
        )

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{_escape_html(title)}</title>
</head>
<body style="margin:0;padding:0;background:#f5f7fb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans KR',Arial,sans-serif;color:#0f172a;">
  <div style="max-width:920px;margin:0 auto;padding:22px 14px;">
    <div style="background:#ffffff;border-radius:18px;border:1px solid #e6e9ef;box-shadow:0 6px 18px rgba(0,0,0,0.06);overflow:hidden;">
      <div style="height:4px;background:#0055a5;"></div>
      <div style="padding:18px 20px 14px 20px;">
        <div style="font-size:18px;font-weight:800;color:#0055a5;">{_escape_html(title)}</div>
        <div style="font-size:13px;color:#475569;margin-top:4px;">{_escape_html(subtitle)}</div>
        {"<div style='margin-top:10px;font-size:12px;'><b>Hub:</b> <a href='"+_escape_html(hub_url)+"' style='color:#0b5bd3;text-decoration:none;'>"+_escape_html(hub_url)+"</a></div>" if hub_url else ""}
      </div>

      <div style="border-top:1px solid #eef2fb;"></div>

      <div style="padding:14px 18px 18px 18px;">
        <div style="font-size:13px;font-weight:800;color:#223;margin:6px 0 8px 0;">Daily Reports</div>
        <div style="padding:10px 12px;background:#fbfdff;border:1px solid #eef2fb;border-radius:14px;">
          {pills(daily_items)}
        </div>

        <div style="font-size:13px;font-weight:800;color:#223;margin:16px 0 8px 0;">Weekly (7D Cumulative)</div>
        <div style="padding:10px 12px;background:#fbfdff;border:1px solid #eef2fb;border-radius:14px;">
          {pills(weekly_items)}
        </div>

        <div style="margin-top:16px;font-size:10px;color:#94a3b8;text-align:right;">Auto-generated · GitHub Actions</div>
      </div>
    </div>
  </div>
</body>
</html>"""


def normalize_email_html(html_report: str, hub_url: str, subject: str, kst_stamp: str, mode: str) -> str:
    """
    - If html_report is full HTML => send as-is.
    - Else treat as hub text and convert into pretty HTML.
    """
    if _looks_like_full_html(html_report):
        return html_report

    # plain text hub -> convert
    subtitle = f"{kst_stamp} · Mode: {mode}"
    return _convert_hub_text_to_html(html_report, hub_url=hub_url, title=subject, subtitle=subtitle)


def main():
    smtp_host = env("SMTP_HOST")
    smtp_port = int(env("SMTP_PORT", "587") or "587")
    smtp_user = env("SMTP_USER")
    smtp_pass = env("SMTP_PASS")

    recipients_raw = env("DAILY_RECIPIENTS")
    to_list = split_recipients(recipients_raw)

    if not smtp_host:
        raise RuntimeError("Missing SMTP_HOST (set vars.SMTP_HOST, e.g. smtp.gmail.com)")
    if not smtp_port:
        raise RuntimeError("Missing SMTP_PORT (set vars.SMTP_PORT, e.g. 587)")
    if not smtp_user:
        raise RuntimeError("Missing SMTP_USER (set secrets.SMTP_USER)")
    if not smtp_pass:
        raise RuntimeError("Missing SMTP_PASS (set secrets.SMTP_PASS)")
    if not to_list:
        raise RuntimeError("Missing DAILY_RECIPIENTS (set vars.DAILY_RECIPIENTS)")

    hub_url = env("HUB_URL")
    kst_stamp = env("KST_STAMP")

    mode_daily = env("MODE_DAILY_ONLY").lower() == "true"
    mode_voc = env("MODE_VOC_ONLY").lower() == "true"
    mode_blog = env("MODE_BLOG_ONLY").lower() == "true"

    mode = "FULL"
    if mode_daily:
        mode = "DAILY ONLY"
    elif mode_voc:
        mode = "VOC ONLY"
    elif mode_blog:
        mode = "BLOG ONLY"

    subject = build_subject(kst_stamp, mode_daily, mode_voc, mode_blog)

    html_path = env("DAILY_DIGEST_HTML_PATH", "reports/daily_digest/index.html")
    html_report = read_file(html_path)

    email_render_mode = env("EMAIL_RENDER_MODE", "computed").lower()  # computed | raw
    base_url = env("REPORT_BASE_URL") or derive_report_base_url(hub_url, html_path)

    if html_report:
        if looks_like_full_html(html_report):
            cleaned = strip_unsafe_email_nodes(html_report)

            if email_render_mode == "computed":
                rendered = inline_computed_styles_via_playwright(html_path)
                if rendered:
                    cleaned = rendered
                    print("[OK] Email render: computed-style inlined via Playwright")
                else:
                    print("[WARN] Email render failed; falling back to raw HTML")

            cleaned = rewrite_relative_urls(cleaned, base_url)
            html_body = cleaned
        else:
            # 허브(텍스트 링크) 같은 경우: 보기 좋게 HTML로 변환
            html_body = normalize_email_html(html_report, hub_url=hub_url, subject=subject, kst_stamp=kst_stamp, mode=mode)

        text_fallback = f"CSK E-COMM Daily Update ({mode}) - {kst_stamp}\n{hub_url}".strip()
        print(f"[OK] Using report body: {html_path}")
        if base_url:
            print(f"[OK] Base URL for assets: {base_url}")
    else:
        html_body = build_fallback_body(kst_stamp, hub_url, mode)
        text_fallback = f"CSK E-COMM Daily Update ({mode}) - {kst_stamp}".strip()
        print(f"[WARN] Report HTML not found. Using fallback body. (looked for: {html_path})")

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
