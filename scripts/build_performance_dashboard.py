#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build the CSK Performance Dashboard from GA4 BigQuery only.

Temporary mode: MSSQL-dependent product/actual-sales metrics are intentionally excluded
until the Windows MSSQL -> BigQuery sync is restored.
"""
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd

OUT = Path(os.getenv("OUT_DIR", "reports"))
OUT.mkdir(parents=True, exist_ok=True)
PROJECT = os.getenv("GCP_PROJECT", "columbia-ga4")
DATASET = os.getenv("GA4_DATASET", "analytics_358593394")
TODAY = pd.Timestamp.today().normalize()
END = pd.Timestamp(os.getenv("REPORT_END_DATE", (TODAY - pd.Timedelta(days=1)).strftime("%Y-%m-%d")))
START = pd.Timestamp(os.getenv("REPORT_START_DATE", (END - pd.Timedelta(days=30)).strftime("%Y-%m-%d")))
LY_START, LY_END = START - pd.DateOffset(years=1), END - pd.DateOffset(years=1)


def rx(text, pattern):
    return bool(re.search(pattern, text or "", re.I))


def classify(source, medium, campaign=""):
    sm = f"{source or ''} / {medium or ''}"
    cp = campaign or ""
    if rx(sm, r"youtube\s*/\s*live"):
        return ("4. Official SNS", "YouTube Referral", "유튜브 라이브")
    if rx(sm, r"lighthouse"):
        return ("3. Organic Traffic", "Referral", "라이트하우스 팝 디스플레이 존")
    if rx(sm, r"instagram.*story"):
        return ("4. Official SNS", "Instagram Story", "인스타그램 스토리")
    if rx(sm, r"instagram.*feed"):
        return ("4. Official SNS", "Instagram Feed", "인스타그램 피드")
    if rx(sm, r"benz"):
        return ("3. Organic Traffic", "Referral", "벤츠 러닝 프로그램")
    if rx(sm, r"nap.*da|toss|blind"):
        return ("2. Paid Ad", "Paid Display", "배너/리워드 광고")
    if rx(sm, r"kakaobs"):
        return ("2. Paid Ad", "Paid Search", "카카오 브랜드검색광고")
    if rx(sm, r"inhouse"):
        return ("3. Organic Traffic", "Inhouse Purchase", "Inhouse Purchase")
    if rx(sm, r"lms") or rx(cp, r"lms"):
        return ("5. Owned Channel", "LMS", "문자메시지유입")
    if rx(sm, r"email|edm"):
        return ("5. Owned Channel", "Email", "이메일유입")
    if rx(sm, r"kakao_fridnstalk"):
        return ("5. Owned Channel", "Kakao Friendstalk", "카카오톡친구톡")
    if rx(sm, r"mkt|_bd") or rx(cp, r"mkt|\[bd"):
        return ("1. Awareness", "Awareness", "Awareness")
    if rx(sm, r"igshopping"):
        return ("4. Official SNS", "Instagram Official Shop", "인스타그램 샵")
    if rx(sm, r"facebook.*referral"):
        return ("3. Organic Traffic", "Social", "페이스북자연유입")
    if rx(sm, r"instagram.*referral"):
        return ("4. Official SNS", "Instagram Official Account", "인스타그램 공식계정")
    if rx(sm, r"meta|facebook|instagram|\big\b|\bfb\b"):
        return ("2. Paid Ad", "Paid Social", "메타광고")
    if rx(sm, r"google") and rx(cp, r"demand|demend|디멘드|gdn"):
        return ("2. Paid Ad", "Paid Display", "구글 디스플레이 광고")
    if rx(sm, r"google\s*/\s*cpc") and rx(cp, r"pmax"):
        return ("2. Paid Ad", "Paid Omni Channel", "구글피맥스광고")
    if rx(sm, r"google\s*/\s*cpc") and rx(cp, r"youtube|yt|video|instream|vac|vvc|유튜브"):
        return ("1. Awareness", "Paid Video", "구글동영상광고")
    if rx(sm, r"google\s*/\s*cpc") and rx(cp, r"discovery"):
        return ("1. Awareness", "Paid Display", "구글디스커버리광고")
    if rx(sm, r"google\s*/\s*cpc"):
        return ("2. Paid Ad", "Paid Search", "구글검색광고")
    if rx(sm, r"google\s*/\s*organic"):
        return ("3. Organic Traffic", "Organic Search", "구글자연검색")
    if rx(sm, r"google"):
        return ("3. Organic Traffic", "Referral", "구글기타유입")
    if rx(sm, r"youtube"):
        return ("3. Organic Traffic", "YouTube", "유튜브자연유입")
    if rx(sm, r"naver.*(da|gfa)|gfa"):
        return ("2. Paid Ad", "Paid Display", "네이버배너광고")
    if rx(sm, r"naverbs"):
        return ("2. Paid Ad", "Paid Search", "네이버브랜드검색광고")
    if rx(sm, r"naver.*shopping_ad"):
        return ("2. Paid Ad", "Paid Search", "네이버쇼핑검색광고")
    if rx(sm, r"naver.*cpc"):
        return ("2. Paid Ad", "Paid Search", "네이버파워링크광고")
    if rx(sm, r"naver.*shopping"):
        return ("3. Organic Traffic", "Organic Search", "네이버쇼핑자연검색")
    if rx(sm, r"naver.*organic"):
        return ("3. Organic Traffic", "Organic Search", "네이버사이트자연검색")
    if rx(sm, r"naver"):
        return ("3. Organic Traffic", "Referral", "네이버기타유입")
    if rx(sm, r"daum.*organic"):
        return ("3. Organic Traffic", "Organic Search", "다음자연검색")
    if rx(sm, r"daum"):
        return ("3. Organic Traffic", "Referral", "다음기타유입")
    if rx(sm, r"kakao_ch"):
        return ("5. Owned Channel", "Kakao Channel", "카카오톡채널메시지")
    if rx(sm, r"kakao_alimtalk"):
        return ("5. Owned Channel", "Kakao Alimtalk", "카카오톡알림톡")
    if rx(sm, r"kakao_coupon"):
        return ("5. Owned Channel", "Kakao Coupon", "카카오톡쿠폰")
    if rx(sm, r"kakao_chatbot"):
        return ("5. Owned Channel", "Kakao Chatbot", "카카오톡챗봇")
    if rx(sm, r"kakao"):
        return ("2. Paid Ad", "Paid Display", "카카오광고")
    if rx(sm, r"\(direct\).*\(none\)"):
        return ("3. Organic Traffic", "Direct", "직접유입")
    if rx(sm, r"signal|buzzvill|criteo|mobon|snow|smr|tg|t_cafe|banner|\bda\b"):
        return ("2. Paid Ad", "Paid Display", "기타배너광고")
    if rx(sm, r"cpc"):
        return ("2. Paid Ad", "Paid Search", "기타검색광고")
    if rx(sm, r"organic"):
        return ("3. Organic Traffic", "Organic Search", "기타자연검색")
    if rx(sm, r"referral"):
        return ("3. Organic Traffic", "Referral", "기타추천유입")
    if rx(sm, r"shopping"):
        return ("3. Organic Traffic", "Organic Search", "기타쇼핑유입")
    if rx(sm, r"social"):
        return ("3. Organic Traffic", "Social", "기타소셜유입")
    return ("6. etc", "미분류", "미분류")


def ga4_query(a, b):
    a, b = a.strftime("%Y%m%d"), b.strftime("%Y%m%d")
    return f"""
    WITH base AS (
      SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_ts,
        user_pseudo_id,
        event_name,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS ga_session_id,
        COALESCE(
          session_traffic_source_last_click.manual_campaign.source,
          collected_traffic_source.manual_source,
          traffic_source.source,
          '(direct)'
        ) AS source,
        COALESCE(
          session_traffic_source_last_click.manual_campaign.medium,
          collected_traffic_source.manual_medium,
          traffic_source.medium,
          '(none)'
        ) AS medium,
        COALESCE(
          session_traffic_source_last_click.manual_campaign.campaign_name,
          collected_traffic_source.manual_campaign_name,
          '(not set)'
        ) AS campaign,
        ecommerce.transaction_id AS transaction_id,
        COALESCE(ecommerce.purchase_revenue, 0) AS revenue
      FROM `{PROJECT}.{DATASET}.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{a}' AND '{b}'
    ), sessions AS (
      SELECT
        event_date,
        CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)) AS session_key,
        ARRAY_AGG(source IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] AS source,
        ARRAY_AGG(medium IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] AS medium,
        ARRAY_AGG(campaign IGNORE NULLS ORDER BY event_ts LIMIT 1)[SAFE_OFFSET(0)] AS campaign,
        COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS conversions,
        SUM(IF(event_name='purchase', revenue, 0)) AS revenue
      FROM base
      WHERE ga_session_id IS NOT NULL
      GROUP BY 1,2
    )
    SELECT
      event_date, source, medium, campaign,
      COUNT(DISTINCT session_key) AS sessions,
      SUM(conversions) AS conversions,
      SUM(revenue) AS revenue
    FROM sessions
    GROUP BY 1,2,3,4
    """


def load_bq(a, b):
    from google.cloud import bigquery

    df = bigquery.Client(project=PROJECT).query(ga4_query(a, b)).to_dataframe()
    df["event_date"] = pd.to_datetime(df["event_date"])
    cls = [classify(s, m, c) for s, m, c in zip(df.source, df.medium, df.campaign)]
    df[["channel_major", "channel_middle", "channel_minor"]] = pd.DataFrame(cls, index=df.index)
    return df


def _safe_yoy(ty, ly):
    return np.where(ly != 0, ty / ly - 1, np.nan)


def build_channel(cur, ly):
    dims = ["channel_major", "channel_middle", "channel_minor"]
    c = cur.groupby(dims, dropna=False).agg(
        sessions_ty=("sessions", "sum"),
        conversions_ty=("conversions", "sum"),
        revenue_ty=("revenue", "sum"),
    ).reset_index()
    l = ly.groupby(dims, dropna=False).agg(
        sessions_ly=("sessions", "sum"),
        conversions_ly=("conversions", "sum"),
        revenue_ly=("revenue", "sum"),
    ).reset_index()
    x = c.merge(l, on=dims, how="outer").fillna(0)
    x["cvr_ty"] = np.where(x.sessions_ty > 0, x.conversions_ty / x.sessions_ty, 0)
    x["cvr_ly"] = np.where(x.sessions_ly > 0, x.conversions_ly / x.sessions_ly, 0)
    x["sessions_yoy"] = _safe_yoy(x.sessions_ty, x.sessions_ly)
    x["conversions_yoy"] = _safe_yoy(x.conversions_ty, x.conversions_ly)
    x["revenue_yoy"] = _safe_yoy(x.revenue_ty, x.revenue_ly)
    x["cvr_yoy_pp"] = (x.cvr_ty - x.cvr_ly) * 100
    return x.sort_values("revenue_ty", ascending=False)


def build_major(cur, ly):
    c = cur.groupby("channel_major", dropna=False).agg(
        sessions_ty=("sessions", "sum"), conversions_ty=("conversions", "sum"), revenue_ty=("revenue", "sum")
    ).reset_index()
    l = ly.groupby("channel_major", dropna=False).agg(
        sessions_ly=("sessions", "sum"), conversions_ly=("conversions", "sum"), revenue_ly=("revenue", "sum")
    ).reset_index()
    x = c.merge(l, on="channel_major", how="outer").fillna(0)
    x["cvr_ty"] = np.where(x.sessions_ty > 0, x.conversions_ty / x.sessions_ty, 0)
    x["cvr_ly"] = np.where(x.sessions_ly > 0, x.conversions_ly / x.sessions_ly, 0)
    x["sessions_yoy"] = _safe_yoy(x.sessions_ty, x.sessions_ly)
    x["conversions_yoy"] = _safe_yoy(x.conversions_ty, x.conversions_ly)
    x["revenue_yoy"] = _safe_yoy(x.revenue_ty, x.revenue_ly)
    x["cvr_yoy_pp"] = (x.cvr_ty - x.cvr_ly) * 100
    return x.sort_values("revenue_ty", ascending=False)


def build_source_campaign(cur, ly):
    dims = ["source", "medium", "campaign"]
    c = cur.groupby(dims, dropna=False).agg(
        sessions_ty=("sessions", "sum"), conversions_ty=("conversions", "sum"), revenue_ty=("revenue", "sum")
    ).reset_index()
    l = ly.groupby(dims, dropna=False).agg(
        sessions_ly=("sessions", "sum"), conversions_ly=("conversions", "sum"), revenue_ly=("revenue", "sum")
    ).reset_index()
    x = c.merge(l, on=dims, how="outer").fillna(0)
    x["cvr_ty"] = np.where(x.sessions_ty > 0, x.conversions_ty / x.sessions_ty, 0)
    x["cvr_ly"] = np.where(x.sessions_ly > 0, x.conversions_ly / x.sessions_ly, 0)
    x["sessions_yoy"] = _safe_yoy(x.sessions_ty, x.sessions_ly)
    x["conversions_yoy"] = _safe_yoy(x.conversions_ty, x.conversions_ly)
    x["revenue_yoy"] = _safe_yoy(x.revenue_ty, x.revenue_ly)
    x["cvr_yoy_pp"] = (x.cvr_ty - x.cvr_ly) * 100
    return x.sort_values("revenue_ty", ascending=False)


def build_daily(cur, ly):
    c = cur.groupby("event_date").agg(sessions_ty=("sessions", "sum"), conversions_ty=("conversions", "sum"), revenue_ty=("revenue", "sum")).reset_index()
    l = ly.groupby("event_date").agg(sessions_ly=("sessions", "sum"), conversions_ly=("conversions", "sum"), revenue_ly=("revenue", "sum")).reset_index()
    l["event_date"] = l["event_date"] + pd.DateOffset(years=1)
    x = c.merge(l, on="event_date", how="outer").sort_values("event_date").fillna(0)
    x["cvr_ty"] = np.where(x.sessions_ty > 0, x.conversions_ty / x.sessions_ty, 0)
    x["cvr_ly"] = np.where(x.sessions_ly > 0, x.conversions_ly / x.sessions_ly, 0)
    return x


def fmt_num(v):
    return f"{float(v):,.0f}"


def fmt_pct(v):
    return "-" if pd.isna(v) else f"{float(v)*100:+.1f}%"


def fmt_cvr(v):
    return f"{float(v)*100:.2f}%"


def table_html(df, cols, max_rows=30):
    rows = []
    for _, r in df.head(max_rows).iterrows():
        cells = []
        for key, label, kind in cols:
            val = r.get(key, "")
            if kind == "num":
                txt = fmt_num(val)
            elif kind == "won":
                txt = "₩" + fmt_num(val)
            elif kind == "pct":
                txt = fmt_pct(val)
            elif kind == "cvr":
                txt = fmt_cvr(val)
            elif kind == "pp":
                txt = f"{float(val):+.2f}pp"
            else:
                txt = str(val)
            cls = ""
            if kind in {"pct", "pp"} and not pd.isna(val):
                try:
                    cls = " pos" if float(val) > 0 else (" neg" if float(val) < 0 else "")
                except Exception:
                    pass
            cells.append(f'<td class="{cls.strip()}">{txt}</td>')
        rows.append("<tr>" + "".join(cells) + "</tr>")
    head = "".join(f"<th>{label}</th>" for _, label, _ in cols)
    return f"<div class='table-wrap'><table><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"


def write_excel(channel, major, source_campaign, daily):
    path = OUT / "performance_export.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        major.to_excel(w, sheet_name="Major Channel", index=False)
        channel.to_excel(w, sheet_name="Channel Detail", index=False)
        source_campaign.to_excel(w, sheet_name="Source Campaign", index=False)
        daily.to_excel(w, sheet_name="Daily Trend", index=False)
    return path


def build_html(channel, major, source_campaign, daily):
    totals_ty = {
        "sessions": float(major.sessions_ty.sum()),
        "conversions": float(major.conversions_ty.sum()),
        "revenue": float(major.revenue_ty.sum()),
    }
    totals_ly = {
        "sessions": float(major.sessions_ly.sum()),
        "conversions": float(major.conversions_ly.sum()),
        "revenue": float(major.revenue_ly.sum()),
    }
    cvr_ty = totals_ty["conversions"] / totals_ty["sessions"] if totals_ty["sessions"] else 0
    cvr_ly = totals_ly["conversions"] / totals_ly["sessions"] if totals_ly["sessions"] else 0
    cards = [
        ("Sessions", fmt_num(totals_ty["sessions"]), fmt_pct(_safe_yoy(np.array([totals_ty["sessions"]]), np.array([totals_ly["sessions"]]))[0])),
        ("Purchases", fmt_num(totals_ty["conversions"]), fmt_pct(_safe_yoy(np.array([totals_ty["conversions"]]), np.array([totals_ly["conversions"]]))[0])),
        ("CVR", fmt_cvr(cvr_ty), f"{(cvr_ty-cvr_ly)*100:+.2f}pp"),
        ("GA4 Revenue", "₩" + fmt_num(totals_ty["revenue"]), fmt_pct(_safe_yoy(np.array([totals_ty["revenue"]]), np.array([totals_ly["revenue"]]))[0])),
    ]
    cards_html = "".join(f"<div class='card'><div class='label'>{k}</div><div class='value'>{v}</div><div class='delta'>{d} YoY</div></div>" for k,v,d in cards)

    major_cols = [
        ("channel_major", "구분", "text"), ("sessions_ty", "Sessions", "num"), ("sessions_yoy", "Sessions YoY", "pct"),
        ("conversions_ty", "Purchases", "num"), ("cvr_ty", "CVR", "cvr"), ("cvr_yoy_pp", "CVR YoY", "pp"),
        ("revenue_ty", "GA4 Revenue", "won"), ("revenue_yoy", "Revenue YoY", "pct"),
    ]
    channel_cols = [
        ("channel_major", "대분류", "text"), ("channel_middle", "중분류", "text"), ("channel_minor", "소분류", "text"),
        ("sessions_ty", "Sessions", "num"), ("sessions_yoy", "YoY", "pct"), ("conversions_ty", "Purchases", "num"),
        ("cvr_ty", "CVR", "cvr"), ("cvr_yoy_pp", "CVR YoY", "pp"), ("revenue_ty", "GA4 Revenue", "won"), ("revenue_yoy", "Revenue YoY", "pct"),
    ]
    sc_cols = [
        ("source", "Source", "text"), ("medium", "Medium", "text"), ("campaign", "Campaign", "text"),
        ("sessions_ty", "Sessions", "num"), ("sessions_yoy", "YoY", "pct"), ("conversions_ty", "Purchases", "num"),
        ("cvr_ty", "CVR", "cvr"), ("revenue_ty", "GA4 Revenue", "won"), ("revenue_yoy", "Revenue YoY", "pct"),
    ]

    html = f"""<!doctype html>
<html lang='ko'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Performance Dashboard</title>
<style>
:root{{--bg:#08111f;--panel:#0f1b2d;--line:#20304a;--text:#e8eef8;--muted:#8fa1b8;--accent:#65b9ff;--good:#6ee7a8;--bad:#ff8d8d}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 Arial,'Noto Sans KR',sans-serif}}
.wrap{{max-width:1500px;margin:auto;padding:28px}} h1{{font-size:28px;margin:0 0 4px}} h2{{font-size:18px;margin:28px 0 12px}} .sub{{color:var(--muted)}}
.notice{{margin:18px 0;padding:12px 14px;border:1px solid var(--line);background:#0b1728;border-radius:10px;color:#b8c8dc}}
.cards{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:18px 0}} .card{{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:16px}}
.label{{color:var(--muted);font-size:12px}} .value{{font-size:25px;font-weight:700;margin:4px 0}} .delta{{color:var(--accent);font-size:12px}}
.table-wrap{{overflow:auto;border:1px solid var(--line);border-radius:12px}} table{{border-collapse:collapse;width:100%;background:var(--panel)}} th,td{{padding:10px 12px;border-bottom:1px solid var(--line);white-space:nowrap;text-align:right}} th{{position:sticky;top:0;background:#142238;color:#aebed3;font-size:12px}} th:first-child,td:first-child{{text-align:left}} td:nth-child(2),td:nth-child(3){{text-align:left}} .pos{{color:var(--good)}} .neg{{color:var(--bad)}}
.foot{{margin:28px 0 8px;color:var(--muted);font-size:12px}}
@media(max-width:900px){{.wrap{{padding:16px}}.cards{{grid-template-columns:repeat(2,1fr)}}}}
</style>
</head>
<body><div class='wrap'>
<h1>Performance Dashboard</h1>
<div class='sub'>{START.date()} ~ {END.date()} · 전년 동기간 {LY_START.date()} ~ {LY_END.date()}</div>
<div class='notice'>BigQuery-only 임시 모드입니다. 현재 GA4 BigQuery 기준 Sessions · Purchases · CVR · Revenue · Source/Medium/Campaign · 채널 YoY를 제공합니다. MSSQL 상품별 실제 매출/수량/할인율 지표는 Windows → BigQuery 적재 복구 후 다시 연결합니다.</div>
<div class='cards'>{cards_html}</div>
<h2>Channel Group Health</h2>
{table_html(major, major_cols, 20)}
<h2>Channel Detail</h2>
{table_html(channel, channel_cols, 50)}
<h2>Source / Medium / Campaign</h2>
{table_html(source_campaign, sc_cols, 100)}
<div class='foot'>Generated from GA4 BigQuery export · Excel: performance_export.xlsx</div>
</div></body></html>"""
    path = OUT / "performance_dashboard.html"
    path.write_text(html, encoding="utf-8")
    return path


def main():
    print(f"[1/4] BigQuery TY {START.date()} ~ {END.date()}")
    current = load_bq(START, END)
    print(f"[2/4] BigQuery LY {LY_START.date()} ~ {LY_END.date()}")
    last_year = load_bq(LY_START, LY_END)
    print("[3/4] Build metrics")
    channel = build_channel(current, last_year)
    major = build_major(current, last_year)
    source_campaign = build_source_campaign(current, last_year)
    daily = build_daily(current, last_year)
    print("[4/4] Output")
    write_excel(channel, major, source_campaign, daily)
    build_html(channel, major, source_campaign, daily)
    print("OK", OUT / "performance_dashboard.html")


if __name__ == "__main__":
    main()
