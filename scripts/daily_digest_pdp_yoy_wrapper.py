#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import math
from typing import Any

import pandas as pd

import daily_digest_live_final as base


def _safe(v: Any) -> float:
    try:
        x = float(v or 0)
        return x if math.isfinite(x) else 0.0
    except Exception:
        return 0.0


def _dual_svg(
    xlabels: list[str],
    current: list[float],
    previous: list[float],
    current_year: int,
    previous_year: int,
    width: int = 330,
    height: int = 104,
) -> str:
    current = [_safe(x) for x in current]
    previous = [_safe(x) for x in previous]
    n = max(len(xlabels), len(current), len(previous), 1)
    xlabels = (xlabels + ["--"] * n)[:n]
    current = (current + [0.0] * n)[:n]
    previous = (previous + [0.0] * n)[:n]

    pad_l, pad_r, pad_t, pad_b = 38, 10, 28, 22
    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b
    ymax = max(current + previous + [1.0])
    ymax = ymax * 1.12 if ymax > 0 else 1.0

    def xy(i: int, value: float) -> tuple[float, float]:
        x = pad_l + inner_w * (i / max(n - 1, 1))
        y = pad_t + inner_h * (1 - value / ymax)
        return x, y

    grid = []
    ylabels = []
    for frac in (0.0, 0.5, 1.0):
        y = pad_t + inner_h * (1 - frac)
        val = ymax * frac
        grid.append(f"<line x1='{pad_l}' y1='{y:.1f}' x2='{width-pad_r}' y2='{y:.1f}' stroke='#e2e8f0' stroke-width='1'/>")
        ylabels.append(f"<text x='{pad_l-6}' y='{y+3:.1f}' text-anchor='end' font-size='8.5' fill='#64748b'>{int(round(val))}</text>")

    def series(values: list[float], color: str, dash: str, label: str) -> str:
        pts = [xy(i, v) for i, v in enumerate(values)]
        poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        dash_attr = f" stroke-dasharray='{dash}'" if dash else ""
        circles = []
        for i, ((x, y), value) in enumerate(zip(pts, values)):
            other = previous[i] if values is current else current[i]
            yoy = ((value - other) / other * 100.0) if other else (0.0 if value == 0 else 100.0)
            title = f"{xlabels[i]} · {label} {value:,.0f} · comparison {other:,.0f} · YoY {yoy:+.1f}%"
            circles.append(
                f"<circle cx='{x:.1f}' cy='{y:.1f}' r='3' fill='{color}' stroke='white' stroke-width='1'>"
                f"<title>{title}</title></circle>"
            )
        return f"<polyline fill='none' stroke='{color}' stroke-width='2.6'{dash_attr} points='{poly}'/>" + "".join(circles)

    xsvg = []
    for i, label in enumerate(xlabels):
        x, _ = xy(i, 0)
        xsvg.append(f"<text x='{x:.1f}' y='{height-5}' text-anchor='middle' font-size='8.5' fill='#64748b'>{label}</text>")

    cur_total = sum(current)
    py_total = sum(previous)
    yoy_total = ((cur_total - py_total) / py_total * 100.0) if py_total else (0.0 if cur_total == 0 else 100.0)
    yoy_color = "#059669" if yoy_total >= 0 else "#dc2626"

    return f"""
    <svg width='{width}' height='{height}' viewBox='0 0 {width} {height}' xmlns='http://www.w3.org/2000/svg' style='display:block;overflow:visible'>
      {''.join(grid)}
      <line x1='{pad_l}' y1='{height-pad_b}' x2='{width-pad_r}' y2='{height-pad_b}' stroke='#cbd5e1' stroke-width='1'/>
      {series(previous, '#94a3b8', '5 4', str(previous_year))}
      {series(current, '#0092CE', '', str(current_year))}
      {''.join(ylabels)}
      {''.join(xsvg)}
      <g transform='translate({pad_l},4)'>
        <line x1='0' y1='7' x2='18' y2='7' stroke='#0092CE' stroke-width='3'/>
        <text x='23' y='10' font-size='9' font-weight='700' fill='#334155'>{current_year}</text>
        <line x1='65' y1='7' x2='83' y2='7' stroke='#94a3b8' stroke-width='3' stroke-dasharray='5 4'/>
        <text x='88' y='10' font-size='9' font-weight='700' fill='#64748b'>{previous_year}</text>
        <text x='142' y='10' font-size='9' font-weight='800' fill='{yoy_color}'>7D YoY {yoy_total:+.1f}%</text>
      </g>
    </svg>
    """


def get_category_pdp_view_trend_bq_yoy(end_date: dt.date):
    axis_dates = [end_date - dt.timedelta(days=i) for i in range(6, -1, -1)]
    previous_dates = [d - dt.timedelta(days=364) for d in axis_dates]
    xlabels = [d.strftime("%m/%d") for d in axis_dates]

    if base.bigquery is None or not base.BQ_EVENTS_TABLE:
        return pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"]), {}

    try:
        bq = base.bigquery.Client()
        cur_start, cur_end = axis_dates[0], axis_dates[-1]
        py_start, py_end = previous_dates[0], previous_dates[-1]
        lookup_cur_start = end_date - dt.timedelta(days=30)
        lookup_py_start = py_end - dt.timedelta(days=30)

        sql = f"""
        WITH item_lookup AS (
          SELECT
            items.item_id AS item_id,
            ARRAY_AGG(
              STRUCT(
                CASE WHEN items.item_category = 'SALE' THEN items.item_category2 ELSE items.item_category END AS c1_norm,
                CASE WHEN items.item_category = 'SALE' THEN items.item_category3 ELSE items.item_category2 END AS c2_norm
              ) IGNORE NULLS ORDER BY event_timestamp DESC LIMIT 1
            )[SAFE_OFFSET(0)] AS cat
          FROM `{base.BQ_EVENTS_TABLE}`
          CROSS JOIN UNNEST(items) AS items
          WHERE (
              _TABLE_SUFFIX BETWEEN '{lookup_cur_start:%Y%m%d}' AND '{cur_end:%Y%m%d}'
              OR _TABLE_SUFFIX BETWEEN '{lookup_py_start:%Y%m%d}' AND '{py_end:%Y%m%d}'
            )
            AND items.item_id IS NOT NULL
            AND (items.item_category IS NOT NULL OR items.item_category2 IS NOT NULL OR items.item_category3 IS NOT NULL)
          GROUP BY 1
        ), pdp AS (
          SELECT
            CASE
              WHEN PARSE_DATE('%Y%m%d', event_date) BETWEEN DATE '{cur_start}' AND DATE '{cur_end}' THEN 'current'
              ELSE 'previous'
            END AS period,
            PARSE_DATE('%Y%m%d', event_date) AS d,
            UPPER(IFNULL(l.cat.c1_norm,'')) AS c1,
            IFNULL(l.cat.c2_norm,'') AS c2,
            COUNT(*) AS views
          FROM `{base.BQ_EVENTS_TABLE}` e
          CROSS JOIN UNNEST(e.items) AS items
          JOIN item_lookup l ON l.item_id = items.item_id
          WHERE (
              _TABLE_SUFFIX BETWEEN '{cur_start:%Y%m%d}' AND '{cur_end:%Y%m%d}'
              OR _TABLE_SUFFIX BETWEEN '{py_start:%Y%m%d}' AND '{py_end:%Y%m%d}'
            )
            AND event_name = 'view_item'
            AND items.item_id IS NOT NULL
          GROUP BY 1,2,3,4
        )
        SELECT period, d, c1, c2, views FROM pdp
        """

        df = bq.query(sql, location=base.BQ_LOCATION or None).to_dataframe()
        if df.empty:
            return pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"]), {}

        df["d"] = pd.to_datetime(df["d"], errors="coerce").dt.date
        df["views"] = pd.to_numeric(df["views"], errors="coerce").fillna(0.0)
        df["c1"] = df["c1"].astype(str).str.strip().str.upper()
        df["c2"] = df["c2"].astype(str).str.strip()

        rows = []
        for c1, subs in base.PDP_CATEGORY_MAP.items():
            for sub_label, c2_list in subs.items():
                def values(period: str, dates: list[dt.date]) -> list[float]:
                    out = []
                    for d in dates:
                        mask = (df["period"] == period) & (df["d"] == d) & (df["c1"] == c1)
                        mask &= df["c2"].isin(c2_list) if c2_list else (df["c2"] == sub_label)
                        out.append(float(df.loc[mask, "views"].sum()))
                    return out

                ys = values("current", axis_dates)
                ys_py = values("previous", previous_dates)
                cur_avg = sum(ys) / len(ys) if ys else 0.0
                py_avg = sum(ys_py) / len(ys_py) if ys_py else 0.0
                rows.append({
                    "itemCategory": f"{c1} · {sub_label}",
                    "views_d1": ys[-1] if ys else 0.0,
                    "views_avg7d": cur_avg,
                    "views_d1_py": ys_py[-1] if ys_py else 0.0,
                    "views_avg7d_py": py_avg,
                    "ys": ys,
                    "ys_py": ys_py,
                    "trend_svg": _dual_svg(xlabels, ys, ys_py, end_date.year, end_date.year - 1),
                })

        frame = pd.DataFrame(rows)
        payload = {
            "x": xlabels,
            "current_year": end_date.year,
            "previous_year": end_date.year - 1,
            "comparison_shift_days": 364,
            "rows": [{
                "itemCategory": r["itemCategory"],
                "views_d1": r["views_d1"],
                "views_avg7d": r["views_avg7d"],
                "views_d1_py": r["views_d1_py"],
                "views_avg7d_py": r["views_avg7d_py"],
                "ys": r["ys"],
                "ys_py": r["ys_py"],
            } for r in rows],
        }
        return frame, payload
    except Exception as exc:
        print(f"[WARN] PDP Category Trend YoY BigQuery failed: {type(exc).__name__}: {exc}")
        return pd.DataFrame(columns=["itemCategory", "views_d1", "views_avg7d", "trend_svg"]), {}


base.get_category_pdp_view_trend_bq = get_category_pdp_view_trend_bq_yoy

if __name__ == "__main__":
    base.main()
