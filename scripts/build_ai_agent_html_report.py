#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia AI Agent HTML Report Builder

What this file does
-------------------
1) Reads AI Agent order/admin KPI data from BigQuery
2) Builds compact KPI JSON for the AI/commentary layer
3) Calls free/free-tier LLM APIs in order if keys exist:
   Gemini -> Groq -> OpenRouter
4) Falls back to deterministic rule-based insights when no API key exists or API fails
5) Writes a self-contained HTML report:
   reports/ai_agent/daily/YYYY-MM-DD.html
   reports/ai_agent/data/YYYY-MM-DD.json
   reports/ai_agent/index.html

Design note
-----------
CSS is embedded and inspired by the uploaded Columbia visual direction:
large editorial hero, dark forest/black palette, warm off-white background,
rounded metric cards, clean Korean typography. No font files are embedded.

Required BigQuery tables by default
-----------------------------------
- columbia-ga4.crm_mart.member_funnel_admin_daily
- columbia-ga4.crm_raw.tb_order_product_search_mart

Optional env / args
-------------------
GEMINI_API_KEY / GROQ_API_KEY / OPENROUTER_API_KEY are optional.
No key = rule-based report still builds.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import html
import json
import os
import re
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from google.cloud import bigquery

KST = dt.timezone(dt.timedelta(hours=9))
BASE_DIR = Path(__file__).resolve().parents[1] if Path(__file__).resolve().parent.name == "scripts" else Path(__file__).resolve().parent


def log(msg: str) -> None:
    now = dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    print(f"[{now}] {msg}", flush=True)


def getenv(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def setup_google_credentials() -> None:
    cred_path = getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and Path(cred_path).exists():
        log(f"Using GOOGLE_APPLICATION_CREDENTIALS: {cred_path}")
        return

    b64 = getenv("GOOGLE_SA_JSON_B64")
    if b64:
        out = Path(getenv("GOOGLE_SA_JSON_OUT", "gcp_service_account.json"))
        out.write_bytes(base64.b64decode(b64))
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(out.resolve())
        log(f"Wrote service account json: {out.resolve()}")
        return

    raise RuntimeError("Google credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_SA_JSON_B64.")


def kst_yesterday() -> str:
    return (dt.datetime.now(KST).date() - dt.timedelta(days=1)).isoformat()


def parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def krw(v: Any) -> str:
    try:
        return f"{int(round(float(v))):,}원"
    except Exception:
        return "-"


def num(v: Any) -> str:
    try:
        return f"{int(round(float(v))):,}"
    except Exception:
        return "-"


def pct(v: Any, digits: int = 1) -> str:
    if v is None:
        return "-"
    try:
        x = float(v)
    except Exception:
        return "-"
    sign = "+" if x > 0 else ""
    return f"{sign}{x:.{digits}f}%"


def pp(v: Any, digits: int = 1) -> str:
    if v is None:
        return "-"
    try:
        x = float(v)
    except Exception:
        return "-"
    sign = "+" if x > 0 else ""
    return f"{sign}{x:.{digits}f}p"


def safe_pct_change(cur: float, prev: float) -> Optional[float]:
    if prev is None or float(prev) == 0:
        return None
    return (float(cur) - float(prev)) / float(prev) * 100


def esc(v: Any) -> str:
    return html.escape("" if v is None else str(v))


def clean_records(df: pd.DataFrame, limit: Optional[int] = None) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    if limit:
        df = df.head(limit)
    out: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        item = {}
        for k, v in row.items():
            if pd.isna(v):
                item[k] = None
            elif hasattr(v, "item"):
                item[k] = v.item()
            elif isinstance(v, (dt.date, dt.datetime)):
                item[k] = v.isoformat()
            else:
                item[k] = v
        out.append(item)
    return out


class BigQueryReportData:
    def __init__(self, project: str, location: str, raw_dataset: str, mart_dataset: str, order_table: str, admin_table: str):
        self.project = project
        self.location = location
        self.raw_dataset = raw_dataset
        self.mart_dataset = mart_dataset
        self.order_table = order_table
        self.admin_table = admin_table
        self.client = bigquery.Client(project=project, location=location)

    def query_df(self, sql: str) -> pd.DataFrame:
        log("Running BigQuery SQL")
        return self.client.query(sql, location=self.location).to_dataframe()

    def load(self, target_date: str) -> dict[str, Any]:
        d = parse_date(target_date)
        prev = (d - dt.timedelta(days=7)).isoformat()
        start_30 = (d - dt.timedelta(days=29)).isoformat()
        start_7 = (d - dt.timedelta(days=6)).isoformat()

        admin_sql = f"""
        WITH base AS (
          SELECT *
          FROM `{self.project}.{self.mart_dataset}.{self.admin_table}`
          WHERE report_date IN (DATE('{target_date}'), DATE('{prev}'))
        )
        SELECT * FROM base ORDER BY report_date
        """
        admin = self.query_df(admin_sql)

        order_table_ref = f"`{self.project}.{self.raw_dataset}.{self.order_table}`"

        category_sql = f"""
        WITH cur AS (
          SELECT
            COALESCE(NULLIF(category_title_kr, ''), NULLIF(category_title, ''), NULLIF(relation_category, ''), CAST(category_manager_no AS STRING), '미분류') AS category_name,
            SUM(COALESCE(net_erp_revenue, 0)) AS revenue,
            COUNT(DISTINCT order_no) AS orders,
            COUNT(DISTINCT member_id) AS buyers,
            SUM(COALESCE(purchase_qty, 0)) AS qty,
            SUM(COALESCE(order_use_coupon_price, 0)) AS coupon_used,
            COUNT(DISTINCT CASE WHEN is_coupon_order = 1 THEN order_no END) AS coupon_orders,
            SUM(CASE WHEN is_soldout_size = 1 THEN 1 ELSE 0 END) AS soldout_lines,
            SUM(CASE WHEN is_low_stock_size = 1 THEN 1 ELSE 0 END) AS low_stock_lines
          FROM {order_table_ref}
          WHERE DATE(order_date) = DATE('{target_date}')
          GROUP BY category_name
        ), prev AS (
          SELECT
            COALESCE(NULLIF(category_title_kr, ''), NULLIF(category_title, ''), NULLIF(relation_category, ''), CAST(category_manager_no AS STRING), '미분류') AS category_name,
            SUM(COALESCE(net_erp_revenue, 0)) AS revenue_prev,
            COUNT(DISTINCT order_no) AS orders_prev,
            COUNT(DISTINCT member_id) AS buyers_prev,
            SUM(COALESCE(purchase_qty, 0)) AS qty_prev
          FROM {order_table_ref}
          WHERE DATE(order_date) = DATE('{prev}')
          GROUP BY category_name
        )
        SELECT
          COALESCE(cur.category_name, prev.category_name) AS category_name,
          COALESCE(cur.revenue, 0) AS revenue,
          COALESCE(prev.revenue_prev, 0) AS revenue_prev,
          COALESCE(cur.revenue, 0) - COALESCE(prev.revenue_prev, 0) AS revenue_diff,
          SAFE_DIVIDE(COALESCE(cur.revenue, 0) - COALESCE(prev.revenue_prev, 0), NULLIF(COALESCE(prev.revenue_prev, 0), 0)) * 100 AS revenue_change_pct,
          COALESCE(cur.orders, 0) AS orders,
          COALESCE(prev.orders_prev, 0) AS orders_prev,
          COALESCE(cur.buyers, 0) AS buyers,
          COALESCE(prev.buyers_prev, 0) AS buyers_prev,
          COALESCE(cur.qty, 0) AS qty,
          COALESCE(prev.qty_prev, 0) AS qty_prev,
          COALESCE(cur.coupon_used, 0) AS coupon_used,
          COALESCE(cur.coupon_orders, 0) AS coupon_orders,
          COALESCE(cur.soldout_lines, 0) AS soldout_lines,
          COALESCE(cur.low_stock_lines, 0) AS low_stock_lines
        FROM cur
        FULL OUTER JOIN prev USING(category_name)
        ORDER BY revenue_diff ASC
        LIMIT 30
        """
        category = self.query_df(category_sql)

        product_sql = f"""
        WITH cur AS (
          SELECT
            product_code,
            ANY_VALUE(COALESCE(NULLIF(product_name_kor, ''), NULLIF(product_name, ''), product_code)) AS product_name,
            ANY_VALUE(COALESCE(NULLIF(category_title_kr, ''), NULLIF(category_title, ''), NULLIF(relation_category, ''), CAST(category_manager_no AS STRING), '미분류')) AS category_name,
            SUM(COALESCE(net_erp_revenue, 0)) AS revenue,
            COUNT(DISTINCT order_no) AS orders,
            COUNT(DISTINCT member_id) AS buyers,
            SUM(COALESCE(purchase_qty, 0)) AS qty,
            MAX(COALESCE(size_stock_qty, 0)) AS max_size_stock_qty,
            MIN(COALESCE(size_stock_qty, 0)) AS min_size_stock_qty,
            MAX(COALESCE(is_soldout_size, 0)) AS any_soldout,
            MAX(COALESCE(is_low_stock_size, 0)) AS any_low_stock,
            SUM(COALESCE(order_use_coupon_price, 0)) AS coupon_used,
            SUM(COALESCE(product_promotion_sale_price, 0)) AS promotion_sale_amount,
            MAX(COALESCE(cart_add_count_180d, 0)) AS cart_add_count_180d
          FROM {order_table_ref}
          WHERE DATE(order_date) = DATE('{target_date}')
          GROUP BY product_code
        ), prev AS (
          SELECT
            product_code,
            SUM(COALESCE(net_erp_revenue, 0)) AS revenue_prev,
            COUNT(DISTINCT order_no) AS orders_prev,
            COUNT(DISTINCT member_id) AS buyers_prev,
            SUM(COALESCE(purchase_qty, 0)) AS qty_prev
          FROM {order_table_ref}
          WHERE DATE(order_date) = DATE('{prev}')
          GROUP BY product_code
        )
        SELECT
          COALESCE(cur.product_code, prev.product_code) AS product_code,
          COALESCE(cur.product_name, COALESCE(cur.product_code, prev.product_code)) AS product_name,
          COALESCE(cur.category_name, '미분류') AS category_name,
          COALESCE(cur.revenue, 0) AS revenue,
          COALESCE(prev.revenue_prev, 0) AS revenue_prev,
          COALESCE(cur.revenue, 0) - COALESCE(prev.revenue_prev, 0) AS revenue_diff,
          SAFE_DIVIDE(COALESCE(cur.revenue, 0) - COALESCE(prev.revenue_prev, 0), NULLIF(COALESCE(prev.revenue_prev, 0), 0)) * 100 AS revenue_change_pct,
          COALESCE(cur.orders, 0) AS orders,
          COALESCE(prev.orders_prev, 0) AS orders_prev,
          COALESCE(cur.buyers, 0) AS buyers,
          COALESCE(cur.qty, 0) AS qty,
          COALESCE(prev.qty_prev, 0) AS qty_prev,
          COALESCE(cur.max_size_stock_qty, 0) AS max_size_stock_qty,
          COALESCE(cur.min_size_stock_qty, 0) AS min_size_stock_qty,
          COALESCE(cur.any_soldout, 0) AS any_soldout,
          COALESCE(cur.any_low_stock, 0) AS any_low_stock,
          COALESCE(cur.coupon_used, 0) AS coupon_used,
          COALESCE(cur.promotion_sale_amount, 0) AS promotion_sale_amount,
          COALESCE(cur.cart_add_count_180d, 0) AS cart_add_count_180d
        FROM cur
        FULL OUTER JOIN prev USING(product_code)
        ORDER BY revenue_diff ASC
        LIMIT 50
        """
        products = self.query_df(product_sql)

        stock_sql = f"""
        SELECT
          product_code,
          ANY_VALUE(COALESCE(NULLIF(product_name_kor, ''), NULLIF(product_name, ''), product_code)) AS product_name,
          ANY_VALUE(COALESCE(NULLIF(category_title_kr, ''), NULLIF(category_title, ''), NULLIF(relation_category, ''), CAST(category_manager_no AS STRING), '미분류')) AS category_name,
          SUM(CASE WHEN DATE(order_date) BETWEEN DATE('{start_30}') AND DATE('{target_date}') THEN COALESCE(net_erp_revenue, 0) ELSE 0 END) AS revenue_30d,
          SUM(CASE WHEN DATE(order_date) BETWEEN DATE('{start_7}') AND DATE('{target_date}') THEN COALESCE(net_erp_revenue, 0) ELSE 0 END) AS revenue_7d,
          COUNT(DISTINCT CASE WHEN DATE(order_date) BETWEEN DATE('{start_30}') AND DATE('{target_date}') THEN order_no END) AS orders_30d,
          MAX(COALESCE(size_stock_qty, 0)) AS max_size_stock_qty,
          MIN(COALESCE(size_stock_qty, 0)) AS min_size_stock_qty,
          MAX(COALESCE(is_soldout_size, 0)) AS any_soldout,
          MAX(COALESCE(is_low_stock_size, 0)) AS any_low_stock
        FROM {order_table_ref}
        WHERE DATE(order_date) BETWEEN DATE('{start_30}') AND DATE('{target_date}')
        GROUP BY product_code
        HAVING revenue_30d > 0 AND (any_soldout = 1 OR any_low_stock = 1)
        ORDER BY revenue_30d DESC
        LIMIT 20
        """
        stock = self.query_df(stock_sql)

        channel_sql = f"""
        SELECT
          order_device_type,
          COUNT(DISTINCT order_no) AS orders,
          COUNT(DISTINCT member_id) AS buyers,
          SUM(COALESCE(net_erp_revenue, 0)) AS revenue,
          SUM(COALESCE(order_use_coupon_price, 0)) AS coupon_used,
          COUNT(DISTINCT CASE WHEN is_coupon_order = 1 THEN order_no END) AS coupon_orders,
          COUNT(DISTINCT CASE WHEN is_promotion_line = 1 THEN order_no END) AS promotion_orders,
          SUM(COALESCE(product_promotion_sale_price, 0)) AS promotion_sale_amount
        FROM {order_table_ref}
        WHERE DATE(order_date) = DATE('{target_date}')
        GROUP BY order_device_type
        ORDER BY revenue DESC
        """
        device = self.query_df(channel_sql)

        return {
            "target_date": target_date,
            "previous_date": prev,
            "admin": clean_records(admin),
            "category": clean_records(category),
            "products": clean_records(products),
            "stock_risks": clean_records(stock),
            "device": clean_records(device),
        }


def get_admin_pair(data: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    target = data["target_date"]
    prev = data["previous_date"]
    rows = data.get("admin", [])
    cur_row = next((r for r in rows if str(r.get("report_date"))[:10] == target), {})
    prev_row = next((r for r in rows if str(r.get("report_date"))[:10] == prev), {})
    return cur_row, prev_row


def build_kpi_payload(data: dict[str, Any]) -> dict[str, Any]:
    cur, prev = get_admin_pair(data)

    def f(row: dict[str, Any], key: str) -> float:
        try:
            return float(row.get(key) or 0)
        except Exception:
            return 0.0

    current = {
        "revenue": f(cur, "revenue"),
        "orders": f(cur, "orders"),
        "buyers": f(cur, "buyers"),
        "sessions": f(cur, "sessions"),
        "pv": f(cur, "pv"),
        "signups": f(cur, "signups"),
        "aov": f(cur, "aov"),
        "cvr": f(cur, "cvr") * 100 if f(cur, "cvr") <= 1 else f(cur, "cvr"),
        "coupon_used": f(cur, "coupon_used"),
        "point_used": f(cur, "point_used"),
        "cancel_amount": f(cur, "cancel_amount"),
        "coupon_order_rate": f(cur, "coupon_order_rate") * 100 if f(cur, "coupon_order_rate") <= 1 else f(cur, "coupon_order_rate"),
        "promotion_order_rate": f(cur, "promotion_order_rate") * 100 if f(cur, "promotion_order_rate") <= 1 else f(cur, "promotion_order_rate"),
    }
    previous = {
        "revenue": f(prev, "revenue"),
        "orders": f(prev, "orders"),
        "buyers": f(prev, "buyers"),
        "sessions": f(prev, "sessions"),
        "pv": f(prev, "pv"),
        "signups": f(prev, "signups"),
        "aov": f(prev, "aov"),
        "cvr": f(prev, "cvr") * 100 if f(prev, "cvr") <= 1 else f(prev, "cvr"),
        "coupon_used": f(prev, "coupon_used"),
        "point_used": f(prev, "point_used"),
        "cancel_amount": f(prev, "cancel_amount"),
    }
    changes = {f"{k}_change_pct": safe_pct_change(current[k], previous.get(k, 0)) for k in current if k in previous}

    return {
        "target_date": data["target_date"],
        "previous_date": data["previous_date"],
        "current": current,
        "previous": previous,
        "changes": changes,
        "category_drops": data.get("category", [])[:10],
        "product_drops": data.get("products", [])[:15],
        "stock_risks": data.get("stock_risks", [])[:10],
        "device": data.get("device", []),
    }


class InsightGenerator:
    def __init__(self, provider_order: str = "gemini,groq,openrouter", timeout: int = 45):
        self.providers = [x.strip().lower() for x in provider_order.split(",") if x.strip()]
        self.timeout = timeout

    def generate(self, payload: dict[str, Any]) -> dict[str, Any]:
        last_error = None
        for provider in self.providers:
            try:
                if provider == "gemini":
                    result = self._gemini(payload)
                elif provider == "groq":
                    result = self._groq(payload)
                elif provider == "openrouter":
                    result = self._openrouter(payload)
                else:
                    continue
                result["provider"] = provider
                return self._normalize(result)
            except Exception as e:
                last_error = f"{provider}: {type(e).__name__}: {e}"
                log(f"AI provider failed: {last_error}")
        result = self.rule_based(payload)
        if last_error:
            result["llm_error"] = last_error
        return result

    def _system(self) -> str:
        return """
너는 Columbia Korea 자사몰 이커머스 마케팅 분석 에이전트다.
주어진 집계 KPI만 근거로 분석한다.
없는 데이터는 단정하지 말고 추가 확인 필요라고 말한다.
개인정보나 개별 회원 추정은 하지 않는다.
한국어로 작성한다.
반드시 JSON만 반환한다.
스키마:
{
  "headline": "한 줄 핵심 진단",
  "summary": "2~4문장 요약",
  "alerts": ["주요 이상징후"],
  "causes": ["원인 후보"],
  "actions_today": ["오늘 바로 할 일"],
  "actions_week": ["이번 주 할 일"],
  "watch_next": ["다음 리포트에서 추적할 지표"],
  "confidence": "low|medium|high",
  "data_limits": ["해석 주의점"]
}
""".strip()

    def _user(self, payload: dict[str, Any]) -> str:
        safe_payload = json.dumps(payload, ensure_ascii=False, indent=2)
        return f"아래 KPI_JSON을 분석해줘.\n\nKPI_JSON:\n{safe_payload}"

    def _extract_json(self, text: str) -> dict[str, Any]:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if not m:
            raise ValueError("No JSON object found")
        return json.loads(m.group(0))

    def _normalize(self, x: dict[str, Any]) -> dict[str, Any]:
        defaults = {
            "headline": "자사몰 성과 자동 진단",
            "summary": "데이터 기반 자동 진단을 생성했습니다.",
            "alerts": [],
            "causes": [],
            "actions_today": [],
            "actions_week": [],
            "watch_next": [],
            "confidence": "medium",
            "data_limits": [],
        }
        for k, v in defaults.items():
            x.setdefault(k, v)
        return x

    def _gemini(self, payload: dict[str, Any]) -> dict[str, Any]:
        api_key = getenv("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY missing")
        model = getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        body = {
            "contents": [{"parts": [{"text": self._system() + "\n\n" + self._user(payload)}]}],
            "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"},
        }
        r = requests.post(url, json=body, timeout=self.timeout)
        r.raise_for_status()
        text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
        return self._extract_json(text)

    def _groq(self, payload: dict[str, Any]) -> dict[str, Any]:
        api_key = getenv("GROQ_API_KEY")
        if not api_key:
            raise RuntimeError("GROQ_API_KEY missing")
        model = getenv("GROQ_MODEL", "llama-3.1-8b-instant")
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": self._system()},
                {"role": "user", "content": self._user(payload)},
            ],
            "temperature": 0.2,
            "response_format": {"type": "json_object"},
        }
        r = requests.post("https://api.groq.com/openai/v1/chat/completions", headers={"Authorization": f"Bearer {api_key}"}, json=body, timeout=self.timeout)
        r.raise_for_status()
        return self._extract_json(r.json()["choices"][0]["message"]["content"])

    def _openrouter(self, payload: dict[str, Any]) -> dict[str, Any]:
        api_key = getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY missing")
        model = getenv("OPENROUTER_MODEL", "openrouter/free")
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": self._system()},
                {"role": "user", "content": self._user(payload)},
            ],
            "temperature": 0.2,
            "response_format": {"type": "json_object"},
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "HTTP-Referer": getenv("OPENROUTER_SITE_URL", "https://github.com"),
            "X-Title": getenv("OPENROUTER_APP_NAME", "Columbia AI Agent Report"),
        }
        r = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=body, timeout=self.timeout)
        r.raise_for_status()
        return self._extract_json(r.json()["choices"][0]["message"]["content"])

    def rule_based(self, p: dict[str, Any]) -> dict[str, Any]:
        cur = p["current"]
        ch = p["changes"]
        category_drops = p.get("category_drops", [])
        product_drops = p.get("product_drops", [])
        stock_risks = p.get("stock_risks", [])

        revenue_change = ch.get("revenue_change_pct")
        buyers_change = ch.get("buyers_change_pct")
        orders_change = ch.get("orders_change_pct")
        aov_change = ch.get("aov_change_pct")
        sessions_change = ch.get("sessions_change_pct")

        alerts: list[str] = []
        causes: list[str] = []
        today: list[str] = []
        week: list[str] = []
        watch: list[str] = []

        if revenue_change is not None and revenue_change <= -10:
            alerts.append(f"매출이 전주 동일 요일 대비 {pct(revenue_change)} 하락했습니다.")
        elif revenue_change is not None and revenue_change >= 10:
            alerts.append(f"매출이 전주 동일 요일 대비 {pct(revenue_change)} 상승했습니다.")
        else:
            alerts.append("전주 동일 요일 대비 매출 급변은 크지 않습니다.")

        if sessions_change is not None and sessions_change >= 0 and buyers_change is not None and buyers_change < 0:
            causes.append("세션은 유지/증가했지만 구매자가 감소해 유입보다는 전환율 이슈 가능성이 있습니다.")
            today.append("구매전환 하락 상품의 상세페이지, 가격, 쿠폰, 재고를 우선 점검하세요.")
        if orders_change is not None and orders_change < -5:
            causes.append(f"주문수가 {pct(orders_change)} 감소했습니다.")
        if buyers_change is not None and buyers_change < -5:
            causes.append(f"구매자 수가 {pct(buyers_change)} 감소했습니다.")
        if aov_change is not None and aov_change < -5:
            causes.append(f"객단가가 {pct(aov_change)} 하락했습니다.")
            today.append("고가 상품 판매 비중과 쿠폰/프로모션 적용률을 함께 확인하세요.")

        worst_cat = next((x for x in category_drops if float(x.get("revenue_diff") or 0) < 0), None)
        if worst_cat:
            causes.append(f"카테고리 하락 기여는 {worst_cat.get('category_name')}가 가장 큽니다. 매출 증감 {krw(worst_cat.get('revenue_diff'))}입니다.")
            today.append(f"{worst_cat.get('category_name')} 내 하락 상품과 품절/저재고 여부를 확인하세요.")
            watch.append(f"{worst_cat.get('category_name')} 카테고리의 7일 누적 매출과 구매전환 추이")

        drop_products = [x for x in product_drops if float(x.get("revenue_diff") or 0) < 0][:3]
        if drop_products:
            codes = ", ".join(str(x.get("product_code")) for x in drop_products)
            causes.append(f"상품 하락 기여 Top 후보는 {codes}입니다.")
            today.append("하락 상품 Top 3의 가격, 노출, 상세페이지 상단, 쿠폰 적용 여부를 점검하세요.")

        if stock_risks:
            causes.append("최근 매출이 있는 상품 중 품절/저재고 신호가 감지됩니다.")
            today.append("재고 위험 상품은 광고/EDM 노출 전 사이즈별 재고를 먼저 확인하세요.")
            week.append("상위 매출 상품의 사이즈 품절률을 별도 KPI로 추적하세요.")

        if not today:
            today.append("카테고리별 하락 기여도와 상품별 순매출 증감을 먼저 확인하세요.")
        week.append("GA4 상품조회/장바구니 데이터와 결합해 조회 대비 구매전환 낮은 상품을 분리하세요.")
        watch.extend(["매출/주문수/구매자/객단가 WoW", "쿠폰 주문 비중", "품절/저재고 상품의 매출 기여도"])

        headline = f"{p['target_date']} 매출 {krw(cur.get('revenue'))}, WoW {pct(revenue_change)}"
        summary = f"주문수는 WoW {pct(orders_change)}, 구매자 수는 {pct(buyers_change)}, 객단가는 {pct(aov_change)}입니다. 주요 하락 상품과 재고/혜택 시그널을 함께 확인하는 구성이 적합합니다."

        return self._normalize({
            "provider": "rule_based",
            "headline": headline,
            "summary": summary,
            "alerts": alerts[:5],
            "causes": causes[:6],
            "actions_today": today[:6],
            "actions_week": week[:5],
            "watch_next": watch[:6],
            "confidence": "medium",
            "data_limits": [
                "본 리포트는 BigQuery에 적재된 MSSQL 주문상품 마트 기준입니다.",
                "재고는 주문 당시 재고가 아니라 현재/최근 적재 시점의 재고 신호일 수 있습니다.",
                "GA4 상품조회/광고비/발송 데이터가 결합되면 원인 판단 정확도가 올라갑니다.",
            ],
        })


def render_list(items: list[Any]) -> str:
    if not items:
        return "<li>데이터 없음</li>"
    return "\n".join(f"<li>{esc(x)}</li>" for x in items)


def delta_class(v: Any) -> str:
    try:
        x = float(v)
    except Exception:
        return "neutral"
    if x > 0:
        return "positive"
    if x < 0:
        return "negative"
    return "neutral"


def table_rows(records: list[dict[str, Any]], columns: list[tuple[str, str, str]], limit: int = 10) -> str:
    if not records:
        return f"<tr><td colspan='{len(columns)}' class='empty'>데이터 없음</td></tr>"
    trs = []
    for r in records[:limit]:
        tds = []
        for key, label, kind in columns:
            v = r.get(key)
            cls = ""
            if kind == "krw":
                text = krw(v)
            elif kind == "num":
                text = num(v)
            elif kind == "pct":
                text = pct(v)
                cls = delta_class(v)
            elif kind == "delta_krw":
                text = krw(v)
                cls = delta_class(v)
            else:
                text = esc(v)
            tds.append(f"<td class='{cls}'>{text}</td>")
        trs.append("<tr>" + "".join(tds) + "</tr>")
    return "\n".join(trs)


def render_html(payload: dict[str, Any], insight: dict[str, Any]) -> str:
    cur = payload["current"]
    prev = payload["previous"]
    ch = payload["changes"]
    generated = dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")

    product_cols = [
        ("product_code", "상품코드", "text"),
        ("product_name", "상품명", "text"),
        ("category_name", "카테고리", "text"),
        ("revenue", "매출", "krw"),
        ("revenue_prev", "전주", "krw"),
        ("revenue_diff", "증감", "delta_krw"),
        ("revenue_change_pct", "증감률", "pct"),
        ("max_size_stock_qty", "재고", "num"),
        ("cart_add_count_180d", "장바구니", "num"),
    ]
    category_cols = [
        ("category_name", "카테고리", "text"),
        ("revenue", "매출", "krw"),
        ("revenue_prev", "전주", "krw"),
        ("revenue_diff", "증감", "delta_krw"),
        ("revenue_change_pct", "증감률", "pct"),
        ("orders", "주문", "num"),
        ("buyers", "구매자", "num"),
        ("soldout_lines", "품절라인", "num"),
    ]
    stock_cols = [
        ("product_code", "상품코드", "text"),
        ("product_name", "상품명", "text"),
        ("category_name", "카테고리", "text"),
        ("revenue_30d", "30일 매출", "krw"),
        ("orders_30d", "30일 주문", "num"),
        ("max_size_stock_qty", "최대재고", "num"),
        ("min_size_stock_qty", "최소재고", "num"),
        ("any_soldout", "품절", "num"),
        ("any_low_stock", "저재고", "num"),
    ]
    device_cols = [
        ("order_device_type", "디바이스", "text"),
        ("revenue", "매출", "krw"),
        ("orders", "주문", "num"),
        ("buyers", "구매자", "num"),
        ("coupon_used", "쿠폰", "krw"),
        ("coupon_orders", "쿠폰주문", "num"),
        ("promotion_orders", "프로모션주문", "num"),
    ]

    css = r"""
    :root{
      --forest:#17231d; --forest2:#26382f; --charcoal:#111513; --ink:#1f261f;
      --paper:#f4f0e8; --paper2:#faf8f2; --card:#fffdf8; --line:#e0d8c9;
      --muted:#7d776b; --accent:#d6ff5f; --accent2:#8ba878; --red:#b42318; --green:#067647;
      --shadow:0 24px 80px rgba(23,35,29,.14);
    }
    *{box-sizing:border-box} body{margin:0;background:var(--paper);color:var(--ink);font-family:'SUIT','Pretendard','Apple SD Gothic Neo','Segoe UI',sans-serif;}
    .wrap{max-width:1440px;margin:0 auto;padding:28px 28px 80px}.hero{min-height:360px;border-radius:34px;background:radial-gradient(circle at 75% 10%,rgba(214,255,95,.22),transparent 28%),linear-gradient(135deg,var(--forest) 0%,var(--charcoal) 56%,#39473b 100%);color:#fff;position:relative;overflow:hidden;padding:42px 44px;box-shadow:var(--shadow)}
    .hero:after{content:'COLUMBIA';position:absolute;right:-32px;bottom:-36px;font-size:148px;font-weight:900;letter-spacing:-.08em;color:rgba(255,255,255,.055)}
    .eyebrow{display:inline-flex;gap:10px;align-items:center;padding:8px 13px;border:1px solid rgba(255,255,255,.24);border-radius:999px;background:rgba(255,255,255,.08);backdrop-filter:blur(10px);font-size:13px;color:#eee9db}.dot{width:8px;height:8px;border-radius:99px;background:var(--accent);box-shadow:0 0 18px var(--accent)}
    h1{font-size:64px;line-height:.96;letter-spacing:-.07em;margin:58px 0 0;font-weight:900}.hero-sub{max-width:760px;margin:18px 0 0;font-size:18px;line-height:1.65;color:#dcd8cd}.hero-meta{position:absolute;right:42px;top:42px;text-align:right;color:#d8d3c8;font-size:14px;line-height:1.6}.provider{color:var(--accent);font-weight:800}.grid{display:grid;gap:18px}.kpis{grid-template-columns:repeat(5,1fr);margin-top:-54px;position:relative;z-index:3;padding:0 28px}.card{background:var(--card);border:1px solid var(--line);border-radius:28px;box-shadow:0 16px 45px rgba(32,38,32,.08);padding:24px}.kpi .label{font-size:13px;color:var(--muted);font-weight:800;text-transform:uppercase;letter-spacing:.06em}.kpi .value{font-size:34px;font-weight:900;letter-spacing:-.05em;margin-top:12px}.delta{font-size:13px;margin-top:8px;font-weight:800}.positive{color:var(--green)!important}.negative{color:var(--red)!important}.neutral{color:var(--muted)!important}.section{margin-top:34px}.section-title{display:flex;align-items:flex-end;justify-content:space-between;margin:0 0 14px}.section-title h2{margin:0;font-size:26px;letter-spacing:-.045em}.section-title p{margin:0;color:var(--muted);font-size:14px}.two{grid-template-columns:1.15fr .85fr}.three{grid-template-columns:repeat(3,1fr)}.insight h3{margin:0 0 12px;font-size:18px}.insight p{font-size:17px;line-height:1.75;margin:0;color:#373c35}.list{margin:10px 0 0;padding:0;list-style:none}.list li{position:relative;padding:10px 0 10px 18px;border-bottom:1px solid #eee7da;line-height:1.55}.list li:before{content:'';position:absolute;left:0;top:19px;width:7px;height:7px;border-radius:50%;background:var(--forest2)}.panel-dark{background:linear-gradient(135deg,#202a23,#131815);color:#fff;border:0}.panel-dark .list li{border-color:rgba(255,255,255,.12)}.panel-dark .list li:before{background:var(--accent)}.panel-dark h3,.panel-dark p{color:#fff}.panel-dark .muted{color:#cbc7bc}.table-card{padding:0;overflow:hidden}table{width:100%;border-collapse:collapse;font-size:13px}th{background:#eee7da;color:#6b655d;text-align:left;padding:14px 16px;font-size:12px;text-transform:uppercase;letter-spacing:.04em;white-space:nowrap}td{padding:14px 16px;border-top:1px solid #eee7da;vertical-align:middle}tr:hover td{background:#fbf7ef}.empty{text-align:center;color:var(--muted);padding:30px}.badge{display:inline-flex;align-items:center;border-radius:999px;background:#eef2e6;border:1px solid #d7dfca;padding:5px 10px;font-size:12px;font-weight:800;color:#39473b}.footer{margin-top:36px;color:var(--muted);font-size:13px;text-align:center}.mobile-note{display:none}.mini{font-size:13px;color:var(--muted);line-height:1.6}.headline{font-size:30px;line-height:1.22;font-weight:900;letter-spacing:-.05em;margin:0 0 14px}.summary-box{border-left:5px solid var(--accent2);padding-left:18px}@media(max-width:1100px){.kpis,.two,.three{grid-template-columns:1fr}.kpis{padding:0;margin-top:18px}.hero{min-height:auto}.hero-meta{position:static;text-align:left;margin-top:24px}h1{font-size:44px}.wrap{padding:18px}.table-scroll{overflow-x:auto}.mobile-note{display:block}}
    """

    return f"""<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Columbia AI Agent Report · {esc(payload['target_date'])}</title>
<link href="https://fonts.googleapis.com/css2?family=SUIT:wght@400;500;700;800;900&display=swap" rel="stylesheet">
<style>{css}</style>
</head>
<body>
<div class="wrap">
  <section class="hero">
    <div class="eyebrow"><span class="dot"></span> AI COMMERCE DIAGNOSIS</div>
    <div class="hero-meta">Generated {esc(generated)}<br>Provider <span class="provider">{esc(insight.get('provider','rule_based'))}</span><br>Target {esc(payload['target_date'])}</div>
    <h1>Commerce<br>Performance Agent</h1>
    <p class="hero-sub">MSSQL 주문·상품·재고·카테고리·혜택 데이터를 BigQuery 기준으로 통합 진단한 자동 리포트입니다.</p>
  </section>

  <section class="grid kpis">
    <div class="card kpi"><div class="label">Revenue</div><div class="value">{krw(cur.get('revenue'))}</div><div class="delta {delta_class(ch.get('revenue_change_pct'))}">WoW {pct(ch.get('revenue_change_pct'))}</div></div>
    <div class="card kpi"><div class="label">Orders</div><div class="value">{num(cur.get('orders'))}</div><div class="delta {delta_class(ch.get('orders_change_pct'))}">WoW {pct(ch.get('orders_change_pct'))}</div></div>
    <div class="card kpi"><div class="label">Buyers</div><div class="value">{num(cur.get('buyers'))}</div><div class="delta {delta_class(ch.get('buyers_change_pct'))}">WoW {pct(ch.get('buyers_change_pct'))}</div></div>
    <div class="card kpi"><div class="label">AOV</div><div class="value">{krw(cur.get('aov'))}</div><div class="delta {delta_class(ch.get('aov_change_pct'))}">WoW {pct(ch.get('aov_change_pct'))}</div></div>
    <div class="card kpi"><div class="label">CVR</div><div class="value">{pct(cur.get('cvr'))}</div><div class="delta {delta_class(ch.get('cvr_change_pct'))}">WoW {pct(ch.get('cvr_change_pct'))}</div></div>
  </section>

  <section class="section grid two">
    <div class="card insight">
      <span class="badge">Executive Summary</span>
      <h2 class="headline">{esc(insight.get('headline'))}</h2>
      <div class="summary-box"><p>{esc(insight.get('summary'))}</p></div>
    </div>
    <div class="card panel-dark insight">
      <h3>Today Actions</h3>
      <ul class="list">{render_list(insight.get('actions_today', []))}</ul>
    </div>
  </section>

  <section class="section grid three">
    <div class="card insight"><h3>Alerts</h3><ul class="list">{render_list(insight.get('alerts', []))}</ul></div>
    <div class="card insight"><h3>Causes</h3><ul class="list">{render_list(insight.get('causes', []))}</ul></div>
    <div class="card insight"><h3>Watch Next</h3><ul class="list">{render_list(insight.get('watch_next', []))}</ul></div>
  </section>

  <section class="section">
    <div class="section-title"><h2>Category Drop / Growth</h2><p>전주 동일 요일 대비 카테고리 기여도</p></div>
    <div class="card table-card table-scroll"><table><thead><tr>{''.join(f'<th>{esc(label)}</th>' for _,label,_ in category_cols)}</tr></thead><tbody>{table_rows(payload.get('category_drops', []), category_cols, 12)}</tbody></table></div>
  </section>

  <section class="section">
    <div class="section-title"><h2>Product Detail</h2><p>하락 기여 상품 · 재고 · 장바구니 시그널</p></div>
    <div class="card table-card table-scroll"><table><thead><tr>{''.join(f'<th>{esc(label)}</th>' for _,label,_ in product_cols)}</tr></thead><tbody>{table_rows(payload.get('product_drops', []), product_cols, 15)}</tbody></table></div>
  </section>

  <section class="section grid two">
    <div>
      <div class="section-title"><h2>Stock Risk</h2><p>최근 30일 매출 상품 중 품절/저재고</p></div>
      <div class="card table-card table-scroll"><table><thead><tr>{''.join(f'<th>{esc(label)}</th>' for _,label,_ in stock_cols)}</tr></thead><tbody>{table_rows(payload.get('stock_risks', []), stock_cols, 10)}</tbody></table></div>
    </div>
    <div>
      <div class="section-title"><h2>Device / Benefit</h2><p>주문 디바이스와 혜택 사용</p></div>
      <div class="card table-card table-scroll"><table><thead><tr>{''.join(f'<th>{esc(label)}</th>' for _,label,_ in device_cols)}</tr></thead><tbody>{table_rows(payload.get('device', []), device_cols, 10)}</tbody></table></div>
    </div>
  </section>

  <section class="section grid two">
    <div class="card insight"><h3>This Week Actions</h3><ul class="list">{render_list(insight.get('actions_week', []))}</ul></div>
    <div class="card insight"><h3>Data Limits</h3><ul class="list">{render_list(insight.get('data_limits', []))}</ul><p class="mini">Confidence: {esc(insight.get('confidence'))}</p></div>
  </section>

  <div class="footer">Columbia AI Agent Report · BigQuery + MSSQL Integrated Mart · {esc(generated)}</div>
</div>
</body>
</html>"""


def write_outputs(payload: dict[str, Any], insight: dict[str, Any], html_text: str, out_base: Path) -> dict[str, str]:
    daily_dir = out_base / "daily"
    data_dir = out_base / "data"
    daily_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    target = payload["target_date"]
    html_path = daily_dir / f"{target}.html"
    json_path = data_dir / f"{target}.json"
    index_path = out_base / "index.html"

    html_path.write_text(html_text, encoding="utf-8")
    json_path.write_text(json.dumps({"kpi": payload, "insight": insight}, ensure_ascii=False, indent=2), encoding="utf-8")
    index_path.write_text(html_text, encoding="utf-8")
    return {"html": str(html_path), "json": str(json_path), "index": str(index_path)}


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--target-date", default=getenv("TARGET_DATE", "") or kst_yesterday())
    p.add_argument("--project", default=getenv("BQ_PROJECT", "columbia-ga4"))
    p.add_argument("--location", default=getenv("BQ_LOCATION", "asia-northeast3"))
    p.add_argument("--raw-dataset", default=getenv("BQ_RAW_DATASET", "crm_raw"))
    p.add_argument("--mart-dataset", default=getenv("BQ_MART_DATASET", "crm_mart"))
    p.add_argument("--order-table", default=getenv("BQ_ORDER_PRODUCT_TABLE", "tb_order_product_search_mart"))
    p.add_argument("--admin-table", default=getenv("BQ_ADMIN_DAILY_TABLE", "member_funnel_admin_daily"))
    p.add_argument("--out-dir", default=getenv("AI_AGENT_REPORT_DIR", "reports/ai_agent"))
    p.add_argument("--provider-order", default=getenv("LLM_PROVIDER_ORDER", "gemini,groq,openrouter"))
    return p


def main() -> int:
    try:
        args = build_arg_parser().parse_args()
        setup_google_credentials()
        log("=" * 72)
        log("Columbia AI Agent HTML Report Builder")
        log(f"target_date={args.target_date}, order_table={args.raw_dataset}.{args.order_table}")
        log("=" * 72)

        loader = BigQueryReportData(
            project=args.project,
            location=args.location,
            raw_dataset=args.raw_dataset,
            mart_dataset=args.mart_dataset,
            order_table=args.order_table,
            admin_table=args.admin_table,
        )
        raw_data = loader.load(args.target_date)
        payload = build_kpi_payload(raw_data)
        insight = InsightGenerator(provider_order=args.provider_order).generate(payload)
        html_text = render_html(payload, insight)
        paths = write_outputs(payload, insight, html_text, BASE_DIR / args.out_dir)
        log("Report generated:")
        log(json.dumps(paths, ensure_ascii=False, indent=2))
        return 0
    except Exception as e:
        log(f"ERROR: {type(e).__name__}: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
