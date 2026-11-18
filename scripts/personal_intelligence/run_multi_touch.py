"""Private report runner. Outputs never belong in reports/ or Git."""
import argparse,json,os,sys
from datetime import date,datetime,timedelta
from pathlib import Path
from html import escape
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from multi_touch_bq import load
from attribution import marketing_attribution
from multi_touch import MODELS,CHANNELS
from run_budget import load_budget,render_budget

def render(m):
    def won(x): return f"{x:,.0f}"
    def percent(x): return "—" if x is None else f"{x:.1f}%"
    rows=[]
    for i,c in enumerate(CHANNELS):
        cells="".join(f"<td>{won(m['models'][k][i]['revenue'])}</td>" for k in MODELS)
        assist=m["assisted_orders_by_channel"].get(c,0)
        rows.append(f"<tr><th>{escape(c)}</th>{cells}<td>{assist:,}</td></tr>")
    paths=[]
    for r in m["top_paths"]:
        paths.append(f"<tr><th>{escape(' → '.join(r['path']))}</th><td>{r['orders']:,}</td><td>{won(r['revenue'])}</td></tr>")
    if not paths: paths=["<tr><td colspan='3'>No journey reaches the disclosure threshold (5 orders).</td></tr>"]
    coverage=percent(m["order_coverage_pct"])
    revcoverage=percent(m["revenue_coverage_pct"])
    period=escape(m["period_end"])
    html=f"""<!doctype html><html lang="en"><meta charset="utf-8"><title>CSK Multi-touch Attribution</title>
<style>*{{box-sizing:border-box}}body{{margin:0;background:#fff;color:#173650;font:15px Arial,'Noto Sans',sans-serif}}main{{width:1120px;padding:36px;margin:auto}}header{{border-bottom:3px solid #173650;padding-bottom:17px}}h1{{font-size:30px;margin:10px 0}}h2{{font-size:18px;margin:25px 0 9px}}small,p{{color:#536779}}.kpi{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0}}.kpi div{{border:1px solid #d5dfe9;padding:15px}}.kpi strong{{font-size:25px;display:block;margin-top:8px}}table{{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}}th,td{{padding:12px 10px;border-bottom:1px solid #dfe6ed;text-align:right}}th:first-child{{text-align:left}}thead{{background:#173650;color:#fff}}tbody tr:nth-child(even){{background:#f1f5f8}}footer{{border-top:1px solid #d5dfe9;margin-top:24px;padding-top:12px;font-size:12px;color:#536779}}</style>
<main><header><small>CSK DAILY INTELLIGENCE · PRIVATE · {period}</small><h1>11. MULTI-TOUCH ATTRIBUTION</h1><small>30-day observed session lookback · KRW · Rule-based models</small></header>
<div class="kpi"><div>OBSERVED ORDERS<strong>{m['observed_orders']:,}</strong></div><div>ORDER COVERAGE<strong>{coverage}</strong></div><div>MULTI-SESSION ORDERS<strong>{m['multi_session_orders']:,}</strong></div><div>REVENUE COVERAGE<strong>{revcoverage}</strong></div></div>
<h2>Revenue credit by model</h2><table><thead><tr><th>Channel</th><th>First touch</th><th>Last touch</th><th>Linear</th><th>Position 40/20/40</th><th>Assists (orders)</th></tr></thead><tbody>{''.join(rows)}</tbody></table>
<h2>Top purchase journeys</h2><table><thead><tr><th>Observed path</th><th>Orders</th><th>Revenue</th></tr></thead><tbody>{''.join(paths)}</tbody></table>
<footer>Unattributed: {m['unattributed_orders']:,} orders / KRW {won(m['unattributed_revenue'])}. Same-browser observed sessions only. Assists overlap; rule-based credit is not incremental lift. Paths below 5 orders are omitted. Do not publish or forward.</footer></main></html>"""
    return html

def run(source,out,maximum_gib=15,budget_evidence=None):
    from daily_digest_live_final import classify_looker_channel,BQ_EVENTS_TABLE,BQ_LOCATION
    d=json.loads(Path(source).read_text(encoding="utf-8"))
    meta=d.get("meta") or {}
    start,end=(date.fromisoformat(meta[k]) for k in ("cur_start","cur_end"))
    if start!=end or d.get("end_date")!=end.isoformat(): raise ValueError("Expected complete daily source")
    m=load(BQ_EVENTS_TABLE,BQ_LOCATION,start,end,classify_looker_channel,30,maximum_gib)
    d["multi_touch_attribution"]=m
    section=marketing_attribution(d)
    if section["multi_touch_status"]!="available":
        raise RuntimeError("MTA did not reconcile to source date and headline totals")
    p=Path(out).resolve()
    if p==ROOT or ROOT in p.parents:
        raise ValueError("Private outputs must be outside the repository")
    p.mkdir(parents=True,exist_ok=True,mode=0o700)
    (p/"multi_touch.json").write_text(json.dumps(m,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf-8")
    budget=load_budget(d,evidence_path=budget_evidence)
    sections={"date":end.isoformat(),"sections":{"Marketing Attribution":{"status":"available","data":section},"Budget Optimizer":{"status":budget["status"],"data":budget}}}
    (p/"report_sections.json").write_text(json.dumps(sections,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf-8")
    html=render(m).replace("</main></html>",render_budget(budget)+"</main></html>")
    (p/"multi_touch.html").write_text(html,encoding="utf-8")
    return m

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--date",default=(datetime.now(ZoneInfo("Asia/Seoul")).date()-timedelta(days=1)).isoformat())
    parser.add_argument("--out",required=True)
    parser.add_argument("--maximum-gib",type=int,default=15)
    parser.add_argument("--budget-evidence",default=os.getenv("PRIVATE_BUDGET_EVIDENCE"))
    args=parser.parse_args()
    day=date.fromisoformat(args.date).isoformat()
    source=ROOT/"reports"/"daily_digest"/"data"/"daily"/(day+".json")
    try:
        run(source,args.out,args.maximum_gib,args.budget_evidence)
    except Exception as exc:
        # Public Actions logs must never contain raw customer or financial data.
        print("[MTA] validation/execution failed:",type(exc).__name__,flush=True)
        return 1
    print("[MTA] validated; aggregate private files created",flush=True)
    return 0

if __name__=="__main__": raise SystemExit(main())
