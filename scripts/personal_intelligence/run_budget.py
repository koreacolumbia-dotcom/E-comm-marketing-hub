"""Read cached GA4 aggregates and verified cost evidence; never modify ad accounts."""
import csv,json
from datetime import date,timedelta
from pathlib import Path
from html import escape
from budget_optimizer import optimize

ROOT=Path(__file__).resolve().parents[2]
def load_budget(d, daily_root=None, evidence_path=None):
    daily_root=Path(daily_root or ROOT/"reports/daily_digest/data/daily")
    end=date.fromisoformat(d["end_date"])
    history=[]
    for i in range(1,7):
        try:
            path=daily_root/((end-timedelta(days=i)).isoformat()+".json")
            doc=json.loads(path.read_text(encoding="utf-8"))
            history.append(doc)
        except (OSError,ValueError): pass
    evidence={}
    if evidence_path:
        p=Path(evidence_path).resolve()
        if ROOT==p or ROOT in p.parents:
            raise ValueError("Verified cost evidence must be supplied from a private path")
        evidence=json.loads(p.read_text(encoding="utf-8"))
    if not evidence:
        # Existing legacy cost file is not platform-reconciled. Discover its age only.
        latest=None
        try:
            with (daily_root.parent/"paid_media_spend_history.csv").open(encoding="utf-8-sig",newline="") as f:
                for row in csv.DictReader(f):
                    day=date.fromisoformat(row["date"])
                    if day<=end and (latest is None or day>latest): latest=day
        except (OSError,ValueError,KeyError): pass
        evidence={"status":"stale" if latest and (end-latest).days>2 else "unverified" if latest else "missing",
                  "latest_date":latest.isoformat() if latest else None}
    return optimize(d,history,evidence)

def render_budget(b):
    def f(v,digits=0):return "—" if v is None else f"{v:,.{digits}f}"
    def e(v):return escape(str(v))
    rows=[]
    for r in b["media"]:
        rows.append("<tr><th>"+e(r["media"])+"</th><td>"+f(r["sessions"])+"</td><td>"+f(r["orders"])+"</td><td>"+f(r["revenue"])+"</td><td>"+f(r["cvr_pct"],2)+"%</td><td>"+f(r["spend"])+"</td><td>"+(f(r["roas_pct"],1)+"%" if r["roas_pct"] is not None else "—")+"</td><td>"+e(r["recommendation"])+"</td></tr>")
    status=e(b["cost_status"])
    latest=e(b.get("latest_cost_date") or "not available")
    p=b.get("scenario")
    scenario=(f"<p>Budget-neutral test: move KRW {p['move_krw']:,} from {e(p['from'])} to {e(p['to'])}; review after seven days. No automatic changes.</p>" if p else "<p>No budget reallocation recommended until spend coverage and media mapping are validated.</p>")
    return f"""<section style="margin-top:32px"><header><small>CSK DAILY INTELLIGENCE · PRIVATE</small><h1>12. BUDGET OPTIMIZER</h1></header><p>Cost status: {status} | Last cost date: {latest} | KRW | {'7-day window' if b.get('status')=='available' else 'daily GA4 performance only'}</p><table><thead><tr><th>Channel</th><th>Sessions</th><th>Orders</th><th>Revenue</th><th>CVR</th><th>Spend</th><th>ROAS</th><th>Action</th></tr></thead><tbody>{''.join(rows)}</tbody></table>{scenario}<p>Historical average ROAS is not incremental or marginal return.</p></section>"""
