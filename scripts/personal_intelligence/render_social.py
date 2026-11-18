"""Private HTML section from aggregate social listening data."""
from html import escape
from social_listening import build_social
from pathlib import Path
import argparse,json
def render_social(m):
    def f(v):return "—" if v is None else f"{v:,}"
    def e(v):return escape(str(v),quote=True)
    v=m["voc"]; avg="—" if v["average_rating"] is None else f"{v['average_rating']:.2f}"
    def table(labels,rows):
        return "<table><thead><tr>"+"".join("<th>"+e(x)+"</th>" for x in labels)+"</tr></thead><tbody>"+"".join("<tr>"+"".join("<td>"+e(x)+"</td>" for x in r)+"</tr>" for r in rows)+"</tbody></table>"
    s=table(["Source","Coverage","Matches"],[(x["source"],x["status"],f(x["observed_matches"])) for x in m["community_source_status"]])
    t=table(["Topic","Mentions"],[(x["topic"],x["mentions"]) for x in m["community_topics"]])
    p=table(["Product","Reviews","Low rating","Size mentions"],[(x["product"],x["reviews"],x["low_rating"],x["size_mentions"]) for x in v["product_voc"]])
    links="".join(f'<tr><td>{e(x["source"])}</td><td>{e(x["recorded_date"])}</td><td>{e(", ".join(x["topics"]))}</td><td><a href="{e(x["url"])}" rel="noopener noreferrer">Open source</a></td></tr>' for x in m["community_evidence"])
    kpi="".join(f"<div>{e(k)}<strong>{e(val)}</strong></div>" for k,val in [("OBSERVED MENTIONS",f(m["observed_community_mentions"])),("OFFICIAL REVIEWS",f(v["reviews"])),("AVERAGE RATING",avg),("LOW RATINGS",f(v["low_rating_count"]))])
    return f"""<section><header><small>CSK DAILY INTELLIGENCE · PRIVATE · {e(m['period_start'])} — {e(m['date'])}</small><h1>13. SOCIAL LISTENING</h1></header><div class="kpi">{kpi}</div><h2>Source coverage</h2>{s}<h2>Community topics</h2>{t}<h2>Product VOC (minimum 3 reviews)</h2>{p}<h2>Source evidence</h2><table><thead><tr><th>Source</th><th>Date recorded</th><th>Topics</th><th>Link</th></tr></thead><tbody>{links}</tbody></table><p>Review files: {v['files_present_days']}/7. {e(m['note'])}</p></section>"""
def main():
    p=argparse.ArgumentParser();p.add_argument("--date",required=True);p.add_argument("--out",required=True);a=p.parse_args()
    out=Path(a.out).resolve();root=Path(__file__).resolve().parents[2]
    if out==root or root in out.parents:raise ValueError("Private output outside repository required")
    out.mkdir(parents=True,exist_ok=True,mode=0o700)
    m=build_social(a.date)
    (out/"social.json").write_text(json.dumps(m,ensure_ascii=False,allow_nan=False),encoding="utf-8")
    css="body{font:15px Arial;color:#173650;background:white;margin:30px}main{max-width:1100px;margin:auto}.kpi{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.kpi div{border:1px solid #d5dfe9;padding:14px}.kpi strong{display:block;font-size:26px;margin-top:8px}table{border-collapse:collapse;width:100%;margin-bottom:18px}td,th{padding:10px;border-bottom:1px solid #dfe6ed;text-align:left}thead{color:white;background:#173650}header{border-bottom:3px solid #173650}"
    (out/"social.html").write_text('<!doctype html><meta charset="utf-8"><style>'+css+'</style><main>'+render_social(m)+'</main>',encoding="utf-8")
    print("[SOCIAL] generated; status="+m["status"])
if __name__=="__main__":main()
