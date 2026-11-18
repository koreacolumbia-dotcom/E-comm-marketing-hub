"""Publication dates require explicit, attributable official page evidence."""
import re
from datetime import date,datetime
DATE=re.compile(r"(?<!\d)(20\d{2})\s*(?:년|[./-])\s*(\d{1,2})\s*(?:월|[./-])\s*(\d{1,2})(?:일)?(?!\d)")
SHORT=re.compile(r"(?<!\d)(\d{2})[./](\d{1,2})[./](\d{1,2})(?!\d)")
LABEL=re.compile(r"(?:게시일|등록일|발행일|작성일|공개일|published(?:\s+on)?|posted(?:\s+on)?)\s*[:：]?\s*",re.I)
RANGE=re.compile(r"[~〜–—]|\s+-\s+|부터|까지|기간")
def parse_date(raw,as_of,allow_short=False):
    raw=str(raw or "").strip()
    try:
        d=datetime.fromisoformat(raw.replace("Z","+00:00")).date()
    except ValueError:
        m=DATE.fullmatch(raw) or (SHORT.fullmatch(raw) if allow_short else None)
        if not m:return None
        try:
            y,mn,day=map(int,m.groups())
            d=date(y+2000 if y<100 else y,mn,day)
        except ValueError:return None
    return d.isoformat() if date(2000,1,1)<=d<=as_of else None
def publication(block,checked):
    as_of=datetime.fromisoformat(checked).date()
    candidates=[]
    for x in block.get("date_evidence",[]) or []:
        raw=str(x.get("raw","")).strip()
        method=x.get("method")
        if method not in ("article_meta","article_jsonld","datePublished_itemprop","explicit_publication_label","article_header_time"):continue
        if method in ("explicit_publication_label","article_header_time"):
            if RANGE.search(raw):continue
            if method=="explicit_publication_label":
                label=LABEL.match(raw)
                if not label:continue
                value=raw[label.end():].strip()
            else:value=raw
            m=DATE.fullmatch(value) or SHORT.fullmatch(value)
            value=m[0] if m else value
        else:value=raw
        d=parse_date(value,as_of,method in ("explicit_publication_label","article_header_time"))
        if d:candidates.append({"date":d,"method":method,"evidence":raw[:180]})
    unique=sorted({x["date"] for x in candidates})
    return {"date":unique[0] if len(unique)==1 else None,"status":"explicit" if len(unique)==1 else "conflicting" if unique else "unknown","evidence":candidates[:12]}
def self_test():
    now="2026-11-18T21:00:00+09:00"
    def check(raw,method="explicit_publication_label"):
        return publication({"date_evidence":[{"raw":raw,"method":method}]},now)
    assert check("게시일: 2026.11.03")["date"]=="2026-11-03"
    assert check("발행일 2026년 11월 3일")["date"]=="2026-11-03"
    assert check("등록일 26.09.23")["date"]=="2026-09-23"
    assert check("행사기간 2026.11.03")["date"] is None
    assert check("게시일 2026.11.01 ~ 2026.11.30")["date"] is None
    assert check("2026-11-20T00:00:00Z","article_meta")["date"] is None
    assert check("2026-10-01T00:00:00Z","article_meta")["date"]=="2026-10-01"
    assert publication({"date_evidence":[{"raw":"2026-11-01","method":"article_meta"},{"raw":"2026-11-02","method":"article_jsonld"}]},now)["status"]=="conflicting"
if __name__=="__main__":
    self_test()
    print("publication evidence tests passed")
