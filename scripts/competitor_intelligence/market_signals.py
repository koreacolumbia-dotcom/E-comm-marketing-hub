"""Conservative public storefront evidence extraction; never infer launch/ending dates."""
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

MEMBER = re.compile(r"신규\s*(?:회원|가입|고객)|(?:회원\s*)?가입\s*(?:즉시|시|축하|혜택|쿠폰)|첫\s*(?:구매|주문|결제|로그인)|웰컴|welcome|new\s*member|first\s*(?:purchase|order)|앱\s*(?:설치|다운로드)", re.I)
REWARD = re.compile(r"쿠폰|할인|마일리지|포인트|적립(?:금)?|캐시백|페이백|리워드|무료\s*(?:배송|교환|반품)|사은품|증정|coupon|reward|cashback|points", re.I)
ISSUES = [
    ("운영 이슈", re.compile(r"배송\s*(?:지연|중단)|(?:서비스|사이트|결제|전산|시스템)\s*(?:점검|장애|중단)|리콜|회수\s*안내|멤버십\s*(?:개편|변경|종료)|쿠폰\s*(?:정책|혜택)\s*변경", re.I)),
    ("신상품·협업", re.compile(r"신제품\s*(?:출시|공개)|(?:신상|신제품|컬렉션).{0,20}(?:론칭|런칭|출시|공개)|콜라보(?:레이션)?|협업|collaboration|limited edition|앰버서더|ambassador|팝업\s*(?:스토어|오픈)", re.I)),
    ("캠페인·프로모션", re.compile(r"블랙\s*(?:프라이데이|페스타)|black\s*(?:friday|week|festa)|winter\s*festival|(?:페스티벌|페스타)\s*(?:오픈|시작)|래플\s*(?:오픈|이벤트)|단독\s*(?:출시|론칭)", re.I)),
]
NOTICE_PATH = re.compile(r"notice|announcement|공지", re.I)
PUBLIC_TOPIC = re.compile(r"membership|member/.*join|joininfo|register|benefit|customer/grade|cscenter|cs-center|notice|announcement|new_members|혜택|멤버십|신규회원|회원가입|첫구매|공지사항|협업|콜라보|캠페인", re.I)
PRIVATE = re.compile(r"my[-_]?page|cart|checkout|order(?:/|\.asp|\.php)|admin|account|password|logout|wishlist|(?:/|_)qna|inquiry|review/write|개인정보|비밀번호|본인인증|주문조회", re.I)
MONEY = re.compile(r"(?<!\d)(?:\d{1,3}(?:,\d{3})+|\d{1,7})(?:\.\d+)?\s*(?:%|만원|천원|원|만점|천점|점|M\b|마일리지|포인트)", re.I)

def clean(v):
    return re.sub(r"\s+", " ", str(v or "")).strip()[:300]

def public_topic(url, label=""):
    return not PRIVATE.search(url) and bool(PUBLIC_TOPIC.search(url+" "+label))

def source_url(url):
    try:
        p=urlsplit(str(url or ""))
        if p.scheme!="https" or not p.hostname or p.username or p.password or PRIVATE.search(p.path):
            return None
        return urlunsplit((p.scheme,p.netloc,p.path,p.query,""))
    except ValueError:
        return None

def classify(text, source, context=""):
    t=clean(text)
    if len(t)<8 or PRIVATE.search(t) or re.search(r"[\w.+-]+@[\w.-]+\.[a-zA-Z]{2,}|(?:01[016789])[- ]?\d{3,4}[- ]?\d{4}",t):
        return None
    if MEMBER.search(t) and REWARD.search(t):
        kind="member"
        category="첫 구매" if re.search(r"첫\s*(?:구매|주문|결제)|first\s*(?:purchase|order)",t,re.I) else "앱 혜택" if re.search(r"앱\s*(?:설치|다운로드)|첫\s*로그인",t) else "신규 가입"
        return {"kind":kind,"category":category,"text":t,"numbers":MONEY.findall(t)[:8],"source_url":source}
    for category, pattern in ISSUES:
        if pattern.search(t):
            # Generic product delivery terms are not incidents; require a notice
            # destination or explicitly announced issue wording.
            if category=="운영 이슈" and not (NOTICE_PATH.search(context) or re.search(r"공지|안내|변경|중단|리콜|시행",t)):
                continue
            if re.search(r"경우|될 수 있|문의하시|약관",t) and category=="운영 이슈":
                continue
            return {"kind":"issue","category":category,"text":t,"numbers":[],"source_url":source}
    return None

def extract_market_signals(body, links, page_url):
    source=source_url(page_url)
    if not source:
        return []
    out={}
    def add(raw, url, context):
        r=classify(raw,url,context)
        if not r: return
        token=(r["kind"],r["category"],r["text"])
        if token not in out: out[token]=r
    lines=[clean(x) for x in body.splitlines() if clean(x)]
    for i,line in enumerate(lines):
        add(line,source,page_url)
        # Join a split heading and a nearby amount only when the heading itself
        # names a signup/first-purchase benefit; preserve the original wording.
        if MEMBER.search(line) and (REWARD.search(line) or len(line)<70):
            nearby=lines[i+1:i+3]
            for next_line in nearby:
                if len(next_line)<100 and (REWARD.search(next_line) or MONEY.search(next_line)) and not PRIVATE.search(next_line):
                    add(line+" "+next_line,source,page_url)
                    break
    for l in links:
        u=source_url(l.get("url"))
        if not u or urlsplit(u).hostname!=urlsplit(source).hostname: continue
        add(l.get("label"),u,u+" "+page_url)
    return list(out.values())[:45]

def publish_signals(snapshot, root=Path("reports/competitor_intelligence")):
    root.mkdir(parents=True,exist_ok=True)
    latest=root/"latest-market-signals.json"
    old={}
    if latest.exists():
        try: old=json.loads(latest.read_text(encoding="utf-8"))
        except (OSError,ValueError): pass
    prev={r["id"]:r for r in old.get("signals",[]) if r.get("id")}
    rows={}
    for b in snapshot.get("brands",[]):
        for p in b.get("pages",[]):
            signals=p.get("market_signals")
            if signals is None:
                signals=extract_market_signals("\n".join(p.get("evidence",[])),p.get("banner_labels",[]),p.get("url",""))
            for r in signals:
                if not r.get("source_url"): continue
                key=hashlib.sha256((b["brand"]+"|"+r["kind"]+"|"+r["category"]+"|"+r["text"]).encode()).hexdigest()[:20]
                if key in rows: continue
                rows[key]={**r,"id":key,"brand":b["brand"],"state":"baseline" if not old.get("checked_at_kst") else "previously_seen" if key in prev else "newly_observed","first_seen":prev.get(key,{}).get("first_seen",snapshot["checked_at_kst"]),"observed_at":snapshot["checked_at_kst"]}
    stamp=snapshot["checked_at_kst"]
    payload={"schema_version":1,"checked_at_kst":stamp,"comparison_at":old.get("checked_at_kst"),"brands":[{"brand":b["brand"],"pages":len(b.get("pages",[]))} for b in snapshot.get("brands",[])],"signals":list(rows.values()),"note":"자동 수집 문구는 검증 후보. 금액·조건·게시일을 추정하지 않음. 첫 관측은 실제 시작일 아님. 미관측은 종료 아님."}
    (root/"market_signals").mkdir(exist_ok=True)
    (root/"market_signals"/(stamp[:10]+".json")).write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8")
    latest.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8")
    return payload

if __name__=="__main__":
    snapshot=Path("competitor-output/snapshot.json")
    if snapshot.exists(): publish_signals(json.loads(snapshot.read_text(encoding="utf-8")))
