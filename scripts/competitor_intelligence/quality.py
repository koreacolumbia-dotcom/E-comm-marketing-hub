import json, os, re, math
from pathlib import Path
p=Path("competitor-output")
f=p/"snapshot.json"
data=json.loads(f.read_text()) if f.exists() else {"brands":[]}
def products(raw):
    if isinstance(raw,list):
        for v in raw: yield from products(v)
    elif isinstance(raw,dict):
        if "Product" in ([raw.get("@type")] if isinstance(raw.get("@type"),str) else (raw.get("@type") or [])):
            yield raw
        for v in raw.values(): yield from products(v)
def paired(page):
    records=[]
    for raw in page.get("structured_data",[]):
        try: parsed=json.loads(raw)
        except (ValueError,TypeError): continue
        for product in products(parsed):
            offers=product.get("offers",[])
            if isinstance(offers,dict): offers=[offers]
            if not isinstance(offers,list): continue
            for offer in offers:
                if not isinstance(offer,dict): continue
                try: price=float(str(offer.get("price","")).replace(",",""))
                except (ValueError,TypeError): continue
                if math.isfinite(price) and price>0 and offer.get("priceCurrency")=="KRW" and isinstance(product.get("name"),str) and product["name"].strip():
                    records.append({"name":product["name"],"price":price,"url":page["url"],"currency":"KRW"})
    return records
rows=[]
for b in data["brands"]:
    pages=b.get("pages",[])
    promo=[x for x in pages if x.get("evidence") and re.search(r"event|promotion|special|exh|기획전|이벤트|페스타|black friday",x.get("url","")+" "+x.get("title",""),re.I)]
    priced=[x for x in pages if x.get("price_mentions")]
    records=[r for x in pages for r in paired(x)]
    dated=[x for x in promo if x.get("date_mentions")]
    cards=[r for x in pages for r in x.get("product_cards",[]) if r.get("status")=="candidate_same_card"]
    unique_cards={}
    for card in cards:
        try:
            regular,sale,rate=map(float,(card["regular_price"],card["sale_price"],card["discount_pct"]))
            if all(math.isfinite(v) for v in (regular,sale,rate)) and regular>sale>0 and abs((1-sale/regular)*100-rate)<=1 and card.get("name") and card.get("url"):
                unique_cards[(card["url"],card["name"],regular,sale)]=card
        except (KeyError,ValueError,TypeError): continue
    cards=list(unique_cards.values())
    strict_candidate=bool(dated and cards)
    usable=bool(promo and (priced or cards))
    rows.append({"brand":b["brand"],"pages":len(pages),"promotion_pages":len(promo),"price_pages":len(priced),"candidate_usable":usable,"strict_candidate":strict_candidate,"paired_discount_cards":cards,"dated_promotion_pages":len(dated),"structured_product_prices":records,"errors":b.get("errors",[])})
owned=next((r for r in rows if r["brand"]=="COLUMBIA"),None)
EXPECTED={"TNF","K2","DISCOVERY","BLACKYAK","NEPA","KOLON SPORT","EIDER","NATIONAL GEOGRAPHIC","SNOW PEAK","MILLET","SALOMON"}
rows=[r for r in rows if r["brand"] in EXPECTED]
TARGET=len(EXPECTED)
complete={r["brand"] for r in rows}==EXPECTED and len(rows)==TARGET
def pct(n): return round(n*100/TARGET,2)
n=sum(r["candidate_usable"] for r in rows)
structured=sum(bool(r["dated_promotion_pages"] and (r["structured_product_prices"] or r["paired_discount_cards"])) for r in rows)
strict=sum(r["strict_candidate"] for r in rows)
pair_brands=sum(bool(r["paired_discount_cards"]) for r in rows)
quality={"discount_pair_brands":pair_brands,"discount_pair_coverage_pct":pct(pair_brands),"discount_pair_target_met":complete and pair_brands==TARGET,"discount_pair_missing_brands":[r["brand"] for r in rows if not r["paired_discount_cards"]],"owned_discount_pair_available":bool(owned and owned.get("paired_discount_cards")),"accessible_brands":sum(r["pages"]>0 for r in rows),"accessible_pct":pct(sum(r["pages"]>0 for r in rows)),"collected_pages":sum(r["pages"] for r in rows),"strict_candidate_brands":strict,"strict_candidate_pct":pct(strict),"target_met_strict_candidate":strict>=TARGET,"measurement_available":f.exists() and complete,"structured_evidence_brands":structured,"structured_evidence_pct":pct(structured),"verified_accuracy_pct":None,"verified_success_brands":None,"verified_success_pct":None,"verified_sample_count":None,"target_met_verified":None,"checked_at_kst":data.get("checked_at_kst"),"target":TARGET,"denominator":TARGET,"candidate_usable":n,"candidate_coverage_pct":pct(n),"target_met_candidate_only":n>=TARGET,"brands":rows,"owned":owned,"note":"Candidate coverage only; human verification of dates and paired prices still required."}
# An incomplete snapshot cannot establish a coverage percentage.
if not quality["measurement_available"]:
    for key in ("discount_pair_brands","discount_pair_coverage_pct","discount_pair_target_met",
                "accessible_brands","accessible_pct","strict_candidate_brands","strict_candidate_pct",
                "structured_evidence_brands","structured_evidence_pct","candidate_usable",
                "candidate_coverage_pct","target_met_strict_candidate","target_met_candidate_only"):
        quality[key]=None
    quality["measurement_error"]="Missing snapshot or incomplete/duplicate brand rows"
p.mkdir(exist_ok=True)
(p/"quality.json").write_text(json.dumps(quality,ensure_ascii=False,indent=2))
summary=["# Competitor collection quality",f"Candidate evidence: {n}/{TARGET} (target {TARGET}/{TARGET})",f"Same-product price pairs: {pair_brands}/{TARGET} (target {TARGET}/{TARGET})","| Brand | Pages | Promo | Price |","|---|---:|---:|---:|"]
summary += [f"| {r['brand']} | {r['pages']} | {r['promotion_pages']} | {r['price_pages']} |" for r in rows]
if os.getenv("GITHUB_STEP_SUMMARY"):
    with open(os.environ["GITHUB_STEP_SUMMARY"],"a") as out: out.write("\n".join(summary)+"\n")
print(json.dumps(quality,ensure_ascii=False))
if not complete or strict<TARGET: raise SystemExit(1)
