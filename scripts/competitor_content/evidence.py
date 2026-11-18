"""Conservative editorial evidence extraction; no remote calls or identity inference."""
import re
from urllib.parse import urlsplit
PRODUCT_PATH=re.compile(r"/(?:products/[^/]+/?$|products?/view|products?/detail|product-detail/|goods/(?:detail/)?[A-Z0-9-]+/?$|goods_view|shop/view)|[?&](?:pno|goodsNo|productNo)=",re.I)
GENERIC=re.compile(r"^(?:(?:자세히|더|상품|제품|쇼핑)\s*보기|구매하기|바로가기|shop now|learn more|view|read more|상품명|이미지|slider\s*\d+|banner\s*\d+|더보기|신상품|베스트|전체보기)$",re.I)
NOISE=re.compile(r"^[\d\s,.%원₩-]+$|^(?:이런 기획전도 놓치지 마세요|관련 상품|추천 상품|함께 보면 좋은 상품|전체 상품|상품 목록|공식몰)$",re.I)
PERSON_ROLES=r"앰배서더|애슬릿|모델|배우|크리에이터|인플루언서|러너|클라이머|등반가|산악인|ambassador|athlete|actor|creator|climber|runner"
GENERIC_NAMES={"브랜드","컬렉션","제품","시즌","어반","아웃도어","자연","우리","당신","함께","새로운","올겨울","겨울","다양한","고객","브랜드와","컬럼비아","살로몬","코오롱","공식","대표","캠페인","다운","재킷","재킷과","스타일","다음","이름","소개","선수","모델","여러분","함께한","새롭게","스포츠","트레킹","하이킹","노스페이스","디스커버리","뉴발란스","파타고니아","블랙야크"}
def clean(s): return " ".join(str(s or "").split())
def valid_title(s):
    s=clean(s)
    return bool(len(s)>=4 and not GENERIC.fullmatch(s) and not NOISE.search(s) and not re.match(r"^(?:slider|banner|image|img)[ _-]*\d+$",s,re.I))
def editorial_title(title,description,og_title):
    for s in [title,og_title,description]:
        s=clean(s)
        if valid_title(s) and not re.fullmatch(r"\d{4}",s) and not re.search(r"공식\s*(?:온라인\s*)?몰$|안목을 넓히는 새로운 시선, 코오롱몰$|official online store$",s,re.I):
            return s[:160]
    return ""
def product_evidence(links,allowed):
    out={}
    for a in links or []:
        url=a.get("url","");name=clean(a.get("product_name") or a.get("label"))
        if not allowed(url) or not PRODUCT_PATH.search(url): continue
        name=re.sub(r"\s*\d[\d,]*\s*원.*$","",name).strip()
        name=re.sub(r"\s+\d{1,3}%$","",name).strip()
        if not valid_title(name) or len(name)>110 or re.search(r"로그인|회원가입|구매하기|바로가기|상세보기",name):continue
        out.setdefault(url,{"name":name,"url":url,"evidence":clean(a.get("product_name") or a.get("label"))[:180],"method":"official_product_link_label"})
    return list(out.values())[:20]
def person_evidence(text):
    # Only explicit name-role association. Collaboration mentions remain candidates.
    out={}
    text=clean(text)
    name=r"(?:[가-힣]{2,4}|[A-Z][a-z]+(?:\s+[A-Z][a-z'-]+){1,2})"
    for pattern,method in [(rf"(?:{PERSON_ROLES})\s*[:：]\s*(?P<name>{name})(?![가-힣])","role_label"),(rf"(?:{PERSON_ROLES})\s+(?P<name>{name})(?![가-힣])(?=\s*(?:님|선수|와|과|의|,|[.!?:]|$))","role_name_phrase"),(rf"(?P<name>{name})(?:\s*)(?:와|과)\s*함께한","collaboration_phrase")]:
        for m in re.finditer(pattern,text):
            n=m["name"].strip()
            if n in GENERIC_NAMES or n.endswith(("컬렉션","브랜드","스타일")): continue
            out.setdefault(n,{"name":n,"evidence":m[0],"method":method,"status":"explicit_text_candidate" if method!="collaboration_phrase" else "collaboration_candidate"})
    return list(out.values())[:10]
def image_candidates(block):
    seen=set();out=[]
    for x in block.get("image_details",[]) or []:
        u=x.get("url","")
        if not u.startswith("https://") or re.search(r"logo|sprite|icon|placeholder|blank|loading|footer",u,re.I): continue
        if u in seen or x.get("width",0)<400 or x.get("height",0)<120: continue
        seen.add(u);out.append({"url":u,"alt":clean(x.get("alt"))[:180],"width":x["width"],"height":x["height"],"method":x.get("method","rendered_image")})
    return out[:6]
def self_test():
    assert editorial_title("이런 기획전도 놓치지 마세요","정은채와 함께한 26FW 다운컬렉션","")=="정은채와 함께한 26FW 다운컬렉션"
    assert not valid_title("slider1")
    assert not editorial_title("1969","","")
    assert person_evidence("정은채와 함께한 26FW 다운컬렉션")[0]["name"]=="정은채"
    assert person_evidence("앰배서더: 김연아")[0]["method"]=="role_label"
    assert not person_evidence("브랜드와 함께한 캠페인")
    assert not person_evidence("고어텍스 제품의 보온 기능")
    p=product_evidence([{"url":"https://shop.example/products/alpha","label":"ALPHA JACKET 199,000 원"},{"url":"https://shop.example/products/beta","label":"제품 보기"}],lambda u:True)
    assert len(p)==1 and p[0]["name"]=="ALPHA JACKET"
    assert len(image_candidates({"image_details":[{"url":"https://shop.example/logo.png","width":600,"height":200},{"url":"https://shop.example/campaign.jpg","width":1200,"height":600}]}))==1
if __name__=="__main__":
    self_test()
    print("evidence tests passed")
