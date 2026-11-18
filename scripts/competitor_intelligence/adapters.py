import re
PROFILES={
"COLUMBIA":{"seeds":["https://www.columbiakorea.co.kr/main/main.asp","https://www.columbiakorea.co.kr/event/261120/teaser.asp","https://www.columbiakorea.co.kr/product/view.asp?pno=19322","https://www.columbiakorea.co.kr/product/view.asp?pno=18774"],"cards":["li","article","[class*=\'product\']"]},
"TNF":{"seeds":["https://image.thenorthfacekorea.co.kr/category/n/Winter-supplies"],"hosts":["image.thenorthfacekorea.co.kr"],"cards":["li","article"]},
"K2":{"seeds":["https://www.k-village.co.kr/K2/","https://www.k-village.co.kr/k2","https://www.k-village.co.kr/goods/KWU26546YS","https://www.k-village.co.kr/","https://www.k-village.co.kr/eventDetail?eventNo=1096"],"hosts":["www.k-village.co.kr","k-village.co.kr"],"cards":["li","article"]},
"DISCOVERY":{"seeds":["https://www.discovery-expedition.com/product-detail/DMDJ7E056-BKS","https://www.discovery-expedition.com/display/promotions/collection/1556","https://www.discovery-expedition.com/display/promotions?type=EVENT&gf=F","https://www.discovery-expedition.com/display/promotions/event/1716","https://www.discovery-expedition.com/display/promotions/collection/705"],"cards":["article","li"]},
"BLACKYAK":{"seeds":["https://www.byn.kr/_exh/?no=2336","https://www.byn.kr/blackyak/shop/big_section.php?cno1=1612","https://www.byn.kr/blackyak/shop/big_section.php?cno1=1012"],"cards":["li","article"]},
"NEPA":{"seeds":[],"cards":["li","article"]},
"KOLON SPORT":{"seeds":["https://www.kolonmall.com/Brands/kolonsport/List/4895","https://www.kolonmall.com/Brands/KOLONSPORT/List/133010020000","https://www.kolonmall.com/Brands/KOLONSPORT/List/4898"],"cards":["article","li"]},
"EIDER":{"seeds":["https://www.k-village.co.kr/eider","https://www.k-village.co.kr/","https://www.k-village.co.kr/eventDetail?eventNo=1096","https://www.eider.co.kr/search?k=%EB%8B%A4%EC%9A%B4"],"hosts":["www.k-village.co.kr","k-village.co.kr"],"cards":["article","li"]},
"NATIONAL GEOGRAPHIC":{"seeds":["https://nstationmall.com/goods/detail/24055","https://nstationmall.com/goods/category/002014004","https://www.nstationmall.com/event/planning/1165","https://www.nstationmall.com/event/planning/1171","https://www.nstationmall.com/natgeo","https://www.nstationmall.com/event/planning"],"hosts":["www.nstationmall.com","nstationmall.com"],"cards":["article","li"]},
"SNOW PEAK":{"seeds":["https://www.snowpeakstore.co.kr/board/list.php?bdId=newevent","https://m.snowpeakstore.co.kr/goods/event_sale.php?sno=697"],"cards":["li",".item_cont"]},
"MILLET":{"seeds":["https://www.millet.co.kr/front/promo/event/348","https://www.millet.co.kr/front/product/25561","https://www.millet.co.kr/front/product/18060"],"cards":["article","li"]}}
def normalize_cards(rows):
    out={}
    def amounts(text):
        return [int(x.replace(",","")) for x in re.findall(r'(?<![\d,])([1-9]\d{0,2}(?:,\d{3})+|[1-9]\d{3,})(?![\d,])\s*(?:원)?',str(text or ""))]
    def single(text):
        vals=set(amounts(text))
        return next(iter(vals)) if len(vals)==1 else None
    for r in rows:
        text=r.get("text","")
        name=r.get("name","").strip()
        url=r.get("url","")
        if len(name)<3 or name not in text or not url.startswith("https://"): continue
        if re.search(r'쿠폰\s*적용가|최대\s*혜택가|카드\s*할인가|적립금|적립\s*예정',text): continue
        rate_text=re.sub(r'(?<!\d)\d{1,2}\s*%\s*(?:페이백|마일리지|포인트|적립)', '', text)
        rates=re.findall(r'(?<!\d)(\d{1,2})\s*%',rate_text)
        regular=single(r.get("regular_text"))
        sale=single(r.get("sale_text"))
        method="dom_price_roles"
        if not (regular and sale):
            regular=single(" ".join(re.findall(r'(?:정상가|소비자가|정가|기존가)\s*[:：]?\s*([\d,]+)\s*원?',text)))
            sale=single(" ".join(re.findall(r'(?:판매가|할인가|세일가|현재가)\s*[:：]?\s*([\d,]+)\s*원?',text)))
            method="explicit_price_labels"
        if not (regular and sale):
            # Without independent semantic price roles require two currency amounts
            # and an advertised percentage that reconciles to the same card.
            prices=re.findall(r'(?<!\d)([1-9][0-9,]{3,})\s*원',text)
            if len(prices)<2:
                # Mixed suffixes (599,000 / 239,000원) and bare KRW on list cards.
                prices=re.findall(r"(?<![\d,])([1-9]\d{0,2}(?:,\d{3})+)(?![\d,])",text)
            if len(prices)!=2 or len(rates)!=1: continue
            regular,sale=sorted([int(x.replace(",","")) for x in prices],reverse=True)
            method="advertised_rate_check"
        if not regular>sale>0: continue
        calculated=(1-sale/regular)*100
        if rates and (len(set(rates))!=1 or abs(calculated-int(rates[0]))>1): continue
        # Reject ambiguous competing amounts even if price roles exist.
        currency=set(int(v.replace(",","")) for v in re.findall(r'(?<!\d)([1-9][0-9,]{3,})\s*원',text))
        if currency-{regular,sale}: continue
        out[(url,name,regular,sale)]={"name":name,"url":url,"image":r.get("image",""),
             "regular_price":regular,"sale_price":sale,"discount_pct":int(rates[0]) if rates else round(calculated,2),
             "evidence":text,"status":"candidate_same_card","price_pair_method":method}
    return list(out.values())[:250]

async def extract_cards(page,profile):
    selector=",".join(profile["cards"]+["[class*='product-item']","[class*='goods-item']","[class*='productItem']","[class*='goods_list_item']","[class*='productCard']","[class*='ProductCard']","[class*='prd_item']",".item_cont",".goods_info"])
    rows=await page.locator(selector).evaluate_all("""els=>{
      const reg='del,s,strike,[class*="original_price"],[class*="originalPrice"],[class*="regularPrice"],[class*="regular_price"],[class*="consumer_price"],[class*="normal_price"]';
      const sale='[class*="sale_price"],[class*="salePrice"],[class*="sellingPrice"],[class*="selling_price"],[class*="discountPrice"],[class*="discount_price"],[class*="finalPrice"]';
      const product=/\\/product\\/|\\/product-detail\\/|\\/goods\\/|goodsNo=|productNo=|pno=|\\/prd\\/|\\/item\\/|\\/shop\\/view/i;
      const nameSel='[class*="goods_name"],[class*="goodsName"],[class*="product_name"],[class*="productName"],[class*="product-name"],[class*="item_name"],[class*="itemName"],[class*="item-name"],[class*="prd_name"],[class*="prdName"],[class*="prd-name"],h3,h4';
      const describe=e=>{
        const text=(e.innerText||'').trim();
        if(text.length<6||text.length>420)return null;
        const links=[...e.querySelectorAll('a[href]')].filter(a=>product.test(a.href));
        const self=e.closest('a[href]');
        if(self&&product.test(self.href))links.unshift(self);
        const urls=[...new Set(links.map(a=>a.href.split('#')[0]))];
        if(urls.length!==1)return null;
        const img=e.querySelector('img[alt]');
        const name=(e.querySelector(nameSel)?.innerText||img?.alt||'').trim();
        if(!name||!text.includes(name))return null;
        const read=sel=>[...new Set([...e.querySelectorAll(sel)].map(x=>(x.innerText||'').trim()).filter(Boolean))].join(' ');
        return {text,url:urls[0],image:img?.currentSrc||'',name,regular_text:read(reg),sale_text:read(sale)};
      };
      const candidates=new Set(els.slice(0,3500));
      // Inspect small ancestors of product anchors, not the entire list/container.
      for(const a of [...document.querySelectorAll('a[href]')].filter(a=>product.test(a.href)).slice(0,600)){
        let p=a;for(let i=0;i<4&&p;i++,p=p.parentElement)candidates.add(p);
      }
      return [...candidates].map(describe).filter(Boolean);
    }""")
    return normalize_cards(rows)
