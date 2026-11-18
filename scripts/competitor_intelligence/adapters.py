import re
PROFILES={
"COLUMBIA":{"seeds":["https://www.columbiakorea.co.kr/main/main.asp","https://www.columbiakorea.co.kr/event/261120/teaser.asp"],"cards":["li","article","[class*=\'product\']"]},
"TNF":{"seeds":["https://image.thenorthfacekorea.co.kr/category/n/Winter-supplies"],"hosts":["image.thenorthfacekorea.co.kr"],"cards":["li","article"]},
"K2":{"seeds":["https://www.k-village.co.kr/k2","https://www.k-village.co.kr/eventDetail?eventNo=1096"],"hosts":["www.k-village.co.kr","k-village.co.kr"],"cards":["li","article"]},
"DISCOVERY":{"seeds":["https://www.discovery-expedition.com/display/promotions?type=EVENT&gf=F","https://www.discovery-expedition.com/display/promotions/event/1716","https://www.discovery-expedition.com/display/promotions/collection/705"],"cards":["article","li"]},
"BLACKYAK":{"seeds":[],"cards":["li","article"]},
"NEPA":{"seeds":[],"cards":["li","article"]},
"KOLON SPORT":{"seeds":[],"cards":["article","li"]},
"EIDER":{"seeds":["https://www.k-village.co.kr/eider","https://www.k-village.co.kr/eventDetail?eventNo=1096","https://www.eider.co.kr/search?k=%EB%8B%A4%EC%9A%B4"],"hosts":["www.k-village.co.kr","k-village.co.kr"],"cards":["article","li"]},
"NATIONAL GEOGRAPHIC":{"seeds":["https://www.nstationmall.com/event/planning/1165","https://www.nstationmall.com/event/planning/1171","https://www.nstationmall.com/event/planning"],"hosts":["www.nstationmall.com","nstationmall.com"],"cards":["article","li"]},
"SNOW PEAK":{"seeds":["https://www.snowpeakstore.co.kr/board/list.php?bdId=newevent","https://m.snowpeakstore.co.kr/goods/event_sale.php?sno=697"],"cards":["li",".item_cont"]},
"MILLET":{"seeds":[],"cards":["article","li"]}}
def normalize_cards(rows):
    out={}
    for r in rows:
        prices=re.findall(r'(?<!\d)([1-9][0-9,]{3,})\s*원',r["text"])
        if len(prices)!=2: continue
        a,b=sorted([int(x.replace(",","")) for x in prices],reverse=True)
        rates=re.findall(r'(?<!\d)(\d{1,2})\s*%',r["text"])
        if not (a>b>0 and len(rates)==1 and abs((1-b/a)*100-int(rates[0]))<=1): continue
        name=r["name"].strip()
        if len(name)<3 or name not in r["text"]: continue
        out[(r["url"],name,a,b)]={"name":name,"url":r["url"],"image":r.get("image",""),"regular_price":a,"sale_price":b,"discount_pct":int(rates[0]),"evidence":r["text"],"status":"candidate_same_card"}
    return list(out.values())[:100]
async def extract_cards(page,profile):
    selector=",".join(profile["cards"]+["[class*='product-item']","[class*='goods-item']","[class*='productItem']","[class*='goods_list_item']"])
    rows=await page.locator(selector).evaluate_all("""els=>els.slice(0,1800).map(e=>({text:(e.innerText||'').trim(),url:e.querySelector('a[href]')?.href||'',image:e.querySelector('img')?.currentSrc||'',name:(e.querySelector('[class*="goods_name"],[class*="product_name"],[class*="item_name"],h3,h4')?.innerText||e.querySelector('img[alt]')?.alt||'').trim()})).filter(x=>x.text.length>5&&x.text.length<220&&x.url&&x.name)""")
    return normalize_cards(rows)
