import re
PROFILES={
"TNF":{"seeds":["https://image.thenorthfacekorea.co.kr/category/n/Winter-supplies"],"hosts":["image.thenorthfacekorea.co.kr"],"cards":["li","article"]},
"K2":{"seeds":[],"cards":["li","article"]},
"DISCOVERY":{"seeds":["https://www.discovery-expedition.com/display/promotions/collection/705"],"cards":["article","li"]},
"BLACKYAK":{"seeds":[],"cards":["li","article"]},
"NEPA":{"seeds":[],"cards":["li","article"]},
"KOLON SPORT":{"seeds":[],"cards":["article","li"]},
"EIDER":{"seeds":["https://www.eider.co.kr/search?k=%EB%8B%A4%EC%9A%B4"],"cards":["article","li"]},
"NATIONAL GEOGRAPHIC":{"seeds":[],"cards":["article","li"]},
"SNOW PEAK":{"seeds":["https://www.snowpeakstore.co.kr/board/list.php?bdId=newevent","https://m.snowpeakstore.co.kr/goods/event_sale.php?sno=697"],"cards":["li",".item_cont"]},
"MILLET":{"seeds":[],"cards":["article","li"]}}
async def extract_cards(page,profile):
    selector=",".join(profile["cards"])
    rows=await page.locator(selector).evaluate_all("""els=>els.slice(0,1800).map(e=>({text:(e.innerText||'').trim(),url:e.querySelector('a[href]')?.href||'',image:e.querySelector('img')?.currentSrc||'',name:e.querySelector('img[alt]')?.alt||''})).filter(x=>x.text.length>5&&x.text.length<220&&x.url&&x.name)""")
    out={}
    for r in rows:
        prices=re.findall(r'([1-9][0-9,]{3,})\s*원',r["text"])
        if len(prices)!=2: continue
        a,b=[int(x.replace(",","")) for x in prices]
        rates=re.findall(r'(\d{1,2})\s*%',r["text"])
        if not (a>b>0 and len(rates)==1 and abs((1-b/a)*100-int(rates[0]))<=1): continue
        if r["name"] not in r["text"]: continue
        out[(r["url"],r["name"],a,b)]={"name":r["name"],"url":r["url"],"image":r["image"],"regular_price":a,"sale_price":b,"discount_pct":int(rates[0]),"evidence":r["text"],"status":"candidate_same_card"}
    return list(out.values())[:100]
