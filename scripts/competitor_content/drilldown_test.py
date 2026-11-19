"""Offline browser regression for exact metric drilldown; no external requests."""
import asyncio, json, os
from playwright.async_api import async_playwright
from pathlib import Path
BASE=Path(__file__).resolve().parents[2]/"reports/competitor_content"
ROOT="https://content-test.invalid/"
def item(i,brand,tpo):
    return {"id":str(i),"brand":brand,"placement":"editorial_page","title":"Hiking field story "+str(i),"summary":"하이킹 방수 제품 스토리","source_url":"https://example.org/stories/"+str(i),"url":"https://example.org/stories/"+str(i),"type":"PRODUCT_STORY","tpo":tpo,"selling_points":["방수"],"type_tags":["PRODUCT_STORY"],"last_seen_at":"2026-11-18T21:00:00+09:00","first_seen_at":"2026-11-18T21:00:00+09:00","change":"newly_observed"}
async def run():
    feed={"checked_at_kst":"2026-11-18T21:00:00+09:00","coverage":{"target":2,"accessible":1,"with_candidates":1,"items":2},"items":[item(1,"COLUMBIA",["하이킹"]),item(2,"COLUMBIA",["하이킹","여행"])],"history":[item(1,"COLUMBIA",["하이킹"]),item(2,"COLUMBIA",["하이킹","여행"]),item(3,"TNF",["하이킹"])],"brands":[{"brand":"COLUMBIA","pages_checked":1,"candidate_count":2,"status":"candidate"},{"brand":"TNF","pages_checked":0,"candidate_count":0,"status":"not_collected"}]}
    feed["history"][-1]["last_seen_at"]="2026-11-17T21:00:00+09:00"
    async with async_playwright() as p:
        browser=await p.chromium.launch(headless=True,**({"executable_path":os.environ["CRAWLER_CHROME"]} if os.environ.get("CRAWLER_CHROME") else {}))
        page=await browser.new_page()
        errors=[]
        page.on("pageerror",lambda e:errors.append(str(e)))
        async def route(r):
            u=r.request.url
            if u==ROOT or u==ROOT+"index.html":
                await r.fulfill(content_type="text/html",body=(BASE/"index.html").read_text())
            elif u in (ROOT+"strategy.mjs",ROOT+"gap-summary.mjs",ROOT+"calendar.mjs",ROOT+"advanced.mjs",ROOT+"brand-dna.mjs"):
                await r.fulfill(content_type="text/javascript",body=(BASE/u.rsplit("/",1)[-1]).read_text())
            elif u==ROOT+"latest.json":
                await r.fulfill(content_type="application/json",body=json.dumps(feed))
            else: await r.fulfill(status=404,body="")
        await page.route("**/*",route)
        await page.goto(ROOT)
        await page.locator("#gapRadar").wait_for()
        assert await page.locator("#gapCards .gap-quiet").count()==1
        assert "표본 부족" in (await page.locator("#gapCards").inner_text())
        await page.locator("#tpo .barrow").first.wait_for()
        await page.locator("#dnaSummary .dna-kpi").first.wait_for()
        assert await page.locator("#dnaSummary .dna-kpi").count()==4
        assert await page.locator("#dnaMatrix table").count()==1
        await page.locator("#dnaBrand").select_option("TNF")
        await page.locator("#dnaBrand").select_option("COLUMBIA")
        assert await page.locator("#advancedPanel").count()==1
        assert await page.locator("#purposeGrid .purpose-card").count()==6
        assert await page.locator("#monthlyTpo table").count()==1
        assert await page.locator("#uxDeep .deep-row").count()>=5
        await page.locator("#deepBrand").select_option("TNF")
        await page.locator("#deepBrand").select_option("")
        await page.locator("#calGrid .has-events").first.wait_for()
        await page.locator("#calGrid .has-events").first.click()
        assert await page.locator("#drillCards .tile").count()==3
        await page.locator("#drillClose").click()
        await page.locator("#calMode button[data-mode=published]").click()
        assert await page.locator("#calGrid .has-events").count()==0
        await page.locator("#calMode button[data-mode=observed]").click()
        await page.locator("#calBrand").select_option("TNF")
        await page.locator("#calGrid .has-events").first.click()
        assert await page.locator("#drillCards .tile").count()==1
        await page.locator("#drillClose").click()
        await page.locator("#tpo .barrow").filter(has_text="하이킹").click()
        assert await page.locator("#drillCards .tile").count()==2
        assert "2건" in (await page.locator("#drillTitle").inner_text())
        assert await page.locator("#drillCards .original").count()==2
        await page.locator("#drillClose").click()
        await page.locator("#strategyBody tr").filter(has_text="COLUMBIA").locator("td.heat button").first.click()
        assert await page.locator("#drillCards .tile").count()==2
        await page.locator("#drillClose").click()
        await page.locator("#coverage button").filter(has_text="노스페이스").click()
        assert await page.locator("#drillCards .tile").count()==1
        assert "이력" in (await page.locator("#drillMeta").inner_text())
        await page.locator("#drillClose").click()
        await page.locator("#scope").select_option("history")
        await page.locator("#tpoFilter").select_option("여행")
        assert await page.locator("#cards .tile").count()==1
        assert not errors,errors
        await browser.close()
    print("Content drilldown browser regressions passed")
if __name__=="__main__":asyncio.run(run())
