import asyncio,json
from pathlib import Path
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
class PolicyCache:
    def __init__(self,session,ua,same):
        self.session,self.ua,self.same=session,ua,same
        self.cache={}
        self.lock=asyncio.Lock()
    async def permitted(self,url):
        p=urlparse(url)
        if p.scheme!='https' or not self.same(url): return False
        origin=p.scheme+'://'+p.netloc
        async with self.lock:
            if origin not in self.cache:
                self.cache[origin]=None
                try:
                    async with self.session.get(origin+'/robots.txt',allow_redirects=False) as r:
                        if r.status in (200,404):
                            rp=RobotFileParser()
                            rp.parse((await r.text() if r.status==200 else '').splitlines())
                            self.cache[origin]=rp
                except Exception: pass
        rp=self.cache[origin]
        return bool(rp and rp.can_fetch(self.ua,url))
async def navigate(page,url):
    for attempt in range(2):
        try:
            response=await page.goto(url,wait_until='domcontentloaded',timeout=15000)
            if response and response.status in (500,502,503,504) and not attempt:
                await asyncio.sleep(4);continue
            return response
        except Exception as e:
            if attempt or 'timeout' not in str(e).lower(): raise
            await asyncio.sleep(3)
def checkpoint(name,result):
    root=Path('competitor-output/checkpoints');root.mkdir(parents=True,exist_ok=True)
    p=root/(name+'.json');tmp=p.with_suffix('.tmp')
    tmp.write_text(json.dumps(result,ensure_ascii=False));tmp.replace(p)
