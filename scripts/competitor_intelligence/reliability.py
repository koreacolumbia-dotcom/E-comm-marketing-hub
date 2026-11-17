import asyncio,json
from pathlib import Path
from urllib.parse import urlparse,urljoin
from urllib.robotparser import RobotFileParser
class PolicyCache:
    def __init__(self,session,ua,same):
        self.session,self.ua,self.same=session,ua,same
        self.cache={}
        self.reasons={}
        self.lock=asyncio.Lock()
    def reason(self,url):
        p=urlparse(url)
        origin=p.scheme+'://'+p.netloc
        if self.cache.get(origin): return 'robots disallowed path'
        return self.reasons.get(origin,'robots unavailable or out of scope')
    async def permitted(self,url):
        p=urlparse(url)
        if p.scheme!='https' or not self.same(url): return False
        origin=p.scheme+'://'+p.netloc
        async with self.lock:
            if origin not in self.cache:
                self.cache[origin]=None
                try:
                    target=origin+'/robots.txt'
                    for _ in range(4):
                        async with self.session.get(target,allow_redirects=False) as r:
                            self.reasons[origin]='robots HTTP '+str(r.status)
                            if r.status in (301,302,303,307,308):
                                nxt=urljoin(target,r.headers.get('Location',''))
                                if nxt==target or urlparse(nxt).scheme!='https' or not self.same(nxt): break
                                target=nxt
                                continue
                            if r.status in (200,404):
                                rp=RobotFileParser()
                                body=await r.text() if r.status==200 else ''
                                if '<html' in body[:500].lower():
                                    self.reasons[origin]='robots returned HTML'
                                    break
                                rp.parse(body.splitlines())
                                self.cache[origin]=rp
                            break
                except Exception as exc:
                    self.reasons[origin]='robots '+type(exc).__name__
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
