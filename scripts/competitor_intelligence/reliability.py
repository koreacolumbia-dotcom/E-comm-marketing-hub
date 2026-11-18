import asyncio,json
from contextlib import asynccontextmanager
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
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
    def delay(self,url):
        p=urlparse(url)
        rp=self.cache.get(p.scheme+'://'+p.netloc)
        if not rp: return 3
        delay=rp.crawl_delay(self.ua) or 0
        rate=rp.request_rate(self.ua)
        return max(3,delay,rate.seconds/rate.requests if rate and rate.requests else 0)
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
class OriginGate:
    """One shared, conservative navigation budget per origin across all brands."""
    def __init__(self):
        self.locks={}
        self.next_at={}
        self.stopped={}
    @staticmethod
    def key(url):
        p=urlparse(url)
        return p.scheme+'://'+p.netloc.lower()
    @asynccontextmanager
    async def slot(self,url,delay=3):
        key=self.key(url)
        lock=self.locks.setdefault(key,asyncio.Lock())
        async with lock:
            if key in self.stopped:
                yield False
                return
            await asyncio.sleep(max(0,self.next_at.get(key,0)-asyncio.get_running_loop().time()))
            try:
                yield True
            finally:
                self.next_at[key]=asyncio.get_running_loop().time()+max(3,delay)
    def stop(self,url,reason):
        self.stopped[self.key(url)]=reason

def retry_seconds(value):
    value=(value or "").strip()
    if value.isdigit(): return int(value)
    try:
        date=parsedate_to_datetime(value)
        if date.tzinfo is None: date=date.replace(tzinfo=timezone.utc)
        return max(0,(date-datetime.now(timezone.utc)).total_seconds())
    except (ValueError,TypeError,OverflowError):
        return None

async def navigate(page,url):
    for attempt in range(2):
        try:
            response=await page.goto(url,wait_until='domcontentloaded',timeout=15000)
            if response and response.status == 429 and not attempt:
                # Respect server-specified backoff; do not bypass rate limits.
                retry=retry_seconds(await response.header_value("retry-after"))
                if retry is not None and retry<=30:
                    await asyncio.sleep(max(8,retry))
                    continue
                return response
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
