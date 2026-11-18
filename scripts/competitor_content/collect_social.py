"""Authorized public business-account discovery. No browser login, private endpoints or bypass.
Meta credentials are provided only through Actions secrets. Never persist tokens, signed media URLs or API errors containing URLs.
"""
import json,os,re,time,urllib.parse,urllib.request,urllib.error,hashlib,io
from pathlib import Path
from datetime import datetime,timezone,timedelta
ROOT=Path("reports/competitor_content")
HANDLE=re.compile(r"^[A-Za-z0-9_.]{1,30}$")
def load(path,default):
    try:return json.loads(path.read_text())
    except (OSError,ValueError):return default
def permitted_media(url):
    try:
        p=urllib.parse.urlsplit(url)
        host=(p.hostname or "").lower()
        return p.scheme=="https" and not p.username and not p.password and p.port in (None,443) and any(host==d or host.endswith("."+d) for d in ("cdninstagram.com","fbcdn.net","fbsbx.com"))
    except (TypeError,ValueError):return False
class MediaRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        if not permitted_media(newurl):raise ValueError("unapproved media redirect")
        return super().redirect_request(req,fp,code,msg,headers,newurl)
def visual_metrics(url):
    # API-returned public media only; no video download, no persistent asset or signed URL.
    if not permitted_media(url):return {}
    try:
        from PIL import Image,ImageOps,ImageStat
        req=urllib.request.Request(url,headers={"User-Agent":"CSKMarketResearchBot","Accept":"image/*"})
        with urllib.request.build_opener(MediaRedirect()).open(req,timeout=12) as r:
            if not permitted_media(r.geturl()) or not r.headers.get_content_type().startswith("image/"):return {}
            raw=r.read(6_000_001)
            if len(raw)>6_000_000:return {}
        with Image.open(io.BytesIO(raw)) as original:
            if original.width*original.height>25_000_000:return {}
            im=ImageOps.exif_transpose(original).convert("RGB")
            w,h=im.size;im.thumbnail((128,128))
            palette=im.quantize(colors=5).convert("RGB").getcolors(16384) or []
            hsv=im.convert("HSV");means=ImageStat.Stat(hsv).mean
            return {"visual_palette":["#%02x%02x%02x"%c for _,c in sorted(palette,reverse=True)[:5]],"media_fingerprint":hashlib.sha256(im.tobytes()).hexdigest(),"visual_palette_source":"authorized_meta_image_pixels","media_width":w,"media_height":h,"brightness":round(means[2]/255,3),"saturation":round(means[1]/255,3)}
    except Exception:return {}
def run():
    now=datetime.now(timezone(timedelta(hours=9))).isoformat()
    cfg=load(ROOT/"social-accounts.json",{})
    feed=load(ROOT/"latest.json",{})
    dest=ROOT/"social-feed.json"
    old=load(dest,{});posts={(p["brand"],p["id"]):p for p in old.get("posts",[]) if p.get("brand") and p.get("id")}
    token=os.getenv("IG_GRAPH_ACCESS_TOKEN","").strip(); uid=os.getenv("IG_GRAPH_USER_ID","").strip();version=os.getenv("IG_GRAPH_VERSION","").strip()
    ready=bool(token and uid.isdigit() and re.fullmatch(r"v\d+\.\d+",version))
    accounts=list(cfg.get("accounts",[]))
    for b in feed.get("brands",[]):
        for a in b.get("official_social_links",[]):
            handle=a.get("handle","")
            if not HANDLE.fullmatch(handle):continue
            if any(x.get("brand")==b["brand"] and x.get("handle","").lower()==handle.lower() for x in accounts):continue
            accounts.append({"brand":b["brand"],"handle":handle,"source":a.get("source"),"url":"https://www.instagram.com/"+handle+"/","verification":"site_link_candidate","approved":False})
    # Explicit approved official attribution only; no same-name guessing.
    result=[]
    for a in accounts:
        handle=a.get("handle","");status=dict(a)
        if not HANDLE.fullmatch(handle) or not a.get("approved"):
            status["collection_status"]="account_review_required";result.append(status);continue
        if not ready:
            status["collection_status"]="api_credentials_required";result.append(status);continue
        fields="business_discovery.username("+handle+"){username,media_count,media.limit(50){id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count}}"
        endpoint="https://graph.facebook.com/"+version+"/"+uid+"?"+urllib.parse.urlencode({"fields":fields})
        req=urllib.request.Request(endpoint,headers={"Authorization":"Bearer "+token,"User-Agent":"CSKMarketResearchBot"})
        try:
            with urllib.request.urlopen(req,timeout=35) as res:
                obj=json.loads(res.read(4_000_000))
            bd=obj.get("business_discovery",{})
            if bd.get("username","").lower()!=handle.lower():raise ValueError("account mismatch")
            media=bd.get("media",{}).get("data",[])
            n=0
            for x in media:
                permalink=x.get("permalink","")
                u=urllib.parse.urlparse(permalink)
                if u.scheme!="https" or u.hostname not in ("instagram.com","www.instagram.com") or not x.get("id"):continue
                cap=str(x.get("caption",""))
                post={"id":str(x["id"]),"brand":a["brand"],"channel":"instagram","account":handle,"account_verified":True,"title":" ".join(cap.split())[:95] or x.get("media_type","Instagram"),"caption":" ".join(cap.split())[:240],"url":permalink,"media_type":x.get("media_type"),"published_at":x.get("timestamp"),"like_count":x.get("like_count"),"comments_count":x.get("comments_count"),"observed_at":now,"source":"authorized_meta_business_discovery","caption_truncated":len(cap)>240}
                key=(post["brand"],post["id"])
                existing=posts.get(key,{})
                features={k:existing[k] for k in ("visual_palette","media_fingerprint","visual_palette_source","media_width","media_height","brightness","saturation") if k in existing}
                if not features and n<30:
                    asset=x.get("thumbnail_url") if x.get("media_type")=="VIDEO" else x.get("media_url")
                    if asset:features=visual_metrics(asset)
                post.update(features);posts[key]=post;n+=1
            status.update(collection_status="collected",observed_posts=n,media_count=bd.get("media_count"),last_success_at=now)
        except urllib.error.HTTPError as e:
            status["collection_status"]="api_http_"+str(e.code)
            if e.code==429:
                result.append(status);break
        except (OSError,ValueError,TypeError) as e:status["collection_status"]="api_unavailable_"+type(e).__name__
        result.append(status);time.sleep(2)
    seen={(a["brand"],a["handle"]) for a in result}
    for a in accounts:
        if (a["brand"],a["handle"]) not in seen:result.append(dict(a,collection_status="deferred"))
    cutoff=(datetime.fromisoformat(now)-timedelta(days=180)).isoformat()
    rows=[p for p in posts.values() if (p.get("observed_at") or "")>=cutoff]
    out={"schema_version":1,"checked_at_kst":now,"api_configured":ready,"window":"latest up to 50 per approved professional account per run; retained observations 180 days","accounts":result,"posts":rows}
    dest.write_text(json.dumps(out,ensure_ascii=False,separators=(",",":"))+"\n")
    print(json.dumps({"accounts":len(result),"posts":len(rows),"configured":ready}))
if __name__=="__main__":run()
