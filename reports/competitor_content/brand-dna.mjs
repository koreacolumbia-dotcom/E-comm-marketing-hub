// Brand DNA: interpretable, evidence-based copy similarity and measured palette consistency.
// No aesthetic ranking, inferred personality, or fabricated feed metrics.
export const TONES=[
["TECH","기술·기능",/기술|기능|설계|소재|방수|발수|투습|보온|체열|경량|내구|쿠셔닝|고어텍스|옴니히트|프리마로프트|테크놀로지|performance|technology|engineered|waterproof|insulation|lightweight|gore.tex|technical|durab/i],
["ADVENTURE","탐험·도전",/탐험|도전|한계|정상|모험|트레일|등반|하이킹|러닝|산행|adventure|explor|challenge|summit|trail|climb|running|hiking/i],
["EMOTION","감성·서사",/순간|여정|기억|온기|따스|포근|빛|마음|이야기|감각|설렘|여유|풍경|겨울의|따뜻함|moment|journey|story|warmth|cozy|feeling|dream/i],
["LIFESTYLE","스타일·일상",/일상|데일리|도심|스타일|착장|실루엣|룩북|패션|컬러|코디|라이프스타일|daily|urban|style|silhouette|lookbook|fashion|lifestyle/i],
["HERITAGE","철학·헤리티지",/헤리티지|역사|철학|유산|지속가능|자연을|환경|재활용|수선|heritage|history|since \d{4}|sustainab|recycl|repair/i],
["COMMUNITY","참여·커뮤니티",/커뮤니티|크루|함께|클래스|참가|모집|워크숍|선수|앰배서더|community|crew|join us|workshop|athlete|ambassador/i],
["SALES","판매·혜택",/쿠폰|할인|특가|구매하기|상품 보기|혜택|증정|세일|\d+\s*%\s*(?:off|할인)|shop now|buy now|sale\b|coupon|offer/i]
];
export function copyEvidence(x){
 const title=String(x.title||""),body=String(x.caption||x.summary||"");
 const dirty=/로그인.{0,60}회원가입|매장찾기.{0,80}공지사항|회원가입.{0,60}장바구니|멤버십 혜택\s*>\s*얼리 윈터/i.test(body);
 const eligible=x.channel==="instagram"?body.length>=12:(!dirty&&(x.evidence_scope==="local_block"||x.evidence_scope==="editorial_root"||x.placement==="editorial_page"||body.toLowerCase().startsWith(title.toLowerCase())));
 const text=(x.channel==="instagram"?body:title+" "+(eligible?body:"")).normalize("NFKC").trim();
 if(text.length<12||/^(?:new balance|columbia|the north face|collection|stay connected|image-img|메인 비주얼)$/i.test(text))return null;
 const matches=TONES.map(([key,label,re])=>{const a=text.match(re);return a?{key,label,phrase:a[0]}:null}).filter(Boolean);
 if(!matches.length)return null;
 return {vector:TONES.map(([key])=>matches.some(m=>m.key===key)?1:0),matches,textLength:text.length,scope:x.channel==="instagram"?"caption":eligible?"title_and_local_copy":"title_only"};
}
function cosine(a,b){const dot=a.reduce((s,v,i)=>s+v*b[i],0),norm=Math.sqrt(a.reduce((s,v)=>s+v*v,0)*b.reduce((s,v)=>s+v*v,0));return norm?dot/norm:null}
function mean(vs){if(!vs.length)return null;return vs[0].map((_,i)=>vs.reduce((s,v)=>s+v[i],0)/vs.length)}
function pairs(vs){if(vs.length<5)return null;let sum=0,n=0;for(let i=0;i<vs.length;i++)for(let j=i+1;j<vs.length;j++){const v=cosine(vs[i],vs[j]);if(v!==null){sum+=v;n++}}return n?Math.round(sum/n*100):null}
function paletteVector(x){
 const hex=(x.visual_palette||[]).filter(v=>/^#[a-f0-9]{6}$/i.test(v));if(!hex.length)return null;
 const v=Array(12).fill(0);
 for(const h of hex){const rgb=[1,3,5].map(i=>parseInt(h.slice(i,i+2),16)/255),max=Math.max(...rgb),min=Math.min(...rgb),d=max-min;
 if(d<.12){v[max<.35?0:max>.72?2:1]++;continue}
 let hue=max===rgb[0]?((rgb[1]-rgb[2])/d+6)%6:max===rgb[1]?(rgb[2]-rgb[0])/d+2:(rgb[0]-rgb[1])/d+4;
 v[3+Math.min(8,Math.floor(hue*1.5))]++;}
 return v.map(z=>z/hex.length)
}
function imageKey(x){if(/^[a-f0-9]{64}$/.test(x.media_fingerprint||""))return x.media_fingerprint;try{const u=new URL(x.thumbnail||x.media_url);u.search="";u.hash="";return u.href}catch{return null}}
function group(rows){const valid=rows.map(x=>({item:x,copy:copyEvidence(x)})).filter(v=>v.copy),seen=new Set(),colors=[];for(const x of rows){const k=imageKey(x),v=paletteVector(x);if(k&&v&&!seen.has(k)){seen.add(k);colors.push(v)}}
 const vector=mean(valid.map(v=>v.copy.vector)),palette=mean(colors);
 const toneCounts=TONES.map(([key,label],i)=>({key,label,n:valid.filter(v=>v.copy.vector[i]).length,evidence:valid.filter(v=>v.copy.vector[i]).map(v=>({id:v.item.id,phrase:v.copy.matches.find(m=>m.key===key)?.phrase}))}));
 const score=pairs(valid.map(v=>v.copy.vector));
 const deviations=vector&&valid.length>=5?valid.map(v=>({id:v.item.id,similarity:Math.round((cosine(v.copy.vector,vector)||0)*100),matches:v.copy.matches})).sort((a,b)=>a.similarity-b.similarity).slice(0,5):[];
 return {total:rows.length,copyN:valid.length,colorN:colors.length,copyScore:score,colorScore:pairs(colors),vector,palette,toneCounts,deviations,scopeCounts:Object.fromEntries(["caption","title_and_local_copy","title_only"].map(k=>[k,valid.filter(v=>v.copy.scope===k).length]))};
}
export function analyzeDNA(web,postRows,brands,accounts=[]){
 const map=new Map();for(const x of postRows||[]){if(x.channel!=="instagram"||x.account_verified!==true||!x.id||!x.url)continue;map.set(x.brand+"|"+x.id,x)}
 const social=[...map.values()];
 return brands.map(brand=>{const w=web.filter(x=>x.brand===brand),p=social.filter(x=>x.brand===brand),ws=group(w),ig=group(p);
 const cross=(a,b,n,m)=>n>=5&&m>=5&&a&&b?Math.round((cosine(a,b)||0)*100):null;
 const purposes=[...new Set(w.map(x=>x.purpose?.code||"UNCLASSIFIED"))].map(code=>({code,...group(w.filter(x=>(x.purpose?.code||"UNCLASSIFIED")===code))}));
 return {brand,web:ws,instagram:ig,copyCross:cross(ws.vector,ig.vector,ws.copyN,ig.copyN),colorCross:cross(ws.palette,ig.palette,ws.colorN,ig.colorN),purposes,accounts:accounts.filter(x=>x.brand===brand),posts:p};});
}
