// Standalone offline content candidate adapter. No outbound requests.
function contentFeed(feed, previous={items:[]}) {
 const brands=["KOLON SPORT","TNF","ARC'TERYX","SNOW PEAK","DISCOVERY","PATAGONIA","SALOMON","BLACKYAK","K2","EIDER","NEW BALANCE"];
 const old=new Map((previous.items||[]).map(x=>[x.url,x])), items=[], status=[];
 for(const brand of brands){
 const b=(feed.brands||[]).find(x=>x.brand===brand);let count=0;const seen=new Set();
 for(const p of b?.evidence||[]){
 const title=(p.title||"").trim(), raw=(p.url||"").split("#")[0], [base,query=""]=raw.split("?");
 if(!/^https:\/\/[^/]+\//i.test(base)||!title||!/\/(?:special\/\d+|exhibition\/[^/]+|display\/promotions\/collection\/\d+|_exh\/)/i.test(base))continue;
 if(/_exh\/$/i.test(base)&&!/(^|&)no=\d+/.test(query))continue;
 const qs=query.split("&").filter(x=>x&&!/^(utm_[^=]*|gf|v|fbclid)=/i.test(x)).sort().join("&"),url=base+(qs?"?"+qs:"");
 if(seen.has(url)||/black friday|블랙프라이데이|쿠폰|할인|구매하기 가장 좋은 기회/i.test(title))continue;seen.add(url);
 const prior=old.get(url),fingerprint=p.fingerprint||null;
 const tpo=[["캠핑",/camp|캠핑/i],["도심",/city|도심/i],["트레킹",/ridge|forest|hiking|트레킹|하이킹/i],["여행",/coast|여행/i]].filter(z=>z[1].test(title)).map(z=>z[0]);
 const thumbnail=(p.images||[]).find(x=>/\.(jpg|jpeg|webp|png)([?#]|$)/i.test(x.src||"")&&!/logo|icon|sprite|footer/i.test(x.src||""))?.src||null;
 items.push({brand,title,url,published_at:null,first_seen_at:prior?.first_seen_at||feed.checked_at_kst,thumbnail,thumbnail_status:thumbnail?"candidate":"unknown",type:"editorial_candidate",placement:null,influencer:null,tpo,tpo_status:"title_inferred",selling_points:null,products:null,cta:null,collected_at:feed.checked_at_kst,status:/^BYN 블랙야크 공식몰$/i.test(title)?"title_unverified":"candidate",fingerprint,change:!prior?"newly_observed":!fingerprint||!prior.fingerprint?"unknown":fingerprint!==prior.fingerprint?"changed":"unchanged"});count++;
 }
 status.push({brand,count,status:!b?"not_collected":!b.pages_checked?"failed":count?"candidate":"no_editorial_candidate",errors:b?.errors||[]});
 }
 return {source_run:feed.run_id||null,checked_at:feed.checked_at_kst,items,brands:status,warning:"Public competitor snapshot reuse; editorial candidates only. Publication dates and influencer identities are not inferred."};
}
export { contentFeed };
