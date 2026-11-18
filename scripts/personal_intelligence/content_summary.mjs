// Offline content reporting; no outbound requests or mail.
function summarizeContent(feed, asOf) {
 const end=Date.parse(asOf);if(!Number.isFinite(end))throw new Error("invalid asOf");
 const start=end-30*86400000, brands=new Map((feed.brands||[]).map(b=>[b.brand,{brand:b.brand,collection_status:b.status,published_30d:0,discovered_30d:0,unknown_publication:0}]));
 const mix={}, calendar=[], ss=[], seen=new Set();
 for(const item of feed.items||[]){
  if(!item.url||seen.has(item.url)||!brands.has(item.brand))continue;seen.add(item.url);
  const row=brands.get(item.brand),pub=item.published_at?Date.parse(item.published_at):NaN,first=Date.parse(item.first_seen_at);
  if(!Number.isFinite(pub))row.unknown_publication++;
  if(first>start&&first<=end)row.discovered_30d++;
  if(pub>start&&pub<=end){row.published_30d++;const type=item.type||"unknown";mix[type]=(mix[type]||0)+1;calendar.push({date:item.published_at,brand:item.brand,title:item.title,url:item.url,date_type:"publication"});}
  if(item.status!=="title_unverified"&&((item.tpo||[]).length||item.influencer))ss.push({brand:item.brand,title:item.title,url:item.url,thumbnail:item.thumbnail||null,tpo:item.tpo||[],influencer:item.influencer||null,evidence_status:item.status,idea_status:"reference_candidate"});
 }
 return {as_of:asOf,window_days:30,brands:[...brands.values()].map(r=>({...r,published_30d:r.published_30d||null})),publication_mix:mix,calendar:calendar.sort((a,b)=>a.date.localeCompare(b.date)),ss_references:ss,warning:"Known publication dates only; null is unmeasured, not zero. Discoveries are not publications. Candidate TPO is not verified."};
}
export { summarizeContent };
