// Calendar groups exact deduplicated observations. Unknown publishing dates never inferred.
export function dateKey(value){
 const s=String(value||"").slice(0,10);
 if(!/^\d{4}-\d{2}-\d{2}$/.test(s))return null;
 const d=new Date(s+"T00:00:00Z");return Number.isFinite(d.getTime())&&d.toISOString().slice(0,10)===s?s:null
}
export function calendarData(items,mode,month,brand,tpo){
 const key=mode==="published"?"published_at":mode==="changed"?"changed_at":"first_seen_at";
 const rows=(items||[]).filter(x=>(!brand||x.brand===brand)&&(!tpo||(x.tpo||[]).includes(tpo)));
 const valid=x=>!!dateKey(x[key])&&(mode!=="published"||x.publication_status==="explicit"||x.publication_status==="confirmed");
 const mapped=new Map(),unknown=rows.filter(x=>!valid(x)).length;
 for(const x of rows){const d=dateKey(x[key]);if(!valid(x)||!d.startsWith(month+"-"))continue;const a=mapped.get(d)||[];a.push(x);mapped.set(d,a)}
 return {days:Object.fromEntries(mapped),count:[...mapped.values()].reduce((a,b)=>a+b.length,0),unknown,scope:rows.length}
}
export function monthCells(month){
 const [y,m]=month.split("-").map(Number),start=new Date(Date.UTC(y,m-1,1)),n=new Date(Date.UTC(y,m,0)).getUTCDate();
 const cells=Array(start.getUTCDay()===0?6:start.getUTCDay()-1).fill(null);
 for(let i=1;i<=n;i++)cells.push(month+"-"+String(i).padStart(2,"0"));
 while(cells.length%7)cells.push(null);return cells
}
export function moveMonth(month,delta){const [y,m]=month.split("-").map(Number);return new Date(Date.UTC(y,m-1+delta,1)).toISOString().slice(0,7)}
