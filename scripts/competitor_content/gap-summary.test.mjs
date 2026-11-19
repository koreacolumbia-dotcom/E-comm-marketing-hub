import assert from 'node:assert/strict';
import {compareOwnedContent,GAP_TOPICS} from '../../reports/competitor_content/gap-summary.mjs';
const make=(brand,n,signals=()=>[])=>Array.from({length:n},(_,i)=>({
 id:brand+'-'+i,brand,title:brand+' campaign '+i,
 strategy_signals:signals(i),placement:'editorial_page',
 source_url:'https://example.org/stories/'+brand+'/'+i
}));
const own=make('COLUMBIA',40,i=>i<2?['SHOPPABLE']:i<6?['TPO']:i<30?['TECH']:[]);
const peers=['TNF','BLACKYAK','NEPA','SALOMON'].flatMap(brand=>make(brand,20,i=>i<11?['SHOPPABLE']:i<18?['TPO']:['TECH']));
const brandRows=[{brand:'COLUMBIA'},{brand:'TNF'},{brand:'BLACKYAK'},{brand:'NEPA'},{brand:'SALOMON'},{brand:'PATAGONIA'}];
const result=compareOwnedContent({items:[...own,...peers],brands:brandRows,sourceCheckedAt:'2026-11-19T09:00:00+09:00'});
assert.equal(result.status,'ready');
assert.equal(result.ownedTotal,40);
assert.equal(result.peerBrands,4);
assert.equal(result.excluded.length,1);
assert.equal(result.excluded[0],'PATAGONIA');
assert.deepEqual(result.review.map(x=>x.id),['SHOPPABLE','TPO']);
assert.equal(result.review[0].ownedPct,5);
assert.equal(result.review[0].peerPct,55);
assert.equal(result.review[0].delta,50);
assert.equal(result.review[0].references.length,2);
assert.equal(result.strengths[0].id,'TECH');
const thin=compareOwnedContent({items:own,brands:brandRows});
assert.equal(thin.status,'insufficient');
assert.equal(thin.review.length,0);
const oneBrand=compareOwnedContent({items:[...own,...make('TNF',100,()=>['SHOPPABLE'])],brands:brandRows});
assert.equal(oneBrand.status,'insufficient');
assert.equal(oneBrand.review.length,0);
const lowPeer=compareOwnedContent({items:[...own,...['TNF','BLACKYAK','NEPA','SALOMON'].flatMap(b=>make(b,4,()=>['SHOPPABLE']))]});
assert.equal(lowPeer.status,'insufficient');
assert.equal(lowPeer.review.length,0);
assert.equal(GAP_TOPICS.length,6);
console.log('Owned versus competitor gap-summary safeguards passed');
