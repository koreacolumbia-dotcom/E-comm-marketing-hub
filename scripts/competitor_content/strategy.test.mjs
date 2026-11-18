import assert from 'node:assert/strict';
import {analyze} from '../../reports/competitor_content/strategy.mjs';
const x={brand:'TEST',placement:'main_visual',title:'Trail Story',source_url:'https://test.example/',url:'https://test.example/Special/12',thumbnail:'https://test.example/hero.jpg',id:'a'};
const rows=[x,{...x,id:'b',placement:'editorial_page',source_url:x.url},{...x,id:'c',title:'slider1'},{...x,id:'e',title:'3595-img'},{...x,id:'d',brand:'OTHER'},{...x,id:'f',title:'PACKS',url:'https://test.example/products/category/410'}];
const r=analyze({items:rows,brands:[{brand:'TEST',candidate_count:5},{brand:'OTHER',candidate_count:1},{brand:'MISSING',candidate_count:0}]});
assert.equal(r.quality.unique_count,2);
assert.equal(r.quality.merged_count,1);
assert.equal(r.quality.rejected_count,3);
assert.equal(r.brands[2].signals.TPO,null);
assert.equal(r.items.find(a=>a.brand==='TEST').member_ids.length,2);
assert.equal(r.items.find(a=>a.brand==='TEST').placements.length,2);
assert.equal(r.insights.every(a=>a.owned_total===0&&a.gap_pp===null),true);
console.log('Content strategy regression tests passed');

// Purpose regressions: type is not primary intent, URL alone is insufficient.
import {classifyPurpose} from '../../reports/competitor_content/strategy.mjs';
assert.equal(classifyPurpose({title:"겨울 컬렉션",placement:"main_visual",url:"https://example.org/event/planning/123"}).code,"UNCLASSIFIED");
assert.equal(classifyPurpose({title:"상품 기획전",placement:"main_visual"}).code,"COMMERCIAL");
assert.equal(classifyPurpose({title:"앰배서더 인터뷰",placement:"editorial_page"}).code,"EDITORIAL");
assert.equal(classifyPurpose({title:"앰배서더 인터뷰",placement:"editorial_page",products:[{name:"재킷",url:"https://example.org/product/12345"}]}).code,"HYBRID");
assert.equal(classifyPurpose({title:"구매 혜택 쿠폰"}).code,"PROMOTION");
assert.equal(classifyPurpose({title:"하이킹 클래스 참가 신청"}).code,"COMMUNITY");
assert.equal(classifyPurpose({title:"아우터",placement:"main_visual",summary:"다른 배너 30% 할인"}).code,"UNCLASSIFIED");

import {visualPairs} from '../../reports/competitor_content/strategy.mjs';
const sig=(hash,sha='a')=>({dhash:hash,aspect:1.5,pixel_sha256:sha});
const v=[{brand:'TEST',id:'a',visual_signature:sig('1234567890abcdef')},
 {brand:'TEST',id:'b',visual_signature:sig('1234567890abcdef','b')},
 {brand:'TEST',id:'c',visual_signature:sig('1234567890abcdef')},
 {brand:'OTHER',id:'d',visual_signature:sig('1234567890abcdef')}];
const pairs=visualPairs(v);
assert.equal(pairs.length,3);
assert.equal(pairs.filter(x=>x.kind==='same_rendered_image').length,1);
assert.equal(visualPairs(v,1).length,1);
const audited=analyze({items:rows,history:rows,brands:[{brand:'TEST',url_coverage:{known_urls:12,observed_urls:8,observed_pct:66.7}}]});
assert.equal(audited.brands[0].url_coverage.known_urls,12);
assert.equal(audited.quality.retained_unique_count,audited.quality.unique_count);
assert.equal(audited.quality.visual_review_pairs,0);
console.log('Visual and coverage audit regression tests passed');
