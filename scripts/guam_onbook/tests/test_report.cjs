// Exercise the real report renderer with a minimal in-memory document.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const path = require('node:path');
class Element {
  constructor() {this.children=[]; this.value=''; this.style={}; this.hidden=false; this.checked=false; this.textContent=''; this.events={}; this.attributes={};}
  append(...nodes) {this.children.push(...nodes);}
  appendChild(node) {this.children.push(node);}
  replaceChildren(...nodes) {this.children=nodes; if(nodes[0]?.value) this.value=nodes[0].value;}
  get options() {return this.children;}
  addEventListener(name, callback) {this.events[name]=callback;}
  setAttribute(name,value) {this.attributes[name]=value;}
}
const ids = Object.fromEntries(['months-select','venues-select','detail','refresh','rows','cards','updated','warnings','notes','report','status','monthly-chart','mix-chart','analysis','nationalities','nationality-original','nationality-adjusted','nationality-note','uu-resolution','uu-audit','original-total','rule-list'].map(id=>[id,new Element()]));
function venue(pax,rev,budget) {
  return {total:{pax,rev},budget_pax:10,budget_rev:budget,prev_pax:10,prev_rev:200,delta:null,
    categories:[{id:'kr',label:'KR',is_group:true,parent:null,pax,rev,budget_pax:10,budget_rev:budget,prev_pax:10,prev_rev:200,delta:null},
      {id:'kr_individual',label:'한국 개인',is_group:false,parent:'kr',pax,rev,budget_pax:10,budget_rev:budget,prev_pax:10,prev_rev:200,delta:null},
      {id:'unmapped',label:'미분류',is_group:false,parent:null,pax:0,rev:0,budget_pax:null,budget_rev:null,prev_pax:null,prev_rev:null,delta:null}]};
}
const fixture={schema_version:1,as_of:'2026-10-07',coverage:{start:'2026-10-01',end:'2026-11-30'},comparison:null,
  months:[{month:'2026-10',venues:{mangilao:venue(2,300,600),talofofo:venue(1,150,300)}},
    {month:'2026-11',venues:{mangilao:venue(4,500,1000),talofofo:venue(3,450,null)}}]};
fixture.months[0].venues.mangilao.nationalities=[{code:'UU',pax:1,rev:150},{code:'KR',pax:1,rev:150}];
fixture.months[0].venues.mangilao.uu_resolution=[{market:'JP',basis:'clientName = GORA',pax:1,rev:150}];
const context={document:{getElementById:id=>ids[id],createElement:()=>new Element()},
  fetch:async()=>({ok:true,json:async()=>fixture}),Date};
vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../../..','docs/js/guam-onbook.js'),'utf8'),context);
setImmediate(()=> {
  assert.equal(ids.nationalities.children[0].children[0].textContent,'UU · 미상');
  ids['nationality-adjusted'].events.click();
  assert.ok(ids.nationalities.children.some(x=>x.children[0].textContent.includes('JP')));
  assert.ok(!ids.nationalities.children.some(x=>x.children[0].textContent.startsWith('UU')));
  assert.equal(ids.nationalities.children.reduce((a,x)=>a+Number(x.children[1].textContent),0),2);
  assert.equal(fixture.months[0].venues.mangilao.nationalities[0].code,'UU');
  ids['nationality-original'].events.click();
  assert.equal(ids.nationalities.children[0].children[0].textContent,'UU · 미상');
  assert.equal(ids.rows.children[0].children[2].textContent,'10.0');
  assert.equal(ids.rows.children[0].children[8].textContent,'$1,400.0');
  assert.equal(ids.rows.children[0].children[9].textContent,'—'); // one missing target must not become zero
  assert.ok(ids.analysis.children.length >= 2);
  assert.equal(ids['monthly-chart'].children[0].children[0].textContent,'2026-10');
  ids['months-select'].children[2].events.click(); // remove November from all
  assert.equal(ids.rows.children[0].children[2].textContent,'3.0');
  assert.equal(ids.rows.children[0].children[8].textContent,'$450.0');
  assert.equal(ids.rows.children[0].children[9].textContent,'50.0%');
  ids['venues-select'].children[2].events.click(); // remove Talofofo
  assert.equal(ids.rows.children[0].children[2].textContent,'2.0');
  assert.equal(ids.rows.children[0].children[8].textContent,'$300.0');
  assert.equal(ids.rows.children[0].children[11].className,'up'); // +50% vs prior
  assert.equal(ids.rows.children.length,2);
  ids.detail.checked=true; ids.detail.events.change();
  assert.equal(ids.rows.children.length,4);
  assert.equal(ids.rows.children[2].children[0].textContent,'한국 개인');
  ids['months-select'].children[0].events.click();
  ids['venues-select'].children[0].events.click();
  assert.equal(ids.rows.children[0].children[2].textContent,'10.0');
  assert.equal(ids['months-select'].children[0].attributes['aria-pressed'],'true');
  fixture.months[0].venues.mangilao.prev_rev = 400;
  ids['months-select'].children[2].events.click();
  ids['venues-select'].children[2].events.click();
  assert.equal(ids.rows.children[0].children[11].textContent,'-25.0%');
  assert.equal(ids.rows.children[0].children[11].className,'down');
  console.log('Report rendering: period, venue, detail and missing-target checks passed.');
});
