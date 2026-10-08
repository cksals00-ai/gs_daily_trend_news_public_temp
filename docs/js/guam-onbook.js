/* Pure aggregate report. No raw booking records or personal identifiers. */
(() => {
  'use strict';
  let data;
  let currencyMode = 'USD', fxRate = 1480;
  try { if (typeof localStorage !== 'undefined') {const saved=JSON.parse(localStorage.getItem('guam-onbook-currency')||'{}');currencyMode=saved.currency==='KRW'?'KRW':'USD';fxRate=Number.isFinite(Number(saved.rate))&&Number(saved.rate)>0?Number(saved.rate):1480;} } catch (_) {}
  let nationalityMode = "original";
  let selectedMonths = new Set(), selectedVenues = new Set(['mangilao','talofofo']);
  const $ = id => document.getElementById(id);
  const num = (value, decimal = 1) => value == null ? '—' : value.toLocaleString('ko-KR', {minimumFractionDigits: decimal, maximumFractionDigits: decimal});
  const usd = value => value == null ? '—' : (value < 0 ? '-' : '') + (currencyMode==='KRW'?'':'$') + num(Math.abs(value) * (currencyMode==='KRW'?fxRate/1000000:1), 1);
  const pct = value => value == null ? '—' : num(value * 100, 1) + '%';
  const sum = values => values.some(x => x == null) ? null : values.reduce((a, b) => a + b, 0);
  const change = (value, formatter) => value == null ? '—' : (value > 0 ? '+' : '') + formatter(value);
  const fields = ['budget_pax', 'budget_rev', 'prev_pax', 'prev_rev'];

  function combine(items, totals = false) {
    const value = {};
    for (const key of ['pax', 'rev']) value[key] = sum(items.map(x => totals ? x.total[key] : x[key]));
    for (const key of fields) value[key] = sum(items.map(x => x[key]));
    for (const key of ['pax', 'rev']) {
      value['budget_' + key + '_rate'] = value['budget_' + key] ? value[key] / value['budget_' + key] : null;
      value['prev_' + key + '_change'] = value['prev_' + key] ? (value[key] - value['prev_' + key]) / value['prev_' + key] : null;
    }
    value.delta = items.every(x => x.delta) ? {pax: sum(items.map(x => x.delta.pax)), rev: sum(items.map(x => x.delta.rev))} : null;
    return value;
  }

  function addCell(tr, value, className = '') {
    const td = document.createElement('td'); td.textContent = value;
    if (className) td.className = className;
    else if (String(value).startsWith('-') && /[0-9]/.test(value)) td.className = 'down';
    tr.appendChild(td);
    return td;
  }

  function chartRow(label, actual, budget, maximum, extra = '') {
    const row = document.createElement('div'); row.className = 'chart-row';
    const title = document.createElement('h3'); title.textContent = label; row.append(title);
    [actual, budget].forEach((value, index) => {
      if (value == null) return;
      const track = document.createElement('div'); track.className = 'track';
      const bar = document.createElement('div'); bar.className = index ? 'bar budget' : 'bar';
      bar.style.width = (maximum > 0 ? Math.max(0, value) / maximum * 100 : 0) + '%';
      track.append(bar); row.append(track);
    });
    const values = document.createElement('div'); values.className = 'values';
    const text = document.createElement('span'); text.textContent = '매출 ' + usd(actual) + (budget == null ? '' : ' / 목표 ' + usd(budget));
    const share = document.createElement('span'); share.textContent = extra;
    values.append(text, share); row.append(values); return row;
  }

  function renderAnalysis(months, venueRows, total, records) {
    const monthly = months.map(month => {
      const venues = Object.entries(month.venues).filter(([key]) => selectedVenues.has(key)).map(([,v]) => v);
      return {label:month.month, ...combine(venues, true)};
    });
    const max = Math.max(0, ...monthly.flatMap(x => [x.rev, x.budget_rev ?? 0]));
    $('monthly-chart').replaceChildren(...monthly.map(x => chartRow(x.label, x.rev, x.budget_rev, max, '확보율 ' + pct(x.budget_rev_rate))));
    const mix = records.filter(x => ['kr','jp','others','local_total','unmapped'].includes(x.id));
    const largest = Math.max(0, ...mix.map(x => x.rev));
    $('mix-chart').replaceChildren(...mix.filter(x => x.pax || x.is_group).map(x => chartRow(x.label,x.rev,null,largest,'비중 ' + pct(total.rev ? x.rev / total.rev : null))));
    const statements = [];
    if(data.previous_year_source?.months?.length) statements.push('전년비: 현재 예약 / 전년 동월 최종 실적 · 채널 미확정은 비교 제외');
    statements.push('매출 ' + usd(total.rev) + ' · 목표 확보율 ' + pct(total.budget_rev_rate));
    const ranked = mix.filter(x => x.rev > 0).sort((a,b) => b.rev - a.rev);
    if (ranked.length) statements.push('최대 매출 비중: ' + ranked[0].label + ' ' + pct(ranked[0].rev / total.rev));
    const comparable = monthly.filter(x => x.budget_rev_rate != null).sort((a,b) => a.budget_rev_rate - b.budget_rev_rate);
    if (comparable.length > 1) statements.push('확보율 최저: ' + comparable[0].label + ' ' + pct(comparable[0].budget_rev_rate));
    if (selectedVenues.size === 2) {
      const averages = ['mangilao','talofofo'].map(key => {
        const p = sum(months.map(x => x.venues[key].total.pax)), r = sum(months.map(x => x.venues[key].total.rev));
        return [key, p ? r / p : null];
      });
      statements.push('라운드당 매출: Mangilao ' + usd(averages[0][1]) + ' / Talofofo ' + usd(averages[1][1]));
    }
    statements.push(data.comparison ? '예약 순증감: ' + data.comparison.as_of + ' 대비 (' + data.comparison.days + '일)' : '예약 증감: 비교 이력 없음');
    $('analysis').replaceChildren(...statements.map(text => {const li=document.createElement('li'); li.textContent=text; return li;}));
  }

  function renderNationality(venues) {
    const countries = new Map(), resolutions = new Map();
    for (const venue of venues) {
      for (const item of venue.nationalities ?? []) {
        const old = countries.get(item.code) ?? {pax:0,rev:0};
        countries.set(item.code,{pax:old.pax+item.pax,rev:old.rev+item.rev});
      }
      for (const item of venue.uu_resolution ?? []) {
        const key=item.market+'|'+item.basis, old=resolutions.get(key) ?? {pax:0,rev:0};
        resolutions.set(key,{...item,pax:old.pax+item.pax,rev:old.rev+item.rev});
      }
    }
    const total=sum([...countries.values()].map(x=>x.pax));
    const display = new Map([...countries].map(([code,item])=>[code,{...item}]));
    const supplemented = new Set();
    if (nationalityMode === 'adjusted') {
      for (const item of resolutions.values()) {
        if (['UNRESOLVED','REVIEW'].includes(item.market)) continue;
        const uu = display.get('UU');
        if (!uu) continue;
        uu.pax -= item.pax; uu.rev -= item.rev;
        const code = item.market === 'OTHER' ? 'OTHER_ANALYSIS' : item.market;
        const prior = display.get(code) ?? {pax:0,rev:0};
        display.set(code,{pax:prior.pax+item.pax,rev:prior.rev+item.rev}); supplemented.add(code);
      }
    }
    const labels={KR:'KR · 한국',JP:'JP · 일본',GU:'GU · 괌',UU:'UU · 미상',US:'US · 미국',TW:'TW · 대만',CN:'CN · 중국',MISSING:'미입력',LOCAL:'로컬·군인',OTHER_ANALYSIS:'기타 시장'};
    $('nationalities').replaceChildren(...[...display].filter(([,x])=>x.pax||Math.abs(x.rev)>0.005).sort((a,b)=>b[1].pax-a[1].pax).map(([code,item])=>{const tr=document.createElement('tr');[ (labels[code]??code)+(supplemented.has(code)?' · 분석 포함':''),num(item.pax),usd(item.rev),pct(total?item.pax/total:null)].forEach(x=>addCell(tr,x));return tr;}));
    $('nationality-note').textContent=nationalityMode==='original'?'원본 국적 그대로 · UU 미상':'UU 분석용 재분류 · 실제 국적 미확정';
    $('nationality-original').setAttribute('aria-pressed',String(nationalityMode==='original'));
    $('nationality-adjusted').setAttribute('aria-pressed',String(nationalityMode==='adjusted'));
    const markets={KR:'한국 판매시장',JP:'일본 판매시장',LOCAL:'로컬·군인 시장',OTHER:'기타 시장',UNRESOLVED:'분류 미확인',REVIEW:'규칙 충돌 · 검토 필요'};
    $('uu-resolution').replaceChildren(...[...resolutions.values()].sort((a,b)=>b.pax-a.pax).map(item=>{const tr=document.createElement('tr');[markets[item.market],item.basis,num(item.pax),usd(item.rev)].forEach(x=>addCell(tr,x));return tr;}));
    const uu=countries.get('UU')?.pax??0, unresolved=sum([...resolutions.values()].filter(x=>['UNRESOLVED','REVIEW'].includes(x.market)).map(x=>x.pax));
    $('uu-audit').textContent='선택 범위 UU '+num(uu)+'라운드 / 분류 확인 필요 '+num(unresolved)+'라운드. 실제 국적 변경 0건.';
    const legacy=data.original_total;
    if ($('channel-reference')) $('channel-reference').textContent = data.classification_reference ? '원본 채널 유지 · 엑셀 분류 ' + num(data.classification_reference.applied_rows) + '건 연결. 같은 예약·고객·상품·골프장·그룹만 적용. 신규 예약은 추정하지 않음.' : '엑셀 보완 채널 미적용';
    $('original-total').textContent=legacy ? legacy.months.join(' + ')+' / '+num(legacy.pax)+'라운드 / '+usd(legacy.rev)+' · 9월은 원본 고정값 참고' : '원본 범위의 9월 자료가 없어 참고 합계를 계산하지 않습니다.';
    $('rule-list').replaceChildren(...(data.classification_rules??[]).map(rule=>{const li=document.createElement('li');li.textContent=rule.label+' → '+rule.field+' = '+rule.match+' (원본 '+rule.report_row+'행)';return li;}));
  }

  function render() {
    $('fx-field').hidden=currencyMode!=='KRW';
    $('currency-note').textContent=currencyMode==='KRW'?(fxRate?'1 USD = ₩'+num(fxRate)+' · 입력 환율':'환율 입력 필요'):'USD 기준';
    for(const node of document.querySelectorAll?.('.currency-code')??[]) node.textContent=currencyMode==='KRW'?'백만원':'USD';
    renderFilters();
    const months = data.months.filter(x => selectedMonths.has(x.month));
    const venueRows = months.flatMap(x => Object.entries(x.venues).filter(([key]) => selectedVenues.has(key)).map(([, value]) => value));
    const total = combine(venueRows, true);
    const cards = [
      ['18홀 라운딩', num(total.pax), '예약 행 기준 · 9홀 포함'],
      ['매출', usd(total.rev), currencyMode==='KRW'?'백만원 · 환산 매출':'USD · 매출'],
      ['매출 목표 달성률', pct(total.budget_rev_rate), '목표 ' + usd(total.budget_rev)],
      ['이전 기준일 대비 매출', change(total.delta?.rev, usd), data.comparison ? data.comparison.as_of + ' 대비' : '비교 가능한 이전 데이터 없음']
    ];
    $('cards').replaceChildren(...cards.map(([label, value, note]) => {
      const card = document.createElement('div'); card.className = 'card';
      const small = document.createElement('small'); small.textContent = label;
      const strong = document.createElement('strong'); strong.textContent = value;
      if (value.includes('-')) strong.className = 'down';
      else if (value.startsWith('+')) strong.className = 'up';
      const p = document.createElement('p'); p.textContent = note;
      card.append(small, strong, p); return card;
    }));
    const meta = venueRows[0].categories;
    const records = [{id: 'total', label: 'TOTAL', is_group: true, ...total},
      ...meta.map(item => ({...item, ...combine(venueRows.map(v => v.categories.find(x => x.id === item.id)))}))];
    renderAnalysis(months, venueRows, total, records);
    renderNationality(venueRows);
    const children = new Map(meta.map(x => [x.id, x.parent]));
    function metricRow(item, type=null) {
      const tr=document.createElement('tr');
      tr.className=item.id==='total'?'total':item.id==='unmapped'?'unmapped':'';
      if(type!=null) addCell(tr,type);
      addCell(tr,item.label);
      ['pax','rev'].forEach(key => {
        const format=key==='rev'?usd:num;
        [format(item['budget_'+key]),format(item[key]),pct(item['budget_'+key+'_rate']),format(item['prev_'+key])].forEach(x=>addCell(tr,x));
        const previous=item['prev_'+key+'_change'];addCell(tr,change(previous,pct),previous>0?'up':previous<0?'down':'');
        const delta=item.delta?.[key];addCell(tr,change(delta,format),delta>0?'up':delta<0?'down':'');
      });
      return tr;
    }
    const byId=new Map(records.map(x=>[x.id,x]));
    $('rows').replaceChildren(...['total','local_total','outbound','unmapped'].map(id=>byId.get(id)).filter(x=>x&&(x.id!=='unmapped'||x.pax||x.rev)).map(item=>{
      const label={total:'TOTAL',local_total:'로컬',outbound:'해외',unmapped:'미분류'}[item.id];
      const tr=metricRow({...item,label});if(item.id==='local_total')tr.className='market-local';if(item.id==='outbound')tr.className='market-overseas';return tr;
    }));
    const roots=['kr','jp','others','local_total'];
    function kind(item) {
      if(item.id==='total') return '전체';let key=item.id;
      while(!roots.includes(key)&&children.get(key)) key=children.get(key);
      return {kr:'한국',jp:'일본',others:'기타',local_total:'로컬'}[key]??'미분류';
    }
    const channelRecords=records.filter(x=>x.id==='total'||x.id==='unmapped'&&(x.pax||x.rev)||x.id!=='unmapped'&&($('detail').checked?!x.is_group:roots.includes(x.parent)));
    const order=['전체','한국','일본','기타','로컬','미분류'];
    channelRecords.sort((a,b)=>order.indexOf(kind(a))-order.indexOf(kind(b))||(a.report_row??0)-(b.report_row??0));
    let lastKind=null;
    $('channel-rows').replaceChildren(...channelRecords.map(item=>{const type=kind(item),tr=metricRow(item,type);if(lastKind!==type&&lastKind!==null)tr.className+=' channel-start';lastKind=type;return tr;}));
    $('updated').textContent = '예약 기준일 ' + data.as_of;
    const unmapped = sum(venueRows.map(v => v.categories.find(x => x.id === 'unmapped').pax));
    const mismatch=venueRows.some(v=>v.source_total&&(v.total.pax!==v.source_total.pax||Math.abs(v.total.rev-v.source_total.rev)>0.005));
    $('warnings').hidden = !unmapped && !mismatch;
    $('warnings').textContent = (unmapped?'미분류 '+num(unmapped)+'라운드가 있습니다. 전체 합계에 포함했으며 고객·채널 매핑 확인이 필요합니다. ':'')+
      (mismatch?'원본 분류·차감 규칙의 합계와 원천 예약 행 합계가 다릅니다. 원본 조건은 유지했으며 중복·차감 조건 검토가 필요합니다.':'');
    const range = selectedMonths.size === data.months.length ? data.coverage.start + ' ~ ' + data.coverage.end : [...selectedMonths].sort().join(', ');
    $('notes').textContent = '이용일 ' + range + ' · 취소·식당·스파 제외 · — 미입력' +
      (data.comparison ? ' · ' + data.comparison.as_of + ' 대비' : ' · 증감 이력 없음');
  }

  function renderFilters() {
    function buttons(id, choices, selected) {
      const options = [['all','전체'], ...choices];
      $(id).replaceChildren(...options.map(([key,label]) => {
        const button=document.createElement('button'); button.type='button'; button.textContent=label;
        button.setAttribute('aria-pressed',String(key === 'all' ? selected.size === choices.length : selected.has(key)));
        button.addEventListener('click',() => {
          if (key === 'all') choices.forEach(([value]) => selected.add(value));
          else if (selected.has(key)) {if (selected.size > 1) selected.delete(key);}
          else selected.add(key);
          render();
        });
        return button;
      }));
    }
    buttons('months-select',data.months.map(x=>[x.month,x.month]),selectedMonths);
    buttons('venues-select',[['mangilao','Mangilao'],['talofofo','Talofofo']],selectedVenues);
  }

  async function load() {
    try {
      const response = await fetch('./data/guam_onbook.json?t=' + Date.now(), {cache: 'no-store'});
      if (!response.ok) throw new Error('no-data');
      const next = await response.json();
      if (next.schema_version !== 1 || !next.months?.length) throw new Error('invalid-data');
      data = next;
      selectedMonths = new Set([...selectedMonths].filter(key => data.months.some(x => x.month === key)));
      if (!selectedMonths.size) selectedMonths = new Set(data.months.map(x=>x.month));
      $('status').hidden = true; $('report').hidden = false; render();
    } catch (_) {
      $('status').hidden = false;
      $('status').textContent = data ? '새 데이터를 불러오지 못했습니다. 아래에 이전에 불러온 데이터를 표시합니다.' : '아직 예약 데이터가 반영되지 않았습니다. 예약 엑셀을 맥의 입력 폴더에 저장하고 업데이트 프로그램을 실행해 주세요.';
    }
  }
  $('currency-select').value=currencyMode;
  if(fxRate) $('fx-rate').value=fxRate;
  function updateCurrency() {
    currencyMode=$('currency-select').value==='KRW'?'KRW':'USD';
    const value=Number($('fx-rate').value);if(Number.isFinite(value)&&value>0)fxRate=value;
    try {if(typeof localStorage!=='undefined') localStorage.setItem('guam-onbook-currency',JSON.stringify({currency:currencyMode,rate:fxRate}));} catch (_) {}
    if(data)render();
  }
  $('currency-select').addEventListener('change',updateCurrency);
  $('fx-rate').addEventListener('input',updateCurrency);
  $('nationality-original').addEventListener('click',()=>{nationalityMode='original';render();});
  $('nationality-adjusted').addEventListener('click',()=>{nationalityMode='adjusted';render();});
  $('detail').addEventListener('change', render);
  $('refresh').addEventListener('click', load);
  load();
})();
