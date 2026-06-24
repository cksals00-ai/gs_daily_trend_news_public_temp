/* campaign_match.js — 기획전 유사 과거 실적 매처 (공유)
 * data/campaign_catalog.json (build_campaign_history.py 산출) 을 로드해
 * 임의의 기획전명에 대해 "예전 비슷한 상품/기획전 + 실제 실적"을 찾아준다.
 * 토큰화 규칙은 빌더(tokenize)와 동일하게 유지.
 */
(function (global) {
  'use strict';

  var CAT = null, LOADING = null;

  var BR = /[\[\(#<][^\]\)#>]*[\]\)#>]/g;
  var NOISE = /\bg-?ota\d*\b|\bota\d*\b|\bg\d{1,2}\b|\b\d{2}y\b/gi;
  var SPLIT = /[^가-힣a-z0-9]+/i;
  var STOP = {};
  ('pkg ota gota g15 ro hp cc 패키지 코드 전체 전체번호 상품 통합 정규 객실 룸 박 ' +
   'nights night 2nights 전사 외 등 및 the and for').split(' ').forEach(function (w) { STOP[w] = 1; });

  function tokenize(name) {
    var s = (name || '').toLowerCase().replace(BR, ' ').replace(NOISE, ' ');
    var parts = s.split(SPLIT), out = [];
    for (var i = 0; i < parts.length; i++) {
      var t = parts[i];
      if (t.length >= 2 && !STOP[t] && !/^\d+$/.test(t)) out.push(t);
    }
    return out;
  }

  function load() {
    if (CAT) return Promise.resolve(CAT);
    if (LOADING) return LOADING;
    LOADING = fetch('data/campaign_catalog.json', { cache: 'no-store' })
      .then(function (r) { if (!r.ok) throw new Error('campaign_catalog.json ' + r.status); return r.json(); })
      .then(function (j) { CAT = j; return j; });
    return LOADING;
  }

  function propTokens(p) {
    return tokenize(String(p || '').replace(/소노벨|소노문|소노캄|소노펠리체|소노펫|빌리지|쏠비치/g, ' '));
  }

  /* months: ['05','06',...] from a YYYYMMDD~YYYYMMDD range or array */
  function monthsFromRange(start, end) {
    var s = String(start || ''), e = String(end || '');
    if (s.length < 6) return [];
    var sm = parseInt(s.slice(4, 6), 10), em = e.length >= 6 ? parseInt(e.slice(4, 6), 10) : sm;
    var out = [], m = sm, guard = 0;
    while (guard++ < 13) { out.push(('0' + m).slice(-2)); if (m === em) break; m = m % 12 + 1; }
    return out;
  }

  /* findSimilar(queryText, opts)
   * opts: {property, months:[..]|{start,end}, topN=5, minScore=0.18, type}
   * returns [{name,type,score,rn,rev_m,adr,by_year,segs,agents,months,prop,same}]
   */
  function findSimilar(queryText, opts) {
    opts = opts || {};
    if (!CAT) return [];
    var q = tokenize(queryText);
    if (!q.length) return [];
    var idf = CAT.idf, dflt = CAT.meta.default_idf || 5;
    var qset = {}, qw = 0;
    q.forEach(function (t) { if (!qset[t]) { qset[t] = 1; qw += (idf[t] || dflt); } });
    if (qw <= 0) return [];

    var pTok = opts.property ? propTokens(opts.property) : [];
    var months = Array.isArray(opts.months) ? opts.months
      : (opts.months ? monthsFromRange(opts.months.start, opts.months.end) : []);
    var topN = opts.topN || 5, minScore = opts.minScore == null ? 0.18 : opts.minScore;

    var res = [];
    for (var i = 0; i < CAT.entries.length; i++) {
      var e = CAT.entries[i];
      if (opts.type && e.type !== opts.type) continue;
      var es = e._set || (e._set = e.tokens.reduce(function (a, t) { a[t] = 1; return a; }, {}));
      var shared = 0, nShared = 0;
      for (var t in qset) { if (es[t]) { shared += (idf[t] || dflt); nShared++; } }
      if (nShared === 0) continue;
      var score = shared / qw;                       // 질의 설명력 0..1
      // 사업장 보정
      if (pTok.length) {
        for (var p = 0; p < pTok.length; p++) { if (e.prop && e.prop.indexOf(pTok[p]) >= 0) { score += 0.15; break; } }
      }
      // 시즌(월) 보정
      if (months.length && e.months && e.months.length) {
        for (var mi = 0; mi < months.length; mi++) { if (e.months.indexOf(months[mi]) >= 0) { score += 0.10; break; } }
      }
      if (score < minScore) continue;
      var same = (nShared === q.length && e.tokens.length <= q.length + 1);
      res.push({ name: e.name, type: e.type, score: score, rn: e.rn, rev_m: e.rev_m, adr: e.adr,
        by_year: e.by_year, segs: e.segs, agents: e.agents, months: e.months, prop: e.prop, same: same });
    }
    res.sort(function (a, b) { return b.score - a.score; });
    return res.slice(0, topN);
  }

  /* 벤치마크: 매칭들의 실적 합/평균 + 채널·세그 믹스 */
  function benchmark(matches) {
    if (!matches || !matches.length) return null;
    var sumRn = 0, sumRev = 0, seg = {}, ag = {};
    matches.forEach(function (m) {
      sumRn += m.rn; sumRev += m.rev_m;
      (m.segs || []).forEach(function (s) { seg[s[0]] = (seg[s[0]] || 0) + s[1]; });
      (m.agents || []).forEach(function (a) { ag[a[0]] = (ag[a[0]] || 0) + a[1]; });
    });
    var segArr = Object.keys(seg).map(function (k) { return [k, seg[k]]; }).sort(function (a, b) { return b[1] - a[1]; });
    var agArr = Object.keys(ag).map(function (k) { return [k, ag[k]]; }).sort(function (a, b) { return b[1] - a[1]; });
    return { n: matches.length, sumRn: sumRn, avgRn: Math.round(sumRn / matches.length),
      sumRev: sumRev, avgRev: sumRev / matches.length, segMix: segArr, topAgents: agArr.slice(0, 5) };
  }

  global.CampaignMatch = { load: load, findSimilar: findSimilar, tokenize: tokenize,
    monthsFromRange: monthsFromRange, benchmark: benchmark,
    get catalog() { return CAT; } };
})(window);
