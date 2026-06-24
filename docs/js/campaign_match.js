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

  // 활동/미정 항목(부킹 실적 개념이 없는 마케팅 액션) — 매칭 대상에서 제외
  var ACTIVITY = ('협의중 미정 tbd lms 발송 구좌 골드박스 메이커스 러쉬 라이브 메인노출 ' +
    '카테고리 광고 메세지 메시지 동시판매 참여 빅세일 선착순 쿠폰 적립 노출 메인배너 단독노출').split(' ');
  function isActivity(text) {
    var t = (text || '').toLowerCase();
    for (var i = 0; i < ACTIVITY.length; i++) { if (t.indexOf(ACTIVITY[i]) >= 0) return true; }
    return false;
  }

  // idf 분포 55퍼센타일 = '구별력 있는 토큰' 임계 (단일 흔한토큰 매칭·제너릭 엔트리 억제용)
  var _distinct = null;
  function distinctThreshold() {
    if (_distinct != null) return _distinct;
    var vals = []; for (var k in CAT.idf) vals.push(CAT.idf[k]);
    vals.sort(function (a, b) { return a - b; });
    _distinct = vals.length ? vals[Math.floor(vals.length * 0.55)] : (CAT.meta.default_idf || 5);
    return _distinct;
  }

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
    var topN = opts.topN || 5, minScore = opts.minScore == null ? 0.30 : opts.minScore;
    var D = distinctThreshold();

    var res = [];
    for (var i = 0; i < CAT.entries.length; i++) {
      var e = CAT.entries[i];
      if (opts.type && e.type !== opts.type) continue;
      // 제너릭 엔트리(엔트리 최고 idf가 낮음 = 흔한 단어뿐) 억제
      if (e._spec == null) { e._spec = 0; for (var z = 0; z < e.tokens.length; z++) { var iv = idf[e.tokens[z]] || dflt; if (iv > e._spec) e._spec = iv; } }
      if (e._spec < D * 0.8) continue;
      var es = e._set || (e._set = e.tokens.reduce(function (a, t) { a[t] = 1; return a; }, {}));
      var shared = 0, nShared = 0, soleTok = null;
      for (var t in qset) { if (es[t]) { shared += (idf[t] || dflt); nShared++; soleTok = t; } }
      if (nShared === 0) continue;
      // 사업장(거래) 일치 — 같은 사업장의 유사 기획전은 단일 이름토큰이어도 유효
      var propMatch = false;
      if (pTok.length && e.prop) { for (var p = 0; p < pTok.length; p++) { if (e.prop.indexOf(pTok[p]) >= 0) { propMatch = true; break; } } }
      // 단일 공통토큰이면: 구별력 있는 토큰이거나 사업장 일치일 때만 인정 (흔한 토큰 1개 매칭 방지)
      if (nShared === 1 && (idf[soleTok] || dflt) < D && !propMatch) continue;
      var score = shared / qw;                       // 질의 설명력 0..1
      if (propMatch) score += 0.20;                  // 같은 사업장 가중
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

  /* suggestKpi(matches) — 유사 매칭의 실적을 유사도 가중평균 → 추천 목표(RN·매출) + 범위 + 신뢰도 */
  function suggestKpi(matches) {
    if (!matches || !matches.length) return null;
    var sw = 0, srn = 0, srev = 0, rns = [], adrs = [];
    matches.forEach(function (m) {
      var w = Math.max(m.score, 0.01);
      sw += w; srn += w * m.rn; srev += w * m.rev_m;
      rns.push(m.rn); if (m.adr) adrs.push(m.adr);
    });
    rns.sort(function (a, b) { return a - b; });
    var rnHat = Math.round(srn / sw), revHat = Math.round((srev / sw) * 10) / 10;
    var top = matches[0].score, n = matches.length;
    var conf = (n >= 3 && top >= 0.45) ? 'high' : (n >= 2 && top >= 0.3) ? 'medium' : 'low';
    return {
      rn: rnHat, rev_m: revHat,
      rnLow: rns[0], rnHigh: rns[rns.length - 1],
      adr: adrs.length ? Math.round(adrs.reduce(function (a, b) { return a + b; }, 0) / adrs.length) : 0,
      n: n, confidence: conf
    };
  }

  global.CampaignMatch = { load: load, findSimilar: findSimilar, tokenize: tokenize,
    monthsFromRange: monthsFromRange, benchmark: benchmark, suggestKpi: suggestKpi,
    isActivity: isActivity,
    get catalog() { return CAT; } };
})(window);
