/*
 * agg-loader.js — db_aggregated.json 압축본(.json.gz) 투명 로더
 *
 * docs/data/db_aggregated.json (82MB) 은 git/Pages 용량 절감을 위해
 * gzip 압축본 db_aggregated.json.gz (약 12MB) 로만 배포한다.
 * 이 스크립트는 window.fetch 를 감싸서 db_aggregated.json 요청을
 * 자동으로 .json.gz 요청 + DecompressionStream('gzip') 해제로 바꿔준다.
 *
 * - 기존 fetch(...).json() / .ok / .text() 호출은 그대로 동작 (표준 Response 반환).
 * - .gz fetch 실패(404) 또는 DecompressionStream 미지원 시 원본 .json 으로 폴백.
 * - ?t= 캐시버스터, {cache:'no-store'} 등 init 옵션은 그대로 전달.
 *
 * <head> 에 다른 스크립트보다 먼저 (블로킹) 로드되어야 한다.
 */
(function () {
  if (window.__aggLoaderInstalled) return;
  if (typeof window.fetch !== 'function') return; // 구형 환경 — 손대지 않음
  window.__aggLoaderInstalled = true;

  var origFetch = window.fetch.bind(window);

  function urlOf(input) {
    if (typeof input === 'string') return input;
    if (input && typeof input.url === 'string') return input.url; // Request 객체
    return '';
  }

  window.fetch = function (input, init) {
    var url = urlOf(input);

    // db_aggregated.json 요청만, 이미 .gz 가 아닌 경우만 가로챈다
    if (url.indexOf('db_aggregated.json') === -1 || url.indexOf('.json.gz') !== -1) {
      return origFetch(input, init);
    }

    var gzUrl = url.replace('db_aggregated.json', 'db_aggregated.json.gz');

    return origFetch(gzUrl, init).then(function (resp) {
      // .gz 없음/오류 → 원본 .json 폴백
      if (!resp || !resp.ok) return origFetch(input, init);
      // 스트리밍 해제 미지원 → 원본 .json 폴백
      if (!resp.body || typeof DecompressionStream === 'undefined') {
        return origFetch(input, init);
      }
      try {
        var stream = resp.body.pipeThrough(new DecompressionStream('gzip'));
        return new Response(stream, {
          status: 200,
          statusText: 'OK',
          headers: { 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return origFetch(input, init);
      }
    }).catch(function () {
      // 네트워크 예외 등 → 원본 .json 폴백
      return origFetch(input, init);
    });
  };
})();
