/*
 * visit-log.js — 페이지 방문 로그 (1개월 사용도 트라이얼)
 * =========================================================
 *  목적 : 방문/클릭 로그가 전무해 "사용도"를 추정할 수밖에 없는 상태를 해소.
 *         한 달간 페이지뷰를 실측해 유지/숨김/삭제 정리의 근거로 삼는다.
 *
 *  동작 : 페이지 로드 시 navigator.sendBeacon 으로 GAS 웹앱에 1회 전송(논블로킹).
 *         전송값 = { page, token, ref } (모두 POST 본문, PII 를 URL 에 싣지 않음).
 *         신원(uid/role)은 서버가 토큰을 검증해 도출한다 → 클라이언트 위조 불가.
 *         이메일 원문은 서버에 저장하지 않고 salt 해시(uid)만 기록. 비로그인=anon.
 *
 *  프라이버시 : 외부(타 오리진) 유입 경로는 기록하지 않는다.
 *  중단 방법 : 이 파일 상단 ENABLED=false, 또는 브라우저에서
 *              localStorage.setItem('gsn_visit_log_off','1').
 *  중복방지 : 같은 페이지를 30분 내 다시 열면 재전송하지 않는다(새로고침/멀티탭 노이즈 제거).
 */
(function () {
  'use strict';

  var ENABLED = true;                         // ← 즉시 중단하려면 false
  try { if (localStorage.getItem('gsn_visit_log_off') === '1') return; } catch (e) {}
  if (!ENABLED) return;

  // auth.js 의 DEFAULT_AUTH_API_URL 과 동일해야 함(비인증 페이지에도 로드되므로 자체 보유).
  var DEFAULT_API = 'https://script.google.com/macros/s/AKfycbz9JwH0BJfcH-AVo9Vy1EYasER5jAz_5ZL9e2v22PtrGZ7Yb5ATbJLuUJ9UvGDjv07MJA/exec';
  var THROTTLE_MS = 30 * 60 * 1000;           // 같은 페이지 30분 내 중복 로깅 방지

  function apiUrl() {
    try { var v = localStorage.getItem('gsn_auth_api_url'); if (v) return v; } catch (e) {}
    if (window.GSNAuth && typeof GSNAuth.getApiUrl === 'function') {
      try { return GSNAuth.getApiUrl(); } catch (e) {}
    }
    return DEFAULT_API;
  }

  function token() {
    try { return localStorage.getItem('gsn_auth_token') || ''; } catch (e) { return ''; }
  }

  function pageId() {
    var p = (location.pathname || '').split('?')[0].split('#')[0];
    var b = p.replace(/^.*\//, '');
    if (!b) b = 'index.html';
    return b;
  }

  // 같은 오리진에서 온 경우만 진입 경로(파일명)를 기록. 외부 유입은 '' (프라이버시).
  function sameOriginRef() {
    try {
      if (!document.referrer) return '';
      var u = new URL(document.referrer);
      if (u.origin !== location.origin) return '';
      var b = u.pathname.replace(/^.*\//, '');
      return b || 'index.html';
    } catch (e) { return ''; }
  }

  function throttled(page) {
    try {
      var k = 'gsn_vl_' + page;
      var last = +(localStorage.getItem(k) || 0);
      var now = Date.now();
      if (last && (now - last) < THROTTLE_MS) return true;
      localStorage.setItem(k, String(now));
      return false;
    } catch (e) { return false; }
  }

  function send() {
    try {
      var page = pageId();
      if (throttled(page)) return;
      var payload = JSON.stringify({ page: page, token: token(), ref: sameOriginRef() });
      var url = apiUrl() + '?action=log_visit';
      var blob;
      try { blob = new Blob([payload], { type: 'text/plain;charset=UTF-8' }); }
      catch (e) { blob = payload; }
      if (navigator.sendBeacon && navigator.sendBeacon(url, blob)) return;
      // 폴백: sendBeacon 미지원/실패
      if (window.fetch) {
        fetch(url, {
          method: 'POST',
          body: payload,
          keepalive: true,
          mode: 'no-cors',
          headers: { 'Content-Type': 'text/plain;charset=UTF-8' }
        }).catch(function () {});
      }
    } catch (e) { /* 페이지 동작에 영향 주지 않음 */ }
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') send();
  else document.addEventListener('DOMContentLoaded', send);
})();
