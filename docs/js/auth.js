/**
 * SONO GS 리포트 - 클라이언트 인증 가드
 * =====================================
 *
 * 사용법:
 *   <head>
 *     <meta name="auth-required" content="executive">  <!-- admin / sales / executive / external -->
 *     <script src="./js/auth.js"></script>
 *   </head>
 *
 *   ※ 가능한 한 <head> 최상단에 두어 본문 렌더링 전에 권한 체크가 끝나도록 함.
 *   ※ 로그인 페이지(login.html) 처럼 가드가 필요 없는 페이지는 <meta> 를 빼면 됨.
 *
 * 권한 모델:
 *   admin     : 모든 페이지
 *   sales     : 리포트(executive) + FCST 키인 + 세일즈가이드 등
 *   executive : 리포트 열람만
 *   external  : 팔라티움(외부) 전용
 *
 *   data-auth-required 값에 따라 허용되는 role 셋:
 *     'admin'     → admin
 *     'sales'     → admin, sales
 *     'executive' → admin, sales, executive
 *     'external'  → admin, external      (※ executive/sales 는 외부 페이지를 보지 않음)
 */
(function () {
  'use strict';

  // ──────────────────────────────────────────────────────────────────────────
  // 설정 (배포 후 채워넣기)
  //   - AUTH_API_URL : scripts/apps_script_auth.js 를 배포한 웹앱 URL
  //   - GOOGLE_CLIENT_ID : Google Identity Services 클라이언트 ID
  //
  // localStorage('gsn_auth_api_url') / localStorage('gsn_google_client_id') 로 덮어쓸 수 있음
  // (로그인 페이지 하단의 설정 버튼에서 변경 가능)
  // ──────────────────────────────────────────────────────────────────────────
  var DEFAULT_AUTH_API_URL    = 'https://script.google.com/macros/s/AKfycbz9JwH0BJfcH-AVo9Vy1EYasER5jAz_5ZL9e2v22PtrGZ7Yb5ATbJLuUJ9UvGDjv07MJA/exec';
  var DEFAULT_GOOGLE_CLIENT_ID = '139205036006-gnsq7iveu3tp85tlumkeqnrujvc7094t.apps.googleusercontent.com';

  var LS_TOKEN     = 'gsn_auth_token';
  var LS_USER      = 'gsn_auth_user';
  var LS_API_URL   = 'gsn_auth_api_url';
  var LS_CLIENT_ID = 'gsn_google_client_id';

  function getApiUrl() {
    try { var v = localStorage.getItem(LS_API_URL); if (v) return v; } catch (_) {}
    return DEFAULT_AUTH_API_URL;
  }
  function getGoogleClientId() {
    try { var v = localStorage.getItem(LS_CLIENT_ID); if (v) return v; } catch (_) {}
    return DEFAULT_GOOGLE_CLIENT_ID;
  }

  // ──────────────────────────────────────────────────────────────────────────
  // 토큰 / 사용자
  // ──────────────────────────────────────────────────────────────────────────
  function b64urlDecode(s) {
    try {
      s = s.replace(/-/g, '+').replace(/_/g, '/');
      while (s.length % 4) s += '=';
      return decodeURIComponent(escape(atob(s)));
    } catch (e) { return null; }
  }
  function decodePayload(token) {
    if (!token || typeof token !== 'string') return null;
    var parts = token.split('.');
    if (parts.length !== 2) return null;
    var json = b64urlDecode(parts[0]);
    if (!json) return null;
    try { return JSON.parse(json); } catch (e) { return null; }
  }
  function readToken() {
    try { return localStorage.getItem(LS_TOKEN) || ''; } catch (_) { return ''; }
  }
  function readUser() {
    try {
      var raw = localStorage.getItem(LS_USER);
      return raw ? JSON.parse(raw) : null;
    } catch (_) { return null; }
  }
  function saveSession(token, user) {
    try {
      localStorage.setItem(LS_TOKEN, token);
      localStorage.setItem(LS_USER, JSON.stringify(user));
    } catch (_) {}
  }
  function clearSession() {
    try {
      localStorage.removeItem(LS_TOKEN);
      localStorage.removeItem(LS_USER);
    } catch (_) {}
  }

  function tokenIsValid(token) {
    var p = decodePayload(token);
    return !!(p && p.exp && p.exp > Date.now());
  }

  // ──────────────────────────────────────────────────────────────────────────
  // 권한 검사
  // ──────────────────────────────────────────────────────────────────────────
  var ALLOWED = {
    'admin':     ['admin'],
    'sales':     ['admin', 'sales'],
    'executive': ['admin', 'sales', 'executive'],
    'external':  ['admin', 'external']
  };

  function hasRole(userRole, requirement) {
    var list = ALLOWED[requirement] || [requirement];
    return list.indexOf(userRole) >= 0;
  }

  function readRequirement() {
    var m = document.querySelector('meta[name="auth-required"]');
    if (!m) return null;
    var v = (m.getAttribute('content') || '').trim().toLowerCase();
    return v || null;
  }

  function currentPath() {
    return location.pathname + location.search + location.hash;
  }

  function redirectToLogin(reason) {
    var ret = encodeURIComponent(currentPath());
    var url = './login.html?return=' + ret + (reason ? '&reason=' + encodeURIComponent(reason) : '');
    location.replace(url);
  }

  function redirectToNoAccess(role, requirement) {
    // 권한이 없는 경우 - 로그인 페이지에 메시지로 노출
    clearSession();
    var ret = encodeURIComponent(currentPath());
    location.replace('./login.html?return=' + ret + '&reason=forbidden&need=' + encodeURIComponent(requirement) + '&have=' + encodeURIComponent(role || ''));
  }

  // ──────────────────────────────────────────────────────────────────────────
  // 사용자 배지 / 로그아웃 버튼 UI
  // ──────────────────────────────────────────────────────────────────────────
  function injectBadgeCss() {
    if (document.getElementById('gsn-auth-style')) return;
    var st = document.createElement('style');
    st.id = 'gsn-auth-style';
    st.textContent = [
      '.gsn-auth-fab{position:fixed;top:10px;right:14px;z-index:9999;display:flex;align-items:center;gap:6px;background:rgba(20,23,28,.92);border:1px solid rgba(255,255,255,.18);border-radius:18px;padding:4px 6px 4px 10px;font-family:Pretendard,system-ui,sans-serif;font-size:11px;color:#e0e0e3;backdrop-filter:blur(8px);box-shadow:0 4px 14px rgba(0,0,0,.4)}',
      '.gsn-auth-fab .who{font-weight:700;letter-spacing:.02em;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}',
      '.gsn-auth-fab .role{font-family:JetBrains Mono,Consolas,monospace;font-size:9px;font-weight:800;letter-spacing:.12em;padding:1px 5px;border-radius:3px;background:rgba(201,160,99,.18);color:#e6b970;text-transform:uppercase}',
      '.gsn-auth-fab .role.admin{background:rgba(229,115,115,.18);color:#e57373}',
      '.gsn-auth-fab .role.sales{background:rgba(232,162,86,.18);color:#e8a256}',
      '.gsn-auth-fab .role.executive{background:rgba(90,159,196,.18);color:#5a9fc4}',
      '.gsn-auth-fab .role.external{background:rgba(168,146,200,.18);color:#a892c8}',
      '.gsn-auth-fab .logout{background:transparent;border:1px solid rgba(255,255,255,.18);color:#a8acb3;font-size:10px;font-weight:700;padding:3px 8px;border-radius:12px;cursor:pointer;font-family:inherit}',
      '.gsn-auth-fab .logout:hover{color:#fff;border-color:rgba(255,255,255,.4)}',
      '@media print{.gsn-auth-fab{display:none}}',
      '@media(max-width:480px){.gsn-auth-fab{top:auto;bottom:8px;right:8px;font-size:10px}.gsn-auth-fab .who{max-width:80px}}'
    ].join('\n');
    document.head.appendChild(st);
  }

  function renderBadge(user) {
    if (!user) return;
    injectBadgeCss();
    if (document.getElementById('gsn-auth-fab')) return;
    var fab = document.createElement('div');
    fab.id = 'gsn-auth-fab';
    fab.className = 'gsn-auth-fab';
    var who = document.createElement('span');
    who.className = 'who';
    who.textContent = user.name || user.email || '';
    who.title = user.email || '';
    var role = document.createElement('span');
    role.className = 'role ' + (user.role || '');
    role.textContent = user.role || '';
    var logout = document.createElement('button');
    logout.className = 'logout';
    logout.type = 'button';
    logout.textContent = '로그아웃';
    logout.addEventListener('click', function () { GSNAuth.logout(); });
    fab.appendChild(who);
    fab.appendChild(role);
    fab.appendChild(logout);
    var attach = function () { document.body.appendChild(fab); };
    if (document.body) attach();
    else document.addEventListener('DOMContentLoaded', attach);
  }

  // ──────────────────────────────────────────────────────────────────────────
  // 본문 렌더링 보호 (가드 통과 전까지 화면 숨김)
  // ──────────────────────────────────────────────────────────────────────────
  var hideStyle;
  function hideBody() {
    if (hideStyle) return;
    hideStyle = document.createElement('style');
    hideStyle.id = 'gsn-auth-hide';
    hideStyle.textContent = 'html,body{visibility:hidden!important}';
    (document.head || document.documentElement).appendChild(hideStyle);
  }
  function revealBody() {
    if (hideStyle && hideStyle.parentNode) hideStyle.parentNode.removeChild(hideStyle);
    hideStyle = null;
  }

  // ──────────────────────────────────────────────────────────────────────────
  // 공개 API
  // ──────────────────────────────────────────────────────────────────────────
  var GSNAuth = {
    LS_TOKEN: LS_TOKEN, LS_USER: LS_USER,
    getApiUrl: getApiUrl,
    setApiUrl: function (v) { try { localStorage.setItem(LS_API_URL, v || ''); } catch (_) {} },
    getGoogleClientId: getGoogleClientId,
    setGoogleClientId: function (v) { try { localStorage.setItem(LS_CLIENT_ID, v || ''); } catch (_) {} },
    getToken: readToken,
    getUser: readUser,
    saveSession: saveSession,
    clearSession: clearSession,
    hasRole: hasRole,
    decodePayload: decodePayload,
    tokenIsValid: tokenIsValid,

    logout: function () {
      clearSession();
      location.replace('./login.html');
    },

    // 로그인 API 호출 (login.html 에서 사용)
    login: function (params) {
      var url = getApiUrl();
      if (!url || url.indexOf('http') !== 0) {
        return Promise.reject(new Error('AUTH_API_URL 이 설정되지 않았습니다. js/auth.js 의 DEFAULT_AUTH_API_URL 또는 로그인 페이지 하단 설정에서 지정하세요.'));
      }
      return fetch(url + '?action=login', {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain;charset=utf-8' }, // CORS preflight 회피
        body: JSON.stringify(params)
      }).then(function (r) { return r.json(); }).then(function (j) {
        if (!j || j.status !== 'ok') throw new Error(j && j.message ? j.message : '로그인 실패');
        saveSession(j.token, j.user);
        return j;
      });
    },

    // token 으로 서버 측 검증 (선택적 - 강한 검증 필요 시)
    me: function () {
      var url = getApiUrl();
      var t = readToken();
      if (!url || !t) return Promise.reject(new Error('no-session'));
      return fetch(url + '?action=me&token=' + encodeURIComponent(t)).then(function (r) { return r.json(); });
    },

    // 회원 관리 API (admin-users.html)
    listUsers: function () {
      var t = readToken();
      return fetch(getApiUrl() + '?action=users&token=' + encodeURIComponent(t)).then(function (r) { return r.json(); });
    },
    upsertUser: function (payload) {
      var t = readToken();
      return fetch(getApiUrl() + '?action=users&token=' + encodeURIComponent(t), {
        method: 'POST', headers: { 'Content-Type': 'text/plain;charset=utf-8' },
        body: JSON.stringify(payload)
      }).then(function (r) { return r.json(); });
    },
    deactivateUser: function (email) {
      var t = readToken();
      return fetch(getApiUrl() + '?action=delete&token=' + encodeURIComponent(t), {
        method: 'POST', headers: { 'Content-Type': 'text/plain;charset=utf-8' },
        body: JSON.stringify({ email: email })
      }).then(function (r) { return r.json(); });
    },
    deleteUser: function (email) {
      var t = readToken();
      return fetch(getApiUrl() + '?action=delete&token=' + encodeURIComponent(t), {
        method: 'POST', headers: { 'Content-Type': 'text/plain;charset=utf-8' },
        body: JSON.stringify({ email: email, permanent: true })
      }).then(function (r) { return r.json(); });
    },

    // FCST 키인 저장 (GAS 백엔드)
    saveFcst: function (data) {
      var t = readToken();
      if (!t || !tokenIsValid(t)) return Promise.reject(new Error('인증이 만료되었습니다. 다시 로그인해주세요.'));
      return fetch(getApiUrl() + '?action=save_fcst', {
        method: 'POST', headers: { 'Content-Type': 'text/plain;charset=utf-8' },
        body: JSON.stringify({ token: t, data: data })
      }).then(function (r) { return r.json(); }).then(function (j) {
        if (!j || j.status !== 'ok') throw new Error(j && j.message ? j.message : 'FCST 저장 실패');
        return j;
      });
    },

    // FCST 키인 로드 (GAS 백엔드)
    loadFcst: function () {
      var t = readToken();
      if (!t || !tokenIsValid(t)) return Promise.reject(new Error('인증이 만료되었습니다. 다시 로그인해주세요.'));
      return fetch(getApiUrl() + '?action=load_fcst&token=' + encodeURIComponent(t))
        .then(function (r) { return r.json(); }).then(function (j) {
          if (!j || j.status !== 'ok') throw new Error(j && j.message ? j.message : 'FCST 로드 실패');
          return j;
        });
    },

    // 데일리 리포트 키인(admin_input) 저장 (GAS 백엔드) — saveFcst 와 동일 패턴
    saveDaily: function (data) {
      var t = readToken();
      if (!t || !tokenIsValid(t)) return Promise.reject(new Error('인증이 만료되었습니다. 다시 로그인해주세요.'));
      return fetch(getApiUrl() + '?action=save_admin', {
        method: 'POST', headers: { 'Content-Type': 'text/plain;charset=utf-8' },
        body: JSON.stringify({ token: t, data: data })
      }).then(function (r) { return r.json(); }).then(function (j) {
        if (!j || j.status !== 'ok') throw new Error(j && j.message ? j.message : '데일리 키인 저장 실패');
        return j;
      });
    },

    // 데일리 리포트 키인(admin_input) 로드 (GAS 백엔드)
    loadDaily: function () {
      var t = readToken();
      if (!t || !tokenIsValid(t)) return Promise.reject(new Error('인증이 만료되었습니다. 다시 로그인해주세요.'));
      return fetch(getApiUrl() + '?action=load_admin&token=' + encodeURIComponent(t))
        .then(function (r) { return r.json(); }).then(function (j) {
          if (!j || j.status !== 'ok') throw new Error(j && j.message ? j.message : '데일리 키인 로드 실패');
          return j;
        });
    }
  };
  window.GSNAuth = GSNAuth;

  // ──────────────────────────────────────────────────────────────────────────
  // 즉시 실행 가드
  // ──────────────────────────────────────────────────────────────────────────
  var requirement = readRequirement();
  if (!requirement) {
    // 가드 메타가 없어도 로그인되어 있으면 뱃지만 표시 (경쟁사모니터링, 팔라티움 등)
    var _t = readToken(), _u = readUser();
    if (_t && tokenIsValid(_t) && _u) {
      var _show = function(){ renderBadge(_u); };
      if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', _show);
      else _show();
    }
    return;
  }

  hideBody();

  var token = readToken();
  var user  = readUser();

  if (!token || !tokenIsValid(token) || !user) {
    redirectToLogin('expired');
    return;
  }
  if (!hasRole(user.role, requirement)) {
    redirectToNoAccess(user.role, requirement);
    return;
  }

  // 가드 통과 → 화면 노출 + 배지 표시
  function reveal() {
    revealBody();
    renderBadge(user);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', reveal);
  } else {
    reveal();
  }
})();
