/*
 * menu-visibility.js — 대시보드 네비게이션 "숨김 메뉴" 처리 (단일 소스)
 *
 *  - GSN_HIDDEN_MENUS 가 "숨김 메뉴 정의(목록 + hidden 플래그)"의 단일 소스다.
 *    여기서 항목을 빼면 해당 메뉴는 다시 메인 네비에 노출된다(영구 노출).
 *  - 페이지 자체(파일)는 삭제하지 않으며, 직접 URL 접근은 계속 가능하다.
 *    (네비 링크/카드만 화면에서 감춘다.)
 *  - 관리자는 hidden-menus.html(관리자 전용)에서 항목별로 임시 "다시 노출"
 *    토글을 걸 수 있다 → localStorage('gsn_menu_overrides') 에 { href: 'show' } 저장.
 *  - 숨김 관리 페이지의 "열기" 링크 등 감추면 안 되는 링크는
 *    class="hidden-menu-link" 또는 조상에 data-keep-hidden-menu-visible 를 두면 제외된다.
 */
(function () {
  // ── 숨김 메뉴 정의(단일 소스) ──────────────────────────────────────────
  window.GSN_HIDDEN_MENUS = [
    {
      key: 'gs-proposal',
      href: 'gs-proposal.html',
      label: '품의 기획',
      reason: '미사용 — 품의서 작성 프로토타입(요금표 업로드·시각화). 허브 카드/링크로만 노출돼 있었음.'
    },
    {
      key: 'manager-analysis',
      href: 'manager-analysis.html',
      label: '담당자 분석',
      reason: '미사용 — 기존 담당자 분석 페이지. (신규 "세일즈 KPI 현황" sales-kpi.html 과는 별개이며 그쪽은 유지.)'
    },
    {
      key: 'ly-product',
      href: 'ly-product.html',
      label: '전년상품 (전년 동기간 상품)',
      reason: '미사용 — 전년 동기간 상품 비교.'
    },
    {
      key: 'segment-analysis',
      href: 'gs-segment-analysis.html',
      label: '전체 세그 분석 자료',
      reason: '대외비 · 관리자 전용 — 공개 네비/외부 대시보드 미노출. (직접 URL·관리자 열람만.)'
    }
  ];

  var LS_OVERRIDES = 'gsn_menu_overrides';

  function readOverrides() {
    try { return JSON.parse(localStorage.getItem(LS_OVERRIDES) || '{}') || {}; }
    catch (e) { return {}; }
  }

  // 관리자 토글(override='show') 이 걸린 항목은 제외하고, 실제로 숨길 목록 반환
  window.gsnEffectiveHiddenMenus = function () {
    var ov = readOverrides();
    return window.GSN_HIDDEN_MENUS.filter(function (m) { return ov[m.href] !== 'show'; });
  };

  function basename(h) {
    return (h || '').split('?')[0].split('#')[0].replace(/^.*\//, '');
  }

  function hideMenus() {
    var hidden = window.gsnEffectiveHiddenMenus();
    if (!hidden.length) return;
    var byBase = {};
    hidden.forEach(function (m) { byBase[m.href] = m.key; });

    var anchors = document.querySelectorAll('a[href]');
    for (var i = 0; i < anchors.length; i++) {
      var a = anchors[i];
      // 감추면 안 되는 링크(숨김 관리 페이지의 "열기" 등)는 제외
      if (a.classList.contains('hidden-menu-link')) continue;
      if (a.closest && a.closest('[data-keep-hidden-menu-visible]')) continue;

      var key = byBase[basename(a.getAttribute('href'))];
      if (key) {
        a.setAttribute('data-hidden-menu', key);
        a.style.display = 'none';
      }
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', hideMenus);
  } else {
    hideMenus();
  }
})();
