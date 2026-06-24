#!/usr/bin/env python3
"""
docs/otb.html(GS 실적 리포트 · 당일현황)을 복제해 docs/sales-kpi.html(세일즈 KPI 현황 리포트)을 생성.

축만 '사업장/권역' → '세그먼트(1뎁스)→담당자(2뎁스)'로 변경하고 카드/게이지/피벗/차트 UI는 그대로 재사용.
담당자↔사업장 매핑은 docs/data/sales_manager_map.json(config)에서 런타임 fetch (하드코딩 금지).

멱등: otb.html을 원본으로 매번 재생성하므로 otb.html UI 변경이 자동 반영됨. build.py 이후/이전 어디서 실행해도 동작.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "docs" / "otb.html"
DST = ROOT / "docs" / "sales-kpi.html"


def must_sub(pattern, repl, text, flags=0, label="", count=0):
    new, n = re.subn(pattern, repl, text, count=count, flags=flags)
    if n == 0:
        sys.exit(f"[make_sales_kpi] 치환 실패: {label} (패턴 미매치) — otb.html 구조 변경 의심")
    return new


def main():
    html = SRC.read_text(encoding="utf-8")

    # 1) <title>
    html = must_sub(
        r"<title>GS 실적 리포트 · SONO & TRINITY GS팀</title>",
        "<title>세일즈 KPI 현황 리포트 · SONO & TRINITY GS팀</title>",
        html, label="title",
    )

    # 2) 모바일 시트 탭(서브페이지 네비) 제거 — 새 페이지는 서브페이지가 없음
    html = must_sub(
        r"<!-- MOBILE SHEET TABS \(visible < 860px\) -->.*?</script>\n",
        "",
        html, flags=re.DOTALL, label="mobile-sheet-tabs",
    )

    # 3) 모바일 필터바: 월/세그/권역(+사업장) → 월/세그/담당자
    mobile_bar = (
        '<!-- MOBILE FILTER BAR (visible < 860px) -->\n'
        '<div class="mobile-filter-bar" id="mobile-filter-bar">\n'
        '  <span style="font-family:var(--mono);font-size:10px;color:var(--ink-faint);letter-spacing:0.12em;">월:</span>\n'
        '  <div id="mobile-month-chips" class="filter-chips" style="flex-wrap:nowrap;overflow-x:auto;"></div>\n'
        '  <span style="font-family:var(--mono);font-size:10px;color:var(--ink-faint);letter-spacing:0.12em;margin-left:8px;">세그:</span>\n'
        '  <div id="mobile-seg1-chips" class="filter-chips" style="flex-wrap:nowrap;overflow-x:auto;"></div>\n'
        '  <span style="font-family:var(--mono);font-size:10px;color:var(--ink-faint);letter-spacing:0.12em;margin-left:8px;">담당자:</span>\n'
        '  <div id="mobile-manager-chips" class="filter-chips" style="flex-wrap:nowrap;overflow-x:auto;"></div>\n'
        '</div>\n'
    )
    html = must_sub(
        r"<!-- MOBILE FILTER BAR \(visible < 860px\) -->.*?(?=<!-- ========== APP SHELL)",
        lambda m: mobile_bar,
        html, flags=re.DOTALL, label="mobile-filter-bar",
    )

    # 4) 사이드바 내부: GS 서브네비 + 투숙월/세그/권역/사업장 → 세그먼트→담당자→투숙월
    sidebar_inner = (
        '<div>\n'
        '      <div class="filter-section-label">세그먼트</div>\n'
        '      <div class="filter-chips" id="seg1-chips"></div>\n'
        '    </div>\n'
        '    <div class="sidebar-divider"></div>\n'
        '    <div>\n'
        '      <div class="filter-section-label">담당자 <span id="manager-seg-label" style="font-weight:400;opacity:0.6;"></span></div>\n'
        '      <div class="filter-chips" id="manager-chips"></div>\n'
        '    </div>\n'
        '    <div class="sidebar-divider"></div>\n'
        '    <div>\n'
        '      <div class="filter-section-label">투숙월</div>\n'
        '      <div class="filter-chips" id="month-chips"></div>\n'
        '    </div>\n'
        '    '
    )
    html = must_sub(
        r'(<aside class="sidebar" id="sidebar">\s*).*?(?=<div class="refresh-info">)',
        lambda m: m.group(1) + sidebar_inner,
        html, flags=re.DOTALL, label="sidebar-inner",
    )

    # 4b) 마스트헤드 라벨
    html = must_sub(
        r'<div class="team-tag">GS팀 · OTB 현황</div>',
        '<div class="team-tag">GS팀 · 세일즈 KPI</div>',
        html, label="masthead-team-tag",
    )
    html = must_sub(
        r'<span class="page-badge">📊 GS실적 · 당일현황</span>',
        '<span class="page-badge">📊 세일즈 KPI · 담당자별</span>',
        html, label="masthead-badge",
    )

    # 5) 페이지 헤더 타이틀
    html = must_sub(
        r'GS 실적 리포트\n        <span class="sub">ON THE BOOKS · DAILY SNAPSHOT</span>',
        '세일즈 KPI 현황 리포트\n        <span class="sub">BY SALES MANAGER · ON THE BOOKS</span>',
        html, label="page-title",
    )

    # 6) 피벗 타이틀 (담당 사업장임을 명시)
    html = must_sub(
        r'<span class="table-card-title">영업장별 OTB 현황 \(피벗\)</span>',
        '<span class="table-card-title">담당 영업장별 OTB 현황 (피벗)</span>',
        html, label="pivot-title",
    )

    # 6b) 동기간 대비 추이 표: 빌드 주입 정적표(전 사업장) → 클라이언트 렌더 컨테이너로 교체.
    #     마커를 제거해 build.py의 inject_yoy_property_table가 sales-kpi엔 재주입하지 않도록 함.
    html = must_sub(
        r"<!-- YOY_PROP_TABLE_START -->.*?<!-- YOY_PROP_TABLE_END -->",
        lambda m: '<div id="yoy-prop-table-container"></div>',
        html, flags=re.DOTALL, label="yoy-table-container",
    )
    html = must_sub(
        r'<span class="table-card-title">사업장별 4·5·6월 전년 동기간 대비 추이 \(동기간 보정\)</span>',
        '<span class="table-card-title">담당 사업장별 전년 동기간 대비 추이 (동기간 보정)</span>',
        html, label="yoy-table-title",
    )

    # 6c) renderYoyPropertyTable: 담당자 축(세그+담당 사업장)으로 필터해 클라이언트 렌더
    new_yoy_fn = r'''function renderYoyPropertyTable() {
  const container = document.getElementById('yoy-prop-table-container');
  if (!container) return;
  if (!rawData || !rawData.yoyTable) { container.innerHTML = ''; return; }

  const seg = activeSeg;
  const mgrs = managersForSeg(seg);
  const mgr = mgrs.find(m => m.name === activeManager) || mgrs[0] || null;
  if (!mgr) { container.innerHTML = ''; return; }
  const mgrProps = new Set(resolveProps(mgr));
  const segDef = SEG_DEFS.find(s => s.key === seg);
  const segLabel = segDef ? segDef.label : seg;

  const yoyTable = rawData.yoyTable;
  const propRows = yoyTable.filter(r => !r.is_segment && mgrProps.has(r.name));
  if (!propRows.length) { container.innerHTML = '<p style="font-family:monospace;font-size:11px;color:#888;">데이터 없음</p>'; return; }
  const months = Object.keys(propRows[0].months || {}).sort((a,b) => +a - +b);

  function arrow(yoy) {
    if (yoy == null) return '<span style="color:#888;">—</span>';
    const c = yoy >= 3 ? '#4caf89' : yoy <= -3 ? '#e05555' : '#b0a060';
    const a = yoy >= 3 ? '▲' : yoy <= -3 ? '↓' : '→';
    return `<span style="color:${c};font-weight:700;">${a} ${yoy >= 0 ? '+' : ''}${yoy.toFixed(1)}%</span>`;
  }

  let html = '<div style="overflow-x:auto;"><p style="font-family:monospace;font-size:10.5px;color:#888;margin-bottom:8px;">'
    + '세그먼트: ' + segLabel + ' · 담당자: ' + mgr.name
    + ' &nbsp;·&nbsp; ▲ 전년(동기간) 대비 개선 / → 유사 / ↓ 부진</p>';
  html += '<table style="width:100%;border-collapse:collapse;font-family:var(--sans,sans-serif);"><thead><tr>';
  html += '<th style="padding:8px 10px;font-family:var(--mono,monospace);font-size:11px;font-weight:600;letter-spacing:0.08em;border-bottom:2px solid #555;text-align:left;">사업장</th>';
  for (const m of months) {
    html += `<th style="padding:8px 10px;font-family:var(--mono,monospace);font-size:11px;font-weight:600;letter-spacing:0.08em;border-bottom:2px solid #555;text-align:left;">${m}월</th>`;
  }
  html += '</tr></thead><tbody>';

  for (const row of propRows) {
    html += '<tr>';
    html += `<td style="padding:8px 10px;border-bottom:1px solid #333;white-space:nowrap;"><span style="font-size:12px;font-weight:600;">${row.name}</span></td>`;
    for (const m of months) {
      let cell = '<td style="padding:8px 10px;border-bottom:1px solid #333;vertical-align:top;">';
      let actSum = 0, lastSum = 0, budSum = 0, rmSum = 0, hasData = false;
      // 해당 세그먼트 sub-row(parent=사업장, name=세그)
      const segRow = yoyTable.find(r => r.is_segment && r.parent === row.name && r.name === seg);
      if (segRow && segRow.months && segRow.months[m]) {
        const smd = segRow.months[m];
        actSum += smd.act_rn || 0; lastSum += smd.last_rn || 0; budSum += smd.bud_rn || 0; hasData = true;
      }
      if (rmFcstSegments && rmFcstSegments[row.name]) {
        const mKey = `2026-${String(m).padStart(2,'0')}`;
        const mEntry = rmFcstSegments[row.name][mKey];
        if (mEntry && mEntry.segments) {
          const sd = mEntry.segments[seg];
          if (sd && sd.rm_fcst_rn != null) rmSum += sd.rm_fcst_rn;
        }
      }
      if (hasData) {
        const yoy = lastSum > 0 ? ((actSum / lastSum - 1) * 100) : null;
        cell += `<div style="font-size:12px;">${actSum.toLocaleString()}실</div>`;
        cell += `<div style="font-size:11px;color:#888;">전년<span style="font-size:0.75em;opacity:0.55;font-weight:normal">(동기간)</span> ${lastSum.toLocaleString()}실</div>`;
        cell += `<div style="font-size:12px;margin-top:3px;">${arrow(yoy)}</div>`;
        if (rmSum > 0) {
          const rmAch = budSum > 0 ? (rmSum / budSum * 100).toFixed(1) + '%' : '';
          cell += `<div style="font-size:10px;color:#e8a256;margin-top:1px;">RM: ${rmSum.toLocaleString()}실${rmAch ? ' (' + rmAch + ')' : ''}</div>`;
        }
      } else {
        cell += '—';
      }
      cell += '</td>';
      html += cell;
    }
    html += '</tr>';
  }
  html += '</tbody></table></div>';
  container.innerHTML = html;
}'''
    html = must_sub(
        r"function renderYoyPropertyTable\(\) \{.*?\n  container\.innerHTML = html;\n\}",
        lambda m: new_yoy_fn,
        html, flags=re.DOTALL, label="renderYoyPropertyTable",
    )

    # 7) STATE 변수
    html = must_sub(
        r"let activeMonths = new Set\(\[new Date\(\)\.getMonth\(\) \+ 1\]\);\n"
        r"let activeSegs = new Set\(\['전체'\]\);\n"
        r"let activeRegion = '전체';\n"
        r"let activeProps = new Set\(\);  // empty = 전체 \(no filter\)",
        (
            "let activeMonths = new Set([new Date().getMonth() + 1]);\n"
            "let activeSeg = 'OTA';            // 1뎁스 세그먼트 (otb_data 세그 key)\n"
            "let activeManager = null;          // 2뎁스 담당자명\n"
            "let managerMap = null;             // sales_manager_map.json (세그→담당자→사업장 config)\n"
            "let allPropNames = [];             // otb_data 전체 사업장명 (props:'ALL' 해석용)"
        ),
        html, label="state-vars",
    )

    # 8) buildFilterChips + buildPropChips 전체 교체 (담당자 축)
    new_filter_fns = r'''const SEG_DEFS = [
  { key: 'OTA',     label: 'OTA' },
  { key: 'G-OTA',   label: 'GOTA' },
  { key: 'Inbound', label: '인바운드' },
];

function managersForSeg(seg) {
  return (managerMap && managerMap[seg]) ? managerMap[seg] : [];
}
function resolveProps(mgr) {
  if (!mgr) return [];
  if (mgr.props === 'ALL' || mgr.props === '*') return allPropNames.slice();
  return Array.isArray(mgr.props) ? mgr.props.slice() : [];
}

function buildFilterChips(data) {
  // ─── 세그먼트(1뎁스) ───
  const segC  = document.getElementById('seg1-chips');
  const mSegC = document.getElementById('mobile-seg1-chips');
  [segC, mSegC].forEach(c => {
    if (!c) return;
    c.innerHTML = '';
    SEG_DEFS.forEach(s => {
      const btn = document.createElement('button');
      btn.className = 'chip' + (s.key === activeSeg ? ' active' : '');
      btn.textContent = s.label;
      btn.dataset.value = s.key;
      btn.addEventListener('click', () => {
        activeSeg = btn.dataset.value;
        const mgrs = managersForSeg(activeSeg);
        activeManager = mgrs.length ? mgrs[0].name : null;
        document.querySelectorAll('#seg1-chips .chip, #mobile-seg1-chips .chip').forEach(b => {
          b.classList.toggle('active', b.dataset.value === activeSeg);
        });
        buildManagerChips(data);
        applyFilters();
      });
      c.appendChild(btn);
    });
  });

  // ─── 담당자(2뎁스) ───
  buildManagerChips(data);

  // ─── 투숙월 ───
  const monthC  = document.getElementById('month-chips');
  const mMonthC = document.getElementById('mobile-month-chips');
  [monthC, mMonthC].forEach(c => {
    if (!c) return;
    c.innerHTML = '';
    data.filters.months.forEach(m => {
      const btn = document.createElement('button');
      btn.className = 'chip' + (activeMonths.has(m.value) ? ' active' : '');
      btn.textContent = m.label;
      btn.dataset.value = m.value;
      btn.addEventListener('click', () => {
        const v = parseInt(btn.dataset.value);
        if (v === 0) {
          activeMonths = new Set([0]);
        } else {
          activeMonths.delete(0);
          if (activeMonths.has(v)) {
            activeMonths.delete(v);
            if (activeMonths.size === 0) activeMonths.add(0);
          } else {
            activeMonths.add(v);
          }
        }
        document.querySelectorAll('#month-chips .chip, #mobile-month-chips .chip').forEach(b => {
          b.classList.toggle('active', activeMonths.has(parseInt(b.dataset.value)));
        });
        applyFilters();
      });
      c.appendChild(btn);
    });
  });
}

function buildManagerChips(data) {
  const mgrC  = document.getElementById('manager-chips');
  const mMgrC = document.getElementById('mobile-manager-chips');
  const segLabelEl = document.getElementById('manager-seg-label');
  const segDef = SEG_DEFS.find(s => s.key === activeSeg);
  if (segLabelEl) segLabelEl.textContent = segDef ? '(' + segDef.label + ')' : '';
  const mgrs = managersForSeg(activeSeg);
  if (activeManager == null && mgrs.length) activeManager = mgrs[0].name;
  if (mgrs.length && !mgrs.some(m => m.name === activeManager)) activeManager = mgrs[0].name;
  [mgrC, mMgrC].forEach(c => {
    if (!c) return;
    c.innerHTML = '';
    mgrs.forEach(mgr => {
      const btn = document.createElement('button');
      btn.className = 'chip' + (mgr.name === activeManager ? ' active' : '');
      btn.textContent = mgr.name;
      btn.dataset.value = mgr.name;
      btn.addEventListener('click', () => {
        activeManager = btn.dataset.value;
        document.querySelectorAll('#manager-chips .chip, #mobile-manager-chips .chip').forEach(b => {
          b.classList.toggle('active', b.dataset.value === activeManager);
        });
        applyFilters();
      });
      c.appendChild(btn);
    });
  });
}

function buildPropChips() { /* 담당자 축 페이지 — 사용 안 함 */ }
'''
    html = must_sub(
        r"function buildFilterChips\(data\) \{.*?\n\}\n\nfunction buildPropChips\(data\) \{.*?\n\}\n(?=\nfunction summaryFromProps)",
        lambda m: new_filter_fns,
        html, flags=re.DOTALL, label="buildFilterChips/buildPropChips",
    )

    # 9) applyFilters 전체 교체 (세그 + 담당자 축)
    new_apply = r'''function pickSnap() {
  if (!rawData) return null;
  if (!rawData.allMonths) return rawData;
  const monthKeys = activeMonths.has(0) ? [0] : [...activeMonths];
  if (monthKeys.length === 1) return rawData.allMonths[String(monthKeys[0])];
  const snaps = monthKeys.map(k => rawData.allMonths[String(k)]).filter(Boolean);
  if (!snaps.length) return null;
  const snap = {
    summary: mergeSegSummaries(snaps.map(s => s.summary).filter(Boolean)),
    byProperty: mergeSegByProp(snaps.map(s => s.byProperty).filter(Boolean)),
    segmentData: {}, byPropertySegment: {}
  };
  const allSegKeys = new Set();
  snaps.forEach(s => { if (s.segmentData) Object.keys(s.segmentData).forEach(k => allSegKeys.add(k)); });
  allSegKeys.forEach(seg => {
    const segSums = snaps.map(s => s.segmentData && s.segmentData[seg]).filter(Boolean);
    if (segSums.length) snap.segmentData[seg] = mergeSegSummaries(segSums);
    const segProps = snaps.map(s => s.byPropertySegment && s.byPropertySegment[seg]).filter(Boolean);
    if (segProps.length) snap.byPropertySegment[seg] = mergeSegByProp(segProps);
  });
  return snap;
}

function applyFilters() {
  if (!rawData) return;
  const seg = activeSeg;
  const segDef = SEG_DEFS.find(s => s.key === seg);
  const segLabel = segDef ? segDef.label : seg;
  const mgrs = managersForSeg(seg);
  const mgr = mgrs.find(m => m.name === activeManager) || mgrs[0] || null;

  const monthLabel = activeMonths.has(0) ? '전체 월' : [...activeMonths].sort((a,b)=>a-b).map(m=>m+'월').join('+');
  setText('table-filter-label', monthLabel + ' · ' + segLabel + (mgr ? ' · ' + mgr.name : ''));

  const snap = pickSnap();
  if (!snap || !mgr) return;

  const mgrProps = new Set(resolveProps(mgr));
  let byProp = (snap.byPropertySegment && snap.byPropertySegment[seg]) ? snap.byPropertySegment[seg] : (snap.byProperty || []);
  byProp = byProp.filter(p => mgrProps.has(p.name));
  const summary = summaryFromProps(byProp);

  const view = { ...rawData, summary, byProperty: byProp };
  renderPivot(view);
  updateKPIs(view);

  // 월별 차트: 세그 + 담당자 담당 사업장 필터 (filterRegion 센티넬 '__mgr__')
  buildMonthlyChart(rawData, '__mgr__', mgrProps, new Set([seg]));
  // 차트 타이틀을 담당자 기준으로 덮어쓰기
  const titleEl = document.getElementById('monthly-chart-title');
  if (titleEl) titleEl.textContent = '월별 RNS 실적 vs 목표 vs 전년(동기간) — ' + segLabel + (mgr ? ' · ' + mgr.name : '');

  // 동기간 대비 추이 표(담당 사업장만)
  renderYoyPropertyTable();
}
'''
    html = must_sub(
        r"function applyFilters\(\) \{.*?\n\}\n(?=\n\n// ─── YoY 사업장별 추이)",
        lambda m: new_apply,
        html, flags=re.DOTALL, label="applyFilters",
    )

    # 10) loadData: manager map 로드 함수 추가 + 호출
    loadmgr = r'''async function loadManagerMap() {
  try {
    const res = await fetch('data/sales_manager_map.json?t=' + Date.now());
    if (res.ok) managerMap = await res.json();
  } catch (e) { console.error('manager map load failed:', e); }
  if (!managerMap) managerMap = { 'OTA': [], 'G-OTA': [], 'Inbound': [] };
  allPropNames = (rawData && rawData.byProperty ? rawData.byProperty : []).map(p => p.name);
  const mgrs = managerMap[activeSeg] || [];
  if (mgrs.length) activeManager = mgrs[0].name;
}

async function loadData() {'''
    html = must_sub(
        r"async function loadData\(\) \{",
        lambda m: loadmgr,
        html, count=1, label="loadData-prefix",
    )
    html = must_sub(
        r"rawData = await otbRes\.json\(\);\n    render\(rawData\);",
        "rawData = await otbRes.json();\n    await loadManagerMap();\n    render(rawData);",
        html, label="loadData-call",
    )

    DST.write_text(html, encoding="utf-8")
    print(f"[make_sales_kpi] 생성 완료: {DST}  ({len(html):,} bytes)")


if __name__ == "__main__":
    main()
