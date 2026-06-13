#!/usr/bin/env python3
"""build_weekly_report_html.py — gs-closing-report.html 주간리포트 서브탭(WEEKLY_REPORT_HTML) 생성/주입.

소스: data/db_aggregated.json, data/rm_fcst.json, data/properties.json,
      docs/data/weekly_comparison.json, docs/data/campaign_data.json,
      docs/data/campaign_performance.json, data/enriched_notes.json
출력: docs/gs-closing-report.html 의 WEEKLY_REPORT_INJECT_START~END 블록 교체

규칙: RN/매출 = OTA+G-OTA+Inbound 3개 세그먼트만. '기타' 금지. 더미 금지.
      상품카테고리(룸온리/연박/패키지) vs 세그먼트(OTA/G-OTA/Inbound) 분리.
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path
from datetime import datetime, date, timedelta

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DOCS = ROOT / "docs"
DDATA = DOCS / "data"

SEGS = ("OTA", "G-OTA", "Inbound")

# RM 사업장 코드(사업계획 번호순) → db 사업장명
RM2DB = {
    "01.벨비발디": "소노벨 비발디파크", "02.캄비발디": "소노캄 비발디파크",
    "03.펫비발디": "소노펫 비발디파크", "04.펠리체비발디": "소노펠리체 비발디파크",
    "05.빌리지비발디": "소노펠리체 빌리지 비발디파크", "06.양평": "소노휴 양평",
    "07.델피노": "델피노", "08.쏠비치양양": "쏠비치 양양", "09.쏠비치삼척": "쏠비치 삼척",
    "10.소노벨단양": "소노문 단양", "11.소노캄경주": "소노벨 경주", "12.소노벨청송": "소노벨 청송",
    "13.소노벨천안": "소노벨 천안", "14.소노벨변산": "소노벨 변산", "15.소노캄여수": "소노캄 여수",
    "16.소노캄거제": "소노캄 거제", "17.쏠비치진도": "쏠비치 진도", "18.소노벨제주": "소노벨 제주",
    "19.소노캄제주": "소노캄 제주", "20.소노캄고양": "소노캄 고양", "21.소노문해운대": "소노문 해운대",
    "22.쏠비치남해": "쏠비치 남해", "23.르네블루": "르네블루",
}
DB2RM = {v: k for k, v in RM2DB.items()}
APPEND_PROPS = ["소노문 비발디파크", "오션월드빌리지"]  # RM 코드 없음 → 말미 노출

PCAT_GROUPS = {
    "룸온리": ["룸온니/프로모션"],
    "연박": ["연박/투나잇"],
    "패키지": ["조식패키지", "올인클루시브", "시즌패키지", "워터풀/오션", "세일/기획전", "액티비티/레저"],
}


def daterange(s: str, e: str):
    d0 = datetime.strptime(s, "%Y%m%d"); d1 = datetime.strptime(e, "%Y%m%d")
    out = []
    while d0 <= d1:
        out.append(d0.strftime("%Y%m%d")); d0 += timedelta(days=1)
    return out


def fmt(n):
    return f"{round(n):,}"


def pct_tag(p, neutral_if_none=True):
    """전년비/WoW % → kpi-tag span"""
    if p is None:
        return '<span class="kpi-tag tag-neutral">–</span>'
    if p > 999:
        return '<span class="kpi-tag tag-yoy-up">+999%↑</span>'
    if p < -999:
        return '<span class="kpi-tag tag-yoy-down">-999%↓</span>'
    cls = "tag-yoy-up" if p > 0 else ("tag-yoy-down" if p < 0 else "tag-yoy-flat")
    sign = "+" if p > 0 else ""
    return f'<span class="kpi-tag {cls}">{sign}{p:.1f}%</span>'


def ach_tag(pct):
    if pct is None:
        return '<span class="kpi-tag tag-neutral">–</span>'
    cls = "tag-yoy-up" if pct >= 100 else ("tag-yoy-flat" if pct >= 80 else "tag-yoy-down")
    return f'<span class="kpi-tag {cls}">{pct:.1f}%</span>'


def yoy_of(cur, ly):
    if ly is None or ly <= 0:
        return None
    return (cur - ly) / ly * 100


def main():
    agg = json.load(open(DATA / "db_aggregated.json", encoding="utf-8"))
    rm = json.load(open(DATA / "rm_fcst.json", encoding="utf-8"))
    wc = json.load(open(DDATA / "weekly_comparison.json", encoding="utf-8"))
    camp = json.load(open(DDATA / "campaign_data.json", encoding="utf-8"))
    enr = json.load(open(DATA / "enriched_notes.json", encoding="utf-8"))

    wt = wc["week_totals"]
    this_dates = daterange(wt["this_week"]["start"], wt["this_week"]["end"])
    prev_dates = daterange(wt["prev_week"]["start"], wt["prev_week"]["end"])
    ly_dates = daterange(wt["ly_week"]["start"], wt["ly_week"]["end"])
    days = wt["this_week"]["days"]
    pf = days / 30.0  # 일할 환산 계수

    pps = agg["pickup_daily_by_property_segment"]
    cps = agg["cancel_daily_by_property_segment"]

    def net_prop(prop, dates, segs=SEGS):
        rn = 0.0; rev = 0.0
        pu = pps.get(prop, {}); cu = cps.get(prop, {})
        for seg in segs:
            for d in dates:
                x = pu.get(seg, {}).get(d)
                if x:
                    rn += x["rn"]; rev += x["rev"]
                y = cu.get(seg, {}).get(d)
                if y:
                    rn -= y["rn"]; rev -= y["rev"]
        return rn, rev  # rev = 백만원

    def budget_prop(prop, month="2026-06", segs=SEGS):
        rk = DB2RM.get(prop)
        if not rk:
            return None, None
        node = rm["properties"].get(rk, {}).get(month, {}).get("segments", {})
        brn = sum(node.get(s, {}).get("rm_budget_rn", 0) for s in segs)
        brev = sum(node.get(s, {}).get("rm_budget_rev_mil", 0) for s in segs)
        return brn * pf, brev * pf  # rn(실), rev(백만원)

    # ── 사업장 순서 (RM 번호순 + 말미 2개) ──
    db_props = set(pps.keys())
    ordered = [RM2DB[k] for k in sorted(RM2DB) if RM2DB[k] in db_props]
    ordered += [p for p in APPEND_PROPS if p in db_props]
    for p in sorted(db_props):  # 누락 방지
        if p not in ordered:
            ordered.append(p)

    # ── 섹션1 행 데이터 ──
    s1_rows = []
    tb_rn = tb_rev = ta_rn = ta_rev = tl_rn = tl_rev = 0.0
    for prop in ordered:
        a_rn, a_rev = net_prop(prop, this_dates)
        l_rn, l_rev = net_prop(prop, ly_dates)
        b_rn, b_rev = budget_prop(prop)
        ta_rn += a_rn; ta_rev += a_rev; tl_rn += l_rn; tl_rev += l_rev
        if b_rn is not None:
            tb_rn += b_rn; tb_rev += b_rev
        s1_rows.append((prop, b_rn, b_rev, a_rn, a_rev, l_rn, l_rev))

    # ── 섹션2 세그먼트 ──
    seg_rows = []
    for seg in SEGS:
        a_rn = a_rev = l_rn = l_rev = 0.0
        for prop in db_props:
            ar, av = net_prop(prop, this_dates, (seg,))
            lr, lv = net_prop(prop, ly_dates, (seg,))
            a_rn += ar; a_rev += av; l_rn += lr; l_rev += lv
        rk_b_rn = sum(rm["properties"].get(DB2RM[p], {}).get("2026-06", {}).get("segments", {})
                      .get(seg, {}).get("rm_budget_rn", 0) for p in db_props if p in DB2RM)
        rk_b_rev = sum(rm["properties"].get(DB2RM[p], {}).get("2026-06", {}).get("segments", {})
                       .get(seg, {}).get("rm_budget_rev_mil", 0) for p in db_props if p in DB2RM)
        seg_rows.append((seg, rk_b_rn * pf, rk_b_rev * pf, a_rn, a_rev, l_rn, l_rev))

    seg_a_rn = {r[0]: r[3] for r in seg_rows}
    total_a_rn = sum(seg_a_rn.values())

    # ── 섹션3 채널 (weekly_comparison.by_channel, OTA/G-OTA 배지) ──
    bcs = agg.get("by_channel_segment", {})
    def seg_total(ch, s):
        m = bcs.get(ch, {}).get(s, {})
        if not isinstance(m, dict):
            return 0
        return sum(v.get("net_rn", 0) for v in m.values() if isinstance(v, dict))
    def badge(ch):
        return "G-OTA" if seg_total(ch, "G-OTA") > seg_total(ch, "OTA") else "OTA"
    # OTA 또는 G-OTA 세그먼트 실적이 있는 거래처만 (단체/회원 등 제외, Inbound-only는 wc에서 이미 제외)
    chans = [c for c in wc["by_channel"]
             if seg_total(c["channel"], "OTA") > 0 or seg_total(c["channel"], "G-OTA") > 0]
    # 금주 net_rn 내림차순(이미 정렬됨)

    # ── 섹션6 stay-month ──
    pm = agg["pickup_daily_by_property_month"]; cm = agg["cancel_daily_by_property_month"]
    def stay_net(prop, ym):
        p = pm.get(prop, {}).get(ym, {}); c = cm.get(prop, {}).get(ym, {})
        prn = sum(v["rn"] for v in p.values()); prev = sum(v["rev"] for v in p.values())
        crn = sum(v["rn"] for v in c.values()); crev = sum(v["rev"] for v in c.values())
        return prn - crn, (prev - crev)
    s6 = []
    for prop in ordered:
        r25, v25 = stay_net(prop, "202508")
        r24, _ = stay_net(prop, "202408")
        s6.append((prop, r25, v25, r24, yoy_of(r25, r24)))
    s6.sort(key=lambda x: x[1], reverse=True)
    s6_tot_rn = sum(x[1] for x in s6); s6_tot_rev = sum(x[2] for x in s6)

    # 상품 카테고리 (stay-month 2025.08)
    bc = agg["product_detail"]["by_category"]
    def cat_rn(cat, ym):
        return bc.get(cat, {}).get(ym[:4], {}).get(ym, {}).get("rn", 0)
    pcat = {}
    for g, cats in PCAT_GROUPS.items():
        pcat[g] = sum(cat_rn(c, "202508") for c in cats)
    pcat_tot = sum(pcat.values())

    # ── 캠페인 (summer_detail) ──
    det = camp["summer_detail"]
    def pdt(s):
        try: return date.fromisoformat(s)
        except Exception: return None
    wk_s, wk_e = date(2026, 6, 8), date(2026, 6, 14)
    nx_s, nx_e = date(2026, 6, 15), date(2026, 6, 21)
    this_camps = [d for d in det if pdt(d["판매시작"]) and pdt(d["판매종료"])
                  and pdt(d["판매시작"]) <= wk_e and pdt(d["판매종료"]) >= wk_s]
    next_camps = [d for d in det if pdt(d["판매시작"]) and nx_s <= pdt(d["판매시작"]) <= nx_e]

    # ════════════════ HTML 생성 ════════════════
    yy, mm, dd = wt["this_week"]["start"][:4], wt["this_week"]["start"][4:6], wt["this_week"]["start"][6:8]
    ee_m, ee_d = wt["this_week"]["end"][4:6], wt["this_week"]["end"][6:8]
    iso_week = datetime.strptime(wt["this_week"]["start"], "%Y%m%d").isocalendar()[1]
    ly_s = wt["ly_week"]["start"]; ly_e = wt["ly_week"]["end"]

    WD = ["월", "화", "수", "목", "금", "토", "일"]
    def wd(ymd):
        return WD[datetime.strptime(ymd, "%Y%m%d").weekday()]
    this_lbl = f"{yy}.{mm}.{dd}({wd(wt['this_week']['start'])}) ~ {yy}.{ee_m}.{ee_d}({wd(wt['this_week']['end'])})"
    ly_lbl = f"{ly_s[:4]}.{ly_s[4:6]}.{ly_s[6:8]}({wd(ly_s)})~{ly_e[:4]}.{ly_e[4:6]}.{ly_e[6:8]}({wd(ly_e)})"

    # KPI  (seg_rows tuple = (seg, b_rn, b_rev, a_rn, a_rev, l_rn, l_rev))
    a_rn = total_a_rn
    a_rev = sum(r[4] for r in seg_rows)   # 금주 매출 (백만원)
    b_rn = sum(r[1] for r in seg_rows); b_rev = sum(r[2] for r in seg_rows)  # 목표
    l_rn = sum(r[5] for r in seg_rows)    # 전년 RN
    ly_rev = sum(r[6] for r in seg_rows)  # 전년 매출 (백만원)
    ach_rn = a_rn / b_rn * 100 if b_rn else None
    yoy_rn = yoy_of(a_rn, l_rn)
    ach_rev = a_rev / b_rev * 100 if b_rev else None
    yoy_rev = yoy_of(a_rev, ly_rev)
    # 전주 OTA+GOTA+IB (WoW 기준)
    prev_rn = 0.0
    for prop in db_props:
        pr, _ = net_prop(prop, prev_dates); prev_rn += pr
    wow_rn = yoy_of(a_rn, prev_rn)

    def tagcls(p):
        return "tag-yoy-up" if (p is not None and p > 0) else ("tag-yoy-down" if (p is not None and p < 0) else "tag-yoy-flat")

    seg_compo = " · ".join(f"{s} {fmt(seg_a_rn[s])}" for s in SEGS)
    seg_pct = " · ".join(f"{s} {seg_a_rn[s]/a_rn*100:.0f}%" for s in SEGS) if a_rn else ""

    H = []
    H.append(f'''<section class="section" style="margin-bottom:24px">
  <span class="section-num">WEEKLY REPORT · {yy}-W{iso_week}</span>
  <h2 class="section-title"><span class="st-icon">🗓️</span> 주간 리포트 — {this_lbl}</h2>
  <div style="font-size:12px;color:var(--ink-muted);line-height:1.8;margin-bottom:18px">
    · <strong style="color:var(--ink-soft)">집계 기준</strong> — 금주 {wd(wt['this_week']['start'])}~{wd(wt['this_week']['end'])}({mm}/{dd}~{ee_m}/{ee_d}) {days}일. 06/11(목)~06/12(금)은 예약데이터 미반영(D-1 수집 지연).<br>
    · <strong style="color:var(--ink-soft)">실적 정의</strong> — 예약파일 기준 순예약(Net = Pickup − Cancel), <strong>OTA·G-OTA·Inbound 3개 세그먼트만 합산</strong>.<br>
    · <strong style="color:var(--ink-soft)">전년 동기간</strong> — 요일 정렬 {ly_lbl} {days}일.<br>
    · <strong style="color:var(--ink-soft)">목표(Budget)</strong> — RM Revenue Meeting(2026.06.08) 월 Budget(OTA+G-OTA+Inbound)을 일할 환산(×{days}/30)한 금주 페이스 목표.
  </div>
  <div class="kpi-grid">
    <div class="kpi-card kpi-rn">
      <div class="kpi-label">금주 순예약 RN</div>
      <div class="kpi-value">{fmt(a_rn)}<span style="font-size:13px;color:var(--ink-muted)"> 실</span></div>
      <div class="kpi-keywords">
        <span class="kpi-tag {tagcls(ach_rn-100 if ach_rn else None)}">목표 대비 {ach_rn:.1f}%</span>
        <span class="kpi-tag {tagcls(yoy_rn)}">YoY {('+' if yoy_rn and yoy_rn>0 else '')}{yoy_rn:.1f}%</span>
      </div>
      <div class="kpi-prev">목표 {fmt(b_rn)}실 · 전년 {fmt(l_rn)}실 · 전주 {fmt(prev_rn)}실(WoW {('+' if wow_rn and wow_rn>0 else '')}{wow_rn:.1f}%)</div>
    </div>
    <div class="kpi-card kpi-rev">
      <div class="kpi-label">금주 순예약 매출</div>
      <div class="kpi-value">{fmt(a_rev*1000)}<span style="font-size:13px;color:var(--ink-muted)"> 천원</span></div>
      <div class="kpi-keywords">
        <span class="kpi-tag {tagcls(ach_rev-100 if ach_rev else None)}">목표 대비 {ach_rev:.1f}%</span>
        <span class="kpi-tag {tagcls(yoy_rev)}">YoY {('+' if yoy_rev and yoy_rev>0 else '')}{yoy_rev:.1f}%</span>
      </div>
      <div class="kpi-prev">목표 {fmt(b_rev*1000)}천원 · 전년 {fmt(ly_rev*1000)}천원</div>
    </div>
    <div class="kpi-card kpi-adr">
      <div class="kpi-label">세그먼트 구성 (RN)</div>
      <div class="kpi-value" style="font-size:18px">{seg_compo}</div>
      <div class="kpi-prev">{seg_pct}</div>
    </div>
  </div>
</section>''')

    # ── 섹션 1 ──
    def s1_tr(row, extra=False):
        prop, b_rn, b_rev, a_rn, a_rev, l_rn, l_rev = row
        cls = ' class="s1-extra" style="display:none"' if extra else ''
        bd_present = b_rn is not None
        ach = (a_rn / b_rn * 100) if (bd_present and b_rn) else None
        yoy = yoy_of(a_rn, l_rn)
        badge_no = '' if bd_present else ' <span class="badge badge-top">RM코드外</span>'
        ly_no = (l_rn <= 0)
        b_rn_s = fmt(b_rn) if bd_present else '–'
        b_rev_s = fmt(b_rev*1000) if bd_present else '–'
        ly_rn_s = fmt(l_rn) if not ly_no else '0'
        ly_rev_s = fmt(l_rev*1000) if l_rev else ('0' if not ly_no else '0')
        return f'''<tr{cls}>
<td>{prop}{badge_no}</td>
<td>{b_rn_s}</td><td><strong style="color:var(--ink)">{fmt(a_rn)}</strong></td>
<td>{ach_tag(ach)}</td>
<td>{ly_rn_s}</td><td>{pct_tag(yoy) if not ly_no else '<span class="kpi-tag tag-neutral">–</span>'}</td>
<td>{b_rev_s}</td><td><strong style="color:var(--ink)">{fmt(a_rev*1000)}</strong></td>
<td>{ly_rev_s}</td><td>{pct_tag(yoy_of(a_rev,l_rev)) if (l_rev and l_rev>0) else '<span class="kpi-tag tag-neutral">–</span>'}</td></tr>'''

    vis = s1_rows[:10]; ext = s1_rows[10:]
    tot_ach = ta_rn / tb_rn * 100 if tb_rn else None
    tot_yoy = yoy_of(ta_rn, tl_rn)
    tot_yoy_rev = yoy_of(ta_rev, tl_rev)
    H.append(f'''<section class="section">
  <span class="section-num">SECTION 01</span>
  <h2 class="section-title"><span class="st-icon">🏨</span> 사업장별 실적 <span style="font-size:11px;font-weight:600;color:var(--ink-faint)">· 단위 RN(실) / 매출(천원) · 사업장 순서 = 사업계획 번호순</span></h2>
  <div class="table-wrap">
  <table class="full-table">
    <thead>
      <tr><th rowspan="2">사업장</th><th class="group-header" colspan="5">객실 RN (실)</th><th class="group-header" colspan="4">매출 (천원)</th></tr>
      <tr><th class="sub-header">목표</th><th class="sub-header">실적</th><th class="sub-header">달성률</th><th class="sub-header">전년동기</th><th class="sub-header">전년비</th><th class="sub-header">목표</th><th class="sub-header">실적</th><th class="sub-header">전년동기</th><th class="sub-header">전년비</th></tr>
    </thead>
    <tbody>
{chr(10).join(s1_tr(r) for r in vis)}
{chr(10).join(s1_tr(r, True) for r in ext)}
<tr class="total-row"><td>합계 (OTA+G-OTA+Inbound)</td>
<td>{fmt(tb_rn)}</td><td>{fmt(ta_rn)}</td><td>{tot_ach:.1f}%</td>
<td>{fmt(tl_rn)}</td><td>{('+' if tot_yoy>0 else '')}{tot_yoy:.1f}%</td>
<td>{fmt(tb_rev*1000)}</td><td>{fmt(ta_rev*1000)}</td><td>{fmt(tl_rev*1000)}</td><td>{('+' if tot_yoy_rev>0 else '')}{tot_yoy_rev:.1f}%</td></tr>
    </tbody></table></div>
  {f'<button class="toggle-more" onclick="wkToggle(&#39;s1&#39;,this)">+ {len(ext)}개 사업장 더보기</button>' if ext else ''}
</section>''')

    # ── 섹션 2 ──
    def seg_tr(row):
        seg, b_rn, b_rev, a_rn, a_rev, l_rn, l_rev = row
        ach = a_rn / b_rn * 100 if b_rn else None
        return f'''<tr>
<td>{seg}</td>
<td>{fmt(b_rn)}</td><td><strong style="color:var(--ink)">{fmt(a_rn)}</strong></td>
<td>{ach_tag(ach)}</td>
<td>{fmt(l_rn)}</td><td>{pct_tag(yoy_of(a_rn,l_rn)) if l_rn>0 else '<span class="kpi-tag tag-neutral">–</span>'}</td>
<td>{fmt(b_rev*1000)}</td><td><strong style="color:var(--ink)">{fmt(a_rev*1000)}</strong></td>
<td>{fmt(l_rev*1000)}</td><td>{pct_tag(yoy_of(a_rev,l_rev)) if l_rev>0 else '<span class="kpi-tag tag-neutral">–</span>'}</td></tr>'''
    H.append(f'''<section class="section">
  <span class="section-num">SECTION 02</span>
  <h2 class="section-title"><span class="st-icon">🧩</span> 세그먼트별 실적 <span style="font-size:11px;font-weight:600;color:var(--ink-faint)">· OTA / G-OTA / Inbound</span></h2>
  <div class="table-wrap"><table class="full-table">
    <thead><tr><th rowspan="2">세그먼트</th><th class="group-header" colspan="5">객실 RN (실)</th><th class="group-header" colspan="4">매출 (천원)</th></tr>
    <tr><th class="sub-header">목표</th><th class="sub-header">실적</th><th class="sub-header">달성률</th><th class="sub-header">전년동기</th><th class="sub-header">전년비</th><th class="sub-header">목표</th><th class="sub-header">실적</th><th class="sub-header">전년동기</th><th class="sub-header">전년비</th></tr></thead>
    <tbody>
{chr(10).join(seg_tr(r) for r in seg_rows)}
<tr class="total-row"><td>합계</td>
<td>{fmt(b_rn)}</td><td>{fmt(a_rn)}</td><td>{ach_rn:.1f}%</td>
<td>{fmt(l_rn)}</td><td>{('+' if yoy_rn>0 else '')}{yoy_rn:.1f}%</td>
<td>{fmt(b_rev*1000)}</td><td>{fmt(a_rev*1000)}</td><td>{fmt(ly_rev*1000)}</td><td>{('+' if yoy_rev>0 else '')}{yoy_rev:.1f}%</td></tr>
    </tbody></table></div></section>''')

    # ── 섹션 3 ──
    def ch_tr(c, extra=False):
        cls = ' class="s3-extra" style="display:none"' if extra else ''
        bg = badge(c["channel"])
        if bg == "G-OTA":
            bspan = '<span class="badge" style="background:var(--gold-dim);color:var(--gold-bright)">G-OTA</span>'
        else:
            bspan = '<span class="badge" style="background:var(--info-bg);color:var(--info)">OTA</span>'
        share = c["this_net_rn"] / total_a_rn * 100 if total_a_rn else 0
        ly_rn = c["ly_net_rn"]; ly_rev = c["ly_net_rev"]
        yoy_rn_c = yoy_of(c["this_net_rn"], ly_rn)
        yoy_rev_c = yoy_of(c["this_net_rev"], ly_rev)
        return f'''<tr{cls}>
<td>{c["channel"]}</td>
<td style="text-align:center">{bspan}</td>
<td><strong style="color:var(--ink)">{fmt(c["this_net_rn"])}</strong></td><td>{fmt(ly_rn)}</td><td>{pct_tag(yoy_rn_c) if ly_rn>0 else '<span class="kpi-tag tag-neutral">–</span>'}</td>
<td><strong style="color:var(--ink)">{fmt(c["this_net_rev"]*1000)}</strong></td><td>{fmt(ly_rev*1000)}</td><td>{pct_tag(yoy_rev_c) if ly_rev>0 else '<span class="kpi-tag tag-neutral">–</span>'}</td>
<td>{share:.1f}%</td></tr>'''
    cvis = chans[:10]; cext = chans[10:]
    H.append(f'''<section class="section">
  <span class="section-num">SECTION 03</span>
  <h2 class="section-title"><span class="st-icon">🔗</span> 채널별 실적 <span style="font-size:11px;font-weight:600;color:var(--ink-faint)">· 거래처(채널) {len(chans)}개 · 순예약 RN 순</span></h2>
  <div class="table-wrap"><table class="full-table">
    <thead><tr><th rowspan="2">거래처(채널)</th><th rowspan="2">세그먼트</th><th class="group-header" colspan="3">객실 RN (실)</th><th class="group-header" colspan="3">매출 (천원)</th><th rowspan="2">비중</th></tr>
    <tr><th class="sub-header">금주 실적</th><th class="sub-header">전년동기</th><th class="sub-header">전년비</th><th class="sub-header">금주 실적</th><th class="sub-header">전년동기</th><th class="sub-header">전년비</th></tr></thead>
    <tbody>
{chr(10).join(ch_tr(c) for c in cvis)}
{chr(10).join(ch_tr(c, True) for c in cext)}
    </tbody></table></div>
  {f'<button class="toggle-more" onclick="wkToggle(&#39;s3&#39;,this)">+ {len(cext)}개 채널 더보기</button>' if cext else ''}
</section>''')

    # ── 섹션 4 인사이트 ──
    ins = []
    ins.append(f"<li><span class='insight-kw'>전사</span>금주({mm}/{dd}~{ee_m}/{ee_d}, {wd(wt['this_week']['start'])}~{wd(wt['this_week']['end'])}) OTA·G-OTA·Inbound 순예약 <strong>{fmt(a_rn)}실</strong> · 매출 <strong>{fmt(a_rev*1000)}천원</strong>. 일할 환산 RM Budget {fmt(b_rn)}실 대비 <strong>{ach_rn:.1f}%</strong>, 전년 동기간({fmt(l_rn)}실) 대비 <strong>{('+' if yoy_rn>0 else '')}{yoy_rn:.1f}%</strong>.</li>")
    ins.append(f"<li><span class='insight-kw'>주간 흐름</span>전주({prev_dates[0][4:6]}/{prev_dates[0][6:8]}~{prev_dates[-1][4:6]}/{prev_dates[-1][6:8]}) 순예약 {fmt(prev_rn)}실 대비 <strong>{('+' if wow_rn>0 else '')}{wow_rn:.1f}%</strong>. 전주는 취소 반영분이 커 순예약 베이스가 낮았던 점을 감안해 절대 페이스(RM Budget의 {ach_rn:.0f}%) 기준으로 해석 권장.</li>")
    # 급등/급락 (WoW) 사업장
    prop_wow = []
    for prop in ordered:
        ar, _ = net_prop(prop, this_dates); pr, _ = net_prop(prop, prev_dates)
        w = yoy_of(ar, pr)
        prop_wow.append((prop, ar, pr, w))
    rising = sorted([x for x in prop_wow if x[3] is not None and abs(x[1]) >= 30 and x[3] > 0], key=lambda x: -x[3])
    falling = sorted([x for x in prop_wow if x[3] is not None and abs(x[2]) >= 30 and x[3] < 0], key=lambda x: x[3])
    # YoY 사업장
    prop_yoy = []
    for prop in ordered:
        ar, _ = net_prop(prop, this_dates); lr, _ = net_prop(prop, ly_dates)
        prop_yoy.append((prop, ar, lr, yoy_of(ar, lr)))
    yoy_up = sorted([x for x in prop_yoy if x[3] is not None and x[1] >= 30 and x[3] > 0], key=lambda x: -x[3])[:3]
    yoy_dn = sorted([x for x in prop_yoy if x[3] is not None and x[2] >= 30 and x[3] < 0], key=lambda x: x[3])[:4]
    if yoy_up:
        ins.append("<li><span class='insight-kw'>급등 사업장</span>" + ", ".join(f"{x[0]}({'+' if x[3]>0 else ''}{x[3]:.1f}%)" for x in yoy_up) + " 등이 전년비 두 자릿수 이상 성장하며 전사 실적을 방어.</li>")
    if yoy_dn:
        ins.append("<li><span class='insight-kw'>급락 사업장</span>" + ", ".join(f"{x[0]}({x[3]:.1f}%)" for x in yoy_dn) + ". 해당 사업장은 전년 대비 약세로 채널·요금 점검 필요.</li>")
    # 세그먼트 이동
    seg_str = ", ".join(f"{s} {fmt(seg_a_rn[s])}실(YoY {('+' if (yoy_of(seg_a_rn[s], next(r[6] for r in seg_rows if r[0]==s)) or 0)>0 else '')}{(yoy_of(seg_a_rn[s], next(r[6] for r in seg_rows if r[0]==s)) or 0):.1f}%)" for s in SEGS)
    ins.append(f"<li><span class='insight-kw'>세그먼트 이동</span>{seg_str} 구도. 세그먼트별 YoY·구성비를 함께 보아 채널 이동 신호를 점검.</li>")
    # 채널 호조
    ch_up = sorted([c for c in chans if (yoy_of(c["this_net_rn"], c["ly_net_rn"]) or 0) > 0 and c["this_net_rn"] >= 50 and c["ly_net_rn"] > 0], key=lambda c: -(yoy_of(c["this_net_rn"], c["ly_net_rn"]) or 0))[:3]
    if ch_up:
        ins.append("<li><span class='insight-kw'>채널</span>" + ", ".join(f"{c['channel']}(+{yoy_of(c['this_net_rn'],c['ly_net_rn']):.0f}%)" for c in ch_up) + " 등 거래처가 전년비 고성장하며 회복세가 뚜렷.</li>")
    # 달성률 미달
    low_ach = [(p, a_rn / b_rn * 100) for (p, b_rn, b_rev, a_rn, a_rev, l_rn, l_rev) in s1_rows if b_rn and a_rn / b_rn * 100 < 60]
    low_ach.sort(key=lambda x: x[1])
    if low_ach:
        ins.append("<li><span class='insight-kw'>달성률 미달</span>금주 페이스 목표 60% 미만: " + ", ".join(f"{p}({a:.0f}%)" for p, a in low_ach[:4]) + ". 거래처 영업 강화 필요.</li>")
    # Inbound
    ib_a = seg_a_rn["Inbound"]; ib_l = next(r[6] for r in seg_rows if r[0] == "Inbound")
    ib_yoy = yoy_of(ib_a, ib_l)
    ins.append(f"<li><span class='insight-kw'>Inbound 점검</span>Inbound 순예약 {fmt(ib_a)}실로 전년({fmt(ib_l)}실) 대비 {('+' if ib_yoy and ib_yoy>0 else '')}{ib_yoy:.1f}% — {'회복 흐름, 단체 유치 모멘텀 유지' if ib_yoy and ib_yoy>0 else '전년 대비 둔화, 단체·해외 채널 재점검 필요'}.</li>")
    # 권역 (enriched_notes region_status)
    rs = enr.get("region_status", {})
    if rs:
        rmap = {"vivaldi": "비발디", "central": "중부", "south": "남부", "apac": "APAC"}
        reg_str = " · ".join(f"{rmap.get(k,k)} {v.get('달성률','')}%" for k, v in rs.items())
        ins.append(f"<li><span class='insight-kw'>권역 상황</span>권역별 달성률(stay-month 페이싱) — {reg_str}. 권역별 달성률을 차주 영업 우선순위에 반영 권장.</li>")
    H.append('<section class="section"><span class="section-num">SECTION 04</span><h2 class="section-title"><span class="st-icon">💡</span> 주간 인사이트</h2><div class="insight-box"><h3>금주 실적 · enriched_notes 기반 자동 분석</h3><ul>' + "".join(ins) + '</ul></div></section>')

    # ── 섹션 5 캠페인 ──
    def camp_tr(c):
        return f'''<tr>
<td>{c["사업장"]}</td>
<td style="font-family:var(--font)">{c["채널"]}</td>
<td>{c["판매시작"]} ~ {c["판매종료"]}</td>
<td>{c["투숙시작"]} ~ {c["투숙종료"]}</td>
<td style="text-align:left;font-family:var(--font);white-space:normal">{c.get("상품","협의중") or "협의중"}</td></tr>'''
    H.append(f'''<section class="section"><span class="section-num">SECTION 05</span>
<h2 class="section-title"><span class="st-icon">🎯</span> 기획전 (액션플랜)</h2>
<div class="card"><div class="card-header"><h3>금주 진행 기획전 ({len(this_camps)}건)</h3><span style="font-size:11px;color:var(--ink-muted)">판매기간이 금주(6/08~6/14)와 중첩</span></div>
<div class="card-body flush"><div class="table-wrap"><table class="full-table" style="min-width:900px"><thead><tr><th>사업장</th><th>채널</th><th>판매기간</th><th>투숙기간</th><th>상품</th></tr></thead><tbody>{"".join(camp_tr(c) for c in this_camps)}</tbody></table></div></div></div>
<p style="font-size:11px;color:var(--ink-faint);margin:6px 0 18px">※ 금주 진행 기획전은 86 패키지코드 매핑 전으로 개별 측정실적 집계 전입니다. 측정 실적이 확보되는 항목(campaign_performance.json)만 별도 카드로 표기합니다.</p>
<div class="card"><div class="card-header"><h3>차주 예정 기획전 ({len(next_camps)}건)</h3><span style="font-size:11px;color:var(--ink-muted)">판매 개시 6/15~6/21</span></div>
<div class="card-body flush"><div class="table-wrap"><table class="full-table" style="min-width:900px"><thead><tr><th>사업장</th><th>채널</th><th>판매기간</th><th>투숙기간</th><th>상품</th></tr></thead><tbody>{"".join(camp_tr(c) for c in next_camps)}</tbody></table></div></div></div>
</section>''')

    # ── 섹션 6 +60일 ──
    plus60 = (date.today() + timedelta(days=60))
    # 전년 성장 둔화/역성장 (24→25 YoY 낮은 순, 운영 사업장만)
    slow = sorted([x for x in s6 if x[3] is not None and x[1] > 0], key=lambda x: x[4] if x[4] is not None else 999)[:5]
    nooper = [x[0] for x in s6 if x[3] == 0 or x[1] == 0]
    def s6_tr(row, extra=False):
        prop, r25, v25, r24, yoy = row
        cls = ' class="s6-extra" style="display:none"' if extra else ''
        if yoy is None:
            yoy_s = '<span class="kpi-tag tag-neutral">–</span>'
        else:
            col = 'var(--positive)' if yoy >= 0 else 'var(--negative)'
            yoy_s = f'<span style="color:{col};font-weight:700">{("+" if yoy>=0 else "")}{yoy:.1f}%</span>'
        return f'''<tr{cls}><td>{prop}</td>
<td><strong style="color:var(--ink)">{fmt(r25)}</strong></td>
<td>{fmt(v25*1000)}</td>
<td>{fmt(r24)}</td>
<td>{yoy_s}</td></tr>'''
    s6vis = s6[:10]; s6ext = s6[10:]
    slow_str = ", ".join(f"{x[0]}({('+' if x[4]>=0 else '')}{x[4]:.1f}%)" for x in slow if x[4] is not None)
    H.append(f'''<section class="section"><span class="section-num">SECTION 06</span>
<h2 class="section-title"><span class="st-icon">📆</span> +60일 준비사항 <span style="font-size:11px;font-weight:600;color:var(--ink-faint)">· 기준일 +60일 = {plus60.strftime("%Y.%m.%d")} → 전년 동기간 2025년 8월</span></h2>
<div class="insight-box" style="border-left-color:var(--info);margin-bottom:18px"><h3 style="color:var(--info)">+60일(8월) 준비 포인트</h3><ul>
<li><strong>전년 성장 둔화·역성장</strong> — {slow_str} : 2024→2025 8월 성장세가 멈췄거나 약한 사업장으로, +60일 시점 선제 프로모션·요금 점검 필요.</li>
<li><strong>전년 미운영/재가동 점검</strong> — {", ".join(nooper) if nooper else "해당 없음"} : 2025년 또는 2024년 8월 운영 실적이 없어 2026년 정상 운영 여부·채널 셋업 사전 확인 필요.</li>
<li><strong>전년 8월 호조 카테고리</strong> — 룸온리 {fmt(pcat["룸온리"])}실({pcat["룸온리"]/pcat_tot*100:.0f}%) · 패키지 {fmt(pcat["패키지"])}실({pcat["패키지"]/pcat_tot*100:.0f}%) · 연박 {fmt(pcat["연박"])}실({pcat["연박"]/pcat_tot*100:.0f}%) 순. 룸온리·패키지 중심 상품 구성을 +60일 전 사전 세팅 권장.</li>
<li><strong>RM 2026.08 Budget 참고</strong> — 금회 RM 스냅샷(2026.06.08)이 8월까지 커버 → 8월 OTA+G-OTA+Inbound Budget {fmt(sum(rm["properties"].get(DB2RM[p],{}).get("2026-08",{}).get("segments",{}).get(s,{}).get("rm_budget_rn",0) for p in db_props if p in DB2RM for s in SEGS))}실. 본 섹션의 사업장별 표는 전년 실적·전년비(24→25) 기준으로 준비 우선순위를 제시합니다(전년도 목표 데이터 부재로 더미 목표 미생성).</li>
</ul></div>
<h3 style="font-size:13px;color:var(--gold-bright);margin-bottom:10px">전년 동기간(2025.08) 사업장별 실적</h3>
<div class="table-wrap"><table class="full-table" style="min-width:900px"><thead><tr><th>사업장</th><th>2025.08 실적 RN(실)</th><th>2025.08 매출(천원)</th><th>2024.08 실적 RN(실)</th><th>전년비(24→25)</th></tr></thead><tbody>
{chr(10).join(s6_tr(r) for r in s6vis)}
{chr(10).join(s6_tr(r, True) for r in s6ext)}
<tr class="total-row"><td>합계</td><td>{fmt(s6_tot_rn)}</td><td>{fmt(s6_tot_rev*1000)}</td><td colspan="2" style="text-align:left">전년 동기간(8월) 전체 순예약 기준</td></tr>
</tbody></table></div>
{f'<button class="toggle-more" onclick="wkToggle(&#39;s6&#39;,this)">+ {len(s6ext)}개 사업장 더보기</button>' if s6ext else ''}
<p style="font-size:11px;color:var(--ink-faint);margin-top:8px;line-height:1.7">
※ 상품 카테고리(룸온리/연박/패키지)는 세그먼트(OTA/G-OTA/Inbound)와 별개 분류 — product_detail 9개 카테고리를 3개 그룹으로 통합(룸온리=룸온니/프로모션, 연박=연박/투나잇, 패키지=조식패키지·올인클루시브·시즌패키지·워터풀/오션·세일/기획전·액티비티/레저). 분류 미상(원천 '기타')은 제외.<br>
※ 사업장별 실적은 db_aggregated 투숙월(stay-month) 기준 순예약(net) 집계입니다.</p>
</section>''')

    # ── 푸터 ──
    H.append(f'''<section class="section" style="margin-bottom:8px">
<p style="font-size:11px;color:var(--ink-faint);line-height:1.8;border-top:1px solid var(--rule);padding-top:14px">
데이터 출처 — db_aggregated.json(예약 일별 집계) · rm_fcst.json(RM Budget) · campaign_data.json / campaign_performance.json(기획전) · enriched_notes.json(자동 인사이트).
RN·매출은 OTA·G-OTA·Inbound 3개 세그먼트 합산 기준이며, 금주는 {mm}/{dd}~{ee_m}/{ee_d}({wd(wt['this_week']['start'])}~{wd(wt['this_week']['end'])}) {days}일 — 06/11(목)~06/12(금)는 예약데이터 수집 지연으로 미반영.
자동 생성: 주간리포트 스케줄 작업(매주 토 10:00).</p></section>''')

    block = "const WEEKLY_REPORT_HTML = `\n" + "\n".join(H) + "\n`;"

    # ── 주입 ──
    html_path = DOCS / "gs-closing-report.html"
    html = html_path.read_text(encoding="utf-8")
    # 기존 함수(wkToggle, buildWeeklyTab)는 보존 — const 블록만 직접 슬라이스 교체
    si = html.index("const WEEKLY_REPORT_HTML = `")
    ei = html.index("`;", si) + 2
    new_html = html[:si] + block + html[ei:]
    html_path.write_text(new_html, encoding="utf-8")

    print(f"✓ 주입 완료: 사업장 {len(s1_rows)} / 세그 {len(seg_rows)} / 채널 {len(chans)} / 인사이트 {len(ins)} / 금주캠페인 {len(this_camps)} / 차주캠페인 {len(next_camps)} / 섹션6 {len(s6)}")
    print(f"  금주 RN={fmt(a_rn)} 매출={fmt(a_rev*1000)}천원 달성률={ach_rn:.1f}% YoY={yoy_rn:.1f}% WoW={wow_rn:.1f}%")
    print(f"  세그 OTA/GOTA/IB net_rn: {fmt(seg_a_rn['OTA'])}/{fmt(seg_a_rn['G-OTA'])}/{fmt(seg_a_rn['Inbound'])} = {fmt(total_a_rn)}")
    print(f"  사업장합 ta_rn={fmt(ta_rn)} (세그합 {fmt(total_a_rn)} 일치={'OK' if round(ta_rn)==round(total_a_rn) else 'MISMATCH'})")
    print(f"  섹션6 합계 RN={fmt(s6_tot_rn)} 매출={fmt(s6_tot_rev*1000)}천원 / 카테고리합={fmt(pcat_tot)}")


if __name__ == "__main__":
    main()
