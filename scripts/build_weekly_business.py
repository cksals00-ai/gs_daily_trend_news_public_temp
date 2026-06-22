#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
세일즈마케팅 주간업무 PDF → 마감 보고서 '주간 리포트' 탭 시각화 섹션 생성/주입.

· 입력: '…주간업무….pdf' (기본: data/weekly report/ 드롭 폴더에서 최신본 자동 선택, 경로 인자도 가능)
· 파싱: 세그먼트+사업장 표를 가진 '당월'·'누계' 페이지를 자동 탐지(find_segprop_pages) 후
        텍스트 라인 단위로 OCC·RNs·ADR·객실매출(백만) 추출 — 페이지 인덱스 하드코딩 안 함.
· 출력: docs/gs-closing-report.html 의 WEEKLY_BIZ_INJECT 마커 사이에 시각화 HTML 주입
        (WEEKLY_REPORT_INJECT 마커 밖이라 주간리포트 에이전트 재작성과 독립)

매주:  python3 scripts/build_weekly_business.py   (드롭 폴더 최신 PDF 자동 사용)
레이아웃이 바뀐 주에는 파싱 경고(warnings)를 내므로 확인 후 파서 보정.
"""
import sys, os, re, json, glob, argparse, unicodedata

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HTML_PATH = os.path.join(ROOT, "docs", "gs-closing-report.html")
PDF_DIR = os.path.join(ROOT, "docs", "data", "weekly_pdf")
# 사용자 표준 드롭 폴더: 매주 주간업무/시장·트랜드 PDF를 여기에 둠(.gitignore 처리 → 데몬 안전).
DROP_DIR = os.path.join(ROOT, "data", "weekly report")


def find_latest_pdf():
    """드롭 폴더(우선)·legacy weekly_pdf에서 가장 최근 '주간업무' PDF를 mtime 기준 자동 선택."""
    cands = []
    for d in (DROP_DIR, PDF_DIR):
        if not os.path.isdir(d):
            continue
        for p in glob.glob(os.path.join(d, "*.pdf")):
            # macOS는 파일명을 NFD로 저장 → NFC 정규화 후 부분일치 비교(한글 매칭 보장)
            if "주간업무" in unicodedata.normalize("NFC", os.path.basename(p)):
                cands.append(p)
    if not cands:
        return None
    return max(cands, key=os.path.getmtime)

NUM = r'-?[\d,]+(?:\.\d+)?'  # 숫자(콤마/소수/음수)

def _f(tok):
    """'214,269실'·'745억'·'-7%p'·'88%' → float (단위 제거)."""
    if tok is None: return None
    t = tok.replace(',', '').replace('실', '').replace('억', '').replace('구좌', '')
    t = t.replace('%p', '').replace('%', '').replace('(', '-').replace(')', '')
    t = t.strip()
    try: return float(t)
    except ValueError: return None

def _ints(line):
    """라인에서 토큰 단위로 끊어 원문 토큰 리스트 반환(단위 포함)."""
    return line.split()


# ── 세그먼트/사업장 페이지 탐지 ───────────────────────────────────────
# '세일즈마케팅 주간업무' PDF는 객실영업(OCC·RNs·ADR·객실매출) 중심 — 동일 레이아웃의
# 세그먼트+사업장 표가 '당월' 페이지와 '누계' 페이지에 한 번씩 등장(문서 순서: 당월→누계).
# 각 행은 텍스트 추출이 라인 단위로 깨끗해 표 구조 대신 텍스트 파싱이 견고함.
_NUMTOK = re.compile(r'^-?[\d,]+(?:\.\d+)?(?:%p|%)?$')

def _nums_after(toks, start):
    return [t for t in toks[start:] if _NUMTOK.match(t)]

def find_segprop_pages(pages):
    """세그먼트+사업장 표를 가진 페이지 인덱스(0-based)를 문서 순서로 반환 → [당월, 누계]."""
    hits = []
    for i, pg in enumerate(pages):
        t = pg.extract_text() or ""
        if ("Segment" in t and "Property" in t and "점유비" in t
                and "소계" in t and "합계" in t):
            hits.append(i)
    return hits


def parse_totals(text):
    """사업장 표 맨 끝 '합계' 라인 → OCC/RNs/ADR/Rev 각 전년·목표·실적·증감·달성률.
    페이지엔 세그먼트 합계(점유비 100%…)·사업장 합계(OCC%…) 둘 다 있어 '마지막' 합계(=사업장)를 취함."""
    keys = ["py", "bud", "act", "diff", "ach"]
    last = None
    for ln in text.splitlines():
        toks = ln.split()
        if toks and toks[0] == "합계":
            nums = _nums_after(toks, 1)
            if len(nums) >= 20:
                last = nums
    if not last:
        return None
    f = [_f(x) for x in last[:20]]
    return {
        "occ": dict(zip(keys, f[0:5])),  "rns": dict(zip(keys, f[5:10])),
        "adr": dict(zip(keys, f[10:15])), "rev": dict(zip(keys, f[15:20])),
    }


# ── 사업장별 (권역 → 사업장: OCC/RNs/ADR/Rev × 전년·목표·실적·증감·달성률)
REGIONS = {
    "아시아퍼시픽": ["고양","소노캄제주","소노벨제주"],
    "비발디":       ["소노캄","소노펫","소노펠리체","소노빌리지","양평"],
    "한국중부":     ["델피노","양양","삼척","천안","단양","르네블루"],
    "한국남부":     ["경주","거제","여수","남해","진도","변산","청송","해운대"],
}
ALL_PROPS = [p for ps in REGIONS.values() for p in ps]
PROP_REGION = {p: r for r, ps in REGIONS.items() for p in ps}

def parse_properties(text):
    """사업장 표 텍스트 → {사업장명: {occ/rns/adr/rev}}. 각 라인: [권역접두] 사업장명 + 20수치."""
    keys = ["py", "bud", "act", "diff", "ach"]
    out = {}
    for ln in text.splitlines():
        toks = ln.split()
        ni = next((i for i, t in enumerate(toks) if t in ALL_PROPS), None)
        if ni is None:
            continue
        name = toks[ni]
        nums = _nums_after(toks, ni + 1)
        if len(nums) < 20:
            continue
        f = [_f(x) for x in nums[:20]]
        out[name] = {
            "region": PROP_REGION[name],
            "occ": dict(zip(keys, f[0:5])),  "rns": dict(zip(keys, f[5:10])),
            "adr": dict(zip(keys, f[10:15])), "rev": dict(zip(keys, f[15:20])),
        }
    return out

SEGMENTS = {
    "회원": ["기명","무기명","Staff","Other"],
    "단체": ["G-Mice","E-Mice","Inbound"],
    "FIT":  ["Hompage","OTA","G-OTA","Affiliate","Other"],
}

def parse_segments(text):
    """세그먼트 소계행(회원/단체/FIT) → 점유비·RNs(전년/목표/실적/증감/달성률).
    각 소계행 = [점유비5, RNs5, ADR5, Rev5]. 세그먼트 표가 사업장 표보다 먼저 나와
    처음 3개 '소계'(회원→단체→FIT)를 취함(이후 소계는 권역 소계라 제외)."""
    res = []
    for ln in text.splitlines():
        toks = ln.split()
        if not toks or toks[0] != "소계":
            continue
        nums = _nums_after(toks, 1)
        if len(nums) < 10:
            continue
        f = [_f(x) for x in nums[:20]]
        res.append({
            "share_py": f[0], "share_bud": f[1], "share_act": f[2],
            "rns_py": f[5], "rns_bud": f[6], "rns_act": f[7],
            "rns_diff": f[8], "rns_ach": f[9],
        })
    names = list(SEGMENTS.keys())  # 회원, 단체, FIT
    return [{"name": names[i], **r} for i, r in enumerate(res[:3])]


# ── P7: 해외 실적 (매출 백만원 요약행) ────────────────────────────────
P7_ROWS = ["미주합계", "하와이계", "망길라오계", "탈로포포계", "하이퐁합계"]
P7_INDENT = {"하와이계", "망길라오계", "탈로포포계"}  # 미주합계 하위

def parse_p7_overseas(text):
    numtok = re.compile(rf'^{NUM}(억|%p|%)?$')
    out = []
    for ln in text.splitlines():
        ln = ln.strip()
        toks = ln.split()
        if not toks or toks[0] not in P7_ROWS:
            continue
        label = toks[0]
        vals = [t for t in toks[1:] if numtok.match(t)]
        if len(vals) < 14:
            continue
        f = [_f(v) for v in vals[:14]]
        out.append({
            "label": label, "indent": label in P7_INDENT,
            "m": {"budget": f[0], "actual": f[1], "ach": f[3], "yoy_pct": f[6]},
            "c": {"budget": f[7], "actual": f[8], "ach": f[10], "yoy_pct": f[13]},
        })
    return out


# ── P8~24: 부서·사업장별 정성 주간업무 (구분 / 전주 주요사항 / 금주 계획) ──
QUAL_PAGES = range(8, 25)  # 1-based, P8~P24

def _clean(s):
    return re.sub(r'\s+', ' ', (s or '').replace('\n', ' ')).strip()

def parse_qualitative(pages):
    items = []
    for pi in QUAL_PAGES:
        if pi > len(pages):
            break
        for tb in pages[pi - 1].extract_tables():
            if not tb or len(tb[0]) != 3:
                continue
            for r in tb:
                dept = _clean(r[0]); prev = _clean(r[1]); plan = _clean(r[2])
                if dept == "구분":
                    continue
                if not (prev or plan):
                    continue
                items.append({"page": pi, "dept": dept, "prev": prev, "plan": plan})
            break  # 페이지당 첫 3열 표만
    return items


def parse_pdf(pdf_path):
    import pdfplumber
    pdf = pdfplumber.open(pdf_path)
    pages = pdf.pages
    data = {"n_pages": len(pages), "warnings": [],
            "overall": {}, "property": {}, "segment": [], "segment_cumul": []}

    segprop = find_segprop_pages(pages)
    if not segprop:
        data["warnings"].append("세그먼트/사업장 페이지 미발견 — 레이아웃 변경 의심")
    else:
        cur_text = pages[segprop[0]].extract_text() or ""              # 당월
        cum_text = pages[segprop[1]].extract_text() if len(segprop) > 1 else ""  # 누계
        cum_text = cum_text or ""
        data["pages_used"] = {"당월": segprop[0] + 1,
                              "누계": (segprop[1] + 1 if len(segprop) > 1 else None)}

        tot_m = parse_totals(cur_text)
        tot_c = parse_totals(cum_text)
        data["overall"] = {"m": tot_m, "c": tot_c} if tot_m else {}
        if not tot_m:
            data["warnings"].append("당월 전사 합계 파싱 실패")
        if not tot_c:
            data["warnings"].append("누계 전사 합계 파싱 실패")

        data["property"] = parse_properties(cur_text)
        if len(data["property"]) < 15:
            data["warnings"].append(f"사업장 파싱 {len(data['property'])}개(예상 22개)")

        data["segment"] = parse_segments(cur_text)
        if len(data["segment"]) < 3:
            data["warnings"].append("당월 세그먼트 파싱 부족")
        data["segment_cumul"] = parse_segments(cum_text)
        if len(data["segment_cumul"]) < 3:
            data["warnings"].append("누계 세그먼트 파싱 부족")

    # 부서·사업장별 정성 주간업무 (best-effort — 레이아웃에 따라 일부만 추출될 수 있음)
    data["qualitative"] = parse_qualitative(pages)
    return data


# ── HTML 생성 ────────────────────────────────────────────────────────
def _eok(v):  return ("%s억" % f"{v:,.0f}") if v is not None else "–"
def _rns(v):  return ("%s실" % f"{v:,.0f}") if v is not None else "–"
def _seat(v): return ("%s구좌" % f"{v:,.0f}") if v is not None else "–"
def _mil(v):  return f"{v:,.0f}" if v is not None else "–"
def _pct(v, suffix="%"):
    if v is None: return "–"
    return ("%d%s" % (round(v), suffix)) if abs(v - round(v)) < 1e-9 else ("%.1f%s" % (v, suffix))
def _yoy(v, suffix="%"):
    if v is None: return "–"
    s = "+" if v > 0 else ""
    return s + _pct(v, suffix)

def _ach_tag(v):
    if v is None: return '<span class="kpi-tag tag-neutral">–</span>'
    cls = "tag-yoy-up" if v >= 100 else ("tag-yoy-flat" if v >= 90 else "tag-yoy-down")
    return '<span class="kpi-tag %s">%s</span>' % (cls, _pct(v))
def _yoy_tag(v, suffix="%"):
    if v is None: return '<span class="kpi-tag tag-neutral">–</span>'
    cls = "tag-yoy-up" if v > 0 else ("tag-yoy-down" if v < 0 else "tag-yoy-flat")
    return '<span class="kpi-tag %s">%s</span>' % (cls, _yoy(v, suffix))

def gen_html(data, week_label):
    ovr = data.get("overall", {})
    prop = data.get("property", {})
    seg = data.get("segment", [])
    parts = []

    # ── Section A: 전사 객실영업 요약 (OCC·RNs·ADR·객실매출) ──
    def _yoy(metric, d):
        py, act = d.get("py"), d.get("act")
        if py is None or act is None:
            return None
        if metric == "occ":
            return act - py                      # %p
        return (act / py - 1.0) * 100.0 if py else None

    m = ovr.get("m")
    c = ovr.get("c")
    if m:
        def kpi(label, val, sub, ach, yoy, yoy_suffix="%"):
            return (
                '<div class="kpi-card">'
                '<div class="kpi-label">%s</div>'
                '<div class="kpi-value">%s</div>'
                '<div class="kpi-keywords">%s %s</div>'
                '<div class="kpi-prev">%s</div>'
                '</div>' % (label, val, _ach_tag(ach), _yoy_tag(yoy, yoy_suffix), sub)
            )
        cards = ""
        cards += kpi("객실매출 (당월·백만)", _mil(m["rev"]["act"]), "목표 "+_mil(m["rev"]["bud"]), m["rev"]["ach"], _yoy("rev", m["rev"]))
        cards += kpi("운영 객실 RN (당월)",  _rns(m["rns"]["act"]), "목표 "+_rns(m["rns"]["bud"]), m["rns"]["ach"], _yoy("rns", m["rns"]))
        cards += kpi("운영 OCC (당월)",       _pct(m["occ"]["act"]), "목표 "+_pct(m["occ"]["bud"]), m["occ"]["ach"], _yoy("occ", m["occ"]), "%p")
        cards += kpi("ADR (당월·천원)",       _mil(m["adr"]["act"]), "목표 "+_mil(m["adr"]["bud"]), m["adr"]["ach"], _yoy("adr", m["adr"]))

        # 표 행: (라벨, 키, 포맷, 전년비 단위)
        rowdefs = [
            ("OCC",            "occ", _pct, "%p"),
            ("운영 객실 RN",   "rns", _rns, "%"),
            ("ADR (천원)",     "adr", _mil, "%"),
            ("객실매출 (백만)", "rev", _mil, "%"),
        ]
        body = ""
        for lbl, k, f, ys in rowdefs:
            dm = m.get(k, {})
            dc = (c or {}).get(k, {})
            body += (
                '<tr><td>%s</td>'
                '<td>%s</td><td><strong style="color:var(--ink)">%s</strong></td><td>%s</td><td>%s</td>'
                '<td>%s</td><td><strong style="color:var(--ink)">%s</strong></td><td>%s</td><td>%s</td>'
                '</tr>' % (
                    lbl,
                    f(dm.get("bud")), f(dm.get("act")), _ach_tag(dm.get("ach")), _yoy_tag(_yoy(k, dm), ys),
                    f(dc.get("bud")), f(dc.get("act")), _ach_tag(dc.get("ach")), _yoy_tag(_yoy(k, dc) if dc else None, ys),
                )
            )

        parts.append(
            '<section class="section" style="margin-bottom:24px">'
            '<span class="section-num">WEEKLY BUSINESS · 주간업무 기준</span>'
            '<h2 class="section-title"><span class="st-icon">🏢</span> 전사 객실영업 요약'
            '<span style="font-size:11px;font-weight:600;color:var(--ink-faint)"> · ' + week_label + ' · 전 사업장 합계</span></h2>'
            '<div class="kpi-grid">' + cards + '</div>'
            '<div class="table-wrap"><table class="full-table">'
            '<thead><tr><th rowspan="2">항목</th>'
            '<th class="group-header" colspan="4">당월 (예상)</th>'
            '<th class="group-header" colspan="4">누계</th></tr>'
            '<tr><th class="sub-header">목표</th><th class="sub-header">실적</th><th class="sub-header">달성률</th><th class="sub-header">전년비</th>'
            '<th class="sub-header">목표</th><th class="sub-header">실적</th><th class="sub-header">달성률</th><th class="sub-header">전년비</th></tr></thead>'
            '<tbody>' + body + '</tbody></table></div>'
            '<p style="font-size:11px;color:var(--ink-faint);margin-top:8px">※ 단위: 객실 RN=실, ADR=천원, 객실매출=백만원, OCC=%(전년비 %p). 전년비 = 실적 대비 전년 동기. 주간업무 보고서 전 사업장 합계(당월 예상·누계 실적) 기준.</p>'
            '</section>'
        )

    # ── Section B: 세그먼트 구성 (당월 + 누계) ──
    if seg:
        seg_colors = {"회원": "#c9a063", "단체": "#5a9fc4", "FIT": "#4ecdc4"}
        cumul = {s["name"]: s for s in data.get("segment_cumul", [])}
        total_rns = sum((s["rns_act"] or 0) for s in seg) or 1
        bar = ""
        for s in seg:
            w = (s["rns_act"] or 0) / total_rns * 100
            bar += '<div style="width:%.1f%%;background:%s;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;color:#0d0d0f" title="%s">%s</div>' % (
                w, seg_colors.get(s["name"], "#888"), s["name"], s["name"] if w > 8 else "")
        rows = ""
        for s in seg:
            c = cumul.get(s["name"], {})
            rows += (
                '<tr><td style="font-weight:700;color:%s">%s</td>'
                '<td><strong style="color:var(--ink)">%s</strong></td><td>%s</td><td>%s</td>'
                '<td><strong style="color:var(--ink)">%s</strong></td><td>%s</td><td>%s</td></tr>' % (
                    seg_colors.get(s["name"], "var(--ink)"), s["name"],
                    _rns(s["rns_act"]), _pct(s["share_act"]), _ach_tag(s["rns_ach"]),
                    _rns(c.get("rns_act")), _pct(c.get("share_act")), _ach_tag(c.get("rns_ach")))
            )
        parts.append(
            '<section class="section" style="margin-bottom:24px">'
            '<span class="section-num">SEGMENT</span>'
            '<h2 class="section-title"><span class="st-icon">🧩</span> 세그먼트 구성 (객실 RN)</h2>'
            '<div style="font-size:11px;color:var(--ink-muted);margin-bottom:8px">당월 점유비</div>'
            '<div style="display:flex;height:30px;border-radius:6px;overflow:hidden;margin-bottom:14px">' + bar + '</div>'
            '<div class="table-wrap"><table class="full-table">'
            '<thead><tr><th rowspan="2">세그먼트</th><th class="group-header" colspan="3">당월</th><th class="group-header" colspan="3">누계</th></tr>'
            '<tr><th class="sub-header">실적 RN</th><th class="sub-header">점유비</th><th class="sub-header">달성률</th>'
            '<th class="sub-header">실적 RN</th><th class="sub-header">점유비</th><th class="sub-header">달성률</th></tr></thead>'
            '<tbody>' + rows + '</tbody></table></div>'
            '<p style="font-size:11px;color:var(--ink-faint);margin-top:8px">※ 회원(기명·무기명·Staff·기타) / 단체(G·E-Mice·Inbound) / FIT(홈페이지·OTA·G-OTA·제휴·기타) 소계 기준.</p>'
            '</section>'
        )

    # ── Section C: 사업장별 실적 (당월) ──
    if prop:
        rows = ""
        for region, plist in REGIONS.items():
            members = [p for p in plist if p in prop]
            if not members: continue
            rows += '<tr style="background:var(--bg-soft,rgba(255,255,255,.03))"><td colspan="6" style="font-weight:700;color:var(--gold-bright);font-size:11.5px">%s</td></tr>' % region
            for p in members:
                d = prop[p]
                rows += (
                    '<tr><td>%s</td>'
                    '<td>%s</td>'
                    '<td><strong style="color:var(--ink)">%s</strong></td>'
                    '<td>%s</td>'
                    '<td>%s</td>'
                    '<td>%s</td></tr>' % (
                        p, _pct(d["occ"]["act"]), _rns(d["rns"]["act"]),
                        _ach_tag(d["rns"]["ach"]), _mil(d["rev"]["act"]),
                        _yoy_tag(((d["rns"]["act"]/d["rns"]["py"]-1)*100) if d["rns"]["py"] else None),
                    )
                )
        parts.append(
            '<section class="section" style="margin-bottom:24px">'
            '<span class="section-num">PROPERTY</span>'
            '<h2 class="section-title"><span class="st-icon">🏨</span> 사업장별 실적 (당월)'
            '<span style="font-size:11px;font-weight:600;color:var(--ink-faint)"> · 권역별 · 객실 기준</span></h2>'
            '<div class="table-wrap"><table class="full-table">'
            '<thead><tr><th>사업장</th><th>OCC</th><th>RN 실적</th><th>달성률</th><th>객실매출(백만)</th><th>전년비 RN</th></tr></thead>'
            '<tbody>' + rows + '</tbody></table></div>'
            '<p style="font-size:11px;color:var(--ink-faint);margin-top:8px">※ 당월 예상실적 기준. 객실매출 단위=백만원. 전년비 RN = (당월 실적 RN / 전년 동월 RN − 1).</p>'
            '</section>'
        )

    # ── Section D: 해외 실적 (미주·괌·하이퐁) ──
    ovs = data.get("overseas", [])
    if ovs:
        rows = ""
        for o in ovs:
            name = ("&nbsp;&nbsp;└ " + o["label"]) if o["indent"] else o["label"]
            wt = "400" if o["indent"] else "700"
            rows += (
                '<tr><td style="font-weight:%s;%s">%s</td>'
                '<td>%s</td><td><strong style="color:var(--ink)">%s</strong></td><td>%s</td><td>%s</td>'
                '<td>%s</td><td><strong style="color:var(--ink)">%s</strong></td><td>%s</td><td>%s</td></tr>' % (
                    wt, ("" if o["indent"] else "color:var(--gold-bright)"), name,
                    _mil(o["m"]["budget"]), _mil(o["m"]["actual"]), _ach_tag(o["m"]["ach"]), _yoy_tag(o["m"]["yoy_pct"]),
                    _mil(o["c"]["budget"]), _mil(o["c"]["actual"]), _ach_tag(o["c"]["ach"]), _yoy_tag(o["c"]["yoy_pct"]))
            )
        parts.append(
            '<section class="section" style="margin-bottom:24px">'
            '<span class="section-num">OVERSEAS</span>'
            '<h2 class="section-title"><span class="st-icon">🌏</span> 해외 실적'
            '<span style="font-size:11px;font-weight:600;color:var(--ink-faint)"> · 매출 백만원 · 미주(하와이·괌)·하이퐁</span></h2>'
            '<div class="table-wrap"><table class="full-table">'
            '<thead><tr><th rowspan="2">거점</th><th class="group-header" colspan="4">당월 예상</th><th class="group-header" colspan="4">누계</th></tr>'
            '<tr><th class="sub-header">목표</th><th class="sub-header">실적</th><th class="sub-header">달성률</th><th class="sub-header">전년비</th>'
            '<th class="sub-header">목표</th><th class="sub-header">실적</th><th class="sub-header">달성률</th><th class="sub-header">전년비</th></tr></thead>'
            '<tbody>' + rows + '</tbody></table></div>'
            '<p style="font-size:11px;color:var(--ink-faint);margin-top:8px">※ 매출 합계(객실+식음+골프 등) 단위=백만원. 미주합계 = 하와이+망길라오+탈로포포. 환율 06.11 매매기준율 적용.</p>'
            '</section>'
        )

    # ── Section E: 부서·사업장별 주간업무 (정성, 전주 실적 / 금주 계획) ──
    qual = data.get("qualitative", [])
    if qual:
        def _bullets(txt):
            if not txt: return '<span style="color:var(--ink-faint)">–</span>'
            t = re.sub(r'\s+(?=\d+\.\s)', '\n', txt)            # " 1. " 앞 줄바꿈
            t = re.sub(r'(?=[①-⑩·])', '\n', t)                  # 불릿/원숫자 앞 줄바꿈
            items = [x.strip(' ·').strip() for x in t.split('\n') if x.strip(' ·').strip()]
            if not items: return _h(txt)
            return '<ul style="margin:0;padding-left:16px;line-height:1.7">' + \
                   "".join('<li>%s</li>' % _h(x) for x in items) + '</ul>'
        cards = ""
        for q in qual:
            cards += (
                '<div style="border:1px solid var(--rule,rgba(255,255,255,.08));border-radius:9px;padding:13px 15px;margin-bottom:10px">'
                '<div style="font-weight:700;color:var(--gold-bright);font-size:13px;margin-bottom:9px">%s</div>'
                '<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;font-size:12px;color:var(--ink-soft)">'
                '<div><div style="font-size:10.5px;font-weight:700;color:var(--ink-muted);letter-spacing:.04em;margin-bottom:5px">전주 주요사항</div>%s</div>'
                '<div><div style="font-size:10.5px;font-weight:700;color:var(--ink-muted);letter-spacing:.04em;margin-bottom:5px">금주 계획</div>%s</div>'
                '</div></div>' % (_h(q["dept"]), _bullets(q["prev"]), _bullets(q["plan"]))
            )
        parts.append(
            '<section class="section" style="margin-bottom:24px">'
            '<span class="section-num">WEEKLY TASKS</span>'
            '<h2 class="section-title"><span class="st-icon">🗂️</span> 부서·사업장별 주간업무'
            '<span style="font-size:11px;font-weight:600;color:var(--ink-faint)"> · 전주 실적 / 금주 계획 · ' + str(len(qual)) + '건</span></h2>'
            '<details><summary style="cursor:pointer;color:var(--gold-bright);font-size:12.5px;font-weight:700;margin-bottom:12px">▸ 펼쳐보기 (' + str(len(qual)) + '개 부서·사업장)</summary>'
            '<div style="margin-top:12px">' + cards + '</div></details>'
            '<p style="font-size:11px;color:var(--ink-faint);margin-top:8px">※ 주간업무 보고서 원문 자동 추출 — 일부 항목은 정렬이 다를 수 있음.</p>'
            '</section>'
        )

    return "\n".join(parts)


def _h(s):
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _esc_tpl(s):
    """JS 템플릿 리터럴 안전화: 백슬래시·백틱·${ 이스케이프."""
    return s.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")

def inject_html(html_path, viz_html):
    with open(html_path, encoding="utf-8") as f:
        html = f.read()
    start = "/* WEEKLY_BIZ_INJECT_START"
    end = "/* WEEKLY_BIZ_INJECT_END */"
    si = html.find(start); ei = html.find(end)
    if si < 0 or ei < 0 or ei < si:
        raise SystemExit("[ERR] WEEKLY_BIZ_INJECT 마커를 찾을 수 없음")
    # start 주석 라인 끝까지 보존
    si_line_end = html.find("\n", si)
    new_block = (
        "/* WEEKLY_BIZ_INJECT_START — auto-filled by scripts/build_weekly_business.py. Do not hand-edit. */\n"
        "const WEEKLY_BIZ_HTML = `" + _esc_tpl(viz_html) + "`;\n"
    )
    new_html = html[:si] + new_block + html[ei:]
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(new_html)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf", nargs="?", help="주간업무 PDF 경로")
    ap.add_argument("--dump", action="store_true", help="파싱 결과 JSON만 출력")
    args = ap.parse_args()

    pdf_path = args.pdf or find_latest_pdf()
    if not pdf_path or not os.path.exists(pdf_path):
        print(f"[ERR] 주간업무 PDF 없음 — '{DROP_DIR}'에 '…주간업무….pdf'를 두거나 경로 인자 지정.",
              file=sys.stderr); sys.exit(2)

    data = parse_pdf(pdf_path)
    if args.dump:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        sys.exit(0)

    # 주차 라벨: 파일명의 (N월M주) + 날짜(YYYYMMDD 또는 YYMMDD)
    base = unicodedata.normalize("NFC", os.path.basename(pdf_path))  # macOS NFD → NFC
    wm = re.search(r'(\d+월\s*\d+주)', base)
    dm = re.search(r'(20\d{6})', base) or re.search(r'\b(\d{6})\b', base)
    label_bits = []
    if wm: label_bits.append(wm.group(1).replace(" ", "") + "차")
    if dm:
        d = dm.group(1)
        if len(d) == 6: d = "20" + d            # YYMMDD → YYYYMMDD
        label_bits.append(f"{d[:4]}.{d[4:6]}.{d[6:8]} 기준")
    week_label = " · ".join(label_bits) if label_bits else "주간업무"

    if not data.get("overall"):
        print(f"[ERR] 전사 객실영업 합계 파싱 실패 — 레이아웃 변경 의심. PDF: {base}", file=sys.stderr)
        sys.exit(3)

    viz = gen_html(data, week_label)
    inject_html(HTML_PATH, viz)
    print(json.dumps({
        "ok": True, "pdf": base, "week_label": week_label,
        "n_pages": data["n_pages"], "pages_used": data.get("pages_used"),
        "warnings": data["warnings"],
        "n_property": len(data.get("property", {})),
        "n_segment": len(data.get("segment", [])),
        "n_segment_cumul": len(data.get("segment_cumul", [])),
        "n_qualitative": len(data.get("qualitative", [])),
        "viz_bytes": len(viz),
    }, ensure_ascii=False))
