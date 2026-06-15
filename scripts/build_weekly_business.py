#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주간업무(소노호텔앤리조트) PDF → 마감 보고서 '주간 리포트' 탭 시각화 섹션 생성/주입.

· 입력: 주간업무 PDF (기본 docs/data/weekly_pdf/주간업무_YYYY-MM-DD.pdf, 인자로도 지정 가능)
· 파싱: P2 전사 실적 요약 / P5 당월 세그먼트·사업장별 (pdfplumber)
· 출력: docs/gs-closing-report.html 의 WEEKLY_BIZ_INJECT 마커 사이에 시각화 HTML 주입
        (WEEKLY_REPORT_INJECT 마커 밖이라 주간리포트 에이전트 재작성과 독립)

매주 스케줄 에이전트가:  python3 scripts/build_weekly_business.py <PDF경로>  실행 후 결과 확인.
레이아웃이 바뀐 주에는 파싱 경고를 내고 에이전트가 보정.
"""
import sys, os, re, json, glob, argparse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HTML_PATH = os.path.join(ROOT, "docs", "gs-closing-report.html")
PDF_DIR = os.path.join(ROOT, "docs", "data", "weekly_pdf")

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


# ── P2: 전사 실적 요약 ────────────────────────────────────────────────
# 각 지표행 = 라벨 + 14토큰 [당월: 목표,실적,증감,달성률,25실적,25증감,25증감률]
#                          [누계: 목표,실적,달성차액,달성률,25실적,25증감,25증감률]
P2_ROWS = [
    ("oper", "occ",    "OCC"),
    ("oper", "rns",    "RNs"),
    ("oper", "rev",    "매출"),
    ("oper", "profit", "영업이익"),
    ("sale", "seat",   "구좌"),
    ("sale", "fee",    "입회금"),
    ("sale", "rev",    "매출"),
    ("sale", "profit", "영업이익"),
    ("tot",  "rev",    "매출"),
    ("tot",  "profit", "영업이익"),
]

def parse_p2(text):
    """P2 텍스트 → 전사 요약 dict."""
    # 라벨 사이 공백 제거: 'O C C'→'OCC', 'R N s'→'RNs', '매 출'→'매출' 등
    t = text
    for a, b in [("O C C","OCC"),("R N s","RNs"),("매 출","매출"),
                 ("영 업 이 익","영업이익"),("분 양","분양"),("구 좌","구좌"),
                 ("입 회 금","입회금"),("구 분","구분")]:
        t = t.replace(a, b)
    lines = [l.strip() for l in t.splitlines() if l.strip()]
    res = {}
    numtok = re.compile(rf'^{NUM}(실|억|구좌|%p|%)?$')
    # 행을 순서대로 매칭 (운영4 → 분양4 → 합계2). 라벨 앞 접두어(분양/합계)는 허용 —
    # 라벨 토큰 위치를 찾아 그 "뒤" 숫자 토큰 14개를 취함.
    seq = list(P2_ROWS)
    si = 0
    for ln in lines:
        if si >= len(seq): break
        grp, key, label = seq[si]
        toks = ln.split()
        if label not in toks:
            continue
        idx = len(toks) - 1 - toks[::-1].index(label)  # 라벨의 마지막 출현 위치
        vals = [tok for tok in toks[idx+1:] if numtok.match(tok)]
        if len(vals) < 14:
            continue
        f = [_f(v) for v in vals[:14]]
        res.setdefault(grp, {})[key] = {
            "label": label,
            "m": {"budget": f[0], "actual": f[1], "diff": f[2], "ach": f[3],
                  "yoy_actual": f[4], "yoy_diff": f[5], "yoy_pct": f[6]},
            "c": {"budget": f[7], "actual": f[8], "diff": f[9], "ach": f[10],
                  "yoy_actual": f[11], "yoy_diff": f[12], "yoy_pct": f[13]},
            "raw": vals[:14],
        }
        si += 1
    return res


# ── P5: 당월 사업장별 (권역 → 사업장: OCC/RNs/ADR/Rev × 전년·목표·실적·증감·달성률)
REGIONS = {
    "아시아퍼시픽": ["고양","소노캄제주","소노벨제주"],
    "비발디":       ["소노캄","소노펫","소노펠리체","소노빌리지","양평"],
    "한국중부":     ["델피노","양양","삼척","천안","단양","르네블루"],
    "한국남부":     ["경주","거제","여수","남해","진도","변산","청송","해운대"],
}
ALL_PROPS = [p for ps in REGIONS.values() for p in ps]
PROP_REGION = {p: r for r, ps in REGIONS.items() for p in ps}

def parse_p5_property(table_rows):
    """extract_tables()의 구조화 행에서 사업장행 파싱. 각 행: name + 20수치."""
    out = {}
    for r in table_rows:
        cells = [(c or '').replace('\n',' ').strip() for c in r]
        if len(cells) < 2: continue
        name = cells[1].strip()
        if name not in ALL_PROPS: continue
        nums = [_f(c) for c in cells[2:] if c.strip() != '']
        if len(nums) < 20: continue
        occ, rns, adr, rev = nums[0:5], nums[5:10], nums[10:15], nums[15:20]
        out[name] = {
            "region": PROP_REGION[name],
            "occ":  dict(zip(["py","bud","act","diff","ach"], occ)),
            "rns":  dict(zip(["py","bud","act","diff","ach"], rns)),
            "adr":  dict(zip(["py","bud","act","diff","ach"], adr)),
            "rev":  dict(zip(["py","bud","act","diff","ach"], rev)),
        }
    return out

SEGMENTS = {
    "회원": ["기명","무기명","Staff","Other"],
    "단체": ["G-Mice","E-Mice","Inbound"],
    "FIT":  ["Hompage","OTA","G-OTA","Affiliate","Other"],
}

def parse_p5_segment_subtotals(text):
    """세그먼트 소계행(회원/단체/FIT 소계)의 점유비·RNs·달성률 파싱."""
    # 소계 행: '소계 43% 44% 42% -2%p 96% 76,427 95,165 79,295 -15,870 83% 176 ...'
    res = []
    for m in re.finditer(
        r'소계\s+(\d+)%\s+(\d+)%\s+(\d+)%\s+(-?\d+)%p\s+(\d+)%\s+'
        r'([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+(-?[\d,]+)\s+(\d+)%', text):
        res.append({
            "share_py": _f(m.group(1)), "share_bud": _f(m.group(2)), "share_act": _f(m.group(3)),
            "rns_py": _f(m.group(6)), "rns_bud": _f(m.group(7)),
            "rns_act": _f(m.group(8)), "rns_diff": _f(m.group(9)), "rns_ach": _f(m.group(10)),
        })
    # 순서: 회원, 단체, FIT (등장 순)
    names = list(SEGMENTS.keys())
    return [{"name": names[i], **r} for i, r in enumerate(res[:3])]


def parse_pdf(pdf_path):
    import pdfplumber
    pdf = pdfplumber.open(pdf_path)
    pages = pdf.pages
    data = {"n_pages": len(pages), "warnings": []}

    # P2
    p2_text = pages[1].extract_text() if len(pages) > 1 else ""
    data["overall"] = parse_p2(p2_text or "")
    if "tot" not in data["overall"]:
        data["warnings"].append("P2 전사 합계 파싱 실패")

    # P5 (당월) 사업장 + 세그먼트
    if len(pages) > 4:
        p5 = pages[4]
        tbls = p5.extract_tables()
        prop = {}
        for tb in tbls:
            prop.update(parse_p5_property(tb))
        data["property"] = prop
        if len(prop) < 15:
            data["warnings"].append(f"P5 사업장 파싱 {len(prop)}개(예상 22개)")
        data["segment"] = parse_p5_segment_subtotals(p5.extract_text() or "")
        if len(data["segment"]) < 3:
            data["warnings"].append("P5 세그먼트 소계 파싱 부족")
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

_FMT = {"pct": _pct, "rns": _rns, "eok": _eok, "seat": _seat}

# 전사 표 행 정의: (그룹표시, 키, 단위, yoy단위)
_OVR_ROWS = [
    ("운영", "oper", "occ",    "pct",  "%p"),
    ("운영", "oper", "rns",    "rns",  "%"),
    ("운영", "oper", "rev",    "eok",  "%"),
    ("운영", "oper", "profit", "eok",  "%"),
    ("분양", "sale", "seat",   "seat", "%"),
    ("분양", "sale", "fee",    "eok",  "%"),
    ("분양", "sale", "rev",    "eok",  "%"),
    ("분양", "sale", "profit", "eok",  "%"),
    ("합계", "tot",  "rev",    "eok",  "%"),
    ("합계", "tot",  "profit", "eok",  "%"),
]
_LABEL = {"occ":"OCC","rns":"객실 RN","rev":"매출","profit":"영업이익","seat":"분양구좌","fee":"입회금"}

def gen_html(data, week_label):
    ovr = data.get("overall", {})
    prop = data.get("property", {})
    seg = data.get("segment", [])
    parts = []

    # ── Section A: 전사 실적 요약 ──
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
    if "tot" in ovr:
        r = ovr["tot"]["rev"]["m"];  cards += kpi("합계 매출 (당월 예상)", _eok(r["actual"]), "목표 "+_eok(r["budget"]), r["ach"], r["yoy_pct"])
    if "oper" in ovr:
        r = ovr["oper"]["rns"]["m"]; cards += kpi("운영 객실 RN (당월)", _rns(r["actual"]), "목표 "+_rns(r["budget"]), r["ach"], r["yoy_pct"])
        o = ovr["oper"]["occ"]["m"]; cards += kpi("운영 OCC (당월)", _pct(o["actual"]), "목표 "+_pct(o["budget"]), o["ach"], o["yoy_pct"], "%p")
    if "tot" in ovr:
        p = ovr["tot"]["profit"]["m"]; cards += kpi("합계 영업이익 (당월)", _eok(p["actual"]), "목표 "+_eok(p["budget"]), p["ach"], p["yoy_pct"])

    body = ""
    last_grp = None
    for grp, gk, key, unit, ys in _OVR_ROWS:
        if gk not in ovr or key not in ovr[gk]: continue
        d = ovr[gk][key]; m = d["m"]; c = d["c"]; f = _FMT[unit]
        grp_cell = ('<td rowspan="0" style="font-weight:700;color:var(--gold-bright);vertical-align:top">%s</td>' % grp) if grp != last_grp else ""
        # rowspan 정확 계산은 생략하고 그룹 첫 행에만 라벨 표기
        grp_td = ('<td style="font-weight:700;color:var(--gold-bright)">%s</td>' % grp) if grp != last_grp else '<td></td>'
        last_grp = grp
        body += (
            '<tr>'
            + grp_td +
            '<td>%s</td>'
            '<td>%s</td><td><strong style="color:var(--ink)">%s</strong></td><td>%s</td><td>%s</td>'
            '<td>%s</td><td><strong style="color:var(--ink)">%s</strong></td><td>%s</td><td>%s</td>'
            '</tr>' % (
                _LABEL.get(key, key),
                f(m["budget"]), f(m["actual"]), _ach_tag(m["ach"]), _yoy_tag(m["yoy_pct"], ys),
                f(c["budget"]), f(c["actual"]), _ach_tag(c["ach"]), _yoy_tag(c["yoy_pct"], ys),
            )
        )

    parts.append(
        '<section class="section" style="margin-bottom:24px">'
        '<span class="section-num">WEEKLY BUSINESS · 주간업무 기준</span>'
        '<h2 class="section-title"><span class="st-icon">🏢</span> 전사 실적 요약'
        '<span style="font-size:11px;font-weight:600;color:var(--ink-faint)"> · ' + week_label + ' · 운영+분양</span></h2>'
        '<div class="kpi-grid">' + cards + '</div>'
        '<div class="table-wrap"><table class="full-table">'
        '<thead><tr><th rowspan="2">구분</th><th rowspan="2">항목</th>'
        '<th class="group-header" colspan="4">당월 (6월 예상)</th>'
        '<th class="group-header" colspan="4">누계 (1~6월)</th></tr>'
        '<tr><th class="sub-header">목표</th><th class="sub-header">실적</th><th class="sub-header">달성률</th><th class="sub-header">전년비</th>'
        '<th class="sub-header">목표</th><th class="sub-header">실적</th><th class="sub-header">달성률</th><th class="sub-header">전년비</th></tr></thead>'
        '<tbody>' + body + '</tbody></table></div>'
        '<p style="font-size:11px;color:var(--ink-faint);margin-top:8px">※ 단위: 매출·영업이익·입회금=억원, 객실 RN=실, OCC=%p(전년비). 주간업무 보고서(당월 예상실적·누계실적) 기준.</p>'
        '</section>'
    )

    # ── Section B: 세그먼트 구성 (당월) ──
    if seg:
        seg_colors = {"회원": "#c9a063", "단체": "#5a9fc4", "FIT": "#4ecdc4"}
        total_rns = sum((s["rns_act"] or 0) for s in seg) or 1
        bar = ""
        for s in seg:
            w = (s["rns_act"] or 0) / total_rns * 100
            bar += '<div style="width:%.1f%%;background:%s;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;color:#0d0d0f" title="%s">%s</div>' % (
                w, seg_colors.get(s["name"], "#888"), s["name"], s["name"] if w > 8 else "")
        rows = ""
        for s in seg:
            rows += (
                '<tr><td style="font-weight:700;color:%s">%s</td>'
                '<td><strong style="color:var(--ink)">%s</strong></td><td>%s</td><td>%s</td></tr>' % (
                    seg_colors.get(s["name"], "var(--ink)"), s["name"],
                    _rns(s["rns_act"]), _pct(s["share_act"]), _ach_tag(s["rns_ach"]))
            )
        parts.append(
            '<section class="section" style="margin-bottom:24px">'
            '<span class="section-num">SEGMENT</span>'
            '<h2 class="section-title"><span class="st-icon">🧩</span> 세그먼트 구성 (당월 객실 RN)</h2>'
            '<div style="display:flex;height:30px;border-radius:6px;overflow:hidden;margin-bottom:14px">' + bar + '</div>'
            '<div class="table-wrap"><table class="full-table">'
            '<thead><tr><th>세그먼트</th><th>실적 RN</th><th>점유비</th><th>달성률</th></tr></thead>'
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

    return "\n".join(parts)


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

    pdf_path = args.pdf
    if not pdf_path:
        cands = sorted(glob.glob(os.path.join(PDF_DIR, "주간업무_*.pdf")))
        pdf_path = cands[-1] if cands else None
    if not pdf_path or not os.path.exists(pdf_path):
        print(f"[ERR] PDF 없음: {pdf_path}", file=sys.stderr); sys.exit(2)

    data = parse_pdf(pdf_path)
    if args.dump:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        sys.exit(0)

    # 주차 라벨: 파일명의 (N월M주) + 날짜(YYYYMMDD)
    base = os.path.basename(pdf_path)
    wm = re.search(r'(\d+월\s*\d+주)', base)
    dm = re.search(r'(20\d{6})', base)
    label_bits = []
    if wm: label_bits.append(wm.group(1).replace(" ", "") + "차")
    if dm:
        d = dm.group(1); label_bits.append(f"{d[:4]}.{d[4:6]}.{d[6:8]} 기준")
    week_label = " · ".join(label_bits) if label_bits else "주간업무"

    if not data.get("overall"):
        print(f"[ERR] 전사 실적 파싱 실패 — 레이아웃 변경 의심. PDF: {base}", file=sys.stderr)
        sys.exit(3)

    viz = gen_html(data, week_label)
    inject_html(HTML_PATH, viz)
    print(json.dumps({
        "ok": True, "pdf": base, "week_label": week_label,
        "n_pages": data["n_pages"], "warnings": data["warnings"],
        "overall_keys": list(data.get("overall", {}).keys()),
        "n_property": len(data.get("property", {})),
        "n_segment": len(data.get("segment", [])),
        "viz_bytes": len(viz),
    }, ensure_ascii=False))
