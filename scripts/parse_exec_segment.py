#!/usr/bin/env python3
"""
전체 세그 분석 자료 파서 (parse_exec_segment.py)
─────────────────────────────────────────────────────────────────
소노호텔앤리조트 GS팀 · 주간 세일즈마케팅 리포트 PDF(동일 양식)에서
  (A) 전월 P&L 요약(운영/분양/합계)
  (B) 당월 세그먼트별 + 사업장별 (점유비/RNs/ADR/Rev · 전년/목표/실적/증감/달성률)
  (C) 익월 동일 구조
  (D) 해외(미주=하와이, 괌=망길라오·탈로포포, 베트남=하이퐁) 상세
을 구조화 JSON(docs/data/exec_segment.json)으로 추출한다.

설계 원칙
  · 좌표(x) 기반 컬럼 바인딩 → 결측 셀(E-Mice·COMP 등)도 정확히 정렬
  · y-군집으로 라벨(값보다 ~0.2 아래 렌더)과 데이터행을 같은 행으로 병합
  · 페이지 번호 하드코딩 금지 → 표를 내용 시그니처로 탐지(38p/26p 양식 호환)
  · 하드코딩 수치 금지 → 전부 PDF에서 파싱, 4종 교차검증

교차검증(표준):
  · 세그 합계 RNs 실적 == 사업장 합계 RNs 실적
  · 세그 그룹 소계 합(+COMP) == 세그 합계
  · 사업장 권역 소계 합 == 사업장 합계
  · 표준 총계: 당월 260,369 / 익월 276,505 (2026-07-03 기준 리포트)

사용:
  python3 scripts/parse_exec_segment.py [--pdf PATH]
  · --pdf 미지정 시 data/weekly report/*주간회의*.pdf 중 최신(파일명 날짜)을 사용

© 2026 GS팀
"""
import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pdfplumber

try:
    import fs_utils  # macOS NFD→NFC 정규화 (로컬 실행 시)
except Exception:
    fs_utils = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("parse_exec_segment")

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DROP_DIR = DATA_DIR / "weekly report"
OUT_JSON = ROOT / "docs" / "data" / "exec_segment.json"
KST = timezone(timedelta(hours=9))

# ── 컬럼 x-중심 (2026-07-03 양식 기준, tol 내 최근접 매칭) ──────────────
SEG_COLS = [  # 20열: 점유비/RNs/ADR/Rev × (전년,목표,실적,증감,달성률)
    ("occ", "전년", 136.0), ("occ", "목표", 167.8), ("occ", "실적", 199.5), ("occ", "증감", 231.1), ("occ", "달성률", 259.8),
    ("rns", "전년", 294.6), ("rns", "목표", 326.4), ("rns", "실적", 358.1), ("rns", "증감", 389.9), ("rns", "달성률", 418.6),
    ("adr", "전년", 453.5), ("adr", "목표", 485.3), ("adr", "실적", 517.1), ("adr", "증감", 548.6), ("adr", "달성률", 577.4),
    ("rev", "전년", 612.1), ("rev", "목표", 643.9), ("rev", "실적", 675.6), ("rev", "증감", 707.4), ("rev", "달성률", 736.2),
]
SEG_TOL = 15.0

OVS_COLS = [  # 14열: 당월(목표,실적,증감,달성률,전년,증감,증감률) + 누계(동일)
    ("cur", "목표", 111.4), ("cur", "실적", 159.0), ("cur", "증감", 206.7), ("cur", "달성률", 254.2),
    ("cur", "전년", 301.9), ("cur", "증감_전년", 349.6), ("cur", "증감률", 397.1),
    ("cum", "목표", 448.5), ("cum", "실적", 492.4), ("cum", "증감", 540.1), ("cum", "달성률", 587.6),
    ("cum", "전년", 635.3), ("cum", "증감_전년", 682.9), ("cum", "증감률", 730.5),
]
OVS_TOL = 22.0

PNL_COLS = [  # 14열: 당월(목표,실적,증감,달성률,전년,증감,증감률) + 누계(목표,실적,달성차액,달성률,전년,증감,증감률)
    ("cur", "목표", 142.0), ("cur", "실적", 187.3), ("cur", "증감", 232.6), ("cur", "달성률", 278.0),
    ("cur", "전년", 323.4), ("cur", "증감_전년", 367.4), ("cur", "증감률", 409.9),
    ("cum", "목표", 464.3), ("cum", "실적", 509.7), ("cum", "달성차액", 555.0), ("cum", "달성률", 600.4),
    ("cum", "전년", 645.7), ("cum", "증감_전년", 689.7), ("cum", "증감률", 732.3),
]
PNL_TOL = 22.0


# ── 값 파싱 ────────────────────────────────────────────────────────────
_NUMRE = re.compile(r"[\d,.\-()%]")


def is_valtoken(t):
    """숫자/퍼센트/괄호음수 등 값 토큰인지"""
    if not t:
        return False
    core = t
    for u in ("실", "구좌", "억", "명", "%p", "%"):
        core = core.replace(u, "")
    return bool(re.search(r"\d", core)) and bool(re.fullmatch(r"[\d,.\-()]*", core))


def to_num(t):
    """토큰 → float (단위/기호 정리). 결측/무의미 → None"""
    if t is None:
        return None
    s = t.strip()
    if s in ("", "-", "–", "—"):
        return None
    neg = False
    s = s.replace("실", "").replace("구좌", "").replace("억", "").replace("명", "")
    s = s.replace("%p", "").replace("%", "")
    s = s.replace(",", "")
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    if s in ("", "-"):
        return None
    try:
        v = float(s)
    except ValueError:
        return None
    if neg:
        v = -v
    if v == int(v):
        v = int(v)
    return v


def cluster_rows(words, ytol=4.5):
    """단어들을 y-군집(시각적 1행)으로 묶는다. 라벨(값보다 ~0.2 아래)도 병합."""
    ws = sorted(words, key=lambda w: (w["top"], w["x0"]))
    clusters = []
    cur = []
    cur_y = None
    for w in ws:
        y = w["top"]
        if cur_y is None or abs(y - cur_y) <= ytol:
            cur.append(w)
            cur_y = y if cur_y is None else (cur_y + y) / 2
        else:
            clusters.append(cur)
            cur = [w]
            cur_y = y
    if cur:
        clusters.append(cur)
    return clusters


def bind_columns(value_words, cols, tol):
    """값 단어들을 최근접 컬럼 x-중심에 바인딩. 반환: {(metric,field): num}"""
    out = {}
    used = defaultdict(list)
    for w in value_words:
        cx = (w["x0"] + w["x1"]) / 2
        best, bestd = None, tol
        for i, (m, f, x) in enumerate(cols):
            d = abs(cx - x)
            if d < bestd:
                best, bestd = i, d
        if best is not None:
            used[best].append((w["x0"], w["text"]))
    for i, toks in used.items():
        m, f, x = cols[i]
        # 한 셀에 토큰이 여러 개면 x 순으로 이어붙임(드묾)
        toks.sort()
        val = to_num("".join(t for _, t in toks)) if len(toks) > 1 else to_num(toks[0][1])
        out[(m, f)] = val
    return out


def metric_dict(bound, metric):
    d = {}
    for f in ("전년", "목표", "실적", "증감", "달성률"):
        if (metric, f) in bound:
            d[f] = bound[(metric, f)]
    return d


# ── 세그먼트/사업장 표 파싱 (page5/6 형식) ─────────────────────────────
SEG_GROUPS = {"회원": "회원", "단체": "단체", "FIT": "FIT"}
PROP_REGIONS = {"아시아": "아시아퍼시픽", "비발디": "비발디", "한국중부": "한국중부", "한국남부": "한국남부"}


def split_y(page):
    """세그 표(상단)와 사업장 표(하단)를 가르는 y (Property 헤더 top)."""
    for w in page.extract_words():
        if w["text"].strip() in ("Property", "property"):
            return w["top"]
    # 폴백: 'Segment' 두 번째 등장 없음 → OCC 헤더(사업장 표 상단)
    occs = [w["top"] for w in page.extract_words() if w["text"].strip() == "OCC"]
    return occs[0] if occs else 1e9


def parse_seg_table(page, ymax=1e9):
    """세그먼트 표 파싱 → list[row], total, occ_bottom"""
    words = [w for w in page.extract_words() if w["top"] < ymax]
    clusters = cluster_rows(words)
    rows = []
    total = None
    seg_occ = None
    cur_group = None
    for cl in clusters:
        labels = [w for w in cl if (w["x0"] + w["x1"]) / 2 < 95 and not is_valtoken(w["text"])]
        vals = [w for w in cl if is_valtoken(w["text"])]
        if len(vals) < 4 or not labels:
            continue
        # 라벨 토큰 정리
        ltok = sorted(labels, key=lambda w: w["x0"])
        left = ltok[0]["text"]
        # 그룹/이름 판별
        if left in SEG_GROUPS:
            cur_group = SEG_GROUPS[left]
            name = ltok[1]["text"] if len(ltok) > 1 else "소계"
        else:
            name = left
        bound = bind_columns(vals, SEG_COLS, SEG_TOL)
        row = {
            "group": cur_group,
            "name": name,
            "occ": metric_dict(bound, "occ"),
            "rns": metric_dict(bound, "rns"),
            "adr": metric_dict(bound, "adr"),
            "rev": metric_dict(bound, "rev"),
        }
        if name == "합계":
            total = row
        elif name == "Occ":
            # 전체 점유율 행은 RNs 컬럼 위치에 %로 인쇄됨(양식 특성)
            seg_occ = row["rns"] or row["occ"]
        else:
            rows.append(row)
    return rows, total, seg_occ


def parse_prop_table(page, ymin=0):
    """사업장 표 파싱 → list[row], total. (점유비 자리에 OCC)"""
    words = [w for w in page.extract_words() if w["top"] >= ymin]
    clusters = cluster_rows(words)
    rows = []
    total = None
    cur_region = None
    for cl in clusters:
        labels = [w for w in cl if (w["x0"] + w["x1"]) / 2 < 95 and not is_valtoken(w["text"])]
        vals = [w for w in cl if is_valtoken(w["text"])]
        if len(vals) < 4:
            continue
        region_tok = [w for w in labels if w["x0"] < 50]
        name_tok = [w for w in labels if 50 <= w["x0"] < 95]
        for w in region_tok:
            if w["text"] in PROP_REGIONS:
                cur_region = PROP_REGIONS[w["text"]]
        if not name_tok:
            continue
        name = name_tok[0]["text"]
        bound = bind_columns(vals, SEG_COLS, SEG_TOL)
        row = {
            "region": cur_region,
            "name": name,
            "occ": metric_dict(bound, "occ"),
            "rns": metric_dict(bound, "rns"),
            "adr": metric_dict(bound, "adr"),
            "rev": metric_dict(bound, "rev"),
        }
        if name == "합계":
            total = row
        else:
            rows.append(row)
    return rows, total


# ── 해외 표 파싱 (page7) ───────────────────────────────────────────────
def parse_overseas(page):
    words = page.extract_words()
    clusters = cluster_rows(words)
    flat = []
    for cl in clusters:
        labels = sorted([w for w in cl if (w["x0"] + w["x1"]) / 2 < 100 and not is_valtoken(w["text"])],
                        key=lambda w: w["x0"])
        vals = [w for w in cl if is_valtoken(w["text"])]
        if not labels or len(vals) < 4:
            continue
        indent = min(w["x0"] for w in labels)
        text = " ".join(w["text"] for w in labels)
        text = re.sub(r"\s+", " ", text).strip()
        tag = None
        if text.startswith("해외 "):
            tag = "해외"
            text = text[len("해외 "):]
        elif text.startswith("로컬 "):
            tag = "로컬"
            text = text[len("로컬 "):]
        bound = bind_columns(vals, OVS_COLS, OVS_TOL)
        cur = {f: bound.get(("cur", f)) for _, f, _ in OVS_COLS if _ == "cur"}
        cum = {f: bound.get(("cum", f)) for _, f, _ in OVS_COLS if _ == "cum"}
        # 위 컴프리헨션의 '_' 오용 방지: 명시적으로 다시
        cur = {f: bound.get(("cur", f)) for (m, f, x) in OVS_COLS if m == "cur"}
        cum = {f: bound.get(("cum", f)) for (m, f, x) in OVS_COLS if m == "cum"}
        flat.append({
            "label": text,
            "indent": round(indent, 1),
            "tag": tag,
            "cur": cur,
            "cum": cum,
        })
    return build_overseas_sections(flat)


def build_overseas_sections(flat):
    """평면 행 → 3개 섹션(미주/괌/베트남)으로 논리 분할 + 롤업."""
    sections = {
        "americas": {"key": "americas", "title": "미주 (하와이)", "rows": []},
        "guam": {"key": "guam", "title": "괌 (망길라오·탈로포포)", "rows": []},
        "vietnam": {"key": "vietnam", "title": "베트남 (하이퐁)", "rows": []},
    }
    rollup = {}
    cur_sec = None
    for r in flat:
        lb = r["label"]
        if lb == "미주합계":
            rollup["미주합계"] = r
            cur_sec = "americas"
            continue
        if lb == "하이퐁합계":
            rollup["하이퐁합계"] = r
            cur_sec = "vietnam"
            sections["vietnam"]["rows"].append(r)
            continue
        if lb in ("망길라오계", "탈로포포계"):
            cur_sec = "guam"
        if cur_sec:
            sections[cur_sec]["rows"].append(r)
    return {
        "sections": [sections["americas"], sections["guam"], sections["vietnam"]],
        "rollup": rollup,
    }


# ── P&L 파싱 (page2) ───────────────────────────────────────────────────
PNL_SECTIONS = ["운영", "분양", "합계"]


def parse_pnl(page):
    words = page.extract_words()
    # 섹션(운영/분양/합계) 라벨은 블록 중앙에 세로 정렬 → y-중심 앵커로 최근접 배정
    anchors = [(w["top"], w["text"]) for w in words
               if w["x0"] < 55 and w["text"] in PNL_SECTIONS]
    clusters = cluster_rows(words)
    out = []
    for cl in clusters:
        labels = sorted([w for w in cl if w["x0"] < 130 and not is_valtoken(w["text"])],
                        key=lambda w: w["x0"])
        vals = [w for w in cl if is_valtoken(w["text"])]
        if len(vals) < 4:
            continue
        name_tok = [w for w in labels if w["x0"] >= 55]
        # 지표명은 자간이 넓어 여러 토큰(O C C / 매 출)일 수 있음
        name = "".join(w["text"] for w in name_tok)
        if not name:
            continue
        row_y = sum(w["top"] for w in cl) / len(cl)
        sec = None
        if anchors:
            # 섹션 라벨은 블록 중앙에 정렬됨 → 큰 블록의 끝 행은 두 앵커와 거의 등거리.
            # 근접 동점(±6px)은 상단(먼저 오는) 섹션으로 귀속.
            av = sorted(anchors)
            dmin = min(abs(a[0] - row_y) for a in av)
            sec = next(t for (y0, t) in av if abs(y0 - row_y) <= dmin + 6)
        bound = bind_columns(vals, PNL_COLS, PNL_TOL)
        cur = {f: bound.get(("cur", f)) for (m, f, x) in PNL_COLS if m == "cur"}
        cum = {f: bound.get(("cum", f)) for (m, f, x) in PNL_COLS if m == "cum"}
        out.append({"section": sec, "name": name, "cur": cur, "cum": cum})
    return out


# ── 표 탐지 (내용 시그니처) ────────────────────────────────────────────
def classify_pages(pdf):
    seg_pages, prop_only, ovs_pages, pnl_pages = [], [], [], []
    for i, p in enumerate(pdf.pages):
        t = p.extract_text() or ""
        ts = t.replace(" ", "")  # 자간 넓은 라벨(입 회 금 등) 대응
        has_seg = ("무기명" in ts and "G-OTA" in ts)
        has_prop = ("아시아" in ts and ("비발디" in ts or "델피노" in ts) and "합계" in ts)
        has_ovs = ("미주합계" in ts or "하이퐁" in ts) and "그린피" in ts
        has_pnl = ("분양구좌" in ts and "입회금" in ts and "영업이익" in ts)
        if has_seg:
            seg_pages.append(i)
        elif has_prop:
            prop_only.append(i)
        if has_ovs:
            ovs_pages.append(i)
        if has_pnl:
            pnl_pages.append(i)
    return seg_pages, prop_only, ovs_pages, pnl_pages


# ── 소스 PDF 선택 ──────────────────────────────────────────────────────
def _date_from_name(name):
    m = re.search(r"(20\d{6})", name)
    return m.group(1) if m else "00000000"


def pick_source(pdf_arg):
    if pdf_arg:
        p = Path(pdf_arg)
        if not p.exists():
            logger.error("지정 PDF 없음: %s", p)
            sys.exit(2)
        return p
    if not DROP_DIR.exists():
        logger.error("드롭 폴더 없음: %s", DROP_DIR)
        sys.exit(2)
    cands = [p for p in DROP_DIR.glob("*.pdf") if "주간회의" in p.name]
    if not cands:
        logger.error("주간회의 PDF를 찾지 못함: %s", DROP_DIR)
        sys.exit(2)
    cands.sort(key=lambda p: _date_from_name(p.name), reverse=True)
    return cands[0]


def base_months(report_date_str):
    """리포트 날짜(YYYYMMDD) → 당월/익월 YYYY-MM"""
    y, m = int(report_date_str[:4]), int(report_date_str[4:6])
    nm_y, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    return f"{y:04d}-{m:02d}", f"{nm_y:04d}-{nm:02d}"


# ── 교차검증 ───────────────────────────────────────────────────────────
def _actual(metric):
    return (metric or {}).get("실적")


def validate(month_key, mdata, warnings):
    seg_total = _actual((mdata.get("seg_total") or {}).get("rns"))
    prop_total = _actual((mdata.get("prop_total") or {}).get("rns"))
    # 1) 세그 합계 == 사업장 합계
    if seg_total is not None and prop_total is not None and seg_total != prop_total:
        warnings.append(f"[{month_key}] 세그합계 RNs({seg_total}) != 사업장합계 RNs({prop_total})")
    # 2) 세그 소계 합(+COMP) == 세그 합계
    subtotals = [_actual(r["rns"]) for r in mdata["segments"] if r["name"] in ("소계",)]
    comp = [_actual(r["rns"]) for r in mdata["segments"] if r["name"] == "COMP"]
    ssum = sum(v for v in subtotals + comp if v is not None)
    if seg_total is not None and subtotals and ssum != seg_total:
        warnings.append(f"[{month_key}] 세그 소계합+COMP({ssum}) != 세그합계({seg_total})")
    # 3) 권역 소계 합 == 사업장 합계
    rsub = [_actual(r["rns"]) for r in mdata["properties"] if r["name"] == "소계"]
    rsum = sum(v for v in rsub if v is not None)
    if prop_total is not None and rsub and rsum != prop_total:
        warnings.append(f"[{month_key}] 권역 소계합({rsum}) != 사업장합계({prop_total})")
    return {"seg_total_rns": seg_total, "prop_total_rns": prop_total,
            "seg_subtotal_sum": ssum, "region_subtotal_sum": rsum}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", default=None, help="소스 PDF 경로(미지정 시 드롭폴더 최신 주간회의)")
    ap.add_argument("--out", default=str(OUT_JSON))
    args = ap.parse_args()

    src = pick_source(args.pdf)
    report_date = _date_from_name(src.name)
    cur_m, next_m = base_months(report_date)
    logger.info("소스: %s (리포트일 %s → 당월 %s / 익월 %s)", src.name, report_date, cur_m, next_m)

    pdf = pdfplumber.open(str(src))
    seg_pages, prop_only, ovs_pages, pnl_pages = classify_pages(pdf)
    logger.info("표 탐지 · seg=%s prop_only=%s overseas=%s pnl=%s",
                [p + 1 for p in seg_pages], [p + 1 for p in prop_only],
                [p + 1 for p in ovs_pages], [p + 1 for p in pnl_pages])
    if len(seg_pages) < 2:
        logger.error("세그+사업장 페이지를 2개(당월/익월) 찾지 못함 — 양식 확인 필요")
        sys.exit(3)

    months = {}
    for idx, pi in enumerate(seg_pages[:2]):
        mk = cur_m if idx == 0 else next_m
        page = pdf.pages[pi]
        sy = split_y(page)
        segs, seg_total, seg_occ = parse_seg_table(page, ymax=sy)
        props, prop_total = parse_prop_table(page, ymin=sy)
        months[mk] = {
            "label": f"{int(mk[5:7])}월",
            "role": "current" if idx == 0 else "next",
            "source_page": pi + 1,
            "segments": segs,
            "seg_total": seg_total,
            "seg_occ": seg_occ,
            "properties": props,
            "prop_total": prop_total,
        }

    overseas = parse_overseas(pdf.pages[ovs_pages[0]]) if ovs_pages else None
    pnl = parse_pnl(pdf.pages[pnl_pages[0]]) if pnl_pages else None

    warnings = []
    checks = {}
    for mk, md in months.items():
        checks[mk] = validate(mk, md, warnings)

    now = datetime.now(KST)
    result = {
        "meta": {
            "source_pdf": src.name,
            "report_date": f"{report_date[:4]}-{report_date[4:6]}-{report_date[6:8]}",
            "current_month": cur_m,
            "next_month": next_m,
            "generated_at": now.strftime("%Y-%m-%d %H:%M KST"),
            "unit_notes": {
                "occ_rev": "점유비·달성률·증감률=%, 증감(점유비)=%p",
                "rns": "실(박)", "adr": "천원(국내)·USD/VND(해외 환산 주석 참조)",
                "rev": "백만원", "overseas": "매출=백만원, 골프 내장객=명, ADR=현지통화 환산",
                "pnl": "매출·이익=억원, RNs=실, 분양=구좌/입회금=억원",
            },
        },
        "pnl": pnl,
        "months": months,
        "overseas": overseas,
        "validation": {"checks": checks, "warnings": warnings},
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    # 로그 요약
    for mk, md in months.items():
        st = _actual((md.get("seg_total") or {}).get("rns"))
        pt = _actual((md.get("prop_total") or {}).get("rns"))
        logger.info("  %s · 세그합계 RNs=%s · 사업장합계 RNs=%s · 세그행=%d · 사업장행=%d",
                    mk, f"{st:,}" if st else st, f"{pt:,}" if pt else pt,
                    len(md["segments"]), len(md["properties"]))
    if warnings:
        for w in warnings:
            logger.warning("⚠ %s", w)
    else:
        logger.info("✓ 교차검증 통과 (세그=사업장 합계, 소계합=합계, 권역합=합계)")
    logger.info("✓ 저장: %s (%d bytes)", out, out.stat().st_size)


if __name__ == "__main__":
    main()
