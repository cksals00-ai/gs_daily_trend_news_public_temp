#!/usr/bin/env python3
"""
전체 세그 분석 자료 — 통합 빌더 (build_exec_segment.py)
─────────────────────────────────────────────────────────────────
지정 폴더(data/weekly_total)의 월간 리포트 PDF들을 스캔·파싱하여
월별 세그·사업장·해외 데이터를 하나의 독립 산출물로 통합한다.

두 가지 문서 타입을 자동 식별(내용/파일명 시그니처):
  · 예상(forecast) 리포트 : P&L·세그·사업장·해외 표(점유비/RNs/ADR/Rev)
        → 그 리포트의 '당월' 실적을 채택. (parse_exec_segment 재사용)
  · 확정(actual) 마감 리포트 : 세그(회원/단체/FIT·객실수·ADR·달성률·전년) + 헤드라인
        → 그 리포트의 '당월(단월)' 확정 실적을 채택.

월별 병합 우선순위: 확정(actual) > 예상(forecast).
같은 달의 예상 리포트가 여러 주차면 최신 주차 채택.

⚠️ 독립 산출물 원칙: db_aggregated / otb_data / *.gz 등 공용 데이터에
   절대 접근하지 않는다(읽지도 재생성도 안 함). PDF 파싱 결과만 사용.

교차검증:
  · 세그 (그룹 회원+단체+FIT+COMP) == 합계
  · 예상 리포트: 세그 합계 == 사업장 합계 (parse_exec_segment 검증 재사용)
  · 확정 리포트: 월간 단월 합계의 누계연쇄 정합(로그)

출력: data/exec_segment.json (+ docs/data/exec_segment.json 사본)

사용:
  python3 scripts/build_exec_segment.py [--src DIR] [--out FILE]

© 2026 GS팀
"""
import argparse
import glob
import json
import logging
import os
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pdfplumber

sys.path.insert(0, str(Path(__file__).resolve().parent))
import parse_exec_segment as W  # 예상 리포트 파서(검증 완료) 재사용

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("build_exec_segment")

ROOT = Path(__file__).resolve().parent.parent
SRC_DEFAULT = ROOT / "data" / "weekly_total"
OUT_DEFAULT = ROOT / "data" / "exec_segment.json"
DOCS_OUT = ROOT / "docs" / "data" / "exec_segment.json"
KST = timezone(timedelta(hours=9))


def nfc(s):
    return unicodedata.normalize("NFC", s or "")


# ── 세그 라벨 정규화(예상/확정 taxonomy 정합) ────────────────────────────
SEG_CANON = {
    "기명": "기명", "무기명": "무기명", "Staff": "Staff", "직원": "Staff",
    "G-Mice": "G-Mice", "G-MICE": "G-Mice", "E-Mice": "E-Mice", "E-MICE": "E-Mice",
    "Inbound": "Inbound", "인바운드": "Inbound", "일반": "일반", "학생": "학생",
    "Hompage": "홈페이지", "홈페이지": "홈페이지", "OTA": "OTA",
    "G-OTA": "G-OTA", "GOTA": "G-OTA", "Affiliate": "제휴사", "제휴사": "제휴사",
    "COMP": "COMP",
}
GROUP_CANON = {"회원": "회원", "단체": "단체", "FIT": "FIT"}


def canon_leaf(name, group):
    n = name.strip()
    # 'Other'/'기타'는 그룹에 따라 의미가 다름 → 회원=Other, FIT=기타
    if n in ("Other", "기타"):
        return "Other" if group == "회원" else "기타"
    return SEG_CANON.get(n, n)


# ═══════════════════════════════════════════════════════════════════════
#  예상(forecast) 리포트 — parse_exec_segment 재사용
# ═══════════════════════════════════════════════════════════════════════
def month_from_weekly_name(name):
    """'(N월M주)…' → 1~12 (월-label 우선). 없으면 파일명 날짜의 월."""
    m = re.search(r"\((\d{1,2})월", nfc(name))
    if m:
        return int(m.group(1))
    d = W._date_from_name(nfc(name))
    return int(d[4:6]) if d != "00000000" else None


def date_key(name):
    """파일명 날짜 → 정렬키(YYYYMMDD). 8자리(20260713)·6자리(260713=YYMMDD) 모두."""
    nm = nfc(name)
    m = re.search(r"(20\d{6})", nm)
    if m:
        return m.group(1)
    # 6자리 YYMMDD (예: 260713 → 20260713). 주차 표기 '3~4' 등과 구분 위해 MMDD 유효성 체크
    for g in re.findall(r"(?<!\d)(\d{6})(?!\d)", nm):
        mm, dd = int(g[2:4]), int(g[4:6])
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return "20" + g
    return "00000000"


def _mnum(md):
    return (md or {}).get("실적")


def norm_metric(md):
    """weekly metric dict {전년,목표,실적,증감,달성률} → 표준."""
    if not md:
        return None
    return {
        "v": md.get("실적"), "target": md.get("목표"), "ly": md.get("전년"),
        "delta": md.get("증감"), "ach": md.get("달성률"),
    }


def parse_forecast(pdf, which=0):
    """예상 리포트의 세그/사업장/해외 추출. which=0 당월, 1 익월(해외 없음)."""
    seg_pages, prop_only, ovs_pages, pnl_pages = W.classify_pages(pdf)
    if len(seg_pages) <= which:
        return None
    pi = seg_pages[which]
    page = pdf.pages[pi]
    sy = W.split_y(page)
    segs, seg_total, seg_occ = W.parse_seg_table(page, ymax=sy)
    props, prop_total = W.parse_prop_table(page, ymin=sy)
    overseas = W.parse_overseas(pdf.pages[ovs_pages[0]]) if (ovs_pages and which == 0) else None

    total_rns = _mnum((seg_total or {}).get("rns"))
    # 표준 세그 구조로 변환
    segments = []
    for r in segs:
        if r["name"] == "소계":
            continue
        grp = r.get("group")
        segments.append({
            "group": grp, "name": canon_leaf(r["name"], grp), "level": "leaf",
            "rns": norm_metric(r.get("rns")), "adr": norm_metric(r.get("adr")),
            "rev": norm_metric(r.get("rev")), "occ": norm_metric(r.get("occ")),
        })
    # 그룹 소계(예상 리포트는 '소계' 행으로 존재)
    groups = []
    for r in segs:
        if r["name"] == "소계":
            groups.append({
                "group": r.get("group"), "name": r.get("group"), "level": "group",
                "rns": norm_metric(r.get("rns")), "adr": norm_metric(r.get("adr")),
                "rev": norm_metric(r.get("rev")), "occ": norm_metric(r.get("occ")),
            })
    headline = {
        "rns": total_rns,
        "rns_ach": _mnum((seg_total or {}).get("rns")) and (seg_total["rns"].get("달성률")),
        "adr": _mnum((seg_total or {}).get("adr")),
        "rev": _mnum((seg_total or {}).get("rev")),
        "occ": (seg_occ or {}).get("실적"),
        "rns_ly": (seg_total or {}).get("rns", {}).get("전년"),
        "rev_ly": (seg_total or {}).get("rev", {}).get("전년"),
    }
    return {
        "segments": groups + segments,
        "seg_total": norm_metric((seg_total or {}).get("rns")),
        "seg_total_full": seg_total,
        "properties": props, "prop_total": prop_total,
        "overseas": overseas, "headline": headline,
    }


# ═══════════════════════════════════════════════════════════════════════
#  확정(actual) 마감 리포트 파서
# ═══════════════════════════════════════════════════════════════════════
CLOSE_COLS = [
    ("cur", "목표", 158.4), ("cur", "실적", 218.0), ("cur", "증감", 272.1), ("cur", "달성률", 295.7),
    ("cur", "전년", 338.6), ("cur", "증감_전년", 392.0), ("cur", "신장률", 415.6),
    ("cum", "목표", 458.5), ("cum", "실적", 518.1), ("cum", "증감", 572.2), ("cum", "달성률", 595.8),
    ("cum", "전년", 638.7), ("cum", "증감_전년", 692.1), ("cum", "신장률", 715.7),
]
CLOSE_TOL = 20.0
LABX = 125
GROUPS = {"회원", "단체", "FIT"}
TOTMARK = {"합계", "객실수", "ADR", "객실매출", "Rev", "Rev.", "투숙률", "매출합계"}


def cval(t):
    return W.to_num(t.replace("원", ""))


def cisval(t):
    c = t.replace("실", "").replace("원", "").replace("%p", "").replace("%", "").replace(",", "")
    return bool(re.search(r"\d", c)) and bool(re.fullmatch(r"[\d.\-()]*", c))


def cclusters(ws, tol=4.5):
    ws = sorted(ws, key=lambda w: (w["top"], w["x0"]))
    out, cur, cy = [], [], None
    for w in ws:
        if cy is None or abs(w["top"] - cy) <= tol:
            cur.append(w); cy = w["top"] if cy is None else (cy + w["top"]) / 2
        else:
            out.append(cur); cur = [w]; cy = w["top"]
    if cur:
        out.append(cur)
    return out


def cbind(vals):
    best = defaultdict(list)
    for w in vals:
        cx = (w["x0"] + w["x1"]) / 2
        bi, bd = None, CLOSE_TOL
        for i, (m, f, x) in enumerate(CLOSE_COLS):
            if abs(cx - x) < bd:
                bi, bd = i, abs(cx - x)
        if bi is not None:
            best[bi].append((w["x0"], w["text"]))
    o = {}
    for i, tk in best.items():
        tk.sort()
        o[(CLOSE_COLS[i][0], CLOSE_COLS[i][1])] = cval("".join(t for _, t in tk)) if len(tk) > 1 else cval(tk[0][1])
    return o


def _cmet(b, blk):
    return {
        "v": b.get((blk, "실적")), "target": b.get((blk, "목표")), "ly": b.get((blk, "전년")),
        "delta": b.get((blk, "증감")), "ach": b.get((blk, "달성률")), "growth": b.get((blk, "신장률")),
    }


def _find_page(pdf, needles):
    for i, p in enumerate(pdf.pages):
        ts = nfc(p.extract_text() or "").replace(" ", "")
        if all(k in ts for k in needles):
            return i
    return None


def parse_closing_segment(pdf):
    pi = _find_page(pdf, ("무기명", "GOTA", "객실수", "홈페이지"))
    if pi is None:
        return None
    ws = pdf.pages[pi].extract_words()
    for w in ws:
        w["text"] = nfc(w["text"])
    mem = sorted([w["top"] for w in ws if w["text"] == "회원" and w["x0"] < LABX])
    split = mem[1] if len(mem) >= 2 else 1e9

    def scan(ylo, yhi):
        rows, total, cg = [], None, None
        for cl in cclusters([w for w in ws if ylo <= w["top"] < yhi]):
            labs = sorted([w for w in cl if (w["x0"] + w["x1"]) / 2 < LABX and not cisval(w["text"])], key=lambda w: w["x0"])
            vals = [w for w in cl if cisval(w["text"])]
            if not labs or len(vals) < 3:
                continue
            ltset = set(w["text"] for w in labs)
            left = labs[0]["text"]
            b = cbind(vals)
            if b.get(("cur", "실적")) is None:
                continue
            if ltset & TOTMARK:
                if total is None:
                    total = b
                continue
            if left in GROUPS:
                cg = left
                rows.append({"group": left, "name": left, "level": "group", "b": b})
            else:
                rows.append({"group": cg, "name": left, "level": "leaf", "b": b})
        return rows, total

    rns_rows, rns_total = scan(0, split)
    adr_rows, adr_total = scan(split, 1e9)
    if rns_total is None:
        return None
    # ADR을 (group,name)로 매핑
    adr_map = {(r["group"], r["name"]): r["b"] for r in adr_rows}

    total_rns = rns_total.get(("cur", "실적"))
    total_adr = (adr_total or {}).get(("cur", "실적"))

    groups = {r["name"]: r for r in rns_rows if r["level"] == "group"}
    comp_row = next((r for r in rns_rows if r["name"] == "COMP"), None)
    leaves = [r for r in rns_rows if r["level"] == "leaf" and r["name"] != "COMP"]

    # COMP = 합계 - (회원+단체+FIT) (정의상 잔여, 파싱값과 교차검증)
    gsum = sum(g["b"].get(("cur", "실적"), 0) for g in groups.values())
    comp_calc = total_rns - gsum if total_rns is not None else None
    comp_parsed = comp_row["b"].get(("cur", "실적")) if comp_row else None

    # leaf 검증: leaf 합 == 합계?
    leaf_sum = sum(l["b"].get(("cur", "실적"), 0) for l in leaves) + (comp_parsed or comp_calc or 0)
    leaf_ok = (total_rns is not None and leaf_sum == total_rns)

    segments = []
    for gname in ("회원", "단체", "FIT"):
        g = groups.get(gname)
        if not g:
            continue
        b = g["b"]
        segments.append({
            "group": gname, "name": gname, "level": "group",
            "rns": _cmet(b, "cur"), "adr": _cmet(adr_map.get((None, gname)) or {}, "cur"),
            "rev": None, "occ": None,
        })
        if leaf_ok:
            for l in leaves:
                if l["group"] != gname:
                    continue
                nm = canon_leaf(l["name"], gname)
                segments.append({
                    "group": gname, "name": nm, "level": "leaf",
                    "rns": _cmet(l["b"], "cur"),
                    "adr": _cmet(adr_map.get((gname, l["name"])) or {}, "cur"),
                    "rev": None, "occ": None,
                })
    # COMP
    comp_b = comp_row["b"] if comp_row else None
    if comp_b:
        comp_met = _cmet(comp_b, "cur")
    else:
        comp_met = {"v": comp_calc, "target": None, "ly": None, "delta": None, "ach": None, "growth": None}
    segments.append({"group": None, "name": "COMP", "level": "leaf", "rns": comp_met, "adr": None, "rev": None, "occ": None})

    return {
        "segments": segments,
        "seg_total": _cmet(rns_total, "cur"),
        "seg_total_cum": _cmet(rns_total, "cum"),
        "total_rns": total_rns, "total_adr": total_adr,
        "leaf_validated": leaf_ok,
        "comp_calc": comp_calc, "comp_parsed": comp_parsed,
        "seg_page": pi + 1,
    }


def parse_closing_property_dashboard(pdf):
    """마감 사업장 대시보드(사업장·달성률·YoY ×4열) → list[{name, ach, yoy}]."""
    pi = None
    for i, p in enumerate(pdf.pages):
        ts = nfc(p.extract_text() or "").replace(" ", "")
        if ts.count("달성률") >= 3 and ts.count("YoY") >= 3 and "사업장" in ts:
            pi = i; break
    if pi is None:
        return None
    ws = pdf.pages[pi].extract_words()
    for w in ws:
        w["text"] = nfc(w["text"])
    SKIP = {"사업장", "달성률", "YoY", "목표", "실적", "신장률", "전년", "증감", "구분"}
    out = []
    for cl in cclusters(ws):
        toks = sorted(cl, key=lambda w: w["x0"])
        name = []
        for t in toks:
            s = t["text"]
            if s in SKIP:
                name = []; continue
            mu = re.match(r"^(\d+)%$", s)       # 부호없는 % = 달성률
            if mu:
                nm = " ".join(name).strip(); name = []
                if nm and len(nm) <= 14:
                    out.append({"name": nm, "ach": int(mu.group(1)), "yoy": None, "_a": True})
                continue
            ms = re.match(r"^([+-]\d+)%$", s)    # 부호있는 % = YoY
            if (ms or s == "-") and out and out[-1].get("_a"):
                out[-1]["yoy"] = int(ms.group(1)) if ms else None
                out[-1]["_a"] = False; continue
            if re.match(r"^[가-힣A-Za-z&·]", s):
                name.append(s)
            else:
                name = []
    for b in out:
        b.pop("_a", None)
    # 중복 사업장(동일명) 첫값 채택
    seen = {}
    for b in out:
        if b["name"] not in seen:
            seen[b["name"]] = b
    return list(seen.values())


def parse_closing_overseas(pdf):
    """마감 해외: 괌(망길라오+탈로포포) · 베트남(하이퐁) 당월 총매출 실적."""
    def course_rev(needle, rowlabels):
        for i, p in enumerate(pdf.pages):
            ts = nfc(p.extract_text() or "").replace(" ", "")
            if needle in ts and any(r in ts for r in rowlabels):
                ws = p.extract_words()
                for w in ws:
                    w["text"] = nfc(w["text"])
                for cl in cclusters(ws):
                    labs = [w["text"] for w in cl if (w["x0"] + w["x1"]) / 2 < LABX]
                    if any(r in "".join(labs) for r in rowlabels):
                        b = cbind([w for w in cl if cisval(w["text"])])
                        v = b.get(("cur", "실적"))
                        if v is not None:
                            return v
        return None
    mangi = course_rev("망길라오", ["총매출"])
    talo = course_rev("탈로포포", ["총매출"])
    haiphong = course_rev("하이퐁", ["매출액"])
    guam = None
    if mangi is not None or talo is not None:
        guam = (mangi or 0) + (talo or 0)
    return {"guam": guam, "vietnam": haiphong,
            "detail": {"망길라오": mangi, "탈로포포": talo, "하이퐁": haiphong}}


def parse_closing_headline(pdf, seg):
    """투숙률 합계(OCC) · 매출합계(Rev) 를 헤드라인 페이지에서 보강."""
    occ = rev = rev_ly = None
    pi = _find_page(pdf, ("투숙률", "매출합계", "주중"))
    if pi is not None:
        ws = pdf.pages[pi].extract_words()
        for w in ws:
            w["text"] = nfc(w["text"])
        hap_rows = []
        matchul = None
        for cl in cclusters(ws):
            labs = [w for w in cl if (w["x0"] + w["x1"]) / 2 < LABX and not cisval(w["text"])]
            vals = [w for w in cl if cisval(w["text"])]
            if len(vals) < 3:
                continue
            names = set(w["text"] for w in labs)
            b = cbind(vals)
            if "합계" in names:
                hap_rows.append(b)
            if "매출합계" in names:
                matchul = b
        if hap_rows:  # 첫 합계 = 투숙률(OCC)
            occ = hap_rows[0].get(("cur", "실적"))
        if matchul:
            rev = matchul.get(("cur", "실적"))
            rev_ly = matchul.get(("cur", "전년"))
    tot = seg.get("seg_total") or {}
    return {
        "rns": seg.get("total_rns"), "rns_ach": tot.get("ach"), "rns_ly": tot.get("ly"),
        "adr": seg.get("total_adr"), "occ": occ, "rev": rev, "rev_ly": rev_ly,
    }


# ═══════════════════════════════════════════════════════════════════════
#  분류 · 오케스트레이션
# ═══════════════════════════════════════════════════════════════════════
def classify(pdf, name):
    nm = nfc(name)
    if "경영실적" in nm:
        return "closing"
    # 내용 시그니처
    txt = ""
    for p in pdf.pages[:min(len(pdf.pages), 40)]:
        txt += nfc(p.extract_text() or "")
    ts = txt.replace(" ", "")
    if "EBITDA" in ts.upper() and "투숙률" in ts and "매출합계" in ts:
        return "closing"
    # 예상(주간) 리포트: 세그(무기명·G-OTA) + 사업장(아시아·델피노) 표 존재
    # 해외 라벨(미주합계/하이퐁)은 리포트 판(주간회의/주간업무)마다 달라 시그니처에서 제외.
    if "무기명" in ts and "G-OTA" in ts and "델피노" in ts and ("아시아" in ts or "점유비" in ts):
        return "weekly"
    return "skip"


def month_key(y, m):
    return f"{y:04d}-{m:02d}"


def build(src_dir, out_path):
    files = sorted([f for f in glob.glob(str(Path(src_dir) / "*.pdf"))])
    if not files:
        logger.error("소스 PDF 없음: %s", src_dir)
        sys.exit(2)
    YEAR = 2026
    forecast = {}   # month → (datekey, data)
    forecast_ovs = {}  # month → (datekey, overseas) — 해외 파싱된 최신(폴백용)
    actual = {}     # month → data
    mapping = []    # 보고용: 파일 → 타입/월
    close_chain = {}

    for f in files:
        base = os.path.basename(f)
        try:
            pdf = pdfplumber.open(f)
        except Exception as e:
            logger.warning("열기 실패 skip: %s (%s)", base, e); continue
        kind = classify(pdf, base)
        if kind == "skip":
            mapping.append((base, "skip", None)); continue
        try:
            if kind == "weekly":
                mo = month_from_weekly_name(base)
                if not mo:
                    mapping.append((base, "weekly?", None)); continue
                data = parse_forecast(pdf)
                if not data:
                    mapping.append((base, "weekly(파싱실패)", mo)); continue
                dk = date_key(base)
                if mo not in forecast or dk >= forecast[mo][0]:
                    forecast[mo] = (dk, data, f)  # 파일경로 보관(아젠다 추출용)
                # 해외는 판(리포트 양식)마다 파싱 가능 여부가 달라, 해당 월 '해외가 파싱된 최신' 별도 보관(폴백)
                if data.get("overseas") and data["overseas"].get("sections"):
                    if mo not in forecast_ovs or dk >= forecast_ovs[mo][0]:
                        forecast_ovs[mo] = (dk, data["overseas"])
                mapping.append((base, "예상", mo))
            else:  # closing
                m = re.search(r"\(26\.(\d\d)\)", nfc(base))
                mo = int(m.group(1)) if m else None
                seg = parse_closing_segment(pdf)
                if not seg or mo is None:
                    mapping.append((base, "확정(파싱실패)", mo)); continue
                head = parse_closing_headline(pdf, seg)
                prop_dash = parse_closing_property_dashboard(pdf)
                actual[mo] = {"segments": seg["segments"], "seg_total": seg["seg_total"],
                              "leaf_validated": seg["leaf_validated"], "headline": head,
                              "comp_calc": seg["comp_calc"], "comp_parsed": seg["comp_parsed"],
                              "property_dash": prop_dash}
                close_chain[mo] = (seg["total_rns"], seg["seg_total_cum"]["v"])
                mapping.append((base, "확정", mo))
        except Exception as e:
            logger.warning("파싱 오류 skip: %s (%s)", base, e)
            mapping.append((base, kind + "(오류)", None))

    # ── 병합: 확정 > 예상 ──
    months = {}
    warnings = []
    all_months = sorted(set(forecast) | set(actual))
    for mo in all_months:
        mk = month_key(YEAR, mo)
        fc = forecast.get(mo)
        ac = actual.get(mo)
        entry = {"label": f"{mo}월", "month": mk}
        if ac:
            entry["kind"] = "actual"
            entry["segments"] = ac["segments"]
            entry["seg_total"] = ac["seg_total"]
            entry["headline"] = ac["headline"]
            entry["leaf_validated"] = ac["leaf_validated"]
            entry["seg_source"] = "actual"
            # 교차검증: 그룹+COMP == 합계
            grp_sum = sum((s["rns"] or {}).get("v") or 0 for s in ac["segments"] if s["level"] == "group")
            comp_s = next((s for s in ac["segments"] if s["name"] == "COMP"), None)
            comp_v = ((comp_s or {}).get("rns") or {}).get("v") or 0
            gsum = grp_sum + comp_v
            tot = (ac["seg_total"] or {}).get("v")
            if tot is not None and gsum != tot:
                warnings.append(f"[{mk}] 확정 세그 그룹+COMP({gsum}) != 합계({tot})")
            entry["property_dash"] = ac.get("property_dash")   # 사업장 달성률/YoY
        else:
            entry["kind"] = "forecast"
            entry["segments"] = fc[1]["segments"]
            entry["seg_total"] = fc[1]["seg_total"]
            entry["headline"] = fc[1]["headline"]
            entry["leaf_validated"] = True
            entry["seg_source"] = "forecast"
        # 사업장·해외: 예상 리포트가 있으면 사용(확정 문서엔 정형 표 없음)
        if fc:
            entry["properties"] = fc[1]["properties"]
            entry["prop_total"] = W.metric_dict((fc[1]["prop_total"] or {}).get("rns", {}), "rns") if fc[1].get("prop_total") else None
            entry["prop_total_raw"] = fc[1]["prop_total"]
            # 해외: 선택 리포트에 있으면 사용, 없으면(양식 상이) 그 달 해외 파싱된 최신으로 폴백
            _ovs = fc[1]["overseas"]
            if not (_ovs and _ovs.get("sections")) and mo in forecast_ovs:
                _ovs = forecast_ovs[mo][1]
                entry["overseas_stale"] = True
            entry["overseas"] = _ovs
            entry["property_source"] = "forecast"
            # 예상 교차검증: 세그합==사업장합
            st = (fc[1].get("seg_total_full") or {}).get("rns", {}).get("실적")
            pt = (fc[1].get("prop_total") or {}).get("rns", {}).get("실적")
            if st is not None and pt is not None and st != pt:
                warnings.append(f"[{mk}] 예상 세그합({st}) != 사업장합({pt})")
        else:
            entry["properties"] = None; entry["overseas"] = None; entry["prop_total"] = None
        months[mk] = entry

    # ── 익월(next-month) 예상 추가: 최신 예상 리포트의 '익월' 페이지 ──
    if forecast:
        latest_mo = max(forecast)
        nm = 1 if latest_mo == 12 else latest_mo + 1
        nmk = month_key(YEAR if latest_mo < 12 else YEAR + 1, nm)
        if nmk not in months:
            try:
                npdf = pdfplumber.open(forecast[latest_mo][2])
                nd = parse_forecast(npdf, which=1)
                if nd and (nd["seg_total"] or {}).get("v"):
                    months[nmk] = {
                        "label": f"{nm}월", "month": nmk, "kind": "forecast",
                        "seg_source": "forecast", "leaf_validated": True,
                        "segments": nd["segments"], "seg_total": nd["seg_total"],
                        "headline": nd["headline"], "properties": nd["properties"],
                        "prop_total_raw": nd["prop_total"], "overseas": None,
                        "property_source": "forecast", "next_month": True,
                    }
                    all_months = sorted(set(all_months) | {nm if latest_mo < 12 else 0})
            except Exception as e:
                logger.warning("익월 파싱 실패 %s: %s", nmk, e)

    # ── 아젠다 추출(월별 최신 예상 리포트) ──
    try:
        import parse_agenda
        for mo, fc in forecast.items():
            mk = month_key(YEAR, mo)
            if mk not in months:
                continue
            try:
                apdf = pdfplumber.open(fc[2])
                ag = parse_agenda.extract_agenda(apdf)
                months[mk]["agenda"] = ag
            except Exception as e:
                logger.warning("아젠다 추출 실패 %s: %s", mk, e)
    except Exception as e:
        logger.warning("parse_agenda 임포트 실패: %s", e)

    insights = make_insights(months)
    deep = make_deep_insights(months)
    issues = make_issues(months)

    now = datetime.now(KST)
    _mks_sorted = sorted(months)
    _actual_mks = [mk for mk in _mks_sorted if months[mk]["kind"] == "actual"]
    _forecast_mks = [mk for mk in _mks_sorted if months[mk]["kind"] == "forecast"]
    result = {
        "meta": {
            "generated_at": now.strftime("%Y-%m-%d %H:%M KST"),
            "months": _mks_sorted,
            "actual_months": _actual_mks,
            "forecast_months": _forecast_mks,
            "unit_notes": {
                "rns": "실(박)", "adr": "천원", "rev": "백만원",
                "occ": "%", "share": "구성비 %(실적/합계)",
                "actual": "확정 실적(마감)", "forecast": "예상 실적(당월 Forecast)",
            },
        },
        "months": months,
        "insights": insights,
        "deep_insights": deep,
        "issues": issues,
        "validation": {"warnings": warnings},
    }

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    DOCS_OUT.parent.mkdir(parents=True, exist_ok=True)
    DOCS_OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── 로그 ──
    logger.info("── 파일 매핑 ──")
    for base, typ, mo in mapping:
        logger.info("  [%s] %s%s", typ, base[:46], f"  → {mo}월" if mo else "")
    logger.info("── 월별 합계 RN ──")
    for mk in sorted(months):
        e = months[mk]
        st = (e["seg_total"] or {}).get("v")
        logger.info("  %s (%s): 합계RN=%s · 세그항목=%d · 사업장=%s · 해외=%s",
                    mk, e["kind"], f"{st:,}" if st else st, len(e["segments"]),
                    "○" if e.get("properties") else "—", "○" if e.get("overseas") else "—")
    logger.info("── 확정 누계연쇄 검증 ──")
    for mo in sorted(close_chain):
        if mo - 1 in close_chain:
            exp = close_chain[mo - 1][1] + close_chain[mo][0]
            got = close_chain[mo][1]
            logger.info("  %d누계+%d당월=%s vs %d누계=%s %s", mo - 1, mo, f"{exp:,}", mo, f"{got:,}",
                        "✓" if exp == got else "✗")
    if warnings:
        for w in warnings:
            logger.warning("⚠ %s", w)
    else:
        logger.info("✓ 세그 그룹+COMP=합계 / 예상 세그합=사업장합 전월 통과")
    logger.info("✓ 저장: %s (+docs 사본) · 월=%s", out_path, result["meta"]["months"])
    return result


def _seg(m, name, level=None):
    for s in m.get("segments", []) or []:
        if s["name"] == name and (level is None or s["level"] == level):
            return s
    return None


def _ach(s):
    return ((s or {}).get("rns") or {}).get("ach")


def _yoyp(s):
    r = (s or {}).get("rns") or {}
    v, ly = r.get("v"), r.get("ly")
    return round(v / ly * 100) if v and ly else None


def _yoy_ratio(v, ly):
    return round(v / ly * 100) if (v and ly) else None


def _pget(prop, metric):
    return (prop.get(metric) or {})


def make_issues(months):
    """주간회의 논점을 데이터로 분석한 이슈 리포트. 데이터 없는 항목은 확인필요로 명시."""
    mks = sorted(months)
    out = {"focus_property": None, "yoy_down": {"segments": [], "properties": []},
           "direct_fit": None, "property_issues": [], "notes": []}
    # 대상월: 7·8월(있으면). 없으면 최신 예상월들.
    target = [mk for mk in mks if mk in ("2026-07", "2026-08")] or [mk for mk in mks if months[mk]["kind"] == "forecast"][-2:]

    # ── 1) 단양 포커스 ──
    dy = {"name": "단양", "by_month": [], "segment_note": "사업장별 세그(회원/단체/FIT) 분해는 원자료가 전사 단위로만 제공 → 단양 세그 구성은 확인필요."}
    for mk in target:
        m = months[mk]
        p = next((r for r in (m.get("properties") or []) if r["name"] == "단양"), None)
        if p:
            rn = _pget(p, "rns"); rev = _pget(p, "rev"); occ = _pget(p, "occ")
            dy["by_month"].append({
                "month": mk, "label": m["label"],
                "rns": rn.get("실적"), "target": rn.get("목표"), "ly": rn.get("전년"),
                "ach": rn.get("달성률"), "yoy": _yoy_ratio(rn.get("실적"), rn.get("전년")),
                "rev": rev.get("실적"), "rev_yoy": _yoy_ratio(rev.get("실적"), rev.get("전년")),
                "occ": occ.get("실적"),
            })
    # 판정
    achs = [b["ach"] for b in dy["by_month"] if b["ach"] is not None]
    yoys = [b["yoy"] for b in dy["by_month"] if b["yoy"] is not None]
    if achs:
        if all(a >= 100 for a in achs) and all((y or 0) >= 100 for y in yoys):
            dy["verdict"] = "데이터상 단양은 부진 아님 — 목표 초과·전년비 성장. (회의 지적과 상이, 세그 구성은 확인필요)"
            dy["tone"] = "good"
        elif any(a < 90 for a in achs) or any((y or 100) < 95 for y in yoys):
            dy["verdict"] = "일부월 목표 미달/전년비 둔화 — 세부 원인은 세그 구성 확인필요."
            dy["tone"] = "warn"
        else:
            dy["verdict"] = "목표 부근 등락 — 뚜렷한 부진 신호는 약함(세그 구성 확인필요)."
            dy["tone"] = "info"
    out["focus_property"] = dy

    # ── 2) YoY 하락(전년比 <100%) — 세그·사업장 ──
    for mk in target:
        m = months[mk]
        for s in m.get("segments", []) or []:
            r = s.get("rns") or {}
            y = _yoy_ratio(r.get("v"), r.get("ly"))
            if y is not None and y < 100:
                out["yoy_down"]["segments"].append({"month": mk, "label": m["label"],
                    "name": s["name"], "level": s["level"], "yoy": y,
                    "v": r.get("v"), "ly": r.get("ly"), "ach": r.get("ach")})
        for p in m.get("properties", []) or []:
            if p["name"] == "소계":
                continue
            r = p.get("rns") or {}; rev = p.get("rev") or {}
            y = _yoy_ratio(r.get("실적"), r.get("전년"))
            ry = _yoy_ratio(rev.get("실적"), rev.get("전년"))
            if (y is not None and y < 100) or (ry is not None and ry < 100):
                out["yoy_down"]["properties"].append({"month": mk, "label": m["label"],
                    "name": p["name"], "region": p.get("region"),
                    "rns_yoy": y, "rev_yoy": ry, "ach": r.get("달성률"),
                    "v": r.get("실적"), "ly": r.get("전년")})

    # ── 3) 직영 vs 외부 FIT (데이터 가용성) ──
    prop_names = set()
    for mk in mks:
        for p in (months[mk].get("properties") or []):
            prop_names.add(p["name"])
    dash_names = set()
    for mk in mks:
        for b in (months[mk].get("property_dash") or []):
            dash_names.add(b["name"])
    panak = sorted([n for n in (prop_names | dash_names) if "파나크" in n or "영덕" in n])
    # 전사 채널 YoY(직영성 홈페이지 vs 외부 OTA/G-OTA) — 대상월
    chan_yoy = []
    for mk in target:
        m = months[mk]
        row = {"month": mk, "label": m["label"]}
        for ch in ("홈페이지", "OTA", "G-OTA", "제휴사", "기타"):
            s = _seg(m, ch, "leaf")
            r = (s or {}).get("rns") or {}
            row[ch] = {"yoy": _yoy_ratio(r.get("v"), r.get("ly")), "ach": r.get("ach")}
        chan_yoy.append(row)
    out["direct_fit"] = {
        "channel_yoy": chan_yoy,
        "panak_in_weekly_detail": any(("파나크" in n or "영덕" in n) for n in prop_names),
        "panak_in_closing_dash": any(("파나크" in n or "영덕" in n) for n in dash_names),
        "panak_names": panak,
        "per_property_fit_available": False,
        "verdict": ("‘파나크 영덕’은 사업장별 RN 상세(예상 리포트)에는 없고 마감 대시보드의 매출 달성률/YoY로만 존재. "
                    "또한 FIT(홈페이지·OTA·G-OTA 등) 실적은 원자료가 전사 단위로만 제공되어 사업장별 FIT 분해가 불가 → "
                    "‘직영 vs 파나크 FIT 격차’ 정량 비교는 현재 데이터로 불가. 확인필요: (1)직영/외부위탁 사업장 구분 기준, (2)사업장별 채널(FIT) 실적 자료."),
    }
    out["notes"].append("사업장별 세그/채널(FIT) 분해·직영/외부 구분은 원자료에 없음 → 별도 자료 필요(확인필요).")

    # ── 4) 사업장별 이슈 요약(당월=7월 우선, 없으면 대상 마지막월) ──
    base = "2026-07" if "2026-07" in months else (target[-1] if target else (mks[-1] if mks else None))
    if base:
        m = months[base]
        for p in m.get("properties", []) or []:
            if p["name"] == "소계":
                continue
            r = p.get("rns") or {}; rev = p.get("rev") or {}; occ = p.get("occ") or {}
            ach = r.get("달성률"); yoy = _yoy_ratio(r.get("실적"), r.get("전년"))
            ryoy = _yoy_ratio(rev.get("실적"), rev.get("전년"))
            flags = []
            if ach is not None and ach < 90:
                flags.append(f"목표 미달({ach}%)")
            if yoy is not None and yoy < 100:
                flags.append(f"전년比 하락(RN {yoy}%)")
            if ryoy is not None and ryoy < 100:
                flags.append(f"매출 전년比 {ryoy}%")
            if occ.get("실적") is not None and occ.get("전년") is not None and occ["실적"] < occ["전년"]:
                flags.append(f"OCC↓({occ['전년']}→{occ['실적']}%)")
            out["property_issues"].append({
                "name": p["name"], "region": p.get("region"),
                "ach": ach, "yoy": yoy, "rev_yoy": ryoy,
                "rns": r.get("실적"), "rev": rev.get("실적"),
                "issues": flags, "severity": (0 if not flags else (2 if (ach or 100) < 90 else 1)),
            })
        # 부진 우선 정렬
        out["property_issues"].sort(key=lambda x: (-x["severity"], (x["ach"] if x["ach"] is not None else 999)))
        out["issue_base_month"] = base
    return out


def make_deep_insights(months):
    """여러 축(세그·채널·목표·사업장·아젠다)을 엮은 심층 분석 요약. 전부 실값 근거."""
    mks = sorted(months)
    outs = []
    if not mks:
        return outs
    last = mks[-1]; m = months[last]; lab = m["label"]

    # 1) 채널 믹스 이동 — 회원 미달 vs OTA/G-OTA 초과
    hoe = _seg(m, "회원", "group"); ota = _seg(m, "OTA", "leaf"); gota = _seg(m, "G-OTA", "leaf")
    ha, oa, ga = _ach(hoe), _ach(ota), _ach(gota)
    if ha is not None and (((oa or 0) >= 100) or ((ga or 0) >= 100)) and ha < 98:
        outs.append({"cat": "채널 믹스", "tone": "warn",
                     "title": f"{lab}: 수요가 회원 → OTA·G-OTA로 이동",
                     "detail": f"회원 달성 {ha}%(미달)인 반면 OTA {oa}%·G-OTA {ga}% 초과달성. 직접(회원) 예약이 온라인 채널로 이동 — 채널 수수료·수익성 점검 필요."})

    # 2) 목표 정합성 — 전년비↑인데 달성률↓ (목표 과대)
    over = []
    for s in m.get("segments", []) or []:
        if s["level"] != "leaf" or s["name"] == "COMP":
            continue
        a, y = _ach(s), _yoyp(s)
        if a is not None and y is not None and a < 90 and y >= 105:
            over.append((s["name"], a, y))
    if over:
        over.sort(key=lambda x: x[1])
        txt = "; ".join(f"{n} 달성{a}%·전년비{y}%" for n, a, y in over[:3])
        outs.append({"cat": "목표 정합성", "tone": "info",
                     "title": f"{lab}: 전년비 성장에도 목표 미달 {len(over)}건 — 목표 과대 가능",
                     "detail": f"{txt}. 전년比 105%↑ 성장에도 달성률 90%↓ → 목표 상향이 과했을 신호."})

    # 3) 회원 vs FIT 비중 역전 (여러 달)
    inv = []
    for mk in mks:
        mm = months[mk]
        h = _seg(mm, "회원", "group"); f = _seg(mm, "FIT", "group")
        t = (mm.get("seg_total") or {}).get("v")
        if h and f and t:
            hs = (h["rns"] or {}).get("v"); fs = (f["rns"] or {}).get("v")
            if hs and fs and fs > hs:
                inv.append((mm["label"], round(hs / t * 100, 1), round(fs / t * 100, 1)))
    if inv:
        txt = ", ".join(f"{l}(회원 {a}%·FIT {b}%)" for l, a, b in inv)
        outs.append({"cat": "세그 믹스", "tone": "info",
                     "title": f"FIT가 회원을 추월한 달 {len(inv)}개",
                     "detail": f"{txt}. FIT(홈페이지·OTA·G-OTA) 비중이 회원을 상회 — 회원 기반 약화 구간."})

    # 4) 사업장 지속 부진/호조 (확정월 히트맵)
    amk = [mk for mk in mks if months[mk].get("property_dash")]
    if amk:
        series = defaultdict(dict)
        for mk in amk:
            for b in months[mk]["property_dash"]:
                if b.get("ach") is not None:
                    series[b["name"]][mk] = b["ach"]
        low, high = [], []
        for nm, d in series.items():
            vals = [d[mk] for mk in amk if mk in d]
            if len(vals) >= 3:
                if sum(1 for v in vals if v < 90) >= 3:
                    low.append((nm, sum(vals) // len(vals)))
                if sum(1 for v in vals if v >= 100) >= 3:
                    high.append((nm, sum(vals) // len(vals)))
        if low:
            low.sort(key=lambda x: x[1])
            outs.append({"cat": "사업장 부진", "tone": "warn",
                         "title": f"3개월 이상 달성률 90% 미만 사업장 {len(low)}곳",
                         "detail": "지속 부진: " + ", ".join(f"{n}(평균 {a}%)" for n, a in low[:5]) + " — 목표 재설정·수요 진작 필요."})
        if high:
            high.sort(key=lambda x: -x[1])
            outs.append({"cat": "사업장 호조", "tone": "good",
                         "title": f"3개월 이상 목표 초과 사업장 {len(high)}곳",
                         "detail": "지속 호조: " + ", ".join(f"{n}(평균 {a}%)" for n, a in high[:5]) + " — 성공요인 수평전개 검토."})

    # 5) 세그 전년비 톱/보텀 (그룹, 최신월)
    gy = [(s["name"], _yoyp(s)) for s in m.get("segments", []) or [] if s["level"] == "group" and _yoyp(s) is not None]
    if gy:
        best = max(gy, key=lambda x: x[1]); worst = min(gy, key=lambda x: x[1])
        outs.append({"cat": "전년비", "tone": "info",
                     "title": f"{lab} 전년비: {best[0]} {best[1]}% 견인 · {worst[0]} {worst[1]}% 부진",
                     "detail": f"그룹별 전년 대비 — 최고 {best[0]} {best[1]}%, 최저 {worst[0]} {worst[1]}%. 격차가 세그 믹스 변화의 동인."})

    # 6) ADR–물량 트레이드오프 (최신월 leaf)
    to = []
    for s in m.get("segments", []) or []:
        if s["level"] != "leaf":
            continue
        a = _ach(s); adr = s.get("adr") or {}
        av, aly = adr.get("v"), adr.get("ly")
        if a is not None and av and aly and a < 95 and av > aly:
            to.append((s["name"], a, round((av / aly - 1) * 100)))
    if to:
        to.sort(key=lambda x: x[1])
        txt = "; ".join(f"{n}(물량 {a}%·ADR 전년比+{d}%)" for n, a, d in to[:3])
        outs.append({"cat": "가격–물량", "tone": "info",
                     "title": f"{lab}: 단가는 올랐으나 물량 미달 {len(to)}건",
                     "detail": f"{txt}. ADR 상승이 물량 감소를 유발했을 가능성 — 가격 탄력성 점검."})

    # 7) 아젠다 집중 + 활동
    agmk = [mk for mk in mks if months[mk].get("agenda")]
    if agmk:
        am = months[agmk[-1]]; ag = am["agenda"]
        theme = defaultdict(int); active = []
        for b in ag:
            for it in b["items"]:
                theme[it["theme"]] += 1
            if b["items"]:
                active.append((b["name"], len(b["items"])))
        active.sort(key=lambda x: -x[1])
        tt = sorted(theme.items(), key=lambda x: -x[1])
        if tt:
            outs.append({"cat": "실행 아젠다", "tone": "info",
                         "title": f"{am['label']} 현장 실행은 '{tt[0][0]}'에 집중({tt[0][1]}건)",
                         "detail": "테마 상위 " + " · ".join(f"{k} {v}" for k, v in tt[:3]) + f". 최다 활동 사업장: " + ", ".join(f"{n}({c})" for n, c in active[:3]) + " — 실행 리소스 배분 참고."})

    return outs


def make_insights(months):
    """실값 기반 자동 인사이트(추정 금지)."""
    outs = []
    mks = sorted(months)
    if not mks:
        return outs
    # 1) 최근 확정월 요약
    act = [mk for mk in mks if months[mk]["kind"] == "actual"]
    if act:
        last = act[-1]
        e = months[last]
        h = e["headline"] or {}
        if h.get("rns_ach") is not None:
            tone = "달성" if h["rns_ach"] >= 100 else "미달"
            outs.append({"type": "actual", "month": last,
                         "text": f"{e['label']} 확정 객실 RN {fmtn(h.get('rns'))}실 · 목표 대비 {h['rns_ach']}% {tone}"})
    # 2) 회원 그룹 YoY 추세(확정월)
    def grp_v(mk, g):
        for s in months[mk]["segments"]:
            if s["level"] == "group" and s["name"] == g:
                return s["rns"]
        return None
    if len(act) >= 2:
        a0, a1 = act[0], act[-1]
        for g in ("회원", "단체", "FIT"):
            v0, v1 = grp_v(a0, g), grp_v(a1, g)
            if v0 and v1 and v0.get("v") and v1.get("v"):
                dv = v1["v"] - v0["v"]
                pct = round(dv / v0["v"] * 100, 1) if v0["v"] else 0
                arrow = "▲" if dv > 0 else "▼"
                outs.append({"type": "trend", "text": f"{g} RN {months[a0]['label']}→{months[a1]['label']} {arrow}{abs(pct)}% ({fmtn(v0['v'])}→{fmtn(v1['v'])}실)"})
    # 3) 최근 확정월 세그 YoY (신장률) 부진 top
    if act:
        e = months[act[-1]]
        gy = []
        for s in e["segments"]:
            if s["level"] == "group" and s["rns"] and s["rns"].get("growth") is not None:
                gy.append((s["name"], s["rns"]["growth"]))
        if gy:
            worst = min(gy, key=lambda x: x[1])
            best = max(gy, key=lambda x: x[1])
            outs.append({"type": "yoy", "text": f"{e['label']} 전년비 최고 {best[0]} {sign(best[1])}% · 최저 {worst[0]} {sign(worst[1])}%"})
    # 4) 아젠다 인사이트(최신 아젠다 보유월)
    agmk = [mk for mk in mks if months[mk].get("agenda")]
    if agmk:
        last = agmk[-1]
        ag = months[last]["agenda"]
        props = [b for b in ag if b["type"] == "property"]
        theme_all = defaultdict(int)
        active = []
        for b in ag:
            for it in b["items"]:
                theme_all[it["theme"]] += 1
            if b["items"]:
                active.append((b["name"], len(b["items"])))
        if active:
            active.sort(key=lambda x: -x[1])
            top = ", ".join(f"{n}({c})" for n, c in active[:3])
            outs.append({"type": "agenda", "text": f"{months[last]['label']} 아젠다 최다: {top} — 사업장 {len(props)}곳·항목 {sum(c for _,c in active)}건"})
        if theme_all:
            tt = sorted(theme_all.items(), key=lambda x: -x[1])
            outs.append({"type": "agenda", "text": f"{months[last]['label']} 아젠다 테마 집중: " + " · ".join(f"{k} {v}건" for k, v in tt[:3])})
        # 매출/예상매출 언급 사업장
        amt = [(b["name"], b["amounts"]) for b in ag if b.get("amounts") and b["type"] in ("property", "dept")]
        if amt:
            picks = "; ".join(f"{n} {'/'.join(a[:2])}" for n, a in amt[:3])
            outs.append({"type": "agenda", "text": f"{months[last]['label']} 매출 파이프라인: {picks}"})
    return outs


def fmtn(v):
    try:
        return f"{v:,}"
    except Exception:
        return str(v)


def sign(v):
    return f"+{v}" if (v is not None and v > 0) else str(v)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=str(SRC_DEFAULT))
    ap.add_argument("--out", default=str(OUT_DEFAULT))
    args = ap.parse_args()
    build(args.src, args.out)


if __name__ == "__main__":
    main()
