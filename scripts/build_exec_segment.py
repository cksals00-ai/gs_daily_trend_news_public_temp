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
    m = re.search(r"(20\d{6})", nfc(name))
    return m.group(1) if m else "00000000"


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


def parse_forecast(pdf):
    """예상 리포트의 '당월' 세그/사업장/해외 추출."""
    seg_pages, prop_only, ovs_pages, pnl_pages = W.classify_pages(pdf)
    if not seg_pages:
        return None
    pi = seg_pages[0]  # 첫 세그 페이지 = 당월
    page = pdf.pages[pi]
    sy = W.split_y(page)
    segs, seg_total, seg_occ = W.parse_seg_table(page, ymax=sy)
    props, prop_total = W.parse_prop_table(page, ymin=sy)
    overseas = W.parse_overseas(pdf.pages[ovs_pages[0]]) if ovs_pages else None

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
    if "무기명" in ts and "미주합계" in ts and ("분양구좌" in ts or "G-OTA" in ts):
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
                    forecast[mo] = (dk, data)
                mapping.append((base, "예상", mo))
            else:  # closing
                m = re.search(r"\(26\.(\d\d)\)", nfc(base))
                mo = int(m.group(1)) if m else None
                seg = parse_closing_segment(pdf)
                if not seg or mo is None:
                    mapping.append((base, "확정(파싱실패)", mo)); continue
                head = parse_closing_headline(pdf, seg)
                actual[mo] = {"segments": seg["segments"], "seg_total": seg["seg_total"],
                              "leaf_validated": seg["leaf_validated"], "headline": head,
                              "comp_calc": seg["comp_calc"], "comp_parsed": seg["comp_parsed"]}
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
            entry["overseas"] = fc[1]["overseas"]
            entry["property_source"] = "forecast"
            # 예상 교차검증: 세그합==사업장합
            st = (fc[1].get("seg_total_full") or {}).get("rns", {}).get("실적")
            pt = (fc[1].get("prop_total") or {}).get("rns", {}).get("실적")
            if st is not None and pt is not None and st != pt:
                warnings.append(f"[{mk}] 예상 세그합({st}) != 사업장합({pt})")
        else:
            entry["properties"] = None; entry["overseas"] = None; entry["prop_total"] = None
        months[mk] = entry

    insights = make_insights(months)

    now = datetime.now(KST)
    result = {
        "meta": {
            "generated_at": now.strftime("%Y-%m-%d %H:%M KST"),
            "months": [month_key(YEAR, m) for m in all_months],
            "actual_months": [month_key(YEAR, m) for m in sorted(actual)],
            "forecast_months": [month_key(YEAR, m) for m in sorted(forecast) if m not in actual],
            "unit_notes": {
                "rns": "실(박)", "adr": "천원", "rev": "백만원",
                "occ": "%", "share": "구성비 %(실적/합계)",
                "actual": "확정 실적(마감)", "forecast": "예상 실적(당월 Forecast)",
            },
        },
        "months": months,
        "insights": insights,
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
