#!/usr/bin/env python3
"""Booking Status Report(BSR) PDF의 Segment(OTB) 블록 파서.

각 사업장 페이지 하단 `Segment` 표에서 투숙월별 OTB를
Membership / Group / FIT / Comp / Total 5개 세그로 추출한다.
BSR은 15시 기준 스냅샷이며, 전년 비교일은 요일 정렬(예: 2026-07-09(목) ↔ 2025-07-10(목)).

사용:
    from parse_bsr_segment import parse_bsr
    bsr = parse_bsr(path)            # 최신 PDF 경로
    bsr["props"]["소노벨 청송"]["fit26"]
"""
import re
import sys
import unicodedata
from pathlib import Path

import pdfplumber

SEGS = ["mem", "grp", "fit", "comp", "tot"]
# BSR Total(#1) 집계에서 제외되는 위탁·신규 사업장 (검증용)
_TOTAL_EXCLUDED = {"파나크 영덕 바이 소노벨", "팔라티움 해운대 바이 소노펠리체"}


def _num(tok: str):
    """'▲ 1,275' / '▼1,102' / '0' → int (▼는 음수)."""
    tok = tok.strip()
    neg = tok.startswith("▼")
    tok = tok.lstrip("▲▼ ").replace(",", "")
    if tok in ("", "-"):
        return 0
    return -int(tok) if neg else int(tok)


def _normalize(line: str) -> str:
    """'▲ 1,275' 처럼 기호 뒤 공백을 제거해 토큰이 끊기지 않게 한다."""
    return re.sub(r"([▲▼])\s+", r"\1", line)


def _seg_block(text: str):
    lines = text.splitlines()
    idx = [i for i, l in enumerate(lines) if "Membership Group FIT Comp Total" in l]
    return lines[idx[0] + 1:] if idx else []


def _parse_page(text: str, month: str = "07"):
    """해당 페이지의 `month` 투숙월 OTB를 {2026:{seg:v}, 2025:{seg:v}, 'gap':{seg:v}} 로."""
    out = {}
    for raw in _seg_block(text):
        line = _normalize(raw)
        # "2026년 07월 21,659 4,371 19,443 110 45,583 ..." (앞에 'Segment ' 붙을 수 있음)
        m = re.match(r"^(?:Segment\s+)?(202[0-9])년\s+(\d{2})월\s+(.*)$", line)
        if m and m.group(2) == month:
            vals = [_num(t) for t in m.group(3).split()[:5]]
            out[int(m.group(1))] = dict(zip(SEGS, vals))
            continue
        # "07월 ▼1,097 ▲1,275 ▼1,102 ▼11 ▼935 ..." (2026 vs 2025 차이 행)
        m = re.match(r"^(?:2026 vs\.? 2025\s+)?(\d{2})월\s+(.*)$", line)
        if m and m.group(2) == month and "gap" not in out:
            toks = m.group(2 + 1).split()[:5] if False else m.group(2).split()[:5]
            out["gap"] = dict(zip(SEGS, [_num(t) for t in toks]))
    return out


def _title(text: str) -> str:
    head = text.splitlines()[0]
    m = re.search(r"\[(.*?)\]", head)
    return unicodedata.normalize("NFC", m.group(1).strip()) if m else head.strip()


def parse_bsr(path, month: str = "07"):
    path = Path(path)
    m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", path.name)
    date = f"{m.group(1)}{m.group(2)}{m.group(3)}" if m else None

    props, total = {}, None
    with pdfplumber.open(str(path)) as pdf:
        for pg in pdf.pages:
            text = pg.extract_text() or ""
            if not text:
                continue
            name = _title(text)
            data = _parse_page(text, month)
            if not data or 2026 not in data:
                continue
            if name == "Total":
                total = data
            elif not name.endswith("Total"):  # 권역 소계(#2~#5) 제외
                props[name] = data

    prior = None
    with pdfplumber.open(str(path)) as pdf:
        t0 = pdf.pages[0].extract_text() or ""
        pm = re.search(r"(2025)년 (\d{2})월 (\d{2})일", t0)
        if pm:
            prior = f"{pm.group(1)}{pm.group(2)}{pm.group(3)}"

    flat = {}
    for name, d in props.items():
        cur, pre, gap = d.get(2026, {}), d.get(2025, {}), d.get("gap", {})
        flat[name] = {
            "fit26": cur.get("fit", 0),
            "fit25": pre.get("fit", 0),
            "gap": gap.get("fit", cur.get("fit", 0) - pre.get("fit", 0)),
            "tot26": cur.get("tot", 0),
            "tot25": pre.get("tot", 0),
            "has_prior": pre.get("tot", 0) > 0,
        }
    return {
        "date": date,          # 2026 기준일 (15시 스냅샷)
        "prior_date": prior,   # BSR이 대조하는 2025 일자 (요일 정렬)
        "month": month,
        "props": flat,
        "total": {
            "fit26": (total or {}).get(2026, {}).get("fit", 0),
            "fit25": (total or {}).get(2025, {}).get("fit", 0),
            "gap": (total or {}).get("gap", {}).get("fit", 0),
        },
        "total_excluded": sorted(_TOTAL_EXCLUDED),
    }


if __name__ == "__main__":
    b = parse_bsr(sys.argv[1])
    print(f"BSR {b['date']} (15시) ↔ 전년 {b['prior_date']} / {b['month']}월 투숙 FIT OTB")
    print(f"{'사업장':30} {'2026':>7} {'2025':>7} {'차이':>8}  검증")
    s26 = s25 = 0
    for n, d in b["props"].items():
        ok = "ok" if d["fit26"] - d["fit25"] == d["gap"] else "!!"
        if n not in _TOTAL_EXCLUDED:
            s26 += d["fit26"]
            s25 += d["fit25"]
        print(f"{n:30} {d['fit26']:>7,} {d['fit25']:>7,} {d['gap']:>+8,}  {ok}"
              + ("" if d["has_prior"] else "  ←전년 미집계"))
    print("-" * 62)
    print(f"{'합계(위탁2 제외)':30} {s26:>7,} {s25:>7,}")
    t = b["total"]
    print(f"{'BSR Total 페이지':30} {t['fit26']:>7,} {t['fit25']:>7,} {t['gap']:>+8,}")
    print(f"{'차':30} {s26-t['fit26']:>7,} {s25-t['fit25']:>7,}")
