#!/usr/bin/env python3
"""
build_fcst_trend.py
===================
Parse ALL Revenue Meeting PDFs → build rm_fcst_trend.json with:
  - Per-property Grand Total (rm_fcst_rn, rm_budget_rn) — existing behavior
  - Per-segment totals (OTA, G-OTA, Inbound) from summary pages

Output: docs/data/rm_fcst_trend.json
"""

import json, re, subprocess, sys, shutil
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# 파싱 중 멈추는 PDF 제외 목록
SKIP_PDFS = {
    "Revenue Meeting_2024.01.24.pdf",
}

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

HAS_PDFTOTEXT = shutil.which("pdftotext") is not None

REPO = Path(__file__).resolve().parent.parent
PDF_DIR = REPO / "data" / "RM자료"
OUT = REPO / "docs" / "data" / "rm_fcst_trend.json"

NAME_MAP = {
    "소노벨_비발디파크":   "01.벨비발디",
    "소노캄_비발디파크":   "02.캄비발디",
    "소노펫_비발디파크":   "03.펫비발디",
    "소노펠리체_비발디파크": "04.펠리체비발디",
    "소노빌리지_비발디파크": "05.빌리지비발디",
    "소노벨_양평":        "06.양평",
    "델피노":             "07.델피노",
    "쏠비치_양양":        "08.쏠비치양양",
    "쏠비치_삼척":        "09.쏠비치삼척",
    "소노벨_단양":        "10.소노벨단양",
    "소노캄_경주":        "11.소노캄경주",
    "소노벨_청송":        "12.소노벨청송",
    "소노벨_천안":        "13.소노벨천안",
    "소노벨_변산":        "14.소노벨변산",
    "소노캄_여수":        "15.소노캄여수",
    "소노캄_거제":        "16.소노캄거제",
    "쏠비치_진도":        "17.쏠비치진도",
    "소노벨_제주":        "18.소노벨제주",
    "소노캄_제주":        "19.소노캄제주",
    "소노캄_고양":        "20.소노캄고양",
    "소노문_해운대":       "21.소노문해운대",
    "쏠비치_남해":        "22.쏠비치남해",
    "르네블루 바이 쏠비치": "23.르네블루",
}

SEG_TARGETS = ["G-OTA", "OTA", "Inbound"]  # G-OTA first to avoid OTA false-matching G-OTA lines


def extract_text(pdf):
    """Extract text from PDF using pdftotext (preferred) or pdfplumber (fallback)."""
    if HAS_PDFTOTEXT:
        try:
            r = subprocess.run(["pdftotext", "-layout", str(pdf), "-"],
                               capture_output=True, check=True, text=True, timeout=15)
            return r.stdout
        except Exception as e:
            print(f"  SKIP {pdf.name}: {e}")
            return None
    elif HAS_PDFPLUMBER:
        try:
            with pdfplumber.open(str(pdf)) as p:
                pages = []
                for page in p.pages:
                    text = page.extract_text(layout=True) or ""
                    pages.append(text)
                return "\f".join(pages)
        except Exception as e:
            print(f"  SKIP {pdf.name}: {e}")
            return None
    else:
        print("  ERROR: pdftotext와 pdfplumber 모두 없음. brew install poppler 또는 pip install pdfplumber 실행 필요")
        sys.exit(1)


def parse_nums(line):
    """Extract all numeric tokens from a line, handling ▼ prefix and commas."""
    tokens = []
    for m in re.finditer(r"[▼▲]?\s*(\d[\d,]*(?:\.\d+)?)", line):
        raw = m.group(1).replace(",", "")
        try:
            tokens.append(int(raw))
        except ValueError:
            try:
                tokens.append(float(raw))
            except ValueError:
                pass
    return tokens


def detect_year(pdf_name):
    """Extract year from PDF filename."""
    m = re.search(r"(\d{4})\.\d{2}\.\d{2}", pdf_name)
    return int(m.group(1)) if m else None


def detect_snapshot_date(pdf_name):
    m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", pdf_name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def parse_pdf(pdf):
    """Parse one Revenue Meeting PDF → properties dict + segments dict."""
    text = extract_text(pdf)
    if not text:
        return None, None, []

    pages = text.split("\f")
    year = detect_year(pdf.name)
    if not year:
        return None, None, []

    # Detect months from [N월] markers
    detected_months = []
    for page in pages:
        for m in re.finditer(r"\[(\d+)월\]", page[:300]):
            mo = int(m.group(1))
            if mo not in detected_months:
                detected_months.append(mo)

    if not detected_months:
        return None, None, detected_months

    # ── Parse per-property Grand Total + Segment (OTA/G-OTA/Inbound) ──
    # 각 페이지에서 ▣ 사업장명, Grand Total, OTA/G-OTA/Inbound 행을 동시에 추출
    rows = []
    for page in pages:
        if not page.strip():
            continue
        page_lines = page.split("\n")

        # 1) 사업장명 감지
        head_name = ""
        for ln in [l for l in page_lines if l.strip()][:8]:
            mm = re.search(r"▣\s+(\S[^▣]*?)(?:\s{2,}|\s*$)", ln)
            if not mm:
                continue
            cand = mm.group(1).strip()
            if any(skip in cand for skip in ("주요 지표", "Pick up", "추가", "일별", "주단위", "추이", "Segment")):
                continue
            head_name = cand
            break

        # 2) 주요 지표 섹션에서 Grand Total + 세그먼트 추출
        #    주요 지표 블록 = "Segment" 헤더행 ~ Grand Total 행 사이
        gt_idx = None
        for i, ln in enumerate(page_lines):
            if re.search(r"Grand Total", ln):
                gt_idx = i
                break  # 첫 번째 Grand Total만 (주요 지표 섹션)

        if gt_idx is None:
            continue
        # head_name 없는 페이지 = 전체 총괄 → "_TOTAL" 마커
        if not head_name:
            head_name = "_TOTAL"

        gt_tail = re.search(r"Grand Total\s+(.+)", page_lines[gt_idx])
        gt_nums = parse_nums(gt_tail.group(1).split("\n")[0]) if gt_tail else []
        if len(gt_nums) < 6:
            continue

        # Grand Total: nums[0]=budget_rn, nums[5]=fcst_rn
        row_data = {
            "name": head_name,
            "budget_rn": gt_nums[0],
            "fcst_rn": gt_nums[5] if len(gt_nums) > 5 else gt_nums[0],
            "segments": {},
        }

        # 세그먼트 행 검색 (Grand Total 위 30줄 이내)
        search_start = max(0, gt_idx - 30)
        for li in range(search_start, gt_idx):
            line = page_lines[li]
            for seg in SEG_TARGETS:
                pattern = rf"(?:^|\s){re.escape(seg)}\s"
                if re.search(pattern, line) and not (seg == "OTA" and "G-OTA" in line):
                    nums = parse_nums(line)
                    if len(nums) >= 6:
                        row_data["segments"][seg] = {
                            "rm_budget_rn": nums[0],
                            "rm_fcst_rn": nums[5] if len(nums) > 5 else nums[0],
                        }
                    break

        rows.append(row_data)

    # Assign months (사업장 중복 시 다음 월로 이동)
    section_idx = 0
    seen = set()
    for r in rows:
        if r["name"] in seen:
            section_idx += 1
            seen = set()
        seen.add(r["name"])
        if section_idx < len(detected_months):
            r["month"] = detected_months[section_idx]

    properties = {}
    segments = {}
    for r in rows:
        if "month" not in r:
            continue
        month_key = f"{year}-{r['month']:02d}"
        canonical = NAME_MAP.get(r["name"])

        # 사업장별 데이터 (NAME_MAP에 있는 사업장만)
        if canonical:
            prop_entry = {
                "rm_fcst_rn": r["fcst_rn"],
                "rm_budget_rn": r["budget_rn"],
            }
            if r["segments"]:
                prop_entry["segments"] = r["segments"]
            properties.setdefault(canonical, {})[month_key] = prop_entry

        # 전체 총괄 세그먼트: _TOTAL 마커 페이지에서 추출
        if r["name"] == "_TOTAL" and r["segments"]:
            segments[month_key] = r["segments"]

    return properties, segments, detected_months


def main():
    pdfs = sorted(PDF_DIR.glob("Revenue*Meeting*.pdf"))
    print(f"Found {len(pdfs)} PDFs")

    snapshots = []
    all_years = set()

    for pdf in pdfs:
        if pdf.name in SKIP_PDFS:
            print(f"  SKIP {pdf.name} (블랙리스트)")
            continue
        snap_date = detect_snapshot_date(pdf.name)
        year = detect_year(pdf.name)
        if not snap_date or not year:
            continue

        print(f"  Parsing {pdf.name}...", end="", flush=True)
        props, segs, months = parse_pdf(pdf)
        if not props:
            print(" SKIP (no data)")
            continue

        all_years.add(year)
        snap = {
            "_snapshot_date": snap_date,
            "_source_pdf": pdf.name,
            "_year": year,
            "properties": props,
        }
        if segs:
            snap["segments"] = segs
        snapshots.append(snap)
        seg_count = sum(len(v) for v in (segs or {}).values())
        print(f" OK ({len(props)} props, {seg_count} seg entries)")

    snapshots.sort(key=lambda s: s["_snapshot_date"])

    out = {
        "_description": "FCST 추이: Revenue Meeting PDF에서 추출한 사업장별+세그별 Budget/Forecast RN",
        "_unit": "Room Nights",
        "_generated": datetime.now().isoformat() + "Z",
        "_total_snapshots": len(snapshots),
        "_years": sorted(all_years),
        "snapshots": snapshots,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\n✓ {OUT.relative_to(REPO)} ({len(snapshots)} snapshots, {len(all_years)} years)")

    # Stats
    seg_snaps = sum(1 for s in snapshots if s.get("segments"))
    print(f"  세그먼트 데이터 포함 스냅샷: {seg_snaps}/{len(snapshots)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
