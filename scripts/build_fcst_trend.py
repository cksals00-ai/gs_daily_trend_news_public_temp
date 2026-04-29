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

SEG_TARGETS = ["OTA", "G-OTA", "Inbound"]


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

    # ── Parse per-property Grand Total (existing logic) ──
    rows = []
    for page in pages:
        if not page.strip():
            continue
        head_name = ""
        for ln in [l for l in page.split("\n") if l.strip()][:8]:
            mm = re.search(r"▣\s+(\S[^▣]*?)(?:\s{2,}|\s*$)", ln)
            if not mm:
                continue
            cand = mm.group(1).strip()
            if any(skip in cand for skip in ("주요 지표", "Pick up", "추가", "일별", "주단위", "추이", "Segment")):
                continue
            head_name = cand
            break

        gm = re.search(r"Grand Total\s+(.+)", page)
        nums = []
        if gm:
            tail = gm.group(1).split("\n")[0]
            nums = parse_nums(tail)

        if head_name and len(nums) >= 6:
            # Grand Total line layout: BUDGET(rn,%,adr,rev,%) FCST(rn,%,adr,rev,%) DIFF(...) LY(...)
            # → nums[0]=budget_rn, nums[5]=fcst_rn (NOT nums[3] which is budget_rev)
            rows.append({
                "name": head_name,
                "budget_rn": nums[0],
                "fcst_rn": nums[5] if len(nums) > 5 else nums[0],
            })

    # Assign months
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
    for r in rows:
        if "month" not in r:
            continue
        canonical = NAME_MAP.get(r["name"])
        if not canonical:
            continue
        month_key = f"{year}-{r['month']:02d}"
        properties.setdefault(canonical, {})[month_key] = {
            "rm_fcst_rn": r["fcst_rn"],
            "rm_budget_rn": r["budget_rn"],
        }

    # ── Parse segment totals from summary pages ──
    # Summary pages contain detailed segment rows near Grand Total.
    # We look for lines starting with OTA/G-OTA/Inbound in the summary sections.
    segments = {}
    full_text_lines = text.split("\n")

    # Find all Grand Total lines with line numbers
    gt_lines = []
    for i, ln in enumerate(full_text_lines):
        if re.search(r"Grand Total", ln):
            gt_lines.append(i)

    # For each month's summary section (first 3 Grand Total occurrences = summary pages)
    # The summary Grand Total lines are the ones on pages that DON'T have ▣ property headers
    summary_gt_indices = []
    for gt_idx in gt_lines:
        # Check if this Grand Total is on a summary page (no ▣ property header nearby)
        context_start = max(0, gt_idx - 40)
        context = "\n".join(full_text_lines[context_start:gt_idx])
        # Summary pages have segment detail rows (기명, 무기명, OTA etc.) before Grand Total
        has_ota = any(re.match(r"\s+OTA\s", full_text_lines[j]) for j in range(max(0, gt_idx - 25), gt_idx))
        if has_ota:
            summary_gt_indices.append(gt_idx)

    for si, gt_idx in enumerate(summary_gt_indices):
        month_idx = si if si < len(detected_months) else len(detected_months) - 1
        if month_idx >= len(detected_months):
            break
        mo = detected_months[month_idx]
        month_key = f"{year}-{mo:02d}"

        # Search lines before Grand Total for segment rows
        search_start = max(0, gt_idx - 30)
        for li in range(search_start, gt_idx):
            line = full_text_lines[li]
            stripped = line.strip()

            for seg in SEG_TARGETS:
                # Match line starting with segment name (with possible leading whitespace)
                pattern = rf"^\s+{re.escape(seg)}\s"
                if re.match(pattern, line):
                    nums = parse_nums(line)
                    if len(nums) >= 6:
                        # First number group: budget_rn, then skip %, ADR, rev, rev%
                        # Sixth number group: fcst_rn
                        # Pattern: budget_rn, budget_pct(skip), budget_adr, budget_rev, budget_rev_pct(skip),
                        #          fcst_rn, fcst_pct(skip), fcst_adr, fcst_rev, fcst_rev_pct(skip)
                        budget_rn = nums[0]
                        fcst_rn = nums[5] if len(nums) > 5 else nums[0]
                        segments.setdefault(month_key, {})[seg] = {
                            "rm_budget_rn": budget_rn,
                            "rm_fcst_rn": fcst_rn,
                        }
                    break  # Don't match same line for multiple segments

    return properties, segments, detected_months


def main():
    pdfs = sorted(PDF_DIR.glob("Revenue*Meeting*.pdf"))
    print(f"Found {len(pdfs)} PDFs")

    snapshots = []
    all_years = set()

    for pdf in pdfs:
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
