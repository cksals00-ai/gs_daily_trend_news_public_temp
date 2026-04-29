#!/usr/bin/env python3
"""
parse_rm_fcst.py
================

Parses the LATEST Revenue Meeting PDF in data/RM자료/ and writes per-property
forecast values (전망 = Revenue Meeting Forecast) to data/rm_fcst.json.

Schema notes (verified against PDF on 2026-04-29):
- Each property page contains a "▣ <propertyname>" header at top.
- The ▣ 주요 지표 section's "Grand Total" row holds:
    Budget(RN, RN%, ADR, Revenue, Rev%) | Forecast(RN, RN%, ADR, Revenue, Rev%) | ...
- The PDF covers 3 months (current + next 2). Pages are organized as
  [4월 summary][5월 summary][6월 summary][per-property × 4월][per-property × 5월][per-property × 6월].
- Property pages within each month appear in fixed order (4 region totals + 22 properties = ~26 pages).

OUTPUT: data/rm_fcst.json with `properties` keyed by canonical dashboard names
(e.g. "11.소노캄경주") and months keyed as "YYYY-MM".

Run: python3 scripts/parse_rm_fcst.py
"""

import json
import re
import sys
import warnings
import subprocess
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore")

REPO_ROOT = Path(__file__).resolve().parent.parent
PDF_DIR = REPO_ROOT / "data" / "RM자료"
OUT_PATH = REPO_ROOT / "data" / "rm_fcst.json"
DOCS_OUT_PATH = REPO_ROOT / "docs" / "data" / "rm_fcst.json"

# PDF property name → canonical dashboard name (matches data/daily_notes.json convention)
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


def find_latest_pdf() -> Path:
    pdfs = sorted(PDF_DIR.glob("Revenue Meeting_*.pdf"))
    if not pdfs:
        sys.exit(f"No Revenue Meeting PDFs found in {PDF_DIR}")
    return pdfs[-1]


def extract_pdf_text(pdf: Path) -> str:
    """Use pdftotext -layout (much faster than pdfplumber for 87-page docs)."""
    out = subprocess.run(
        ["pdftotext", "-layout", str(pdf), "-"],
        capture_output=True, check=True, text=True,
    )
    return out.stdout


def parse(pdf: Path) -> dict:
    text = extract_pdf_text(pdf)
    pages = text.split("\f")

    # Discover month boundaries by counting per-month sections.
    # Each month section has 4 region pages + 23 property pages = 27 pages of data.
    # Pages 1-3 are summary pages (one per month).
    # We'll iterate and assign months by detecting [N월] markers and section breaks.
    rows = []
    ctx_month = None
    section_property_count = 0
    detected_months = []

    for i, page in enumerate(pages):
        if not page.strip():
            continue

        # Check for [N월] in the first 200 chars (summary pages 1-3 have it)
        head_blob = page[:200]
        m = re.search(r"\[(\d+)월\]", head_blob)
        if m:
            new_month = int(m.group(1))
            if new_month not in detected_months:
                detected_months.append(new_month)

        # Find ▣ <name> header (not section markers like 주요 지표)
        head_name = ""
        for ln in [l for l in page.split("\n") if l.strip()][:8]:
            mm = re.search(r"▣\s+(\S[^▣]*?)(?:\s{2,}|\s*$)", ln)
            if not mm:
                continue
            cand = mm.group(1).strip()
            if any(skip in cand for skip in ("주요 지표", "Pick up", "추가", "일별", "주단위", "추이")):
                continue
            head_name = cand
            break

        # Find Grand Total row → first 6 numeric tokens
        gm = re.search(r"Grand Total\s+(.+)", page)
        nums = []
        if gm:
            tail = gm.group(1).split("\n")[0]
            for t in re.findall(r"\d[\d,]*(?:\.\d+)?", tail):
                try:
                    nums.append(int(t.replace(",", "")))
                except ValueError:
                    pass

        if head_name and len(nums) >= 6:
            rows.append({
                "page": i + 1,
                "name": head_name,
                "budget_rn":  nums[0],
                "budget_rev": nums[2],
                "fcst_rn":    nums[3],
                "fcst_rev":   nums[5],
            })

    # Assign months by section. Each month section = 27 pages (4 region totals + 23 properties).
    # Section breaks happen when we see a property name we've seen before.
    if not detected_months:
        sys.exit("Could not detect any [N월] markers in PDF — aborting.")

    section_idx = 0
    seen_in_section = set()
    for r in rows:
        # If this name was already seen in current section, advance to next month
        if r["name"] in seen_in_section:
            section_idx += 1
            seen_in_section = set()
        seen_in_section.add(r["name"])
        if section_idx < len(detected_months):
            r["month"] = detected_months[section_idx]
        else:
            r["month"] = None  # extra section beyond known months

    # Build output dicts
    properties: dict = {}
    regions: dict = {}
    unmapped = []

    for r in rows:
        if r["month"] is None:
            continue
        month_key = f"2026-{r['month']:02d}"
        record = {
            "budget_rn":       r["budget_rn"],
            "budget_rev_mil":  r["budget_rev"],
            "rm_fcst_rn":      r["fcst_rn"],
            "rm_fcst_rev_mil": r["fcst_rev"],
        }
        if r["name"].endswith("_Total"):
            region = r["name"].replace("_Total", "")
            regions.setdefault(region, {})[month_key] = record
        else:
            canonical = NAME_MAP.get(r["name"])
            if not canonical:
                unmapped.append(r["name"])
                continue
            properties.setdefault(canonical, {})[month_key] = record

    # Validation: sum of property forecast should equal corresponding region totals.
    sum_4_rev = sum(p["2026-04"]["rm_fcst_rev_mil"] for p in properties.values() if "2026-04" in p)
    sum_4_rn  = sum(p["2026-04"]["rm_fcst_rn"]      for p in properties.values() if "2026-04" in p)
    sum_4_region_rev = sum(r["2026-04"]["rm_fcst_rev_mil"] for r in regions.values() if "2026-04" in r)
    sum_4_region_rn  = sum(r["2026-04"]["rm_fcst_rn"]      for r in regions.values() if "2026-04" in r)

    out = {
        "_source_pdf":      pdf.name,
        "_extracted_at":    datetime.now().isoformat() + "Z",
        "_snapshot_date":   pdf.stem.split("_")[-1],
        "_months_covered":  [f"2026-{m:02d}" for m in sorted(set(detected_months))],
        "_units": {"rn": "실 (rooms)", "rev_mil": "백만원 (million KRW)"},
        "_field_meaning":   ("rm_fcst_rn = Revenue Meeting forecast Room Nights. "
                             "rm_fcst_rev_mil = forecast Revenue in million KRW. "
                             "budget_* = same-PDF Budget reference."),
        "_validation": {
            "sum_4_property_rev_mil": sum_4_rev,
            "sum_4_region_rev_mil":   sum_4_region_rev,
            "sum_4_property_rn":      sum_4_rn,
            "sum_4_region_rn":        sum_4_region_rn,
            "unmapped_pdf_names":     sorted(set(unmapped)),
        },
        "regions":    regions,
        "properties": properties,
    }
    return out


def main() -> int:
    pdf = find_latest_pdf()
    print(f"Latest PDF: {pdf.name}")
    out = parse(pdf)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_PATH.relative_to(REPO_ROOT)} ({len(out['properties'])} properties × {len(out['regions'])} regions)")

    DOCS_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    DOCS_OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {DOCS_OUT_PATH.relative_to(REPO_ROOT)}")

    v = out["_validation"]
    print(f"\nValidation (4월):")
    print(f"  sum_property_rev = {v['sum_4_property_rev_mil']:,}M  vs  sum_region_rev = {v['sum_4_region_rev_mil']:,}M")
    print(f"  sum_property_rn  = {v['sum_4_property_rn']:,}      vs  sum_region_rn  = {v['sum_4_region_rn']:,}")
    if v["unmapped_pdf_names"]:
        print(f"  WARNING: unmapped PDF names: {v['unmapped_pdf_names']}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
