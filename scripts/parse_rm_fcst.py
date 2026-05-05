#!/usr/bin/env python3
"""
parse_rm_fcst.py
================

Parses a Revenue Meeting PDF and writes per-property forecast values to
data/rm_fcst.json.

Backend: pypdfium2 (~50x faster than pdfplumber). Pure-Python, no system deps.

Each property page has a 주요 지표 table with Budget / Forecast columns.
We extract:
  - Grand Total (last data row in the 주요 지표 section)
  - Per-segment cells: OTA, G-OTA (FIT), Inbound (Group)
    Each: Budget RN, Budget REV(M), Forecast RN, Forecast REV(M) — PDF원본, 분배 X.

When run on the latest PDF, writes data/rm_fcst.json + docs/data/rm_fcst.json.

The `parse(pdf_path)` function returns the dict and is reused by
build_fcst_trend.py for historical PDFs.
"""

import json
import re
import sys
import warnings
from datetime import datetime
from pathlib import Path

import pypdfium2 as pdfium

warnings.filterwarnings("ignore")

REPO_ROOT = Path(__file__).resolve().parent.parent
PDF_DIR = REPO_ROOT / "data" / "RM자료"
OUT_PATH = REPO_ROOT / "data" / "rm_fcst.json"
DOCS_OUT_PATH = REPO_ROOT / "docs" / "data" / "rm_fcst.json"

NAME_MAP = {
    "소노벨_비발디파크":      "01.벨비발디",
    "소노캄_비발디파크":      "02.캄비발디",
    "소노펫_비발디파크":      "03.펫비발디",
    "소노펠리체_비발디파크":   "04.펠리체비발디",
    "소노빌리지_비발디파크":   "05.빌리지비발디",
    "소노벨_양평":            "06.양평",
    "델피노":                "07.델피노",
    "쏠비치_양양":            "08.쏠비치양양",
    "쏠비치_삼척":            "09.쏠비치삼척",
    "소노벨_단양":            "10.소노벨단양",
    "소노캄_경주":            "11.소노캄경주",
    "소노벨_청송":            "12.소노벨청송",
    "소노벨_천안":            "13.소노벨천안",
    "소노벨_변산":            "14.소노벨변산",
    "소노캄_여수":            "15.소노캄여수",
    "소노캄_거제":            "16.소노캄거제",
    "쏠비치_진도":            "17.쏠비치진도",
    "소노벨_제주":            "18.소노벨제주",
    "소노캄_제주":            "19.소노캄제주",
    "소노캄_고양":            "20.소노캄고양",
    "소노문_해운대":           "21.소노문해운대",
    "쏠비치_남해":            "22.쏠비치남해",
    "르네블루":               "23.르네블루",
    "르네블루 바이 쏠비치":     "23.르네블루",
}

# Regions in older PDFs may use slightly different naming
REGION_NAMES = {"비발디", "한국중부", "아시아퍼시픽", "한국남부", "한중국부"}


def find_latest_pdf() -> Path:
    pdfs = sorted(PDF_DIR.glob("Revenue Meeting_*.pdf"))
    if not pdfs:
        sys.exit(f"No Revenue Meeting PDFs found in {PDF_DIR}")
    return pdfs[-1]


def extract_pages(pdf_path: Path) -> list[str]:
    doc = pdfium.PdfDocument(str(pdf_path))
    out = []
    for i in range(len(doc)):
        try:
            txt = doc[i].get_textpage().get_text_range() or ""
        except Exception:
            txt = ""
        out.append(txt)
    return out


# Match segment row at line start (no FIT/Group prefix in pypdfium2 output)
SEG_PATTERNS = {
    "OTA":     re.compile(r"^OTA\s+\d"),
    "G-OTA":   re.compile(r"^G-OTA\s+\d"),
    "Inbound": re.compile(r"^Inbound\s+\d"),
}

NUM_TOKEN = re.compile(r"\d+(?:,\d{3})*(?:\.\d+)?%?")


def _parse_10_tokens(s: str):
    """Extract first 10 numeric tokens. Layout:
      [0-4]=Budget {RN, RN%, ADR, REV, REV%}
      [5-9]=Forecast {RN, RN%, ADR, REV, REV%}
    """
    tokens = NUM_TOKEN.findall(s)
    if len(tokens) < 10:
        return None
    nums = []
    for t in tokens[:10]:
        t = t.replace(",", "").rstrip("%")
        try:
            v = float(t)
            nums.append(int(v) if v == int(v) else v)
        except ValueError:
            return None
    return nums


def _to_rec(nums) -> dict:
    return {
        "rm_budget_rn":      int(nums[0]),
        "rm_budget_rev_mil": int(nums[3]),
        "rm_fcst_rn":        int(nums[5]),
        "rm_fcst_rev_mil":   int(nums[8]),
    }


def detect_property_name(page_text: str) -> str | None:
    """In pypdfium2 the '▣ <name> #N' header may appear anywhere on the page.
    Find the marker that maps to a known property or region (_Total).
    """
    for m in re.finditer(r"▣\s+(.+?)(?:\s+#\d+|[\r\n])", page_text):
        cand = m.group(1).strip()
        if not cand:
            continue
        # Skip the section/box headers that don't represent a property.
        if any(skip in cand for skip in (
            "주요 지표", "Pick up", "추가", "일별", "주단위", "추이",
            "Segment", "특이사항", "Forecast",
        )):
            continue
        if cand in NAME_MAP or cand.endswith("_Total"):
            return cand
    return None


def detect_month(page_text: str) -> int | None:
    """Property pages contain row prefix like 'N월 Homepage' (RN-style chart)
    or `[N월]` markers in summary pages. With pypdfium2 the 'N월' prefix on
    Homepage may be lost — fall back to scanning the full text for 'N월' tokens
    that appear immediately before well-known segment/labels.
    """
    # 1) "[N월]" marker
    m = re.search(r"\[(\d+)월\]", page_text)
    if m:
        return int(m.group(1))
    # 2) "N월" prefix on Homepage line (older PDFs)
    m = re.search(r"(\d+)월\s+Homepage", page_text)
    if m:
        return int(m.group(1))
    return None


def parse_property_page(page_text: str) -> dict | None:
    name = detect_property_name(page_text)
    if not name:
        return None

    # Restrict to the main 주요 지표 section
    main = page_text.split("▣ Pick up Trend")[0]
    lines = [l.strip() for l in main.split("\n") if l.strip()]

    # Detect segment rows
    segments: dict[str, dict] = {}
    for line in lines:
        for seg_name, pat in SEG_PATTERNS.items():
            if pat.match(line):
                nums = _parse_10_tokens(line)
                if nums:
                    segments[seg_name] = _to_rec(nums)
                break

    # Grand Total = the LAST data row in the main section that has 10 numeric
    # tokens and looks like a totals row (Budget RN >= 100 typically). In
    # pypdfium2 output the "Grand Total" label is usually missing from this row
    # (it appears on its own line elsewhere). We pick the last data row whose
    # second token is "100.0%" (RN composition = 100% since it's totals).
    grand = None
    for line in reversed(lines):
        if "100.0%" not in line:
            continue
        # also must contain at least 10 numeric tokens
        nums = _parse_10_tokens(line)
        if nums and len(nums) >= 10:
            grand = _to_rec(nums)
            break

    if grand is None:
        return None

    return {"name": name, "grand": grand, "segments": segments}


def parse(pdf_path: Path) -> dict:
    pages = extract_pages(pdf_path)

    # Detect months from summary pages (have [N월] marker)
    detected_months = []
    for p in pages:
        for m in re.finditer(r"\[(\d+)월\]", p[:300]):
            mo = int(m.group(1))
            if mo not in detected_months:
                detected_months.append(mo)

    # Property/region pages: those with ▣ <name> #N marker.
    # PDF layout: per-month section = [region totals × R + property pages × P].
    # To assign month: when we see a property name we've seen before, advance
    # to the next detected month (same approach as the legacy parser).
    rows = []
    for i, page in enumerate(pages):
        rec = parse_property_page(page)
        if not rec:
            continue
        rows.append({"page": i + 1, **rec})

    # Assign months by section (advance when same name appears again)
    section_idx = 0
    seen_in_section: set[str] = set()
    for r in rows:
        if r["name"] in seen_in_section:
            section_idx += 1
            seen_in_section = set()
        seen_in_section.add(r["name"])
        if section_idx < len(detected_months):
            r["month"] = detected_months[section_idx]
        else:
            r["month"] = None

    # If no [N월] markers were found (rare older PDFs), bail.
    if not detected_months:
        return {
            "_source_pdf":     pdf_path.name,
            "_extracted_at":   datetime.now().isoformat() + "Z",
            "_snapshot_date":  pdf_path.stem.split("_")[-1],
            "_months_covered": [],
            "_validation":     {"unmapped_pdf_names": []},
            "regions":         {},
            "properties":      {},
        }

    # Determine the base year from the pdf filename for ym keys.
    # detected_months may wrap (e.g. 12, 1, 2) → bump year on wrap.
    fy = re.search(r"(\d{4})\.\d{2}\.\d{2}", pdf_path.name)
    base_year = int(fy.group(1)) if fy else datetime.now().year
    months_with_year = []
    cy = base_year
    prev_m = None
    for mo in detected_months:
        if prev_m is not None and mo < prev_m:
            cy += 1
        months_with_year.append((cy, mo))
        prev_m = mo

    properties: dict = {}
    regions: dict = {}
    unmapped = []

    for r in rows:
        if r["month"] is None:
            continue
        # Locate (year, month) for this row's section
        sec = next((i for i, (_y, mo) in enumerate(months_with_year) if mo == r["month"]), None)
        if sec is None:
            continue
        cy, mo = months_with_year[sec]
        ym = f"{cy}-{mo:02d}"
        record = {**r["grand"], "segments": r["segments"]}
        nm = r["name"]
        if nm.endswith("_Total"):
            regions.setdefault(nm.replace("_Total", ""), {})[ym] = record
            continue
        canonical = NAME_MAP.get(nm)
        if not canonical:
            unmapped.append(nm)
            continue
        properties.setdefault(canonical, {})[ym] = record

    # Validation
    val = {}
    for ym in sorted({y for p in properties.values() for y in p}):
        sum_grand_rn = sum(p[ym]["rm_fcst_rn"] for p in properties.values() if ym in p)
        sum_grand_rev = sum(p[ym]["rm_fcst_rev_mil"] for p in properties.values() if ym in p)
        sum_seg_rn = sum(
            sum(p[ym]["segments"].get(s, {}).get("rm_fcst_rn", 0) for s in ("OTA", "G-OTA", "Inbound"))
            for p in properties.values() if ym in p
        )
        sum_seg_rev = sum(
            sum(p[ym]["segments"].get(s, {}).get("rm_fcst_rev_mil", 0) for s in ("OTA", "G-OTA", "Inbound"))
            for p in properties.values() if ym in p
        )
        val[ym] = {
            "sum_property_grand_rn":  sum_grand_rn,
            "sum_property_grand_rev": sum_grand_rev,
            "sum_property_seg_rn":    sum_seg_rn,
            "sum_property_seg_rev":   sum_seg_rev,
        }

    return {
        "_source_pdf":      pdf_path.name,
        "_extracted_at":    datetime.now().isoformat() + "Z",
        "_snapshot_date":   pdf_path.stem.split("_")[-1],
        "_months_covered":  [f"{cy}-{mo:02d}" for cy, mo in months_with_year],
        "_units":           {"rn": "실 (rooms)", "rev_mil": "백만원 (million KRW)"},
        "_field_meaning":   ("rm_fcst_rn / rm_fcst_rev_mil = Revenue Meeting Forecast (Grand Total). "
                             "segments[OTA|G-OTA|Inbound] = same-PDF per-segment Budget+Forecast cells (no distribution)."),
        "_validation":      {**val, "unmapped_pdf_names": sorted(set(unmapped))},
        "regions":          regions,
        "properties":       properties,
    }


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

    print("\nValidation by month:")
    for ym, v in out["_validation"].items():
        if ym == "unmapped_pdf_names":
            continue
        print(f"  {ym}: grand_rn={v['sum_property_grand_rn']:,}  grand_rev={v['sum_property_grand_rev']:,}M  "
              f"seg(O+G+I)_rn={v['sum_property_seg_rn']:,}  seg(O+G+I)_rev={v['sum_property_seg_rev']:,}M")

    if out["_validation"]["unmapped_pdf_names"]:
        print(f"  WARNING: unmapped: {out['_validation']['unmapped_pdf_names']}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
