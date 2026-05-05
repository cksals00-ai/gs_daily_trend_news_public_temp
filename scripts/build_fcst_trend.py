#!/usr/bin/env python3
"""
build_fcst_trend.py
===================
Parse ALL Revenue Meeting PDFs in data/RM자료/ → time-series snapshots.

Uses the same per-segment cell extraction as parse_rm_fcst.py (PDF 원본 셀,
분배 X). Each snapshot represents one PDF's view of the future months.

Output:
  - docs/data/rm_fcst_trend.json   (time-series, used by fcst-trend.html etc.)
  - data/fcst_segment_trend.json   (legacy path; kept for backwards compat)

Snapshot schema:
  {
    _snapshot_date: "YYYY-MM-DD",
    _source_pdf:    "Revenue Meeting_*.pdf",
    _year:          int,
    properties: {
      "01.벨비발디": {
        "2026-04": {
          rm_budget_rn, rm_budget_rev_mil,
          rm_fcst_rn,   rm_fcst_rev_mil,
          segments: {
            OTA:     {rm_budget_rn, rm_budget_rev_mil, rm_fcst_rn, rm_fcst_rev_mil},
            G-OTA:   {...},
            Inbound: {...}
          }
        }, ...
      }, ...
    }
  }
"""

import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from parse_rm_fcst import parse as parse_pdf, NAME_MAP

REPO = Path(__file__).resolve().parent.parent
PDF_DIR = REPO / "data" / "RM자료"
OUT_TREND = REPO / "docs" / "data" / "rm_fcst_trend.json"
OUT_SEGMENT_TREND = REPO / "data" / "fcst_segment_trend.json"

# PDFs that hang or are corrupt
SKIP_PDFS = {
    "Revenue Meeting_2024.01.24.pdf",
}


def detect_snapshot_date(pdf_name: str):
    m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", pdf_name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}", int(m.group(1))
    return None, None


def main() -> int:
    pdfs = sorted(PDF_DIR.glob("Revenue Meeting_*.pdf"))
    if not pdfs:
        sys.exit(f"No PDFs in {PDF_DIR}")

    # Preserve historical mix ratios from existing fcst_segment_trend.json (computed from db_aggregated)
    preserved_ratios = {}
    if OUT_SEGMENT_TREND.exists():
        try:
            old = json.loads(OUT_SEGMENT_TREND.read_text(encoding="utf-8"))
            preserved_ratios = old.get("ratios", {}) or {}
        except Exception:
            pass

    snapshots = []
    print(f"Parsing {len(pdfs)} PDFs...")
    t_start = time.time()
    for pdf in pdfs:
        if pdf.name in SKIP_PDFS:
            print(f"  SKIP {pdf.name}")
            continue
        snap_date, year = detect_snapshot_date(pdf.name)
        if not snap_date:
            print(f"  SKIP {pdf.name}: no date")
            continue
        t0 = time.time()
        try:
            parsed = parse_pdf(pdf)
        except Exception as e:
            print(f"  ERR  {pdf.name}: {e}")
            continue
        props = parsed.get("properties", {})
        if not props:
            print(f"  EMPTY {pdf.name}")
            continue
        seg_count = sum(
            1 for pd in props.values()
            for ym_data in pd.values()
            if ym_data.get("segments")
        )
        snapshots.append({
            "_snapshot_date": snap_date,
            "_source_pdf":    pdf.name,
            "_year":          year,
            "properties":     props,
        })
        print(f"  OK {pdf.name}: {len(props)} props, {seg_count} seg-records ({time.time()-t0:.1f}s)")
    print(f"Total parse time: {time.time() - t_start:.1f}s")

    snapshots.sort(key=lambda s: s["_snapshot_date"])
    years = sorted({s["_year"] for s in snapshots})

    out = {
        "_description":     "Time-series RM Forecast snapshots (per-segment from PDF原本 cells, no distribution)",
        "_unit":            {"rn": "실 (rooms)", "rev_mil": "백만원 (million KRW)"},
        "_generated":       datetime.now().isoformat() + "Z",
        "_total_snapshots": len(snapshots),
        "_years":           years,
        "snapshots":        snapshots,
        "ratios":           preserved_ratios,
    }

    OUT_TREND.parent.mkdir(parents=True, exist_ok=True)
    OUT_TREND.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n✓ {OUT_TREND.relative_to(REPO)} ({len(snapshots)} snapshots, years={years})")

    OUT_SEGMENT_TREND.parent.mkdir(parents=True, exist_ok=True)
    OUT_SEGMENT_TREND.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✓ {OUT_SEGMENT_TREND.relative_to(REPO)} (legacy path)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
