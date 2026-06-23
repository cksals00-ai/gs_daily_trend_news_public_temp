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
import fs_utils  # macOS NFD→NFC 유니코드 정규화
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
    # --rebuild / --full : 캐시 무시하고 전체 PDF 재파싱 (전체 121PDF ≈ 50분).
    # 기본(증분): 기존 rm_fcst_trend.json에 없는 PDF만 새로 파싱하고 나머지는 재사용.
    # → 매일 RM 갱신 시 신규 PDF 1건(~50초)만 파싱되어 build.py 내 호출이 가벼워진다.
    force_full = ("--rebuild" in sys.argv) or ("--full" in sys.argv)

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

    # 증분 캐시: 기존 trend json의 스냅샷을 source_pdf 기준으로 재사용 (force_full이면 무시)
    #  - cached_by_pdf : 파싱 성공 스냅샷 (양성 캐시)
    #  - cached_failed : 과거 파싱 시도했으나 사용 불가였던 PDF (음성 캐시) → 매번 재시도 방지
    #    (RM PDF 25건은 'rm_fcst_rn' 키 없음 등으로 항상 실패. 음성 캐시 없으면 증분 호출마다 ~14분 낭비)
    cached_by_pdf = {}
    cached_failed = set()
    if not force_full and OUT_TREND.exists():
        try:
            prev = json.loads(OUT_TREND.read_text(encoding="utf-8"))
            for snap in prev.get("snapshots", []):
                src = snap.get("_source_pdf")
                if src and snap.get("properties"):
                    cached_by_pdf[src] = snap
            cached_failed = set(prev.get("_failed_pdfs", []) or [])
        except Exception:
            cached_by_pdf = {}
            cached_failed = set()

    snapshots = []
    failed_pdfs = []          # 이번 실행에서 실제 디스크에 존재하는 실패 PDF만 기록(삭제분 자동 정리)
    n_cached = n_parsed = n_failskip = 0
    mode = "FULL(재파싱)" if force_full else f"증분(캐시 {len(cached_by_pdf)}건)"
    print(f"Parsing {len(pdfs)} PDFs... [{mode}]")
    t_start = time.time()
    for pdf in pdfs:
        if pdf.name in SKIP_PDFS:
            print(f"  SKIP {pdf.name}")
            continue
        snap_date, year = detect_snapshot_date(pdf.name)
        if not snap_date:
            print(f"  SKIP {pdf.name}: no date")
            continue
        # 증분: 이미 캐시에 있으면 재파싱 없이 재사용
        if pdf.name in cached_by_pdf:
            snapshots.append(cached_by_pdf[pdf.name])
            n_cached += 1
            continue
        # 증분: 과거 파싱 실패가 기록된 PDF는 재시도하지 않음(음성 캐시 유지)
        if pdf.name in cached_failed:
            failed_pdfs.append(pdf.name)
            n_failskip += 1
            continue
        n_parsed += 1
        t0 = time.time()
        try:
            parsed = parse_pdf(pdf)
        except Exception as e:
            print(f"  ERR  {pdf.name}: {e}")
            failed_pdfs.append(pdf.name)
            continue
        props = parsed.get("properties", {})
        if not props:
            print(f"  EMPTY {pdf.name}")
            failed_pdfs.append(pdf.name)
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
    print(f"Total parse time: {time.time() - t_start:.1f}s "
          f"(신규 파싱 {n_parsed}건, 캐시 재사용 {n_cached}건, 실패스킵 {n_failskip}건)")

    snapshots.sort(key=lambda s: s["_snapshot_date"])
    years = sorted({s["_year"] for s in snapshots})

    out = {
        "_description":     "Time-series RM Forecast snapshots (per-segment from PDF原本 cells, no distribution)",
        "_unit":            {"rn": "실 (rooms)", "rev_mil": "백만원 (million KRW)"},
        "_generated":       datetime.now().isoformat() + "Z",
        "_total_snapshots": len(snapshots),
        "_years":           years,
        "_failed_pdfs":     sorted(set(failed_pdfs)),  # 음성 캐시: 다음 증분 실행에서 재시도 안 함
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
