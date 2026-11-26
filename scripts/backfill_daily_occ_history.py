#!/usr/bin/env python3
"""보관 중인 Daily Booking Report PDF 전체 → data/daily_occ_history.json 일괄 적재(멱등, 같은 보고일 덮어씀).

평소에는 parse_daily_booking.py 가 PDF 1건 파싱할 때마다 자동 누적하므로 이 스크립트는 최초 적재·복구용.
사용: python3 scripts/backfill_daily_occ_history.py [--only-missing]
"""
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import parse_daily_booking as pdb  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
PDF_DIR = ROOT / "data" / "Daily Booking Report PDF"
HIST = ROOT / "data" / "daily_occ_history.json"


def main():
    only_missing = "--only-missing" in sys.argv
    have = set()
    if only_missing and HIST.exists():
        have = set(json.loads(HIST.read_text(encoding="utf-8")).keys())
    pdfs = []
    for p in PDF_DIR.glob("Daily Booking Report_*.pdf"):
        m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", p.name)
        if m:
            pdfs.append((f"{m.group(1)}-{m.group(2)}-{m.group(3)}", p))
    pdfs.sort()
    done = 0
    for d, p in pdfs:
        if d in have:
            continue
        try:
            r = pdb.parse_pdf(str(p))
            n = pdb.save_daily_history(r, HIST)
            done += 1
            print(f"✓ {d} → 이력 {n}개")
        except Exception as e:  # noqa: BLE001
            print(f"⚠ {d} 실패: {e}")
    print(f"완료: {done}건 적재 → {HIST.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
