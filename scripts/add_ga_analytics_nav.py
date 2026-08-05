#!/usr/bin/env python3
"""
GSN 네비게이터에 '웹 애널리틱스'(ga-analytics.html) 메뉴를
'Booking Status' 오른쪽(없으면 트렌드 리포트 뒤)에 일괄 삽입.

- docs/*.html 전 페이지의 .gsn-item 네비가 페이지마다 하드코딩 → 전체 순회.
- 멱등: 이미 있으면 skip.
- add_sales_kpi_nav.py 패턴과 동일.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
BASE = "https://cksals00-ai.github.io/gs_daily_trend_news_public_temp"

NEW_ANCHOR = (
    f'<a class="gsn-item" href="{BASE}/ga-analytics.html" '
    'data-gsn="ga-analytics">웹 애널리틱스</a>'
)
# Booking Status 뒤에 삽입, 없으면 트렌드 리포트 뒤
AFTER_BOOKING = re.compile(r'(<a class="gsn-item"[^>]*data-gsn="booking"[^>]*>[^<]*</a>)')
AFTER_TREND = re.compile(r'(<a class="gsn-item"[^>]*data-gsn="trend"[^>]*>[^<]*</a>)')
ACTIVE_CLAUSE = "else if(k==='ga-analytics')hit=h.indexOf('ga-analytics.html')!==-1;"
ACTIVE_RE = re.compile(r"(if\(hit\)a\.classList\.add\('active'\);)")


def main():
    changed = []
    for f in sorted(DOCS.glob("*.html")):
        txt = f.read_text(encoding="utf-8")
        orig = txt
        if 'data-gsn="ga-analytics"' not in txt:
            m = AFTER_BOOKING.search(txt) or AFTER_TREND.search(txt)
            if m:
                ls = txt.rfind("\n", 0, m.start()) + 1
                indent = txt[ls:m.start()]
                txt = txt[:m.end()] + "\n" + indent + NEW_ANCHOR + txt[m.end():]
        if "k==='ga-analytics'" not in txt:
            txt, _ = ACTIVE_RE.subn(ACTIVE_CLAUSE + r"\1", txt, count=1)
        if txt != orig:
            f.write_text(txt, encoding="utf-8")
            changed.append(f.name)
    print(f"패치된 파일 {len(changed)}개")
    for c in changed:
        print("  " + c)
    if not changed:
        print("  (변경 없음 — 이미 반영됨)")


if __name__ == "__main__":
    main()
