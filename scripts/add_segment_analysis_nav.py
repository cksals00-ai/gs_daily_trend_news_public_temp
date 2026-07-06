#!/usr/bin/env python3
"""
GSN 네비게이터에 '전체 세그 분석 자료' 메뉴를 'FCST 추이' 바로 오른쪽에 삽입.

- docs/*.html 모든 페이지의 GSN 네비(.gsn-item)가 페이지마다 하드코딩되어 있으므로
  전 페이지를 순회하며 (1) 앵커 삽입 (2) active-detection 스크립트에 클로즈 추가.
- 멱등: 이미 들어가 있으면 건너뜀.
- 오픈 레벨은 FCST 추이/세일즈 KPI 등 분석 리포트와 동일(meta auth-required=executive).
  네비 자체는 레벨 게이팅이 없으므로 위치만 추가한다.
- add_sales_kpi_nav.py 패턴 이식.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

BASE = "https://cksals00-ai.github.io/gs_daily_trend_news_public_temp"
NEW_ANCHOR = (
    f'<a class="gsn-item" href="{BASE}/gs-segment-analysis.html" '
    'data-gsn="segment-analysis">전체 세그 분석 자료</a>'
)
# FCST 추이 앵커(data-gsn="fcst-trend") 바로 뒤에 삽입
AFTER_RE = re.compile(
    r'(<a class="gsn-item"[^>]*data-gsn="fcst-trend"[^>]*>[^<]*</a>)'
)
# 폴백: 트렌드 리포트 뒤(위 앵커가 없는 페이지 대비)
FALLBACK_RE = re.compile(
    r'(<a class="gsn-item"[^>]*data-gsn="trend"[^>]*>[^<]*</a>)'
)
# active-detection: 'if(hit)a.classList.add' 직전에 클로즈 삽입
ACTIVE_CLAUSE = "else if(k==='segment-analysis')hit=h.indexOf('gs-segment-analysis.html')!==-1;"
ACTIVE_RE = re.compile(r"(if\(hit\)a\.classList\.add\('active'\);)")

# 커밋 오염 방지용 제외 목록(필요 시). 기본 비어 있음.
EXCLUDE = set()


def main():
    changed = []
    skipped_exclude = []
    for f in sorted(DOCS.glob("*.html")):
        if f.name in EXCLUDE:
            skipped_exclude.append(f.name)
            continue
        txt = f.read_text(encoding="utf-8")
        orig = txt

        # (1) 앵커 삽입 — 이미 있으면 skip
        if 'data-gsn="segment-analysis"' not in txt:
            m = AFTER_RE.search(txt) or FALLBACK_RE.search(txt)
            if m:
                line_start = txt.rfind("\n", 0, m.start()) + 1
                indent = txt[line_start:m.start()]
                txt = txt[:m.end()] + "\n" + indent + NEW_ANCHOR + txt[m.end():]

        # (2) active-detection 클로즈 삽입 — 이미 있으면 skip
        if "k==='segment-analysis'" not in txt:
            txt, _ = ACTIVE_RE.subn(ACTIVE_CLAUSE + r"\1", txt, count=1)

        if txt != orig:
            f.write_text(txt, encoding="utf-8")
            nav_n = len(re.findall(r'<a class="gsn-item"', txt))
            changed.append(f"  {f.name}  (nav 항목 {nav_n}개)")

    print(f"패치된 파일 {len(changed)}개:")
    for c in changed:
        print(c)
    if not changed:
        print("  (변경 없음 — 이미 모두 반영됨)")
    if skipped_exclude:
        print(f"제외(무관 변경 보호): {', '.join(skipped_exclude)}")


if __name__ == "__main__":
    main()
