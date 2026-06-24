#!/usr/bin/env python3
"""
GSN 네비게이터에 '세일즈 KPI 현황 리포트' 메뉴를 '트렌드 리포트' 바로 오른쪽에 삽입.

- docs/*.html 모든 페이지의 GSN 네비(.gsn-item) + 모바일 시트탭이 페이지마다 하드코딩되어 있으므로
  전 페이지를 순회하며 (1) 앵커 삽입 (2) active-detection 스크립트에 클로즈 추가.
- 멱등: 이미 들어가 있으면 건너뜀.
- 오픈 레벨은 GS 실적 리포트(otb.html)와 동일(meta auth-required=executive). 네비 자체는 레벨 게이팅이 없으므로 위치만 추가.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

BASE = "https://cksals00-ai.github.io/gs_daily_trend_news_public_temp"
NEW_ANCHOR = (
    f'<a class="gsn-item" href="{BASE}/sales-kpi.html" '
    'data-gsn="sales-kpi">세일즈 KPI 현황 리포트</a>'
)
# 트렌드 리포트 앵커(data-gsn="trend") 바로 뒤에 삽입
TREND_RE = re.compile(
    r'(<a class="gsn-item"[^>]*data-gsn="trend"[^>]*>[^<]*</a>)'
)
# active-detection: 'if(hit)a.classList.add' 직전에 sales-kpi 클로즈 삽입
ACTIVE_CLAUSE = "else if(k==='sales-kpi')hit=h.indexOf('sales-kpi.html')!==-1;"
ACTIVE_RE = re.compile(r"(if\(hit\)a\.classList\.add\('active'\);)")


def main():
    changed = []
    for f in sorted(DOCS.glob("*.html")):
        txt = f.read_text(encoding="utf-8")
        orig = txt

        # (1) 앵커 삽입 — 이미 있으면 skip
        if 'data-gsn="sales-kpi"' not in txt:
            m = TREND_RE.search(txt)
            if m:
                # 들여쓰기(앞 공백) 보존하여 줄바꿈 삽입
                line_start = txt.rfind("\n", 0, m.start()) + 1
                indent = txt[line_start:m.start()]
                txt = (
                    txt[:m.end()]
                    + "\n" + indent + NEW_ANCHOR
                    + txt[m.end():]
                )

        # (2) active-detection 클로즈 삽입 — 이미 있으면 skip
        if "k==='sales-kpi'" not in txt:
            txt, n = ACTIVE_RE.subn(
                ACTIVE_CLAUSE + r"\1", txt, count=1
            )

        if txt != orig:
            f.write_text(txt, encoding="utf-8")
            nav_n = txt.count('data-gsn="')
            changed.append(f"  {f.name}  (data-gsn 개수: {nav_n})")

    print(f"패치된 파일 {len(changed)}개:")
    for c in changed:
        print(c)
    if not changed:
        print("  (변경 없음 — 이미 모두 반영됨)")


if __name__ == "__main__":
    main()
