#!/usr/bin/env python3
"""
add_sidebar_links.py — 상단 네비에서 뺀 항목을 좌측 사이드바로 이동.

  (1) GS 실적 리포트 사이드바(otb.html 및 동일 시트 목록을 공유하는 페이지)
      → FCST 추이 / Action Plan 링크 추가
  (2) 마감리포트(gs-closing-report.html) "관련 페이지"
      → 외부 리포트 분석 1 / 외부 리포트 분석 2 추가

사이드바 목록 마크업이 페이지마다 3가지 형태(<nav style=...>, .otb-nav,
.sidebar-nav)라 형제 앵커의 마크업을 그대로 복제해 붙인다.
멱등: 이미 링크가 있으면 건너뛴다.
"""
import re
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parent.parent / "docs"
COMP = "https://cksals00-ai.github.io/sono-competitor-crawler"
SHEET_LABEL = '<div class="filter-section-label">GS 실적 리포트</div>'

NEW_LINKS = [
    ("fcst-trend.html", "FCST 추이"),
    ("action_plan_dashboard.html", "Action Plan"),
]

CLOSING_LINKS = (
    f'      <a href="{COMP}/external-report.html" class="cr-sb-ext">'
    '📄 외부 리포트 분석 1<span class="cr-sb-arrow">→</span></a>\n'
    '      <a href="market-lodging-2026q2.html" class="cr-sb-ext">'
    '📚 외부 리포트 분석 2<span class="cr-sb-arrow">→</span></a>\n'
)

# 형제 앵커 한 줄: 들여쓰기 + <a ...>라벨</a>
SIBLING_RE = re.compile(r'([ \t]*)(<a\b[^>]*?>)([^<]*)(</a>)')


def clone_anchor(sample_indent: str, sample_tag: str, href: str, label: str) -> str:
    """형제 앵커 여는 태그에서 href만 갈아끼우고(active 클래스 제거) 새 앵커 생성."""
    tag = re.sub(r'href="[^"]*"', f'href="{href}"', sample_tag)
    if 'href="' not in tag:                       # href 없으면 앞에 붙임
        tag = tag.replace("<a", f'<a href="{href}"', 1)
    tag = re.sub(r'class="([^"]*?)\s*active\s*([^"]*)"', r'class="\1\2"', tag)
    tag = re.sub(r'\s+class="\s*"', "", tag)
    return f"{sample_indent}{tag}{label}</a>"


def patch_otb_family():
    changed, failed = [], []
    for f in sorted(DOCS.glob("*.html")):
        txt = f.read_text(encoding="utf-8")
        if SHEET_LABEL not in txt:
            continue
        if 'href="action_plan_dashboard.html"' in txt and 'href="fcst-trend.html"' in txt:
            continue  # 멱등

        i = txt.index(SHEET_LABEL) + len(SHEET_LABEL)
        m = re.search(r"<(nav|div)\b[^>]*>", txt[i:i + 200])
        if not m:
            failed.append(f.name)
            continue
        tag_name = m.group(1)
        open_end = i + m.end()
        close_tok = f"</{tag_name}>"
        close_at = txt.index(close_tok, open_end)   # 내부는 <a>만 → 중첩 없음

        block = txt[open_end:close_at]
        anchors = list(SIBLING_RE.finditer(block))
        if not anchors:
            failed.append(f.name)
            continue
        last = anchors[-1]
        indent, tag = last.group(1), last.group(2)

        add = "\n" + "\n".join(
            clone_anchor(indent, tag, href, label) for href, label in NEW_LINKS
        )
        insert_at = open_end + last.end()
        txt = txt[:insert_at] + add + txt[insert_at:]
        f.write_text(txt, encoding="utf-8")
        changed.append(f.name)
    return changed, failed


def patch_closing():
    f = DOCS / "gs-closing-report.html"
    txt = f.read_text(encoding="utf-8")
    if "외부 리포트 분석 1" in txt:
        return False
    anchor = '<a href="gs-strategy-report.html" class="cr-sb-ext">'
    i = txt.index(anchor)
    end = txt.index("</a>", i) + len("</a>") + 1   # 줄바꿈 포함
    f.write_text(txt[:end] + CLOSING_LINKS + txt[end:], encoding="utf-8")
    return True


def main():
    ch, bad = patch_otb_family()
    print(f"[sidebar] GS 실적 리포트 사이드바 {len(ch)}개 페이지에 FCST 추이·Action Plan 추가")
    for n in ch:
        print(f"  ~ {n}")
    if not ch:
        print("  (변경 없음 — 이미 반영)")
    if bad:
        print(f"  !! 구조 불일치로 건너뜀: {bad}")

    ok = patch_closing()
    print(f"[sidebar] 마감리포트 '관련 페이지' → 외부 리포트 분석 1·2 "
          f"{'추가' if ok else '이미 반영'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
