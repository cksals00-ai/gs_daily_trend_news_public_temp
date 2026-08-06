#!/usr/bin/env python3
"""
normalize_gsn_nav.py — 전 docs/*.html 상단 GSN 네비를 단일 정본으로 통일.

상단 네비 마크업은 페이지마다 하드코딩(공유 include 없음)이라 시간이 지나며
항목 수/순서/active 스크립트가 페이지별로 갈라졌다. 이 스크립트가 정본
(CANONICAL_ITEMS + CANONICAL_SCRIPT)을 단일 소스로 두고 전 페이지에 재주입한다.

  - <nav class="gsn"> 를 가진 페이지만 대상(로그인/키인/제안서 등 네비가
    원래 없는 페이지는 건드리지 않는다).
  - .gsn-links 컨테이너 안쪽 앵커 전체를 정본으로 교체 → 항목/순서/라벨 통일.
  - active-detection 스크립트도 정본으로 교체(없으면 </nav> 뒤에 삽입).
  - 페이지별 .gsn CSS(테마)는 보존한다.
  - 멱등: 이미 정본이면 변경 없음.

숨김 메뉴(대외비 gs-segment-analysis 등)는 앵커를 남기고 js/menu-visibility.js
(GSN_HIDDEN_MENUS)가 클라이언트에서 감춘다 — 기존 정책 그대로.
"""
import re
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parent.parent / "docs"
BASE = "https://cksals00-ai.github.io/gs_daily_trend_news_public_temp"
COMP = "https://cksals00-ai.github.io/sono-competitor-crawler"

# (data-gsn, href, label, extra_attrs)
# 순서 = 화면 노출 순서. segment-analysis 는 대외비(숨김 메뉴)라 마크업만 유지.
CANONICAL_ITEMS = [
    ("trend",            f"{BASE}/",                        "트렌드 리포트", ""),
    ("sales-kpi",        f"{BASE}/sales-kpi.html",          "세일즈 KPI 현황 리포트", ""),
    ("strategy",         f"{BASE}/gs-strategy-report.html", "전략 리포트", ""),
    ("otb",              f"{BASE}/otb.html",                "GS 실적 리포트", ""),
    ("ga-analytics",     f"{BASE}/ga-analytics.html",       "웹 애널리틱스", ""),
    ("segment-analysis", f"{BASE}/gs-segment-analysis.html", "전체 세그 분석 자료", ""),
    ("closing",          f"{BASE}/gs-closing-report.html",  "마감리포트", ""),
    ("monitor",          f"{COMP}/",                        "경쟁사 모니터링", ""),
    ("palatium",         f"{COMP}/palatium.html",           "팔라티움 해운대 by sonofelice 현황 리포트", ""),
    ("outbound",         "https://sono-overseas-dash.higgsfield.app/dashboard",
     "아웃바운드", ' target="_blank" rel="noopener noreferrer"'),
]

CANONICAL_SCRIPT = (
    "<script>(function(){var h=location.href;"
    "document.querySelectorAll('.gsn-item[data-gsn]').forEach(function(a){"
    "var k=a.getAttribute('data-gsn'),hit=false;"
    "if(k==='trend')hit=h.indexOf('gs_daily_trend_news_public_temp')!==-1&&"
    "(h.match(/\\/(?:index\\.html)?(?:[?#]|$)/)||h.match(/gs_daily_trend_news_public_temp\\/?$/));"
    "else if(k==='sales-kpi')hit=h.indexOf('sales-kpi.html')!==-1;"
    "else if(k==='strategy')hit=h.indexOf('gs-strategy-report.html')!==-1;"
    "else if(k==='otb')hit=h.indexOf('otb.html')!==-1;"
    "else if(k==='ga-analytics')hit=h.indexOf('ga-analytics.html')!==-1;"
    "else if(k==='segment-analysis')hit=h.indexOf('gs-segment-analysis.html')!==-1;"
    "else if(k==='closing')hit=h.indexOf('gs-closing-report.html')!==-1;"
    "else if(k==='monitor')hit=h.indexOf('sono-competitor-crawler')!==-1&&h.indexOf('palatium')===-1;"
    "else if(k==='palatium')hit=h.indexOf('palatium.html')!==-1;"
    "if(hit)a.classList.add('active');});})();</script>"
)

# 기존 active-detection 스크립트(형태가 페이지마다 조금씩 다름)
ACTIVE_RE = re.compile(
    r"<script>\(function\(\)\{var h=location\.href;"
    r"document\.querySelectorAll\('\.gsn-item\[data-gsn\]'\).*?</script>",
    re.DOTALL,
)


def build_anchors(indent: str) -> str:
    out = []
    for key, href, label, extra in CANONICAL_ITEMS:
        out.append(
            f'{indent}<a class="gsn-item" href="{href}" data-gsn="{key}"{extra}>{label}</a>'
        )
    return "\n".join(out)


def normalize(path: Path):
    """returns (changed: bool, note: str)"""
    txt = path.read_text(encoding="utf-8")
    if '<nav class="gsn">' not in txt or 'class="gsn-links"' not in txt:
        return False, "no-gsn-nav"
    orig = txt

    # ── 1) .gsn-links 안쪽 앵커 전체 교체 ────────────────────────────────
    i = txt.index('<div class="gsn-links"')
    line_start = txt.rfind("\n", 0, i) + 1
    div_indent = txt[line_start:i]
    open_end = txt.index(">", i) + 1
    close = txt.index("</div>", open_end)   # 앵커만 들어있어 중첩 div 없음
    anchors = build_anchors(div_indent + "  ")
    txt = txt[:open_end] + "\n" + anchors + "\n" + div_indent + txt[close:]

    # ── 2) active-detection 스크립트 교체(없으면 </nav> 뒤 삽입) ─────────
    if ACTIVE_RE.search(txt):
        txt = ACTIVE_RE.sub(lambda m: CANONICAL_SCRIPT, txt, count=1)
    else:
        nav_close = txt.index("</nav>", txt.index('<div class="gsn-links"'))
        ins = nav_close + len("</nav>")
        txt = txt[:ins] + "\n" + CANONICAL_SCRIPT + txt[ins:]

    # ── 3) 숨김 메뉴 스크립트 보장(대외비 항목이 노출되지 않도록) ────────
    if "js/menu-visibility.js" not in txt and "</body>" in txt:
        b = txt.rfind("</body>")
        txt = txt[:b] + '<script src="./js/menu-visibility.js"></script>\n' + txt[b:]

    if txt == orig:
        return False, "already-canonical"
    path.write_text(txt, encoding="utf-8")
    return True, "normalized"


def main():
    changed, same, skipped = [], [], []
    for f in sorted(DOCS.glob("*.html")):
        ok, note = normalize(f)
        if note == "no-gsn-nav":
            skipped.append(f.name)
        elif ok:
            changed.append(f.name)
        else:
            same.append(f.name)

    print(f"[gsn-nav] 통일 {len(changed)} / 이미동일 {len(same)} / 네비없음(제외) {len(skipped)}")
    for n in changed:
        print(f"  ~ {n}")
    if skipped:
        print("  (제외) " + ", ".join(skipped))

    # ── 검증: 대상 전 페이지가 동일한 앵커 시퀀스인지 ────────────────────
    print("\n=== 검증 ===")
    want = [k for k, _, _, _ in CANONICAL_ITEMS]
    bad = 0
    for f in sorted(DOCS.glob("*.html")):
        t = f.read_text(encoding="utf-8")
        if '<nav class="gsn">' not in t:
            continue
        got = re.findall(r'<a class="gsn-item"[^>]*data-gsn="([^"]+)"', t)
        if got != want:
            print(f"  MISMATCH {f.name}: {got}")
            bad += 1
    print(f"  일치 여부: {'전부 동일' if bad == 0 else f'{bad}개 불일치'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
