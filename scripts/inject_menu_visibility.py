#!/usr/bin/env python3
"""
inject_menu_visibility.py — 모든 docs/*.html 에 menu-visibility.js include 를 주입.

  - menu-visibility.js 가 GSN_HIDDEN_MENUS(숨김 메뉴 단일 소스)에 따라
    네비 링크/카드를 클라이언트에서 감춘다(페이지 파일은 보존).
  - 멱등: 이미 include 가 있으면 건너뛴다.
  - build.py 의 페이지 재생성(otb/sales-kpi 등) 이후에도 include 가 유지되도록
    build.py 말미에서 호출한다. 단독 실행도 가능.

  login.html 은 네비가 없어 제외한다.
"""
import re
import sys
from pathlib import Path

try:
    import fs_utils  # macOS NFD→NFC (다른 빌드 스크립트와 동일)
except Exception:
    fs_utils = None

DOCS = Path(__file__).resolve().parent.parent / "docs"
INCLUDE = '<script src="./js/menu-visibility.js"></script>'
SKIP = {"login.html"}


def inject_one(html_file: Path) -> bool:
    content = html_file.read_text(encoding="utf-8")
    if "js/menu-visibility.js" in content:
        return False  # 이미 주입됨
    if "</body>" not in content:
        return False  # 본문 닫힘 없는 파일은 건너뜀
    # 마지막 </body> 직전에 삽입
    idx = content.rfind("</body>")
    new = content[:idx] + INCLUDE + "\n" + content[idx:]
    html_file.write_text(new, encoding="utf-8")
    return True


def main():
    injected, skipped = [], []
    for f in sorted(DOCS.glob("*.html")):
        if f.name in SKIP:
            continue
        if inject_one(f):
            injected.append(f.name)
        else:
            skipped.append(f.name)
    print(f"[menu-visibility] injected into {len(injected)} files, "
          f"{len(skipped)} already-present/skipped")
    for n in injected:
        print(f"  + {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
