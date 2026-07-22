#!/usr/bin/env python3
"""
inject_visit_log.py — 모든 docs/*.html 에 visit-log.js include 를 주입.

  - visit-log.js 가 페이지뷰를 GAS 웹앱에 1회 전송(1개월 사용도 트라이얼).
  - 멱등: 이미 include 가 있으면 건너뛴다.
  - build.py 의 페이지 재생성 이후에도 include 가 유지되도록 build.py 말미에서
    inject_menu_visibility 와 함께 호출한다. 단독 실행도 가능.

  로그인/비인증 페이지 포함 전 페이지에 넣는다(비로그인 방문도 anon 으로 집계).
  트라이얼 종료 시: 이 스크립트로 되돌릴 수 없으므로, 제거는
    git 되돌리기 또는 아래 remove() 로 <script ... visit-log.js></script> 라인 삭제.
"""
import sys
from pathlib import Path

DOCS = Path(__file__).resolve().parent.parent / "docs"
INCLUDE = '<script src="./js/visit-log.js"></script>'


def inject_one(html_file: Path) -> bool:
    content = html_file.read_text(encoding="utf-8")
    if "js/visit-log.js" in content:
        return False  # 이미 주입됨
    if "</body>" not in content:
        return False  # 본문 닫힘 없는 파일은 건너뜀
    idx = content.rfind("</body>")
    new = content[:idx] + INCLUDE + "\n" + content[idx:]
    html_file.write_text(new, encoding="utf-8")
    return True


def remove_one(html_file: Path) -> bool:
    content = html_file.read_text(encoding="utf-8")
    if "js/visit-log.js" not in content:
        return False
    lines = [ln for ln in content.splitlines(keepends=True)
             if "js/visit-log.js" not in ln]
    html_file.write_text("".join(lines), encoding="utf-8")
    return True


def main():
    op = remove_one if (len(sys.argv) > 1 and sys.argv[1] == "--remove") else inject_one
    changed, other = [], []
    for f in sorted(DOCS.glob("*.html")):
        (changed if op(f) else other).append(f.name)
    verb = "removed from" if op is remove_one else "injected into"
    print(f"[visit-log] {verb} {len(changed)} files, {len(other)} unchanged")
    for n in changed:
        print(f"  {'-' if op is remove_one else '+'} {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
