#!/usr/bin/env python3
"""
docs/*.html 파일들에 인증 가드 메타 + js/auth.js 스크립트를 주입한다.

주입 위치: <head> 시작 직후
주입 내용:
    <meta name="auth-required" content="<role>">
    <script src="./js/auth.js"></script>

이미 주입되어 있으면 role 만 갱신한다.
"""
from __future__ import annotations
import re
from pathlib import Path

DOCS = Path(__file__).resolve().parent.parent / "docs"

# 페이지 → 요구 권한 매핑
PAGE_ROLES: dict[str, str] = {
    # ── admin 전용 ────────────────────────────────────────────────────
    "admin.html":                       "admin",
    "admin-users.html":                 "admin",
    "gs-map.html":                      "admin",
    "gs-strategy.html":                 "admin",
    "gs-strategy-report.html":          "admin",
    "strategy-keyin.html":              "admin",
    "inbound-strategy-keyin.html":      "admin",
    "inbound-nationality-keyin.html":   "admin",
    "data-check.html":                  "admin",
    "gs-verify.html":                   "admin",

    # ── sales 이상 (admin + sales) ────────────────────────────────────
    "fcst-admin.html":                  "sales",
    "gs-sales.html":                    "sales",
    "gs-sales-report.html":             "sales",
    "sales-guide.html":                 "sales",
    "gs-request.html":                  "sales",
    "gs-proposal.html":                 "sales",
    "rate-converter.html":              "sales",
    "inbound-nationality-keyin-sales.html": "sales",
    "channel-mapping.html":             "sales",

    # ── executive 이상 (admin + sales + executive) ────────────────────
    "index.html":                       "executive",
    "channel.html":                     "executive",
    "property-report.html":             "executive",
    "monthly.html":                     "executive",
    "weekly-compare.html":              "executive",
    "ytd.html":                         "executive",
    "otb.html":                         "executive",
    "booking-status.html":              "executive",
    "booking-trend.html":               "executive",
    "stay-date.html":                   "executive",
    "ly-product.html":                  "executive",
    "ly-trend.html":                    "executive",
    "product-detail.html":              "executive",
    "overseas.html":                    "executive",
    "pumui.html":                       "executive",
    "fcst-status.html":                 "executive",
    "fcst-trend.html":                  "executive",
    "manager-analysis.html":            "executive",
    "gs-report.html":                   "executive",
    "gs-gsa-report.html":               "executive",
    "gs-closing-report.html":           "executive",
    "action_plan_dashboard.html":       "executive",
    "verification-tracker.html":        "executive",
}

# login.html 은 가드를 적용하지 않음 (로그인 자체 페이지)
SKIP = {"login.html"}

GUARD_BLOCK_TEMPLATE = (
    '<meta name="auth-required" content="{role}">\n'
    '<script src="./js/auth.js"></script>'
)

GUARD_RE = re.compile(
    r'<meta\s+name=["\']auth-required["\']\s+content=["\'][^"\']*["\']\s*/?>\s*'
    r'<script\s+src=["\']\./js/auth\.js["\']\s*></script>',
    re.IGNORECASE,
)

META_ONLY_RE = re.compile(
    r'<meta\s+name=["\']auth-required["\']\s+content=["\']([^"\']*)["\']\s*/?>',
    re.IGNORECASE,
)

HEAD_OPEN_RE = re.compile(r'<head(\s[^>]*)?>', re.IGNORECASE)


def inject(html: str, role: str) -> tuple[str, str]:
    """반환: (new_html, action) action in {'updated','inserted','rewritten','same'}."""
    block = GUARD_BLOCK_TEMPLATE.format(role=role)

    # 이미 가드 블록이 있는 경우 → role 만 갱신
    if GUARD_RE.search(html):
        new = GUARD_RE.sub(block, html, count=1)
        return new, ("same" if new == html else "updated")

    # auth-required 메타만 있고 스크립트가 빠진 경우 → 전체 블록으로 치환
    if META_ONLY_RE.search(html):
        new = META_ONLY_RE.sub(block, html, count=1)
        return new, "rewritten"

    # <head> 시작 직후 삽입
    m = HEAD_OPEN_RE.search(html)
    if not m:
        # head 가 없는 비정상 파일 - 건너뜀
        return html, "no-head"
    insert_at = m.end()
    new = html[:insert_at] + "\n" + block + html[insert_at:]
    return new, "inserted"


def main() -> int:
    changed = 0
    skipped = 0
    for name, role in sorted(PAGE_ROLES.items()):
        p = DOCS / name
        if not p.exists():
            print(f"  SKIP (missing)  {name}")
            skipped += 1
            continue
        original = p.read_text(encoding="utf-8")
        new, action = inject(original, role)
        if action in ("inserted", "updated", "rewritten"):
            p.write_text(new, encoding="utf-8")
            print(f"  {action:10s} {name:42s} role={role}")
            changed += 1
        else:
            print(f"  {action:10s} {name:42s} role={role}")
    print(f"\nDone. changed={changed}, skipped={skipped}, total={len(PAGE_ROLES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
