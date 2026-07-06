#!/usr/bin/env python3
"""
GAS(데일리 키인 서버저장) → admin_input.json 브리지.

admin.html 「⬆ 반영」이 GAS(ADMIN_INPUT 시트)에 저장한 데일리 키인을
빌드 파이프라인 앞단에서 끌어와 data/admin_input.json + docs/data/admin_input.json 에
머지한다. 이후 build.py 가 이 값(action_alerts, selected_insights, headline 등)을
공개 리포트에 반영한다. (FCST용 sync_fcst_from_sheets.py 와 동일한 역할)

인증: 마스터 AUTH_SECRET 을 CI 에 노출하지 않기 위해, 읽기 전용
`?action=export_admin&key=<KEY>` 엔드포인트를 사용한다(GAS 스크립트 속성 SYNC_KEY 와 대조).

환경변수:
  ADMIN_SYNC_KEY    (필수) GAS SYNC_KEY 와 동일한 키. 없으면 스킵(exit 0).
  ADMIN_SYNC_URL    (선택) GAS 웹앱 exec URL. 없으면 아래 DEFAULT 사용.

사용:
  ADMIN_SYNC_KEY=xxx python scripts/sync_admin_from_server.py
  python scripts/sync_admin_from_server.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "admin_input.json"
DOCS_PATH = ROOT / "docs" / "data" / "admin_input.json"

# auth.js 의 DEFAULT_AUTH_API_URL 과 동일 (배포 URL 바뀌면 ADMIN_SYNC_URL 로 덮어씀)
DEFAULT_URL = (
    "https://script.google.com/macros/s/"
    "AKfycbz9JwH0BJfcH-AVo9Vy1EYasER5jAz_5ZL9e2v22PtrGZ7Yb5ATbJLuUJ9UvGDjv07MJA/exec"
)

# admin 패널(admin.html getFormData)이 소유하는 필드만 서버값으로 덮어쓴다.
# 빌드/정적 관리 필드(fcst_history, fcst_keyin, daily_insights, selected_headline,
# _description)는 절대 건드리지 않는다.
KEYIN_FIELDS = [
    "report_date",
    "selected_insights",
    "headline",
    "notices",
    "weekly_key_issues",
    "selected_strategies",
    "action_alerts",
    "property_strategy",
    "cancel_match_window",
    "cancel_rate_criteria",
    "fcst_tolerance_pct",
    "commission_rates",
    "_updated_by",
]


def fetch_server_admin(url: str, key: str, timeout: int = 30) -> dict | None:
    q = urllib.parse.urlencode({"action": "export_admin", "key": key})
    full = f"{url}?{q}"
    req = urllib.request.Request(full, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        # GAS 콜드스타트/리다이렉트 HTML → 일시 실패로 간주
        raise RuntimeError("서버가 JSON 대신 HTML 반환(콜드스타트 추정)")
    if payload.get("status") != "ok":
        raise RuntimeError(f"export_admin 오류: {payload.get('message') or payload}")
    return payload.get("data")  # None 이면 저장분 없음


def load_json(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def parse_ts(s) -> float:
    if not s or not isinstance(s, str):
        return 0.0
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def merge_and_write(server: dict, dry_run: bool = False) -> str:
    base = load_json(DATA_PATH)

    server_ts = parse_ts(server.get("_updated_at"))
    base_ts = parse_ts(base.get("_updated_at"))
    if base_ts and server_ts and server_ts < base_ts:
        return (f"skip: 정적본(_updated_at={base.get('_updated_at')})이 "
                f"서버({server.get('_updated_at')})보다 최신")

    applied = []
    for f in KEYIN_FIELDS:
        if f in server:
            if base.get(f) != server[f]:
                applied.append(f)
            base[f] = server[f]
    base["_updated_at"] = server.get("_updated_at") or datetime.now(timezone.utc).isoformat()

    if dry_run:
        return f"dry-run: 적용 예정 필드={applied or '(변경 없음)'}"

    payload = json.dumps(base, ensure_ascii=False, indent=2)
    for p in (DATA_PATH, DOCS_PATH):
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(payload, encoding="utf-8")
    return f"applied: {applied or '(변경 없음)'} → {DATA_PATH.name}, docs/data/{DOCS_PATH.name}"


def main() -> int:
    ap = argparse.ArgumentParser(description="GAS 데일리 키인 → admin_input.json 브리지")
    ap.add_argument("--url", help="GAS exec URL (없으면 ADMIN_SYNC_URL env 또는 DEFAULT)")
    ap.add_argument("--key", help="SYNC_KEY (없으면 ADMIN_SYNC_KEY env)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    key = args.key or os.environ.get("ADMIN_SYNC_KEY", "").strip()
    if not key:
        print("[admin-sync] ADMIN_SYNC_KEY 없음 → 스킵(정적 admin_input.json 그대로 사용)",
              file=sys.stderr)
        return 0  # 시크릿 미설정 시 빌드 깨지 않고 조용히 스킵

    url = (args.url or os.environ.get("ADMIN_SYNC_URL", "").strip() or DEFAULT_URL)

    print(f"[admin-sync] GET export_admin @ {url}", file=sys.stderr)
    try:
        server = fetch_server_admin(url, key)
    except Exception as e:
        print(f"[admin-sync] 서버 조회 실패({e}) → 스킵", file=sys.stderr)
        return 0  # 서버 일시장애로 빌드 깨지 않음

    if not server or not isinstance(server, dict):
        print("[admin-sync] 서버에 저장된 키인 없음 → 스킵", file=sys.stderr)
        return 0

    msg = merge_and_write(server, dry_run=args.dry_run)
    print(f"[admin-sync] {msg}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
