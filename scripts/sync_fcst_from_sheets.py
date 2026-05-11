#!/usr/bin/env python3
"""
구글시트(Apps Script 웹앱) → docs/data/admin_input.json fcst_history 머지.

빌드 파이프라인 앞단에서 실행하면, 세일즈가 fcst-admin.html에서 키인한 값이
중앙 시트에 모이고, 이 스크립트가 그걸 admin_input.json으로 끌어와
generate_fcst.py / 빌드 파이프라인이 반영하도록 한다.

사용법:
  # URL은 환경변수 또는 인자로
  export FCST_SHEET_URL="https://script.google.com/macros/s/..../exec"
  python scripts/sync_fcst_from_sheets.py

  # 또는
  python scripts/sync_fcst_from_sheets.py --url "https://script.google.com/..." --dry-run

Apps Script 코드: scripts/apps_script_fcst_keyin.js
시트 구조: timestamp | week | month | property | segment | metric | value | updated_by
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ADMIN_INPUT_PATH = Path(__file__).resolve().parent.parent / "docs" / "data" / "admin_input.json"


def fetch_entries(url: str, timeout: int = 30) -> list[dict]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
    payload = json.loads(body)
    if payload.get("status") != "ok":
        raise RuntimeError(f"Apps Script error: {payload}")
    return payload.get("entries", [])


def entries_to_history(entries: list[dict]) -> dict:
    """평탄한 시트 row → fcst-admin.html이 쓰는 fcst_history 구조로 변환."""
    history: dict[str, dict] = {}
    for e in entries:
        week = str(e.get("week") or "").strip()
        prop = str(e.get("property") or "").strip()
        seg = str(e.get("segment") or "").strip()
        metric = str(e.get("metric") or "").strip().upper()
        month_iso = str(e.get("month") or "").strip()  # "2026-05"
        if not (week and prop and seg and metric and month_iso):
            continue
        try:
            value = float(e.get("value"))
        except (TypeError, ValueError):
            continue
        # "2026-05" → "5"
        try:
            month_num = str(int(month_iso.split("-")[1]))
        except (IndexError, ValueError):
            continue
        # 정수로 저장 (HTML이 parseInt로 다루므로)
        ivalue = int(round(value))

        # key 포맷: RN = "사업장|세그|월", ADR = "사업장|세그|ADR|월"
        if metric == "ADR":
            key = f"{prop}|{seg}|ADR|{month_num}"
        else:  # RN / RNS
            key = f"{prop}|{seg}|{month_num}"

        wd = history.setdefault(
            week,
            {
                "month": month_num,
                "week_label": week,
                "created": (e.get("timestamp") or "")[:10],
                "keyin": {},
            },
        )
        wd["keyin"][key] = ivalue
    return history


def merge_into_admin_input(history: dict, dry_run: bool = False) -> tuple[int, int]:
    """admin_input.json fcst_history 머지. (added, updated) 반환."""
    if ADMIN_INPUT_PATH.exists():
        with ADMIN_INPUT_PATH.open("r", encoding="utf-8") as f:
            admin = json.load(f)
    else:
        admin = {}

    existing = admin.get("fcst_history") or {}
    if not isinstance(existing, dict):
        existing = {}
    # 메타 필드 보존 ("_description" 등)
    meta = {k: v for k, v in existing.items() if k.startswith("_")}

    added = updated = 0
    for week, wd in history.items():
        cur = existing.get(week)
        if not isinstance(cur, dict):
            existing[week] = wd
            added += 1
            continue
        cur_keyin = cur.get("keyin") or {}
        new_keyin = wd.get("keyin") or {}
        # 시트값이 신뢰소스 — 시트에 있는 키는 시트값으로 덮어쓰기
        before = dict(cur_keyin)
        cur_keyin.update(new_keyin)
        cur["keyin"] = cur_keyin
        cur["month"] = wd.get("month") or cur.get("month")
        cur["week_label"] = wd.get("week_label") or cur.get("week_label")
        cur["created"] = wd.get("created") or cur.get("created")
        if before != cur_keyin:
            updated += 1

    existing.update(meta)
    admin["fcst_history"] = existing
    admin["_updated_at"] = datetime.now(timezone.utc).isoformat()

    if dry_run:
        print(json.dumps(admin.get("fcst_history"), ensure_ascii=False, indent=2))
        return added, updated

    ADMIN_INPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with ADMIN_INPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(admin, f, ensure_ascii=False, indent=2)
    return added, updated


def main() -> int:
    ap = argparse.ArgumentParser(description="FCST 키인 구글시트 → admin_input.json")
    ap.add_argument("--url", help="Apps Script 웹앱 URL (없으면 FCST_SHEET_URL env)")
    ap.add_argument("--dry-run", action="store_true", help="파일 저장 없이 결과만 출력")
    args = ap.parse_args()

    url = args.url or os.environ.get("FCST_SHEET_URL", "").strip()
    if not url:
        print("ERROR: --url 또는 FCST_SHEET_URL 환경변수 필요", file=sys.stderr)
        return 2

    print(f"[fcst-sync] GET {url}", file=sys.stderr)
    entries = fetch_entries(url)
    print(f"[fcst-sync] received {len(entries)} entries", file=sys.stderr)

    history = entries_to_history(entries)
    print(f"[fcst-sync] grouped into {len(history)} weeks", file=sys.stderr)

    added, updated = merge_into_admin_input(history, dry_run=args.dry_run)
    suffix = " (dry-run)" if args.dry_run else ""
    print(f"[fcst-sync] merged: +{added} new weeks, {updated} weeks updated{suffix}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
