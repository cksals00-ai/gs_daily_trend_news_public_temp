#!/usr/bin/env python3
"""
검증 회차 1건을 docs/data/verification_history.json 에 append 하는 헬퍼.

사용 예:
  python3 scripts/append_verification_history.py \
      --date 2026-05-04 --time 11:00 \
      --excel "GS실시간실적현황_20260504110000.xlsx" \
      --excel-snapshot "2026-05-04 11:00" \
      --parsed-basis "5/4 07:47~07:52 온북 신규 스냅샷" \
      --verdict 정상 \
      --channel OTA=-0.1%~+1.3%,GOTA=-0.6%~+2.3%,INBOUND=-2.3%~+5.7% \
      --notes "채널별 모두 ±3% 이내 정상."

또는 --json 으로 entry 전체를 JSON 문자열로 전달.
중복 검증 (같은 verification_date + excel_file) 은 자동으로 skip.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
HISTORY_PATH = REPO_ROOT / "docs" / "data" / "verification_history.json"


def parse_channel_arg(s: str) -> dict:
    """OTA=-0.1%~+1.3%,GOTA=-0.6%~+2.3% 형식 파싱."""
    if not s:
        return {}
    out: dict = {}
    for part in s.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, rng = part.split("=", 1)
        name = name.strip()
        rng = rng.strip()
        if "~" in rng:
            lo, hi = rng.split("~", 1)
            out[name] = {
                "min_pct": lo.strip(),
                "max_pct": hi.strip(),
                "verdict": "정상",
            }
        else:
            out[name] = {"value_pct": rng, "verdict": "정상"}
    return out


def load_history() -> dict:
    if not HISTORY_PATH.exists():
        return {
            "schema_version": "1.0",
            "description": "GS 실시간 실적현황 Excel ↔ 온북 파싱 데이터 교차 검증 이력.",
            "entries": [],
        }
    with HISTORY_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_history(data: dict) -> None:
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def is_duplicate(entries: list, date: str, excel: str) -> bool:
    return any(
        e.get("verification_date") == date and e.get("excel_file") == excel
        for e in entries
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="검증 이력에 1건 append")
    ap.add_argument("--json", help="entry 전체 JSON 문자열 (이 옵션이 있으면 다른 옵션 무시)")
    ap.add_argument("--date", help="검증일 YYYY-MM-DD (KST)")
    ap.add_argument("--time", default="—", help="검증 시각 HH:MM (KST)")
    ap.add_argument("--excel", help="검증 파일명")
    ap.add_argument("--excel-snapshot", default="", help="Excel 스냅샷 시점")
    ap.add_argument("--parsed-basis", default="", help="비교 대상 파싱 데이터 시점")
    ap.add_argument("--verdict", default="정상", choices=["정상", "경고", "이상"])
    ap.add_argument("--channel", default="", help="채널별 차이율 (OTA=-0.1%~+1.3%,...)")
    ap.add_argument("--notes", default="", help="비고")
    ap.add_argument("--author", default="claude")
    ap.add_argument("--force", action="store_true", help="중복 검증도 강제 추가")
    args = ap.parse_args()

    data = load_history()
    entries = data.setdefault("entries", [])

    if args.json:
        try:
            entry = json.loads(args.json)
        except json.JSONDecodeError as e:
            print(f"JSON parse error: {e}", file=sys.stderr)
            return 2
    else:
        if not args.date or not args.excel:
            ap.error("--date 와 --excel 은 필수입니다 (또는 --json 사용)")
        entry = {
            "verification_date": args.date,
            "verification_time": args.time,
            "excel_file": args.excel,
            "excel_snapshot_at": args.excel_snapshot,
            "parsed_data_basis": args.parsed_basis,
            "verdict": args.verdict,
            "channel_diff": parse_channel_arg(args.channel),
            "notes": args.notes,
            "author": args.author,
        }

    date = entry.get("verification_date", "")
    excel = entry.get("excel_file", "")

    if not args.force and is_duplicate(entries, date, excel):
        print(f"skip: 동일 (date={date}, excel={excel}) 검증이 이미 존재. --force 로 강제 추가 가능.")
        return 0

    entries.append(entry)
    save_history(data)
    print(f"appended: {date} {entry.get('verification_time', '')} → {excel} ({entry.get('verdict')})")
    print(f"total entries: {len(entries)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
