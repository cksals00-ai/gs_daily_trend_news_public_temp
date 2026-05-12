#!/usr/bin/env python3
"""
data/raw_db/ 디렉토리의 txt 파일 변경 감지 스크립트.

사용법:
    python3 scripts/check_raw_db_changes.py [--wait] [--interval 600] [--max-wait 7200]

옵션:
    --wait        변경이 없으면 대기 후 재체크 (없으면 1회 체크 후 종료)
    --interval    재체크 간격 (초, 기본값: 600 = 10분)
    --max-wait    최대 대기 시간 (초, 기본값: 7200 = 2시간)

종료 코드:
    0 = 변경 감지됨 (빌드 진행 가능)
    1 = 최대 대기 시간 초과, 변경 없음
    2 = 에러 발생
"""

import os
import sys
import json
import time
import hashlib
import argparse
from pathlib import Path
from datetime import datetime


RAW_DB_DIR = Path(__file__).resolve().parent.parent / "data" / "raw_db"
SNAPSHOT_FILE = Path(__file__).resolve().parent.parent / "data" / ".raw_db_snapshot.json"


def get_current_snapshot():
    """data/raw_db/ 하위의 모든 txt 파일에 대해 {상대경로: mtime} 딕셔너리 반환."""
    snapshot = {}
    if not RAW_DB_DIR.exists():
        print(f"[ERROR] raw_db 디렉토리 없음: {RAW_DB_DIR}", file=sys.stderr)
        sys.exit(2)

    for txt_file in sorted(RAW_DB_DIR.rglob("*.txt")):
        rel = str(txt_file.relative_to(RAW_DB_DIR))
        try:
            stat = txt_file.stat()
            snapshot[rel] = {
                "mtime": stat.st_mtime,
                "size": stat.st_size,
            }
        except OSError:
            continue
    return snapshot


def load_previous_snapshot():
    """이전 스냅샷 파일 로드. 없으면 빈 딕셔너리."""
    if SNAPSHOT_FILE.exists():
        try:
            with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_snapshot(snapshot):
    """현재 스냅샷을 파일로 저장."""
    SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)


def detect_changes(prev, curr):
    """변경 사항 감지. 변경된 파일 목록과 요약을 반환."""
    changes = {
        "added": [],
        "modified": [],
        "removed": [],
    }

    prev_keys = set(prev.keys())
    curr_keys = set(curr.keys())

    # 새로 추가된 파일
    for k in sorted(curr_keys - prev_keys):
        changes["added"].append(k)

    # 삭제된 파일
    for k in sorted(prev_keys - curr_keys):
        changes["removed"].append(k)

    # 변경된 파일 (mtime 또는 size 변경)
    for k in sorted(prev_keys & curr_keys):
        p = prev[k]
        c = curr[k]
        if p.get("mtime") != c.get("mtime") or p.get("size") != c.get("size"):
            changes["modified"].append(k)

    has_changes = any(changes[cat] for cat in changes)
    return has_changes, changes


def print_changes(changes):
    """변경 사항 출력."""
    if changes["added"]:
        print(f"  [추가] {len(changes['added'])}개 파일:")
        for f in changes["added"]:
            print(f"    + {f}")
    if changes["modified"]:
        print(f"  [수정] {len(changes['modified'])}개 파일:")
        for f in changes["modified"]:
            print(f"    ~ {f}")
    if changes["removed"]:
        print(f"  [삭제] {len(changes['removed'])}개 파일:")
        for f in changes["removed"]:
            print(f"    - {f}")


def main():
    parser = argparse.ArgumentParser(description="raw_db txt 파일 변경 감지")
    parser.add_argument("--wait", action="store_true", help="변경 없으면 대기 후 재체크")
    parser.add_argument("--interval", type=int, default=600, help="재체크 간격 (초, 기본: 600)")
    parser.add_argument("--max-wait", type=int, default=7200, help="최대 대기 시간 (초, 기본: 7200)")
    args = parser.parse_args()

    start_time = time.time()
    attempt = 0

    while True:
        attempt += 1
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{now}] === 변경 감지 체크 (시도 #{attempt}) ===")

        prev_snapshot = load_previous_snapshot()
        curr_snapshot = get_current_snapshot()

        print(f"  이전 스냅샷: {len(prev_snapshot)}개 파일")
        print(f"  현재 스냅샷: {len(curr_snapshot)}개 파일")

        # 첫 실행 (이전 스냅샷 없음) → 스냅샷 저장 후 변경으로 간주
        if not prev_snapshot:
            print("  [INFO] 이전 스냅샷 없음 → 초기 스냅샷 저장 후 빌드 진행")
            save_snapshot(curr_snapshot)
            sys.exit(0)

        has_changes, changes = detect_changes(prev_snapshot, curr_snapshot)

        if has_changes:
            total = len(changes["added"]) + len(changes["modified"]) + len(changes["removed"])
            print(f"  [OK] 변경 감지됨! ({total}개 파일)")
            print_changes(changes)
            # 새 스냅샷 저장
            save_snapshot(curr_snapshot)
            sys.exit(0)

        # 변경 없음
        elapsed = time.time() - start_time
        remaining = args.max_wait - elapsed

        if not args.wait or remaining <= 0:
            if args.wait:
                print(f"  [TIMEOUT] 최대 대기 시간 {args.max_wait}초 초과. 변경 없음.")
            else:
                print("  [INFO] 변경 없음 (단일 체크 모드).")
            sys.exit(1)

        wait_secs = min(args.interval, remaining)
        next_check = datetime.fromtimestamp(time.time() + wait_secs).strftime("%H:%M:%S")
        print(f"  [WAIT] 변경 없음. {int(wait_secs)}초 후 재체크 (다음: {next_check}, 남은 대기: {int(remaining)}초)")
        time.sleep(wait_secs)


if __name__ == "__main__":
    main()
