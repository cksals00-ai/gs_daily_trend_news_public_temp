#!/usr/bin/env python3
"""
generate_campaign_performance.py — 기획전(Key)별 실적 집계

흐름:
1) docs/data/campaign_data.json 읽기 → events[].key + events[].package_codes
2) raw_db/2026/27.*.txt (예약) + 28.*.txt (취소) 파싱
3) 패키지코드(=회원번호 86XXXXXX) 매칭 → Key별 RN / 매출 집계
4) docs/data/campaign_performance.json 생성

중복방지:
- 동일 코드가 여러 Key에 등록되면 첫 Key에만 귀속 (campaign_data.json 입력 순서 기준).
  AP 대시보드 JS의 'seen' 로직과 의미적으로 동일.

기간 필터:
- 기본은 전체 기간 (코드는 캠페인 단위 고유라는 가정).
- 옵션으로 events[].판매기간 / 투숙기간으로 필터링 가능 (CLI 플래그).
"""
from __future__ import annotations
import os, json, sys, logging
from pathlib import Path
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DOCS_DATA_DIR = PROJECT_DIR / "docs" / "data"
CAMPAIGN_JSON = DOCS_DATA_DIR / "campaign_data.json"
OUTPUT_JSON = DOCS_DATA_DIR / "campaign_performance.json"


def find_raw_db() -> Path:
    """워크트리 / 메인 모두 지원. 연도 서브디렉토리(2026)가 있는 것을 우선.
    parse_package_trend.py 동일 패턴을 사용하되, 워크트리에서 빈 raw_db에 매칭되는
    문제를 피하기 위해 실제 데이터(2026/) 존재 여부로 우선순위 결정.
    """
    candidates = [
        PROJECT_DIR / "data" / "raw_db",
        PROJECT_DIR.parents[2] / "data" / "raw_db",  # worktree 3-level up
    ]
    # 1차: 2026/ 같은 연도 디렉토리가 실재하는 후보
    for c in candidates:
        if (c / "2026").exists() or (c / "2025").exists():
            return c
    # 2차: 그냥 존재하는 후보 (빈 디렉토리여도)
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError(f"raw_db 디렉토리를 찾을 수 없음: {candidates}")


def build_code_to_key(events: list[dict]) -> tuple[dict[str, str], dict[str, list[str]]]:
    """events[].key + package_codes → (code → first_key, key → ordered_codes).
    중복방지: 동일 코드가 여러 Key에 등록되어 있어도 첫 등장 Key에만 귀속.
    """
    code_to_key: dict[str, str] = {}
    key_to_codes: dict[str, list[str]] = defaultdict(list)
    for ev in events:
        key = ev.get("key", "").strip()
        if not key:
            continue
        for code in ev.get("package_codes", []) or []:
            c = str(code).strip()
            if not c:
                continue
            if c in code_to_key:
                # 이미 다른 Key에 매핑됨 — 스킵 (첫 등장 Key 우선)
                continue
            code_to_key[c] = key
            if c not in key_to_codes[key]:
                key_to_codes[key].append(c)
    return code_to_key, dict(key_to_codes)


def parse_db_file(fpath: Path, is_cancel: bool, code_to_key: dict[str, str], agg: dict):
    """27/28 파일 파싱 → agg[key]에 booking/cancel 누적.
    회원번호가 code_to_key에 매칭되는 행만 카운트.
    """
    try:
        with open(fpath, encoding="cp949", errors="replace") as f:
            lines = f.readlines()
    except Exception as e:
        logger.warning(f"읽기 실패: {fpath.name} — {e}")
        return 0
    if not lines:
        return 0

    headers = [h.strip() for h in lines[0].rstrip("\n").split(";")]
    col = {h: i for i, h in enumerate(headers)}
    idx_mem = col.get("회원번호", 5)
    idx_date = col.get("판매일자", 1)
    idx_stay_in = col.get("입실일자", 33)
    idx_rn = col.get("객실수", 28)
    idx_rate = col.get("1박객실료", 26)
    idx_prop = col.get("영업장명", 3)

    matched = 0
    for line in lines[1:]:
        parts = line.rstrip("\n").split(";")
        if len(parts) <= max(idx_mem, idx_rn, idx_rate, idx_date):
            continue
        mem = parts[idx_mem].strip()
        if not mem.startswith("86"):
            continue
        key = code_to_key.get(mem)
        if not key:
            continue
        try:
            rn = int(parts[idx_rn].strip() or 0)
            rate = int(parts[idx_rate].strip() or 0)
        except ValueError:
            continue
        if rn <= 0:
            rn = 1

        bucket = agg[key]
        if is_cancel:
            bucket["cancel_rn"] += rn
            bucket["cancel_rev"] += rate
        else:
            bucket["booking_rn"] += rn
            bucket["booking_rev"] += rate
            sale_date = parts[idx_date].strip() if idx_date < len(parts) else ""
            if sale_date and len(sale_date) >= 6:
                ym = sale_date[:6]
                bucket["by_sale_month"][ym] = bucket["by_sale_month"].get(ym, 0) + rn
            stay_in = parts[idx_stay_in].strip() if idx_stay_in < len(parts) else ""
            if stay_in and len(stay_in) >= 6:
                ym = stay_in[:6]
                bucket["by_stay_month"][ym] = bucket["by_stay_month"].get(ym, 0) + rn
        matched += 1

    return matched


def main():
    if not CAMPAIGN_JSON.exists():
        logger.error(f"입력 파일 없음: {CAMPAIGN_JSON}")
        logger.error("먼저 generate_campaign_data.py를 실행해 주세요.")
        sys.exit(1)

    data = json.loads(CAMPAIGN_JSON.read_text(encoding="utf-8"))
    events = data.get("events", [])
    if not events:
        logger.warning("events가 비어있음 — campaign_data.json 재생성 필요")

    code_to_key, key_to_codes = build_code_to_key(events)
    total_codes = len(code_to_key)
    keys_with_codes = len(key_to_codes)
    logger.info(f"패키지코드 매핑: {total_codes}개 코드 → {keys_with_codes}개 Key")

    if total_codes == 0:
        logger.warning("⚠ 매핑된 패키지코드가 없음 — Key 서브시트 발행 후 다시 실행해 주세요.")
        # 그래도 빈 결과를 출력해 다운스트림이 깨지지 않게.
        OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_JSON.write_text(json.dumps({
            "by_key": {},
            "meta": {
                "total_codes": 0,
                "keys_with_codes": 0,
                "duplicate_codes": data.get("duplicate_codes", {}),
                "note": "Key 서브시트가 publish-to-web 되어있지 않거나 패키지코드 미입력",
            },
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"✓ 저장 (빈 결과): {OUTPUT_JSON}")
        return

    raw_db = find_raw_db()
    logger.info(f"raw_db: {raw_db}")

    # 키별 누적 버킷
    def new_bucket():
        return {
            "booking_rn": 0,
            "booking_rev": 0,
            "cancel_rn": 0,
            "cancel_rev": 0,
            "by_sale_month": {},
            "by_stay_month": {},
        }
    agg: dict[str, dict] = defaultdict(new_bucket)

    # 27 (예약) + 28 (취소) — 2026 우선, 다른 연도도 같은 코드면 합산
    years = ["2024", "2025", "2026"]
    total_matched = 0
    for year in years:
        ydir = raw_db / year
        if not ydir.exists():
            continue
        for fname in sorted(os.listdir(ydir)):
            fpath = ydir / fname
            if fname.startswith("27."):
                n = parse_db_file(fpath, is_cancel=False, code_to_key=code_to_key, agg=agg)
                if n:
                    logger.info(f"  {year}/{fname[:50]}: 매칭 {n:,}행")
                    total_matched += n
            elif fname.startswith("28."):
                n = parse_db_file(fpath, is_cancel=True, code_to_key=code_to_key, agg=agg)
                if n:
                    logger.info(f"  {year}/{fname[:50]} (취소): 매칭 {n:,}행")
                    total_matched += n

    logger.info(f"총 매칭 행: {total_matched:,}")

    # Key별 net 집계 + 정리
    by_key: dict[str, dict] = {}
    for key, b in agg.items():
        net_rn = max(0, b["booking_rn"] - b["cancel_rn"])
        net_rev_won = max(0, b["booking_rev"] - b["cancel_rev"])
        adr = round(net_rev_won / net_rn) if net_rn > 0 else 0
        by_key[key] = {
            "rn": net_rn,
            "rev_won": net_rev_won,
            "rev_m": round(net_rev_won / 1_000_000, 2),
            "adr": adr,
            "booking_rn": b["booking_rn"],
            "cancel_rn": b["cancel_rn"],
            "cancel_rate": round(b["cancel_rn"] / b["booking_rn"] * 100, 1) if b["booking_rn"] else 0,
            "package_codes": key_to_codes.get(key, []),
            "by_sale_month": dict(sorted(b["by_sale_month"].items())),
            "by_stay_month": dict(sorted(b["by_stay_month"].items())),
        }

    output = {
        "by_key": by_key,
        "meta": {
            "total_codes": total_codes,
            "keys_with_codes": keys_with_codes,
            "keys_with_data": len(by_key),
            "total_matched_rows": total_matched,
            "raw_db_years": years,
            "duplicate_codes": data.get("duplicate_codes", {}),
        },
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"✓ 저장: {OUTPUT_JSON}")
    logger.info(f"  실적 적재 Key: {len(by_key)}건 / 누적 RN: "
                f"{sum(v['rn'] for v in by_key.values()):,} / 누적 매출: "
                f"{sum(v['rev_m'] for v in by_key.values()):,.1f}백만")


if __name__ == "__main__":
    main()
