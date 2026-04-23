#!/usr/bin/env python3
"""
db_to_notes.py — 온북 DB 파싱 결과 → daily_notes.json 반영
=============================================================
파이프라인: parse_raw_db.py → db_aggregated.json → [이 스크립트] → daily_notes.json → build.py

수집 데이터:
  - property_performance: 권역별 사업장 실적(RNS·ADR·REV·YoY)
  - major_ota_performance: OTA 채널별 실적 TOP 10 (RNS·YoY)

데이터 한계:
  - RNS = OTA/Inbound 채널 예약 객실수(booking_rn) — 전채널 합산 아님
  - REV/ADR = OTA 채널 정산 기준 (rack rate 아님)
  - 목표(budget) 없음 → target_rns=0, achievement=0 (신호등 황색)
  - YoY = 전년동월 booking_rn 대비

© 2026 GS팀 · Haein Kim Manager
"""
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
DOCS_DATA_DIR = ROOT / "docs" / "data"

# 조회 3개월
STAY_MONTHS = ["2026-04", "2026-05", "2026-06"]
# DB 월 키: "2026-04" → "202604"
DB_MONTHS = {m: m.replace("-", "") for m in STAY_MONTHS}
# 전년 동월 (YoY 기준)
DB_LAST_MONTHS = {
    "2026-04": "202504",
    "2026-05": "202505",
    "2026-06": "202506",
}

# ─── 사업장 → 권역 매핑 (properties.json 기준 최종 확정) ───
# parse_raw_db.py의 PROPERTY_REGION과 달리, 사업계획 기준 권역 적용:
#   - 소노벨 경주 → south (parse_raw_db.py는 오류로 central에 배치)
#   - 소노문 해운대, 소노벨 변산, 소노벨 청송 → 각 해당 권역
PROPERTY_REGION: dict[str, str] = {
    # ── Vivaldi ──
    "소노캄 비발디파크":          "vivaldi",
    "소노벨 비발디파크":          "vivaldi",
    "소노펠리체 비발디파크":       "vivaldi",
    "소노펠리체 빌리지 비발디파크": "vivaldi",
    "소노펫 비발디파크":          "vivaldi",
    "소노문 비발디파크":          "vivaldi",
    # ── Central ──
    "델피노":                   "central",
    "소노휴 양평":               "central",
    "쏠비치 양양":               "central",
    "쏠비치 삼척":               "central",
    "소노문 단양":               "central",
    "소노벨 청송":               "central",
    "소노벨 천안":               "central",
    "소노벨 변산":               "central",
    # ── South ──
    "소노벨 경주":               "south",   # parse_raw_db.py 오류 수정 (central→south)
    "소노캄 여수":               "south",
    "소노캄 거제":               "south",
    "쏠비치 남해":               "south",
    "쏠비치 진도":               "south",
    "소노문 해운대":              "south",
    # ── APAC ──
    "소노벨 제주":               "apac",
    "소노캄 제주":               "apac",
    "소노캄 고양":               "apac",
    # 제외: 르네블루, 오션월드빌리지 (properties.json 공식 목록 외)
}

# ─── 채널 티어 분류 ───
CHANNEL_TIER: dict[str, str] = {
    "야놀자":   "국내",
    "여기어때": "국내",
    "네이버":   "국내",
    "카카오":   "국내",
    "쿠팡":     "국내",
    "트립비토즈": "국내",
    "하나투어": "국내",
    "모두투어": "국내",
    "인터파크": "국내",
    "티몬":     "국내",
    "위메프":   "국내",
    "아고다":   "글로벌",
    "익스피디아": "글로벌",
    "부킹닷컴": "글로벌",
    "트립닷컴": "글로벌",
    "호텔스닷컴": "글로벌",
}


def get_tier(ch_name: str) -> str:
    for keyword, tier in CHANNEL_TIER.items():
        if keyword in ch_name:
            return tier
    return "신규"


def load_json(path: Path, default=None):
    if not path.exists():
        logger.error(f"파일 없음: {path}")
        return default if default is not None else {}
    return json.loads(path.read_text(encoding="utf-8"))


def calc_property_month(curr: dict, last: dict) -> dict:
    """단일 사업장 × 단일 월 지표 계산"""
    booking_rn  = curr.get("booking_rn", 0)
    booking_rev = curr.get("booking_rev", 0)   # 백만원 단위 (parse_raw_db.py가 ÷1,000,000 후 저장)
    last_rn     = last.get("booking_rn", 0) if last else 0

    # ADR: 천원 단위 — booking_rev(백만원) × 1000 ÷ RN
    adr = round(booking_rev * 1_000 / booking_rn) if booking_rn > 0 else 0
    # REV: 백만원 단위 (이미 백만원)
    rev = round(booking_rev)
    # YoY
    yoy = round(((booking_rn - last_rn) / last_rn) * 100, 1) if last_rn > 0 else 0

    return {
        "rns":        booking_rn,
        "adr":        adr,
        "rev":        rev,
        "target_rns": 0,
        "target_rev": 0,
        "achievement": 0,
        "yoy_pct":    yoy,
        "last_rns":   last_rn,
    }


def build_property_performance(by_property: dict) -> dict:
    """사업장 → 권역별 분류 + 월별 지표"""
    regions: dict[str, list] = {"vivaldi": [], "central": [], "south": [], "apac": []}

    for prop_name, months_data in by_property.items():
        region = PROPERTY_REGION.get(prop_name)
        if not region:
            logger.debug(f"  권역 미매핑 제외: {prop_name}")
            continue

        entry: dict = {"name": prop_name}
        for month_key in STAY_MONTHS:
            curr = months_data.get(DB_MONTHS[month_key], {})
            last = months_data.get(DB_LAST_MONTHS[month_key], {})
            entry[month_key] = calc_property_month(curr, last)

        regions[region].append(entry)

    # 4월 RNS 오름차순 정렬 (부진 사업장이 위에 표시)
    for region in regions:
        regions[region].sort(key=lambda p: p.get("2026-04", {}).get("rns", 0))

    total = sum(len(v) for v in regions.values())
    for region, props in regions.items():
        logger.info(f"  {region}: {len(props)}개 사업장")
    logger.info(f"  총 {total}개 사업장 반영")

    return {
        "_description": f"온북 DB 자동 파싱 ({datetime.now().strftime('%Y-%m-%d %H:%M')})",
        "_status":      "auto_synced",
        "_source":      "parse_raw_db.py → db_aggregated.json → db_to_notes.py",
        "_last_sync":   datetime.now().isoformat(),
        **regions,
    }


def build_ota_performance(by_channel: dict) -> dict:
    """채널별 OTA 실적 TOP 10"""
    channel_monthly: dict[str, dict] = {}

    for ch_name, months_data in by_channel.items():
        if ch_name == "기타":
            continue  # 미분류 채널 제외

        entry: dict = {}
        for month_key in STAY_MONTHS:
            curr = months_data.get(DB_MONTHS[month_key], {})
            last = months_data.get(DB_LAST_MONTHS[month_key], {})
            rns  = curr.get("booking_rn", 0)
            last_rns = last.get("booking_rn", 0) if last else 0
            yoy  = round(((rns - last_rns) / last_rns) * 100, 1) if last_rns > 0 else 0
            entry[month_key] = {"rns": rns, "yoy_pct": yoy}

        channel_monthly[ch_name] = entry

    # 4월 RNS 내림차순 TOP 10
    ranked = sorted(
        channel_monthly.items(),
        key=lambda x: x[1].get("2026-04", {}).get("rns", 0),
        reverse=True,
    )[:10]

    channels_list = []
    for rank, (name, monthly) in enumerate(ranked, 1):
        ch_entry: dict = {"rank": rank, "name": name, "tier": get_tier(name)}
        ch_entry.update(monthly)
        channels_list.append(ch_entry)

    logger.info(f"  채널 TOP {len(channels_list)}개 반영")

    return {
        "_description": f"온북 DB 채널별 자동 파싱 ({datetime.now().strftime('%Y-%m-%d %H:%M')})",
        "_status":      "auto_synced",
        "_source":      "parse_raw_db.py → db_aggregated.json → db_to_notes.py",
        "_last_sync":   datetime.now().isoformat(),
        "channels":     channels_list,
    }


REGION_LABEL: dict[str, str] = {
    "vivaldi": "비발디파크",
    "central": "한국중부",
    "south":   "한국남부",
    "apac":    "아시아퍼시픽",
}


def build_otb_data(by_property: dict) -> dict:
    """otb.html이 JS fetch로 사용하는 otb_data.json 생성

    otb.html이 기대하는 구조:
      meta, filters, monthly[], byProperty[], summary
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M KST")
    today_str = datetime.now().strftime("%Y-%m-%d")

    # ── 월별 집계 (4/5/6월) ──
    monthly_rows = []
    for month_key in STAY_MONTHS:
        db_m  = DB_MONTHS[month_key]
        db_ly = DB_LAST_MONTHS[month_key]
        label = {"2026-04": "4월", "2026-05": "5월", "2026-06": "6월"}[month_key]

        rns_actual = sum(
            months.get(db_m, {}).get("booking_rn", 0)
            for p, months in by_property.items()
            if PROPERTY_REGION.get(p)
        )
        rns_last = sum(
            months.get(db_ly, {}).get("booking_rn", 0)
            for p, months in by_property.items()
            if PROPERTY_REGION.get(p)
        )
        monthly_rows.append({
            "label":      label,
            "rns_budget": 0,
            "rns_actual": rns_actual,
            "rns_last":   rns_last,
        })

    # ── 사업장별 (4월 기준) ──
    by_prop_rows = []
    db_m  = DB_MONTHS["2026-04"]
    db_ly = DB_LAST_MONTHS["2026-04"]

    tot_rns_actual = tot_rns_last = 0
    tot_booking = tot_cancel = tot_net = 0
    tot_rev_actual = 0

    for prop_name, months_data in sorted(by_property.items()):
        region = PROPERTY_REGION.get(prop_name)
        if not region:
            continue

        curr = months_data.get(db_m, {})
        last = months_data.get(db_ly, {})

        rns_actual  = curr.get("booking_rn", 0)
        rns_last    = last.get("booking_rn", 0)
        cancel_rn   = curr.get("cancel_rn", 0)
        net_rn      = curr.get("net_rn", 0)
        booking_rev_m = curr.get("booking_rev", 0)  # 백만원 단위
        booking_rev_w = round(booking_rev_m * 1_000_000)  # 원 단위로 변환

        adr_actual = round(booking_rev_w / rns_actual) if rns_actual > 0 else 0

        tot_rns_actual += rns_actual
        tot_rns_last   += rns_last
        tot_booking    += rns_actual
        tot_cancel     += cancel_rn
        tot_net        += net_rn
        tot_rev_actual += booking_rev_w  # 원 단위 누적

        by_prop_rows.append({
            "name":            prop_name,
            "region":          REGION_LABEL.get(region, region),
            "rns_budget":      0,
            "rns_actual":      rns_actual,
            "rns_achievement": 0.0,
            "rns_last":        rns_last,
            "today_booking":   rns_actual,
            "today_cancel":    cancel_rn,
            "today_net":       net_rn,
            "adr_budget":      0,
            "adr_actual":      adr_actual,   # 원 단위
            "rev_budget":      0,
            "rev_actual":      booking_rev_w,  # 원 단위
            "rev_achievement": 0.0,
        })

    # 정렬: 권역 → 사업장명
    by_prop_rows.sort(key=lambda r: (r["region"], r["name"]))

    tot_adr = round(tot_rev_actual / tot_rns_actual) if tot_rns_actual > 0 else 0  # 원/RN
    tot_yoy = round(
        ((tot_rns_actual - tot_rns_last) / tot_rns_last) * 100, 1
    ) if tot_rns_last > 0 else 0.0

    summary = {
        "rns_budget":      0,
        "rns_actual":      tot_rns_actual,
        "rns_achievement": 0.0,
        "rns_last":        tot_rns_last,
        "rns_yoy":         tot_yoy,
        "today_booking":   tot_booking,
        "today_cancel":    tot_cancel,
        "today_net":       tot_net,
        "adr_budget":      0,
        "adr_actual":      tot_adr,
        "adr_vs_budget":   0.0,
        "rev_budget":      0,
        "rev_actual":      tot_rev_actual,
        "rev_achievement": 0.0,
    }

    return {
        "meta": {
            "refreshTime": now_str,
            "baseDate":    today_str,
            "dataSource":  "온북 DB 파싱 (parse_raw_db.py → db_aggregated.json)",
        },
        "filters": {
            "months": [
                {"value": 0, "label": "전체"},
                {"value": 4, "label": "4월"},
                {"value": 5, "label": "5월"},
                {"value": 6, "label": "6월"},
            ],
            "segments": ["전체", "OTA", "G-OTA", "Inbound"],
        },
        "monthly":    monthly_rows,
        "byProperty": by_prop_rows,
        "summary":    summary,
    }


def main():
    logger.info("=" * 60)
    logger.info("온북 DB → daily_notes.json + otb_data.json 변환")
    logger.info("=" * 60)

    agg_path   = DATA_DIR / "db_aggregated.json"
    notes_path = DATA_DIR / "daily_notes.json"

    agg   = load_json(agg_path)
    notes = load_json(notes_path)

    if not agg:
        logger.error("db_aggregated.json 없음 — parse_raw_db.py를 먼저 실행하세요.")
        sys.exit(1)

    by_property = agg.get("by_property", {})
    by_channel  = agg.get("by_channel", {})

    logger.info(f"db_aggregated.json 로드: {len(by_property)}개 사업장, {len(by_channel)}개 채널")
    logger.info(f"  생성 시각: {agg.get('generated_at', '알 수 없음')}")

    # ── 사업장 실적 ──
    logger.info("\n[1] 사업장별 실적 집계")
    notes["property_performance"] = build_property_performance(by_property)

    # ── OTA 채널 실적 ──
    logger.info("\n[2] OTA 채널별 실적 집계")
    notes["major_ota_performance"] = build_ota_performance(by_channel)

    # ── OTB 데이터 생성 (otb.html이 fetch하는 docs/data/otb_data.json) ──
    logger.info("\n[3] otb_data.json 생성")
    otb_data = build_otb_data(by_property)
    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    otb_path = DOCS_DATA_DIR / "otb_data.json"
    otb_path.write_text(json.dumps(otb_data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"  ✓ otb_data.json: {len(otb_data['byProperty'])}개 사업장")

    # ── 저장 ──
    notes_path.write_text(json.dumps(notes, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"\n✓ daily_notes.json 저장 완료  → {notes_path}")
    logger.info(f"✓ otb_data.json 저장 완료     → {otb_path}")

    # ── 결과 요약 ──
    logger.info("=" * 60)
    logger.info("변환 완료")
    prop_perf = notes["property_performance"]
    for region in ("vivaldi", "central", "south", "apac"):
        props = prop_perf.get(region, [])
        if props:
            top = props[0]
            m4  = top.get("2026-04", {})
            logger.info(
                f"  [{region}] {len(props)}개 | "
                f"최하위: {top['name']} {m4.get('rns', 0):,}실"
            )

    ota = notes["major_ota_performance"]["channels"]
    if ota:
        top1 = ota[0]
        logger.info(
            f"  [채널 1위] {top1['name']} "
            f"{top1.get('2026-04', {}).get('rns', 0):,}실 (4월)"
        )
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
