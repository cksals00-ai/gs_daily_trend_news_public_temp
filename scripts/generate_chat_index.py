#!/usr/bin/env python3
"""
generate_chat_index.py
=====================
db_aggregated.json + otb_data.json + inbound_enriched.json
→ docs/data/chat_index.json

사업장 × 세그먼트 × 월 + 채널별 + 국적별 + 목표/FCST/LY/YoY
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime
from pathlib import Path
from collections import defaultdict

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
SRC_DB = ROOT / "data" / "db_aggregated.json"
SRC_OTB = ROOT / "docs" / "data" / "otb_data.json"
SRC_INBOUND = ROOT / "docs" / "data" / "inbound_enriched.json"
DST = ROOT / "docs" / "data" / "chat_index.json"

TARGET_SEGMENTS = ["OTA", "G-OTA", "Inbound"]

OTB_NAME_MAP = {
    "01.벨비발디": "소노벨 비발디파크",
    "02.캄비발디": "소노캄 비발디파크",
    "03.펫비발디": "소노펫 비발디파크",
    "04.펠리체비발디": "소노펠리체 비발디파크",
    "05.빌리지비발디": "소노펠리체 빌리지 비발디파크",
    "06.문비발디": "소노문 비발디파크",
    "07.오션월드빌리지": "오션월드빌리지",
    "08.캄고양": "소노캄 고양",
    "09.델피노": "델피노",
    "10.벨변산": "소노벨 변산",
    "11.쏠비치진도": "쏠비치 진도",
    "12.벨천안": "소노벨 천안",
    "13.벨청송": "소노벨 청송",
    "14.휴양평": "소노휴 양평",
    "15.쏠비치삼척": "쏠비치 삼척",
    "16.문단양": "소노문 단양",
    "17.벨경주": "소노벨 경주",
    "18.르네블루": "르네블루",
    "19.벨제주": "소노벨 제주",
    "20.캄제주": "소노캄 제주",
    "21.캄여수": "소노캄 여수",
    "22.캄거제": "소노캄 거제",
    "23.쏠비치양양": "쏠비치 양양",
    "24.문해운대": "소노문 해운대",
    "25.쏠비치남해": "쏠비치 남해",
    "팔라티움": "팔라티움",
    "전사합계": "_total_",
}


def _round(v, d=1):
    if v is None: return 0
    return round(v, d)


def _compact(raw: dict) -> dict:
    return {
        "rn": raw.get("net_rn") or raw.get("booking_rn") or 0,
        "adr": _round(raw.get("adr", 0), 0),
        "rev": _round(raw.get("net_rev") or raw.get("booking_rev") or 0, 2),
    }


def _load_otb_budget_fcst(otb_path: Path) -> dict:
    if not otb_path.exists():
        print("  ⚠ otb_data.json 없음 — 목표/FCST 생략")
        return {}
    with open(otb_path, "r", encoding="utf-8") as f:
        otb = json.load(f)
    result = {}
    for entry in otb.get("yoyTable", []):
        otb_name = entry.get("name", "")
        db_name = OTB_NAME_MAP.get(otb_name, otb_name)
        if db_name == "_total_":
            continue
        months_data = entry.get("months", {})
        prop_budget = {}
        for m_str, md in months_data.items():
            month_key = f"2026{int(m_str):02d}"
            prop_budget[month_key] = {
                "bud": md.get("bud_rn") or 0,
                "fcst": md.get("rns_fcst") or 0,
                "last": md.get("last_rn") or 0,
                "yoy": md.get("yoy"),
                "fcst_src": md.get("fcst_source", ""),
            }
        result[db_name] = prop_budget
    return result


def _load_nationality_monthly(inbound_path: Path) -> tuple[dict, list]:
    if not inbound_path.exists():
        print("  ⚠ inbound_enriched.json 없음 — 국적 생략")
        return {}, []
    with open(inbound_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    mbn = data.get("monthly_by_nationality", {})
    # Restructure: {nationality: {month: {rn, adr, rev}}}
    nat_data: dict[str, dict] = defaultdict(dict)
    all_nats = set()
    skip = {"매핑불가", "미확인"}
    for month_key, nat_map in mbn.items():
        for nat, metrics in nat_map.items():
            if nat in skip:
                continue
            rn = metrics.get("rn_net") or metrics.get("rn_booking") or 0
            rev_raw = metrics.get("rev_net") or metrics.get("rev_booking") or 0
            rev = round(rev_raw / 1_000_000, 2)  # 원 → 백만원
            if rn > 0:
                all_nats.add(nat)
                nat_data[nat][month_key] = {
                    "rn": rn,
                    "adr": round(rev_raw / rn / 1000) if rn else 0,  # 원→천원
                    "rev": rev,
                }
    return dict(nat_data), sorted(all_nats)


def build(src_db: Path = SRC_DB, dst_path: Path = DST) -> Path:
    print(f"  ▸ 소스: {src_db}")
    with open(src_db, "r", encoding="utf-8") as f:
        db = json.load(f)

    by_prop_seg = db.get("by_property_segment", {})
    by_segment = db.get("by_segment", {})
    by_pcs = db.get("by_property_channel_segment", {})

    properties = sorted(by_prop_seg.keys())
    all_months: set[str] = set()

    budget_fcst = _load_otb_budget_fcst(SRC_OTB)
    nat_data, nationalities = _load_nationality_monthly(SRC_INBOUND)

    # ══ 사업장별 데이터 ══
    data: dict = {}
    for prop in properties:
        seg_map = by_prop_seg[prop]
        prop_entry: dict = {}
        prop_all: dict[str, dict] = {}

        for seg in TARGET_SEGMENTS:
            if seg not in seg_map:
                prop_entry[seg] = {}
                continue
            month_data = seg_map[seg]
            compact: dict = {}
            for m, raw in month_data.items():
                c = _compact(raw)
                compact[m] = c
                all_months.add(m)
                if m not in prop_all:
                    prop_all[m] = {"rn": 0, "rev": 0.0}
                prop_all[m]["rn"] += c["rn"]
                prop_all[m]["rev"] += c["rev"]
            prop_entry[seg] = compact

        total_months = {}
        for m, agg in prop_all.items():
            rn, rev = agg["rn"], agg["rev"]
            total_months[m] = {
                "rn": rn,
                "adr": round(rev * 1_000_000 / rn) if rn else 0,
                "rev": round(rev, 2),
            }
        prop_entry["전체"] = total_months
        data[prop] = prop_entry

    # ══ 전사 합계 ══
    totals: dict = {}
    totals_all: dict[str, dict] = {}
    for seg in TARGET_SEGMENTS:
        seg_data = by_segment.get(seg, {})
        compact: dict = {}
        for m, raw in seg_data.items():
            c = _compact(raw)
            compact[m] = c
            all_months.add(m)
            if m not in totals_all:
                totals_all[m] = {"rn": 0, "rev": 0.0}
            totals_all[m]["rn"] += c["rn"]
            totals_all[m]["rev"] += c["rev"]
        totals[seg] = compact
    total_all = {}
    for m, agg in totals_all.items():
        rn, rev = agg["rn"], agg["rev"]
        total_all[m] = {"rn": rn, "adr": round(rev * 1_000_000 / rn) if rn else 0, "rev": round(rev, 2)}
    totals["전체"] = total_all

    # ══ 채널별 데이터 ══
    seg_channels: dict[str, set[str]] = defaultdict(set)
    channel_monthly: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(lambda: {"rn": 0, "rev": 0.0}))

    for _prop, channels in by_pcs.items():
        for ch_name, seg_map in channels.items():
            if not isinstance(seg_map, dict):
                continue
            for seg_name, month_data in seg_map.items():
                if seg_name not in TARGET_SEGMENTS:
                    continue
                seg_channels[seg_name].add(ch_name)
                if not isinstance(month_data, dict):
                    continue
                for m, raw in month_data.items():
                    rn = raw.get("net_rn") or raw.get("booking_rn") or 0
                    rev = raw.get("net_rev") or raw.get("booking_rev") or 0.0
                    agg = channel_monthly[ch_name][m]
                    agg["rn"] += rn
                    agg["rev"] += rev

    channels_list = {seg: sorted(chs) for seg, chs in seg_channels.items()}

    by_channel: dict = {}
    for ch, months_map in channel_monthly.items():
        ch_data = {}
        for m, agg in months_map.items():
            rn, rev = agg["rn"], agg["rev"]
            if rn > 0:
                ch_data[m] = {"rn": rn, "adr": round(rev * 1_000_000 / rn) if rn else 0, "rev": round(rev, 2)}
        if ch_data:
            by_channel[ch] = ch_data

    months_sorted = sorted(all_months)

    result = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "properties": properties,
        "segments": TARGET_SEGMENTS + ["전체"],
        "months": months_sorted,
        "data": data,
        "totals": totals,
        "channels": channels_list,
        "budget_fcst": budget_fcst,
        "by_channel": by_channel,
        "by_nationality": nat_data,
        "nationalities": nationalities,
    }

    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dst_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = dst_path.stat().st_size / 1024
    print(f"  ▸ 출력: {dst_path}  ({size_kb:.0f} KB)")
    print(f"  ▸ 사업장 {len(properties)}개 × 세그먼트 {len(TARGET_SEGMENTS)+1}개 × 월 {len(months_sorted)}개")
    print(f"  ▸ 채널 {len(by_channel)}개 · 국적 {len(nat_data)}개")
    print(f"  ▸ 목표/FCST 사업장: {len(budget_fcst)}개")
    return dst_path


if __name__ == "__main__":
    build()
