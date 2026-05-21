#!/usr/bin/env python3
"""
generate_chat_index.py
=====================
db_aggregated.json → docs/data/chat_index.json

사업장 × 세그먼트(OTA / G-OTA / Inbound) × 월 단위로
RN·ADR·Revenue 경량 색인을 생성한다.

출력 구조:
{
  "generated_at": "...",
  "properties": ["델피노", ...],
  "segments": ["OTA","G-OTA","Inbound"],
  "months": ["202201", ...],
  "data": {
    "델피노": {
      "OTA":     { "202201": {"rn":..,"adr":..,"rev":..}, ... },
      "G-OTA":   { ... },
      "Inbound": { ... },
      "전체":    { ... }
    },
    ...
  },
  "totals": {            # 전사 합계
    "OTA":     { "202201": {...}, ... },
    "G-OTA":   { ... },
    "Inbound": { ... },
    "전체":    { ... }
  },
  "channels": {          # 세그먼트별 주요 채널(거래처) 목록
    "OTA": ["여기어때","야놀자","네이버",...],
    "G-OTA": ["아고다","트립닷컴","익스피디아",...],
    "Inbound": ["Inbound"]
  }
}
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime
from pathlib import Path
from collections import defaultdict

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
SRC = ROOT / "data" / "db_aggregated.json"
DST = ROOT / "docs" / "data" / "chat_index.json"

TARGET_SEGMENTS = ["OTA", "G-OTA", "Inbound"]


def _round_metric(v, decimals=1):
    """숫자 반올림 (None → 0)"""
    if v is None:
        return 0
    return round(v, decimals)


def _compact_month(raw: dict) -> dict:
    """원본 월별 메트릭에서 필요한 3개만 추출"""
    return {
        "rn":  raw.get("net_rn") or raw.get("booking_rn") or 0,
        "adr": _round_metric(raw.get("adr", 0), 0),
        "rev": _round_metric(raw.get("net_rev") or raw.get("booking_rev") or 0, 2),
    }


def _sum_months(*dicts) -> dict:
    """여러 월별 메트릭 합산"""
    total_rn = sum(d.get("rn", 0) for d in dicts)
    total_rev = sum(d.get("rev", 0) for d in dicts)
    return {
        "rn": total_rn,
        "adr": round(total_rev * 1_000_000 / total_rn) if total_rn else 0,
        "rev": round(total_rev, 2),
    }


def _collect_channels(bpcs: dict) -> dict[str, list[str]]:
    """by_property_channel_segment 에서 세그먼트별 채널(거래처) 목록 수집"""
    seg_channels: dict[str, set[str]] = defaultdict(set)
    for _prop, channels in bpcs.items():
        for ch_name, seg_map in channels.items():
            if not isinstance(seg_map, dict):
                continue
            for seg_name in seg_map:
                if seg_name in TARGET_SEGMENTS:
                    seg_channels[seg_name].add(ch_name)
    # 정렬 후 반환
    return {seg: sorted(chs) for seg, chs in seg_channels.items()}


def build(src_path: Path = SRC, dst_path: Path = DST) -> Path:
    print(f"  ▸ 소스: {src_path}")
    with open(src_path, "r", encoding="utf-8") as f:
        db = json.load(f)

    by_prop_seg = db.get("by_property_segment", {})
    by_segment = db.get("by_segment", {})
    by_pcs = db.get("by_property_channel_segment", {})

    properties = sorted(by_prop_seg.keys())
    all_months: set[str] = set()

    # ── 사업장별 데이터 ──────────────────────────────────
    data: dict = {}
    for prop in properties:
        seg_map = by_prop_seg[prop]
        prop_entry: dict = {}
        prop_all_months: dict[str, dict] = {}  # month -> aggregated

        for seg in TARGET_SEGMENTS:
            if seg not in seg_map:
                prop_entry[seg] = {}
                continue
            month_data = seg_map[seg]
            compact: dict = {}
            for m, raw in month_data.items():
                c = _compact_month(raw)
                compact[m] = c
                all_months.add(m)
                # 전체 합산용
                if m not in prop_all_months:
                    prop_all_months[m] = {"rn": 0, "rev": 0.0}
                prop_all_months[m]["rn"] += c["rn"]
                prop_all_months[m]["rev"] += c["rev"]
            prop_entry[seg] = compact

        # 사업장 전체 (OTA+G-OTA+Inbound 합산)
        total_months: dict = {}
        for m, agg in prop_all_months.items():
            rn = agg["rn"]
            rev = agg["rev"]
            total_months[m] = {
                "rn": rn,
                "adr": round(rev * 1_000_000 / rn) if rn else 0,
                "rev": round(rev, 2),
            }
        prop_entry["전체"] = total_months
        data[prop] = prop_entry

    # ── 전사 합계 ────────────────────────────────────────
    totals: dict = {}
    totals_all: dict[str, dict] = {}
    for seg in TARGET_SEGMENTS:
        seg_data = by_segment.get(seg, {})
        compact: dict = {}
        for m, raw in seg_data.items():
            c = _compact_month(raw)
            compact[m] = c
            all_months.add(m)
            if m not in totals_all:
                totals_all[m] = {"rn": 0, "rev": 0.0}
            totals_all[m]["rn"] += c["rn"]
            totals_all[m]["rev"] += c["rev"]
        totals[seg] = compact
    # 전체
    total_all: dict = {}
    for m, agg in totals_all.items():
        rn = agg["rn"]
        rev = agg["rev"]
        total_all[m] = {
            "rn": rn,
            "adr": round(rev * 1_000_000 / rn) if rn else 0,
            "rev": round(rev, 2),
        }
    totals["전체"] = total_all

    # ── 채널 목록 ────────────────────────────────────────
    channels = _collect_channels(by_pcs)

    months_sorted = sorted(all_months)

    result = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "properties": properties,
        "segments": TARGET_SEGMENTS + ["전체"],
        "months": months_sorted,
        "data": data,
        "totals": totals,
        "channels": channels,
    }

    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dst_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = dst_path.stat().st_size / 1024
    print(f"  ▸ 출력: {dst_path}  ({size_kb:.0f} KB)")
    print(f"  ▸ 사업장 {len(properties)}개 × 세그먼트 {len(TARGET_SEGMENTS)+1}개 × 월 {len(months_sorted)}개")
    return dst_path


if __name__ == "__main__":
    build()
