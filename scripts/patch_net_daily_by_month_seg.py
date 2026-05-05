#!/usr/bin/env python3
"""기존 db_aggregated.json에 net_daily_by_month_seg 키를 패치.

근본 원인: net_daily_by_month는 ALL 세그먼트 합산이라 booking-trend.html의 페이스 차트가
Booking Status 표(OTA+G-OTA+Inbound)와 일치하지 않음. 세그먼트 차원을 보존한
net_daily_by_month_seg를 추가해 페이스 차트가 OTA+G-OTA+Inbound만 합산할 수 있도록 함.

데이터 소스: pickup_daily_by_segment_month, cancel_daily_by_segment_month (이미 존재).
출력: net_daily_by_month_seg[segment][stay_month][date] = {pickup_rn, cancel_rn, net_rn}
"""
import json
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_DIR / "data" / "db_aggregated.json"


def main():
    print(f"[patch_net_daily_by_month_seg] Loading {DB_PATH}")
    db = json.loads(DB_PATH.read_text(encoding="utf-8"))

    pdsm = db.get("pickup_daily_by_segment_month", {})
    cdsm = db.get("cancel_daily_by_segment_month", {})

    if not pdsm and not cdsm:
        print("  ⚠ pickup/cancel_daily_by_segment_month 부재 — 스킵")
        return

    all_segs = sorted(set(list(pdsm.keys()) + list(cdsm.keys())))
    out = {}
    for seg in all_segs:
        seg_pd_m = pdsm.get(seg, {})
        seg_cd_m = cdsm.get(seg, {})
        seg_months = sorted(set(seg_pd_m.keys()) | set(seg_cd_m.keys()))
        seg_block = {}
        for sm in seg_months:
            pd_block = seg_pd_m.get(sm, {})
            cd_block = seg_cd_m.get(sm, {})
            dates = sorted(set(pd_block.keys()) | set(cd_block.keys()))
            seg_block[sm] = {
                d: {
                    "pickup_rn": pd_block.get(d, {}).get("rn", 0) or 0,
                    "cancel_rn": cd_block.get(d, {}).get("rn", 0) or 0,
                    "net_rn":    (pd_block.get(d, {}).get("rn", 0) or 0)
                              -  (cd_block.get(d, {}).get("rn", 0) or 0),
                }
                for d in dates
            }
        out[seg] = seg_block

    db["net_daily_by_month_seg"] = out
    DB_PATH.write_text(json.dumps(db, ensure_ascii=False), encoding="utf-8")
    print(f"  ✓ net_daily_by_month_seg 추가: 세그={len(out)}, 첫 키={list(out.keys())[:3]}")


if __name__ == "__main__":
    main()
