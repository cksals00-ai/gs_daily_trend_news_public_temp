#!/usr/bin/env python3
"""
DEPRECATED — parse_raw_db.py now produces pickup/cancel_daily_by_channel(_month)
directly from raw rows (channel-key 원본 합산). 비율 배분 금지 정책에 따라 이 스크립트는
폴백 안전망으로만 동작하며, 키가 이미 존재하면 즉시 종료한다.
"""
import json, sys
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
AGG_FILE = ROOT / "data" / "db_aggregated.json"


def _to_m(raw_won):
    return round(raw_won / 1_000_000, 2)


print("Loading db_aggregated.json ...")
with open(AGG_FILE) as f:
    db = json.load(f)

# Skip if already present (parse_raw_db.py가 정상 작동했다면 항상 존재)
if all(k in db and db[k] for k in [
    "pickup_daily_by_channel", "cancel_daily_by_channel",
    "pickup_daily_by_channel_month", "cancel_daily_by_channel_month",
]):
    print("Channel daily data already exists (from parse_raw_db). Skipping.")
    sys.exit(0)

# 비례 배분은 비활성. 키 누락 시 즉시 실패해 사용자가 parse_raw_db.py 재실행하도록 유도.
print("ERROR: pickup/cancel_daily_by_channel keys missing — re-run parse_raw_db.py to regenerate.")
sys.exit(1)

# Source data
bpc = db.get("by_property_channel", {})
pdbp = db.get("pickup_daily_by_property", {})      # prop -> date -> {rn, rev}
cdbp = db.get("cancel_daily_by_property", {})
pdbpm = db.get("pickup_daily_by_property_month", {})  # prop -> month -> date -> {rn, rev}
cdbpm = db.get("cancel_daily_by_property_month", {})

if not bpc:
    print("ERROR: by_property_channel not found!")
    sys.exit(1)

# ── Build channel ratios per property per month ──
# bpc structure: property -> channel -> month_key -> {booking_rn, cancel_rn, ...}
# We need: prop -> month -> { channel: {booking_ratio, cancel_ratio} }

prop_month_channel_booking = defaultdict(lambda: defaultdict(dict))  # prop -> month -> {ch: rn}
prop_month_channel_cancel = defaultdict(lambda: defaultdict(dict))

for prop, channels in bpc.items():
    for ch, month_data in channels.items():
        if not isinstance(month_data, dict):
            continue
        for mkey, vals in month_data.items():
            if not isinstance(vals, dict):
                continue
            b_rn = vals.get("booking_rn", 0) or 0
            c_rn = vals.get("cancel_rn", 0) or 0
            if b_rn > 0:
                prop_month_channel_booking[prop][mkey][ch] = \
                    prop_month_channel_booking[prop][mkey].get(ch, 0) + b_rn
            if c_rn > 0:
                prop_month_channel_cancel[prop][mkey][ch] = \
                    prop_month_channel_cancel[prop][mkey].get(ch, 0) + c_rn


def _get_ratios(dist):
    """dict {ch: count} -> {ch: ratio}"""
    total = sum(dist.values())
    if total <= 0:
        return {}
    return {ch: cnt / total for ch, cnt in dist.items()}


def _infer_month(date_str):
    """'20260427' -> '202604'"""
    return date_str[:6] if len(date_str) >= 6 else None


# ── 1) pickup/cancel_daily_by_channel (channel -> date -> {rn, rev}) ──
print("Building daily_by_channel ...")
pd_ch = defaultdict(lambda: defaultdict(lambda: {"rn": 0, "rev": 0.0}))
cd_ch = defaultdict(lambda: defaultdict(lambda: {"rn": 0, "rev": 0.0}))

for prop, dates in pdbp.items():
    for date_key, vals in dates.items():
        mkey = _infer_month(date_key)
        if not mkey:
            continue
        rn = vals.get("rn", 0) or 0
        rev = vals.get("rev", 0) or 0
        dist = prop_month_channel_booking.get(prop, {}).get(mkey, {})
        ratios = _get_ratios(dist)
        if not ratios:
            pd_ch["기타"][date_key]["rn"] += rn
            pd_ch["기타"][date_key]["rev"] += rev
            continue
        for ch, ratio in ratios.items():
            pd_ch[ch][date_key]["rn"] += round(rn * ratio)
            pd_ch[ch][date_key]["rev"] += rev * ratio

for prop, dates in cdbp.items():
    for date_key, vals in dates.items():
        mkey = _infer_month(date_key)
        if not mkey:
            continue
        rn = vals.get("rn", 0) or 0
        rev = vals.get("rev", 0) or 0
        dist = prop_month_channel_cancel.get(prop, {}).get(mkey, {})
        ratios = _get_ratios(dist)
        if not ratios:
            cd_ch["기타"][date_key]["rn"] += rn
            cd_ch["기타"][date_key]["rev"] += rev
            continue
        for ch, ratio in ratios.items():
            cd_ch[ch][date_key]["rn"] += round(rn * ratio)
            cd_ch[ch][date_key]["rev"] += rev * ratio

# ── 2) pickup/cancel_daily_by_channel_month (channel -> month -> date -> {rn, rev}) ──
print("Building daily_by_channel_month ...")
pd_chm = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"rn": 0, "rev": 0.0})))
cd_chm = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"rn": 0, "rev": 0.0})))

for prop, months in pdbpm.items():
    for mkey, dates in months.items():
        dist = prop_month_channel_booking.get(prop, {}).get(mkey, {})
        ratios = _get_ratios(dist)
        for date_key, vals in dates.items():
            rn = vals.get("rn", 0) or 0
            rev = vals.get("rev", 0) or 0
            if not ratios:
                pd_chm["기타"][mkey][date_key]["rn"] += rn
                pd_chm["기타"][mkey][date_key]["rev"] += rev
                continue
            for ch, ratio in ratios.items():
                pd_chm[ch][mkey][date_key]["rn"] += round(rn * ratio)
                pd_chm[ch][mkey][date_key]["rev"] += rev * ratio

for prop, months in cdbpm.items():
    for mkey, dates in months.items():
        dist = prop_month_channel_cancel.get(prop, {}).get(mkey, {})
        ratios = _get_ratios(dist)
        for date_key, vals in dates.items():
            rn = vals.get("rn", 0) or 0
            rev = vals.get("rev", 0) or 0
            if not ratios:
                cd_chm["기타"][mkey][date_key]["rn"] += rn
                cd_chm["기타"][mkey][date_key]["rev"] += rev
                continue
            for ch, ratio in ratios.items():
                cd_chm[ch][mkey][date_key]["rn"] += round(rn * ratio)
                cd_chm[ch][mkey][date_key]["rev"] += rev * ratio

# ── Serialize ──
def _serialize_daily(dd):
    return {
        ch: {d: {"rn": v["rn"], "rev": round(v["rev"], 2)} for d, v in sorted(dates.items())}
        for ch, dates in sorted(dd.items())
    }

def _serialize_daily_month(dd):
    return {
        ch: {
            m: {d: {"rn": v["rn"], "rev": round(v["rev"], 2)} for d, v in sorted(dates.items())}
            for m, dates in sorted(months.items())
        }
        for ch, months in sorted(dd.items())
    }

db["pickup_daily_by_channel"] = _serialize_daily(pd_ch)
db["cancel_daily_by_channel"] = _serialize_daily(cd_ch)
db["pickup_daily_by_channel_month"] = _serialize_daily_month(pd_chm)
db["cancel_daily_by_channel_month"] = _serialize_daily_month(cd_chm)

print(f"pickup channels: {len(db['pickup_daily_by_channel'])}")
print(f"cancel channels: {len(db['cancel_daily_by_channel'])}")
print(f"pickup channel-months: {len(db['pickup_daily_by_channel_month'])}")
print(f"cancel channel-months: {len(db['cancel_daily_by_channel_month'])}")

print("Saving db_aggregated.json ...")
with open(AGG_FILE, "w") as f:
    json.dump(db, f, ensure_ascii=False)
print("Done!")
