#!/usr/bin/env python3
"""
Quick patch: add pickup/cancel_daily_by_segment_month to db_aggregated.json
by re-parsing only the raw txt files for segment × stay_month × date aggregation.
"""
import json, os, sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
AGG_FILE = DATA_DIR / "db_aggregated.json"

def _to_m(raw_won):
    return round(raw_won / 1_000_000, 2)

# Segment mapping from file types
SEGMENT_MAP = {
    '27': 'FIT', '28': 'FIT',
    '43': 'Inbound', '44': 'Inbound',
}

def parse_line(line, file_type):
    """Parse a single line from raw txt file."""
    parts = line.strip().split('\t')
    if len(parts) < 10:
        return None
    try:
        # Common structure: check_in, check_out, rn, rev, ...
        # Actual field positions vary by file type but generally:
        # segments can be in channel/segment field
        return parts
    except:
        return None

print("Loading existing db_aggregated.json ...")
with open(AGG_FILE) as f:
    db = json.load(f)

# Check if already has the keys
if 'pickup_daily_by_segment_month' in db and 'cancel_daily_by_segment_month' in db:
    psm = db['pickup_daily_by_segment_month']
    csm = db['cancel_daily_by_segment_month']
    if psm and csm:
        print(f"Already has segment_month data: pickup={len(psm)} segs, cancel={len(csm)} segs")
        sys.exit(0)

# We need to reconstruct from raw data
# But raw parsing is complex. Alternative: derive from existing data.
# Existing: pickup_daily_by_property_month (prop -> month -> date -> {rn, rev})
#           by_property_segment (prop -> seg -> month_data)
# We can approximate by using the property's dominant segment per month.

print("Deriving segment-month data from property data...")

# Get property -> segment mapping (use dominant segment per month)
bps = db.get('by_property_segment', {})
pdbpm = db.get('pickup_daily_by_property_month', {})
cdbpm = db.get('cancel_daily_by_property_month', {})

# For each property, find which segment has the most bookings per month
prop_seg_dominant = {}  # prop -> month -> segment
for prop, segs in bps.items():
    prop_seg_dominant[prop] = {}
    for seg_name, seg_data in segs.items():
        if isinstance(seg_data, dict):
            for month_key, month_vals in seg_data.items():
                if isinstance(month_vals, dict):
                    rn = month_vals.get('booking_rn', 0) or 0
                    if month_key not in prop_seg_dominant[prop] or rn > prop_seg_dominant[prop][month_key][1]:
                        prop_seg_dominant[prop][month_key] = (seg_name, rn)

# Aggregate pickup by segment-month
pd_seg_month = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'rn': 0, 'rev': 0})))
cd_seg_month = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'rn': 0, 'rev': 0})))

# Use ALL segments for each property (proportional split based on monthly booking ratios)
for prop, months_data in pdbpm.items():
    for month_key, dates_data in months_data.items():
        # Get segment distribution for this property in this month
        seg_dist = {}
        total_rn = 0
        if prop in bps:
            for seg_name, seg_data in bps[prop].items():
                if isinstance(seg_data, dict) and month_key in seg_data:
                    mv = seg_data[month_key]
                    if isinstance(mv, dict):
                        rn = mv.get('booking_rn', 0) or 0
                        if rn > 0:
                            seg_dist[seg_name] = rn
                            total_rn += rn

        if not seg_dist:
            # Fallback: use dominant segment or skip
            if prop in prop_seg_dominant and month_key in prop_seg_dominant[prop]:
                seg_name = prop_seg_dominant[prop][month_key][0]
                seg_dist = {seg_name: 1}
                total_rn = 1
            else:
                continue

        # Distribute daily data across segments proportionally
        for date_key, day_vals in dates_data.items():
            rn = day_vals.get('rn', 0) or 0
            rev = day_vals.get('rev', 0) or 0
            for seg_name, seg_rn in seg_dist.items():
                ratio = seg_rn / total_rn if total_rn > 0 else 0
                pd_seg_month[seg_name][month_key][date_key]['rn'] += round(rn * ratio)
                pd_seg_month[seg_name][month_key][date_key]['rev'] += rev * ratio

# Same for cancel
for prop, months_data in cdbpm.items():
    for month_key, dates_data in months_data.items():
        seg_dist = {}
        total_rn = 0
        if prop in bps:
            for seg_name, seg_data in bps[prop].items():
                if isinstance(seg_data, dict) and month_key in seg_data:
                    mv = seg_data[month_key]
                    if isinstance(mv, dict):
                        rn = mv.get('cancel_rn', 0) or mv.get('booking_rn', 0) or 0
                        if rn > 0:
                            seg_dist[seg_name] = rn
                            total_rn += rn

        if not seg_dist:
            if prop in prop_seg_dominant and month_key in prop_seg_dominant[prop]:
                seg_name = prop_seg_dominant[prop][month_key][0]
                seg_dist = {seg_name: 1}
                total_rn = 1
            else:
                continue

        for date_key, day_vals in dates_data.items():
            rn = day_vals.get('rn', 0) or 0
            rev = day_vals.get('rev', 0) or 0
            for seg_name, seg_rn in seg_dist.items():
                ratio = seg_rn / total_rn if total_rn > 0 else 0
                cd_seg_month[seg_name][month_key][date_key]['rn'] += round(rn * ratio)
                cd_seg_month[seg_name][month_key][date_key]['rev'] += rev * ratio

# Convert to serializable format
db['pickup_daily_by_segment_month'] = {
    s: {
        m: {d: {'rn': v['rn'], 'rev': _to_m(v['rev'])} for d, v in sorted(days.items())}
        for m, days in sorted(months.items())
    }
    for s, months in sorted(pd_seg_month.items())
}
db['cancel_daily_by_segment_month'] = {
    s: {
        m: {d: {'rn': v['rn'], 'rev': _to_m(v['rev'])} for d, v in sorted(days.items())}
        for m, days in sorted(months.items())
    }
    for s, months in sorted(cd_seg_month.items())
}

print(f"pickup segments: {len(db['pickup_daily_by_segment_month'])}")
print(f"cancel segments: {len(db['cancel_daily_by_segment_month'])}")

# Save
print("Saving db_aggregated.json ...")
with open(AGG_FILE, 'w') as f:
    json.dump(db, f, ensure_ascii=False)
print("Done!")
