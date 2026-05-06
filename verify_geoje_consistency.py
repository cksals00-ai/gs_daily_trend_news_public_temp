#!/usr/bin/env python3
"""
Verify data consistency for Geoje (거제) property across different aggregation views.
Tests that net_rn and net_rev totals match across:
1. by_property (direct)
2. by_property_channel (sum of all channels)
3. by_property_segment (sum of all segments)
"""
import json
import sys
from pathlib import Path

def load_json(filepath):
    """Load JSON from file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def extract_geoje_data(data):
    """Extract and verify Geoje data consistency"""

    # Find the exact property name containing 거제
    by_property = data.get('by_property', {})
    geoje_names = [key for key in by_property.keys() if '거제' in key]

    if not geoje_names:
        print("ERROR: No property containing '거제' found in by_property")
        return None

    geoje_name = geoje_names[0]
    print(f"Found property: {geoje_name}\n")

    # Extract 2025 months from by_property
    geoje_prop = by_property.get(geoje_name, {})
    months_2025 = {k: v for k, v in geoje_prop.items() if k.startswith('2025')}

    if not months_2025:
        print("ERROR: No 2025 data found for this property")
        return None

    print(f"Found {len(months_2025)} months in 2025: {sorted(months_2025.keys())}\n")

    # 1. by_property totals
    print("=" * 70)
    print("1. by_property totals for 거제 2025:")
    print("=" * 70)

    by_prop_net_rn = 0
    by_prop_net_rev = 0

    for month in sorted(months_2025.keys()):
        month_data = months_2025[month]
        net_rn = month_data.get('net_rn', 0)
        net_rev = month_data.get('net_rev', 0)  # in millions
        by_prop_net_rn += net_rn
        by_prop_net_rev += net_rev
        print(f"  {month}: net_rn={net_rn:6.0f}, net_rev={net_rev:10.2f}M")

    print(f"\n  TOTAL (by_property):")
    print(f"    net_rn:  {by_prop_net_rn:10.0f}")
    print(f"    net_rev: {by_prop_net_rev:10.2f}M\n")

    # 2. by_property_channel totals
    print("=" * 70)
    print("2. by_property_channel totals for 거제 2025:")
    print("=" * 70)

    by_prop_channel = data.get('by_property_channel', {}).get(geoje_name, {})

    if not by_prop_channel:
        print(f"WARNING: No by_property_channel data for {geoje_name}")
        channels = {}
    else:
        channels = by_prop_channel

    by_channel_net_rn = 0
    by_channel_net_rev = 0

    for channel in sorted(channels.keys()):
        channel_data = channels[channel]
        months_2025_ch = {k: v for k, v in channel_data.items() if k.startswith('2025')}

        ch_net_rn = 0
        ch_net_rev = 0
        for month in months_2025_ch:
            month_data = months_2025_ch[month]
            net_rn = month_data.get('net_rn', 0)
            net_rev = month_data.get('net_rev', 0)
            ch_net_rn += net_rn
            ch_net_rev += net_rev

        by_channel_net_rn += ch_net_rn
        by_channel_net_rev += ch_net_rev
        print(f"  {channel:15}: net_rn={ch_net_rn:6.0f}, net_rev={ch_net_rev:10.2f}M")

    print(f"\n  TOTAL (by_property_channel):")
    print(f"    net_rn:  {by_channel_net_rn:10.0f}")
    print(f"    net_rev: {by_channel_net_rev:10.2f}M\n")

    # 3. by_property_segment totals
    print("=" * 70)
    print("3. by_property_segment totals for 거제 2025:")
    print("=" * 70)

    by_prop_segment = data.get('by_property_segment', {}).get(geoje_name, {})

    if not by_prop_segment:
        print(f"WARNING: No by_property_segment data for {geoje_name}")
        segments = {}
    else:
        segments = by_prop_segment

    by_segment_net_rn = 0
    by_segment_net_rev = 0

    for segment in sorted(segments.keys()):
        segment_data = segments[segment]
        months_2025_seg = {k: v for k, v in segment_data.items() if k.startswith('2025')}

        seg_net_rn = 0
        seg_net_rev = 0
        for month in months_2025_seg:
            month_data = months_2025_seg[month]
            net_rn = month_data.get('net_rn', 0)
            net_rev = month_data.get('net_rev', 0)
            seg_net_rn += net_rn
            seg_net_rev += net_rev

        by_segment_net_rn += seg_net_rn
        by_segment_net_rev += seg_net_rev
        print(f"  {segment:15}: net_rn={seg_net_rn:6.0f}, net_rev={seg_net_rev:10.2f}M")

    print(f"\n  TOTAL (by_property_segment):")
    print(f"    net_rn:  {by_segment_net_rn:10.0f}")
    print(f"    net_rev: {by_segment_net_rev:10.2f}M\n")

    # Consistency verification
    print("=" * 70)
    print("CONSISTENCY VERIFICATION")
    print("=" * 70)

    rn_match = (by_prop_net_rn == by_channel_net_rn) and (by_prop_net_rn == by_segment_net_rn)
    rev_match = (by_prop_net_rev == by_channel_net_rev) and (by_prop_net_rev == by_segment_net_rev)

    print(f"\nnet_rn consistency:")
    print(f"  by_property:        {by_prop_net_rn:10.0f}")
    print(f"  by_property_channel:{by_channel_net_rn:10.0f}")
    print(f"  by_property_segment:{by_segment_net_rn:10.0f}")
    print(f"  MATCH: {rn_match} {'✓' if rn_match else '✗'}\n")

    print(f"net_rev consistency (Millions):")
    print(f"  by_property:        {by_prop_net_rev:10.2f}M")
    print(f"  by_property_channel:{by_channel_net_rev:10.2f}M")
    print(f"  by_property_segment:{by_segment_net_rev:10.2f}M")
    print(f"  MATCH: {rev_match} {'✓' if rev_match else '✗'}\n")

    # Check for stay_date_by_property or similar
    print("=" * 70)
    print("CHECKING FOR stay_date RELATED KEYS")
    print("=" * 70)

    stay_date_keys = [k for k in data.keys() if 'stay_date' in k.lower()]
    print(f"\nFound {len(stay_date_keys)} stay_date related keys:")
    for key in sorted(stay_date_keys):
        print(f"  - {key}")

    return {
        'geoje_name': geoje_name,
        'by_property': {
            'net_rn': by_prop_net_rn,
            'net_rev': by_prop_net_rev,
        },
        'by_property_channel': {
            'net_rn': by_channel_net_rn,
            'net_rev': by_channel_net_rev,
        },
        'by_property_segment': {
            'net_rn': by_segment_net_rn,
            'net_rev': by_segment_net_rev,
        },
        'rn_match': rn_match,
        'rev_match': rev_match,
    }

if __name__ == '__main__':
    # Try different file paths
    possible_paths = [
        Path('/Users/chanminpark/Desktop/gs_daily_trend_news_public_temp/docs/data/db_aggregated.json'),
        Path('/Users/chanminpark/Desktop/gs_daily_trend_news_public_temp/data/db_aggregated.json'),
    ]

    filepath = None
    for path in possible_paths:
        if path.exists():
            filepath = path
            print(f"Using: {filepath}\n")
            break

    if not filepath:
        print("ERROR: db_aggregated.json not found")
        sys.exit(1)

    print("Loading JSON (this may take a moment)...\n")
    data = load_json(filepath)

    print(f"JSON loaded successfully. Meta info:")
    meta = data.get('meta', {})
    print(f"  Years: {meta.get('years', [])}")
    print(f"  Properties: {len(meta.get('properties', []))} total")
    print(f"  Months: {meta.get('months', [])[:3]}...{meta.get('months', [])[-3:]}\n")

    result = extract_geoje_data(data)

    if result:
        print("=" * 70)
        print("FINAL RESULT")
        print("=" * 70)
        print(f"\nAll views CONSISTENT: {result['rn_match'] and result['rev_match']}\n")
        if not (result['rn_match'] and result['rev_match']):
            print("INCONSISTENCIES DETECTED - Check details above\n")
