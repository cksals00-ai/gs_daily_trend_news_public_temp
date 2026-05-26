#!/usr/bin/env python3
"""
build_same_month_ratio.py — 당월 예약 비중 집계 (OTA + G-OTA만)
투숙월별 '예약월==투숙월' RN 비중 계산 (27번 예약파일, 세그먼트 OTA/G-OTA만)
출력: data/same_month_booking.json, docs/data/same_month_booking.json
"""
import os, sys, json, re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw_db"
OUTPUT_PATH = PROJECT_DIR / "data" / "same_month_booking.json"
DOCS_PATH = PROJECT_DIR / "docs" / "data" / "same_month_booking.json"


def _parse_ym(s):
    s = s.strip().replace('-','').replace('/','').replace('.','')
    return s[:6] if len(s) >= 6 else None


def classify_segment(code_num):
    num = (code_num or "").strip()
    if num in ("A4", "A5"): return "G-OTA"
    if num in ("53", "72"): return "OTA"
    if num == "58": return "Inbound"
    return "기타"


def detect_month_filter(filename):
    basename = os.path.basename(filename)
    m = re.search(r'재전송\((\d{8})-(\d{8})\)', basename)
    if m:
        return ('retransmit', m.group(1)[:6], m.group(2)[:6])
    m2 = re.search(r'_(\d{8})_생성시간', basename)
    if m2:
        return ('snapshot', None, None)
    return ('unknown', None, None)


def find_types_with_retransmit(year_dir):
    result = set()
    for f in os.listdir(year_dir):
        if '재전송' in f and os.path.basename(f).startswith("27"):
            result.add("27")
    return result


def process_file(filepath, min_month=None, max_month=None):
    """27번 파일에서 OTA/G-OTA만 (stay_month, booking_month) → rn 집계"""
    result = defaultdict(int)
    encodings = ['cp949', 'euc-kr', 'utf-8']

    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                header_line = f.readline().strip()
                headers = header_line.split(';')
                col_map = {h.strip(): i for i, h in enumerate(headers)}

                idx_selldate = col_map.get('판매일자', -1)
                idx_checkin = col_map.get('입실일자', -1)
                idx_pickup_date = col_map.get('최초입력일자', -1)
                idx_rooms = col_map.get('객실수', -1)
                code_col = '변경예약집계코드' if '변경예약집계코드' in col_map else '예약집계코드'
                idx_code_num = col_map.get(code_col, -1)

                seen = set()
                count = 0

                for line in f:
                    h = hash(line.rstrip('\n\r'))
                    if h in seen: continue
                    seen.add(h)
                    parts = line.split(';')
                    plen = len(parts)

                    try:
                        # 세그먼트 필터: OTA/G-OTA만
                        code_num = parts[idx_code_num].strip() if idx_code_num >= 0 and idx_code_num < plen else ''
                        seg = classify_segment(code_num)
                        if seg not in ("OTA", "G-OTA"):
                            continue

                        sell_date = parts[idx_selldate].strip() if idx_selldate >= 0 and idx_selldate < plen else ''
                        if len(sell_date) < 6:
                            sell_date = parts[idx_checkin].strip() if idx_checkin >= 0 and idx_checkin < plen else ''
                            if len(sell_date) < 6: continue
                        stay_month = _parse_ym(sell_date)
                        if not stay_month: continue

                        if min_month and stay_month < min_month: continue
                        if max_month and stay_month > max_month: continue

                        pickup_str = parts[idx_pickup_date].strip() if idx_pickup_date >= 0 and idx_pickup_date < plen else ''
                        booking_month = _parse_ym(pickup_str)
                        if not booking_month: continue

                        rn = int(parts[idx_rooms].strip()) if idx_rooms >= 0 and idx_rooms < plen else 1
                        result[(stay_month, booking_month)] += rn
                        count += 1
                    except (IndexError, ValueError):
                        continue

                print(f"  {os.path.basename(filepath)}: {count:,}행 (OTA+G-OTA)", file=sys.stderr)
                return result
        except UnicodeDecodeError:
            continue
    return result


def main():
    agg = defaultdict(int)
    year_dirs = sorted([d for d in RAW_DIR.iterdir() if d.is_dir() and d.name.isdigit()])

    for year_dir in year_dirs:
        year = year_dir.name
        print(f"Processing {year}...", file=sys.stderr)

        has_retransmit = find_types_with_retransmit(year_dir)
        txt_files = sorted(year_dir.glob("27*.txt"))  # 27번만

        for fpath in txt_files:
            ftype, ret_start, ret_end = detect_month_filter(fpath.name)
            min_month = max_month = None

            if ftype == 'retransmit':
                max_month = min(ret_end, f"{year}03") if int(year) >= 2026 else ret_end
            elif ftype == 'snapshot' and has_retransmit:
                min_month = f"{year}04" if int(year) >= 2026 else None

            result = process_file(fpath, min_month, max_month)
            for k, v in result.items():
                agg[k] += v

    # Build output by year
    stay_month_total = defaultdict(int)
    stay_month_same = defaultdict(int)

    for (stay_m, booking_m), rn in agg.items():
        stay_month_total[stay_m] += rn
        if stay_m == booking_m:
            stay_month_same[stay_m] += rn

    output = {}
    for sm in sorted(stay_month_total.keys()):
        year, month = sm[:4], sm[4:6]
        if year not in output: output[year] = {}
        total = stay_month_total[sm]
        same = stay_month_same.get(sm, 0)
        output[year][month] = {
            "total_rn": total,
            "same_month_rn": same,
            "ratio": round(same / total * 100, 1) if total > 0 else 0
        }

    result_json = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "description": "당월 예약 비중 (OTA+G-OTA): 투숙월=예약월 RN 비중",
        "by_year": output
    }

    for p in (OUTPUT_PATH, DOCS_PATH):
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, 'w', encoding='utf-8') as f:
            json.dump(result_json, f, ensure_ascii=False, indent=2 if p == OUTPUT_PATH else None)

    print(f"\n✅ {OUTPUT_PATH}", file=sys.stderr)
    print(f"✅ {DOCS_PATH}", file=sys.stderr)

    for year in sorted(output.keys()):
        print(f"\n{year}년:", file=sys.stderr)
        for month in sorted(output[year].keys()):
            d = output[year][month]
            print(f"  {month}월: 당월 {d['same_month_rn']:,} / 전체 {d['total_rn']:,} = {d['ratio']}%", file=sys.stderr)


if __name__ == "__main__":
    main()
