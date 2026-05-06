"""사업장별 stayDateDaily 생성 — 27+43번 파일만 빠르게 파싱 (GS 세그먼트만)"""
import os, re, json, sys
from collections import defaultdict
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw_db')
GS_CODES = {'A4': 'G-OTA', 'A5': 'G-OTA', '53': 'OTA', '72': 'OTA', '58': 'Inbound'}

def norm_prop(name):
    return re.sub(r'^\d+\.\s*', '', (name or '').strip()) or '미분류'

def parse_file(filepath, result):
    encs = ['cp949', 'euc-kr', 'utf-8']
    for enc in encs:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                hdr = f.readline().strip().split(';')
                cm = {h.strip(): i for i, h in enumerate(hdr)}
                i_prop = cm.get('변경사업장명', cm.get('영업장명', -1))
                i_prop2 = cm.get('영업장명', -1)
                i_sell = cm.get('판매일자', -1)
                i_code = cm.get('변경예약집계코드', cm.get('예약집계코드', -1))
                i_rooms = cm.get('객실수', -1)
                i_rate = cm.get('1박객실료', -1)
                i_member = cm.get('회원명', -1)
                i_user = cm.get('이용자명', -1)
                seen = set()
                for line in f:
                    h = hash(line.rstrip())
                    if h in seen: continue
                    seen.add(h)
                    p = line.split(';')
                    if len(p) < max(i_prop, i_sell, i_code, i_rooms, i_rate, i_member, i_user) + 1:
                        continue
                    prop_raw = p[i_prop].strip() if i_prop >= 0 else ''
                    if not prop_raw and i_prop2 >= 0:
                        prop_raw = p[i_prop2].strip()
                    prop = norm_prop(prop_raw)
                    code = p[i_code].strip() if i_code >= 0 else ''
                    seg = GS_CODES.get(code)
                    if not seg: continue
                    member = p[i_member].strip() if i_member >= 0 else ''
                    user = p[i_user].strip() if i_user >= 0 else ''
                    if member and user and member == user and code != '58': continue
                    if '매출조정' in member or '매출조정' in user: continue
                    sell = p[i_sell].strip() if i_sell >= 0 else ''
                    if len(sell) < 8: continue
                    rooms = int(p[i_rooms].strip() or '0') if i_rooms >= 0 else 0
                    rate = int(p[i_rate].strip() or '0') if i_rate >= 0 else 0
                    rn = rooms if rooms > 0 else 1
                    rev = int(rate * rn / 1.1)
                    month = sell[:6]
                    day = int(sell[6:8])
                    result[prop][month][day][seg]['rn'] += rn
                    result[prop][month][day][seg]['rev'] += rev
            return True
        except (UnicodeDecodeError, UnicodeError):
            continue
    return False

def main():
    result = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'rn':0,'rev':0}))))
    
    # Process 27+43 files from all years
    for year_dir in sorted(os.listdir(DATA_DIR)):
        ypath = os.path.join(DATA_DIR, year_dir)
        if not os.path.isdir(ypath): continue
        for fname in sorted(os.listdir(ypath)):
            if not fname.endswith('.txt'): continue
            if not (fname.startswith('27') or fname.startswith('43')): continue
            fpath = os.path.join(ypath, fname)
            print(f'  파싱: {year_dir}/{fname}', file=sys.stderr)
            parse_file(fpath, result)
    
    # Build output structure
    output = {}
    for prop_name in sorted(result.keys()):
        prop_months = {}
        for month in sorted(result[prop_name].keys()):
            all_days = sorted(result[prop_name][month].keys())
            all_segs = set()
            for d in all_days:
                all_segs.update(result[prop_name][month][d].keys())
            seg_list = sorted(all_segs)
            segments = {}
            for seg in seg_list:
                rn_list = [result[prop_name][month][d].get(seg, {}).get('rn', 0) for d in all_days]
                rev_list = [round(result[prop_name][month][d].get(seg, {}).get('rev', 0) / 1_000_000, 2) for d in all_days]
                segments[seg] = {'net_rn': rn_list, 'net_rev': rev_list}
            prop_months[month] = {'days': all_days, 'segments': segments}
        output[prop_name] = prop_months
    
    out_path = os.path.join(os.path.dirname(__file__), '..', 'docs', 'data', 'sdd_by_prop.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False)
    
    total_props = len(output)
    total_months = sum(len(v) for v in output.values())
    print(f'✓ sdd_by_prop.json 생성: {total_props}개 사업장, {total_months}개 월', file=sys.stderr)
    fsize = os.path.getsize(out_path)
    print(f'  파일 크기: {fsize/1024/1024:.1f} MB', file=sys.stderr)

if __name__ == '__main__':
    main()
