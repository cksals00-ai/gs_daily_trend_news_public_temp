#!/usr/bin/env python3
"""
세그먼트별 FCST 추이 데이터 생성 스크립트
- db_aggregated.json에서 과거 실적 기반 사업장별 세그먼트 구성비 계산
- rm_fcst_trend.json의 총계 FCST를 세그먼트 비율로 분배
- 결과를 fcst_segment_trend.json으로 출력 + rm_fcst_trend.json에 segments 주입
"""
import json, os, sys
from collections import defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── 1. 사업장명 매핑 ──
# rm_fcst_trend 이름 → db_aggregated 이름
PROP_MAP = {
    '01.벨비발디':      '소노벨 비발디파크',
    '02.캄비발디':      '소노캄 비발디파크',
    '03.펫비발디':      '소노펫 비발디파크',
    '04.펠리체비발디':   '소노펠리체 비발디파크',
    '05.빌리지비발디':   '소노펠리체 빌리지 비발디파크',
    '06.양평':          '소노휴 양평',
    '07.델피노':        '델피노',
    '08.쏠비치양양':     '쏠비치 양양',
    '09.쏠비치삼척':     '쏠비치 삼척',
    '10.소노벨단양':     '소노문 단양',
    '11.소노캄경주':     '소노벨 경주',
    '12.소노벨청송':     '소노벨 청송',
    '13.소노벨천안':     '소노벨 천안',
    '14.소노벨변산':     '소노벨 변산',
    '15.소노캄여수':     '소노캄 여수',
    '16.소노캄거제':     '소노캄 거제',
    '17.쏠비치진도':     '쏠비치 진도',
    '18.소노벨제주':     '소노벨 제주',
    '19.소노캄제주':     '소노캄 제주',
    '20.소노캄고양':     '소노캄 고양',
    '21.소노문해운대':   '소노문 해운대',
    '22.쏠비치남해':     '쏠비치 남해',
    '23.르네블루':       '르네블루',
}

# ── 세그먼트 분류 ──
MAIN_SEGMENTS = ['OTA', 'G-OTA', 'Inbound']
# 나머지는 모두 '기타(Direct)'

# ── 연도별 가중치 (최근일수록 높음) ──
YEAR_WEIGHTS = {2022: 1.0, 2023: 2.0, 2024: 3.0, 2025: 4.0}

# ── 대상 월 (1~12월 전체 커버, 동일 월끼리 비교) ──
ALL_MONTHS = list(range(1, 13))


def load_json(name):
    path = os.path.join(BASE, 'data', name)
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json(name, data):
    path = os.path.join(BASE, 'data', name)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f'Saved: {path}')


def compute_segment_ratios(db_agg):
    """
    사업장별, 월별 세그먼트 구성비 계산.
    Returns: { db_prop_name: { month_num: { 'OTA': ratio, 'G-OTA': ratio, 'Inbound': ratio, '기타(Direct)': ratio } } }
    """
    bps = db_agg['by_property_segment']
    ratios = {}

    for db_prop in bps:
        ratios[db_prop] = {}
        
        for target_month in ALL_MONTHS:
            # 각 세그먼트별 가중 합산
            seg_weighted = defaultdict(float)
            total_weighted = 0.0

            for year in [2022, 2023, 2024, 2025]:
                month_key = f'{year}{target_month:02d}'
                weight = YEAR_WEIGHTS.get(year, 1.0)

                for seg_name in bps[db_prop]:
                    if month_key not in bps[db_prop][seg_name]:
                        continue
                    net_rn = bps[db_prop][seg_name][month_key].get('net_rn', 0)
                    if net_rn <= 0:
                        continue

                    # 세그먼트 분류
                    if seg_name in MAIN_SEGMENTS:
                        category = seg_name
                    else:
                        category = '기타(Direct)'

                    seg_weighted[category] += net_rn * weight
                    total_weighted += net_rn * weight

            # 비율 계산
            if total_weighted > 0:
                month_ratios = {}
                for cat in ['OTA', 'G-OTA', 'Inbound', '기타(Direct)']:
                    month_ratios[cat] = round(seg_weighted[cat] / total_weighted, 6) if seg_weighted[cat] > 0 else 0.0
                ratios[db_prop][target_month] = month_ratios
            else:
                # 데이터 없으면 균등 분배
                ratios[db_prop][target_month] = {'OTA': 0.4, 'G-OTA': 0.3, 'Inbound': 0.05, '기타(Direct)': 0.25}

    return ratios


def distribute_fcst(rm_data, ratios):
    """
    rm_fcst_trend의 각 스냅샷 총계 FCST를 세그먼트 비율로 분배.
    rm_data의 properties에 segments 키를 주입.
    또한 별도 fcst_segment_trend.json 구조도 생성.
    """
    segment_trend = {
        '_description': 'Segment-level FCST trend derived from RM total FCST × historical segment mix ratios',
        '_generated': rm_data.get('_generated', ''),
        '_segments': ['OTA', 'G-OTA', 'Inbound', '기타(Direct)'],
        '_method': 'Weighted historical segment mix (2022×1, 2023×2, 2024×3, 2025×4) from db_aggregated.json by_property_segment',
        'ratios': {},  # 사업장별 월별 비율
        'snapshots': []
    }

    unmapped_props = set()
    mapped_count = 0

    for snap in rm_data['snapshots']:
        snap_entry = {
            '_snapshot_date': snap['_snapshot_date'],
            '_year': snap['_year'],
            'properties': {}
        }

        # 전체 합산용
        snap_segments_total = defaultdict(lambda: defaultdict(lambda: {'rm_fcst_rn': 0, 'rm_budget_rn': 0}))

        for rm_prop, months_data in snap['properties'].items():
            db_prop = PROP_MAP.get(rm_prop)
            if not db_prop or db_prop not in ratios:
                unmapped_props.add(rm_prop)
                continue

            mapped_count += 1
            snap_entry['properties'][rm_prop] = {}

            for month_key, fcst_data in months_data.items():
                # month_key format: "2026-06" → month_num = 6
                try:
                    month_num = int(month_key.split('-')[1])
                except:
                    continue

                total_fcst = fcst_data.get('rm_fcst_rn', 0)
                total_budget = fcst_data.get('rm_budget_rn', 0)

                month_ratios = ratios[db_prop].get(month_num)
                if not month_ratios:
                    continue

                seg_data = {}
                for seg_name in ['OTA', 'G-OTA', 'Inbound', '기타(Direct)']:
                    r = month_ratios.get(seg_name, 0)
                    seg_fcst = round(total_fcst * r)
                    seg_budget = round(total_budget * r)
                    seg_data[seg_name] = {
                        'rm_fcst_rn': seg_fcst,
                        'rm_budget_rn': seg_budget,
                        'ratio': round(r * 100, 1)
                    }
                    # 합산
                    snap_segments_total[month_key][seg_name]['rm_fcst_rn'] += seg_fcst
                    snap_segments_total[month_key][seg_name]['rm_budget_rn'] += seg_budget

                snap_entry['properties'][rm_prop][month_key] = seg_data

                # rm_data에도 주입 (HTML이 읽는 형식)
                if 'segments' not in fcst_data:
                    fcst_data['segments'] = {}
                for seg_name, seg_vals in seg_data.items():
                    fcst_data['segments'][seg_name] = {
                        'rm_fcst_rn': seg_vals['rm_fcst_rn'],
                        'rm_budget_rn': seg_vals['rm_budget_rn']
                    }

        # 전체 합산 세그 데이터도 snap에 주입
        if not hasattr(snap, '__contains__') or 'segments' not in snap:
            snap['segments'] = {}
        for month_key, seg_totals in snap_segments_total.items():
            if month_key not in snap['segments']:
                snap['segments'][month_key] = {}
            for seg_name, vals in seg_totals.items():
                snap['segments'][month_key][seg_name] = dict(vals)

        segment_trend['snapshots'].append(snap_entry)

    # 비율 정보도 저장
    for rm_prop, db_prop_name in PROP_MAP.items():
        if db_prop_name in ratios:
            segment_trend['ratios'][rm_prop] = {}
            for month_num, month_ratios in ratios[db_prop_name].items():
                segment_trend['ratios'][rm_prop][str(month_num)] = {
                    k: round(v * 100, 1) for k, v in month_ratios.items()
                }

    if unmapped_props:
        print(f'WARNING: Unmapped properties: {unmapped_props}')
    print(f'Mapped {mapped_count} property-snapshot pairs')

    return segment_trend


def main():
    print('Loading data...')
    db_agg = load_json('db_aggregated.json')
    rm_data = load_json('rm_fcst_trend.json')

    print('Computing segment ratios...')
    ratios = compute_segment_ratios(db_agg)

    # 비율 확인 출력
    print('\n=== Sample ratios (델피노, month 6~8) ===')
    for m in [6, 7, 8]:
        r = ratios.get('델피노', {}).get(m, {})
        print(f'  Month {m}: ' + ', '.join(f'{k}={v*100:.1f}%' for k, v in r.items()))

    print('\nDistributing FCST by segments...')
    segment_trend = distribute_fcst(rm_data, ratios)

    # Save separate segment trend file
    save_json('fcst_segment_trend.json', segment_trend)

    # Save updated rm_fcst_trend with injected segments
    save_json('rm_fcst_trend.json', rm_data)

    # Verification
    print('\n=== Verification ===')
    snap = rm_data['snapshots'][-1]
    print(f'Latest snapshot: {snap["_snapshot_date"]}')
    for prop_name in list(snap['properties'].keys())[:3]:
        months = snap['properties'][prop_name]
        for mk, md in months.items():
            if 'segments' in md:
                total = md['rm_fcst_rn']
                seg_sum = sum(s['rm_fcst_rn'] for s in md['segments'].values())
                diff = abs(total - seg_sum)
                print(f'  {prop_name} {mk}: total={total}, seg_sum={seg_sum}, diff={diff}')
                for seg, sv in md['segments'].items():
                    print(f'    {seg}: {sv["rm_fcst_rn"]} ({sv["rm_fcst_rn"]/total*100:.1f}%)' if total > 0 else f'    {seg}: {sv["rm_fcst_rn"]}')

    print('\nDone!')


if __name__ == '__main__':
    main()
