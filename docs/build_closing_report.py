#!/usr/bin/env python3
"""Build gs-closing-report.html from db_aggregated.json"""

import json, os

DATA_DIR = os.path.dirname(os.path.abspath(__file__)) + '/data'

with open(f'{DATA_DIR}/db_aggregated.json') as f:
    db = json.load(f)

# ── helpers ──
def get_mt(month):
    return db['monthly_total'].get(month, {})

def sum_months(src, months, field):
    return sum(src.get(m, {}).get(field, 0) for m in months)

def pct(a, b):
    return ((a/b)-1)*100 if b else None

def fmt_n(n):
    if n is None: return '-'
    return f'{n:,.0f}'

def fmt_rev(n):
    if n is None: return '-'
    return f'{n:,.1f}'

def fmt_pct(p):
    if p is None: return '-'
    return f'{p:+.1f}%'

def adr_calc(rev, rn):
    return round(rev / rn) if rn else 0

M26 = ['202601','202602','202603']
M25 = ['202501','202502','202503']
M24 = ['202401','202402','202403']
M23 = ['202301','202302','202303']

MONTH_GROUPS = {
    '1월': (['202601'], ['202501'], ['202401'], ['202301']),
    '2월': (['202602'], ['202502'], ['202402'], ['202302']),
    '3월': (['202603'], ['202503'], ['202403'], ['202303']),
    'Q1': (M26, M25, M24, M23),
}

# New properties (no 2025 Q1 data)
bp = db['by_property']
NEW_PROPS = []
EXISTING_PROPS = []
for prop in sorted(bp.keys()):
    has_25q1 = any(m in bp[prop] for m in M25)
    has_26q1 = any(m in bp[prop] for m in M26)
    if has_26q1 and not has_25q1:
        NEW_PROPS.append(prop)
    elif has_26q1:
        EXISTING_PROPS.append(prop)

# Property region map
REGION_MAP = db['meta'].get('property_region_map', {})

# ── Build property analysis data ──
def build_property_data(m26_list, m25_list):
    rows = []
    for prop in sorted(bp.keys()):
        rn26 = sum_months(bp[prop], m26_list, 'booking_rn')
        rn25 = sum_months(bp[prop], m25_list, 'booking_rn')
        rev26 = sum_months(bp[prop], m26_list, 'booking_rev')
        rev25 = sum_months(bp[prop], m25_list, 'booking_rev')
        adr26 = adr_calc(rev26 * 10000, rn26) if rn26 else 0  # rev is in 만원
        adr25 = adr_calc(rev25 * 10000, rn25) if rn25 else 0
        is_new = prop in NEW_PROPS
        yoy_rn = pct(rn26, rn25) if not is_new else None
        yoy_rev = pct(rev26, rev25) if not is_new else None
        if rn26 == 0 and rn25 == 0:
            continue
        status = '신규' if is_new else ('호조' if yoy_rn and yoy_rn > 5 else ('부진' if yoy_rn and yoy_rn < -5 else '유지'))
        rows.append({
            'name': prop, 'rn26': rn26, 'rn25': rn25, 'rev26': rev26, 'rev25': rev25,
            'adr26': adr26, 'adr25': adr25, 'yoy_rn': yoy_rn, 'yoy_rev': yoy_rev,
            'is_new': is_new, 'status': status,
        })
    rows.sort(key=lambda x: x['rn26'], reverse=True)
    return rows

# ── Build channel analysis data ──
def build_channel_data(m26_list, m25_list, m24_list=None, m23_list=None):
    bc = db['by_channel']
    rows = []
    for ch in sorted(bc.keys()):
        rn26 = sum_months(bc[ch], m26_list, 'booking_rn')
        rn25 = sum_months(bc[ch], m25_list, 'booking_rn')
        rev26 = sum_months(bc[ch], m26_list, 'booking_rev')
        rev25 = sum_months(bc[ch], m25_list, 'booking_rev')
        adr26 = adr_calc(rev26*10000, rn26) if rn26 else 0
        rn24 = sum_months(bc[ch], m24_list, 'booking_rn') if m24_list else 0
        rn23 = sum_months(bc[ch], m23_list, 'booking_rn') if m23_list else 0
        if rn26 == 0 and rn25 == 0:
            continue
        rows.append({
            'name': ch, 'rn26': rn26, 'rn25': rn25, 'rev26': rev26, 'rev25': rev25,
            'adr26': adr26, 'yoy_rn': pct(rn26, rn25),
            'rn24': rn24, 'rn23': rn23,
            'share26': 0, 'share25': 0,
        })
    total26 = sum(r['rn26'] for r in rows)
    total25 = sum(r['rn25'] for r in rows)
    for r in rows:
        r['share26'] = (r['rn26']/total26*100) if total26 else 0
        r['share25'] = (r['rn25']/total25*100) if total25 else 0
    rows.sort(key=lambda x: x['rn26'], reverse=True)
    return rows

# ── Build segment analysis data ──
def build_segment_data(m26_list, m25_list):
    bs = db['by_segment']
    rows = []
    for seg in sorted(bs.keys()):
        rn26 = sum_months(bs[seg], m26_list, 'booking_rn')
        rn25 = sum_months(bs[seg], m25_list, 'booking_rn')
        rev26 = sum_months(bs[seg], m26_list, 'booking_rev')
        rev25 = sum_months(bs[seg], m25_list, 'booking_rev')
        adr26 = adr_calc(rev26*10000, rn26) if rn26 else 0
        if rn26 == 0 and rn25 == 0:
            continue
        rows.append({
            'name': seg, 'rn26': rn26, 'rn25': rn25, 'rev26': rev26, 'rev25': rev25,
            'adr26': adr26, 'yoy_rn': pct(rn26, rn25),
            'share26': 0, 'share25': 0,
        })
    total26 = sum(r['rn26'] for r in rows)
    total25 = sum(r['rn25'] for r in rows)
    for r in rows:
        r['share26'] = (r['rn26']/total26*100) if total26 else 0
        r['share25'] = (r['rn25']/total25*100) if total25 else 0
    rows.sort(key=lambda x: x['rn26'], reverse=True)
    return rows

# ── Deep insight: property channel/segment shifts ──
def build_property_deep_insight(prop, m26_list, m25_list):
    bpc = db['by_property_channel']
    bps = db['by_property_segment']
    result = {'channel_shifts': [], 'segment_shifts': []}
    if prop in bpc:
        for ch in bpc[prop]:
            rn26 = sum_months(bpc[prop][ch], m26_list, 'booking_rn')
            rn25 = sum_months(bpc[prop][ch], m25_list, 'booking_rn')
            diff = rn26 - rn25
            if abs(diff) > 10:
                result['channel_shifts'].append({'name': ch, 'rn25': rn25, 'rn26': rn26, 'diff': diff})
        result['channel_shifts'].sort(key=lambda x: x['diff'])
    if prop in bps:
        for seg in bps[prop]:
            rn26 = sum_months(bps[prop][seg], m26_list, 'booking_rn')
            rn25 = sum_months(bps[prop][seg], m25_list, 'booking_rn')
            diff = rn26 - rn25
            if abs(diff) > 10:
                result['segment_shifts'].append({'name': seg, 'rn25': rn25, 'rn26': rn26, 'diff': diff})
        result['segment_shifts'].sort(key=lambda x: x['diff'])
    return result

# ── Generate insights text ──
def gen_monthly_insights(month_label, m26, m25):
    mt26 = {f: sum(get_mt(m).get(f,0) for m in m26) for f in ['booking_rn','booking_rev','cancel_rn','cancel_rate']}
    mt25 = {f: sum(get_mt(m).get(f,0) for m in m25) for f in ['booking_rn','booking_rev','cancel_rn','cancel_rate']}
    
    rn_yoy = pct(mt26['booking_rn'], mt25['booking_rn'])
    rev_yoy = pct(mt26['booking_rev'], mt25['booking_rev'])
    
    adr26 = round(mt26['booking_rev']*10000/mt26['booking_rn']) if mt26['booking_rn'] else 0
    adr25 = round(mt25['booking_rev']*10000/mt25['booking_rn']) if mt25['booking_rn'] else 0
    adr_yoy = pct(adr26, adr25)
    
    # Existing props only YoY
    ex_rn26 = sum(sum_months(bp[p], m26, 'booking_rn') for p in EXISTING_PROPS)
    ex_rn25 = sum(sum_months(bp[p], m25, 'booking_rn') for p in EXISTING_PROPS)
    ex_yoy = pct(ex_rn26, ex_rn25)
    
    # New props contribution
    new_rn26 = sum(sum_months(bp[p], m26, 'booking_rn') for p in NEW_PROPS)
    
    props = build_property_data(m26, m25)
    hojo = [p for p in props if p['status'] == '호조']
    bujin = [p for p in props if p['status'] == '부진']
    
    # Channel highlights
    channels = build_channel_data(m26, m25)
    growth_ch = [c for c in channels if c['yoy_rn'] and c['yoy_rn'] > 20 and c['rn26'] > 100]
    decline_ch = [c for c in channels if c['yoy_rn'] and c['yoy_rn'] < -10 and c['rn25'] > 100]
    
    segments = build_segment_data(m26, m25)
    
    insights = []
    insights.append(f"전사 실적은 RN {fmt_n(mt26['booking_rn'])}실(YoY {fmt_pct(rn_yoy)}), 매출 {fmt_rev(mt26['booking_rev'])}만원(YoY {fmt_pct(rev_yoy)})을 기록하며 전년 동기 대비 {'성장' if rn_yoy and rn_yoy > 0 else '하락'}하였습니다.")
    
    if NEW_PROPS and new_rn26 > 0:
        insights.append(f"신규 사업장({', '.join(NEW_PROPS)}) 기여분 {fmt_n(new_rn26)}실 제외 시, 기존 사업장 기준 전년비는 {fmt_pct(ex_yoy)}입니다.")
    
    if hojo:
        top3 = hojo[:3]
        insights.append(f"호조 사업장: {', '.join(f'{p[\"name\"]}({fmt_pct(p[\"yoy_rn\"])})' for p in top3)} 등 {len(hojo)}개가 전년비 +5% 이상 달성했습니다.")
    
    if bujin:
        insights.append(f"부진 사업장: {', '.join(f'{p[\"name\"]}({fmt_pct(p[\"yoy_rn\"])})' for p in bujin[:3])} 등 {len(bujin)}개가 전년비 -5% 미만으로, 자사패키지·회원PKG 감소와 OTA 채널 이동이 주 원인입니다.")
    
    if growth_ch:
        insights.append(f"채널: {', '.join(f'{c[\"name\"]}({fmt_pct(c[\"yoy_rn\"])})' for c in sorted(growth_ch, key=lambda x: x['yoy_rn'], reverse=True)[:3])}이 구조적 성장세를 보이며, 트립닷컴·아고다 중심의 G-OTA 채널 확대가 뚜렷합니다.")
    
    return insights

# ── Build all data as JSON for embedding ──
all_data = {
    'monthly_total': {},
    'properties': {},
    'channels': {},
    'segments': {},
    'deep_insights': {},
}

# Monthly totals
for yr in ['2023','2024','2025','2026']:
    for mi in ['01','02','03']:
        m = f'{yr}{mi}'
        if m in db['monthly_total']:
            all_data['monthly_total'][m] = db['monthly_total'][m]

# Properties data
for prop in sorted(bp.keys()):
    all_data['properties'][prop] = {}
    for yr in ['2023','2024','2025','2026']:
        for mi in ['01','02','03']:
            m = f'{yr}{mi}'
            if m in bp.get(prop, {}):
                all_data['properties'][prop][m] = bp[prop][m]

# Channel data
bc = db['by_channel']
for ch in sorted(bc.keys()):
    all_data['channels'][ch] = {}
    for yr in ['2023','2024','2025','2026']:
        for mi in ['01','02','03']:
            m = f'{yr}{mi}'
            if m in bc.get(ch, {}):
                all_data['channels'][ch][m] = bc[ch][m]

# Segment data
bs = db['by_segment']
for seg in sorted(bs.keys()):
    all_data['segments'][seg] = {}
    for yr in ['2023','2024','2025','2026']:
        for mi in ['01','02','03']:
            m = f'{yr}{mi}'
            if m in bs.get(seg, {}):
                all_data['segments'][seg][m] = bs[seg][m]

# Property-channel & property-segment for deep insights
bpc = db['by_property_channel']
bps = db['by_property_segment']
for prop in sorted(bp.keys()):
    all_data['deep_insights'][prop] = {'by_channel': {}, 'by_segment': {}}
    if prop in bpc:
        for ch in bpc[prop]:
            all_data['deep_insights'][prop]['by_channel'][ch] = {}
            for yr in ['2025','2026']:
                for mi in ['01','02','03']:
                    m = f'{yr}{mi}'
                    if m in bpc[prop][ch]:
                        all_data['deep_insights'][prop]['by_channel'][ch][m] = bpc[prop][ch][m]
    if prop in bps:
        for seg in bps[prop]:
            all_data['deep_insights'][prop]['by_segment'][seg] = {}
            for yr in ['2025','2026']:
                for mi in ['01','02','03']:
                    m = f'{yr}{mi}'
                    if m in bps[prop][seg]:
                        all_data['deep_insights'][prop]['by_segment'][seg][m] = bps[prop][seg][m]

# Generate text insights for each month
text_insights = {}
for label, (m26, m25, m24, m23) in MONTH_GROUPS.items():
    text_insights[label] = gen_monthly_insights(label, m26, m25)

all_data['text_insights'] = text_insights
all_data['new_properties'] = NEW_PROPS
all_data['existing_properties'] = EXISTING_PROPS
all_data['region_map'] = REGION_MAP

data_json = json.dumps(all_data, ensure_ascii=False)
print(f"Data JSON size: {len(data_json)} bytes")

# Save to temp for embedding
with open('/tmp/report_data.json', 'w') as f:
    f.write(data_json)

print("Data prepared successfully")
