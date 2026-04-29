#!/usr/bin/env python3
"""
빨간불(Red Alert) 사업장 플래그 생성 스크립트
- daily_booking.json에서 리드타임 고려한 빨간불 사업장 판별
- 기획전 데이터(ALL_RECORDS)와 매칭
- 결과를 daily_booking.json에 alert_flags로 추가
"""
import json, os, re
from datetime import datetime, date
from collections import defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── 1. 리드타임 기반 월별 적정 Budget 달성률 기준 ──
REPORT_DATE = date(2026, 4, 28)

def get_achievement_threshold(month_key: str) -> float:
    year, month = map(int, month_key.split('-'))
    target_date = date(year, month, 1)
    if year == REPORT_DATE.year and month == REPORT_DATE.month:
        return 90.0
    months_ahead = (target_date.year - REPORT_DATE.year) * 12 + (target_date.month - REPORT_DATE.month)
    thresholds = {1: 70.0, 2: 30.0, 3: 10.0}
    return thresholds.get(months_ahead, 5.0)

# ── 2. 빨간불 판별 기준 ──
def evaluate_red_flags(prop: dict, month_key: str) -> dict:
    flags = {}
    threshold = get_achievement_threshold(month_key)
    if prop.get('name') == 'Grand Total':
        return flags
    ach = prop.get('budget_achievement', 0)
    if ach < threshold:
        flags['budget_under'] = f"달성률 {ach}% < 기준 {threshold}%"
    yoy = prop.get('yoy_pct', 0)
    ly = prop.get('ly_actual', 0)
    if ly > 0 and yoy < -5:
        flags['yoy_decline'] = f"YoY {yoy:+.1f}%"
    occ_change = prop.get('occ_yoy_change', 0)
    if occ_change < -5:
        flags['occ_decline'] = f"OCC YoY {occ_change:+.1f}%p"
    return flags

def get_severity(flags: dict) -> str:
    count = len(flags)
    if count >= 3: return 'critical'
    elif count == 2: return 'warning'
    elif count == 1: return 'watch'
    return 'normal'

# ── 3. 기획전 데이터 로드 ──
def load_promotions():
    # action_plan_dashboard.html은 루트 또는 docs/ 에 있을 수 있음
    html_path = os.path.join(BASE, 'action_plan_dashboard.html')
    if not os.path.exists(html_path):
        html_path = os.path.join(BASE, 'docs', 'action_plan_dashboard.html')
    with open(html_path, 'r', encoding='utf-8') as f:
        content = f.read()
    idx = content.find('const ALL_RECORDS = ')
    if idx == -1:
        return []
    json_start = content.find('[', idx)
    depth = 0
    for i in range(json_start, len(content)):
        if content[i] == '[': depth += 1
        elif content[i] == ']': depth -= 1
        if depth == 0:
            json_end = i + 1
            break
    return json.loads(content[json_start:json_end])

# ── 4. 사업장명 ↔ 기획전 site 매핑 ──
SITE_MAP = {
    '소노벨 비발디파크': '비발디', '소노캄 비발디파크': '비발디',
    '소노펫 비발디파크': '비발디', '소노펠리체 비발디파크': '비발디',
    '소노펠리체빌리지 비발디파크': '비발디', '소노벨 양평': '양평',
    '델피노': '델피노', '쏠비치 양양': '양양', '쏠비치 삼척': '삼척',
    '소노벨 단양': '단양', '소노캄 경주': '경주', '소노벨 청송': '청송',
    '소노벨 천안': '천안', '소노벨 변산': '변산', '소노캄 여수': '여수',
    '소노캄 거제': '거제', '쏠비치 진도': '진도', '소노벨 제주': '벨제주',
    '소노캄 제주': '캄제주', '소노캄 고양': '고양', '소노문 해운대': '해운대',
    '쏠비치 남해': '남해', '파나크 영덕': None, '르네블루': '르네블루',
    '팔라티움 해운대': '해운대',
}

def match_promotions(property_name: str, promotions: list, month_key: str) -> list:
    site = SITE_MAP.get(property_name)
    if not site:
        return []
    year, month = map(int, month_key.split('-'))
    month_start = f"{year}-{month:02d}-01"
    if month == 12:
        month_end = f"{year+1}-01-01"
    else:
        month_end = f"{year}-{month+1:02d}-01"
    matched = []
    for r in promotions:
        r_site = r.get('site', '')
        if site not in r_site and r_site not in site:
            continue
        stay = r.get('stay_period') or {}
        stay_end = stay.get('end') or ''
        stay_start = stay.get('start') or ''
        if stay_end >= month_start and stay_start < month_end:
            matched.append({
                'channel': r.get('channel', ''),
                'gs_channel': r.get('gs_channel', ''),
                'product': r.get('product', ''),
                'sale_period': r.get('sale_period', {}),
                'stay_period': stay,
                'exposure': r.get('exposure', ''),
                'branch': r.get('branch', ''),
            })
    return matched

# ── 5. 메인 ──
def main():
    db_path = os.path.join(BASE, 'docs', 'data', 'daily_booking.json')
    with open(db_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    promotions = load_promotions()
    print(f"기획전 데이터: {len(promotions)}건 로드")

    red_alerts = []
    for md in data['months_detail']:
        month_key = md['month_key']
        threshold = get_achievement_threshold(month_key)
        print(f"\n[{month_key}] 기준: {threshold}%")
        for prop in md['properties']:
            if prop['name'] == 'Grand Total':
                continue
            flags = evaluate_red_flags(prop, month_key)
            if not flags:
                continue
            severity = get_severity(flags)
            matched_promos = match_promotions(prop['name'], promotions, month_key)
            prop['alert_flags'] = {
                'is_red': severity in ('critical', 'warning'),
                'severity': severity,
                'flags': flags,
                'threshold': threshold,
                'matched_promotions_count': len(matched_promos),
            }
            if severity in ('critical', 'warning'):
                red_alerts.append({
                    'month': month_key,
                    'property': prop['name'],
                    'region': prop.get('region', ''),
                    'severity': severity,
                    'budget_achievement': prop.get('budget_achievement', 0),
                    'threshold': threshold,
                    'yoy_pct': prop.get('yoy_pct', 0),
                    'occ_yoy_change': prop.get('occ_yoy_change', 0),
                    'actual_rns': prop.get('actual_rns', 0),
                    'budget_rns': prop.get('budget_rns', 0),
                    'vs_budget': prop.get('vs_budget', 0),
                    'flags': flags,
                    'matched_promotions': matched_promos,
                })
                print(f"  {'🔴' if severity=='critical' else '🟡'} {severity.upper():8s} | {prop['name']}")

    data['red_alerts'] = {
        'generated_at': REPORT_DATE.isoformat(),
        'thresholds': {'2026-04': 90.0, '2026-05': 70.0, '2026-06': 30.0, '2026-07': 10.0},
        'criteria': {
            'budget_under': '월별 적정 달성률 미달',
            'yoy_decline': 'YoY 5% 이상 하락',
            'occ_decline': 'OCC 전년대비 5%p 이상 하락',
        },
        'severity_levels': {
            'critical': '3개 조건 모두 해당 (빨간불)',
            'warning': '2개 조건 해당 (주의)',
            'watch': '1개 조건 해당 (관찰)',
        },
        'alerts': red_alerts,
        'summary': {
            'total_alerts': len(red_alerts),
            'critical': len([a for a in red_alerts if a['severity'] == 'critical']),
            'warning': len([a for a in red_alerts if a['severity'] == 'warning']),
            'no_promotion_count': len([a for a in red_alerts if not a['matched_promotions']]),
        }
    }

    with open(db_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    data_path = os.path.join(BASE, 'data', 'daily_booking.json')
    if os.path.exists(data_path):
        with open(data_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 완료: CRITICAL {data['red_alerts']['summary']['critical']}건, WARNING {data['red_alerts']['summary']['warning']}건")

if __name__ == '__main__':
    main()
