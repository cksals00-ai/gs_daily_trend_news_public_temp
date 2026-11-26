#!/usr/bin/env python3.12
"""2027 상품 기획 매트릭스 빌더 — 상품 큐브 + 2027 예산 + 2026 실적 + GA4 상품 구매로 후보·배치·목표·근거 산출.

입력: docs/data/product_cube.json, data/db_aggregated.json(by_property_segment, 2026 실적), 2027 예산(generate_otb_data.load_budget_2027),
      docs/data/ga4_items.json(선택)
출력: docs/data/product_plan_2027.json  {meta, rows[], props[], months[]}
규칙(스펙 §4.5): 후보=기획연도 2025·2026, GS세그 RN≥100. ADR 가드레일=사업장·투숙월·시즌 가중 베이스라인 ADR×0.95.
      카니벌=캠페인 투숙월 주말 RN YoY ≤ −10% 의심. 배치=+364/728일(요일 정렬). 목표=실적×growth(예산27/실적26, 0.8~1.3).
      비용은 빈칸(페이지 입력), 추정값은 '목표·추정' 라벨. 대외비·관리자용.
"""
import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import product_naming as pn  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
WEEKEND = ('42', '43', '12', '14')
WEEKDAY = ('41', '11')
MIN_RN = 100


# ---------------------------------------------------------------- 입력 로더
def load_actual_by_prop_month(agg_path, prop_alias):
    """db_aggregated.by_property_segment → {display: {YYYYMM: net_rn(OTA+G-OTA+Inbound)}}"""
    if not agg_path or not Path(agg_path).exists():
        return {}
    doc = json.loads(Path(agg_path).read_text(encoding='utf-8'))
    out = defaultdict(lambda: defaultdict(int))
    for dbname, segs in (doc.get('by_property_segment') or {}).items():
        disp = prop_alias.get(dbname) or pn.prop_display(dbname)
        if not disp:
            continue
        for seg in pn.GS_SEGS:
            for ym, v in (segs.get(seg) or {}).items():
                out[disp][ym] += int((v or {}).get('net_rn') or 0)
    return {p: dict(m) for p, m in out.items()}


def load_budget27():
    """generate_otb_data.load_budget_2027 → {display: {MM: rn}}"""
    try:
        import generate_otb_data as g
        budgets, _seg = g.load_budget_2027()
    except Exception as e:  # noqa: BLE001
        print('⚠ 2027 예산 로드 실패:', e)
        return {}
    out = {}
    for disp, months in (budgets or {}).items():
        out[disp] = {}
        for label, v in (months or {}).items():
            mm = re.sub(r'\D', '', str(label)).zfill(2)
            out[disp][mm] = int((v or {}).get('rn') or 0)
    return out


def load_ga4_items(path):
    if not path or not Path(path).exists():
        return []
    return json.loads(Path(path).read_text(encoding='utf-8')).get('items', [])


# ---------------------------------------------------------------- 보조
def _d(s):
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def shift_weekday_aligned(yyyymmdd, years):
    """+1년=364일, +2년=728일 → 같은 요일 유지."""
    if not yyyymmdd or len(yyyymmdd) < 8:
        return None
    return (_d(yyyymmdd) + timedelta(days=364 * years)).strftime('%Y%m%d')


def month_add(ym, n):
    y, m = int(ym[:4]), int(ym[4:6])
    t = y * 12 + (m - 1) + n
    return f'{t // 12}{t % 12 + 1:02d}'


def camp_year(camp):
    if camp.get('year'):
        return int(camp['year'])
    sw = camp.get('sale_window') or {}
    if sw.get('from'):
        return int(sw['from'][:4])
    ys = sorted(camp.get('by_year', {}).keys())
    return int(ys[0]) if ys else None


def series_key(name):
    return pn.product_key(re.sub(r'\s*\(\d{4}\)\s*$', '', name))


# ---------------------------------------------------------------- 핵심 계산
def campaign_prop_metrics(cube, camp, prop):
    """캠페인 코드 중 prop·GS세그 코드의 실적(전 투숙월) + 시즌 믹스 + 채널 믹스."""
    rn = rev = comp = comm = bk = 0
    season_rn = defaultdict(int)
    month_rn = defaultdict(int)
    seg_rn = defaultdict(int)
    chan_rn = defaultdict(int)
    for code in camp['codes']:
        c = cube['codes'].get(code)
        if not c or c['prop'] != prop or c['seg'] not in pn.GS_SEGS:
            continue
        for ym, node in c['by_ym'].items():
            for sk, v in node['s'].items():
                rn += v[0]; rev += v[1]; comp += v[2]; comm += v[3]; bk += v[4]
                season_rn[sk] += v[0]; month_rn[ym] += v[0]
        tot_code = sum(v[0] for node in c['by_ym'].values() for v in node['s'].values())
        seg_rn[c['seg']] += tot_code
        chan_rn[c['chan']] += tot_code
    if not rn:
        return None
    return {'rn': rn, 'rev': rev, 'adr': rev / rn, 'comp_bk': (comp / bk) if bk else 0, 'comm_rate': (comm / (rev + comp)) if (rev + comp) else 0,
            'weekday_share': sum(season_rn[s] for s in WEEKDAY) / rn, 'season_rn': dict(season_rn), 'month_rn': dict(month_rn),
            'seg_mix': {k: round(v / rn, 3) for k, v in sorted(seg_rn.items(), key=lambda kv: -kv[1])},
            'top_chan': [k for k, _ in sorted(chan_rn.items(), key=lambda kv: -kv[1])[:3]], 'bk': bk}


def baseline_adr(cube, prop, months, season_rn):
    """사업장·투숙월·시즌 가중 베이스라인 ADR(GS세그 전 상품). months=캠페인 투숙월 목록."""
    bl = cube.get('baseline', {}).get(prop, {})
    tot = sum(season_rn.values()) or 1
    acc = 0.0; w_acc = 0.0
    for sk, w in season_rn.items():
        rn = rev = 0
        for ym in months:
            cell = bl.get(ym, {}).get(sk)
            if cell:
                rn += cell['rn']; rev += cell['rev']
        if rn:
            acc += (w / tot) * (rev / rn); w_acc += (w / tot)
    return (acc / w_acc) if w_acc else None


def weekend_yoy(cube, prop, months):
    """캠페인 투숙월의 사업장 주말 RN(42/43/12/14) 전년 동월 대비."""
    bl = cube.get('baseline', {}).get(prop, {})
    cur = prev = 0
    for ym in months:
        for sk in WEEKEND:
            cur += bl.get(ym, {}).get(sk, {}).get('rn', 0)
            prev += bl.get(month_add(ym, -12), {}).get(sk, {}).get('rn', 0)
    return (cur / prev - 1) if prev else None


def window_months(win):
    """{'from':YYYYMMDD,'to':YYYYMMDD} → 그 기간이 걸친 YYYYMM 집합."""
    if not win or not win.get('from') or not win.get('to'):
        return set()
    a, b = win['from'][:6], win['to'][:6]
    out, ym = set(), a
    while ym <= b and len(out) < 60:
        out.add(ym)
        ym = month_add(ym, 1)
    return out


def match_web(items_index, prop, camp_name, codes_series, months=None):
    """GA4 items(사업장·정규화키) ↔ 캠페인(사업장·시리즈키 집합). 정확키 → 부분포함(≥4자).
    months(YYYYMM 집합)가 있으면 그 달 구매만 합산 — 해마다 같은 이름으로 다시 나오는 상품이 다른 해 구매까지 끌어오지 않게."""
    keys = {series_key(camp_name)} | {pn.product_key(s) for s in codes_series if s}
    keys = {k for k in keys if len(k) >= 4}
    cand = items_index.get(prop, [])
    pur = rev = 0
    names = []
    for it in cand:
        k = it['_key']
        if not k:
            continue
        if k in keys or any((k in kk or kk in k) for kk in keys if len(k) >= 4):
            if months:
                p = sum(v[0] for ym, v in (it.get('by_month') or {}).items() if ym in months)
                r = sum(v[1] for ym, v in (it.get('by_month') or {}).items() if ym in months)
            else:
                p, r = it['purchases'], it['revenue']
            if p or r:
                pur += p; rev += r; names.append(it['name'])
    return {'purchases': pur, 'revenue': rev, 'items': names[:5]}


# ---------------------------------------------------------------- 구성 유형·구성 분석(스펙 §4.5 구성 제안)
ROOM_ONLY = '객실만'
COMP_ORDER = ('식음', '아쿠아', '부대', '스포츠', '기타')
COMP_LABEL = {'식음': '식음', '아쿠아': '아쿠아', '부대': '부대시설', '스포츠': '스포츠', '기타': '기타구성'}


def comp_type(comp_bk, auto):
    """실제 패키지 구성 매출(예약당 구성가치·구성 비중)로 구성 유형 라벨 — 상품명 추정 없음.
    예약당 구성가치 5천원 미만 또는 구성 없음 → 객실만. 비중 20% 이상 구성만 고정 순서로 표기, 3종 이상은 복합."""
    if (comp_bk or 0) < 5000 or not auto:
        return ROOM_ONLY
    sig = [k for k in COMP_ORDER if (auto.get(k) or 0) >= 0.2]
    if not sig or len(sig) >= 3:
        return '복합(3종+)'
    return '+'.join(COMP_LABEL[k] for k in sig)


def composition_analysis(rows):
    """사업장 × 구성 유형: 2025·26 기획 후보 실적 — 주중 RN·ADR·베이스라인 대비·객실만 대비·예약당 구성가치.
    '_reco'[사업장] = 권장 구성(객실만 제외, 상품 2개↑·주중 200실↑·ADR이 베이스라인 −5% 이내, 주중 매출 상위 2)."""
    acc = defaultdict(lambda: defaultdict(lambda: {'n': 0, 'rn': 0, 'wk_rn': 0.0, 'rev': 0, 'rev_b': 0, 'base_rev': 0.0,
                                                   'comp': 0.0, 'bk': 0, 'top': []}))
    for r in rows:
        a, g = r['actual'], r.get('guard') or {}
        for p in (r['prop'], '전체'):
            c = acc[p][r['comp_type']]
            c['n'] += 1; c['rn'] += a['rn']; c['wk_rn'] += a['rn'] * a['weekday_share']; c['rev'] += a['rev']
            if g.get('baseline_adr'):
                c['rev_b'] += a['rev']; c['base_rev'] += a['rn'] * g['baseline_adr']
            c['comp'] += a['comp_bk'] * a['bk']; c['bk'] += a['bk']
            c['top'].append((a['rn'] * a['weekday_share'], r['campaign']))
    out, reco = {}, {}
    for p, types in acc.items():
        ro = types.get(ROOM_ONLY)
        ro_adr = (ro['rev'] / ro['rn']) if ro and ro['rn'] else None
        out[p] = {}
        for t, c in types.items():
            adr = c['rev'] / c['rn'] if c['rn'] else 0
            out[p][t] = {'n': c['n'], 'rn': c['rn'], 'wk_rn': round(c['wk_rn']), 'wk_share': round(c['wk_rn'] / c['rn'], 3) if c['rn'] else 0,
                         'adr': round(adr), 'prem': round(c['rev_b'] / c['base_rev'] - 1, 3) if c['base_rev'] else None,
                         'vs_room': (round(adr / ro_adr - 1, 3) if (ro_adr and t != ROOM_ONLY) else None),
                         'comp_bk': round(c['comp'] / c['bk']) if c['bk'] else 0,
                         'top': [n for _, n in sorted(c['top'], key=lambda x: -x[0])[:3]]}
        if p != '전체':
            cands = [(t, v) for t, v in out[p].items() if t != ROOM_ONLY and v['n'] >= 2 and v['wk_rn'] >= 200
                     and (v['prem'] is None or v['prem'] >= -0.05)]
            cands.sort(key=lambda tv: -(tv[1]['wk_rn'] * tv[1]['adr']))
            reco[p] = [t for t, _ in cands[:2]]
    out['_reco'] = reco
    return out


def build(cube, actual26, budget27, ga4_items, base_ym):
    items_index = defaultdict(list)
    for it in ga4_items:
        it['_key'] = pn.product_key(it.get('series') or it.get('name') or '')
        # '사업장_객실타입' 형태(룸온리 객실 상품)는 시리즈가 사업장명과 같음 → 캠페인 매칭 대상 아님
        if not it['_key'] or it['_key'] == pn.product_key(it.get('category') or ''):
            continue
        items_index[it.get('prop')].append(it)
    rows = []
    best = {}  # (series_key, prop) → row (최근 연도 우선)
    for name, camp in cube['campaigns'].items():
        yr = camp_year(camp)
        if yr not in (2025, 2026):
            continue
        codes_series = [cube['codes'][c]['series'] for c in camp['codes'] if c in cube['codes']]
        for prop in camp['props']:
            m = campaign_prop_metrics(cube, camp, prop)
            if not m or m['rn'] < MIN_RN:
                continue
            months = sorted(m['month_rn'])
            yrs = 2027 - yr
            sw, st = camp.get('sale_window') or {}, camp.get('stay_window') or {}
            plan_sale_from = shift_weekday_aligned(sw.get('from'), yrs) if sw else None
            plan_sale_to = shift_weekday_aligned(sw.get('to'), yrs) if sw else None
            plan_stay_from = shift_weekday_aligned(st.get('from'), yrs) if st else None
            plan_stay_to = shift_weekday_aligned(st.get('to'), yrs) if st else None
            plan_month = (plan_stay_from or '2027')[:6] if plan_stay_from else None
            # growth: 투숙월별 예산27/실적26 가중(캠페인 투숙월 RN 가중), 미래월·결측은 1.0
            g_acc = g_w = 0.0
            for ym, w in m['month_rn'].items():
                ym26 = '2026' + ym[4:6]
                a26 = actual26.get(prop, {}).get(ym26, 0)
                b27 = budget27.get(prop, {}).get(ym[4:6], 0)
                g = (b27 / a26) if (a26 and b27 and ym26 <= base_ym) else 1.0
                g = max(0.8, min(1.3, g))
                g_acc += g * w; g_w += w
            growth = (g_acc / g_w) if g_w else 1.0
            b_adr = baseline_adr(cube, prop, months, m['season_rn'])
            adr_ok = (b_adr is None) or (m['adr'] >= 0.95 * b_adr)
            wk_yoy = weekend_yoy(cube, prop, months)
            cannibal = (wk_yoy is not None and wk_yoy <= -0.10)
            target_rn = int(round(m['rn'] * growth))
            target_adr = max(m['adr'], b_adr or 0)
            target_rev = target_rn * target_adr
            web = match_web(items_index, prop, name, codes_series, months=window_months(sw) or None)
            flags = []
            if not adr_ok: flags.append('ADR 베이스라인 미달')
            if cannibal: flags.append('주말 잠식 의심')
            if m['weekday_share'] >= 0.5: flags.append('주중 기여')
            basis = (f"{yr} {m['rn']:,}실·ADR {m['adr']/1000:.0f}천·주중 {m['weekday_share']*100:.0f}%·구성 {m['comp_bk']/1000:.0f}천/예약"
                     + (f"·웹 {web['purchases']:,}건" if web['purchases'] else '') + (f"·예산성장 ×{growth:.2f}" if abs(growth - 1) > 0.005 else ''))
            row = {
                'id': f"{prop}|{series_key(name)}", 'campaign': name, 'source': camp.get('source', 'auto'), 'category': camp.get('category'),
                'prop': prop, 'year': yr, 'composition': (camp.get('composition') or {}).get('text') or '',
                'composition_auto': (camp.get('composition') or {}).get('auto') or {}, 'note': camp.get('note') or '',
                'comp_type': comp_type(m['comp_bk'], (camp.get('composition') or {}).get('auto') or {}),
                'actual': {'rn': m['rn'], 'rev': round(m['rev']), 'adr': round(m['adr']), 'bk': m['bk'], 'comp_bk': round(m['comp_bk']),
                           'comm_rate': round(m['comm_rate'], 4), 'weekday_share': round(m['weekday_share'], 3),
                           'sale': camp.get('sale_text') or (f"{sw.get('from','')}~{sw.get('to','')}" if sw else ''),
                           'sale_window': sw or None, 'stay_window': st or None, 'months': months,
                           'seg_mix': m['seg_mix'], 'top_chan': m['top_chan']},
                'guard': {'baseline_adr': round(b_adr) if b_adr else None, 'adr_ok': adr_ok,
                          'weekend_yoy': round(wk_yoy, 3) if wk_yoy is not None else None, 'cannibal': cannibal},
                'plan': {'sale_from': plan_sale_from, 'sale_to': plan_sale_to, 'stay_from': plan_stay_from, 'stay_to': plan_stay_to,
                         'month': plan_month, 'growth': round(growth, 3), 'target_rn': target_rn, 'target_adr': round(target_adr),
                         'target_rev': round(target_rev), 'comm': round(target_rev * m['comm_rate']),
                         'weekday_rn': int(round(target_rn * m['weekday_share'])),
                         'weekday_rev': round(target_rn * m['weekday_share'] * target_adr), 'cost': None},
                'web': web, 'flags': flags, 'basis': basis,
            }
            key = (series_key(name), prop)
            prev = best.get(key)
            if prev is None:
                best[key] = row
            elif (row['year'], row['actual']['rn']) > (prev['year'], prev['actual']['rn']):
                row['basis'] += f" ({'이전' if prev['year'] < row['year'] else '동년'} {prev['year']} {prev['actual']['rn']:,}실)"
                best[key] = row
            else:
                prev['basis'] += f" ({'이전' if row['year'] < prev['year'] else '동년'} {row['year']} {row['actual']['rn']:,}실)"
    rows = sorted(best.values(), key=lambda r: -r['plan']['weekday_rev'])
    for i, r in enumerate(rows, 1):
        r['rank'] = i
    return rows


def web_by_campaign(cube, ga4_items, min_year=2024):
    """SEC8(기획전 레벨) '웹 구매' 열: 사업장|기획전 → [구매건, 매출] — 판매기간 달의 홈페이지(GA4) 같은 이름 상품 구매. 매칭분만."""
    idx = defaultdict(list)
    for it in ga4_items:
        k = pn.product_key(it.get('series') or it.get('name') or '')
        if not k or k == pn.product_key(it.get('category') or ''):
            continue
        it['_key'] = k
        idx[it.get('prop')].append(it)
    out = {}
    for name, camp in cube['campaigns'].items():
        yr = camp_year(camp)
        if not yr or yr < min_year:
            continue
        sw = camp.get('sale_window') or {}
        months = window_months(sw) or None
        series = [cube['codes'][c]['series'] for c in camp['codes'] if c in cube['codes']]
        for prop in camp.get('props') or []:
            if not idx.get(prop):
                continue
            w = match_web(idx, prop, name, series, months=months)
            if w['purchases']:
                out[f'{prop}|{name}'] = [w['purchases'], round(w['revenue'])]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cube', default=str(ROOT / 'docs' / 'data' / 'product_cube.json'))
    ap.add_argument('--agg', default=str(ROOT / 'data' / 'db_aggregated.json'))
    ap.add_argument('--ga4', default=str(ROOT / 'docs' / 'data' / 'ga4_items.json'))
    ap.add_argument('--out', default=str(ROOT / 'docs' / 'data' / 'product_plan_2027.json'))
    a = ap.parse_args()
    cube = json.loads(Path(a.cube).read_text(encoding='utf-8'))
    base_ym = (cube['meta'].get('base_date') or '2026-11-25').replace('-', '')[:6]
    base_ym = month_add(base_ym, -1)   # 완결월까지만 growth 산정
    actual26 = load_actual_by_prop_month(a.agg, cube['dims']['prop_alias'])
    budget27 = load_budget27()
    ga4_items = load_ga4_items(a.ga4)
    rows = build(cube, actual26, budget27, ga4_items, base_ym)
    comp = composition_analysis(rows)
    webc = web_by_campaign(cube, ga4_items)
    out = {'meta': {'built': cube['meta']['built'], 'base_date': cube['meta'].get('base_date'), 'rows': len(rows),
                    'comp_rule': '구성 유형=예약당 구성가치 5천원↑ 상품의 구성 매출 비중 20%↑ 항목(식음·아쿠아·부대시설·스포츠·기타구성), 3종↑ 복합, 그 외 객실만',
                    'reco_rule': '권장 구성=객실만 제외, 상품 2개↑·주중 200실↑·ADR이 베이스라인 −5% 이내인 구성 중 주중 매출 상위 2',
                    'web_rule': '홈페이지(GA4) 같은 이름 상품의 판매기간 달 구매(예약일 기준)',
                    'min_rn': MIN_RN, 'growth_rule': '예산2027/실적2026 월별 가중, 0.8~1.3 클램프, 미래월 1.0',
                    'adr_rule': '캠페인 ADR ≥ 사업장·투숙월·시즌 가중 베이스라인 ADR×0.95', 'placement': '+364/728일(요일 정렬)',
                    'budget27_props': len(budget27), 'actual26_props': len(actual26), 'ga4_items': len(ga4_items),
                    'label': '대외비·관리자용 · 목표 열은 추정'},
           'props': [p['display'] for p in cube['dims']['properties']],
           'months': [f'2027{m:02d}' for m in range(1, 13)], 'rows': rows, 'composition': comp, 'web_by_campaign': webc}
    Path(a.out).write_text(json.dumps(out, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
    print(f"rows={len(rows)} budget27 props={len(budget27)} actual26 props={len(actual26)} ga4 items={len(ga4_items)} → {a.out} ({os.path.getsize(a.out)/1e6:.1f}MB)")
    print('top5:', [(r['prop'], r['campaign'][:28], r['plan']['target_rn'], r['plan']['weekday_rev'] // 1_000_000) for r in rows[:5]])


if __name__ == '__main__':
    main()
