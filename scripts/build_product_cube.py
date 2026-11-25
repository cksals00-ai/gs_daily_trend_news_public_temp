#!/usr/bin/env python3.12
"""상품 큐브 빌더 — raw_db 27 파일 → docs/data/product_cube.json(.gz)

사용:
  python3.12 scripts/build_product_cube.py [--years 2023 2024 2025 2026] [--raw-dir data/raw_db]
      [--out docs/data/product_cube.json] [--campaign-map data/product_campaign_map.json] [--max-rows N]

규칙(스펙 docs/superpowers/specs/2026-11-25-product-plan-2027-design.md):
  - 입력은 27(예약) 파일만. 재전송 = 유효 예약(취소 제외). 2026은 재전송+최신 스냅샷을 투숙월 경계로 분리
  - 객실매출 = 1박객실료 × 객실수 ÷ 1.1 (VAT 제외)
  - 구성가치 = (PKG패키지총금액 − PKG객실) ÷ 1.1, KEY_RSV_NO당 1회
  - 상품코드 = 회원번호 86… ; 베이스라인은 GS 세그(72/53/A4/A5/58) 전 행
대외비·관리자용 산출물. 기존 parse_raw_db/build.py와 독립.
"""
import argparse
import csv
import gzip
import json
import os
import re
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import product_naming as pn  # noqa: E402
from parse_package_trend import classify_v4  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
GS_CODES = {'72', '53', 'A4', 'A5', '58'}
# 'PKG영업장'은 식음+부대+스포츠+아쿠아+기타의 소계 컬럼이므로 구성 분해에서 제외(중복 방지)
PKG_PART_COLS = {'식음': ('PKG영업장식음',), '부대': ('PKG영업장부대', '부대'), '스포츠': ('PKG영업장스포츠', '스포츠'),
                 '아쿠아': ('PKG영업장아쿠아', '아쿠아'), '골프': ('PKG골프',),
                 '기타': ('PKG영업장기타', 'PKG기타', '기타')}
_RETRANS_RE = re.compile(r'\((\d{8})-(\d{8})\)')
_SNAP_RE = re.compile(r'_(\d{8})_생성시간')


# ---------------------------------------------------------------- 파일 선택
def _prev_month(ym):
    y, m = int(ym[:4]), int(ym[4:])
    return f'{y-1}12' if m == 1 else f'{y}{m-1:02d}'


def _snapshot_min_stay_month(path, min_rows=500):
    """스냅샷이 실제 보유한 최소 투숙월(행 min_rows 이상). parse_raw_db와 동일 규칙."""
    counts = Counter()
    with open(path, encoding='cp949', errors='replace', newline='') as f:
        rd = csv.reader(f, delimiter=';')
        header = [h.strip().lstrip('﻿') for h in next(rd)]
        i = header.index('판매일자')
        for parts in rd:
            if i < len(parts):
                d = parts[i].strip()
                if len(d) >= 6 and d[:6].isdigit():
                    counts[d[:6]] += 1
    valid = sorted(ym for ym, c in counts.items() if c >= min_rows)
    return valid[0] if valid else None


def select_files(raw_dir, years):
    """연도별 27 파일 선택 → [(path, min_ym|None, max_ym|None)]."""
    out = []
    for y in years:
        d = Path(raw_dir) / str(y)
        if not d.is_dir():
            continue
        files = [p for p in d.iterdir() if p.is_file() and p.name.startswith('27') and p.suffix == '.txt']
        retrans = [p for p in files if _RETRANS_RE.search(p.name)]
        snaps = [p for p in files if _SNAP_RE.search(p.name)]
        snaps.sort(key=lambda p: _SNAP_RE.search(p.name).group(1), reverse=True)
        snaps = snaps[:1]
        if retrans and snaps:
            smin = _snapshot_min_stay_month(snaps[0])
            rmax = _prev_month(smin) if smin else None
            for p in retrans:
                out.append((p, None, rmax))
            out.append((snaps[0], smin, None))
        else:
            for p in retrans or snaps:
                out.append((p, None, None))
    return out


# ---------------------------------------------------------------- 행 파서
def _num(s):
    s = (s or '').replace(',', '').strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def iter_rows(path, min_ym, max_ym, max_rows=None):
    with open(path, encoding='cp949', errors='replace', newline='') as f:
        rd = csv.reader(f, delimiter=';')
        header = [h.strip().lstrip('﻿') for h in next(rd)]
        idx = {h: i for i, h in enumerate(header)}

        def col(name, alt=None):
            if name in idx:
                return idx[name]
            return idx.get(alt, -1) if alt else -1

        i_key, i_stay, i_prop, i_prop2 = col('KEY_RSV_NO'), col('판매일자'), col('변경사업장명'), col('영업장명')
        i_code, i_name, i_rt, i_seg = col('회원번호'), col('회원명'), col('객실타입'), col('변경예약집계코드', '예약집계코드')
        i_season, i_rate, i_nights, i_rooms = col('요금시즌코드'), col('1박객실료'), col('박수'), col('객실수')
        i_booked, i_agent, i_pr, i_pt = col('최초입력일자'), col('AGENT명'), col('PKG객실'), col('PKG패키지총금액')
        i_sale, i_dep, i_comm, i_pc = col('판매가'), col('입금가'), col('수수료'), col('패키지분류코드명')
        parts_idx = {k: [idx[c] for c in cols if c in idx] for k, cols in PKG_PART_COLS.items()}
        n = 0
        for p in rd:
            if len(p) <= i_stay:
                continue
            stay = p[i_stay].strip().replace('-', '')[:8]
            if len(stay) != 8 or not stay.isdigit():
                continue
            ym = stay[:6]
            if min_ym and ym < min_ym:
                continue
            if max_ym and ym > max_ym:
                continue

            def g(i, _p=p):
                return _p[i].strip() if 0 <= i < len(_p) else ''

            pkg_parts = {}
            for k, ii in parts_idx.items():
                v = sum(_num(p[i]) for i in ii if i < len(p))
                if v:
                    pkg_parts[k] = v
            yield {
                'key': g(i_key), 'stay': stay, 'prop_raw': g(i_prop) or g(i_prop2), 'code': g(i_code), 'name': g(i_name),
                'roomtype': g(i_rt), 'season': g(i_season), 'rate': _num(g(i_rate)),
                'nights': int(_num(g(i_nights)) or 1), 'rooms': int(_num(g(i_rooms)) or 1),
                'booked': g(i_booked).replace('-', '')[:8], 'agent': g(i_agent), 'seg_code': g(i_seg),
                'pkg_room': _num(g(i_pr)), 'pkg_total': _num(g(i_pt)), 'pkg_parts': pkg_parts,
                'sale_price': _num(g(i_sale)), 'deposit': _num(g(i_dep)), 'comm': _num(g(i_comm)), 'pkg_class': g(i_pc),
            }
            n += 1
            if max_rows and n >= max_rows:
                return


def _ymd(s):
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------- 집계기
class CubeBuilder:
    def __init__(self):
        self.codes = {}
        # prop → year → season → [rn, rev]
        self.baseline = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: [0, 0.0])))
        self.rows_parsed = 0
        self._seen = set()  # (code, key): 구성가치·예약건수·리드타임 1회 가산

    def _code_node(self, code, prop):
        c = self.codes.get(code)
        if c is None:
            c = self.codes[code] = {
                'prop': prop, 'by_ym': {}, 'rt': defaultdict(lambda: [0, 0.0]), 'ag': defaultdict(lambda: [0, 0.0]),
                'seg_counts': Counter(), 'chan_counts': Counter(), 'comm_ratios': [], 'parts': Counter(),
                'pkg_class': Counter(), 'sale_days': Counter(), 'stay_days': Counter(), 'names': Counter(),
            }
        return c

    def add(self, r):
        self.rows_parsed += 1
        prop = pn.prop_display(r['prop_raw'])
        if prop is None:
            return
        seg, chan = pn.channel_class(r['agent'], r['seg_code'], r['name'])
        rn = r['rooms']
        rev = r['rate'] * rn / 1.1
        year, ym = r['stay'][:4], r['stay'][:6]
        if r['seg_code'] in GS_CODES:
            b = self.baseline[prop][year][r['season'] or '00']
            b[0] += rn
            b[1] += rev
        if not r['code'].startswith('86'):
            return
        c = self._code_node(r['code'], prop)
        c['names'][r['name']] += 1
        node = c['by_ym'].get(ym)
        if node is None:
            node = c['by_ym'][ym] = {'s': {}, 'dow': [0] * 7, 'lead_list': []}
        s = node['s'].setdefault(r['season'] or '00', [0, 0.0, 0.0, 0.0, 0])
        s[0] += rn
        s[1] += rev
        s[3] += r['comm'] / 1.1
        d = _ymd(r['stay'])
        if d:
            node['dow'][(d.weekday() + 1) % 7] += rn  # 0=일 … 6=토
        k = (r['code'], r['key'] or (r['stay'] + r['booked'] + r['roomtype']))
        if k not in self._seen:
            self._seen.add(k)
            s[4] += 1
            room_part = r['pkg_room'] if r['pkg_room'] > 0 else r['rate'] * r['nights']
            if r['pkg_total'] > room_part > 0:
                s[2] += (r['pkg_total'] - room_part) / 1.1
            for pk, pv in r['pkg_parts'].items():
                c['parts'][pk] += pv
            bd = _ymd(r['booked'])
            if bd and d:
                node['lead_list'].append(max((d - bd).days, 0))
            if r['pkg_total'] > 0 and r['comm'] > 0:
                c['comm_ratios'].append(r['comm'] / r['pkg_total'])
            if r['booked']:
                c['sale_days'][r['booked'][:8]] += 1
        c['stay_days'][r['stay']] += rn
        c['rt'][r['roomtype']][0] += rn
        c['rt'][r['roomtype']][1] += rev
        c['ag'][chan][0] += rn
        c['ag'][chan][1] += rev
        c['seg_counts'][seg] += 1
        c['chan_counts'][chan] += 1
        if r['pkg_class']:
            c['pkg_class'][r['pkg_class']] += 1


# ---------------------------------------------------------------- 마무리
def _pct(sorted_list, q):
    if not sorted_list:
        return None
    i = min(int(round((len(sorted_list) - 1) * q)), len(sorted_list) - 1)
    return sorted_list[i]


def _window(counter, lo=0.05, hi=0.95):
    """{'YYYYMMDD': n} → p05~p95 가중 분위 구간. 빈 경우 None."""
    items = sorted((d, n) for d, n in counter.items() if len(d) == 8 and d.isdigit())
    tot = sum(n for _, n in items)
    if not tot:
        return None
    acc, lo_d, hi_d, lo_set = 0, items[0][0], items[-1][0], False
    for d, n in items:
        acc += n
        if not lo_set and acc >= tot * lo:
            lo_d, lo_set = d, True
        if acc >= tot * hi:
            hi_d = d
            break
    return {'from': lo_d, 'to': hi_d}


def finalize(builder, cmap, years, base_date):
    codes_out, campaigns = {}, {}
    for code, c in builder.codes.items():
        name = c['names'].most_common(1)[0][0]
        seg = c['seg_counts'].most_common(1)[0][0] if c['seg_counts'] else '기타'
        chan = c['chan_counts'].most_common(1)[0][0] if c['chan_counts'] else seg
        sale_days = [d for d in c['sale_days'] if d]
        first_sale = min(sale_days) if sale_days else None
        sale_year = int(first_sale[:4]) if first_sale else int(min(c['by_ym'])[:4])
        campaign = pn.assign_campaign(cmap, code, name, sale_year, first_sale)
        clean = pn.clean_product_name(name)
        by_ym = {}
        for ym, node in sorted(c['by_ym'].items()):
            ll = sorted(node['lead_list'])
            by_ym[ym] = {
                's': {k: [v[0], round(v[1]), round(v[2]), round(v[3]), v[4]] for k, v in sorted(node['s'].items())},
                'dow': node['dow'],
                'lead': {'p50': _pct(ll, 0.5), 'p90': _pct(ll, 0.9), 'mean': round(statistics.fmean(ll), 1) if ll else None},
            }
        rt = dict(sorted(((k, [v[0], round(v[1])]) for k, v in c['rt'].items()), key=lambda kv: -kv[1][0])[:8])
        ag = dict(sorted(((k, [v[0], round(v[1])]) for k, v in c['ag'].items()), key=lambda kv: -kv[1][0])[:8])
        codes_out[code] = {
            'name': name, 'series': clean, 'campaign': campaign, 'category': classify_v4(clean), 'prop': c['prop'],
            'seg': seg, 'chan': chan,
            'comm_rate': round(statistics.median(c['comm_ratios']), 4) if c['comm_ratios'] else None,
            'pkg_class': c['pkg_class'].most_common(1)[0][0] if c['pkg_class'] else '',
            'by_ym': by_ym, 'rt': rt, 'ag': ag,
        }
        camp = campaigns.setdefault(campaign, {
            'codes': [], 'props': set(), 'cats': Counter(), 'by_year': defaultdict(lambda: [0, 0.0, 0.0, 0.0, 0, 0]),
            'sale_days': Counter(), 'stay_days': Counter(), 'parts': Counter()})
        camp['codes'].append(code)
        camp['props'].add(c['prop'])
        camp['cats'][codes_out[code]['category']] += 1
        camp['sale_days'].update(c['sale_days'])
        camp['stay_days'].update(c['stay_days'])
        camp['parts'].update(c['parts'])
        for ym, node in c['by_ym'].items():
            y = ym[:4]
            for sk, v in node['s'].items():
                b = camp['by_year'][y]
                b[0] += v[0]; b[1] += v[1]; b[2] += v[2]; b[3] += v[3]; b[4] += v[4]
                if sk in pn.WEEKDAY_SEASONS:
                    b[5] += v[0]

    camps_out = {}
    for name, camp in campaigns.items():
        meta = cmap['meta'].get(name, {})
        parts_tot = sum(camp['parts'].values())
        auto = {k: round(v / parts_tot, 3) for k, v in camp['parts'].most_common()} if parts_tot else {}
        by_year = {}
        for y, b in sorted(camp['by_year'].items()):
            by_year[y] = {'rn': b[0], 'rev': round(b[1]), 'comp': round(b[2]), 'comm': round(b[3]), 'bk': b[4],
                          'adr': round(b[1] / b[0]) if b[0] else 0, 'weekday_share': round(b[5] / b[0], 3) if b[0] else 0}
        camps_out[name] = {
            'codes': sorted(camp['codes']),
            'props': sorted(camp['props'], key=lambda p: pn.PROP_ORDER.index(p) if p in pn.PROP_ORDER else 99),
            'category': camp['cats'].most_common(1)[0][0], 'year': meta.get('year'), 'scope': meta.get('scope'),
            'composition': {'auto': auto, 'text': meta.get('composition') or ''},
            'sale_window': _window(camp['sale_days']), 'stay_window': _window(camp['stay_days']),
            'sale_text': meta.get('sale') or '', 'note': meta.get('note') or '', 'by_year': by_year,
        }

    baseline = {p: {y: {s: {'rn': v[0], 'rev': round(v[1]), 'adr': round(v[1] / v[0]) if v[0] else 0}
                        for s, v in ss.items()} for y, ss in ys.items()} for p, ys in builder.baseline.items()}
    prop_alias = {}
    for _bi, disp, _reg, dbs in pn.PROPERTY_DEFS:
        for n in dbs:
            prop_alias[n] = disp
        prop_alias[disp] = disp
    return {
        'meta': {'built': datetime.now().strftime('%Y-%m-%d %H:%M'), 'years': list(years), 'base_date': base_date,
                 'rows_parsed': builder.rows_parsed, 'codes': len(codes_out), 'campaigns': len(camps_out),
                 'rules': {'rev': '1박객실료×객실수÷1.1', 'comp': '(PKG총−PKG객실)÷1.1 예약당 1회',
                           'src': '27 파일(유효 예약)', 'weekday': '요금시즌 41(보조 11)'},
                 'label': '대외비·관리자용'},
        'dims': {'seasons': pn.SEASONS, 'gs_segs': list(pn.GS_SEGS),
                 'properties': [{'display': d[1], 'region': d[2]} for d in pn.PROPERTY_DEFS], 'prop_alias': prop_alias,
                 'categories': sorted({v['category'] for v in codes_out.values()})},
        'codes': codes_out, 'campaigns': camps_out, 'baseline': baseline,
    }


def write_outputs(cube, out_path):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    txt = json.dumps(cube, ensure_ascii=False, separators=(',', ':'))
    out_path.write_text(txt, encoding='utf-8')
    gz_path = out_path.with_suffix('.json.gz')
    with gzip.open(gz_path, 'wb', compresslevel=9) as f:
        f.write(txt.encode('utf-8'))
    return len(txt.encode('utf-8')), gz_path.stat().st_size


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--years', nargs='+', type=int, default=[2023, 2024, 2025, 2026])
    ap.add_argument('--raw-dir', default=str(ROOT / 'data' / 'raw_db'))
    ap.add_argument('--out', default=str(ROOT / 'docs' / 'data' / 'product_cube.json'))
    ap.add_argument('--campaign-map', default=str(ROOT / 'data' / 'product_campaign_map.json'))
    ap.add_argument('--max-rows', type=int, default=None)
    a = ap.parse_args()
    t0 = time.time()
    files = select_files(a.raw_dir, a.years)
    if not files:
        sys.exit('27 파일 없음')
    b = CubeBuilder()
    base_date = None
    for p, lo, hi in files:
        m = _SNAP_RE.search(p.name)
        if m:
            base_date = f'{m.group(1)[:4]}-{m.group(1)[4:6]}-{m.group(1)[6:]}'
        n0 = b.rows_parsed
        for r in iter_rows(p, lo, hi, a.max_rows):
            b.add(r)
        print(f'  {p.name[:60]:60s} [{lo or "-"}..{hi or "-"}] rows={b.rows_parsed - n0:,} ({time.time()-t0:.0f}s)', flush=True)
    cube = finalize(b, pn.load_campaign_map(a.campaign_map), a.years, base_date or datetime.now().strftime('%Y-%m-%d'))
    n1, n2 = write_outputs(cube, a.out)
    print(f'codes={cube["meta"]["codes"]:,} campaigns={cube["meta"]["campaigns"]:,} '
          f'json={n1/1e6:.1f}MB gz={n2/1e6:.1f}MB total {time.time()-t0:.0f}s')


if __name__ == '__main__':
    main()
