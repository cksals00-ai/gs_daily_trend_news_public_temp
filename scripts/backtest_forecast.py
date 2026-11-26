#!/usr/bin/env python3.12
"""예상 마감(포캐스트) 방식별 과거 정확도 검증 — GS 세그(OTA·G-OTA·Inbound), 사업장 × 세그 × 투숙월.

기준일 = RM 회의 날짜들(docs/data/rm_fcst_trend.json 스냅샷). 그날까지 들어와 있던 예약만으로 각 방식의
예상 마감을 계산하고, 같은 날의 RM 예측과 함께 실제 마감(raw 최종 유효 예약)과 비교한다.

방식
  RATIO  올해 온북 × 작년 마감/작년 같은 시점 온북 (작년 대비 비율, 상한 없음)
  RATIOc 올해 온북 + 작년 남은 판매 × 올해 속도(0.5~2배로 제한)        ← 기존 가이드 방식
  ADD    올해 온북 + 작년 같은 시점 이후 판매량(그대로 더함)
  BLEND  (RATIOc + ADD) / 2
  LYF    작년 마감 그대로
  RM     그 날짜 RM 예측
온북 = 기준일 이전 최초입력 + 그때 살아있던 취소건 복원(최초입력 ≤ 기준일 < 취소일). 작년 기준일 = 기준일 − 364일.
거래처(회원명=이용자명, 58 제외)·매출조정 제외 — 대시보드 정본 규칙과 동일.

출력: 콘솔 요약 + data/forecast_backtest.json (대외비·관리자용)
"""
import bisect
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
import build_adr_guide as G  # noqa: E402

GS = ('OTA', 'G-OTA', 'Inbound')
OUT = ROOT / 'data' / 'forecast_backtest.json'


def _d(s):
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


class Book:
    """(사업장, 세그, 투숙월) 단위 예약 이력 → 임의 기준일 온북."""
    __slots__ = ('alive', 'can_first', 'can_cancel', '_ready')

    def __init__(self):
        self.alive, self.can_first, self.can_cancel, self._ready = [], [], [], False

    def _prep(self):
        def pref(rows):
            rows.sort(key=lambda r: r[0])
            keys = [r[0] for r in rows]
            rn, rv = [0], [0]
            for _, a, b in rows:
                rn.append(rn[-1] + a); rv.append(rv[-1] + b)
            return keys, rn, rv
        self.alive = pref(self.alive)
        self.can_first = pref(self.can_first)
        self.can_cancel = pref(self.can_cancel)
        self._ready = True

    def onbook(self, cut):
        """cut(YYYYMMDD) 시점 온북 [rn, rev]. 살아있는 예약(최초 ≤ cut) + 취소건 중 최초 ≤ cut < 취소."""
        if not self._ready:
            self._prep()
        out = [0, 0]
        for (keys, rn, rv), sign in ((self.alive, 1), (self.can_first, 1), (self.can_cancel, -1)):
            i = bisect.bisect_right(keys, cut)
            out[0] += sign * rn[i]; out[1] += sign * rv[i]
        return out

    def final(self):
        if not self._ready:
            self._prep()
        return [self.alive[1][-1], self.alive[2][-1]]


def load_books(folders, months):
    """months: 필요한 투숙월(YYYYMM) 집합 → {(ym, disp, seg): Book}"""
    import parse_raw_db as P
    books = defaultdict(Book)
    years = {int(Path(f).name): f for f in folders if Path(f).exists() and Path(f).name.isdigit()}
    src = {}
    for ym in months:
        cand = [y for y in years if y <= int(ym[:4])]
        if cand:
            src[ym] = years[max(cand)]
    for folder in sorted(set(src.values())):
        own = {ym for ym, f in src.items() if f == folder}
        win = G._windows(folder)
        seen_c = set()
        for fp, (mn, mx) in sorted(win.items(), key=lambda kv: kv[0].name):
            ft = fp.name[:2]
            is_bk = ft in ('27', '43')
            f, cm = G._open(fp)
            if not f:
                continue
            g = lambda k: cm.get(k, -1)
            i_sell, i_ci, i_rooms, i_night = g('판매일자'), g('입실일자'), g('객실수'), g('1박객실료')
            i_mem, i_user, i_first, i_cancel = g('회원명'), g('이용자명'), g('최초입력일자'), g('취소일자')
            i_code = cm.get('변경예약집계코드', cm.get('예약집계코드', -1))
            i_cname = cm.get('변경예약집계코드명', cm.get('예약집계명', -1))
            i_agent = g('AGENT명')
            i_prop = cm.get('변경사업장명', cm.get('영업장명', -1))
            hi = max(i_sell, i_ci, i_rooms, i_night, i_mem, i_user, i_first, i_code, i_cname, i_agent, i_prop, i_cancel)
            seen = set()
            for line in f:
                key = hash(line.rstrip('\n\r'))
                if is_bk:
                    if key in seen:
                        continue
                    seen.add(key)
                else:
                    if key in seen_c:
                        continue
                    seen_c.add(key)
                p = line.split(';')
                if len(p) <= hi:
                    continue
                sell = p[i_sell].strip() if i_sell >= 0 else ''
                if len(sell) < 8:
                    sell = p[i_ci].strip() if i_ci >= 0 else ''
                    if len(sell) < 8:
                        continue
                ym = sell[:6]
                if ym not in own:
                    continue
                if is_bk and ((mn and ym < mn) or (mx and ym > mx)):
                    continue
                code = p[i_code].strip() if i_code >= 0 else ''
                mname = p[i_mem].strip() if i_mem >= 0 else ''
                uname = p[i_user].strip() if i_user >= 0 else ''
                if mname and uname and mname == uname and code != '58':
                    continue
                if '매출조정' in mname or '매출조정' in uname:
                    continue
                disp = G.prop_disp(p[i_prop].strip()) if i_prop >= 0 else None
                if not disp:
                    continue
                seg = P.classify_segment(code, p[i_cname].strip() if i_cname >= 0 else '',
                                         p[i_agent].strip() if i_agent >= 0 else '', ft)
                if seg not in GS:
                    continue
                rooms = p[i_rooms].strip() if i_rooms >= 0 else ''
                rn = int(rooms) if rooms.isdigit() and int(rooms) > 0 else 1
                night = p[i_night].strip() if i_night >= 0 else ''
                rev = int(int(night) * rn / 1.1) if night.lstrip('-').isdigit() else 0
                first = p[i_first].strip()[:8] if i_first >= 0 else ''
                if len(first) != 8:
                    continue
                b = books[(ym, disp, seg)]
                if is_bk:
                    b.alive.append((first, rn, rev))
                else:
                    canc = p[i_cancel].strip()[:8] if i_cancel >= 0 else ''
                    if len(canc) == 8 and canc >= first:
                        b.can_first.append((first, rn, rev))
                        b.can_cancel.append((canc, rn, rev))
            f.close()
    return books


def forecasts(cur, lys, lyf):
    """RN 기준 방식별 예상 마감."""
    pick = lyf - lys
    out = {'LYF': lyf, 'ADD': cur + pick}
    out['RATIO'] = cur * lyf / lys if lys > 0 else cur + pick
    pace = min(2.0, max(0.5, cur / lys)) if lys > 0 else 1.0
    out['RATIOc'] = cur + pick * pace
    out['BLEND'] = (out['RATIOc'] + out['ADD']) / 2
    return out


def horizon_bucket(asof, ym):
    m0 = date(int(ym[:4]), int(ym[4:]), 1)
    h = (m0 - asof).days
    if h <= 0:
        return 'a.당월 진행중', h
    if h <= 30:
        return 'b.1~30일 전', h
    if h <= 60:
        return 'c.31~60일 전', h
    if h <= 90:
        return 'd.61~90일 전', h
    return 'e.91일 이상 전', h


def main():
    rmt = json.loads((ROOT / 'docs' / 'data' / 'rm_fcst_trend.json').read_text(encoding='utf-8'))
    snaps = rmt['snapshots']
    last_closed = '202610'          # 마감 확정 월(이번 달 이전)
    cases = []                      # (asof, ym, disp, seg, rm_rn)
    months = set()
    for s in snaps:
        asof = datetime.strptime(s['_snapshot_date'], '%Y-%m-%d').date()
        for disp, pm in (s.get('properties') or {}).items():
            for ymk, v in pm.items():
                ym = ymk.replace('-', '')
                if ym > last_closed:
                    continue
                for seg in GS:
                    sv = ((v.get('segments') or {}).get(seg)) or {}
                    rmv = sv.get('rm_fcst_rn')
                    cases.append((asof, ym, disp, seg, rmv))
                    months.add(ym)
                    months.add(f"{int(ym[:4]) - 1}{ym[4:]}")
    folders = [str(ROOT / 'data' / 'raw_db' / str(y)) for y in range(2022, 2028)]
    t0 = datetime.now()
    books = load_books(folders, months)
    print(f"raw 로드 {len(books):,} 묶음 · {(datetime.now() - t0).seconds}초")

    rows = []
    for asof, ym, disp, seg, rmv in cases:
        lym = f"{int(ym[:4]) - 1}{ym[4:]}"
        b, lb = books.get((ym, disp, seg)), books.get((lym, disp, seg))
        if not b or not lb:
            continue
        act = b.final()[0]
        cur = b.onbook(asof.strftime('%Y%m%d'))[0]
        lys = lb.onbook((asof - timedelta(days=364)).strftime('%Y%m%d'))[0]
        lyf = lb.final()[0]
        fc = forecasts(cur, lys, lyf)
        if rmv is not None:
            fc['RM'] = rmv
        hb, h = horizon_bucket(asof, ym)
        rows.append({'asof': asof.isoformat(), 'ym': ym, 'disp': disp, 'seg': seg, 'h': h, 'hb': hb,
                     'act': act, 'cur': cur, 'lys': lys, 'lyf': lyf, 'fc': {k: round(v, 1) for k, v in fc.items()}})
    print(f"비교 건수 {len(rows):,} (사업장×세그×월×기준일)")

    methods = ['RATIO', 'RATIOc', 'ADD', 'BLEND', 'LYF', 'RM']

    def summarize(level):
        """level: 'gs'(전 사업장 GS 합) | 'seg'(세그 합) | 'prop'(사업장 GS 합). RM 있는 건만(같은 표본)."""
        agg = defaultdict(lambda: defaultdict(lambda: {'act': 0, 'fc': defaultdict(float), 'rm_ok': True}))
        for r in rows:
            k = {'gs': ('GS',), 'seg': (r['seg'],), 'prop': (r['disp'],)}[level]
            g = agg[r['hb']][(r['asof'], r['ym']) + k]
            g['act'] += r['act']
            for m in methods:
                if m in r['fc']:
                    g['fc'][m] += r['fc'][m]
                elif m == 'RM':
                    g['rm_ok'] = False
        res = {}
        for hb in sorted(agg):
            groups = [g for g in agg[hb].values() if g['rm_ok'] and g['act'] > 0]
            if not groups:
                continue
            A = sum(g['act'] for g in groups)
            res[hb] = {'n': len(groups), 'act': A}
            for m in methods:
                ae = sum(abs(g['fc'][m] - g['act']) for g in groups)
                bias = sum(g['fc'][m] - g['act'] for g in groups)
                res[hb][m] = {'wape': round(ae / A * 100, 1), 'bias': round(bias / A * 100, 1)}
        return res

    report = {'built': datetime.now().strftime('%Y-%m-%d %H:%M'), 'n_rows': len(rows),
              'levels': {lv: summarize(lv) for lv in ('gs', 'seg', 'prop')},
              'label': '대외비·관리자용 · 예측 방식 검증'}
    for lv, title in (('gs', 'GS 전체(전 사업장 OTA+GOTA+인바운드)'), ('seg', '세그별(전 사업장 합)'), ('prop', '사업장별(GS 합)')):
        print(f"\n■ {title} — 오차율 WAPE % (치우침 bias %)")
        print(f"{'시점':<14}{'건수':>6}" + ''.join(f"{m:>16}" for m in methods))
        for hb, v in report['levels'][lv].items():
            print(f"{hb:<14}{v['n']:>6}" + ''.join(f"{v[m]['wape']:>9.1f} ({v[m]['bias']:+5.1f})" for m in methods))
    OUT.write_text(json.dumps({'report': report, 'rows': rows}, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
    print(f"\n✓ {OUT.relative_to(ROOT)}")


if __name__ == '__main__':
    main()
