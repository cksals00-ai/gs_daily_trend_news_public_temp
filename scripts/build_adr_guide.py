#!/usr/bin/env python3.12
"""예약추이 ADR 가이드 빌더 — 작년 같은 시점 이후 ADR 흐름 + 올해 남은 주말·주중 재고로
'남은 판매 예상 ADR'과 '예상 마감 ADR'을 계산할 재료를 만든다. 계산 자체는 페이지(booking-trend.html)가
선택한 세그·사업장 조합으로 즉시 수행하고, 같은 알고리즘을 guide_calc()로 이 파일에도 둔다(테스트·대조용).

입력
  data/raw_db/{2025,2026}  27/43 예약 + 28/44 취소 (온라인영업팀: 대매점·OTA·패키지) → GS 세그(OTA·G-OTA·Inbound)
  data/daily_occ_history.json   올해 날짜별 온북 RN·OCC·일 보유객실(전 세그, Daily Booking Report 최신 보고일)
  docs/data/booking_status_detail.json  사업장 전체(전 세그) 월별: 올해 온북 · 작년 같은 날 온북 · 작년 마감 (parse_booking_status.py)
출력 docs/data/adr_guide.json
  {meta, months: {YYYYMM: {ly_ym, props: {display: {
      seg: {OTA: {cur:{w:[rn,rev], e:[rn,rev]}, lys:{…}, lyf:{…}}, G-OTA: …, Inbound: …},
      ly_days: {w: n, e: n},                      # 작년 기준일 이후 남은 날짜 수(유형별)
      dates: [[일, 'w'|'e', 빈객실, OCC, 온북RN], …] | null   # 올해 기준일 다음날부터(전 세그, PDF)
      whole: {cur|lys|lyf: [RN, 매출천원, 판매가능객실]} | null    # 사업장 전체(전 세그) 월 합
  }}}}}
규칙
  - 기준일(ty_asof) = Daily Booking Report 보고일. 작년 기준일 = ty_asof − 364일(같은 요일, Booking Status 표기와 동일).
  - 전년동기(lys) = 최초입력일자 ≤ 작년 기준일 + 그때 살아있던 취소건 복원(최초입력 ≤ 기준일 < 취소일). 전년마감(lyf) = 최종 유효 예약.
  - 정본: REV = 1박객실료 × 객실수 ÷ 1.1, 거래처(회원명=이용자명, 58 제외)·매출조정 제외.
  - 유형: 요금시즌명에 '주중' → 주중(w), 그 외(주말·금요일·연휴·최성수기) → 주말·연휴(e). 날짜 유형은 그 날짜 예약의 요금시즌 최빈값, 없으면 금·토=주말.
  - 남은 판매 물량 = 작년 같은 시점 이후 판매량 그대로(속도 배수 없음) — 과거 검증상 가장 정확(scripts/backtest_forecast.py).
대외비·관리자용.
"""
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))

GS = ('OTA', 'G-OTA', 'Inbound')
OUT = ROOT / 'docs' / 'data' / 'adr_guide.json'

# raw 사업장명 → display (generate_otb_data PROPERTY_DEFS와 동일)
PROP_DEFS = [
    ("01.벨비발디", ["소노벨 비발디파크", "소노문 비발디파크"]), ("02.캄비발디", ["소노캄 비발디파크"]),
    ("03.펫비발디", ["소노펫 비발디파크"]), ("04.펠리체비발디", ["소노펠리체 비발디파크"]),
    ("05.빌리지비발디", ["소노펠리체 빌리지 비발디파크"]), ("06.양평", ["소노휴 양평", "소노벨 양평"]),
    ("07.델피노", ["델피노"]), ("08.쏠비치양양", ["쏠비치 양양"]), ("09.쏠비치삼척", ["쏠비치 삼척"]),
    ("10.소노벨단양", ["소노문 단양", "소노벨 단양"]), ("11.소노캄경주", ["소노벨 경주", "소노캄 경주"]),
    ("12.소노벨청송", ["소노벨 청송"]), ("13.소노벨천안", ["소노벨 천안"]), ("14.소노벨변산", ["소노벨 변산"]),
    ("15.소노캄여수", ["소노캄 여수"]), ("16.소노캄거제", ["소노캄 거제"]), ("17.쏠비치진도", ["쏠비치 진도"]),
    ("18.소노벨제주", ["소노벨 제주"]), ("19.소노캄제주", ["소노캄 제주"]), ("20.소노캄고양", ["소노캄 고양"]),
    ("21.소노문해운대", ["소노문 해운대"]), ("22.쏠비치남해", ["쏠비치 남해"]), ("23.르네블루", ["르네블루"]),
]
_N2D = {re.sub(r'\s+', '', n): d for d, ns in PROP_DEFS for n in ns}


def prop_disp(raw_name):
    n = (raw_name or '').split('. ', 1)[-1]
    n = re.sub(r'\s*(바이|by)\s.*$', '', n.strip(), flags=re.I)
    return _N2D.get(re.sub(r'\s+', '', n))


def day_type(season_name):
    return 'w' if '주중' in (season_name or '') else 'e'


def cal_type(d):
    return 'e' if d.weekday() in (4, 5) else 'w'   # 금·토 밤 = 주말


def _ymd(s):
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


# ------------------------------------------------------------------ raw 파싱
def _open(fp):
    for enc in ('cp949', 'euc-kr', 'utf-8'):
        try:
            f = open(fp, encoding=enc)
            header = f.readline().strip().split(';')
            return f, {h.strip(): i for i, h in enumerate(header)}
        except UnicodeDecodeError:
            continue
    return None, {}


def _windows(folder):
    """parse_raw_db 규칙: 재전송(≤ 스냅샷최소월−1) + 최신 스냅샷(≥ 그 달). 파일→(min_ym, max_ym)."""
    import parse_raw_db as P
    is_re = lambda fp: bool(re.search(r'\(\d{8}-\d{8}\)', fp.name))
    by_type = defaultdict(list)
    for fp in Path(folder).iterdir():
        if fp.name.lower().endswith('.txt') and fp.name[:2] in ('27', '28', '43', '44'):
            by_type[fp.name[:2]].append(fp)
    win = {}
    for ft, fps in by_type.items():
        re_ = [fp for fp in fps if is_re(fp)]
        snaps = [fp for fp in fps if not is_re(fp)]
        if len(snaps) > 1:
            sd = lambda fp: (re.search(r'_(\d{8})_생성시간', fp.name) or [None, '0'])[1]
            snaps = [sorted(snaps, key=sd, reverse=True)[0]]
        if re_ and snaps:
            smin = P._snapshot_min_stay_month(snaps, ft)
            rmax = P._prev_month(smin) if smin else None
            for fp in re_:
                win[fp] = (None, rmax)
            for fp in snaps:
                win[fp] = (smin, None)
        else:
            for fp in fps:
                win[fp] = (None, None)
    return win


def parse_raw(folders, ty_months, ly_months, ty_asof, ly_cut):
    """→ cur/lys/lyf: {ym: {disp: {seg: {'w'|'e': [rn, rev]}}}}, votes: {(disp, yyyymmdd): Counter(type)}"""
    import parse_raw_db as P
    mk = lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: [0, 0]))))
    cur, lys, lyf = mk(), mk(), mk()
    votes = defaultdict(Counter)
    want = set(ty_months) | set(ly_months)
    ty_s, ly_s = ty_asof.strftime('%Y%m%d'), ly_cut.strftime('%Y%m%d')
    # 투숙월마다 정본 폴더 1개: 투숙연도 폴더, 없으면 그 이전 가장 최근 연도 폴더(예: 2027년 1월 → 2026 폴더 스냅샷).
    # 이전 연도 스냅샷에도 다음 해 투숙분이 들어 있어 폴더를 섞으면 중복 집계됨.
    years = {int(Path(f).name): f for f in folders if Path(f).exists() and Path(f).name.isdigit()}
    src = {}
    for ym in want:
        cand = [y for y in years if y <= int(ym[:4])]
        if cand:
            src[ym] = years[max(cand)]
    for folder in folders:
        if not Path(folder).exists():
            continue
        own = {ym for ym, f in src.items() if f == folder}
        if not own:
            continue
        win = _windows(folder)
        seen_c = set()
        for fp, (mn, mx) in sorted(win.items(), key=lambda kv: kv[0].name):
            ft = fp.name[:2]
            is_bk = ft in ('27', '43')
            f, cm = _open(fp)
            if not f:
                continue
            g = lambda k: cm.get(k, -1)
            i_sell, i_ci, i_rooms, i_night = g('판매일자'), g('입실일자'), g('객실수'), g('1박객실료')
            i_mem, i_user, i_first, i_cancel = g('회원명'), g('이용자명'), g('최초입력일자'), g('취소일자')
            i_code = cm.get('변경예약집계코드', cm.get('예약집계코드', -1))
            i_cname = cm.get('변경예약집계코드명', cm.get('예약집계명', -1))
            i_agent, i_season = g('AGENT명'), g('요금시즌명')
            i_prop = cm.get('변경사업장명', cm.get('영업장명', -1))
            hi = max(i_sell, i_ci, i_rooms, i_night, i_mem, i_user, i_first, i_code, i_cname, i_agent, i_prop, i_season, i_cancel)
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
                sell = sell[:8]
                ym = sell[:6]
                if ym not in own:
                    continue
                if is_bk and ((mn and ym < mn) or (mx and ym > mx)):
                    continue
                code = p[i_code].strip() if i_code >= 0 else ''
                mname = p[i_mem].strip() if i_mem >= 0 else ''
                uname = p[i_user].strip() if i_user >= 0 else ''
                disp = prop_disp(p[i_prop].strip()) if i_prop >= 0 else None
                ty = day_type(p[i_season].strip()) if i_season >= 0 else None
                if is_bk and disp and ty:
                    votes[(disp, sell)][ty] += 1          # 날짜 유형 투표(세그 무관, 올해·작년 모두 — 작년 연휴일 분류용)
                if mname and uname and mname == uname and code != '58':
                    continue
                if '매출조정' in mname or '매출조정' in uname:
                    continue
                seg = P.classify_segment(code, p[i_cname].strip() if i_cname >= 0 else '',
                                         p[i_agent].strip() if i_agent >= 0 else '', ft)
                if seg not in GS or not disp:
                    continue
                if ty is None:
                    ty = cal_type(_ymd(sell))
                rooms = p[i_rooms].strip() if i_rooms >= 0 else ''
                rn = int(rooms) if rooms.isdigit() and int(rooms) > 0 else 1
                night = p[i_night].strip() if i_night >= 0 else ''
                rev = int(int(night) * rn / 1.1) if night.lstrip('-').isdigit() else 0
                first = p[i_first].strip()[:8] if i_first >= 0 else ''
                if is_bk:
                    if ym in ty_months:
                        c = cur[ym][disp][seg][ty]; c[0] += rn; c[1] += rev
                    if ym in ly_months:
                        c = lyf[ym][disp][seg][ty]; c[0] += rn; c[1] += rev
                        if first and first <= ly_s:
                            c = lys[ym][disp][seg][ty]; c[0] += rn; c[1] += rev
                else:   # 취소 → 전년동기에만 복원
                    if ym not in ly_months:
                        continue
                    canc = p[i_cancel].strip()[:8] if i_cancel >= 0 else ''
                    if len(first) == 8 and len(canc) == 8 and first <= ly_s < canc:
                        c = lys[ym][disp][seg][ty]; c[0] += rn; c[1] += rev
            f.close()
    return cur, lys, lyf, votes


# ------------------------------------------------------------------ PDF 재고
def load_inventory(hist_path):
    """data/daily_occ_history.json 최신 보고일 → (ty_asof, {YYYYMM: {display: [(일, rn, occ, cap)]}})
    (parse_daily_booking.py가 PDF 파싱 때마다 보고일 단위로 누적 저장 — 날짜별 온북 RN·OCC·일 보유객실, 전 세그)"""
    hist = json.loads(Path(hist_path).read_text(encoding='utf-8'))
    if not hist:
        return None, {}
    rd = max(hist.keys())
    inv = {}
    for ym, props in hist[rd].items():
        key = ym.replace('-', '')
        for name, v in props.items():
            disp = prop_disp(name)
            rns, occs, cap = v.get('rn') or [], v.get('occ') or [], v.get('cap') or 0
            if not disp or not rns:
                continue
            rows = inv.setdefault(key, {}).setdefault(disp, [])
            for i, rn in enumerate(rns):
                rows.append((i + 1, rn, occs[i] if i < len(occs) else 0, cap))
    return datetime.strptime(rd, '%Y-%m-%d').date(), inv


def _cap(c):
    """BS 셀 {rn, occ} → 그 달 판매가능 객실(OCC 역산). OCC<0.5%면 계산 불가(None)."""
    return (c['rn'] * 100.0 / c['occ']) if (c and c.get('occ') and c['occ'] >= 0.5 and c.get('rn')) else None


def load_whole(bs_path, ty_months):
    """Booking Status Report(사업장 페이지, 전 세그) → (whole, report_date, ly_date)
    whole[YYYYMM][display] = {'cur':[rn, rev_천원, 객실], 'lys':[…], 'lyf':[…]}
      cur=올해 온북 · lys=작년 같은 날(보고일−364) 온북 · lyf=작년 마감. 객실 = 그 행 RN÷OCC(판매가능 객실).
    parse_booking_status.py 산출물(docs/data/booking_status_detail.json)을 읽는다."""
    p = Path(bs_path)
    if not p.exists():
        return {}, None, None
    bs = json.loads(p.read_text(encoding='utf-8'))
    out = {}
    for v in (bs.get('properties') or {}).values():
        disp = prop_disp(v.get('name'))
        if not disp:
            continue
        for ym, mv in (v.get('months') or {}).items():
            key = ym.replace('-', '')
            if key not in ty_months:
                continue
            o, ly, la = mv.get('otb'), mv.get('ly'), mv.get('ly_act')
            if not (o and ly and la):
                continue
            cap_ty = _cap(o) or _cap(mv.get('budget'))
            cap_ly = _cap(la) or _cap(ly)
            node = out.setdefault(key, {}).setdefault(disp, {'cur': [0, 0, 0.0], 'lys': [0, 0, 0.0], 'lyf': [0, 0, 0.0]})
            for k, c, cp in (('cur', o, _cap(o) or cap_ty), ('lys', ly, _cap(ly) or cap_ly), ('lyf', la, _cap(la) or cap_ly)):
                node[k][0] += c['rn']; node[k][1] += c['rev_k']; node[k][2] += round(cp or 0, 1)
    return out, bs.get('report_date'), bs.get('ly_date')


def avail_unsold(rn, occ, cap):
    """그날 판매가능 객실 = OCC 역산(판매중지 반영), OCC<1%면 보유객실. 빈 객실 = 가능 − 온북."""
    avail = (rn * 100.0 / occ) if (occ and occ >= 1 and rn) else float(cap or 0)
    return avail, max(0.0, avail - rn)


def date_type(votes, disp, ymd, all_votes):
    v = votes.get((disp, ymd))
    if v:
        return v.most_common(1)[0][0]
    v = all_votes.get(ymd)
    if v:
        return v.most_common(1)[0][0]
    return cal_type(_ymd(ymd))


# ------------------------------------------------------------------ 가이드 계산(페이지 JS와 동일)
def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


MIN_N = 100          # 요금 수준 비율을 사업장 단위로 쓰기 위한 최소 객실수 — 미만이면 선택 전체 값
PRICE_RANGE = (0.85, 1.2)
# 남은 판매 물량 = 작년 같은 시점 이후 판매량 그대로(올해 속도 배수 없음).
#   과거 검증(scripts/backtest_forecast.py, 2024~2026 10,288건): 속도 배수 방식 오차 14.8~21.9% vs 그대로 더하기 8.0~10.6%
# 가격 반응(ADR 1% 변화 → 남은 판매 객실 −ε%): 과거 추정, 사업장 시뮬에서만 사용(페이지)
ELASTICITY = {'OTA': -0.8, 'G-OTA': -0.2, 'Inbound': 0.0}


def _ratio_adr(a, b):
    return (a[1] / a[0]) / (b[1] / b[0]) if (a[0] and b[0] and b[1]) else None


def guide_calc(month_node, props, segs):
    """선택 사업장·세그 → 가이드. month_node = adr_guide.json months[YYYYMM]. (booking-trend.html adrGuideCalc와 동일)"""
    T = ('w', 'e')
    out = {'cur': [0, 0], 'lys': [0, 0], 'lyf': [0, 0], 'exp_rn': 0.0, 'exp_rev': 0.0, 'props': 0, 'unsold': 0.0,
           'whole': None, 'eps': None,
           't': {t: {'days': 0, 'avail': 0.0, 'onbook': 0, 'unsold': 0.0, 'pick_rn': 0, 'pick_rev': 0, 'ly_days': 0,
                     'exp_rn': 0.0, 'exp_rev': 0.0, 'capped': 0} for t in T}}
    # 1패스: 사업장별 집계 + 선택 전체 비율(작은 사업장의 대체값) + 사업장 전체(전 세그) 합
    rows = []
    sel = {k: {t: [0, 0] for t in T} for k in ('cur', 'lys', 'lyf')}
    whole = {k: [0, 0, 0.0] for k in ('cur', 'lys', 'lyf')}
    whole_n = 0
    pick_by_seg = defaultdict(float)
    for disp in props:
        pn = (month_node.get('props') or {}).get(disp)
        if not pn:
            continue
        agg = {k: {t: [0, 0] for t in T} for k in ('cur', 'lys', 'lyf')}
        for s in segs:
            sn = (pn.get('seg') or {}).get(s) or {}
            for k in ('cur', 'lys', 'lyf'):
                for t in T:
                    v = (sn.get(k) or {}).get(t) or [0, 0]
                    agg[k][t][0] += v[0]; agg[k][t][1] += v[1]
                    sel[k][t][0] += v[0]; sel[k][t][1] += v[1]
            pick_by_seg[s] += sum(((sn.get('lyf') or {}).get(t) or [0, 0])[0] - ((sn.get('lys') or {}).get(t) or [0, 0])[0]
                                  for t in T)
        w = pn.get('whole')
        if w:
            whole_n += 1
            for k in whole:
                for i in range(3):
                    whole[k][i] += (w.get(k) or [0, 0, 0])[i]
        rows.append((pn, agg))
    stot = {k: [sum(sel[k][t][0] for t in T), sum(sel[k][t][1] for t in T)] for k in sel}
    for k in stot:
        out[k] = list(stot[k])
    if whole_n:
        out['whole'] = whole
    # 가격 반응: 선택 세그의 작년 남은 판매량 가중 평균
    pw = {s: max(0.0, v) for s, v in pick_by_seg.items()}
    out['eps'] = (sum(pw[s] * ELASTICITY.get(s, 0.0) for s in pw) / sum(pw.values())) if sum(pw.values()) > 0 else None
    sel_f = _clamp(_ratio_adr(stot['cur'], stot['lys']) or 1.0, *PRICE_RANGE)
    sel_ft = {t: (_clamp(_ratio_adr(sel['cur'][t], sel['lys'][t]), *PRICE_RANGE)
                  if (sel['cur'][t][0] >= MIN_N and sel['lys'][t][0] >= MIN_N) else sel_f) for t in T}
    # 2패스: 사업장별 수요(작년 남은 판매 일평균, 속도 배수 없음)를 날짜별 빈 객실 한도로 배분
    for pn, agg in rows:
        tot = {k: [sum(agg[k][t][0] for t in T), sum(agg[k][t][1] for t in T)] for k in agg}
        if tot['lys'][0] <= 0 or tot['lyf'][0] <= 0:
            continue
        out['props'] += 1
        per = {}
        for t in T:
            pr = agg['lyf'][t][0] - agg['lys'][t][0]
            pv = agg['lyf'][t][1] - agg['lys'][t][1]
            lyf_adr_t = agg['lyf'][t][1] / agg['lyf'][t][0] if agg['lyf'][t][0] else None
            if pr >= 30:
                a = pv / pr
                if lyf_adr_t and not (0.33 * lyf_adr_t <= a <= 3 * lyf_adr_t):
                    a = lyf_adr_t
            else:                      # 작년 이후 판매가 미미 → 그 유형 작년 최종 ADR
                pr, a = max(0, pr), lyf_adr_t
            f = (_clamp(_ratio_adr(agg['cur'][t], agg['lys'][t]), *PRICE_RANGE)
                 if (agg['cur'][t][0] >= MIN_N and agg['lys'][t][0] >= MIN_N) else sel_ft[t])
            ld = max(1, (pn.get('ly_days') or {}).get(t, 0))
            per[t] = {'pd': pr / ld, 'adr': (a or 0) * f}
            o = out['t'][t]
            o['pick_rn'] += pr; o['pick_rev'] += pr * (a or 0); o['ly_days'] += (pn.get('ly_days') or {}).get(t, 0)
        dates = pn.get('dates')
        if dates:
            for day, t, unsold, occ, onbook in dates:
                dem = per[t]['pd']
                ex = min(dem, unsold)
                o = out['t'][t]
                o['days'] += 1; o['onbook'] += onbook; o['unsold'] += unsold
                o['avail'] += onbook + unsold
                o['exp_rn'] += ex; o['exp_rev'] += ex * per[t]['adr']
                out['unsold'] += unsold
                if dem > unsold + 1e-9:
                    o['capped'] += 1
        else:   # 재고 정보 없음 → 작년 남은 날짜 수 그대로, 제한 없음
            out['inv_missing'] = out.get('inv_missing', 0) + 1
            for t in T:
                ex = per[t]['pd'] * max(0, (pn.get('ly_days') or {}).get(t, 0))
                out['t'][t]['exp_rn'] += ex; out['t'][t]['exp_rev'] += ex * per[t]['adr']
    for t in T:
        out['exp_rn'] += out['t'][t]['exp_rn']; out['exp_rev'] += out['t'][t]['exp_rev']
    cur_rn, cur_rev = out['cur']
    out['cur_adr'] = cur_rev / cur_rn if cur_rn else None
    out['lys_adr'] = out['lys'][1] / out['lys'][0] if out['lys'][0] else None
    out['lyf_adr'] = out['lyf'][1] / out['lyf'][0] if out['lyf'][0] else None
    out['ly_drift'] = (out['lyf_adr'] / out['lys_adr'] - 1) if (out['lys_adr'] and out['lyf_adr']) else None
    pk_rn = out['lyf'][0] - out['lys'][0]
    out['ly_pick_adr'] = ((out['lyf'][1] - out['lys'][1]) / pk_rn) if pk_rn > 0 else None
    out['pick_adr'] = out['exp_rev'] / out['exp_rn'] if out['exp_rn'] > 0 else None
    out['close_adr'] = ((cur_rev + out['exp_rev']) / (cur_rn + out['exp_rn'])) if (cur_rn + out['exp_rn']) > 0 else None
    out['close_chg'] = (out['close_adr'] / out['cur_adr'] - 1) if (out['close_adr'] and out['cur_adr']) else None
    out['pick_vs_cur'] = (out['pick_adr'] / out['cur_adr'] - 1) if (out['pick_adr'] and out['cur_adr']) else None
    w = out['whole']
    if w:   # 사업장 전체(전 세그) OCC: 작년 같은 날·작년 마감·올해 지금 + 같은 방식 예상 마감(올해 + 작년 같은 날 이후 판매)
        occ = lambda k: (w[k][0] / w[k][2]) if w[k][2] else None
        w_exp = w['cur'][0] + (w['lyf'][0] - w['lys'][0])
        out['whole_occ'] = {'lys': occ('lys'), 'lyf': occ('lyf'), 'cur': occ('cur'),
                            'exp': min(1.0, w_exp / w['cur'][2]) if w['cur'][2] else None,
                            'adr_lys': w['lys'][1] * 1000 / w['lys'][0] if w['lys'][0] else None,
                            'adr_lyf': w['lyf'][1] * 1000 / w['lyf'][0] if w['lyf'][0] else None,
                            'adr_cur': w['cur'][1] * 1000 / w['cur'][0] if w['cur'][0] else None}
    return out


def sim_pickup(base_rn, base_adr, adr, eps, cap=None):
    """가격 반응: 기준(가이드 ADR에서의 예상 추가 판매) → 설정 ADR에서의 예상 추가 판매. 빈 객실 한도."""
    if not base_rn or not base_adr or not adr or base_rn <= 0:
        return max(0.0, base_rn or 0.0)
    rn = base_rn * (adr / base_adr) ** (eps or 0.0)
    return max(0.0, min(rn, cap) if cap is not None else rn)


# ------------------------------------------------------------------ 메인
def build(daily_path, folders, bs_path=None):
    ty_asof, inv = load_inventory(daily_path)
    if not ty_asof:
        raise SystemExit('daily_booking.json 보고일 없음')
    ly_cut = ty_asof - timedelta(days=364)
    ty_months = sorted(inv.keys())
    whole, bs_date, bs_ly = load_whole(bs_path or (ROOT / 'docs' / 'data' / 'booking_status_detail.json'), set(ty_months))
    # 사업장 전체 값은 같은 보고일(±2일) 것만 사용 — 작년 기준일이 어긋나면 비교가 틀어짐
    if not bs_date or abs((datetime.strptime(bs_date, '%Y-%m-%d').date() - ty_asof).days) > 2:
        whole = {}
    ly_of = {ym: f"{int(ym[:4]) - 1}{ym[4:]}" for ym in ty_months}
    cur, lys, lyf, votes = parse_raw(folders, ty_months, list(ly_of.values()), ty_asof, ly_cut)
    all_votes = defaultdict(Counter)
    for (disp, ymd), c in votes.items():
        all_votes[ymd].update(c)
    months = {}
    for ym in ty_months:
        lym = ly_of[ym]
        y, m = int(ym[:4]), int(ym[4:])
        ly_y = y - 1
        props = {}
        disps = set(cur.get(ym, {})) | set(lyf.get(lym, {})) | set(inv.get(ym, {}))
        for disp in sorted(disps):
            seg = {}
            for s in GS:
                node = {}
                for k, src, key in (('cur', cur, ym), ('lys', lys, lym), ('lyf', lyf, lym)):
                    v = src.get(key, {}).get(disp, {}).get(s, {})
                    node[k] = {t: [int(v[t][0]), int(v[t][1])] for t in ('w', 'e') if t in v}
                if any(node[k] for k in node):
                    seg[s] = node
            # 작년 기준일 이후 남은 날짜(유형별)
            ly_days = Counter()
            d0 = date(ly_y, m, 1)
            while d0.month == m:
                if d0 > ly_cut:
                    ly_days[date_type(votes, disp, d0.strftime('%Y%m%d'), all_votes)] += 1
                d0 += timedelta(days=1)
            # 올해 기준일 다음날부터 남은 날짜(PDF)
            dates = None
            if disp in inv.get(ym, {}):
                dates = []
                for day, rn, occ, cap in inv[ym][disp]:
                    dd = date(y, m, day)
                    if dd <= ty_asof:
                        continue
                    _, uns = avail_unsold(rn, occ, cap)
                    dates.append([day, date_type(votes, disp, dd.strftime('%Y%m%d'), all_votes), round(uns), occ, rn])
            props[disp] = {'seg': seg, 'ly_days': dict(ly_days), 'dates': dates,
                           'whole': (whole.get(ym) or {}).get(disp)}
        months[ym] = {'ly_ym': lym, 'props': props}
    meta = {'built': datetime.now().strftime('%Y-%m-%d %H:%M'), 'ty_asof': ty_asof.isoformat(), 'ly_asof': ly_cut.isoformat(),
            'whole_asof': bs_date if whole else None, 'whole_ly_asof': bs_ly if whole else None,
            'months': ty_months, 'segs': list(GS), 'elasticity': ELASTICITY,
            'rule': '작년 같은 시점(−364일) 이후 GS 순증 판매량(유형별 일평균, 속도 배수 없음)·ADR × 올해 요금 수준, 날짜별 빈 객실(전 세그) 한도',
            'types': {'w': '주중', 'e': '주말·연휴'}, 'label': '대외비·관리자용 · 가이드는 추정'}
    return {'meta': meta, 'months': months}


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--daily', default=str(ROOT / 'data' / 'daily_occ_history.json'))
    ap.add_argument('--raw', nargs='*', default=[str(ROOT / 'data' / 'raw_db' / '2025'), str(ROOT / 'data' / 'raw_db' / '2026'),
                                                 str(ROOT / 'data' / 'raw_db' / '2027')])
    ap.add_argument('--out', default=str(OUT))
    a = ap.parse_args()
    doc = build(a.daily, a.raw)
    Path(a.out).write_text(json.dumps(doc, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
    print(f"✓ {a.out} — 기준일 {doc['meta']['ty_asof']} (작년 {doc['meta']['ly_asof']}) · 월 {doc['meta']['months']}")
    allp = None
    for ym, node in doc['months'].items():
        allp = list(node['props'].keys())
        g = guide_calc(node, allp, list(GS))
        fmt = lambda x: f"{x/1000:,.0f}천" if x else '—'
        pct = lambda x: f"{x*100:+.1f}%" if x is not None else '—'
        print(f"  {ym}: 온북 {g['cur'][0]:,}실 ADR {fmt(g['cur_adr'])} | 작년 동기 {fmt(g['lys_adr'])}→마감 {fmt(g['lyf_adr'])} ({pct(g['ly_drift'])}) "
              f"| 남은판매 예상 {g['exp_rn']:,.0f}실 ADR {fmt(g['pick_adr'])} ({pct(g['pick_vs_cur'])}) | 예상마감 {fmt(g['close_adr'])} ({pct(g['close_chg'])}) "
              f"| 주말 제한일 {g['t']['e']['capped']}")


if __name__ == '__main__':
    main()
