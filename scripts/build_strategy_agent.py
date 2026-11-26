#!/usr/bin/env python3.12
# 예약추이 전략 섹션 데이터(docs/data/strategy_agent.json) 빌더 — 2026-11-26 scratchpad에서 정본 승격, build.py가 매일 호출.
# 검증: 거래처 합 = 세그 온북(otb_data) 일치, GS실시간실적현황 '전년 OTB' 정합(메모리 booking-trend-strategy-sameperiod).
import os as _os
from pathlib import Path as _Path
_os.chdir(_Path(__file__).resolve().parent.parent)   # 상대경로(scripts/, docs/data/) 기준 = 레포 루트
"""전략분석 거래처(AGENT) 데이터 v2 — 투숙월×세그×거래처: 온북(2026) + 전년마감(2025) + 전년동기(2025, 예약일 cutoff).
   정본 로직: REV=1박객실료×객실수÷1.1, booking=net, 거래처/매출조정 제외.
   전년동기 cutoff = otb 기준일 − 1년(최초입력일자 ≤ cutoff). 세그 Inbound도 tot 포함(거래처 분해는 OTA/GOTA만).
   출력: docs/data/strategy_agent.json
     {ym: {seg: {"lyf":[rn,rev], "lys":[rn,rev], "agents": {name: {"cur":[rn,rev],"lyf":[rn,rev],"lys":[rn,rev]}}}}}"""
import os, sys, re, json
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, 'scripts')
import parse_raw_db as P

# 기준일(동기 cutoff): GS실시간실적현황 '전년 OTB'와 동일 방식 = 데이터 스냅샷일 − 1년 (−1일 보정 없음).
# 예) baseDate 2026-11-20 → 전년동기 cutoff 2025-11-20. 데이터가 당일로 갱신되면 실시간과 일치(검증: cutoff 11-24에서 OTA 4,062 ≈ 실시간 4,052).
from datetime import datetime, timedelta
meta = json.load(open('docs/data/otb_data.json'))['meta']
_asof = datetime.strptime(meta.get('baseDate', '2026-11-20'), '%Y-%m-%d')  # 스냅샷일 = 기준일(−1일 제거)
try: CUTOFF = _asof.replace(year=_asof.year - 1).strftime('%Y%m%d')
except ValueError: CUTOFF = (_asof - timedelta(days=1)).replace(year=_asof.year - 1).strftime('%Y%m%d')  # 2/29 방어
print('전년 동기 cutoff(최초입력일자 ≤, 스냅샷일−1년):', CUTOFF)

# raw 사업장명(변경사업장명/영업장명, "NN. " 프리픽스 제거) → otb byProperty display 이름 (generate_otb_data PROPERTY_DEFS 동일)
_PROP_DEFS = [
 ("01.벨비발디",["소노벨 비발디파크","소노문 비발디파크"]), ("02.캄비발디",["소노캄 비발디파크"]),
 ("03.펫비발디",["소노펫 비발디파크"]), ("04.펠리체비발디",["소노펠리체 비발디파크"]),
 ("05.빌리지비발디",["소노펠리체 빌리지 비발디파크"]), ("06.양평",["소노휴 양평","소노벨 양평"]),
 ("07.델피노",["델피노"]), ("08.쏠비치양양",["쏠비치 양양"]), ("09.쏠비치삼척",["쏠비치 삼척"]),
 ("10.소노벨단양",["소노문 단양","소노벨 단양"]), ("11.소노캄경주",["소노벨 경주","소노캄 경주"]),
 ("12.소노벨청송",["소노벨 청송"]), ("13.소노벨천안",["소노벨 천안"]), ("14.소노벨변산",["소노벨 변산"]),
 ("15.소노캄여수",["소노캄 여수"]), ("16.소노캄거제",["소노캄 거제"]), ("17.쏠비치진도",["쏠비치 진도"]),
 ("18.소노벨제주",["소노벨 제주"]), ("19.소노캄제주",["소노캄 제주"]), ("20.소노캄고양",["소노캄 고양"]),
 ("21.소노문해운대",["소노문 해운대"]), ("22.쏠비치남해",["쏠비치 남해"]), ("23.르네블루",["르네블루"]),
]
_P2D = {}
for _disp, _dbs in _PROP_DEFS:
    for _n in _dbs: _P2D[_n] = _disp
def prop_disp(raw):
    n = raw.split('. ', 1)[-1].strip() if raw else ''
    return _P2D.get(n)

def ftype(n):
    for p in ('27', '28', '43', '44'):
        if n.startswith(p): return p
P._is_retrans = lambda fp: bool(re.search(r'\(\d{8}-\d{8}\)', fp.name))

def parse_year(folder, want_booking_date):
    """folder의 booking(27/43) 파싱 → {(ym,seg,agent): [rn,rev]} 전체, 그리고 동기(cutoff 이하)만.
       반환 (all_map, same_map). want_booking_date=False면 same_map=all_map(현재연도)."""
    RAW = Path(folder)
    if not RAW.exists(): return {}, {}
    files = [fp for fp in RAW.iterdir() if fp.name.lower().endswith('.txt')]
    by_type = defaultdict(list); windows = {}
    for fp in files:
        ft = ftype(fp.name)
        if ft: by_type[ft].append(fp)
    for ft, fps in by_type.items():
        retrans = [fp for fp in fps if P._is_retrans(fp)]; snaps = [fp for fp in fps if not P._is_retrans(fp)]
        if len(snaps) > 1:
            def sd(fp): m = re.search(r'_(\d{8})_생성시간', fp.name); return m.group(1) if m else '0'
            snaps.sort(key=sd, reverse=True); snaps = [snaps[0]]
        if retrans and snaps:
            snap_min = P._snapshot_min_stay_month(snaps, ft); rmax = P._prev_month(snap_min) if snap_min else None
            for fp in retrans: windows[fp] = (None, rmax)
            for fp in snaps: windows[fp] = (snap_min, None)
        else:
            for fp in fps: windows[fp] = (None, None)
    allm = defaultdict(lambda: [0, 0]); samem = defaultdict(lambda: [0, 0])
    allm_p = defaultdict(lambda: [0, 0]); samem_p = defaultdict(lambda: [0, 0])  # 사업장×세그 (ym,seg,display)
    for fp, (minm, maxm) in windows.items():
        ft = ftype(fp.name)
        if ft not in ('27', '43'): continue
        for enc in ('cp949', 'euc-kr', 'utf-8'):
            try: f = open(fp, encoding=enc); header = f.readline().strip().split(';'); break
            except UnicodeDecodeError: continue
        cm = {h.strip(): i for i, h in enumerate(header)}
        i_sell = cm.get('판매일자', -1); i_ci = cm.get('입실일자', -1)
        i_rooms = cm.get('객실수', -1); i_night = cm.get('1박객실료', -1)
        i_mem = cm.get('회원명', -1); i_user = cm.get('이용자명', -1)
        i_first = cm.get('최초입력일자', -1)
        i_codenum = cm.get('변경예약집계코드' if '변경예약집계코드' in cm else '예약집계코드', -1)
        i_codename = cm.get('변경예약집계코드명' if '변경예약집계코드명' in cm else '예약집계명', -1)
        i_agent = cm.get('AGENT명', -1)
        i_prop = cm.get('변경사업장명', cm.get('영업장명', -1))
        seen = set(); hi = max(i_sell, i_ci, i_rooms, i_night, i_mem, i_user, i_first, i_codenum, i_codename, i_agent, i_prop)
        for line in f:
            h = hash(line.rstrip('\n\r'))
            if h in seen: continue
            seen.add(h)
            p = line.split(';')
            if len(p) <= hi: continue
            sell = p[i_sell].strip() if i_sell >= 0 else ''
            if len(sell) < 6:
                sell = p[i_ci].strip() if i_ci >= 0 else ''
                if len(sell) < 6: continue
            ym = sell[:6]
            if minm and ym < minm: continue
            if maxm and ym > maxm: continue
            codenum = p[i_codenum].strip() if i_codenum >= 0 else ''
            if i_mem >= 0 and i_user >= 0:
                mn = p[i_mem].strip(); un = p[i_user].strip()
                if mn and un and mn == un and codenum != '58': continue
                if '매출조정' in p[i_mem] or '매출조정' in p[i_user]: continue
            codename = p[i_codename].strip() if i_codename >= 0 else ''
            agent = (p[i_agent].strip() if i_agent >= 0 else '') or '(미상)'
            seg = P.classify_segment(codenum, codename, agent, ft)
            if seg not in ('OTA', 'G-OTA', 'Inbound'): continue
            rooms = int(p[i_rooms].strip() or 0) if i_rooms >= 0 and p[i_rooms].strip() else 0
            night = int(p[i_night].strip() or 0) if i_night >= 0 and p[i_night].strip() else 0
            rn = rooms if rooms > 0 else 1; rev = int(night * rn / 1.1)
            agent = re.sub(r'^(OTA|GOTA)_', '', agent)
            k = (ym, seg, agent)
            allm[k][0] += rn; allm[k][1] += rev
            disp = prop_disp(p[i_prop].strip()) if i_prop >= 0 else None
            if disp: pk = (ym, seg, disp); allm_p[pk][0] += rn; allm_p[pk][1] += rev
            if want_booking_date:
                first = p[i_first].strip()[:8] if i_first >= 0 else ''
                if first and first <= CUTOFF:
                    samem[k][0] += rn; samem[k][1] += rev
                    if disp: samem_p[pk][0] += rn; samem_p[pk][1] += rev
        f.close()
    # 취소복원(28/44): 동기(samem)에만 가산 — pickup≤CUTOFF AND cancel>CUTOFF (그 시점엔 살아있던 예약)
    # ※ 윈도우(minm/maxm) 적용 안 함: 28 스냅샷/재전송은 투숙월 범위가 27과 달라, 윈도우로 자르면
    #   마감월(예:202601) 취소가 통째로 누락됨. 대신 전 28파일 통합 line-dedup으로 중복만 제거.
    if want_booking_date:
        seen_c = set()
        for fp, (minm, maxm) in windows.items():
            ft = ftype(fp.name)
            if ft not in ('28', '44'): continue
            for enc in ('cp949', 'euc-kr', 'utf-8'):
                try: f = open(fp, encoding=enc); header = f.readline().strip().split(';'); break
                except UnicodeDecodeError: continue
            cm = {h.strip(): i for i, h in enumerate(header)}
            i_sell = cm.get('판매일자', -1); i_ci = cm.get('입실일자', -1)
            i_rooms = cm.get('객실수', -1); i_night = cm.get('1박객실료', -1)
            i_mem = cm.get('회원명', -1); i_user = cm.get('이용자명', -1)
            i_first = cm.get('최초입력일자', -1); i_cancel = cm.get('취소일자', -1)
            i_codenum = cm.get('변경예약집계코드' if '변경예약집계코드' in cm else '예약집계코드', -1)
            i_codename = cm.get('변경예약집계코드명' if '변경예약집계코드명' in cm else '예약집계명', -1)
            i_agent = cm.get('AGENT명', -1)
            i_prop = cm.get('변경사업장명', cm.get('영업장명', -1))
            if i_first < 0 or i_cancel < 0: f.close(); continue
            hi = max(i_sell, i_ci, i_rooms, i_night, i_mem, i_user, i_first, i_cancel, i_codenum, i_codename, i_agent, i_prop)
            for line in f:
                h = hash(line.rstrip('\n\r'))
                if h in seen_c: continue       # 전 28파일 통합 중복제거
                seen_c.add(h)
                p = line.split(';')
                if len(p) <= hi: continue
                first = p[i_first].strip()[:8]; canc = p[i_cancel].strip()[:8]
                if len(first) < 8 or len(canc) < 8: continue
                if first > CUTOFF: continue      # 그 시점 이후 예약 → 애초에 없던 건
                if canc <= CUTOFF: continue       # 그 시점 전에 이미 취소됨 → 복원 안 함
                sell = p[i_sell].strip() if i_sell >= 0 else ''
                if len(sell) < 6:
                    sell = p[i_ci].strip() if i_ci >= 0 else ''
                    if len(sell) < 6: continue
                ym = sell[:6]
                codenum = p[i_codenum].strip() if i_codenum >= 0 else ''
                # ※ 거래처제거(회원명==이용자명)를 취소복원에도 적용 — GS실시간 '전년 OTB'와 정합.
                #   미적용 시 회원명=이용자명 취소건까지 복원되어 OTA 동기가 +6.7% 과다(4,325 vs 실시간 4,052).
                mn_c = p[i_mem].strip() if i_mem >= 0 else ''; un_c = p[i_user].strip() if i_user >= 0 else ''
                if mn_c and un_c and mn_c == un_c and codenum != '58': continue
                if '매출조정' in mn_c or '매출조정' in un_c: continue
                codename = p[i_codename].strip() if i_codename >= 0 else ''
                agent = (p[i_agent].strip() if i_agent >= 0 else '') or '(미상)'
                seg = P.classify_segment(codenum, codename, agent, ft)
                if seg not in ('OTA', 'G-OTA', 'Inbound'): continue
                rooms = int(p[i_rooms].strip() or 0) if i_rooms >= 0 and p[i_rooms].strip() else 0
                night = int(p[i_night].strip() or 0) if i_night >= 0 and p[i_night].strip() else 0
                rn = rooms if rooms > 0 else 1; rev = int(night * rn / 1.1)
                agent = re.sub(r'^(OTA|GOTA)_', '', agent)
                k = (ym, seg, agent)
                samem[k][0] += rn; samem[k][1] += rev
                disp = prop_disp(p[i_prop].strip()) if i_prop >= 0 else None
                if disp: pk = (ym, seg, disp); samem_p[pk][0] += rn; samem_p[pk][1] += rev
            f.close()
    if not want_booking_date: samem = allm; samem_p = allm_p
    return allm, samem, allm_p, samem_p

# 2026 extract: allm26=현재 온북(2026 마감월+진행월 + 2027 진행), same26=최초입력≤CUTOFF(동기)
print('2026 파싱(cur + 2027 전년마감/동기)…'); allm26, same26, allm26_p, same26_p = parse_year('data/raw_db/2026', True)
# 2025 extract: 2026의 전년마감/동기
print('2025 파싱(2026 전년마감/동기)…'); ly_all, ly_same, ly_all_p, ly_same_p = parse_year('data/raw_db/2025', True)

out = {}
def ensure(ym, seg):
    out.setdefault(ym, {}).setdefault(seg, {'lyf': [0, 0], 'lys': [0, 0], 'agents': {}})
    return out[ym][seg]
def ag(ym, seg, name):
    node = ensure(ym, seg)['agents']
    return node.setdefault(name, {'cur': [0, 0], 'lyf': [0, 0], 'lys': [0, 0]})
def to_next(ym):  # 투숙연도 +1 (동월): 전년→당년 매핑
    return str(int(ym[:4]) + 1) + ym[4:6]

# cur (온북) — 2026 extract 전체: 2026 당년 + 2027 당년
for (ym, seg, agent), v in allm26.items():
    ensure(ym, seg)
    if seg in ('OTA', 'G-OTA'):
        a = ag(ym, seg, agent); a['cur'][0] += v[0]; a['cur'][1] += v[1]

def merge_prev(all_map, same_map, src_year):
    """src_year(전년) 투숙분을 당년(src_year+1)의 전년마감(lyf)/전년동기(lys)로 매핑."""
    for (ym, seg, agent), v in all_map.items():
        if ym[:4] != src_year: continue
        y2 = to_next(ym); s = ensure(y2, seg)
        s['lyf'][0] += v[0]; s['lyf'][1] += v[1]
        if seg in ('OTA', 'G-OTA'):
            a = ag(y2, seg, agent); a['lyf'][0] += v[0]; a['lyf'][1] += v[1]
    for (ym, seg, agent), v in same_map.items():
        if ym[:4] != src_year: continue
        y2 = to_next(ym); s = ensure(y2, seg)
        s['lys'][0] += v[0]; s['lys'][1] += v[1]
        if seg in ('OTA', 'G-OTA'):
            a = ag(y2, seg, agent); a['lys'][0] += v[0]; a['lys'][1] += v[1]

merge_prev(ly_all, ly_same, '2025')   # → 2026 전년마감/동기
merge_prev(allm26, same26, '2026')    # → 2027 전년마감/동기 (2026 마감 + 2026 동기 스냅샷)

# ── 동기(lys) 유효 구간 가드 ─────────────────────────────────────────────
# 기준월+3 밖 투숙월은 리드타임상 전년동기 표본이 과소(수십 실)→YoY 왜곡. lys=0 으로 비워
# 페이지가 '전년마감 진행률' 폴백을 타게 함. GS실시간실적현황 '동기간은 현재월로부터 3개월까지만' 과 동일.
_bm = _asof.year * 12 + _asof.month    # 기준월 (예: 2026-11 → 24323)
HORIZON = 3
_guarded = []
for ym in out:
    ahead = (int(ym[:4]) * 12 + int(ym[4:6])) - _bm
    if ahead > HORIZON:
        for seg in out[ym]:
            if out[ym][seg]['lys'][0]: _guarded.append(ym)
            out[ym][seg]['lys'] = [0, 0]
            for a in out[ym][seg]['agents'].values():
                a['lys'] = [0, 0]
print(f'동기 가드(기준월+{HORIZON} 밖 lys=0): {sorted(set(_guarded))}')

# ── 사업장×세그 전년동기/전년마감 (_prop) — 전략 섹션 사업장 필터용 ──
out_prop = {}
def ensure_p(ym, disp, seg):
    return out_prop.setdefault(ym, {}).setdefault(disp, {}).setdefault(seg, {'lyf': [0, 0], 'lys': [0, 0]})
def merge_prev_p(all_map, same_map, src_year):
    for (ym, seg, disp), v in all_map.items():
        if ym[:4] != src_year: continue
        s = ensure_p(to_next(ym), disp, seg); s['lyf'][0] += v[0]; s['lyf'][1] += v[1]
    for (ym, seg, disp), v in same_map.items():
        if ym[:4] != src_year: continue
        s = ensure_p(to_next(ym), disp, seg); s['lys'][0] += v[0]; s['lys'][1] += v[1]
merge_prev_p(ly_all_p, ly_same_p, '2025')
merge_prev_p(allm26_p, same26_p, '2026')
# 동일 가드: 기준월+3 밖 lys=0
for ym in out_prop:
    if (int(ym[:4]) * 12 + int(ym[4:6])) - _bm > HORIZON:
        for disp in out_prop[ym]:
            for seg in out_prop[ym][disp]:
                out_prop[ym][disp][seg]['lys'] = [0, 0]
out['_prop'] = out_prop
print('사업장 전년동기(_prop) 월수:', len(out_prop), '| 2027-01 사업장수:', len(out_prop.get('202701', {})))

# 거래처 RN(cur, 없으면 lyf) 내림차순 정렬
for ym in out:
    if ym == '_prop': continue
    for seg in out[ym]:
        ags = out[ym][seg]['agents']
        srt = dict(sorted(ags.items(), key=lambda kv: -(kv[1]['cur'][0] or kv[1]['lyf'][0])))
        out[ym][seg]['agents'] = srt

json.dump(out, open('docs/data/strategy_agent.json', 'w'), ensure_ascii=False, separators=(',', ':'))
_yms = sorted(k for k in out if k != '_prop')
print('wrote docs/data/strategy_agent.json', os.path.getsize('docs/data/strategy_agent.json') // 1024, 'KB · 투숙월', _yms[0], '~', _yms[-1])
# 샘플: 2027-01 (전년동기=2026-01 동기 스냅샷)
for ym in ('202701',):
    print(f'=== {ym} (전년동기=2026 동일 리드타임) ===')
    for seg in ('OTA', 'G-OTA', 'Inbound'):
        s = out.get(ym, {}).get(seg, {})
        if not s: continue
        print(f'  {seg}: 전년동기(lys) {s.get("lys")} · 전년마감(lyf) {s.get("lyf")}')
