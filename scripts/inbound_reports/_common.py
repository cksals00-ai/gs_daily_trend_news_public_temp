#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""인바운드 리포트 공용 로직 — 거래처→국가/여행사 매핑 + raw_db 집계.

- 인바운드 정의: 변경예약집계코드(없으면 예약집계코드) == '58'
- 지표: RNS = 객실수(room count, 파일 정의와 동일, 레코드별 RNS==객실수 확인됨)
         REV(백만원) = 1박객실료 × 객실수 / 1.1 (VAT 제외),  ADR(천원) = REV*1000/RNS
- 국가: partner_mapping.json(기존 전략파일 피벗캐시에서 1회 추출) 우선,
        없으면 회원명 '(국적)' 파싱 폴백 → 파일 국가 카테고리로 정규화.
"""
import os, re, glob, json

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
MAPPING_PATH = os.path.join(HERE, 'partner_mapping.json')
RAW_DB = os.path.join(REPO, 'data', 'raw_db')

_M = json.load(open(MAPPING_PATH, encoding='utf-8'))['mem2nat_agent']

NORM = {'대만법인':'대만','대만FIT':'대만','말레이':'말레이시아','말레이FIT':'말레이시아',
        '싱가포르':'싱가폴','홍콩마카오':'홍콩','미국':'미주','캐나다':'미주','구미주':'미주'}
VALID = {'대만','중국','일본','홍콩','말레이시아','태국','싱가폴','베트남','인도네시아',
         '필리핀','미주','영국','호주','러시아','국내','기타'}

def _parse_nat(name):
    for tok in re.findall(r'[(]([^)]*)[)]', name):
        t = NORM.get(tok.strip(), tok.strip())
        if t in VALID: return t
        for v in VALID:
            if v != '기타' and v in t: return v
    return None

def _clean(s):
    s = re.sub(r'^[\*#\s]+', '', s)
    return re.sub(r'^\[[^\]]*\]\s*', '', s).strip()

# ── 국가 오버라이드 (수기 교정) ──────────────────────────────
# nat_overrides.csv: "키,국가"  (키 = 회원명 또는 여행사명, 공백무시 매칭).
# 자동매핑이 틀린 거래처/여행사만 한 줄씩 적으면 최우선 적용된다.
_OVR = {}
_OVR_PATH = os.path.join(HERE, 'nat_overrides.csv')
def _nsp(s): return re.sub(r'\s', '', str(s or ''))
if os.path.exists(_OVR_PATH):
    with open(_OVR_PATH, encoding='utf-8-sig') as _f:
        for _ln in _f:
            _p = _ln.rstrip('\n').split(',')
            if len(_p) >= 2 and _p[0].strip() and not _p[0].strip().startswith('#') and _p[0].strip() != '키':
                _OVR[_nsp(_p[0])] = _p[1].strip()

def agent_of(mem):
    a = _M.get(mem, {}).get('여행사명') or _M.get(_clean(mem), {}).get('여행사명')
    return a or (re.sub(r'\s*[(\[].*', '', _clean(mem)).strip() or _clean(mem))

def nat_of(mem):
    # 1) 오버라이드(회원명/여행사명) 최우선 2) 파일매핑 3) (국적)파싱 4) 기타
    ov = _OVR.get(_nsp(mem)) or _OVR.get(_nsp(agent_of(mem)))
    if ov: return NORM.get(ov, ov)
    g = _M.get(mem, {}).get('국가') or _M.get(_clean(mem), {}).get('국가') or _parse_nat(mem) or '기타'
    return NORM.get(g, g)

def scan_inbound(year, max_month='12'):
    """해당 연도 인바운드(58) 예약 라인.
    returns list of dict(agent, nat, prop(영업장명), month, rns, rev_백만)."""
    out = []
    ydir = os.path.join(RAW_DB, str(year))
    for ft in ('27', '43'):
        for f in glob.glob(os.path.join(ydir, f'{ft}.*.txt')):
            with open(f, encoding='cp949', errors='replace') as fh:
                h = fh.readline().strip().split(';'); ci = {c.strip(): i for i, c in enumerate(h)}
                isd=ci.get('판매일자',-1); irm=ci.get('객실수',-1); i1=ci.get('1박객실료',-1)
                icode=ci.get('변경예약집계코드', ci.get('예약집계코드', -1))
                imem=ci.get('회원명',-1); ip=ci.get('영업장명',-1)
                seen=set()
                for line in fh:
                    hh=hash(line.rstrip('\n\r'))
                    if hh in seen: continue
                    seen.add(hh)
                    p=line.split(';'); n=len(p)
                    if not (0 <= icode < n and p[icode].strip() == '58'): continue
                    sd=p[isd].strip() if 0<=isd<n else ''
                    if len(sd) < 6 or sd[:4] != str(year) or sd[4:6] > max_month: continue
                    try: rm=int(p[irm] or 0)
                    except ValueError: rm=0
                    if rm <= 0 or rm > 5000: continue   # 부패행(예: 110,000) 컷
                    try: rate=int(p[i1] or 0)
                    except ValueError: rate=0
                    mem=p[imem].strip() if 0<=imem<n else ''
                    out.append({'agent': agent_of(mem), 'nat': nat_of(mem),
                                'prop': p[ip].strip() if 0<=ip<n else '',
                                'month': sd[4:6], 'rns': rm, 'rev': rate*rm/1.1/1e6})
    return out
