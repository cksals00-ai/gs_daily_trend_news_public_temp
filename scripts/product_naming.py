"""상품 큐브용 순수 함수: 상품명 정규화 · 채널 분류 · 사업장 display · 요금시즌 라벨 · 기획전 맵.

build_product_cube.py가 사용. 외부 상태 없음(테스트: scripts/tests/test_product_naming.py).
"""
import json
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
from generate_otb_data import PROPERTY_DEFS  # (bi_name, display, region, db_props)

SEASONS = {'41': '비수기 주중', '43': '비수기 주말(금)', '42': '비수기 주말(토)',
           '11': '성수기 주중', '14': '성수기 주말(금)', '12': '성수기 주말(토)',
           '13': '연휴', '30': '연휴'}
WEEKDAY_SEASONS = ('41', '11')
GS_SEGS = ('OTA', 'G-OTA', 'Inbound')
SEG_BY_CODE = {'72': 'OTA', '53': 'OTA', 'A4': 'G-OTA', 'A5': 'G-OTA', '58': 'Inbound',
               'MP': '회원', '34': 'D멤버스', '73': '무기명', 'CP': '제휴'}

PROP_ORDER = [d[1] for d in PROPERTY_DEFS]
PROP_REGION = {d[1]: d[2] for d in PROPERTY_DEFS}
_PROP_LOOKUP = {}
for _bi, _disp, _region, _dbs in PROPERTY_DEFS:
    for _n in _dbs:
        _PROP_LOOKUP[_n.replace(' ', '')] = _disp
# 긴 이름 먼저 매칭되도록 정렬 (예: '소노펠리체 빌리지 비발디파크' > '소노펠리체 비발디파크')
_PROP_KEYS_BY_LEN = sorted(_PROP_LOOKUP, key=len, reverse=True)


def prop_display(raw_prop):
    """'02. 소노벨 비발디파크' → '01.벨비발디'. 미매핑(팔라티움 등) → None."""
    if not raw_prop:
        return None
    s = re.sub(r'^\s*\d+\.\s*', '', raw_prop).replace(' ', '')
    if s in _PROP_LOOKUP:
        return _PROP_LOOKUP[s]
    for k in _PROP_KEYS_BY_LEN:  # 부분 일치(예: '소노벨비발디파크B·C')
        if s.startswith(k):
            return _PROP_LOOKUP[k]
    return None


def season_label(code):
    return SEASONS.get((code or '').strip(), '기타')


# ---- 상품명 정규화 ---------------------------------------------------------
_PREFIX_RE = re.compile(r'^\s*[\[(（【][^\])）】]{1,16}[\])）】]\s*')
# 연도 토큰: 25Y, 25-26, 2025, 26/27 … (2인·2박·22평 같은 수량 표현은 뒤 글자로 배제)
_YEAR_RE = re.compile(r'(?<![\d가-힣A-Za-z])(?:20)?2[2-8](?:\s*[-~/]\s*(?:20)?2[2-9])?\s*(?:Y|년)?(?![\d%인박명개평실])')
_CHAN_WORDS = ['GOTA', 'G-OTA', 'OTA', 'D멤버스', '디멤버스', 'D_PKG', 'D PKG', 'DPKG', '무기명', '기명',
               '회원', '직불', '네이버', '쿠팡', '제휴', '대매점', '홈페이지', '자사', '야놀자', '여기어때',
               '아고다', '트립닷컴', '익스피디아', '부킹닷컴', '트립비토즈', '놀유니버스', '맥스모바일',
               '타이드스퀘어', '스마트인피니', '인터파크', '마이리얼트립', '수수료', '신규', 'N']
_CHAN_RE = re.compile(
    r'(?<![가-힣A-Za-z])(?:' + '|'.join(re.escape(w) for w in sorted(_CHAN_WORDS, key=len, reverse=True))
    + r'|G\d{1,2}|\d{1,2}(?:\.\d)?%|0\.1)(?![가-힣A-Za-z])', re.I)
_SEP_RE = re.compile(r'[\s_/,+·]+')
_EMPTY_PAREN_RE = re.compile(r'[\[(（【]\s*[\])）】]')
# 회원구분 토큰은 한글에 붙어 나오는 경우가 많음('노블리안무기명PKG') → PKG/구분자/끝 앞에서만 제거
_MEMBER_RE = re.compile(r'(?:무기명|기명|회원)(?=\s*PKG|\s|_|$|[)\]）】])')


def clean_product_name(name):
    """사업장 접두사·연도·채널/수수료 토큰 제거, 패키지→PKG, 공백 정리."""
    s = (name or '').strip()
    for _ in range(2):
        s = _PREFIX_RE.sub('', s)
    s = _YEAR_RE.sub(' ', s)
    s = s.replace('패키지', 'PKG')
    s = _MEMBER_RE.sub('', s)
    s = _CHAN_RE.sub(' ', s)
    s = re.sub(r'[\[(（【]\s*\d{1,2}\s*[\])）】]', ' ', s)   # '(GOTA15)' → '( 15)' 잔여 숫자 괄호
    s = _EMPTY_PAREN_RE.sub(' ', s)
    s = _SEP_RE.sub(' ', s).strip(' -_·')
    s = re.sub(r'\s*\(\s*\)', '', s).strip()
    s = re.sub(r'^[)\]）】]+\s*', '', s)                     # 'New)SONO…' 선행 닫는 괄호
    s = re.sub(r'\s*[(\[（【]+$', '', s).strip()              # 후행 여는 괄호
    return s


def product_key(name):
    return re.sub(r'[\s\-_()（）\[\]【】·.]', '', clean_product_name(name)).upper()


# ---- 채널 -----------------------------------------------------------------
def channel_class(agent, seg_code, name):
    """(AGENT명, 변경예약집계코드, 회원명) → (seg, 거래처/채널명)."""
    a = (agent or '').strip()
    code = (seg_code or '').strip()
    seg = SEG_BY_CODE.get(code, '기타')
    chan = None
    for pre, s in (('OTA_', 'OTA'), ('GOTA_', 'G-OTA')):
        if a.startswith(pre):
            if seg in ('OTA', 'G-OTA', '기타'):
                seg = s
            chan = a[len(pre):].strip()
            break
    if chan is None:
        chan = a if (a and seg == '기타') else seg
    if seg == 'Inbound':
        chan = 'Inbound'
    return (seg, chan or seg)


# ---- 기획전 맵 ---------------------------------------------------------------
def load_campaign_map(path):
    """data/product_campaign_map.json → {'by_code': {code: name}, 'rules': [(regex, name, year|None)], 'meta': {name: {...}}}"""
    if not path or not os.path.exists(path):
        return {'by_code': {}, 'rules': [], 'meta': {}}
    with open(path, encoding='utf-8') as f:
        doc = json.load(f)
    out = {'by_code': {}, 'rules': [], 'meta': {}}
    for c in doc.get('campaigns', []):
        out['meta'][c['name']] = {k: c.get(k) for k in ('year', 'composition', 'sale', 'stay', 'note', 'scope')}
        out['meta'][c['name']]['source'] = 'map'
        for code in c.get('match', {}).get('codes', []):
            out['by_code'][str(code)] = c['name']
        m = c.get('match', {})
        rx = m.get('regex')
        if rx:
            out['rules'].append((re.compile(rx, re.I), c['name'], c.get('year'),
                                 (m.get('sale_from') or None), (m.get('sale_to') or None)))
    return out


def assign_campaign(cmap, code, name, sale_year, sale_date=None):
    """수기 코드 매핑 → 정규식 규칙(연도 일치 + 판매시작일 범위) → 자동 '{정규화명} ({판매연도})'.
    sale_date = 코드의 최초 판매일(YYYYMMDD). 규칙에 sale_from/sale_to가 있으면 그 범위 안일 때만 매칭."""
    if code in cmap['by_code']:
        return cmap['by_code'][code]
    for rx, cname, yr, s_from, s_to in cmap['rules']:
        if not rx.search(name or ''):
            continue
        if yr is not None and yr != sale_year:
            continue
        if (s_from or s_to):
            if not sale_date:
                continue
            if s_from and sale_date < s_from:
                continue
            if s_to and sale_date > s_to:
                continue
        return cname
    base = clean_product_name(name) or '미상'
    return f'{base} ({sale_year})'
