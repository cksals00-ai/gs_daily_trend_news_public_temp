#!/usr/bin/env python3
"""
build_inbound_enriched.py — Inbound (변경예약집계코드=58) 거래처별 국적 매핑

87-prefix 회원번호 = 신코드 (2026.03~), 회원명에 '거래처(국적)' 형식
57-prefix 회원번호 = 구코드 (2022~2026.02), 회원명에 거래처명만 (국적 없음)

처리:
1. 22~26년 raw_db 의 43_/44_ [57,87] 파일에서 인바운드(58) 레코드 추출 (booking + cancel)
2. 87 마스터: 거래처명 → {국적: count}, 가장 많은 국적이 대표 국적
3. 57 매핑:
   - 회원명에 (국적) 있으면 그대로 사용 (confidence='57원본')
   - 키인 매핑이 있으면 키인 (confidence='키인매핑')
   - 87 master에 base 거래처명이 있으면 master 국적 (confidence='87매핑')
   - 그 외 → '미확인' (confidence='미확인')
4. docs/data/inbound_enriched.json 저장: 연도별/월별/거래처별 국적 통계 + 매핑 신뢰도 + YoY 구조

매출(REV) = 1박객실료 × 객실수 ÷ 1.1 (BI Power Query 기준 VAT 제외)
RN = 객실수 (각 행이 1박 단위)
"""
import os, re, json, sys, logging, glob
from pathlib import Path
from collections import defaultdict, Counter

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
RAW_DB_DIR = PROJECT_DIR / 'data' / 'raw_db'
OUTPUT_PATH = PROJECT_DIR / 'docs' / 'data' / 'inbound_enriched.json'
KEYIN_PATH = PROJECT_DIR / 'docs' / 'data' / 'inbound_partner_nationality_keyin.json'

YEARS = ['2022', '2023', '2024', '2025', '2026']

# 매핑 자체가 의미 없는 회원명 (배제도 매핑도 아님 — '미확인' 상태로 두지만 키인에 추가하지 않음)
UNMAPPABLE_PARTNERS = {'티케이트래블', '원더트립', '에스에이투어', '코리얼트립'}


def load_keyin_mappings():
    """관리자 키인 거래처→국적 매핑 로드. 없으면 빈 dict."""
    if not KEYIN_PATH.exists():
        return {}
    try:
        with open(KEYIN_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        m = data.get('mappings', {}) or {}
        return {k.strip(): normalize_nationality(v) for k, v in m.items()
                if k and v and v.strip()}
    except Exception as e:
        logger.warning(f'키인 파일 로드 실패: {e}')
        return {}


# 국적 표기 정규화: 별칭 매핑 + 비-국적 토큰 무효화 (None)
NATIONALITY_NORMALIZE = {
    '대만법인': '대만', '대만FIT': '대만',
    '말레이FIT': '말레이시아', '말레이': '말레이시아',
    '싱가폴': '싱가포르', '미국': '미주',
    # 비-국적 토큰 — 87 회원명 ()에 잘못 들어간 케이스 (내부 사용·예약상태 등)
    '대체건': None, '답사건': None, '노쇼대체': None, '사전입소': None,
    '장기숙박': None, '장기숙박건': None,
    '직원객실': None, '직원객실건': None,
    '행사참석': None, '인바운드': None,
    '공연팀': None, '촬영팀': None,
    '변산': None, '강원도청': None, '강원관광재단': None,
    'TK트래블': None, '클룩': None, '유신여행사': None,
    '팸투어': None,
}

# 의미 있는 국적 레이블 화이트리스트 — 그 외 토큰은 normalize에서 None 반환
VALID_NATIONALITIES = {
    '대만', '중국', '일본', '홍콩', '말레이시아', '태국',
    '인도네시아', '베트남', '싱가포르', '필리핀', '러시아',
    '호주', '미주', '캐나다', '인도', '몽골',
    '이스라엘', '프랑스', '터키', '에스토니아', 'UAE',
    '브라질', '카자흐스탄', '이슬람', '스페인', '이탈리아',
    '덴마크', '독일', '캄보디아', '우크라이나', '라오스',
    '유럽', '네덜란드', '중동', '다국적',
}

PFX_RE = re.compile(r'^[#*\s]+')
PAREN_RE = re.compile(r'\(([^)]+)\)')

# 43 (예약) 컬럼 인덱스 — KEY_RSV_NO 포함 헤더
SPEC_43 = {
    'btype': 'booking',
    'idx_sell': 1, 'idx_prop': 3, 'idx_member_no': 5, 'idx_member_name': 6,
    'idx_user': 7, 'idx_code_num': 12, 'idx_night_rate': 26, 'idx_rooms': 28,
}
# 44 (취소) 컬럼 인덱스 — KEY_RSV_NO 없음
SPEC_44 = {
    'btype': 'cancel',
    'idx_sell': 0, 'idx_prop': 3, 'idx_member_no': 4, 'idx_member_name': 5,
    'idx_user': 6, 'idx_code_num': 11, 'idx_night_rate': 23, 'idx_rooms': 25,
}


def discover_files():
    """22~26년 디렉터리에서 43.[57,87].../44.[57,87]... .txt 자동 검색."""
    out = []
    for y in YEARS:
        d = RAW_DB_DIR / y
        if not d.exists():
            logger.warning(f'연도 디렉터리 없음: {d}')
            continue
        for fp in sorted(d.glob('43.[[]57,87[]]*.txt')):
            out.append({'path': fp, 'year': y, **SPEC_43})
        for fp in sorted(d.glob('44.[[]57,87[]]*.txt')):
            out.append({'path': fp, 'year': y, **SPEC_44})
    return out


def normalize_nationality(nat):
    """국적 토큰 정규화. 알 수 없는/비-국적 토큰은 None.
    혼합 토큰 ('대만 팸투어', '중국 콩쿨대회', '중국인플루언서')은 substring 매칭으로 국가명 추출."""
    if not nat:
        return None
    n = nat.strip()
    if n in NATIONALITY_NORMALIZE:
        return NATIONALITY_NORMALIZE[n]
    if n in VALID_NATIONALITIES:
        return n
    # 혼합 토큰: 긴 토큰 먼저
    for token in sorted(KNOWN_COUNTRY_TOKENS.keys(), key=len, reverse=True):
        if token in n:
            return KNOWN_COUNTRY_TOKENS[token]
    return None


# 거래처명 정규화: 동일 거래처를 다양하게 표기한 케이스를 같은 base로 모음.
LEADING_CO_RE = re.compile(r'^(\(주\)|㈜|㈜\s*)')
TRAILING_CO_RE = re.compile(r'(\(주\)|㈜|주식회사)\s*$')
# 입력에서 마지막 ')' 앞에 한글/한자/영문 토큰이 있고 매칭되는 '('가 없는 경우의 보정용.
MALFORMED_PAREN_RE = re.compile(r'([가-힯一-鿿A-Za-z]{1,8})\)\s*$')

# 이름 본문에 등장하면 국적으로 추정 가능한 토큰 (긴 토큰 우선 매칭).
KNOWN_COUNTRY_TOKENS = {
    '말레이시아': '말레이시아', '말레이': '말레이시아', '인도네시아': '인도네시아',
    '카자흐스탄': '카자흐스탄', '우크라이나': '우크라이나', '에스토니아': '에스토니아',
    '캄보디아': '캄보디아', '이스라엘': '이스라엘', '네덜란드': '네덜란드',
    '이탈리아': '이탈리아', '싱가포르': '싱가포르', '싱가폴': '싱가포르',
    '필리핀': '필리핀', '베트남': '베트남', '캐나다': '캐나다',
    '브라질': '브라질', '스페인': '스페인', '터키': '터키',
    '대만': '대만', '중국': '중국', '일본': '일본', '홍콩': '홍콩',
    '태국': '태국', '러시아': '러시아', '호주': '호주',
    '독일': '독일', '프랑스': '프랑스', '몽골': '몽골',
    '인도': '인도', '미국': '미주', '미주': '미주',
    '덴마크': '덴마크', '라오스': '라오스', 'UAE': 'UAE',
}


def _strip_malformed_close_paren(s):
    """'화동여행사대만)' 처럼 닫는 괄호만 있는 형식 보정 — (token, rest) 반환.
    매칭 안되면 (None, s) 반환."""
    if ')' in s and s.count('(') < s.count(')'):
        m = MALFORMED_PAREN_RE.search(s)
        if m:
            return m.group(1), s[:m.start()].strip()
    return None, s


def base_partner(member_name):
    """거래처 base 이름 추출:
    - 선두 #/*/공백 제거
    - 닫는 괄호만 있는 형식 보정
    - 첫 '(' 앞부분 (단, '(주)'/'㈜'은 회사형식이므로 별도 처리)
    - 끝의 -NNN, _A, ' A' 등 정리
    - (주)/㈜/주식회사 prefix·suffix 제거
    - 내부 다중 공백 단일화
    """
    if not member_name:
        return ''
    s = PFX_RE.sub('', member_name).strip()
    # 닫는 괄호만 있는 형식 → 토큰 분리
    _, s = _strip_malformed_close_paren(s)
    # 첫 '(' 앞부분 (단 (주)/㈜는 보존했다가 아래에서 제거)
    if '(' in s:
        head, sep, rest = s.partition('(')
        if rest.startswith('주)'):
            after = rest[2:]
            s2 = head + '(주)' + after
            if '(' in after:
                s = (head + '(주)' + after.split('(', 1)[0]).rstrip()
            else:
                s = s2.rstrip()
        else:
            s = head.rstrip()
    s = LEADING_CO_RE.sub('', s).strip()
    s = TRAILING_CO_RE.sub('', s).strip()
    s = re.sub(r'[\-_]\d{2,}\s*$', '', s).strip()
    s = re.sub(r'[\s_]+[A-Za-z]{1,2}\s*$', '', s).strip()
    # 끝의 장식용 ' - ' / '_' / 공백 제거 ('US아주투어 - ' → 'US아주투어')
    s = re.sub(r'[\s\-_]+$', '', s).strip()
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def extract_country_from_name(member_name):
    """회원명에서 () 안 텍스트 = 국적. '(주)' 같은 회사형식은 무시.
    잘못된 형식 '...국적)' (앞 '(' 없음)도 마지막 한글/영문 토큰을 국적으로 시도."""
    if not member_name:
        return None
    matches = PAREN_RE.findall(member_name)
    if matches:
        last = matches[-1].strip()
        if last == '주' or last.startswith('주식회사'):
            return None
        return normalize_nationality(last)
    # Fallback: 닫는 괄호만 있는 케이스
    token, _ = _strip_malformed_close_paren(member_name)
    if token:
        return normalize_nationality(token)
    return None


def infer_nationality_from_name(member_name):
    """이름 본문에 국가명 토큰이 있으면 그 국적을 추정 — '추정매핑' 신뢰도 부여용.
    - 마스터/키인에서 못 찾은 거래처에만 fallback 적용.
    - 긴 토큰 먼저 검사 (말레이시아 vs 말레이).
    - 토큰 매칭은 단순 substring — 한글 단어경계가 모호하므로 토큰 길이로 정확도 확보.
    """
    if not member_name:
        return None
    s = PFX_RE.sub('', member_name).strip()
    # '(주)' 같은 회사형식은 검사에서 제거
    s_clean = re.sub(r'\(주\)|㈜', '', s)
    for token in sorted(KNOWN_COUNTRY_TOKENS.keys(), key=len, reverse=True):
        if token in s_clean:
            return KNOWN_COUNTRY_TOKENS[token]
    return None


def parse_int(v):
    try:
        return int((v or '').strip())
    except (ValueError, TypeError):
        return 0


def iter_inbound_rows(specs):
    """모든 파일에서 inbound(58) 레코드만 yield. line hash로 GLOBAL 중복 제거 — 동일 라인이
    재전송 파일과 일자 갱신 파일에 동시 등장해도 1번만 카운트."""
    seen = set()
    for spec in specs:
        fp = spec['path']
        if not fp.exists():
            logger.warning(f'파일 없음: {fp}')
            continue
        n_total = 0
        n_emitted = 0
        n_dup = 0
        with open(fp, 'r', encoding='cp949', errors='replace') as f:
            f.readline()  # header
            for line in f:
                n_total += 1
                line_stripped = line.rstrip('\n\r')
                h = hash(line_stripped)
                if h in seen:
                    n_dup += 1
                    continue
                seen.add(h)
                parts = line.split(';')
                if len(parts) <= max(spec['idx_code_num'], spec['idx_member_no'],
                                     spec['idx_member_name'], spec['idx_night_rate'],
                                     spec['idx_rooms'], spec['idx_sell']):
                    continue
                code_num = parts[spec['idx_code_num']].strip()
                if code_num != '58':
                    continue
                member_no = parts[spec['idx_member_no']].strip()
                if not (member_no.startswith('57') or member_no.startswith('87')):
                    continue
                sell_date = parts[spec['idx_sell']].strip()
                if len(sell_date) < 6:
                    continue
                stay_month = sell_date[:6]
                year = stay_month[:4]
                member_name = parts[spec['idx_member_name']].strip()
                rooms = parse_int(parts[spec['idx_rooms']])
                night_rate = parse_int(parts[spec['idx_night_rate']])
                rn = rooms if rooms > 0 else 1
                rev = int(night_rate * rn / 1.1)
                n_emitted += 1
                yield {
                    'btype': spec['btype'],
                    'member_no': member_no,
                    'prefix': member_no[:2],
                    'member_name': member_name,
                    'stay_month': stay_month,
                    'stay_year': year,
                    'sell_date': sell_date[:8],
                    'rn': rn,
                    'rev': rev,
                }
        logger.info(f'  {fp.name[:60]:60s} {n_total:>8,}행→인바운드 {n_emitted:>6,}  (dup{n_dup:,})')


def build_master(rows):
    """87-prefix 거래처명별 국적 분포 → 가장 많은 국적이 대표.
    반환: {base_partner: {'top': nat, 'distribution': {nat: count}}}
    """
    raw_dist = defaultdict(Counter)
    for r in rows:
        if r['prefix'] != '87':
            continue
        nat = extract_country_from_name(r['member_name'])
        if not nat:
            continue
        b = base_partner(r['member_name'])
        if not b:
            continue
        raw_dist[b][nat] += r['rn']

    master = {}
    for b, dist in raw_dist.items():
        top = dist.most_common(1)[0][0]
        master[b] = {
            'top': top,
            'distribution': dict(dist),
        }
    return master


def apply_mapping(rows, master, keyin=None):
    """각 row에 nationality + mapping_confidence 부여."""
    keyin = keyin or {}
    enriched = []
    for r in rows:
        prefix = r['prefix']
        member_name = r['member_name']
        nat_in_name = extract_country_from_name(member_name)
        b = base_partner(member_name)

        if prefix == '87':
            if nat_in_name:
                r['nationality'] = nat_in_name
                r['mapping_confidence'] = '87원본'
            elif b in keyin:
                r['nationality'] = keyin[b]
                r['mapping_confidence'] = '키인매핑'
            elif b in master:
                r['nationality'] = master[b]['top']
                r['mapping_confidence'] = '87매핑'
            else:
                inferred = infer_nationality_from_name(member_name)
                if inferred:
                    r['nationality'] = inferred
                    r['mapping_confidence'] = '추정매핑'
                else:
                    r['nationality'] = '미확인'
                    r['mapping_confidence'] = '미확인'
        else:  # 57
            if nat_in_name:
                r['nationality'] = nat_in_name
                r['mapping_confidence'] = '57원본'
            elif b in keyin:
                r['nationality'] = keyin[b]
                r['mapping_confidence'] = '키인매핑'
            elif b in master:
                r['nationality'] = master[b]['top']
                r['mapping_confidence'] = '87매핑'
            else:
                inferred = infer_nationality_from_name(member_name)
                if inferred:
                    r['nationality'] = inferred
                    r['mapping_confidence'] = '추정매핑'
                else:
                    r['nationality'] = '미확인'
                    r['mapping_confidence'] = '미확인'
        r['base_partner'] = b
        enriched.append(r)
    return enriched


def _zero_metrics():
    return {'rn_booking': 0, 'rev_booking': 0, 'rn_cancel': 0, 'rev_cancel': 0}


def _add_metric(d, r):
    if r['btype'] == 'booking':
        d['rn_booking'] += r['rn']
        d['rev_booking'] += r['rev']
    else:
        d['rn_cancel'] += r['rn']
        d['rev_cancel'] += r['rev']


def _with_net(v):
    return {**v, 'rn_net': v['rn_booking'] - v['rn_cancel'],
            'rev_net': v['rev_booking'] - v['rev_cancel']}


def aggregate(enriched):
    """월별·연도별·거래처별 국적별 / 신뢰도별 요약 + YoY 비교 구조."""
    monthly = defaultdict(lambda: defaultdict(_zero_metrics))      # YYYYMM → nat
    yearly = defaultdict(lambda: defaultdict(_zero_metrics))       # YYYY  → nat
    monthly_yoy = defaultdict(lambda: defaultdict(lambda: defaultdict(_zero_metrics)))  # MM → YYYY → nat
    by_partner = defaultdict(lambda: defaultdict(lambda: {'rn_booking': 0, 'rev_booking': 0,
                                                          'rn_cancel': 0, 'rev_cancel': 0,
                                                          'prefix': set(), 'years': set()}))
    confidence_summary = defaultdict(_zero_metrics)
    confidence_summary_count = defaultdict(int)
    confidence_by_year = defaultdict(lambda: defaultdict(_zero_metrics))
    confidence_by_year_count = defaultdict(lambda: defaultdict(int))

    for r in enriched:
        m = r['stay_month']      # YYYYMM
        y = r['stay_year']       # YYYY
        mm = m[4:6]              # MM
        nat = r['nationality']
        b = r['base_partner'] or '(미상)'
        c = r['mapping_confidence']

        _add_metric(monthly[m][nat], r)
        _add_metric(yearly[y][nat], r)
        _add_metric(monthly_yoy[mm][y][nat], r)
        _add_metric(by_partner[b][nat], r)
        by_partner[b][nat]['prefix'].add(r['prefix'])
        by_partner[b][nat]['years'].add(y)

        _add_metric(confidence_summary[c], r)
        confidence_summary_count[c] += 1
        _add_metric(confidence_by_year[y][c], r)
        confidence_by_year_count[y][c] += 1

    monthly_out = {m: {n: _with_net(v) for n, v in by_n.items()} for m, by_n in monthly.items()}
    yearly_out = {y: {n: _with_net(v) for n, v in by_n.items()} for y, by_n in yearly.items()}
    monthly_yoy_out = {mm: {y: {n: _with_net(v) for n, v in by_n.items()}
                            for y, by_n in by_y.items()}
                       for mm, by_y in monthly_yoy.items()}

    by_partner_out = {}
    for b, by_n in by_partner.items():
        by_partner_out[b] = {}
        for nat, v in by_n.items():
            v_clean = {k: vv for k, vv in v.items() if k not in ('prefix', 'years')}
            by_partner_out[b][nat] = {
                **_with_net(v_clean),
                'prefix': sorted(v['prefix']),
                'years': sorted(v['years']),
            }

    confidence_summary_out = {}
    for c, v in confidence_summary.items():
        confidence_summary_out[c] = {**_with_net(v), 'rows': confidence_summary_count[c]}

    confidence_by_year_out = {}
    for y, by_c in confidence_by_year.items():
        confidence_by_year_out[y] = {}
        for c, v in by_c.items():
            confidence_by_year_out[y][c] = {**_with_net(v), 'rows': confidence_by_year_count[y][c]}

    return {
        'monthly_by_nationality': monthly_out,
        'yearly_by_nationality': yearly_out,
        'monthly_yoy_by_nationality': monthly_yoy_out,
        'by_partner_nationality': by_partner_out,
        'mapping_confidence_summary': confidence_summary_out,
        'mapping_confidence_by_year': confidence_by_year_out,
    }


def collect_unmapped_partners(enriched):
    """미확인 거래처 + 빈도 + 연도 분포. 키인 매핑 보강 후보 식별용."""
    unk = defaultdict(lambda: {'rows': 0, 'rn_booking': 0, 'rn_cancel': 0,
                               'rev_booking': 0, 'rev_cancel': 0,
                               'prefix': set(), 'years': defaultdict(int),
                               'sample_member_names': set()})
    for r in enriched:
        if r['nationality'] != '미확인':
            continue
        b = r['base_partner'] or '(미상)'
        d = unk[b]
        d['rows'] += 1
        if r['btype'] == 'booking':
            d['rn_booking'] += r['rn']
            d['rev_booking'] += r['rev']
        else:
            d['rn_cancel'] += r['rn']
            d['rev_cancel'] += r['rev']
        d['prefix'].add(r['prefix'])
        d['years'][r['stay_year']] += r['rn']
        if len(d['sample_member_names']) < 3:
            d['sample_member_names'].add(r['member_name'])

    out = []
    for b, d in unk.items():
        out.append({
            'partner': b,
            'rows': d['rows'],
            'rn_booking': d['rn_booking'],
            'rn_cancel': d['rn_cancel'],
            'rn_net': d['rn_booking'] - d['rn_cancel'],
            'rev_booking': d['rev_booking'],
            'prefix': sorted(d['prefix']),
            'years_rn': dict(sorted(d['years'].items())),
            'sample_names': sorted(d['sample_member_names']),
            'unmappable_flag': b in UNMAPPABLE_PARTNERS,
        })
    out.sort(key=lambda x: x['rn_net'], reverse=True)
    return out


def print_report(enriched, master, agg, unmapped):
    rows = enriched
    n_total = len(rows)
    n_57 = sum(1 for r in rows if r['prefix'] == '57')
    n_87 = sum(1 for r in rows if r['prefix'] == '87')

    print('\n' + '=' * 78)
    print('인바운드(58) 거래처-국적 매핑 결과 — 22~26년 통합')
    print('=' * 78)
    print(f'전체 inbound 레코드(행 단위): {n_total:,}  (57:{n_57:,}  87:{n_87:,})')

    # 연도별 행수
    print('\n[연도별 행수 + booking RN 합]')
    by_year_rows = Counter(r['stay_year'] for r in rows)
    for y in sorted(by_year_rows):
        rn = sum(r['rn'] for r in rows if r['stay_year'] == y and r['btype'] == 'booking')
        ca = sum(r['rn'] for r in rows if r['stay_year'] == y and r['btype'] == 'cancel')
        print(f'  {y}  rows={by_year_rows[y]:>7,}  booking_RN={rn:>7,}  cancel_RN={ca:>7,}  net={rn-ca:>7,}')

    # 매핑 신뢰도 (전체)
    print('\n[매핑 신뢰도 분포 — 전체]')
    print(f'{"신뢰도":<10}{"행 수":>10}{"booking RN":>14}{"cancel RN":>14}{"net RN":>12}')
    for k in ['87원본', '87매핑', '57원본', '키인매핑', '추정매핑', '미확인']:
        v = agg['mapping_confidence_summary'].get(k)
        if not v:
            continue
        print(f'{k:<10}{v["rows"]:>10,}{v["rn_booking"]:>14,}{v["rn_cancel"]:>14,}{v["rn_net"]:>12,}')

    # 매핑 신뢰도 (연도별, 행수만)
    print('\n[매핑 신뢰도 — 연도별 행수]')
    confs = ['87원본', '87매핑', '57원본', '키인매핑', '추정매핑', '미확인']
    print(f'{"연도":<6}', end='')
    for c in confs:
        print(f'{c:>10}', end='')
    print(f'{"미확인%":>10}')
    for y in sorted(agg['mapping_confidence_by_year']):
        per = agg['mapping_confidence_by_year'][y]
        total = sum(v['rows'] for v in per.values())
        unkn = per.get('미확인', {'rows': 0})['rows']
        print(f'{y:<6}', end='')
        for c in confs:
            print(f'{per.get(c, {"rows": 0})["rows"]:>10,}', end='')
        pct = (unkn / total * 100) if total else 0
        print(f'{pct:>9.1f}%')

    # 87 master 통계
    print(f'\n[87-prefix 마스터] 고유 거래처: {len(master)}개')
    by_nat = Counter(v['top'] for v in master.values())
    print('대표 국적별 분포:')
    for nat, c in by_nat.most_common():
        print(f'  {nat:<14} {c:>4}개사')

    # 미확인 거래처 Top
    print(f'\n[미확인 거래처 Top 25 — net RN 기준]')
    print(f'{"#":>3}  {"net RN":>8}  {"prefix":<8}  {"거래처":<28}  연도별RN')
    for i, u in enumerate(unmapped[:25], 1):
        years_str = ' '.join(f'{y}:{rn}' for y, rn in u['years_rn'].items())
        flag = ' [UNMAPPABLE]' if u['unmappable_flag'] else ''
        print(f'{i:>3}  {u["rn_net"]:>8,}  {",".join(u["prefix"]):<8}  {u["partner"][:28]:<28}  {years_str}{flag}')

    # 월별 인바운드 RN (최근 12개월)
    print('\n[최근 12개월 인바운드 booking RN — 국적 Top 8]')
    months = sorted(agg['monthly_by_nationality'].keys())[-12:]
    nat_total = Counter()
    for m in months:
        for n, v in agg['monthly_by_nationality'][m].items():
            nat_total[n] += v['rn_booking']
    top_nats = [n for n, _ in nat_total.most_common(8)]
    print(f'{"월":<8}{"전체":>9}', end='')
    for n in top_nats:
        print(f'{n:>9}', end='')
    print()
    for m in months:
        per = agg['monthly_by_nationality'][m]
        total = sum(v['rn_booking'] for v in per.values())
        print(f'{m:<8}{total:>9,}', end='')
        for n in top_nats:
            v = per.get(n, {'rn_booking': 0})['rn_booking']
            print(f'{v:>9,}', end='')
        print()
    print('=' * 78)


def main():
    specs = discover_files()
    logger.info(f'대상 파일 {len(specs)}개 — 연도별 스캔 시작')
    rows = list(iter_inbound_rows(specs))
    logger.info(f'전체 inbound 레코드: {len(rows):,}행')
    master = build_master(rows)
    logger.info(f'87 master 거래처: {len(master)}개')
    keyin = load_keyin_mappings()
    if keyin:
        logger.info(f'키인 매핑: {len(keyin)}개')
    enriched = apply_mapping(rows, master, keyin)
    agg = aggregate(enriched)
    unmapped = collect_unmapped_partners(enriched)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'generated_at': '2026-05-04',
        'description': 'Inbound(변경예약집계코드=58) 거래처-국적 매핑 enriched 데이터 — 22~26년 통합',
        'years_covered': sorted({r['stay_year'] for r in rows}),
        'mapping_rule': {
            '87원본': '87-prefix 회원명에 (국적) 직접 표기',
            '87매핑': '거래처명을 87 master에서 lookup (대표 국적)',
            '57원본': '57-prefix 회원명에 (국적) 직접 표기 (드물게 발생)',
            '키인매핑': '관리자 키인 매핑 (inbound_partner_nationality_keyin.json)',
            '추정매핑': '거래처 이름 본문에 명시된 국가명 토큰 기반 추정',
            '미확인': '매핑 불가 — 87 master/키인/이름 추정 모두 실패',
        },
        'master_partner_count': len(master),
        'keyin_mapping_count': len(keyin),
        'keyin_mappings': keyin,
        'master_top_nationality': {
            b: {'top': v['top'], 'distribution': v['distribution']}
            for b, v in sorted(master.items())
        },
        'unmapped_partners_top': unmapped[:100],  # 키인 보강 후보 식별용
        **agg,
    }
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f'저장: {OUTPUT_PATH}  ({OUTPUT_PATH.stat().st_size/1024:.1f} KB)')

    print_report(enriched, master, agg, unmapped)


if __name__ == '__main__':
    main()
