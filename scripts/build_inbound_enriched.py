#!/usr/bin/env python3
"""
build_inbound_enriched.py — Inbound (변경예약집계코드=58) 거래처별 국적 매핑

87-prefix 회원번호 = 신코드 (2026.03~), 회원명에 '거래처(국적)' 형식
57-prefix 회원번호 = 구코드 (2022~2026.02), 회원명에 거래처명만 (국적 없음)

처리:
1. 22~26년 raw_db 의 43_/44_ [57,87] 파일에서 인바운드(58) 레코드 추출 (booking + cancel)
   ─ 단, 코드=58 이라도 EVENT_KEYWORD_RE / EVENT_EXACT_PARTNERS 에 매칭되면 제외
     (세계피트니스선수권대회·비바윈터페스티벌·일러스타페스·국제회의 등 — 외국인 단체참가
      이벤트라 58로 분류되지만 거래처 단위 분석엔 노이즈)
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

# 매핑 자체가 의미 없는 회원명 fallback (키인 JSON 의 unmappable 섹션이 비어있을 때만 사용).
# 운영 데이터는 docs/data/inbound_partner_nationality_keyin.json 의 "unmappable" 섹션이 정답.
UNMAPPABLE_FALLBACK = {
    '티케이트래블': '다국적 인바운드 거래처 — 단일 국적 매핑 불가',
    '원더트립': '다국적 인바운드 거래처 — 단일 국적 매핑 불가',
    '에스에이투어': '다국적 인바운드 거래처 — 단일 국적 매핑 불가',
    '코리얼트립': '다국적 인바운드 거래처 — 단일 국적 매핑 불가',
    'TK트래블': '다국적 인바운드 거래처 — 단일 국적 매핑 불가',
}

# 코드=58(인바운드)이지만 거래처(여행사)가 아닌 이벤트/단체성 행사 — 인바운드 거래처 분석에서 제외.
# 원본은 외국인 단체참가자라서 58로 분류되지만, 거래처×국적 거래처 단위 분석엔 노이즈.
# member_name(또는 base_partner) 에 다음 토큰이 포함되면 행 자체를 dataset 에서 drop.
EVENT_KEYWORD_RE = re.compile(
    r'(대회|페스티벌|페스티발|페스\b|페스$|컨퍼런스|컨퍼\b|국제회의|학술회의|심포지엄|심포지움|'
    r'포럼|박람회|엑스포|EXPO|expo|콩쿨|콩쿠르|챔피언십|올림픽|선수권|월드컵|'
    r'협력회의|학술대회|기념식|시상식)'
)
# 이벤트성으로 알려진 정확명 — keyword 만으로 안 잡히는 것 보강.
EVENT_EXACT_PARTNERS = {
    '비바윈터페스티벌', '비바윈터페스티벌2차', '비바윈터페스티벌_인바운드',
    '일러스타페스', '일러스타페',
    '세계피트니스선수권대회', '2022 유럽다자안보협력 국제회의',
}


def load_keyin_mappings():
    """관리자 키인 로드. 반환: (mappings dict, unmappable dict).
    - mappings: {거래처명: 국적}
    - unmappable: {거래처명: 사유메모}
    """
    if not KEYIN_PATH.exists():
        return {}, dict(UNMAPPABLE_FALLBACK)
    try:
        with open(KEYIN_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        m = data.get('mappings', {}) or {}
        mappings = {k.strip(): normalize_nationality(v) for k, v in m.items()
                    if k and v and v.strip() and normalize_nationality(v)}
        u = data.get('unmappable', {}) or {}
        unmappable = {k.strip(): (str(v).strip() if v else '매핑 불가')
                      for k, v in u.items() if k and k.strip()}
        if not unmappable:
            unmappable = dict(UNMAPPABLE_FALLBACK)
        return mappings, unmappable
    except Exception as e:
        logger.warning(f'키인 파일 로드 실패: {e}')
        return {}, dict(UNMAPPABLE_FALLBACK)


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
        n_event = 0
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
                # 변경예약집계코드 = 58 (인바운드) 만 통과
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
                # 이벤트/단체성 행사 제외 (code=58 이지만 여행사 거래처가 아님)
                if EVENT_KEYWORD_RE.search(member_name):
                    n_event += 1
                    continue
                _b = base_partner(member_name)
                if _b in EVENT_EXACT_PARTNERS:
                    n_event += 1
                    continue
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
        logger.info(f'  {fp.name[:60]:60s} {n_total:>8,}행→인바운드 {n_emitted:>6,}  (dup{n_dup:,}, event-skip{n_event:,})')


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


def apply_mapping(rows, master, keyin=None, unmappable=None):
    """각 row에 nationality + mapping_confidence 부여.
    매핑 우선순위 (양 prefix 공통):
      1) 키인 unmappable 등재 → '매핑불가' (단일 국적 매핑이 부적합한 다국적 거래처)
      2) 회원명 () 표기 → 'XX원본'
      3) 키인 mappings → '키인매핑'
      4) 87 master 룩업 → '87매핑'
      5) 이름 본문 토큰 추정 → '추정매핑'
      6) 그 외 → '미확인'
    """
    keyin = keyin or {}
    unmappable = unmappable or {}
    enriched = []
    for r in rows:
        prefix = r['prefix']
        member_name = r['member_name']
        nat_in_name = extract_country_from_name(member_name)
        b = base_partner(member_name)

        if b in unmappable:
            r['nationality'] = '매핑불가'
            r['mapping_confidence'] = '매핑불가'
        elif nat_in_name:
            r['nationality'] = nat_in_name
            r['mapping_confidence'] = '87원본' if prefix == '87' else '57원본'
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


def aggregate_member_detail(enriched, recent_months=24):
    """월별 member_name 단위 상세 — gs-sales-report 캘린더 우측(거래처×회원명×국적) 표시용.

    최근 N개월만 출력해 파일 크기 제어. 각 항목에 stay_days 셋을 포함해
    시리즈(반복·다일자) vs 인센티브(단독·단기 대량) 패턴 구분 가능.
    """
    all_months = sorted({r['stay_month'] for r in enriched}, reverse=True)
    recent_set = set(all_months[:recent_months])

    detail = defaultdict(lambda: defaultdict(lambda: {
        'base_partner': '', 'nationality': '', 'prefix': '',
        'rn_booking': 0, 'rev_booking': 0, 'rn_cancel': 0, 'rev_cancel': 0,
        'stay_days': set(),
    }))
    for r in enriched:
        m = r['stay_month']
        if m not in recent_set:
            continue
        mn = r['member_name'] or '(이름 미상)'
        d = detail[m][mn]
        d['base_partner'] = r['base_partner'] or '(미상)'
        d['nationality'] = r['nationality']
        d['prefix'] = r['prefix']
        if r['btype'] == 'booking':
            d['rn_booking'] += r['rn']
            d['rev_booking'] += r['rev']
        else:
            d['rn_cancel'] += r['rn']
            d['rev_cancel'] += r['rev']
        d['stay_days'].add(r['sell_date'])

    out = {}
    for m, by_mn in detail.items():
        items = []
        for mn, v in by_mn.items():
            items.append({
                'member_name': mn,
                'base_partner': v['base_partner'],
                'nationality': v['nationality'],
                'prefix': v['prefix'],
                'rn_booking': v['rn_booking'],
                'rn_cancel': v['rn_cancel'],
                'rn_net': v['rn_booking'] - v['rn_cancel'],
                'rev_booking': v['rev_booking'],
                'rev_net': v['rev_booking'] - v['rev_cancel'],
                'stay_days': sorted(v['stay_days']),
                'n_days': len(v['stay_days']),
            })
        items.sort(key=lambda x: x['rn_net'], reverse=True)
        out[m] = items
    return out


def aggregate_daily(enriched, recent_months=24):
    """투숙일자(YYYYMMDD) 단위 인바운드 RN/매출 합 — 캘린더 좌측 셀의 일별 실적용.
    최근 N개월만 포함해 파일 크기 제어.
    """
    all_months = sorted({r['stay_month'] for r in enriched}, reverse=True)
    recent_set = set(all_months[:recent_months])

    daily = defaultdict(lambda: {'rn_booking': 0, 'rev_booking': 0,
                                  'rn_cancel': 0, 'rev_cancel': 0})
    for r in enriched:
        if r['stay_month'] not in recent_set:
            continue
        d = daily[r['sell_date']]
        if r['btype'] == 'booking':
            d['rn_booking'] += r['rn']
            d['rev_booking'] += r['rev']
        else:
            d['rn_cancel'] += r['rn']
            d['rev_cancel'] += r['rev']

    out = {}
    for ymd, v in sorted(daily.items()):
        out[ymd] = {
            **v,
            'rn_net': v['rn_booking'] - v['rn_cancel'],
            'rev_net': v['rev_booking'] - v['rev_cancel'],
        }
    return out


def _collect_partners_with_confidence(enriched, target_confidence):
    """주어진 confidence 의 거래처별 RN/매출/연도분포 수집."""
    grp = defaultdict(lambda: {'rows': 0, 'rn_booking': 0, 'rn_cancel': 0,
                               'rev_booking': 0, 'rev_cancel': 0,
                               'prefix': set(), 'years': defaultdict(int),
                               'sample_member_names': set()})
    for r in enriched:
        if r['mapping_confidence'] != target_confidence:
            continue
        b = r['base_partner'] or '(미상)'
        d = grp[b]
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
    for b, d in grp.items():
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
        })
    out.sort(key=lambda x: x['rn_net'], reverse=True)
    return out


def collect_unmapped_partners(enriched):
    """미확인 거래처 + 빈도 + 연도 분포. 키인 매핑 보강 후보 식별용."""
    return _collect_partners_with_confidence(enriched, '미확인')


def collect_unmappable_partners(enriched, unmappable):
    """매핑불가 거래처 + 빈도 + 연도 분포 + 사유. 키인 페이지에서 편집 가능하게."""
    out = _collect_partners_with_confidence(enriched, '매핑불가')
    for item in out:
        item['reason'] = unmappable.get(item['partner'], '매핑 불가')
    return out


def collect_keyin_mapped_partners(enriched, keyin):
    """키인매핑된 거래처 + 빈도. 페이지에서 매핑 변경 가능하게."""
    out = _collect_partners_with_confidence(enriched, '키인매핑')
    for item in out:
        item['mapped_to'] = keyin.get(item['partner'], '')
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
    for k in ['87원본', '87매핑', '57원본', '키인매핑', '추정매핑', '매핑불가', '미확인']:
        v = agg['mapping_confidence_summary'].get(k)
        if not v:
            continue
        print(f'{k:<10}{v["rows"]:>10,}{v["rn_booking"]:>14,}{v["rn_cancel"]:>14,}{v["rn_net"]:>12,}')

    # 매핑 신뢰도 (연도별, 행수만)
    print('\n[매핑 신뢰도 — 연도별 행수]')
    confs = ['87원본', '87매핑', '57원본', '키인매핑', '추정매핑', '매핑불가', '미확인']
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
        print(f'{i:>3}  {u["rn_net"]:>8,}  {",".join(u["prefix"]):<8}  {u["partner"][:28]:<28}  {years_str}')

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
    keyin, unmappable = load_keyin_mappings()
    if keyin:
        logger.info(f'키인 매핑: {len(keyin)}개')
    if unmappable:
        logger.info(f'키인 매핑불가(UNMAPPABLE) 거래처: {len(unmappable)}개')
    enriched = apply_mapping(rows, master, keyin, unmappable)
    agg = aggregate(enriched)
    unmapped = collect_unmapped_partners(enriched)
    unmappable_partners = collect_unmappable_partners(enriched, unmappable)
    keyin_mapped_partners = collect_keyin_mapped_partners(enriched, keyin)
    member_detail = aggregate_member_detail(enriched, recent_months=24)
    daily = aggregate_daily(enriched, recent_months=24)

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
            '매핑불가': '키인 unmappable 등재 (다국적/혼합 — 단일 국적 매핑 부적합)',
            '미확인': '매핑 불가 — 87 master/키인/이름 추정 모두 실패',
        },
        'master_partner_count': len(master),
        'keyin_mapping_count': len(keyin),
        'keyin_mappings': keyin,
        'unmappable_count': len(unmappable),
        'unmappable_mappings': unmappable,
        'master_top_nationality': {
            b: {'top': v['top'], 'distribution': v['distribution']}
            for b, v in sorted(master.items())
        },
        'unmapped_partners_top': unmapped[:100],         # 미확인 → 키인 보강 후보
        'unmappable_partners_list': unmappable_partners,  # 매핑불가 - 편집/해제 가능
        'keyin_mapped_partners_list': keyin_mapped_partners,  # 키인매핑된 - 변경 가능
        'recent_member_detail': member_detail,           # 최근 24개월 회원명 단위 상세 (캘린더 우측용)
        'daily_inbound_rn': daily,                       # 최근 24개월 일자별 인바운드 합계 (캘린더 좌측용)
        **agg,
    }
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f'저장: {OUTPUT_PATH}  ({OUTPUT_PATH.stat().st_size/1024:.1f} KB)')

    print_report(enriched, master, agg, unmapped)


if __name__ == '__main__':
    main()
