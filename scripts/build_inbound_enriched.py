#!/usr/bin/env python3
"""
build_inbound_enriched.py — Inbound (변경예약집계코드=58) 거래처별 국적 매핑

87-prefix 회원번호 = 신코드, 회원명에 '거래처(국적)' 형식
57-prefix 회원번호 = 구코드, 회원명에 거래처명만 (국적 없음)

처리:
1. 43_/44_ 파일에서 인바운드(58) 레코드 추출 (booking + cancel)
2. 87 마스터: 거래처명 → {국적: count}, 가장 많은 국적이 대표 국적
3. 57 매핑:
   - 회원명에 (국적) 있으면 그대로 사용 (confidence='57원본')
   - 87 master에 base 거래처명이 있으면 master 국적 (confidence='87매핑')
   - 그 외 → '미확인' (confidence='미확인')
4. docs/data/inbound_enriched.json 저장: 월별/거래처별 국적 통계 + 매핑 신뢰도

매출(REV) = 1박객실료 × 객실수 ÷ 1.1 (BI Power Query 기준 VAT 제외)
RN = 객실수 (각 행이 1박 단위)
"""
import os, re, json, sys, logging
from pathlib import Path
from collections import defaultdict, Counter

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
RAW_DB_DIR = PROJECT_DIR / 'data' / 'raw_db' / '2026'
OUTPUT_PATH = PROJECT_DIR / 'docs' / 'data' / 'inbound_enriched.json'
KEYIN_PATH = PROJECT_DIR / 'docs' / 'data' / 'inbound_partner_nationality_keyin.json'

UNMAPPABLE_PARTNERS = {'티케이트래블', '원더트립', '에스에이투어', '코리얼트립'}


def load_keyin_mappings():
    """관리자 키인 거래처→국적 매핑 로드. 없으면 빈 dict."""
    if not KEYIN_PATH.exists():
        return {}
    try:
        with open(KEYIN_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        m = data.get('mappings', {}) or {}
        # 정규화: 빈 문자열 제외
        return {k.strip(): normalize_nationality(v) for k, v in m.items()
                if k and v and v.strip()}
    except Exception as e:
        logger.warning(f'키인 파일 로드 실패: {e}')
        return {}

# 국적 표기 정규화: '대만법인'/'대만FIT' → '대만' 등
NATIONALITY_NORMALIZE = {
    '대만법인': '대만',
    '대만FIT': '대만',
    '말레이FIT': '말레이시아',
    '말레이': '말레이시아',
    '싱가폴': '싱가포르',
    '대체건': None,
    '답사건': None,
    '노쇼대체': None,
}

PFX_RE = re.compile(r'^[#*\s]+')
PAREN_RE = re.compile(r'\(([^)]+)\)')

# 43 (예약): KEY_RSV_NO 포함, 컬럼 인덱스 다름
FILES = [
    {
        'path': RAW_DB_DIR / '43.[57,87]온라인영업팀(대매점,OTA,패키지) 예약자료_재전송(20260101-20260331).txt',
        'btype': 'booking',
        'idx_sell': 1, 'idx_prop': 3, 'idx_member_no': 5, 'idx_member_name': 6,
        'idx_user': 7, 'idx_code_num': 12, 'idx_night_rate': 26, 'idx_rooms': 28,
    },
    {
        'path': RAW_DB_DIR / '43.[57,87]온라인영업팀(대매점,OTA,패키지) 예약자료_20260503_생성시간(202605030751).txt',
        'btype': 'booking',
        'idx_sell': 1, 'idx_prop': 3, 'idx_member_no': 5, 'idx_member_name': 6,
        'idx_user': 7, 'idx_code_num': 12, 'idx_night_rate': 26, 'idx_rooms': 28,
    },
    {
        'path': RAW_DB_DIR / '44.[57,87]온라인영업팀(대매점,OTA,패키지) 취소자료_재전송(20260101-20260331).txt',
        'btype': 'cancel',
        'idx_sell': 0, 'idx_prop': 3, 'idx_member_no': 4, 'idx_member_name': 5,
        'idx_user': 6, 'idx_code_num': 11, 'idx_night_rate': 23, 'idx_rooms': 25,
    },
    {
        'path': RAW_DB_DIR / '44.[57,87]온라인영업팀(대매점,OTA,패키지) 취소자료_20260503_생성시간(202605030752).txt',
        'btype': 'cancel',
        'idx_sell': 0, 'idx_prop': 3, 'idx_member_no': 4, 'idx_member_name': 5,
        'idx_user': 6, 'idx_code_num': 11, 'idx_night_rate': 23, 'idx_rooms': 25,
    },
]


def normalize_nationality(nat):
    if not nat:
        return None
    n = nat.strip()
    if n in NATIONALITY_NORMALIZE:
        return NATIONALITY_NORMALIZE[n]
    return n


def base_partner(member_name):
    """거래처 base 이름 추출: 선두 #/*/공백 제거, 첫 '(' 앞부분, 끝의 -NNN 등 정리."""
    if not member_name:
        return ''
    s = PFX_RE.sub('', member_name).strip()
    if '(' in s:
        s = s.split('(', 1)[0].rstrip()
    # 'TK트래블-Viva Festival' 같은 캠페인 suffix는 보존하되, 순수 숫자 suffix만 제거
    s = re.sub(r'[\-_]\d{2,}\s*$', '', s).strip()
    # 끝의 '_A','_B' 또는 ' A' (단일 알파벳)
    s = re.sub(r'[\s_]+[A-Za-z]{1,2}\s*$', '', s).strip()
    return s


def extract_country_from_name(member_name):
    """회원명에서 마지막 () 안 텍스트 = 국적 (정규화 적용)."""
    matches = PAREN_RE.findall(member_name)
    if not matches:
        return None
    return normalize_nationality(matches[-1])


def parse_int(v):
    try:
        return int((v or '').strip())
    except (ValueError, TypeError):
        return 0


def iter_inbound_rows():
    """각 파일에서 inbound(58) 레코드만 yield. (line hash로 within-file 중복 제거)"""
    for spec in FILES:
        fp = spec['path']
        if not fp.exists():
            logger.warning(f'파일 없음: {fp}')
            continue
        seen = set()
        n_total = 0
        n_emitted = 0
        with open(fp, 'r', encoding='cp949', errors='replace') as f:
            f.readline()  # header
            for line in f:
                n_total += 1
                line_stripped = line.rstrip('\n\r')
                h = hash(line_stripped)
                if h in seen:
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
                    'sell_date': sell_date[:8],
                    'rn': rn,
                    'rev': rev,
                }
        logger.info(f'  {fp.name[:40]}... → {n_total:,}행 → 인바운드 {n_emitted:,}행')


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
        raw_dist[b][nat] += r['rn']  # RN 가중치로 대표 국적 결정 (행 수보다 RN이 정확)

    master = {}
    for b, dist in raw_dist.items():
        # 가장 RN 많은 국적
        top = dist.most_common(1)[0][0]
        master[b] = {
            'top': top,
            'distribution': dict(dist),
        }
    return master


def apply_mapping(rows, master, keyin=None):
    """각 row에 nationality + mapping_confidence 부여.
    keyin 매핑은 87원본/57원본보다 낮고 87매핑/미확인보다 높은 우선순위로
    적용 — 즉, 회원명에 (국적)이 직접 있으면 원본을 신뢰하되, 그 외엔 키인 우선.
    """
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
            elif b in UNMAPPABLE_PARTNERS:
                r['nationality'] = '미확인'
                r['mapping_confidence'] = '미확인'
            else:
                r['nationality'] = '미확인'
                r['mapping_confidence'] = '미확인'
        r['base_partner'] = b
        enriched.append(r)
    return enriched


def aggregate(enriched):
    """월별 국적별 / 거래처별 국적별 / 신뢰도별 요약."""
    monthly = defaultdict(lambda: defaultdict(lambda: {'rn_booking': 0, 'rev_booking': 0,
                                                       'rn_cancel': 0, 'rev_cancel': 0}))
    by_partner = defaultdict(lambda: defaultdict(lambda: {'rn_booking': 0, 'rev_booking': 0,
                                                          'rn_cancel': 0, 'rev_cancel': 0,
                                                          'prefix': set()}))
    confidence_summary = defaultdict(lambda: {'rows': 0, 'rn_booking': 0, 'rn_cancel': 0,
                                               'rev_booking': 0, 'rev_cancel': 0})

    for r in enriched:
        m = r['stay_month']
        nat = r['nationality']
        b = r['base_partner'] or '(미상)'
        key_b = b
        bt = r['btype']
        if bt == 'booking':
            monthly[m][nat]['rn_booking'] += r['rn']
            monthly[m][nat]['rev_booking'] += r['rev']
            by_partner[key_b][nat]['rn_booking'] += r['rn']
            by_partner[key_b][nat]['rev_booking'] += r['rev']
        else:
            monthly[m][nat]['rn_cancel'] += r['rn']
            monthly[m][nat]['rev_cancel'] += r['rev']
            by_partner[key_b][nat]['rn_cancel'] += r['rn']
            by_partner[key_b][nat]['rev_cancel'] += r['rev']
        by_partner[key_b][nat]['prefix'].add(r['prefix'])

        c = r['mapping_confidence']
        confidence_summary[c]['rows'] += 1
        if bt == 'booking':
            confidence_summary[c]['rn_booking'] += r['rn']
            confidence_summary[c]['rev_booking'] += r['rev']
        else:
            confidence_summary[c]['rn_cancel'] += r['rn']
            confidence_summary[c]['rev_cancel'] += r['rev']

    # net 컬럼 계산
    monthly_out = {}
    for m, by_nat in monthly.items():
        monthly_out[m] = {}
        for nat, v in by_nat.items():
            monthly_out[m][nat] = {
                **v,
                'rn_net': v['rn_booking'] - v['rn_cancel'],
                'rev_net': v['rev_booking'] - v['rev_cancel'],
            }
    by_partner_out = {}
    for b, by_nat in by_partner.items():
        by_partner_out[b] = {}
        for nat, v in by_nat.items():
            v_clean = {k: vv for k, vv in v.items() if k != 'prefix'}
            by_partner_out[b][nat] = {
                **v_clean,
                'rn_net': v['rn_booking'] - v['rn_cancel'],
                'rev_net': v['rev_booking'] - v['rev_cancel'],
                'prefix': sorted(v['prefix']),
            }
    return {
        'monthly_by_nationality': monthly_out,
        'by_partner_nationality': by_partner_out,
        'mapping_confidence_summary': dict(confidence_summary),
    }


def print_report(enriched, master, agg):
    rows = enriched
    n_total = len(rows)
    n_57 = sum(1 for r in rows if r['prefix'] == '57')
    n_87 = sum(1 for r in rows if r['prefix'] == '87')

    print('\n' + '=' * 70)
    print('인바운드(58) 거래처-국적 매핑 결과 요약')
    print('=' * 70)
    print(f'전체 inbound 레코드(행 단위): {n_total:,}  (57:{n_57:,}  87:{n_87:,})')

    # 매핑 신뢰도
    print('\n[매핑 신뢰도 분포]')
    print(f'{"신뢰도":<12}{"행 수":>10}{"booking RN":>12}{"cancel RN":>12}{"net RN":>10}')
    for k in ['87원본', '87매핑', '57원본', '키인매핑', '미확인']:
        v = agg['mapping_confidence_summary'].get(k, {'rows': 0, 'rn_booking': 0, 'rn_cancel': 0})
        net = v['rn_booking'] - v['rn_cancel']
        print(f'{k:<12}{v["rows"]:>10,}{v["rn_booking"]:>12,}{v["rn_cancel"]:>12,}{net:>10,}')

    # 87 master 통계
    print(f'\n[87-prefix 마스터] 고유 거래처: {len(master)}개')
    print('대표 국적별 분포:')
    by_nat = Counter(v['top'] for v in master.values())
    for nat, c in by_nat.most_common():
        print(f'  {nat:<10} {c:>4}개사')

    # 57-prefix 매핑 분해
    rows_57 = [r for r in rows if r['prefix'] == '57']
    by_conf_57 = Counter(r['mapping_confidence'] for r in rows_57)
    print(f'\n[57-prefix 매핑 분해] 총 {len(rows_57):,}행')
    for k, c in by_conf_57.most_common():
        print(f'  {k:<10} {c:>6,}행')

    # 미확인 거래처 Top
    unknown_rows = [r for r in rows_57 if r['nationality'] == '미확인']
    cnt = Counter(r['base_partner'] for r in unknown_rows)
    print(f'\n[57-prefix 미확인 거래처] (행 단위 top 10)')
    for b, c in cnt.most_common(10):
        print(f'  {c:>6,}  {b}')

    # 월별 RN (booking 기준)
    print('\n[월별 인바운드 booking RN]')
    print(f'{"월":<8}{"전체":>10}', end='')
    top_nats = [n for n, _ in Counter(
        nn for m in agg['monthly_by_nationality'].values() for nn, vv in m.items()
        for _ in range(vv["rn_booking"])
    ).most_common(8)]
    for n in top_nats:
        print(f'{n:>10}', end='')
    print()
    for m in sorted(agg['monthly_by_nationality'].keys()):
        per = agg['monthly_by_nationality'][m]
        total = sum(v['rn_booking'] for v in per.values())
        print(f'{m:<8}{total:>10,}', end='')
        for n in top_nats:
            v = per.get(n, {'rn_booking': 0})['rn_booking']
            print(f'{v:>10,}', end='')
        print()
    print('=' * 70)


def main():
    rows = list(iter_inbound_rows())
    logger.info(f'전체 inbound 레코드: {len(rows):,}행')
    master = build_master(rows)
    logger.info(f'87 master 거래처: {len(master)}개')
    keyin = load_keyin_mappings()
    if keyin:
        logger.info(f'키인 매핑: {len(keyin)}개 ({", ".join(f"{k}→{v}" for k, v in keyin.items())})')
    enriched = apply_mapping(rows, master, keyin)
    agg = aggregate(enriched)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'generated_at': '2026-05-04',
        'description': 'Inbound(변경예약집계코드=58) 거래처-국적 매핑 enriched 데이터',
        'mapping_rule': {
            '87원본': '87-prefix 회원명에 (국적) 직접 표기',
            '87매핑': '57-prefix 거래처명을 87 master에서 lookup',
            '57원본': '57-prefix 회원명에 (국적) 직접 표기',
            '키인매핑': '관리자 키인 매핑 (inbound_partner_nationality_keyin.json)',
            '미확인': '매핑 불가 (티케이트래블/원더트립 등)',
        },
        'master_partner_count': len(master),
        'keyin_mapping_count': len(keyin),
        'keyin_mappings': keyin,
        'master_top_nationality': {
            b: {'top': v['top'], 'distribution': v['distribution']}
            for b, v in sorted(master.items())
        },
        **agg,
    }
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f'저장: {OUTPUT_PATH}')

    print_report(enriched, master, agg)


if __name__ == '__main__':
    main()
