"""시트 일정 행(코드 없음) ↔ raw 상품코드 추정 매칭 — 반복 정제(v3).

0) AP 연결: 같은 시트의 코드 있는 Action Plan 행과 같은 기획전(사업장·시작일 ±3d·인플루언서/채널/상품 토큰)이면
   그 행의 코드를 그대로 확정(conf='ap'). 추정 아님.
1~4) 추정 매칭(점수):
  채널명      : 이벤트 채널/채널카테고리 키워드가 코드 회원명에 있음 +3 / 코드 AGENT명과 일치 +2 / 학습 토큰 +2
  인플루언서명: 채널 텍스트 괄호 안 이름(한브로·제이드아이…)이 코드 회원명에 있음 +4, AGENT에 있음 +2
  타채널 페널티: 코드 회원명에 이벤트 채널이 아닌 다른 채널 토큰(쿠팡·11번가·네이버…)이 있으면 −3
  구성        : 이벤트 '상품'의 구성(조식→식음, 오션→아쿠아 …) ↔ 코드 PKG 별도매출 구성 정합 +2/일치 −1/부재 (최대 +4)
  패키지명    : 상품 토큰이 코드 회원명에 포함 +1/개(최대 2)
  판매일자    : 코드 최초판매일 ∈ [판매시작−7d, min(판매종료, 시작+30d)+10d] 필수 · ±3d +1 · ±1d +2
  판매집중    : 코드 판매(최초입력)의 70%↑가 이벤트 판매기간(+3d) 안 +2 / 40%↑ +1 · 상시코드(판매기간 60일↑)가 단기 딜(≤14일)에 붙으면 −2
  투숙기간    : 코드 투숙기간(p05~p95)과 이벤트 투숙기간 겹침 +1
  객실판매가  : 이벤트 텍스트 가격(≥20,000원)과 코드 평균 패키지가(예약당, VAT포함) ±15% +2 / ±30% +1
  세그        : GOTA류 채널 ↔ G-OTA, 그 외 ↔ OTA +1
  '협의중'/상품 미기재 행은 채널명·AGENT·인플루언서명 중 하나는 있어야 매칭.
패스1 엄격 → 패스2 매칭쌍에서 채널→토큰/AGENT 학습 후 미매칭 재채점 → 패스3 기간 완화(−14d/+20d) → 패스4 충돌 정리.
신뢰도: ap(확정) · high ≥9 · medium 6~8 · low 4~5. 대외비·관리자용.
"""
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta

PROP_ORDER = ['01.벨비발디', '02.캄비발디', '03.펫비발디', '04.펠리체비발디', '05.빌리지비발디', '06.양평', '07.델피노', '08.쏠비치양양',
              '09.쏠비치삼척', '10.소노벨단양', '11.소노캄경주', '12.소노벨청송', '13.소노벨천안', '14.소노벨변산', '15.소노캄여수',
              '16.소노캄거제', '17.쏠비치진도', '18.소노벨제주', '19.소노캄제주', '20.소노캄고양', '21.소노문해운대', '22.쏠비치남해', '23.르네블루']
VIVALDI = {'01.벨비발디', '02.캄비발디', '03.펫비발디', '04.펠리체비발디', '05.빌리지비발디'}
_SITE_RULES = [('빌리지', '05.빌리지비발디'), ('펠리체', '04.펠리체비발디'), ('펠', '04.펠리체비발디'), ('펫', '03.펫비발디'),
               ('양평', '06.양평'), ('델피노', '07.델피노'), ('델', '07.델피노'), ('양양', '08.쏠비치양양'), ('삼척', '09.쏠비치삼척'),
               ('단양', '10.소노벨단양'), ('경주', '11.소노캄경주'), ('청송', '12.소노벨청송'), ('천안', '13.소노벨천안'),
               ('변산', '14.소노벨변산'), ('여수', '15.소노캄여수'), ('거제', '16.소노캄거제'), ('진도', '17.쏠비치진도'),
               ('고양', '20.소노캄고양'), ('해운대', '21.소노문해운대'), ('남해', '22.쏠비치남해'), ('르네', '23.르네블루')]


def event_props(site, biz=''):
    """이벤트 사업장/영업장 표기 → display 집합. '전체'→전 사업장, '비발디'(세부 없음)→비발디 5개."""
    for text in ((biz or ''), (site or '')):
        t = text.replace(' ', '')
        if not t:
            continue
        if '전체' in t or '전사업장' in t:
            return set(PROP_ORDER)
        out = set()
        if '제주' in t:   # '캄제주' / '벨제주' / '벨,캄 제주' / '제주'
            if '캄' in t:
                out.add('19.소노캄제주')
            if '벨' in t:
                out.add('18.소노벨제주')
            return out or {'18.소노벨제주', '19.소노캄제주'}
        for k, v in _SITE_RULES:
            if k in t:
                out.add(v)
        if '벨' in t:
            out.add('01.벨비발디')
        if '캄' in t:
            out.add('02.캄비발디')
        if '비발디' in t and not (out & VIVALDI):
            out |= VIVALDI
        if out:
            return out
    return set()


CH_KW = {'인플루언서': ['인플', '@'], '카카오예약/톡': ['카카오', '예약하기', '타이드', '톡예약'], '11번가': ['11번가', '11st'],
         '놀유니버스/놀이의발견': ['놀유', '야놀자', '놀이의발견', '놀이'], '티딜/톡딜': ['티딜', '톡딜', 'tdeal'],
         '마이리얼트립': ['마리트', '마이리얼', 'MRT', '마이트립'], '여기어때': ['여기어때', '여어', '여기'],
         '카카오메이커스': ['메이커스'], '쿠팡': ['쿠팡'], '카카오쇼핑라이브': ['쇼핑라이브', '카쇼라', '라이브'],
         '프리즘': ['프리즘', 'PRISM'], 'G마켓/옥션': ['G마켓', '지마켓', '옥션', 'GMARKET'], '트립비토즈': ['트립비토즈', '비토즈'],
         '아고다': ['아고다'], '네이버': ['네이버', 'NAVER'], '트립닷컴': ['트립닷컴'], '익스피디아': ['익스피디아'], '부킹닷컴': ['부킹'],
         '롯데온': ['롯데온', '롯데'], '여행사': ['하나투어', '하나', '모두투어', '웹투어'], '야놀자': ['야놀자', '놀유'], '맘맘': ['맘맘'],
         '와디즈': ['와디즈'], '티몬': ['티몬'], '위메프': ['위메프'], '인터파크': ['인터파크']}
CH_AGENT = {'쿠팡': ['쿠팡'], '놀유니버스/놀이의발견': ['놀유니버스', '야놀자', '놀이의발견', '웅진'], '여기어때': ['여기어때'],
            '카카오예약/톡': ['타이드스퀘어', '카카오'], '11번가': ['11번가', '타이드스퀘어'], '티딜/톡딜': ['티딜', '스마트인피니'],
            '마이리얼트립': ['마이리얼트립', '스마트인피니'], '인플루언서': ['스마트인피니', '올마이투어', '트립비토즈', '코이스토리', '종이비행기', '인플'],
            '카카오메이커스': ['올마이투어', '메이커스'], '카카오쇼핑라이브': ['스마트인피니', '카카오'], '프리즘': ['프리즘'],
            'G마켓/옥션': ['G마켓', '옥션', '지마켓', '종이비행기'], '네이버': ['네이버', '호텔패스'], '아고다': ['아고다'], '트립닷컴': ['트립닷컴'],
            '롯데온': ['롯데온', '스마트인피니'], '여행사': ['하나투어', '웹투어', '모두투어'], '야놀자': ['놀유니버스', '야놀자'], '트립비토즈': ['트립비토즈']}
# 타채널 페널티용 전체 채널 토큰(코드 회원명에 등장하는 표기)
ALL_CH_TOKENS = ['쿠팡', '11번가', '네이버', '메이커스', '티딜', '톡딜', '마리트', '마이리얼', '여기어때', '놀유', '야놀자', 'G마켓', '지마켓', '옥션',
                 '트립비토즈', '아고다', '트립닷컴', '익스피디아', '부킹', '프리즘', '카쇼라', '와디즈', '롯데온', '하나투어', '웹투어', '휴가샵', '예약하기',
                 '카카오', '라이브', '호텔패스', '티몬', '위메프', '인터파크', '하나']
CH_SYNONYM = {'카카오': ['카카오예약/톡', '카카오메이커스', '카카오쇼핑라이브'], '라이브': ['카카오쇼핑라이브', '여행사'], '하나': ['여행사'], '하나투어': ['여행사'],
              '웹투어': ['여행사'], '야놀자': ['놀유니버스/놀이의발견', '야놀자'], '놀유': ['놀유니버스/놀이의발견', '야놀자'], '휴가샵': ['놀유니버스/놀이의발견', '야놀자'],
              '예약하기': ['카카오예약/톡'], '메이커스': ['카카오메이커스'], '카쇼라': ['카카오쇼핑라이브'], '마리트': ['마이리얼트립'], '마이리얼': ['마이리얼트립'],
              '티딜': ['티딜/톡딜'], '톡딜': ['티딜/톡딜'], 'G마켓': ['G마켓/옥션'], '지마켓': ['G마켓/옥션'], '옥션': ['G마켓/옥션'], '놀이의발견': ['놀유니버스/놀이의발견']}
COMP_KW = {'식음': ['조식', '브런치', '런치', '디너', '바베큐', 'BBQ', '뷔페', '식사', '음료', '커피', '와인', '맥주', '케이크', '식음', '다이닝', '모닝', '웰컴티', '웰컴'],
           '아쿠아': ['오션', '워터', '아쿠아', '스노위', '스노우', '사우나', '찜질', '스파', '온천', '수영', '풀', '인피니티'],
           '스포츠': ['스키', '보드', '리프트', '골프', '요트', '히어로즈', '액티비티', '레저', '카트', '승마', '볼링', '글램핑', '캠핑', '서핑', '크루즈', '투어패스'],
           '부대': ['사우나', '스파', '온천', '찜질', '마사지', '피트니스', '키즈', '플레이', '입장권', '할인권', '라운지', '체험', '앤트월드'],
           '기타': ['굿즈', '선물', '인형', '브릭', '키트', '박스', '증정']}
GOTA_CH = ('아고다', '트립닷컴', '익스피디아', '부킹', '트립비토즈', 'gota', 'g-ota')
_STOP = {'객실', '패키지', 'pkg', '성인', '소인', '추가', '혜택', '구성', '협의중', '포함', '이용', '제공', '기준', '판매', '상품', '특전', '무료', 'or', '인'}
_AGENCY = {'스마트인피니', '종이비행기', '코이스토리', '올마이투어', '트립비토즈', '웹투어', '하나투어', '타이드스퀘어', '웅진컴퍼스', '호텔패스'}


def _tokens(text):
    return [w for w in re.split(r'[\s/+,()\[\]·×xX&\-_:;]+', (text or '').lower()) if len(w) >= 2 and w not in _STOP and not w.isdigit()]


def _paren_names(text):
    """채널 텍스트 괄호 안 이름들: '인플루언서xG마켓(한브로)' → ['한브로']; '(11번가,스마트인피니)' → ['11번가','스마트인피니']"""
    out = []
    for m in re.finditer(r'[(（]([^()（）]{2,24})[)）]', text or ''):
        for part in re.split(r'[,/·x×X&+]', m.group(1)):
            p = part.strip()
            if len(p) >= 2 and p not in _STOP:
                out.append(p)
    return out


def _prices(text):
    out = []
    for m in re.finditer(r'(\d{1,3}(?:,\d{3})+|\d{5,7})\s*원?', text or ''):
        try:
            v = int(m.group(1).replace(',', ''))
        except ValueError:
            continue
        if 20000 <= v <= 3000000:
            out.append(v)
    return out


def _d(s, fmt='%Y-%m-%d'):
    try:
        return datetime.strptime(s, fmt)
    except (ValueError, TypeError):
        return None


class CodeInfo:
    __slots__ = ('code', 'prop', 'name', 'name_l', 'seg', 'chan', 'fs', 'sale_days', 'sale_span', 'stay_lo', 'stay_hi', 'rn', 'pkg_price', 'parts', 'parts_set')

    def __init__(self, code, c):
        self.code = code; self.prop = c['prop']
        self.name = c['names'].most_common(1)[0][0]; self.name_l = self.name.lower()
        self.seg = c['seg_counts'].most_common(1)[0][0] if c['seg_counts'] else '기타'
        self.chan = c['chan_counts'].most_common(1)[0][0] if c['chan_counts'] else ''
        self.sale_days = {datetime.strptime(d, '%Y%m%d'): n for d, n in c['sale_days'].items() if d and len(d) == 8}
        sd = sorted(self.sale_days)
        self.fs = sd[0] if sd else None
        tot = sum(self.sale_days.values())
        if sd and tot:
            acc = 0; lo = sd[0]; hi = sd[-1]
            for d in sd:
                acc += self.sale_days[d]
                if acc >= tot * 0.05 and lo == sd[0]:
                    lo = d
                if acc >= tot * 0.95:
                    hi = d; break
            self.sale_span = (hi - lo).days
        else:
            self.sale_span = 0
        st = sorted(c['stay_days'])
        self.stay_lo = datetime.strptime(st[int(len(st) * 0.05)], '%Y%m%d') if st else None
        self.stay_hi = datetime.strptime(st[min(int(len(st) * 0.95), len(st) - 1)], '%Y%m%d') if st else None
        rn = rev = comp = bk = 0
        for node in c['by_ym'].values():
            for v in node['s'].values():
                rn += v[0]; rev += v[1]; comp += v[2]; bk += v[4]
        self.rn = rn
        self.pkg_price = ((rev + comp) * 1.1 / bk) if bk else 0.0
        ptot = sum(c['parts'].values()) or 0
        self.parts = {k: v / ptot for k, v in c['parts'].items()} if ptot else {}
        self.parts_set = {k for k, s in self.parts.items() if s >= 0.05}

    def sale_concentration(self, s0, s1):
        tot = sum(self.sale_days.values())
        if not tot:
            return 0.0
        return sum(n for d, n in self.sale_days.items() if s0 <= d <= s1) / tot


def _event_components(text):
    t = (text or '').lower()
    return {comp for comp, kws in COMP_KW.items() if any(k.lower() in t for k in kws)}


def _event_channel_tokens(ev):
    ch_cat = ev.get('채널카테고리') or ''
    ch_text = ev.get('채널') or ''
    kws = set(k.lower() for k in CH_KW.get(ch_cat, []))
    for w in _tokens(ch_text):
        kws.add(w)
    for tok, cats in CH_SYNONYM.items():
        if ch_cat in cats or tok.lower() in ch_text.lower():
            kws.add(tok.lower())
    return kws


def score_pair(ev, ci, learned_kw, learned_agent):
    """(점수, 근거 리스트). 날짜 창은 호출 측에서 필터."""
    score, why = 0, []
    ch_cat = ev.get('채널카테고리') or ''
    ch_text = (ev.get('채널') or '')
    own = _event_channel_tokens(ev) | set(k.lower() for k in learned_kw.get(ch_cat, []))
    names = _paren_names(ch_text)
    infl = [n for n in names if n not in _AGENCY and not any(c.lower() in n.lower() for c in ALL_CH_TOKENS)]
    agencies = [n for n in names if n in _AGENCY]
    if any(n.lower() in ci.name_l for n in infl):
        score += 4; why.append('인플명')
    if any(k and k.lower() in ci.name_l for k in own):
        score += 3; why.append('채널명')
    agents = [a.lower() for a in CH_AGENT.get(ch_cat, [])] + [a.lower() for a in learned_agent.get(ch_cat, [])] + [a.lower() for a in agencies] + [n.lower() for n in infl]
    if ci.chan and any(a in ci.chan.lower() for a in agents):
        score += 2; why.append('AGENT')
    # 타채널 토큰 페널티
    foreign = [t for t in ALL_CH_TOKENS if t.lower() in ci.name_l and t.lower() not in own and not any(t.lower() in o or o in t.lower() for o in own)]
    if foreign:
        score -= 3; why.append('타채널(' + foreign[0] + ')')
    exp = _event_components(ev.get('상품'))
    if exp:
        hits = len(exp & ci.parts_set); miss = len(exp - ci.parts_set)
        cs = max(0, min(4, 2 * hits - miss))
        if cs:
            score += cs; why.append(f'구성{hits}/{len(exp)}')
    elif not ci.parts_set and (ev.get('상품') or '').strip() and '협의' not in (ev.get('상품') or ''):
        score += 1; why.append('룸온리')
    toks = [t for t in _tokens(ev.get('상품')) if len(t) >= 2][:12]
    nh = sum(1 for t in toks if t in ci.name_l)
    if nh:
        score += min(nh, 2); why.append(f'명칭{min(nh, 2)}')
    s_start, s_end = _d(ev.get('판매시작')), _d(ev.get('판매종료') or ev.get('판매시작'))
    if s_start and ci.fs:
        dd = abs((ci.fs - s_start).days)
        if dd <= 1:
            score += 2; why.append('판매일±1')
        elif dd <= 3:
            score += 1; why.append('판매일±3')
    if s_start and s_end:
        conc = ci.sale_concentration(s_start - timedelta(days=1), s_end + timedelta(days=3))
        if conc >= 0.7:
            score += 2; why.append('판매집중70%')
        elif conc >= 0.4:
            score += 1; why.append('판매집중40%')
        if ci.sale_span >= 60 and (s_end - s_start).days <= 14:
            score -= 2; why.append('상시코드')
    st0, st1 = _d(ev.get('투숙시작')), _d(ev.get('투숙종료'))
    if st0 and st1 and ci.stay_lo and ci.stay_hi and ci.stay_lo <= st1 and ci.stay_hi >= st0:
        score += 1; why.append('투숙겹침')
    prices = _prices((ev.get('상품') or '') + ' ' + (ev.get('비고') or ''))
    if prices and ci.pkg_price:
        ratio = min(abs(p - ci.pkg_price) / ci.pkg_price for p in prices)
        if ratio <= 0.15:
            score += 2; why.append('가격±15%')
        elif ratio <= 0.30:
            score += 1; why.append('가격±30%')
    gota_like = any(g in (ch_text + ' ' + ch_cat).lower() for g in GOTA_CH)
    if (gota_like and ci.seg == 'G-OTA') or (not gota_like and ci.seg == 'OTA'):
        score += 1; why.append('세그')
    return score, why


def _conf(score):
    return 'high' if score >= 9 else 'medium' if score >= 6 else 'low' if score >= 4 else 'none'


def _link_ap(ev, ap_rows, ap_by_code):
    """이벤트 ↔ AP 코드 행 연결. 사업장 겹침 + 시작일 ±3d(또는 AP 시작일이 판매기간 안) + (인플명/상품/채널 토큰 ≥1)."""
    props = event_props(ev.get('사업장'), ev.get('영업장'))
    s_start, s_end = _d(ev.get('판매시작')), _d(ev.get('판매종료') or ev.get('판매시작'))
    if not s_start:
        return None
    names = [n.lower() for n in _paren_names(ev.get('채널') or '')]
    ev_tok = set(_tokens(ev.get('상품'))) | set(_tokens(ev.get('채널')))
    best = None
    for r in ap_rows:
        if not r.get('codes'):
            continue
        rp = event_props(r.get('사업장'), '')
        if not (rp & props):
            continue
        rs = _d((r.get('시작일') or '')[:10])
        if not rs or not (s_start - timedelta(days=3) <= rs <= (s_end or s_start) + timedelta(days=3)):
            continue
        infl = (r.get('인플루언서') or '').lower()
        r_tok = set(_tokens(r.get('상품'))) | set(_tokens(infl))
        sc = 0
        if infl and any(n and (n in infl or infl.replace('@', '') in n) for n in names):
            sc += 4
        ov = len(ev_tok & r_tok)
        sc += min(ov, 3)
        if rs == s_start:
            sc += 1
        if sc >= 2 and (best is None or sc > best[0]):
            best = (sc, r)
    return best[1] if best else None


def match_events(builder_codes, events, taken, ap_rows=None, ap_by_code=None, passes=4):
    """builder_codes = CubeBuilder.codes (finalize 전). 반환 (by_code, meta, event_records)."""
    ap_rows = ap_rows or []; ap_by_code = ap_by_code or {}
    infos = {}
    for code, c in builder_codes.items():
        if code in taken or not c['sale_days'] or not c['by_ym']:
            continue
        nm0 = c['names'].most_common(1)[0][0]
        if '매출조정' in nm0 or '조정' in nm0[:6]:
            continue
        ci = CodeInfo(code, c)
        if ci.fs:
            infos[code] = ci
    by_prop = defaultdict(list)
    for ci in infos.values():
        by_prop[ci.prop].append(ci)
    seen = Counter()
    events = [dict(e) for e in events]
    for e in events:
        k = str(e.get('key', ''))
        seen[k] += 1
        e['key'] = k if seen[k] == 1 else f'{k}#{seen[k]}'
    recs = {}
    for e in events:
        recs[e['key']] = {k: e.get(k, '') for k in ('key', '구분', '사업장', '영업장', '채널', '채널카테고리', '판매시작', '판매종료', '투숙시작', '투숙종료', '상품', '노출영역', 'KPI_RN', 'KPI_REV_M')}
        recs[e['key']].update({'props': sorted(event_props(e.get('사업장'), e.get('영업장'))), 'codes': [], 'conf': 'none', 'score': 0, 'why': [], 'pass': 0, 'campaign': None, 'alts': 0, 'ap_row': None})
    # 0) AP 행 연결(확정)
    linked = set()
    for e in events:
        r = _link_ap(e, ap_rows, ap_by_code)
        if r:
            rec = recs[e['key']]
            rec.update({'codes': sorted(str(c) for c in r['codes']), 'conf': 'ap', 'score': 99, 'why': ['AP행 연결'], 'pass': 0, 'ap_row': r.get('row'),
                        'campaign': ap_by_code.get(str(r['codes'][0])) if r['codes'] else None})
            linked.add(e['key'])
    claims = {}            # code → (score, event_key, why)
    learned_kw, learned_agent = defaultdict(list), defaultdict(list)

    def run_pass(pn_, before, after, min_score, only_unmatched):
        for e in events:
            rec = recs[e['key']]
            if e['key'] in linked or (only_unmatched and rec['codes']):
                continue
            s_start, s_end = _d(e.get('판매시작')), _d(e.get('판매종료') or e.get('판매시작'))
            if not s_start:
                rec['conf'] = 'bad_date'; continue
            lo, hi = s_start - timedelta(days=before), min(s_end or s_start, s_start + timedelta(days=30)) + timedelta(days=after)
            vague = ('협의' in (e.get('상품') or '')) or not (e.get('상품') or '').strip()
            cands = []
            for prop in rec['props']:
                for ci in by_prop.get(prop, []):
                    if not (lo <= ci.fs <= hi):
                        continue
                    sc, why = score_pair(e, ci, learned_kw, learned_agent)
                    if vague and not any(w in ('채널명', 'AGENT', '인플명') for w in why):
                        continue
                    if sc >= min_score:
                        cands.append((sc, ci, why))
            if not cands:
                continue
            best = max(c[0] for c in cands)
            chosen = [c for c in cands if c[0] >= max(min_score, best - 2)]
            got = []
            for sc, ci, why in chosen:
                prev = claims.get(ci.code)
                if prev and (prev[0] > sc or (prev[0] == sc and prev[1] != e['key'])):
                    continue
                claims[ci.code] = (sc, e['key'], why)
                got.append(ci.code)
            if got:
                rec.update({'codes': got, 'score': best, 'why': sorted({w for sc, ci, why in chosen for w in why}), 'pass': pn_, 'alts': len(cands) - len(chosen)})

    run_pass(1, 7, 10, 5, False)
    tok_cnt, ag_cnt = defaultdict(Counter), defaultdict(Counter)
    for code, (sc, ek, why) in claims.items():
        if sc < 6:
            continue
        ci = infos[code]; ch_cat = recs[ek].get('채널카테고리') or ''
        for t in _tokens(ci.name):
            if t not in [k.lower() for k in CH_KW.get(ch_cat, [])] and len(t) >= 2 and not any(t in k.lower() for kws in COMP_KW.values() for k in kws):
                tok_cnt[ch_cat][t] += 1
        if ci.chan:
            ag_cnt[ch_cat][ci.chan] += 1
    for ch_cat, cnt in tok_cnt.items():
        learned_kw[ch_cat] = [t for t, n in cnt.most_common(8) if n >= 2]
    for ch_cat, cnt in ag_cnt.items():
        learned_agent[ch_cat] = [a for a, n in cnt.most_common(5) if n >= 2]
    run_pass(2, 7, 10, 5, True)
    run_pass(3, 14, 20, 5, True)
    ev_codes = defaultdict(list)
    for code, (sc, ek, why) in claims.items():
        ev_codes[ek].append(code)
    by_code, meta = {}, {}
    for e in events:
        rec = recs[e['key']]
        if e['key'] in linked:
            continue
        rec['codes'] = sorted(ev_codes.get(e['key'], []))
        if not rec['codes']:
            rec['campaign'] = None
            if rec['conf'] != 'bad_date':
                rec['conf'] = 'none'
            continue
        rec['score'] = max(claims[c][0] for c in rec['codes'])
        rec['why'] = sorted({w for c in rec['codes'] for w in claims[c][2]})
        rec['conf'] = _conf(rec['score'])
        prod = (e.get('상품') or '').strip()
        line = re.sub(r'\s+', ' ', prod.splitlines()[0].strip()) if prod else '상품 미기재'
        ch_cat = e.get('채널카테고리') or (e.get('채널') or '')[:10]
        cname = f"SE{e['key']} {e.get('사업장', '')} {ch_cat} {line[:24]} ({(e.get('판매시작') or '')[5:].replace('-', '.')})"
        rec['campaign'] = cname
        for c in rec['codes']:
            by_code[c] = cname
        meta[cname] = {'year': int(e['판매시작'][:4]) if e.get('판매시작') else None, 'composition': prod,
                       'stay': f"{e.get('투숙시작', '')}~{e.get('투숙종료', '')}",
                       'sale': f"{(e.get('판매시작') or '')[2:].replace('-', '.')}~{(e.get('판매종료') or '')[2:].replace('-', '.')}",
                       'note': f"시트 일정 추정매칭 {rec['conf']}(점수 {rec['score']}, 패스{rec['pass']}) · {e.get('채널', '')}" + (f" · KPI {e.get('KPI_RN')}실" if e.get('KPI_RN') else ''),
                       'scope': e.get('사업장', ''), 'source': 'sheet_event', 'conf': rec['conf'], 'event_key': e['key']}
    return by_code, meta, [recs[e['key']] for e in events]
