#!/usr/bin/env python3
"""
parse_package_trend.py — 온북 DB 패키지(회원번호=86XXXXXX) 분류별 실적 집계
- 회원번호가 86으로 시작하는 행 = 패키지 상품
- 회원명 = 패키지명 (채널/연도 접두사 제거 → 상품계열명 도출)
출력: data/package_series_trend.json
"""
import os, json, re, logging
import fs_utils  # macOS NFD→NFC 유니코드 정규화
from pathlib import Path
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

# raw_db는 메인 프로젝트에 있음 (워크트리 3단계 위).
# 워크트리에 빈 data/raw_db가 있을 수 있으므로, 연도 서브디렉토리가 있는 경로 우선.
def find_raw_db():
    candidates = [
        PROJECT_DIR / "data" / "raw_db",
        PROJECT_DIR.parents[2] / "data" / "raw_db",  # worktree 3-level up
    ]
    for c in candidates:
        if (c / "2026").exists() or (c / "2025").exists():
            return c
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError(f"raw_db 디렉토리를 찾을 수 없음: {candidates}")

RAW_DB_DIR = find_raw_db()
OUTPUT_FILE = PROJECT_DIR / "data" / "package_series_trend.json"
TOP_SERIES_LIMIT = 100

# 채널/판매처 접미사 토큰 (길이 내림차순 정렬 → 긴 것 먼저 매칭)
CHANNEL_TOKENS = sorted([
    'D멤버스', 'GOTA', 'OTA', '네이버', '컨택', '컨텍', '비회원', '예약센터',
    '온라인', '홈페이지', '직불', '후불', '글로벌', '기명', '비기명',
    '고객사', '직판', '대리점', '회원', 'B2B', '쿠팡', '야놀자', '아고다',
    '여기어때', '트립닷컴', '트립비토즈', '익스피디아', '부킹닷컴', '부킹',
    '인터파크', '티몬', '위메프', '소노휴양평', '본사후불', '현장결제',
    'HP', 'CC',
], key=len, reverse=True)

_CH_PAT = '|'.join(map(re.escape, CHANNEL_TOKENS))
# 언더바/슬래시 + 채널토큰 + 나머지를 문자열 끝까지
_SUFFIX_RE = re.compile(
    rf'\s*[_/\s]\s*(?:{_CH_PAT})\b.*$',
    flags=re.IGNORECASE
)
# 괄호 안 채널: (OTA), (회원/홈페이지), (후불), (직불)
_PAREN_CH_RE = re.compile(
    rf'\s*\(\s*(?:{_CH_PAT}|회원[^)]*|후불|직불)\s*\)\s*$',
    flags=re.IGNORECASE
)
# 야간 박수 표기: (3박), (3박_HP)
_NIGHT_RE = re.compile(r'\s*\(\d+박[^)]*\)')
# 연도+사업장 접두사: "23쏠양", "19_델피노_", "21_단양_"
_YEAR_PREFIX_RE = re.compile(r'^\d{2}[_가-힣]{0,15}')
# G-OTA/ 접두사
_GOTA_RE = re.compile(r'^G-OTA/', re.IGNORECASE)
# 대괄호 접두사: [펫_비발디], [경주]
_BRACKET_RE = re.compile(r'^\[[^\]]+\]\s*')
# 해시태그 블록: #변산#, #RO#, #B# 등
_HASH_TAG_RE = re.compile(r'#[^#]+#')

# ── 사업장/리조트 식별자 (길이 내림차순 → 긴 것 먼저 매칭) ──
_SITE_WORDS = [
    '소노캄거제', '소노캄고양', '소노캄 고양', '소노캄 거제',
    '소노벨변산', '소노벨 변산', '소노벨천안', '소노벨 천안',
    '쏠비치 양양', '쏠비치양양', '쏠비치 삼척', '쏠비치삼척', '쏠비치 진도', '쏠비치진도',
    '소노캄', '캄여수', '캄고양', '캄거제', '벨제주', '벨변산', '벨천안', '벨거제',
    '델피노', '비발디', '쏠비치',
    '단양', '진도', '삼척', '양양', '고양', '천안', '거제', '여수', '제주', '변산',
]
# 괄호 안 사업장/약어 + 독립 사업장 + 대매점 + 연도토큰 → 단일 정규식 (성능 최적화)
_SITE_ALL_PAT = '|'.join(re.escape(w) for w in sorted(_SITE_WORDS, key=len, reverse=True))
_CLEANUP_RE = re.compile(
    r'\s*\(\s*(?:단양|델|대매|대|O|Step\s*\d*)\s*\)'    # 괄호 사업장
    r'|(?:' + _SITE_ALL_PAT + r')'                       # 독립 사업장 단어
    r'|\b대매점?\b'                                       # 대매점
    r'|\b\d{2,4}Y\b|\b\d{2}/\d{2}\b|\b\d{2,4}년\b'      # 연도토큰
    r'|\s+일반\s*$',                                      # trailing 일반
    flags=re.IGNORECASE
)


_normalize_cache: dict[str, str] = {}

def normalize_series(name: str) -> str:
    """패키지명(회원명) → 상품계열명 (채널·연도·사업장·박수 제거) — 캐싱"""
    cached = _normalize_cache.get(name)
    if cached is not None:
        return cached
    result = _normalize_series_impl(name)
    _normalize_cache[name] = result
    return result

def _normalize_series_impl(name: str) -> str:
    if not name:
        return '기타'
    # 1) 해시태그 블록 먼저, 그 다음 대괄호 제거
    name = _HASH_TAG_RE.sub('', name)
    name = _BRACKET_RE.sub('', name)
    # 2) G-OTA/ 접두사 제거
    name = _GOTA_RE.sub('', name)
    # 3) 연도+사업장 접두사 제거
    name = _YEAR_PREFIX_RE.sub('', name)
    # 4) 괄호 안 채널 제거
    name = _PAREN_CH_RE.sub('', name)
    # 5) 야간 박수 표기 제거
    name = _NIGHT_RE.sub('', name)
    # 6) 채널 접미사 제거
    name = _SUFFIX_RE.sub('', name)
    # 7) 통합 정리: 사업장·대매점·연도토큰 한방에 제거
    name = _CLEANUP_RE.sub(' ', name)
    # 8) 정리
    name = re.sub(r'^[\s_/]+|[\s_/]+$', '', name)
    name = re.sub(r'_+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name if name else '기타'


_OTA_NAMES = [
    '야놀자', '아고다', '여기어때', '트립닷컴', '부킹닷컴', '부킹', '쿠팡',
    '인터파크', '트립비토즈', '익스피디아', '네이버', '위메프', '티몬',
    '호텔스닷컴', '11번가', '프리즘', '마이리얼트립', '온라인콘도',
]

def classify_v4(series_name: str) -> str:
    """상품계열명 → 패키지 분류 카테고리 v4 (9개 카테고리)
    1. 룸온니/프로모션  — 룸온리, 룸온니, R/O, OTA거래처, 일반 프로모션
    2. 연박/투나잇      — 2Nights, Hours Stay, 스마트초이스
    3. 올인클루시브      — All Inclusive
    4. 조식패키지       — Tasty Morning, 조식
    5. 세일/기획전      — 얼리버드, 빅세일, 기획전, 브랜드데이, 멤버스데이
    6. 워터풀/오션      — Waterful, Simple Ocean
    7. 시즌패키지       — 동계/윈터 + 썸머 + 스프링/가을 통합
    8. 액티비티/레저     — 온천, BBQ, 투어, 가족
    9. 기타
    """
    s = series_name.upper()

    # 룸온니/프로모션: 룸온리/룸온니/R/O 명시 (OTA보다 우선)
    if any(k in series_name for k in ['룸온리', '룸온니', 'R/O', 'RO']):
        return '룸온니/프로모션'
    if 'ROOM ONLY' in s:
        return '룸온니/프로모션'

    # 룸온니/프로모션: OTA 거래처명이 시리즈명
    for ota in _OTA_NAMES:
        if ota in series_name:
            return '룸온니/프로모션'
    if re.search(r'\bOTA\b|GOTA', s):
        return '룸온니/프로모션'

    # 연박/투나잇
    if any(k in s for k in ['2 NIGHTS', '2NIGHTS', '2NIGHT', '2나잇', '연박', 'PRIVILEGE',
                             'HOURS STAY', 'HOUR STAY', '스마트초이스', '스마트 초이스']):
        return '연박/투나잇'
    if re.search(r'\d박\s*(PKG|스테이)', s):
        return '연박/투나잇'

    # 올인클루시브
    if any(k in s for k in ['ALL INCLUSIVE', 'ALLINCLUSIVE', '올인클루시브', '올클',
                             '썸머클루시브']):
        return '올인클루시브'

    # 조식패키지
    if any(k in s for k in ['TASTY MORNING', '조식', 'MORNING', 'BREAKFAST']):
        return '조식패키지'

    # 세일/기획전 (+ 브랜드/멤버스데이 통합)
    if any(k in s for k in ['얼리버드', '얼리바캉스', '얼리 바캉스', '빅세일', '기획전', '숙박세일', '세일페스타',
                             '13%', '당일특가', 'LATE HOLIDAY', '레이트 홀리데이', '특가', 'STEP2',
                             '브랜드데이', '멤버스데이', '소노브랜드', '숙박대전']):
        return '세일/기획전'
    if '세일' in series_name:
        return '세일/기획전'

    # 워터풀/오션
    if any(k in s for k in ['WATERFUL', 'WATER-FUL', '워터풀', '오션에빠지다',
                             'BLUE COAST', 'BLUECOAST', 'SIMPLE OCEAN']):
        return '워터풀/오션'
    if '오션' in series_name:
        return '워터풀/오션'

    # 시즌패키지 (동계/윈터 + 썸머 + 스프링/가을 통합)
    if any(k in s for k in ['동계', '스키', 'WINTER', '윈터', '스노위', 'SNOWY', '겨울',
                             '썸머', '블루데이즈', '바캉스', '베케이션', 'SUMMER', 'VACATION', 'BLUEDAYS', '러브썸',
                             '스프링', '블룸', '추석', 'SPRING', '가을', '단풍', 'BLOOM']):
        return '시즌패키지'

    # 액티비티/레저
    if any(k in s for k in ['온천', 'BBQ', '케이펫', '레저', '낚시', 'SPA', 'MOMENTS',
                             '투어 패키지', '투어패키지', '가족5', '가족 5', '투어']):
        return '액티비티/레저'

    # 프로모션 (일반 프로모션 = 룸온니 계열)
    if '프로모션' in series_name:
        return '룸온니/프로모션'

    return '기타'


# ─── 연박 보강 분류 (박수 / 패키지분류코드명 / 연속 OTA 체인) ───
# 다박(2박+) 승격이 허용되는 "누수" 베이스 카테고리.
# 올인클루시브·조식·워터풀·시즌·액티비티는 박수와 무관하게 상품 정체성을 유지.
_PROMOTE_FROM = {'기타', '룸온니/프로모션', '세일/기획전'}


def classify_v4_ctx(series_name, nights=0, pkg_class_name='', is_ota_chain=False):
    """문맥(박수·패키지분류코드명·연속 OTA)을 반영한 분류.

    우선순위:
      1) 패키지분류코드명에 '연박' → 연박/투나잇 (명시적 연박 프로모션, Blue Coast 연박 등)
      2) 이름기반 분류(base)가 누수버킷(기타/룸온니/세일)이고
         박수>=2 또는 연속 OTA 체인 → 연박/투나잇
      3) 그 외(올인·조식·워터풀·시즌·액티비티)는 base 유지
    """
    if pkg_class_name and '연박' in pkg_class_name:
        return '연박/투나잇'
    base = classify_v4(series_name)
    if base in _PROMOTE_FROM:
        try:
            n = int(str(nights).strip() or 0)
        except (TypeError, ValueError):
            n = 0
        if n >= 2 or is_ota_chain:
            return '연박/투나잇'
    return base


_chain_cache: dict = {}


def ota_chain_nights(raw_db_dir, years):
    """OTA(변경예약집계코드 53/72) 패키지86 '1박' 행 중 연속숙박 체인에 속한 '밤'을 식별.

    OTA는 다박 예약을 1박씩 쪼개 별도 KEY_RSV_NO로 들여보낸다(같은 예약정보·연속 날짜).
    같은 (이용자명, 영업장명, 객실타입, 회원명) 그룹에서 한 행의 퇴실일자가
    다른 행의 입실일자와 일치하면(=연속) 두 밤 모두 연박으로 본다.

    반환: set of (이용자, 영업장, 객실타입, 회원명, 입실YYYYMMDD)
    예약(27/43)·취소(28/44) 행을 모두 포함해 픽업/취소 분류가 대칭이 되도록 함.
    """
    key = (str(raw_db_dir), tuple(sorted(str(y) for y in years)))
    if key in _chain_cache:
        return _chain_cache[key]
    groups = defaultdict(list)  # gid -> [(입실, 퇴실), ...]
    base = Path(raw_db_dir)
    for year in years:
        ydir = base / str(year)
        if not ydir.exists():
            continue
        for fname in sorted(os.listdir(ydir)):
            if not (fname.startswith('27.') or fname.startswith('43.')
                    or fname.startswith('28.') or fname.startswith('44.')):
                continue
            try:
                with open(ydir / fname, encoding='cp949', errors='replace') as f:
                    lines = f.read().splitlines()
            except Exception:
                continue
            if not lines:
                continue
            col = {h.strip(): i for i, h in enumerate(lines[0].split(';'))}
            i_mem = col.get('회원번호', 5); i_code = col.get('변경예약집계코드', 12)
            i_user = col.get('이용자명', 7); i_prop = col.get('영업장명', 3)
            i_rt = col.get('객실타입', 8); i_name = col.get('회원명', 6)
            i_n = col.get('박수', 27); i_in = col.get('입실일자', 33); i_out = col.get('퇴실일자', 34)
            need = max(i_mem, i_code, i_user, i_prop, i_rt, i_name, i_n, i_in, i_out)
            for line in lines[1:]:
                p = line.split(';')
                if len(p) <= need:
                    continue
                if not p[i_mem].strip().startswith('86'):
                    continue
                if p[i_code].strip() not in ('53', '72'):   # OTA만
                    continue
                if p[i_n].strip() != '1':                    # 1박 행만 (2박+ 는 박수규칙이 처리)
                    continue
                ci = p[i_in].strip()[:8]; co = p[i_out].strip()[:8]
                if len(ci) != 8 or len(co) != 8:
                    continue
                gid = (p[i_user].strip(), p[i_prop].strip(), p[i_rt].strip(), p[i_name].strip())
                groups[gid].append((ci, co))
    chain = set()
    for gid, items in groups.items():
        if len(items) < 2:
            continue
        checkins = {ci for ci, co in items}
        checkouts = {co for ci, co in items}
        for ci, co in set(items):
            # 이 밤의 퇴실이 같은 그룹의 다른 입실이거나(뒤로 연속),
            # 이 밤의 입실이 같은 그룹의 다른 퇴실이면(앞과 연속) 체인 소속.
            if co in checkins or ci in checkouts:
                chain.add((gid[0], gid[1], gid[2], gid[3], ci))
    _chain_cache[key] = chain
    return chain


def parse_file(fpath: Path, is_cancel: bool, agg: dict, prop_agg: dict,
               cat_agg: dict = None, prop_cat_agg: dict = None, chain_set: set = None):
    """type 27/28 파일 파싱 → agg[시리즈][YYYYMM] 집계 + prop_agg[사업장][시리즈][YYYYMM] 집계.

    cat_agg/prop_cat_agg 가 주어지면 행 단위 문맥분류(박수·패키지분류·연속OTA)로
    카테고리 집계도 함께 누적한다(상품 구분 롤업용). 시리즈 집계는 이름기반 그대로 유지.
    """
    try:
        with open(fpath, encoding='cp949', errors='replace') as f:
            lines = f.readlines()
    except Exception as e:
        logger.warning(f"읽기 실패: {fpath.name} — {e}")
        return 0

    if not lines:
        return 0

    # 컬럼 인덱스 동적 추출
    headers = [h.strip() for h in lines[0].strip().split(';')]
    col = {h: i for i, h in enumerate(headers)}
    idx_mem = col.get('회원번호', 5)
    idx_mem_name = col.get('회원명', 6)
    idx_date = col.get('판매일자', 1)
    idx_rn = col.get('객실수', 28)
    idx_rate = col.get('1박객실료', 26)
    idx_seg_code = col.get('변경예약집계코드')
    idx_prop = col.get('변경사업장명', 4)  # 사업장명 추가
    # 문맥분류용 컬럼
    idx_nights = col.get('박수', 27)
    idx_pkgcls = col.get('패키지분류코드명', 56)
    idx_prop3 = col.get('영업장명', 3)      # 체인 gid 매칭은 영업장명(원본) 기준
    idx_rt = col.get('객실타입', 8)
    idx_user = col.get('이용자명', 7)
    idx_checkin = col.get('입실일자', 33)
    do_cat = cat_agg is not None and prop_cat_agg is not None
    chain_set = chain_set or set()

    # GS 채널만 집계 (OTA + G-OTA + Inbound)
    GS_CODES = {'A4', 'A5', '53', '72', '58'}

    def _new_month():
        return {'booking_rn': 0, 'booking_rev': 0, 'cancel_rn': 0, 'cancel_rev': 0}

    count = 0
    for line in lines[1:]:
        parts = line.rstrip('\n').split(';')
        if len(parts) <= max(idx_mem, idx_mem_name, idx_date, idx_rn, idx_rate):
            continue
        mem_no = parts[idx_mem].strip()
        if not mem_no.startswith('86'):
            continue

        # GS 채널 필터: 변경예약집계코드가 GS_CODES에 해당하는 행만 집계
        if idx_seg_code is not None:
            seg_code = parts[idx_seg_code].strip() if len(parts) > idx_seg_code else ''
            if seg_code not in GS_CODES:
                continue

        date_str = parts[idx_date].strip()
        if len(date_str) < 6:
            continue
        yyyymm = date_str[:6]

        try:
            rn = int(parts[idx_rn].strip() or 0)
            rate = int(parts[idx_rate].strip() or 0)
        except ValueError:
            continue

        if rn <= 0:
            rn = 1

        series = normalize_series(parts[idx_mem_name].strip())

        # 사업장명 추출
        prop_name = ''
        if idx_prop is not None and len(parts) > idx_prop:
            prop_name = parts[idx_prop].strip()
        prop_name = prop_name if prop_name else '미분류'

        # 전체 집계 (시리즈 기준 — 이름기반)
        m = agg[series][yyyymm]
        if is_cancel:
            m['cancel_rn'] += rn
            m['cancel_rev'] += rate
        else:
            m['booking_rn'] += rn
            m['booking_rev'] += rate

        # 사업장별 집계 (시리즈 기준)
        m_prop = prop_agg[prop_name][series][yyyymm]
        if is_cancel:
            m_prop['cancel_rn'] += rn
            m_prop['cancel_rev'] += rate
        else:
            m_prop['booking_rn'] += rn
            m_prop['booking_rev'] += rate

        # 카테고리 집계 (행 단위 문맥분류 — 박수/패키지분류/연속OTA 반영)
        if do_cat:
            nights = parts[idx_nights].strip() if 0 <= idx_nights < len(parts) else ''
            pkgcls = parts[idx_pkgcls].strip() if 0 <= idx_pkgcls < len(parts) else ''
            is_chain = False
            if (0 <= idx_user < len(parts) and 0 <= idx_prop3 < len(parts)
                    and 0 <= idx_rt < len(parts) and 0 <= idx_checkin < len(parts)):
                gid_key = (parts[idx_user].strip(), parts[idx_prop3].strip(),
                           parts[idx_rt].strip(), parts[idx_mem_name].strip(),
                           parts[idx_checkin].strip()[:8])
                is_chain = gid_key in chain_set
            cat = classify_v4_ctx(series, nights, pkgcls, is_chain)
            # 카테고리 net 클램핑은 (cat, series, month) 최소단위로 유지
            # (원본 by_category와 동일하게 시리즈별 max(0, booking-cancel) 후 합산).
            mc = cat_agg.setdefault(cat, {}).setdefault(series, defaultdict(_new_month))[yyyymm]
            mcp = (prop_cat_agg.setdefault(prop_name, {}).setdefault(cat, {})
                   .setdefault(series, defaultdict(_new_month))[yyyymm])
            if is_cancel:
                mc['cancel_rn'] += rn; mc['cancel_rev'] += rate
                mcp['cancel_rn'] += rn; mcp['cancel_rev'] += rate
            else:
                mc['booking_rn'] += rn; mc['booking_rev'] += rate
                mcp['booking_rn'] += rn; mcp['booking_rev'] += rate

        count += 1

    return count


def main():
    logger.info(f"raw_db 경로: {RAW_DB_DIR}")
    years = ['2022', '2023', '2024', '2025', '2026']

    # series → yyyymm → {booking_rn, booking_rev, cancel_rn, cancel_rev}
    def new_month():
        return {'booking_rn': 0, 'booking_rev': 0, 'cancel_rn': 0, 'cancel_rev': 0}
    agg = defaultdict(lambda: defaultdict(new_month))
    # prop_name → series → yyyymm → {booking_rn, booking_rev, cancel_rn, cancel_rev}
    prop_agg = defaultdict(lambda: defaultdict(lambda: defaultdict(new_month)))
    # 카테고리 롤업(행 단위 문맥분류): cat → yyyymm → {...}, prop → cat → yyyymm → {...}
    cat_agg = {}
    prop_cat_agg = {}

    # 연속 OTA 체인 사전계산 (박수=1 쪼개기 → 연박 식별)
    chain_set = ota_chain_nights(RAW_DB_DIR, years)
    logger.info(f"연속 OTA 체인 밤 수: {len(chain_set):,}")

    total_rows = 0
    for year in years:
        year_dir = RAW_DB_DIR / year
        if not year_dir.exists():
            logger.warning(f"연도 디렉토리 없음: {year_dir}")
            continue
        for fname in sorted(os.listdir(year_dir)):
            if fname.startswith('27.'):
                n = parse_file(year_dir / fname, is_cancel=False, agg=agg, prop_agg=prop_agg,
                               cat_agg=cat_agg, prop_cat_agg=prop_cat_agg, chain_set=chain_set)
                logger.info(f"  {year}/{fname[:40]}: {n:,}행")
                total_rows += n
            elif fname.startswith('28.'):
                n = parse_file(year_dir / fname, is_cancel=True, agg=agg, prop_agg=prop_agg,
                               cat_agg=cat_agg, prop_cat_agg=prop_cat_agg, chain_set=chain_set)
                logger.info(f"  {year}/{fname[:40]} (취소): {n:,}행")
                total_rows += n

    logger.info(f"총 파싱 행 수: {total_rows:,}")

    # 카테고리별 연도 net (문맥분류 기준, 시리즈별 클램핑) — 헬퍼
    # series_months = {series: {yyyymm: {...}}}
    def _cat_year_net(series_months, year):
        rn = rev = 0
        for months in series_months.values():
            for ym, m in months.items():
                if ym.startswith(year):
                    rn += max(0, m['booking_rn'] - m['cancel_rn'])
                    rev += max(0, m['booking_rev'] - m['cancel_rev'])
        return rn, rev

    # 카테고리별 연도×월 net (시리즈별 클램핑 후 합산) — 헬퍼
    def _cat_by_year(series_months):
        by_yr = defaultdict(lambda: defaultdict(lambda: {'rn': 0, 'rev_won': 0}))
        for months in series_months.values():
            for yyyymm, m in months.items():
                slot = by_yr[yyyymm[:4]][yyyymm]
                slot['rn'] += max(0, m['booking_rn'] - m['cancel_rn'])
                slot['rev_won'] += max(0, m['booking_rev'] - m['cancel_rev'])
        out = {}
        for yr, mss in by_yr.items():
            out[yr] = {ym: {'rn': d['rn'], 'rev': round(d['rev_won'] / 1_000_000, 1),
                            'adr': round(d['rev_won'] / d['rn'] / 1000) if d['rn'] > 0 else 0}
                       for ym, d in sorted(mss.items())}
        return out

    # 시리즈별 합계
    series_totals = {}
    for series, months in agg.items():
        t_rn = sum(max(0, m['booking_rn'] - m['cancel_rn']) for m in months.values())
        t_rev = sum(max(0, m['booking_rev'] - m['cancel_rev']) for m in months.values())
        series_totals[series] = {
            'total_rn': t_rn,
            'total_rev': round(t_rev / 1_000_000, 1),
        }

    sorted_series = sorted(series_totals.items(), key=lambda x: x[1]['total_rn'], reverse=True)
    top_names = [s[0] for s in sorted_series[:TOP_SERIES_LIMIT]]
    logger.info(f"총 상품계열 수: {len(series_totals):,}, TOP {len(top_names)} 추출")

    # by_series: 시리즈별 연도×월 데이터
    by_series = {}
    for series in top_names:
        by_year = defaultdict(dict)
        for yyyymm in sorted(agg[series]):
            m = agg[series][yyyymm]
            net_rn = max(0, m['booking_rn'] - m['cancel_rn'])
            net_rev_won = max(0, m['booking_rev'] - m['cancel_rev'])
            net_rev_m = round(net_rev_won / 1_000_000, 1)
            adr = round(net_rev_won / net_rn / 1000) if net_rn > 0 else 0
            by_year[yyyymm[:4]][yyyymm] = {'rn': net_rn, 'rev': net_rev_m, 'adr': adr}
        by_series[series] = {yr: dict(months) for yr, months in by_year.items()}

    # by_year_ranking: 연도별 TOP 20 (카테고리 기준 — 행단위 문맥분류, 시리즈별 클램핑)
    by_year_ranking = {}
    for year in years:
        ranking = []
        for cat, series_months in cat_agg.items():
            yr_rn, yr_rev_won = _cat_year_net(series_months, year)
            if yr_rn > 0:
                ranking.append({
                    'name': cat, 'category': cat, 'rn': yr_rn,
                    'rev': round(yr_rev_won / 1_000_000, 1),
                    'adr': round(yr_rev_won / yr_rn / 1000) if yr_rn > 0 else 0,
                })
        ranking.sort(key=lambda x: x['rn'], reverse=True)
        by_year_ranking[year] = ranking[:20]

    # top_series에 category 필드 추가 (시리즈 단위 — 이름기반 유지)
    top_series_out = []
    for s in top_names:
        cat = classify_v4(s)
        top_series_out.append({'name': s, 'category': cat, **series_totals[s]})

    # by_category: 카테고리별 연도×월 집계 (행단위 문맥분류, 시리즈별 클램핑)
    by_category = {cat: _cat_by_year(series_months) for cat, series_months in cat_agg.items()}

    # by_property: 사업장별 카테고리 집계 (행단위 문맥분류, 시리즈별 클램핑)
    by_property = {}
    for prop_name, cats in prop_cat_agg.items():
        prop_by_category = {cat: _cat_by_year(series_months) for cat, series_months in cats.items()}
        prop_by_year_ranking = {}
        for year in years:
            ranking = []
            for cat, series_months in cats.items():
                yr_rn, yr_rev_won = _cat_year_net(series_months, year)
                if yr_rn > 0:
                    ranking.append({
                        'name': cat, 'category': cat, 'rn': yr_rn,
                        'rev': round(yr_rev_won / 1_000_000, 1),
                        'adr': round(yr_rev_won / yr_rn / 1000) if yr_rn > 0 else 0,
                    })
            ranking.sort(key=lambda x: x['rn'], reverse=True)
            prop_by_year_ranking[year] = ranking[:20]

        by_property[prop_name] = {
            'by_category': prop_by_category,
            'by_year_ranking': prop_by_year_ranking,
        }

    output = {
        'top_series': top_series_out,
        'by_series': by_series,
        'by_category': by_category,
        'by_year_ranking': by_year_ranking,
        'by_property': by_property,
        'meta': {
            'years': years,
            'total_series': len(series_totals),
            'top_series_count': len(top_names),
            'total_parsed_rows': total_rows,
        },
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info(f"✓ 저장: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
