#!/usr/bin/env python3
"""
build_weekly_comparison.py — 당일/금주 vs 전주/전년동기간 비교 데이터 생성

데이터 소스:
  data/db_aggregated.json
    - pickup_daily_by_property / pickup_daily_by_segment (예약파일 기준 들어온 예약)
    - cancel_daily_by_property / cancel_daily_by_segment (취소파일 기준 빠진 예약)
    → net = pickup - cancel  (당일 들어온 순예약)
  data/raw_db/{2025,2026}/  ← 패키지(회원번호=86) 행 → 상품카테고리 일별 집계
    (product-detail 페이지와 동일한 9개 카테고리)

출력: docs/data/weekly_comparison.json
- today: 당일(가장 최근 데이터일) 단일 일자 결과
- yesterday: 전일
- this_week / prev_week / ly_week: 7일 윈도우(또는 partial WTD) 합계
- by_property / by_segment / by_product_category: 각 차원별 3개 윈도우 비교
- insights: 자동 추출 인사이트
"""
from __future__ import annotations
import json, os, re, sys, logging
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
DOCS_DATA_DIR = PROJECT_DIR / "docs" / "data"

# Cowork worktree에서 실행 시 부모 저장소에 fallback
if not (DATA_DIR / "db_aggregated.json").exists():
    parent_root = PROJECT_DIR
    while parent_root.parent != parent_root:
        parent_root = parent_root.parent
        if (parent_root / "data" / "db_aggregated.json").exists():
            DATA_DIR = parent_root / "data"
            logger.info(f"  worktree 모드: 부모 데이터 사용 → {DATA_DIR}")
            break

INPUT_PATH = DATA_DIR / "db_aggregated.json"
OUTPUT_PATH = DOCS_DATA_DIR / "weekly_comparison.json"


def find_raw_db_dir() -> Path | None:
    """raw_db 디렉토리 위치 탐색 (worktree 모드 fallback 포함).

    빈 stub 디렉토리는 제외 — 연도 하위(20YY/) 가 하나라도 존재해야 유효."""
    candidates = [PROJECT_DIR / "data" / "raw_db"]
    p = PROJECT_DIR
    while p.parent != p:
        p = p.parent
        candidates.append(p / "data" / "raw_db")

    def has_year_subdir(d: Path) -> bool:
        if not (d.exists() and d.is_dir()):
            return False
        for sub in d.iterdir():
            if sub.is_dir() and re.fullmatch(r"20\d{2}", sub.name):
                return True
        return False

    for c in candidates:
        if has_year_subdir(c):
            return c
    return None


# ─── 상품카테고리 분류 (parse_package_trend.py classify_v4 + normalize_series) ───
# product-detail 페이지와 동일한 9개 카테고리: 룸온니/프로모션, 연박/투나잇,
# 올인클루시브, 조식패키지, 세일/기획전, 워터풀/오션, 시즌패키지,
# 액티비티/레저, 기타
_CHANNEL_TOKENS = sorted([
    'D멤버스', 'GOTA', 'OTA', '네이버', '컨택', '컨텍', '비회원', '예약센터',
    '온라인', '홈페이지', '직불', '후불', '글로벌', '기명', '비기명',
    '고객사', '직판', '대리점', '회원', 'B2B', '쿠팡', '야놀자', '아고다',
    '여기어때', '트립닷컴', '트립비토즈', '익스피디아', '부킹닷컴', '부킹',
    '인터파크', '티몬', '위메프', '소노휴양평', '본사후불', '현장결제',
    'HP', 'CC',
], key=len, reverse=True)
_CH_PAT = '|'.join(map(re.escape, _CHANNEL_TOKENS))
_SUFFIX_RE = re.compile(rf'\s*[_/\s]\s*(?:{_CH_PAT})\b.*$', re.IGNORECASE)
_PAREN_CH_RE = re.compile(rf'\s*\(\s*(?:{_CH_PAT}|회원[^)]*|후불|직불)\s*\)\s*$', re.IGNORECASE)
_NIGHT_RE = re.compile(r'\s*\(\d+박[^)]*\)')
_YEAR_PREFIX_RE = re.compile(r'^\d{2}[_가-힣]{0,15}')
_GOTA_RE = re.compile(r'^G-OTA/', re.IGNORECASE)
_BRACKET_RE = re.compile(r'^\[[^\]]+\]\s*')
_HASH_TAG_RE = re.compile(r'#[^#]+#')
_SITE_WORDS = [
    '소노캄거제', '소노캄고양', '소노캄 고양', '소노캄 거제',
    '소노벨변산', '소노벨 변산', '소노벨천안', '소노벨 천안',
    '쏠비치 양양', '쏠비치양양', '쏠비치 삼척', '쏠비치삼척', '쏠비치 진도', '쏠비치진도',
    '소노캄', '캄여수', '캄고양', '캄거제', '벨제주', '벨변산', '벨천안', '벨거제',
    '델피노', '비발디', '쏠비치',
    '단양', '진도', '삼척', '양양', '고양', '천안', '거제', '여수', '제주', '변산',
]
_SITE_ALL_PAT = '|'.join(re.escape(w) for w in sorted(_SITE_WORDS, key=len, reverse=True))
_CLEANUP_RE = re.compile(
    r'\s*\(\s*(?:단양|델|대매|대|O|Step\s*\d*)\s*\)'
    r'|(?:' + _SITE_ALL_PAT + r')'
    r'|\b대매점?\b'
    r'|\b\d{2,4}Y\b|\b\d{2}/\d{2}\b|\b\d{2,4}년\b'
    r'|\s+일반\s*$',
    flags=re.IGNORECASE
)
_OTA_NAMES = [
    '야놀자', '아고다', '여기어때', '트립닷컴', '부킹닷컴', '부킹', '쿠팡',
    '인터파크', '트립비토즈', '익스피디아', '네이버', '위메프', '티몬',
    '호텔스닷컴', '11번가', '프리즘', '마이리얼트립', '온라인콘도',
]

_normalize_cache: dict[str, str] = {}


def _normalize_series(name: str) -> str:
    cached = _normalize_cache.get(name)
    if cached is not None:
        return cached
    if not name:
        _normalize_cache[name] = '기타'
        return '기타'
    s = _HASH_TAG_RE.sub('', name)
    s = _BRACKET_RE.sub('', s)
    s = _GOTA_RE.sub('', s)
    s = _YEAR_PREFIX_RE.sub('', s)
    s = _PAREN_CH_RE.sub('', s)
    s = _NIGHT_RE.sub('', s)
    s = _SUFFIX_RE.sub('', s)
    s = _CLEANUP_RE.sub(' ', s)
    s = re.sub(r'^[\s_/]+|[\s_/]+$', '', s)
    s = re.sub(r'_+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    out = s if s else '기타'
    _normalize_cache[name] = out
    return out


def _classify_v4(series_name: str) -> str:
    """상품계열명 → 9개 카테고리 (parse_package_trend.classify_v4와 동일)"""
    s = series_name.upper()
    if any(k in series_name for k in ['룸온리', '룸온니', 'R/O', 'RO']):
        return '룸온니/프로모션'
    if 'ROOM ONLY' in s:
        return '룸온니/프로모션'
    for ota in _OTA_NAMES:
        if ota in series_name:
            return '룸온니/프로모션'
    if re.search(r'\bOTA\b|GOTA', s):
        return '룸온니/프로모션'
    if any(k in s for k in ['2 NIGHTS', '2NIGHTS', '2NIGHT', '2나잇', '연박', 'PRIVILEGE',
                             'HOURS STAY', 'HOUR STAY', '스마트초이스', '스마트 초이스']):
        return '연박/투나잇'
    if re.search(r'\d박\s*(PKG|스테이)', s):
        return '연박/투나잇'
    if any(k in s for k in ['ALL INCLUSIVE', 'ALLINCLUSIVE', '올인클루시브', '올클',
                             '썸머클루시브']):
        return '올인클루시브'
    if any(k in s for k in ['TASTY MORNING', '조식', 'MORNING', 'BREAKFAST']):
        return '조식패키지'
    if any(k in s for k in ['얼리버드', '얼리바캉스', '얼리 바캉스', '빅세일', '기획전', '숙박세일', '세일페스타',
                             '13%', '당일특가', 'LATE HOLIDAY', '레이트 홀리데이', '특가', 'STEP2',
                             '브랜드데이', '멤버스데이', '소노브랜드', '숙박대전']):
        return '세일/기획전'
    if '세일' in series_name:
        return '세일/기획전'
    if any(k in s for k in ['WATERFUL', 'WATER-FUL', '워터풀', '오션에빠지다',
                             'BLUE COAST', 'BLUECOAST', 'SIMPLE OCEAN']):
        return '워터풀/오션'
    if '오션' in series_name:
        return '워터풀/오션'
    if any(k in s for k in ['동계', '스키', 'WINTER', '윈터', '스노위', 'SNOWY', '겨울',
                             '썸머', '블루데이즈', '바캉스', '베케이션', 'SUMMER', 'VACATION', 'BLUEDAYS', '러브썸',
                             '스프링', '블룸', '추석', 'SPRING', '가을', '단풍', 'BLOOM']):
        return '시즌패키지'
    if any(k in s for k in ['온천', 'BBQ', '케이펫', '레저', '낚시', 'SPA', 'MOMENTS',
                             '투어 패키지', '투어패키지', '가족5', '가족 5', '투어']):
        return '액티비티/레저'
    if '프로모션' in series_name:
        return '룸온니/프로모션'
    return '기타'


def parse_pkg_daily_by_category(raw_db_dir: Path, target_dates: set[str]) -> tuple[dict, dict]:
    """raw_db에서 패키지(회원번호=86) 행만 추출하여 상품카테고리별 일별 집계.

    target_dates 윈도우(=this_dates ∪ prev_dates ∪ ly_dates)에 속하는
    pickup_date(최초입력일자) / cancel_date(취소일자) 행만 누적.

    반환: (pickup_daily_by_category, cancel_daily_by_category)
      형태 = {category: {YYYYMMDD: {rn, rev}}}
    """
    pickup = defaultdict(lambda: defaultdict(lambda: {'rn': 0, 'rev': 0.0}))
    cancel = defaultdict(lambda: defaultdict(lambda: {'rn': 0, 'rev': 0.0}))

    if not target_dates:
        return {}, {}

    # 윈도우가 닿는 연도만 스캔 (this/prev/ly가 다른 연도일 수 있음)
    years = sorted({d[:4] for d in target_dates})

    total_rows = 0
    for year in years:
        year_dir = raw_db_dir / year
        if not year_dir.exists():
            continue
        for fname in sorted(os.listdir(year_dir)):
            if not (fname.startswith('27.') or fname.startswith('28.')
                    or fname.startswith('43.') or fname.startswith('44.')):
                continue
            is_cancel = fname.startswith('28.') or fname.startswith('44.')
            fpath = year_dir / fname
            try:
                with open(fpath, encoding='cp949', errors='replace') as f:
                    header = f.readline()
                    headers = [h.strip() for h in header.rstrip('\n').split(';')]
                    col = {h: i for i, h in enumerate(headers)}
                    idx_mem = col.get('회원번호', 5)
                    idx_mem_name = col.get('회원명', 6)
                    idx_rooms = col.get('객실수', 28)
                    idx_rate = col.get('1박객실료', 26)
                    idx_pickup = col.get('최초입력일자', -1)
                    idx_cancel = col.get('취소일자', -1)
                    n_cols = max(idx_mem, idx_mem_name, idx_rooms, idx_rate,
                                 idx_pickup, idx_cancel) + 1

                    for line in f:
                        parts = line.rstrip('\n').split(';')
                        if len(parts) < n_cols:
                            continue
                        if not parts[idx_mem].strip().startswith('86'):
                            continue

                        # 픽업/취소 일자가 윈도우에 있는지 먼저 검사
                        pkup_str = parts[idx_pickup].strip()[:8] if idx_pickup >= 0 else ''
                        cncl_str = parts[idx_cancel].strip()[:8] if idx_cancel >= 0 and is_cancel else ''
                        in_pickup = len(pkup_str) == 8 and pkup_str in target_dates
                        in_cancel = len(cncl_str) == 8 and cncl_str in target_dates
                        if not (in_pickup or in_cancel):
                            continue

                        try:
                            rn = int(parts[idx_rooms].strip() or 0)
                            rate = int(parts[idx_rate].strip() or 0)
                        except ValueError:
                            continue
                        if rn <= 0:
                            rn = 1
                        rev = int(rate * rn / 1.1)

                        cat = _classify_v4(_normalize_series(parts[idx_mem_name].strip()))

                        # parse_raw_db.py와 동일 규칙:
                        # pickup_daily에는 27/43 + 28/44 모두 +로 누적 (예약접수일 기준)
                        # cancel_daily에는 28/44만 +로 누적 (취소일자 기준)
                        # net = pickup - cancel
                        if in_pickup:
                            slot = pickup[cat][pkup_str]
                            slot['rn'] += rn
                            slot['rev'] += rev
                        if in_cancel:
                            slot = cancel[cat][cncl_str]
                            slot['rn'] += rn
                            slot['rev'] += rev
                        total_rows += 1
            except Exception as e:
                logger.warning(f"  raw_db 파싱 실패: {fname} — {e}")

    logger.info(f"  상품카테고리 집계: {total_rows:,}행 (패키지 회원번호=86, 윈도우 필터)")

    # defaultdict → 일반 dict
    pickup_out = {cat: dict(days) for cat, days in pickup.items()}
    cancel_out = {cat: dict(days) for cat, days in cancel.items()}
    return pickup_out, cancel_out


# ─── 일자 유틸 ───
def to_date(ymd: str) -> datetime:
    return datetime.strptime(ymd, "%Y%m%d")


def to_ymd(d: datetime) -> str:
    return d.strftime("%Y%m%d")


def daterange(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def week_monday(d: datetime) -> datetime:
    """월요일 시작 ISO 주차"""
    return d - timedelta(days=d.weekday())


# ─── 합산 ───
def sum_window(daily_map: dict, dates: list[str]) -> dict:
    rn = 0
    rev = 0.0
    for ymd in dates:
        if ymd in daily_map:
            v = daily_map[ymd]
            rn += v.get("rn", 0)
            rev += v.get("rev", 0)
    return {"rn": rn, "rev": round(rev, 2)}


def sum_window_keyed(keyed_daily_map: dict, dates: list[str]) -> dict:
    """{key: {ymd: {rn, rev}}} → {key: {rn, rev}} 윈도우 합"""
    out = {}
    for key, days in keyed_daily_map.items():
        rn, rev = 0, 0.0
        for ymd in dates:
            if ymd in days:
                v = days[ymd]
                rn += v.get("rn", 0)
                rev += v.get("rev", 0)
        if rn != 0 or rev != 0:
            out[key] = {"rn": rn, "rev": round(rev, 2)}
    return out


def calc_net(pickup: dict, cancel: dict) -> dict:
    """pickup - cancel = net. cancel은 빈 dict일 수 있음"""
    return {
        "pickup_rn": pickup.get("rn", 0),
        "pickup_rev": pickup.get("rev", 0.0),
        "cancel_rn": cancel.get("rn", 0),
        "cancel_rev": cancel.get("rev", 0.0),
        "net_rn": pickup.get("rn", 0) - cancel.get("rn", 0),
        "net_rev": round(pickup.get("rev", 0.0) - cancel.get("rev", 0.0), 2),
    }


def pct_change(curr: float, prev: float) -> float | None:
    """((curr-prev)/prev)*100. prev=0이면 None"""
    if prev == 0:
        return None
    return round((curr - prev) / prev * 100, 1)


# ─── 메인 ───
def build():
    if not INPUT_PATH.exists():
        logger.error(f"입력 없음: {INPUT_PATH}")
        sys.exit(1)

    logger.info(f"읽기: {INPUT_PATH}")
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        agg = json.load(f)

    # 필수 키 확인
    needed = [
        "pickup_daily", "cancel_daily",
        "pickup_daily_by_property", "cancel_daily_by_property",
        "pickup_daily_by_segment", "cancel_daily_by_segment",
    ]
    for k in needed:
        if k not in agg:
            logger.error(f"db_aggregated.json에 키 없음: {k}")
            sys.exit(1)

    pickup_total = agg["pickup_daily"]
    cancel_total = agg["cancel_daily"]
    pickup_prop = agg["pickup_daily_by_property"]
    cancel_prop = agg["cancel_daily_by_property"]
    pickup_seg = agg["pickup_daily_by_segment"]
    cancel_seg = agg["cancel_daily_by_segment"]
    pickup_ch = agg.get("pickup_daily_by_channel", {})
    cancel_ch = agg.get("cancel_daily_by_channel", {})
    if not pickup_ch:
        logger.warning("pickup_daily_by_channel 누락 → 거래처별 산출 생략 (patch_channel_daily.py 먼저 실행 필요)")

    # 가장 최근 데이터 일자. 단, 추출 시점이 그날 새벽이면 partial이므로
    # latest day의 pickup이 직전 7일 평균의 30% 미만이면 latest-1을 "마지막 완전일"로 본다.
    all_dates = sorted(pickup_total.keys())
    if not all_dates:
        logger.error("pickup_daily가 비어있음")
        sys.exit(1)
    raw_latest_ymd = all_dates[-1]
    raw_latest_date = to_date(raw_latest_ymd)
    raw_latest_pu = pickup_total.get(raw_latest_ymd, {}).get("rn", 0)

    # 직전 7일(latest 제외) 평균
    prev7 = [pickup_total[to_ymd(raw_latest_date - timedelta(days=i))].get("rn", 0)
             for i in range(1, 8) if to_ymd(raw_latest_date - timedelta(days=i)) in pickup_total]
    avg_prev7 = sum(prev7) / len(prev7) if prev7 else 0
    is_partial = avg_prev7 > 0 and raw_latest_pu < 0.3 * avg_prev7

    if is_partial:
        latest_ymd = to_ymd(raw_latest_date - timedelta(days=1))
        logger.info(f"  최신 데이터일: {raw_latest_ymd} (partial, RN={raw_latest_pu} vs 7일 평균 {avg_prev7:.0f})")
        logger.info(f"  → 마지막 완전일: {latest_ymd} 사용")
    else:
        latest_ymd = raw_latest_ymd
        logger.info(f"  최신 데이터일(완전): {latest_ymd}")
    latest_date = to_date(latest_ymd)
    partial_today_meta = {
        "is_partial": is_partial,
        "raw_latest_date": raw_latest_ymd,
        "raw_latest_pickup_rn": raw_latest_pu,
    }

    # 윈도우 정의
    # this_week = 월요일 ~ latest_date (Week-To-Date)
    this_mon = week_monday(latest_date)
    this_dates = [to_ymd(d) for d in daterange(this_mon, latest_date)]
    days_in_week = len(this_dates)

    # prev_week = 7일 전 같은 요일 범위 (동일 일수)
    prev_mon = this_mon - timedelta(days=7)
    prev_end = latest_date - timedelta(days=7)
    prev_dates = [to_ymd(d) for d in daterange(prev_mon, prev_end)]

    # ly_week = 364일 전 (동요일 보존)
    ly_mon = this_mon - timedelta(days=364)
    ly_end = latest_date - timedelta(days=364)
    ly_dates = [to_ymd(d) for d in daterange(ly_mon, ly_end)]

    logger.info(f"  this_week: {this_dates[0]} ~ {this_dates[-1]} ({days_in_week}일)")
    logger.info(f"  prev_week: {prev_dates[0]} ~ {prev_dates[-1]}")
    logger.info(f"  ly_week:   {ly_dates[0]} ~ {ly_dates[-1]}")

    # ── 당일/전일 ──
    today_pu = pickup_total.get(latest_ymd, {"rn": 0, "rev": 0.0})
    today_cn = cancel_total.get(latest_ymd, {"rn": 0, "rev": 0.0})
    yest_ymd = to_ymd(latest_date - timedelta(days=1))
    yest_pu = pickup_total.get(yest_ymd, {"rn": 0, "rev": 0.0})
    yest_cn = cancel_total.get(yest_ymd, {"rn": 0, "rev": 0.0})

    # ── 전주 동요일/전년 동요일 (당일 비교용) ──
    prev_day_ymd = to_ymd(latest_date - timedelta(days=7))
    ly_day_ymd = to_ymd(latest_date - timedelta(days=364))
    prev_day_pu = pickup_total.get(prev_day_ymd, {"rn": 0, "rev": 0.0})
    prev_day_cn = cancel_total.get(prev_day_ymd, {"rn": 0, "rev": 0.0})
    ly_day_pu = pickup_total.get(ly_day_ymd, {"rn": 0, "rev": 0.0})
    ly_day_cn = cancel_total.get(ly_day_ymd, {"rn": 0, "rev": 0.0})

    today_block = {
        "date": latest_ymd,
        "this": calc_net(today_pu, today_cn),
        "prev_week_same_day": {
            "date": prev_day_ymd,
            **calc_net(prev_day_pu, prev_day_cn),
        },
        "ly_same_day": {
            "date": ly_day_ymd,
            **calc_net(ly_day_pu, ly_day_cn),
        },
    }
    today_block["wow_pct"] = pct_change(today_block["this"]["net_rn"],
                                        today_block["prev_week_same_day"]["net_rn"])
    today_block["yoy_pct"] = pct_change(today_block["this"]["net_rn"],
                                        today_block["ly_same_day"]["net_rn"])

    yesterday_block = {
        "date": yest_ymd,
        "this": calc_net(yest_pu, yest_cn),
    }

    # ── 주간 합계(전체) ──
    this_pu = sum_window(pickup_total, this_dates)
    this_cn = sum_window(cancel_total, this_dates)
    prev_pu = sum_window(pickup_total, prev_dates)
    prev_cn = sum_window(cancel_total, prev_dates)
    ly_pu = sum_window(pickup_total, ly_dates)
    ly_cn = sum_window(cancel_total, ly_dates)

    week_totals = {
        "this_week": {
            "start": this_dates[0], "end": this_dates[-1], "days": days_in_week,
            **calc_net(this_pu, this_cn),
        },
        "prev_week": {
            "start": prev_dates[0], "end": prev_dates[-1], "days": days_in_week,
            **calc_net(prev_pu, prev_cn),
        },
        "ly_week": {
            "start": ly_dates[0], "end": ly_dates[-1], "days": days_in_week,
            **calc_net(ly_pu, ly_cn),
        },
    }
    week_totals["wow_rn_pct"] = pct_change(week_totals["this_week"]["net_rn"],
                                           week_totals["prev_week"]["net_rn"])
    week_totals["yoy_rn_pct"] = pct_change(week_totals["this_week"]["net_rn"],
                                           week_totals["ly_week"]["net_rn"])
    week_totals["wow_rev_pct"] = pct_change(week_totals["this_week"]["net_rev"],
                                            week_totals["prev_week"]["net_rev"])
    week_totals["yoy_rev_pct"] = pct_change(week_totals["this_week"]["net_rev"],
                                            week_totals["ly_week"]["net_rev"])

    # ── 사업장별 ──
    this_p_pu = sum_window_keyed(pickup_prop, this_dates)
    this_p_cn = sum_window_keyed(cancel_prop, this_dates)
    prev_p_pu = sum_window_keyed(pickup_prop, prev_dates)
    prev_p_cn = sum_window_keyed(cancel_prop, prev_dates)
    ly_p_pu = sum_window_keyed(pickup_prop, ly_dates)
    ly_p_cn = sum_window_keyed(cancel_prop, ly_dates)

    all_props = sorted(set(this_p_pu.keys()) | set(prev_p_pu.keys()) | set(ly_p_pu.keys())
                       | set(this_p_cn.keys()) | set(prev_p_cn.keys()) | set(ly_p_cn.keys()))
    by_property = []
    for prop in all_props:
        if prop == "미분류":
            continue
        this_n = calc_net(this_p_pu.get(prop, {}), this_p_cn.get(prop, {}))
        prev_n = calc_net(prev_p_pu.get(prop, {}), prev_p_cn.get(prop, {}))
        ly_n = calc_net(ly_p_pu.get(prop, {}), ly_p_cn.get(prop, {}))
        by_property.append({
            "property": prop,
            "this_net_rn": this_n["net_rn"],
            "this_net_rev": this_n["net_rev"],
            "prev_net_rn": prev_n["net_rn"],
            "prev_net_rev": prev_n["net_rev"],
            "ly_net_rn": ly_n["net_rn"],
            "ly_net_rev": ly_n["net_rev"],
            "wow_rn_pct": pct_change(this_n["net_rn"], prev_n["net_rn"]),
            "yoy_rn_pct": pct_change(this_n["net_rn"], ly_n["net_rn"]),
            "wow_rev_pct": pct_change(this_n["net_rev"], prev_n["net_rev"]),
            "yoy_rev_pct": pct_change(this_n["net_rev"], ly_n["net_rev"]),
        })
    # 금주 net_rn 내림차순
    by_property.sort(key=lambda x: x["this_net_rn"], reverse=True)

    # ── 세그먼트별 ──
    this_s_pu = sum_window_keyed(pickup_seg, this_dates)
    this_s_cn = sum_window_keyed(cancel_seg, this_dates)
    prev_s_pu = sum_window_keyed(pickup_seg, prev_dates)
    prev_s_cn = sum_window_keyed(cancel_seg, prev_dates)
    ly_s_pu = sum_window_keyed(pickup_seg, ly_dates)
    ly_s_cn = sum_window_keyed(cancel_seg, ly_dates)

    all_segs = sorted(set(this_s_pu.keys()) | set(prev_s_pu.keys()) | set(ly_s_pu.keys())
                      | set(this_s_cn.keys()) | set(prev_s_cn.keys()) | set(ly_s_cn.keys()))
    by_segment = []
    for seg in all_segs:
        if not seg:
            continue
        this_n = calc_net(this_s_pu.get(seg, {}), this_s_cn.get(seg, {}))
        prev_n = calc_net(prev_s_pu.get(seg, {}), prev_s_cn.get(seg, {}))
        ly_n = calc_net(ly_s_pu.get(seg, {}), ly_s_cn.get(seg, {}))
        by_segment.append({
            "segment": seg,
            "this_net_rn": this_n["net_rn"],
            "this_net_rev": this_n["net_rev"],
            "prev_net_rn": prev_n["net_rn"],
            "prev_net_rev": prev_n["net_rev"],
            "ly_net_rn": ly_n["net_rn"],
            "ly_net_rev": ly_n["net_rev"],
            "wow_rn_pct": pct_change(this_n["net_rn"], prev_n["net_rn"]),
            "yoy_rn_pct": pct_change(this_n["net_rn"], ly_n["net_rn"]),
            "wow_rev_pct": pct_change(this_n["net_rev"], prev_n["net_rev"]),
            "yoy_rev_pct": pct_change(this_n["net_rev"], ly_n["net_rev"]),
        })
    by_segment.sort(key=lambda x: x["this_net_rn"], reverse=True)

    # ── 거래처(채널)별 ──
    by_channel = []
    if pickup_ch:
        this_c_pu = sum_window_keyed(pickup_ch, this_dates)
        this_c_cn = sum_window_keyed(cancel_ch, this_dates)
        prev_c_pu = sum_window_keyed(pickup_ch, prev_dates)
        prev_c_cn = sum_window_keyed(cancel_ch, prev_dates)
        ly_c_pu = sum_window_keyed(pickup_ch, ly_dates)
        ly_c_cn = sum_window_keyed(cancel_ch, ly_dates)

        all_chs = sorted(set(this_c_pu.keys()) | set(prev_c_pu.keys()) | set(ly_c_pu.keys())
                         | set(this_c_cn.keys()) | set(prev_c_cn.keys()) | set(ly_c_cn.keys()))
        for ch in all_chs:
            if not ch or ch == "기타":
                # 사용자 메모리: '기타' 카테고리 절대 금지 → 노출 제외
                continue
            this_n = calc_net(this_c_pu.get(ch, {}), this_c_cn.get(ch, {}))
            prev_n = calc_net(prev_c_pu.get(ch, {}), prev_c_cn.get(ch, {}))
            ly_n = calc_net(ly_c_pu.get(ch, {}), ly_c_cn.get(ch, {}))
            # 모든 윈도우에서 0이면 노출 가치 없음
            if this_n["net_rn"] == 0 and prev_n["net_rn"] == 0 and ly_n["net_rn"] == 0:
                continue
            by_channel.append({
                "channel": ch,
                "this_net_rn": this_n["net_rn"],
                "this_net_rev": this_n["net_rev"],
                "prev_net_rn": prev_n["net_rn"],
                "prev_net_rev": prev_n["net_rev"],
                "ly_net_rn": ly_n["net_rn"],
                "ly_net_rev": ly_n["net_rev"],
                "wow_rn_pct": pct_change(this_n["net_rn"], prev_n["net_rn"]),
                "yoy_rn_pct": pct_change(this_n["net_rn"], ly_n["net_rn"]),
                "wow_rev_pct": pct_change(this_n["net_rev"], prev_n["net_rev"]),
                "yoy_rev_pct": pct_change(this_n["net_rev"], ly_n["net_rev"]),
            })
        by_channel.sort(key=lambda x: x["this_net_rn"], reverse=True)

    # ── 상품카테고리별 (패키지 회원번호=86 → product-detail 9개 카테고리) ──
    by_product_category = []
    raw_db_dir = find_raw_db_dir()
    if raw_db_dir is None:
        logger.warning("raw_db 디렉토리 미발견 → 상품카테고리 집계 생략")
    else:
        logger.info(f"  raw_db: {raw_db_dir}")
        target_dates = set(this_dates) | set(prev_dates) | set(ly_dates)
        pickup_pc, cancel_pc = parse_pkg_daily_by_category(raw_db_dir, target_dates)

        this_pc_pu = sum_window_keyed(pickup_pc, this_dates)
        this_pc_cn = sum_window_keyed(cancel_pc, this_dates)
        prev_pc_pu = sum_window_keyed(pickup_pc, prev_dates)
        prev_pc_cn = sum_window_keyed(cancel_pc, prev_dates)
        ly_pc_pu = sum_window_keyed(pickup_pc, ly_dates)
        ly_pc_cn = sum_window_keyed(cancel_pc, ly_dates)

        all_pcs = sorted(set(this_pc_pu.keys()) | set(prev_pc_pu.keys()) | set(ly_pc_pu.keys())
                         | set(this_pc_cn.keys()) | set(prev_pc_cn.keys()) | set(ly_pc_cn.keys()))
        for cat in all_pcs:
            if not cat or cat == "기타":
                # '기타' 카테고리 절대 금지
                continue
            this_n = calc_net(this_pc_pu.get(cat, {}), this_pc_cn.get(cat, {}))
            prev_n = calc_net(prev_pc_pu.get(cat, {}), prev_pc_cn.get(cat, {}))
            ly_n = calc_net(ly_pc_pu.get(cat, {}), ly_pc_cn.get(cat, {}))
            if this_n["net_rn"] == 0 and prev_n["net_rn"] == 0 and ly_n["net_rn"] == 0:
                continue
            by_product_category.append({
                "category": cat,
                "this_net_rn": this_n["net_rn"],
                "this_net_rev": this_n["net_rev"],
                "prev_net_rn": prev_n["net_rn"],
                "prev_net_rev": prev_n["net_rev"],
                "ly_net_rn": ly_n["net_rn"],
                "ly_net_rev": ly_n["net_rev"],
                "wow_rn_pct": pct_change(this_n["net_rn"], prev_n["net_rn"]),
                "yoy_rn_pct": pct_change(this_n["net_rn"], ly_n["net_rn"]),
                "wow_rev_pct": pct_change(this_n["net_rev"], prev_n["net_rev"]),
                "yoy_rev_pct": pct_change(this_n["net_rev"], ly_n["net_rev"]),
            })
        by_product_category.sort(key=lambda x: x["this_net_rn"], reverse=True)

    # ── 인사이트 자동 생성 ──
    insights = []

    # 1) 전체 금주 vs 전주
    if week_totals["wow_rn_pct"] is not None:
        wow = week_totals["wow_rn_pct"]
        delta = week_totals["this_week"]["net_rn"] - week_totals["prev_week"]["net_rn"]
        sign = "▲" if wow > 0 else "▼"
        tone = "positive" if wow > 0 else "negative"
        insights.append({
            "tone": tone,
            "title": f"금주 온북 {sign} {abs(wow)}% (전주比)",
            "detail": f"금주 {week_totals['this_week']['net_rn']:,}실 vs 전주 {week_totals['prev_week']['net_rn']:,}실 (Δ{delta:+,}실)"
        })

    # 2) 전체 금주 vs LY
    if week_totals["yoy_rn_pct"] is not None:
        yoy = week_totals["yoy_rn_pct"]
        delta = week_totals["this_week"]["net_rn"] - week_totals["ly_week"]["net_rn"]
        sign = "▲" if yoy > 0 else "▼"
        tone = "positive" if yoy > 0 else "negative"
        insights.append({
            "tone": tone,
            "title": f"금주 온북 {sign} {abs(yoy)}% (전년 동기간比)",
            "detail": f"금주 {week_totals['this_week']['net_rn']:,}실 vs 전년 {week_totals['ly_week']['net_rn']:,}실 (Δ{delta:+,}실)"
        })

    # 3) 사업장 — 최대 상승 (WoW, |this_net_rn|≥30)
    rising = [p for p in by_property
              if p["wow_rn_pct"] is not None and abs(p["this_net_rn"]) >= 30 and p["wow_rn_pct"] > 0]
    rising.sort(key=lambda x: x["wow_rn_pct"], reverse=True)
    if rising:
        top = rising[0]
        insights.append({
            "tone": "positive",
            "title": f"급등 사업장: {top['property']} ▲ {top['wow_rn_pct']}% (전주比)",
            "detail": f"금주 {top['this_net_rn']:,}실 vs 전주 {top['prev_net_rn']:,}실 (Δ{top['this_net_rn']-top['prev_net_rn']:+,}실)"
        })

    # 4) 사업장 — 최대 하락 (WoW)
    falling = [p for p in by_property
               if p["wow_rn_pct"] is not None and abs(p["prev_net_rn"]) >= 30 and p["wow_rn_pct"] < 0]
    falling.sort(key=lambda x: x["wow_rn_pct"])
    if falling:
        bot = falling[0]
        insights.append({
            "tone": "negative",
            "title": f"급락 사업장: {bot['property']} ▼ {abs(bot['wow_rn_pct'])}% (전주比)",
            "detail": f"금주 {bot['this_net_rn']:,}실 vs 전주 {bot['prev_net_rn']:,}실 (Δ{bot['this_net_rn']-bot['prev_net_rn']:+,}실)"
        })

    # 5) 세그먼트 — OTA/G-OTA/Inbound 중 가장 큰 변동
    focus_segs = [s for s in by_segment if s["segment"] in ("OTA", "G-OTA", "Inbound")
                  and s["wow_rn_pct"] is not None]
    if focus_segs:
        focus_segs.sort(key=lambda x: abs(x["wow_rn_pct"]), reverse=True)
        s = focus_segs[0]
        sign = "▲" if s["wow_rn_pct"] > 0 else "▼"
        tone = "positive" if s["wow_rn_pct"] > 0 else "negative"
        insights.append({
            "tone": tone,
            "title": f"{s['segment']} 세그먼트 {sign} {abs(s['wow_rn_pct'])}% (전주比)",
            "detail": f"금주 {s['this_net_rn']:,}실 vs 전주 {s['prev_net_rn']:,}실"
        })

    # 5b) 거래처(채널) — top |WoW| (최소 금주 100실 이상)
    if by_channel:
        ch_movers = [c for c in by_channel
                     if c["wow_rn_pct"] is not None and abs(c["this_net_rn"]) >= 100]
        ch_movers.sort(key=lambda x: abs(x["wow_rn_pct"]), reverse=True)
        if ch_movers:
            c = ch_movers[0]
            sign = "▲" if c["wow_rn_pct"] > 0 else "▼"
            tone = "positive" if c["wow_rn_pct"] > 0 else "negative"
            insights.append({
                "tone": tone,
                "title": f"거래처: {c['channel']} {sign} {abs(c['wow_rn_pct'])}% (전주比)",
                "detail": f"금주 {c['this_net_rn']:,}실 vs 전주 {c['prev_net_rn']:,}실"
            })

    # 5c) 상품카테고리 — top |WoW| (최소 금주 50실 이상)
    if by_product_category:
        pc_movers = [c for c in by_product_category
                     if c["wow_rn_pct"] is not None and abs(c["this_net_rn"]) >= 50]
        pc_movers.sort(key=lambda x: abs(x["wow_rn_pct"]), reverse=True)
        if pc_movers:
            c = pc_movers[0]
            sign = "▲" if c["wow_rn_pct"] > 0 else "▼"
            tone = "positive" if c["wow_rn_pct"] > 0 else "negative"
            insights.append({
                "tone": tone,
                "title": f"상품카테고리: {c['category']} {sign} {abs(c['wow_rn_pct'])}% (전주比)",
                "detail": f"금주 {c['this_net_rn']:,}실 vs 전주 {c['prev_net_rn']:,}실"
            })

    # 6) 당일 vs 전주 동요일
    if today_block["wow_pct"] is not None:
        wow = today_block["wow_pct"]
        sign = "▲" if wow > 0 else "▼"
        tone = "positive" if wow > 0 else "negative"
        insights.append({
            "tone": tone,
            "title": f"당일({latest_ymd[4:6]}/{latest_ymd[6:8]}) 픽업 {sign} {abs(wow)}% (전주 동요일比)",
            "detail": f"당일 {today_block['this']['net_rn']:,}실 vs 전주 {today_block['prev_week_same_day']['net_rn']:,}실"
        })

    # 결과 저장
    result = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_generated_at": agg.get("generated_at", ""),
        "latest_data_date": latest_ymd,
        "partial_today": partial_today_meta,
        "today": today_block,
        "yesterday": yesterday_block,
        "week_totals": week_totals,
        "by_property": by_property,
        "by_segment": by_segment,
        "by_channel": by_channel,
        "by_product_category": by_product_category,
        "insights": insights[:7],
    }

    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info(f"✓ 저장: {OUTPUT_PATH} ({OUTPUT_PATH.stat().st_size:,} bytes)")
    logger.info(f"  사업장 {len(by_property)}개 / 세그먼트 {len(by_segment)}개 / 거래처 {len(by_channel)}개 / 상품카테고리 {len(by_product_category)}개 / 인사이트 {len(result['insights'])}개")


if __name__ == "__main__":
    build()
