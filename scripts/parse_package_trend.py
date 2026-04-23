#!/usr/bin/env python3
"""
parse_package_trend.py — 온북 DB 패키지(회원번호=86XXXXXX) 분류별 실적 집계
- 회원번호가 86으로 시작하는 행 = 패키지 상품
- 회원명 = 패키지명 (채널/연도 접두사 제거 → 상품계열명 도출)
출력: data/package_series_trend.json
"""
import os, json, re, logging
from pathlib import Path
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

# raw_db는 메인 프로젝트에 있음 (워크트리 3단계 위)
def find_raw_db():
    candidates = [
        PROJECT_DIR / "data" / "raw_db",
        PROJECT_DIR.parents[2] / "data" / "raw_db",  # worktree 3-level up
    ]
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


def normalize_series(name: str) -> str:
    """패키지명(회원명) → 상품계열명 (채널·연도·박수 제거)"""
    if not name:
        return '기타'
    name = _BRACKET_RE.sub('', name)
    name = _GOTA_RE.sub('', name)
    name = _YEAR_PREFIX_RE.sub('', name)
    name = _NIGHT_RE.sub('', name)
    name = _SUFFIX_RE.sub('', name)
    name = _PAREN_CH_RE.sub('', name)
    name = re.sub(r'^[\s_]+|[\s_]+$', '', name)
    name = re.sub(r'_+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name if name else '기타'


def parse_file(fpath: Path, is_cancel: bool, agg: dict):
    """type 27/28 파일 파싱 → agg[시리즈][YYYYMM] 집계"""
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

    count = 0
    for line in lines[1:]:
        parts = line.rstrip('\n').split(';')
        if len(parts) <= max(idx_mem, idx_mem_name, idx_date, idx_rn, idx_rate):
            continue
        mem_no = parts[idx_mem].strip()
        if not mem_no.startswith('86'):
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

        m = agg[series][yyyymm]
        if is_cancel:
            m['cancel_rn'] += rn
            m['cancel_rev'] += rate
        else:
            m['booking_rn'] += rn
            m['booking_rev'] += rate

        count += 1

    return count


def main():
    logger.info(f"raw_db 경로: {RAW_DB_DIR}")
    years = ['2022', '2023', '2024', '2025', '2026']

    # series → yyyymm → {booking_rn, booking_rev, cancel_rn, cancel_rev}
    def new_month():
        return {'booking_rn': 0, 'booking_rev': 0, 'cancel_rn': 0, 'cancel_rev': 0}
    agg = defaultdict(lambda: defaultdict(new_month))

    total_rows = 0
    for year in years:
        year_dir = RAW_DB_DIR / year
        if not year_dir.exists():
            logger.warning(f"연도 디렉토리 없음: {year_dir}")
            continue
        for fname in sorted(os.listdir(year_dir)):
            if fname.startswith('27.'):
                n = parse_file(year_dir / fname, is_cancel=False, agg=agg)
                logger.info(f"  {year}/{fname[:40]}: {n:,}행")
                total_rows += n
            elif fname.startswith('28.'):
                n = parse_file(year_dir / fname, is_cancel=True, agg=agg)
                logger.info(f"  {year}/{fname[:40]} (취소): {n:,}행")
                total_rows += n

    logger.info(f"총 파싱 행 수: {total_rows:,}")

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

    # by_year_ranking: 연도별 TOP 20
    by_year_ranking = {}
    for year in years:
        ranking = []
        for series, months in agg.items():
            yr_rn = sum(
                max(0, m['booking_rn'] - m['cancel_rn'])
                for ym, m in months.items() if ym.startswith(year)
            )
            yr_rev_won = sum(
                max(0, m['booking_rev'] - m['cancel_rev'])
                for ym, m in months.items() if ym.startswith(year)
            )
            if yr_rn > 0:
                ranking.append({
                    'name': series,
                    'rn': yr_rn,
                    'rev': round(yr_rev_won / 1_000_000, 1),
                    'adr': round(yr_rev_won / yr_rn / 1000) if yr_rn > 0 else 0,
                })
        ranking.sort(key=lambda x: x['rn'], reverse=True)
        by_year_ranking[year] = ranking[:20]

    output = {
        'top_series': [{'name': s, **series_totals[s]} for s in top_names],
        'by_series': by_series,
        'by_year_ranking': by_year_ranking,
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
