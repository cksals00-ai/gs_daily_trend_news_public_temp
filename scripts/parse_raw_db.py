#!/usr/bin/env python3
"""
parse_raw_db.py — 온북 원시 DB txt 파일 파싱 → JSON 집계
- 27번: FIT(OTA/GOTA) 예약
- 28번: FIT 취소
- 43번: Inbound 예약
- 44번: Inbound 취소
CP949 인코딩, 세미콜론(;) 구분
"""
import os, sys, json, re, logging, glob, unicodedata
from pathlib import Path
from collections import defaultdict
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─── 경로 ───
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
RAW_DB_DIR = PROJECT_DIR / "data" / "raw_db"
OUTPUT_DIR = PROJECT_DIR / "data"

# ─── OTA 채널 매핑 (AGENT명 키워드 → 채널명) ───
OTA_CHANNEL_MAP = {
    "야놀자": "야놀자", "놀유니버스": "야놀자",
    "아고다": "아고다",
    "여기어때": "여기어때", "여기어때컴퍼니": "여기어때",
    "트립닷컴": "트립닷컴",
    "트립비토즈": "트립비토즈",
    "네이버": "네이버",
    "익스피디아": "익스피디아",
    "부킹닷컴": "부킹닷컴", "부킹": "부킹닷컴",
    "쿠팡": "쿠팡",
    "호텔스닷컴": "호텔스닷컴",
    "프리즘": "프리즘",
    "종이비행기": "종이비행기",
    "스마트인피니": "스마트인피니",
    "인터파크": "인터파크",
    "티몬": "티몬",
    "위메프": "위메프",
    "타이드스퀘어": "타이드스퀘어",
    "웹투어": "웹투어",
    "올마이투어": "올마이투어",
    "코이스토리": "코이스토리",
    "마이리얼트립": "마이리얼트립",
    "온라인콘도": "온라인콘도",
    "맥스모바일": "맥스모바일",
    "트립토파즈": "트립토파즈",
    "가자고투어": "가자고투어",
    "플러스앤": "플러스앤",
    "이참조은레저": "이참조은레저",
    "아이러브레저": "아이러브레저",
    "스테이원": "스테이원",
    "새서울여행사": "새서울여행사",
    "샬레코리아": "샬레코리아",
    "웅진컴퍼스": "웅진컴퍼스",
    "기븐존여행클럽": "기븐존여행클럽",
    "레저프라자": "레저프라자",
    "대원관광": "대원관광",
    "다보": "다보",
    "디다트래블": "디다트래블",
    "가고파여행": "가고파여행",
    "호텔패스": "호텔패스",
    "호텔패키지": "호텔패키지",
    "위더스컴즈": "위더스컴즈",
    "콘도닷컴": "콘도닷컴",
    "보군여행사": "보군여행사",
    "이제너두": "이제너두",
    "노랑풍선": "노랑풍선",
    "제주닷컴": "제주닷컴",
    "디어먼데이": "디어먼데이",
    "탐나오": "탐나오",
    "참좋은여행": "참좋은여행",
    "하이월드투어": "하이월드투어",
}

# ─── 세그먼트 분류 ───
def classify_segment(code_num, code_name, agent_name, file_type):
    """변경예약집계코드(숫자/알파) 기준 세그먼트 결정

    - A4, A5 → G-OTA
    - 53, 72  → OTA
    - 58      → Inbound
    - 나머지  → 코드명 그대로 (회원PKG, D멤버스, 일반단체 등)
    """
    num = (code_num or "").strip()
    name = (code_name or "").strip()

    if num in ("A4", "A5"):
        return "G-OTA"
    if num in ("53", "72"):
        return "OTA"
    if num == "58":
        return "Inbound"
    return name if name else "기타"


def extract_channel(agent_name):
    """AGENT명에서 OTA 채널 추출"""
    if not agent_name:
        return "기타"
    for keyword, channel in OTA_CHANNEL_MAP.items():
        if keyword in agent_name:
            return channel
    return "기타"


# ─── 사업장 → 권역 매핑 (BI REGION_MAP 기준) ───
PROPERTY_REGION = {
    # Vivaldi (비발디파크 권역)
    "비발디": "vivaldi", "소노펠리체": "vivaldi", "소노펫": "vivaldi",
    "펠리체": "vivaldi", "오션월드": "vivaldi",
    # Central (중부 권역)
    "델피노": "central", "양평": "central", "양양": "central",
    "삼척": "central", "단양": "central", "청송": "central",
    "천안": "central", "변산": "central", "오크밸리": "central",
    "르네블루": "central",
    # South (남부 권역)
    "여수": "south", "거제": "south", "남해": "south",
    "진도": "south", "경주": "south", "해운대": "south",
    # APAC (아시아퍼시픽 권역)
    "제주": "apac", "고양": "apac",
    "하이퐁": "apac", "괌": "apac", "하와이": "apac",
}

def get_region(prop_name):
    """사업장명 → 권역 (BI REGION_MAP 기준)"""
    if not prop_name:
        return "unknown"
    for key, region in PROPERTY_REGION.items():
        if key in prop_name:
            return region
    return "unknown"


def normalize_property(prop_name):
    """사업장명 정규화 (변경사업장명 우선)"""
    if not prop_name:
        return "미분류"
    # "02. 소노벨 비발디파크" → "소노벨 비발디파크"
    cleaned = re.sub(r'^\d+\.\s*', '', prop_name).strip()
    # " B · C" 같은 동 정보 제거 → 사업장 단위 집계
    # 하지만 변경사업장명은 이미 정리된 형태
    return cleaned if cleaned else "미분류"


def detect_file_type(filename):
    """파일명에서 데이터 타입 감지: 27, 28, 43, 44"""
    basename = os.path.basename(filename)
    if basename.startswith("27"):
        return "27"
    elif basename.startswith("28"):
        return "28"
    elif basename.startswith("43"):
        return "43"
    elif basename.startswith("44"):
        return "44"
    return None


def parse_and_aggregate(filepath, file_type, agg, min_month=None, max_month=None,
                         cancel_daily_agg=None, pickup_daily_agg=None):
    """
    단일 txt 파일 파싱 → 바로 agg 딕셔너리에 집계 (메모리 효율)
    agg 키: (사업장, 권역, 투숙월, 채널, 세그먼트, 타입)
    min_month/max_month: 'YYYYMM' 형식, 범위 밖 stay_month 스킵
    """
    encodings = ['cp949', 'euc-kr', 'utf-8']
    is_cancel = file_type in ("28", "44")
    btype = 'cancel' if is_cancel else 'booking'

    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                header_line = f.readline().strip()
                headers = header_line.split(';')
                col_map = {h.strip(): i for i, h in enumerate(headers)}

                has_change_prop = '변경사업장명' in col_map
                code_col = '변경예약집계코드명' if '변경예약집계코드명' in col_map else '예약집계명'
                code_num_col = '변경예약집계코드' if '변경예약집계코드' in col_map else '예약집계코드'

                # 컬럼 인덱스 사전 조회 (루프 최적화)
                idx_prop = col_map.get('영업장명', -1)
                idx_cprop = col_map.get('변경사업장명', -1) if has_change_prop else -1
                idx_selldate = col_map.get('판매일자', -1)  # 투숙일자 = 판매일자
                idx_checkin = col_map.get('입실일자', -1)    # fallback
                idx_code = col_map.get(code_col, -1)
                idx_code_num = col_map.get(code_num_col, -1)
                idx_agent = col_map.get('AGENT명', -1)
                idx_rooms = col_map.get('객실수', -1)
                idx_1night = col_map.get('1박객실료', -1)    # REV 계산 기준
                idx_cancel_date = col_map.get('취소일자', -1)      # col33: 28/44 파일
                idx_pickup_date = col_map.get('최초입력일자', -1)  # col27: 27/43 파일

                line_count = 0
                ok_count = 0
                error_count = 0

                for line in f:
                    line_count += 1
                    parts = line.split(';')
                    plen = len(parts)

                    try:
                        # 사업장명
                        prop_raw = parts[idx_prop] if idx_prop >= 0 and idx_prop < plen else ''
                        cprop = parts[idx_cprop].strip() if idx_cprop >= 0 and idx_cprop < plen else ''
                        prop_name = normalize_property(cprop) if cprop else normalize_property(prop_raw)

                        # 투숙일자 = 판매일자 (연박은 판매일자가 매일 반복)
                        sell_date = parts[idx_selldate].strip() if idx_selldate >= 0 and idx_selldate < plen else ''
                        if len(sell_date) < 6:
                            # fallback to 입실일자
                            sell_date = parts[idx_checkin].strip() if idx_checkin >= 0 and idx_checkin < plen else ''
                            if len(sell_date) < 6:
                                continue
                        stay_month = sell_date[:6]

                        # 월 필터
                        if min_month and stay_month < min_month:
                            continue
                        if max_month and stay_month > max_month:
                            continue

                        # 코드번호, 코드명, AGENT명
                        code_num  = parts[idx_code_num].strip() if idx_code_num >= 0 and idx_code_num < plen else ''
                        code_name = parts[idx_code].strip() if idx_code >= 0 and idx_code < plen else ''
                        agent_name = parts[idx_agent].strip() if idx_agent >= 0 and idx_agent < plen else ''

                        # 숫자
                        def _int(idx):
                            if idx >= 0 and idx < plen:
                                v = parts[idx].strip()
                                return int(v) if v else 0
                            return 0

                        rooms = _int(idx_rooms)
                        night_rate = _int(idx_1night)

                        # BI 로직:
                        # - 판매일자 기준 DB → 연박은 매일 반복 기록됨
                        # - RN = 객실수 (박수 제외, 각 행이 이미 1박 단위)
                        rn = rooms if rooms > 0 else 1

                        # REV = 1박객실료 (이미 VAT 제외된 기본 객실 요금)
                        # 1박객실료 ≈ 판매가/1.1 (검증 완료, ~1% 이내 일치)
                        rev = night_rate

                        region = get_region(prop_name)
                        channel = extract_channel(agent_name)
                        segment = classify_segment(code_num, code_name, agent_name, file_type)

                        key = (prop_name, region, stay_month, channel, segment, btype)
                        agg[key]['rn'] += rn
                        agg[key]['rev'] += rev
                        agg[key]['count'] += 1
                        ok_count += 1

                        # 취소일자 기반 집계 (28/44)
                        if is_cancel and cancel_daily_agg is not None and idx_cancel_date >= 0 and idx_cancel_date < plen:
                            cancel_date_str = parts[idx_cancel_date].strip()
                            if len(cancel_date_str) >= 8:
                                ckey = (cancel_date_str[:8], prop_name, region, segment)
                                cancel_daily_agg[ckey]['rn'] += rn
                                cancel_daily_agg[ckey]['rev'] += rev

                        # 최초입력일자 기반 픽업 집계 (27/43)
                        if not is_cancel and pickup_daily_agg is not None and idx_pickup_date >= 0 and idx_pickup_date < plen:
                            pickup_date_str = parts[idx_pickup_date].strip()
                            if len(pickup_date_str) >= 8:
                                pkey = (pickup_date_str[:8], prop_name, region, segment)
                                pickup_daily_agg[pkey]['rn'] += rn
                                pickup_daily_agg[pkey]['rev'] += rev

                    except (IndexError, ValueError):
                        error_count += 1
                        continue

                logger.info(f"  파싱 완료: {line_count:,}행 읽음, {ok_count:,}행 성공, {error_count:,}행 오류")
                return ok_count

        except UnicodeDecodeError:
            continue

    logger.error(f"  인코딩 감지 실패: {filepath}")
    return 0


def build_summary(agg, cancel_daily_agg=None, pickup_daily_agg=None):
    """집계 → JSON-serializable 구조"""

    # 1) 월별 총괄 (전체 사업장)
    monthly_total = defaultdict(lambda: {'booking_rn': 0, 'booking_rev': 0, 'cancel_rn': 0, 'cancel_rev': 0})

    # 2) 사업장별 월별
    prop_monthly = defaultdict(lambda: defaultdict(lambda: {'booking_rn': 0, 'booking_rev': 0, 'cancel_rn': 0, 'cancel_rev': 0}))

    # 3) 채널별 월별
    channel_monthly = defaultdict(lambda: defaultdict(lambda: {'booking_rn': 0, 'booking_rev': 0, 'cancel_rn': 0, 'cancel_rev': 0}))

    # 4) 권역별 월별
    region_monthly = defaultdict(lambda: defaultdict(lambda: {'booking_rn': 0, 'booking_rev': 0, 'cancel_rn': 0, 'cancel_rev': 0}))

    # 5) 세그먼트별 월별
    segment_monthly = defaultdict(lambda: defaultdict(lambda: {'booking_rn': 0, 'booking_rev': 0, 'cancel_rn': 0, 'cancel_rev': 0}))

    # 6) 사업장×채널별 월별
    prop_channel_monthly = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'booking_rn': 0, 'booking_rev': 0, 'cancel_rn': 0, 'cancel_rev': 0})))

    # 7) 권역×세그먼트별 월별
    region_segment_monthly = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'booking_rn': 0, 'booking_rev': 0, 'cancel_rn': 0, 'cancel_rev': 0})))

    # 8) 사업장×세그먼트별 월별
    prop_segment_monthly = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'booking_rn': 0, 'booking_rev': 0, 'cancel_rn': 0, 'cancel_rev': 0})))

    for (prop, region, month, channel, segment, btype), vals in agg.items():
        rn = vals['rn']
        rev = vals['rev']

        prefix = 'booking' if btype == 'booking' else 'cancel'

        monthly_total[month][f'{prefix}_rn'] += rn
        monthly_total[month][f'{prefix}_rev'] += rev

        prop_monthly[prop][month][f'{prefix}_rn'] += rn
        prop_monthly[prop][month][f'{prefix}_rev'] += rev

        channel_monthly[channel][month][f'{prefix}_rn'] += rn
        channel_monthly[channel][month][f'{prefix}_rev'] += rev

        region_monthly[region][month][f'{prefix}_rn'] += rn
        region_monthly[region][month][f'{prefix}_rev'] += rev

        segment_monthly[segment][month][f'{prefix}_rn'] += rn
        segment_monthly[segment][month][f'{prefix}_rev'] += rev

        prop_channel_monthly[prop][channel][month][f'{prefix}_rn'] += rn
        prop_channel_monthly[prop][channel][month][f'{prefix}_rev'] += rev

        region_segment_monthly[region][segment][month][f'{prefix}_rn'] += rn
        region_segment_monthly[region][segment][month][f'{prefix}_rev'] += rev

        prop_segment_monthly[prop][segment][month][f'{prefix}_rn'] += rn
        prop_segment_monthly[prop][segment][month][f'{prefix}_rev'] += rev

    def calc_adr(d):
        """
        BI 로직 적용:
        - REV: 원 → 백만원 (÷1,000,000) — 판매가/1.1은 파싱 단계에서 적용 완료
        - ADR: REV(백만원) × 1000 ÷ RNS → 천원 단위
        """
        net_rn = d.get('booking_rn', 0) - d.get('cancel_rn', 0)
        net_rev_won = d.get('booking_rev', 0) - d.get('cancel_rev', 0)  # 원 단위

        # REV: 원 → 백만원
        booking_rev_m = round(d.get('booking_rev', 0) / 1_000_000, 2)
        cancel_rev_m = round(d.get('cancel_rev', 0) / 1_000_000, 2)
        net_rev_m = round(net_rev_won / 1_000_000, 2)

        # ADR: REV(백만원) × 1000 ÷ RNS → 천원
        adr_k = round((net_rev_m * 1000) / net_rn) if net_rn > 0 else 0

        # 취소율: cancel_rn / (booking_rn + cancel_rn) * 100
        gross_rn = d.get('booking_rn', 0) + d.get('cancel_rn', 0)
        cancel_rate = round(d.get('cancel_rn', 0) / gross_rn * 100, 1) if gross_rn > 0 else 0.0

        return {
            'booking_rn': d.get('booking_rn', 0),
            'cancel_rn': d.get('cancel_rn', 0),
            'net_rn': net_rn,
            'booking_rev': booking_rev_m,   # 백만원
            'cancel_rev': cancel_rev_m,     # 백만원
            'net_rev': net_rev_m,           # 백만원
            'adr': adr_k,                   # 천원
            'cancel_rate': cancel_rate,     # %
        }

    # JSON 변환
    result = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'monthly_total': {m: calc_adr(v) for m, v in sorted(monthly_total.items())},
        'by_property': {
            p: {m: calc_adr(v) for m, v in sorted(months.items())}
            for p, months in sorted(prop_monthly.items())
        },
        'by_channel': {
            c: {m: calc_adr(v) for m, v in sorted(months.items())}
            for c, months in sorted(channel_monthly.items())
        },
        'by_region': {
            r: {m: calc_adr(v) for m, v in sorted(months.items())}
            for r, months in sorted(region_monthly.items())
        },
        'by_segment': {
            s: {m: calc_adr(v) for m, v in sorted(months.items())}
            for s, months in sorted(segment_monthly.items())
        },
        'by_property_channel': {
            p: {
                c: {m: calc_adr(v) for m, v in sorted(months.items())}
                for c, months in sorted(channels.items())
            }
            for p, channels in sorted(prop_channel_monthly.items())
        },
        'by_region_segment': {
            r: {
                s: {m: calc_adr(v) for m, v in sorted(months.items())}
                for s, months in sorted(segs.items())
            }
            for r, segs in sorted(region_segment_monthly.items())
        },
        'by_property_segment': {
            p: {
                s: {m: calc_adr(v) for m, v in sorted(months.items())}
                for s, months in sorted(segs.items())
            }
            for p, segs in sorted(prop_segment_monthly.items())
        },
    }

    # 메타 정보
    all_months = sorted(monthly_total.keys())
    all_years = sorted(set(m[:4] for m in all_months))
    all_props = sorted(prop_monthly.keys())
    all_channels = sorted(channel_monthly.keys())
    all_regions = sorted(region_monthly.keys())

    result['meta'] = {
        'years': all_years,
        'months': all_months,
        'properties': all_props,
        'channels': all_channels,
        'regions': all_regions,
        'segments': sorted(segment_monthly.keys()),
        'total_rows': sum(v['count'] for v in agg.values()),
    }

    def _to_m(raw_won):
        return round(raw_won / 1_000_000, 2)

    # 3단계: 취소일자 기반 일별 취소 집계
    if cancel_daily_agg:
        _cd = defaultdict(lambda: {'rn': 0, 'rev': 0})
        _cd_seg = defaultdict(lambda: defaultdict(lambda: {'rn': 0, 'rev': 0}))
        for (cday, prop, region, segment), vals in cancel_daily_agg.items():
            _cd[cday]['rn'] += vals['rn']
            _cd[cday]['rev'] += vals['rev']
            _cd_seg[segment][cday]['rn'] += vals['rn']
            _cd_seg[segment][cday]['rev'] += vals['rev']
        result['cancel_daily'] = {
            d: {'rn': v['rn'], 'rev': _to_m(v['rev'])}
            for d, v in sorted(_cd.items())
        }
        result['cancel_daily_by_segment'] = {
            s: {d: {'rn': v['rn'], 'rev': _to_m(v['rev'])} for d, v in sorted(days.items())}
            for s, days in sorted(_cd_seg.items())
        }

    # 4단계: 최초입력일자 기반 일별 픽업 집계
    if pickup_daily_agg:
        _pd = defaultdict(lambda: {'rn': 0, 'rev': 0})
        _pd_seg = defaultdict(lambda: defaultdict(lambda: {'rn': 0, 'rev': 0}))
        _pd_prop = defaultdict(lambda: defaultdict(lambda: {'rn': 0, 'rev': 0}))
        for (pday, prop, region, segment), vals in pickup_daily_agg.items():
            _pd[pday]['rn'] += vals['rn']
            _pd[pday]['rev'] += vals['rev']
            _pd_seg[segment][pday]['rn'] += vals['rn']
            _pd_seg[segment][pday]['rev'] += vals['rev']
            _pd_prop[prop][pday]['rn'] += vals['rn']
            _pd_prop[prop][pday]['rev'] += vals['rev']
        result['pickup_daily'] = {
            d: {'rn': v['rn'], 'rev': _to_m(v['rev'])}
            for d, v in sorted(_pd.items())
        }
        result['pickup_daily_by_segment'] = {
            s: {d: {'rn': v['rn'], 'rev': _to_m(v['rev'])} for d, v in sorted(days.items())}
            for s, days in sorted(_pd_seg.items())
        }
        result['pickup_daily_by_property'] = {
            p: {d: {'rn': v['rn'], 'rev': _to_m(v['rev'])} for d, v in sorted(days.items())}
            for p, days in sorted(_pd_prop.items())
        }

    return result


def main():
    logger.info("=" * 60)
    logger.info("온북 원시 DB 파싱 시작")
    logger.info(f"데이터 디렉토리: {RAW_DB_DIR}")
    logger.info("=" * 60)

    if not RAW_DB_DIR.exists():
        logger.error(f"데이터 디렉토리가 없습니다: {RAW_DB_DIR}")
        sys.exit(1)

    # 모든 txt 파일 수집 (symlink 폴더도 탐색)
    txt_files = sorted(
        Path(dirpath) / fname
        for dirpath, dirnames, filenames in os.walk(RAW_DB_DIR, followlinks=True)
        for fname in filenames
        if fname.lower().endswith(".txt")
    )
    logger.info(f"총 {len(txt_files)}개 txt 파일 발견")

    # 2026 파일 처리 전략:
    # - 재전송 파일(예약/취소): Jan-Mar만 파싱 (stay_month ≤ 202603)
    # - 최신 누적 스냅샷(예약/취소): Apr 이후만 파싱 (stay_month ≥ 202604)
    # - 재전송/스냅샷 중 하나만 있으면 월 필터 없이 전체 파싱
    folder_type_files = defaultdict(list)
    for fp in txt_files:
        ft = detect_file_type(fp.name)
        if ft:
            folder_type_files[(str(fp.parent), ft)].append(fp)
    def _is_retrans(fp):
        # 재전송 파일: 파일명에 날짜범위 패턴 (YYYYMMDD-YYYYMMDD) 포함
        return bool(re.search(r'\(\d{8}-\d{8}\)', fp.name))

    # file → (min_month, max_month) 또는 'skip'
    file_month_filter = {}

    for (folder, ft), fps in folder_type_files.items():
        folder_path = Path(folder)
        retrans = [fp for fp in fps if _is_retrans(fp)]
        snapshots = [fp for fp in fps if not _is_retrans(fp)]

        if retrans and snapshots:
            # 재전송 + 최신 스냅샷이 공존: 월별 분리 (예약/취소 모두 동일 규칙)
            for fp in retrans:
                file_month_filter[fp] = (None, '202603')
                logger.info(f"  재전송: ≤202603 한정 파싱: {fp.name}")
            for fp in snapshots:
                file_month_filter[fp] = ('202604', None)
                logger.info(f"  누적스냅샷: ≥202604 한정 파싱: {fp.name}")

    agg = defaultdict(lambda: {'rn': 0, 'rev': 0.0, 'count': 0})
    cancel_daily_agg = defaultdict(lambda: {'rn': 0, 'rev': 0})
    pickup_daily_agg = defaultdict(lambda: {'rn': 0, 'rev': 0})
    file_stats = {}
    total_rows = 0

    type_labels = {"27": "FIT예약", "28": "FIT취소", "43": "IB예약", "44": "IB취소"}

    for fpath in txt_files:
        file_type = detect_file_type(fpath.name)
        if not file_type:
            logger.warning(f"  스킵 (타입 불명): {fpath.name}")
            continue

        mfilter = file_month_filter.get(fpath)
        if mfilter == 'skip':
            continue

        folder_name = fpath.parent.name
        min_m = max_m = None
        if isinstance(mfilter, tuple):
            min_m, max_m = mfilter
            logger.info(f"파싱: [{type_labels.get(file_type, file_type)}] {folder_name}/{fpath.name} [월필터: {min_m or '*'}~{max_m or '*'}]")
        else:
            logger.info(f"파싱: [{type_labels.get(file_type, file_type)}] {folder_name}/{fpath.name}")

        row_count = parse_and_aggregate(str(fpath), file_type, agg, min_month=min_m, max_month=max_m,
                                         cancel_daily_agg=cancel_daily_agg, pickup_daily_agg=pickup_daily_agg)
        total_rows += row_count

        file_stats[f"{folder_name}/{fpath.name}"] = {
            'type': file_type,
            'label': type_labels.get(file_type, file_type),
            'rows': row_count,
        }

    logger.info(f"\n총 파싱 행 수: {total_rows:,}")
    logger.info(f"집계 키 수: {len(agg):,}")

    # 요약 생성
    summary = build_summary(agg, cancel_daily_agg=cancel_daily_agg, pickup_daily_agg=pickup_daily_agg)
    summary['file_stats'] = file_stats

    # JSON 출력
    output_path = OUTPUT_DIR / "db_aggregated.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info(f"\n출력: {output_path}")
    logger.info(f"연도: {summary['meta']['years']}")
    logger.info(f"사업장: {len(summary['meta']['properties'])}개")
    logger.info(f"채널: {len(summary['meta']['channels'])}개")
    logger.info(f"권역: {summary['meta']['regions']}")

    # 요약 출력
    print("\n" + "=" * 60)
    print("📊 집계 결과 요약")
    print("=" * 60)

    for year in summary['meta']['years']:
        year_months = [m for m in summary['meta']['months'] if m.startswith(year)]
        total_rn = sum(summary['monthly_total'].get(m, {}).get('net_rn', 0) for m in year_months)
        total_rev = sum(summary['monthly_total'].get(m, {}).get('net_rev', 0) for m in year_months)  # 이미 백만원
        adr = round((total_rev * 1000) / total_rn) if total_rn > 0 else 0  # 천원
        print(f"  {year}년: RN {total_rn:>10,} | REV {total_rev:>10,.0f}백만원 | ADR {adr:>6,}천원")

    print(f"\n  전체: {summary['meta']['total_rows']:,}행")
    print("=" * 60)


if __name__ == "__main__":
    main()
