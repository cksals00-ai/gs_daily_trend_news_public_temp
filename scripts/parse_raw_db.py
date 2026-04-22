#!/usr/bin/env python3
"""
parse_raw_db.py — 온북 원시 DB txt 파일 파싱 → JSON 집계
- 27번: FIT(OTA/GOTA) 예약
- 28번: FIT 취소
- 43번: Inbound 예약
- 44번: Inbound 취소
CP949 인코딩, 세미콜론(;) 구분
"""
import os, sys, json, re, logging, glob
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

# ─── OTA 채널 매핑 (AGENT명 → 채널) ───
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
}

# ─── 세그먼트 분류 (변경예약집계코드명 → 세그먼트) ───
def classify_segment(code_name, agent_name, file_type):
    """파일 타입 + 코드명 + AGENT명으로 세그먼트 결정"""
    if file_type in ("43", "44"):
        return "Inbound"

    code_upper = (code_name or "").upper()
    agent_upper = (agent_name or "").upper()

    # GOTA 판별
    if "GOTA" in code_upper or "GOTA" in agent_upper:
        return "G-OTA"
    # OTA 판별
    if "OTA" in agent_upper or "온라인" in code_upper or "사업장대매점" in code_name:
        return "OTA"

    return "OTA"  # 27/28은 기본 OTA


def extract_channel(agent_name):
    """AGENT명에서 OTA 채널 추출"""
    if not agent_name:
        return "기타"
    for keyword, channel in OTA_CHANNEL_MAP.items():
        if keyword in agent_name:
            return channel
    return "기타"


# ─── 사업장 → 권역 매핑 ───
PROPERTY_REGION = {
    # 비발디파크
    "소노벨 비발디파크": "비발디파크", "소노캄 비발디": "비발디파크",
    "비발디파크": "비발디파크", "소노펠리체": "비발디파크",
    # 중부
    "소노캄 델피노": "한국중부", "델피노": "한국중부",
    "소노캄 양평": "한국중부", "양평": "한국중부",
    "소노벨 양양": "한국중부", "양양": "한국중부",
    "소노벨 삼척": "한국중부", "삼척": "한국중부",
    "소노벨 천안": "한국중부", "천안": "한국중부",
    "소노문 단양": "한국중부", "단양": "한국중부",
    "소노벨 경주": "한국중부",
    # 남부
    "소노캄 여수": "한국남부", "여수": "한국남부",
    "소노캄 거제": "한국남부", "거제": "한국남부",
    "소노벨 거제": "한국남부",
    "소노문 진도": "한국남부", "진도": "한국남부",
    "소노벨 남해": "한국남부", "남해": "한국남부",
    # APAC
    "소노캄 제주": "아시아퍼시픽", "제주": "아시아퍼시픽",
    "소노캄 고양": "아시아퍼시픽", "고양": "아시아퍼시픽",
}

def get_region(prop_name):
    """사업장명 → 권역"""
    if not prop_name:
        return "기타"
    for key, region in PROPERTY_REGION.items():
        if key in prop_name:
            return region
    return "기타"


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


def parse_and_aggregate(filepath, file_type, agg):
    """
    단일 txt 파일 파싱 → 바로 agg 딕셔너리에 집계 (메모리 효율)
    agg 키: (사업장, 권역, 투숙월, 채널, 세그먼트, 타입)
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

                # 컬럼 인덱스 사전 조회 (루프 최적화)
                idx_prop = col_map.get('영업장명', -1)
                idx_cprop = col_map.get('변경사업장명', -1) if has_change_prop else -1
                idx_checkin = col_map.get('입실일자', -1)
                idx_code = col_map.get(code_col, -1)
                idx_agent = col_map.get('AGENT명', -1)
                idx_nights = col_map.get('박수', -1)
                idx_rooms = col_map.get('객실수', -1)
                idx_price = col_map.get('판매가', -1)

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

                        # 입실일자
                        checkin = parts[idx_checkin].strip() if idx_checkin >= 0 and idx_checkin < plen else ''
                        if len(checkin) < 6:
                            continue
                        stay_month = checkin[:6]

                        # 코드명, AGENT명
                        code_name = parts[idx_code].strip() if idx_code >= 0 and idx_code < plen else ''
                        agent_name = parts[idx_agent].strip() if idx_agent >= 0 and idx_agent < plen else ''

                        # 숫자
                        def _int(idx):
                            if idx >= 0 and idx < plen:
                                v = parts[idx].strip()
                                return int(v) if v else 0
                            return 0

                        nights = _int(idx_nights)
                        rooms = _int(idx_rooms)
                        sale_price = _int(idx_price)

                        rn = nights * rooms if nights > 0 and rooms > 0 else max(1, rooms)
                        rev = sale_price

                        region = get_region(prop_name)
                        channel = extract_channel(agent_name)
                        segment = classify_segment(code_name, agent_name, file_type)

                        key = (prop_name, region, stay_month, channel, segment, btype)
                        agg[key]['rn'] += rn
                        agg[key]['rev'] += rev
                        agg[key]['count'] += 1
                        ok_count += 1

                    except (IndexError, ValueError):
                        error_count += 1
                        continue

                logger.info(f"  파싱 완료: {line_count:,}행 읽음, {ok_count:,}행 성공, {error_count:,}행 오류")
                return ok_count

        except UnicodeDecodeError:
            continue

    logger.error(f"  인코딩 감지 실패: {filepath}")
    return 0


def build_summary(agg):
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

    def calc_adr(d):
        """net_rn과 ADR 계산"""
        net_rn = d.get('booking_rn', 0) - d.get('cancel_rn', 0)
        net_rev = d.get('booking_rev', 0) - d.get('cancel_rev', 0)
        adr = round(net_rev / net_rn) if net_rn > 0 else 0
        return {
            'booking_rn': d.get('booking_rn', 0),
            'cancel_rn': d.get('cancel_rn', 0),
            'net_rn': net_rn,
            'booking_rev': d.get('booking_rev', 0),
            'cancel_rev': d.get('cancel_rev', 0),
            'net_rev': net_rev,
            'adr': adr,
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

    return result


def main():
    logger.info("=" * 60)
    logger.info("온북 원시 DB 파싱 시작")
    logger.info(f"데이터 디렉토리: {RAW_DB_DIR}")
    logger.info("=" * 60)

    if not RAW_DB_DIR.exists():
        logger.error(f"데이터 디렉토리가 없습니다: {RAW_DB_DIR}")
        sys.exit(1)

    # 모든 txt 파일 수집
    txt_files = sorted(RAW_DB_DIR.rglob("*.txt"))
    logger.info(f"총 {len(txt_files)}개 txt 파일 발견")

    agg = defaultdict(lambda: {'rn': 0, 'rev': 0, 'count': 0})
    file_stats = {}
    total_rows = 0

    type_labels = {"27": "FIT예약", "28": "FIT취소", "43": "IB예약", "44": "IB취소"}

    for fpath in txt_files:
        file_type = detect_file_type(fpath.name)
        if not file_type:
            logger.warning(f"  스킵 (타입 불명): {fpath.name}")
            continue

        folder_name = fpath.parent.name

        logger.info(f"파싱: [{type_labels.get(file_type, file_type)}] {folder_name}/{fpath.name}")

        row_count = parse_and_aggregate(str(fpath), file_type, agg)
        total_rows += row_count

        file_stats[f"{folder_name}/{fpath.name}"] = {
            'type': file_type,
            'label': type_labels.get(file_type, file_type),
            'rows': row_count,
        }

    logger.info(f"\n총 파싱 행 수: {total_rows:,}")
    logger.info(f"집계 키 수: {len(agg):,}")

    # 요약 생성
    summary = build_summary(agg)
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
        total_rev = sum(summary['monthly_total'].get(m, {}).get('net_rev', 0) for m in year_months)
        adr = round(total_rev / total_rn) if total_rn > 0 else 0
        print(f"  {year}년: RN {total_rn:>10,} | REV {total_rev/1_000_000:>10,.0f}백만원 | ADR {adr/1000:>6,.1f}천원")

    print(f"\n  전체: {summary['meta']['total_rows']:,}행")
    print("=" * 60)


if __name__ == "__main__":
    main()
