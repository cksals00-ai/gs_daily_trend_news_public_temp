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


def parse_file(filepath, file_type):
    """
    단일 txt 파일 파싱 → 행 단위 딕셔너리 리스트
    메모리 효율을 위해 필요한 컬럼만 추출
    """
    rows = []
    encodings = ['cp949', 'euc-kr', 'utf-8']

    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                header_line = f.readline().strip()
                headers = header_line.split(';')

                # 컬럼 인덱스 매핑
                col_map = {h.strip(): i for i, h in enumerate(headers)}

                # 필요 컬럼
                is_cancel = file_type in ("28", "44")

                # 공통 필수 컬럼
                required = ['영업장명', '입실일자']

                # 변경사업장명 (있으면 사용)
                has_change_prop = '변경사업장명' in col_map

                # 예약집계코드명 컬럼 (27/43은 '변경예약집계코드명', 28/44는 '예약집계명')
                code_col = '변경예약집계코드명' if '변경예약집계코드명' in col_map else '예약집계명'

                line_count = 0
                error_count = 0

                for line in f:
                    line_count += 1
                    parts = line.strip().split(';')

                    if len(parts) < len(headers) - 2:  # 약간의 여유
                        error_count += 1
                        continue

                    try:
                        prop_raw = parts[col_map['영업장명']] if '영업장명' in col_map else ''
                        change_prop = parts[col_map['변경사업장명']] if has_change_prop and col_map['변경사업장명'] < len(parts) else ''
                        prop_name = normalize_property(change_prop) if change_prop.strip() else normalize_property(prop_raw)

                        checkin = parts[col_map['입실일자']] if '입실일자' in col_map and col_map['입실일자'] < len(parts) else ''

                        code_name = ''
                        if code_col in col_map and col_map[code_col] < len(parts):
                            code_name = parts[col_map[code_col]]

                        agent_name = ''
                        if 'AGENT명' in col_map and col_map['AGENT명'] < len(parts):
                            agent_name = parts[col_map['AGENT명']]

                        # 숫자 컬럼
                        def safe_int(col):
                            if col in col_map and col_map[col] < len(parts):
                                val = parts[col_map[col]].strip()
                                try:
                                    return int(val) if val else 0
                                except ValueError:
                                    return 0
                            return 0

                        nights = safe_int('박수')
                        rooms = safe_int('객실수')
                        rate_1night = safe_int('1박객실료')
                        sale_price = safe_int('판매가')

                        # RN = 박수 × 객실수
                        rn = nights * rooms if nights > 0 and rooms > 0 else max(1, rooms)

                        # REV = 판매가 (원 단위)
                        rev = sale_price

                        # 입실월 추출 (YYYYMMDD → YYYYMM)
                        if len(checkin) >= 6:
                            stay_month = checkin[:6]
                        else:
                            continue

                        # 입실년도
                        stay_year = checkin[:4] if len(checkin) >= 4 else ''

                        cancel_date = ''
                        if is_cancel and '취소일자' in col_map and col_map['취소일자'] < len(parts):
                            cancel_date = parts[col_map['취소일자']]

                        row = {
                            'prop': prop_name,
                            'region': get_region(prop_name),
                            'stay_month': stay_month,
                            'stay_year': stay_year,
                            'checkin': checkin,
                            'code_name': code_name,
                            'agent': agent_name,
                            'channel': extract_channel(agent_name),
                            'segment': classify_segment(code_name, agent_name, file_type),
                            'rn': rn,
                            'rev': rev,
                            'rate': rate_1night,
                            'type': 'cancel' if is_cancel else 'booking',
                            'file_type': file_type,
                        }
                        if cancel_date:
                            row['cancel_date'] = cancel_date

                        rows.append(row)

                    except (IndexError, ValueError) as e:
                        error_count += 1
                        continue

                logger.info(f"  파싱 완료: {line_count:,}행 읽음, {len(rows):,}행 성공, {error_count:,}행 오류")
                return rows

        except UnicodeDecodeError:
            continue

    logger.error(f"  인코딩 감지 실패: {filepath}")
    return []


def aggregate(rows):
    """
    행 단위 → 집계 딕셔너리
    키: (사업장, 권역, 투숙월, 채널, 세그먼트, 타입)
    값: RN합계, REV합계
    """
    agg = defaultdict(lambda: {'rn': 0, 'rev': 0, 'count': 0})

    for r in rows:
        key = (r['prop'], r['region'], r['stay_month'], r['channel'], r['segment'], r['type'])
        agg[key]['rn'] += r['rn']
        agg[key]['rev'] += r['rev']
        agg[key]['count'] += 1

    return agg


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

    all_rows = []
    file_stats = {}

    for fpath in txt_files:
        file_type = detect_file_type(fpath.name)
        if not file_type:
            logger.warning(f"  스킵 (타입 불명): {fpath.name}")
            continue

        # 2023 폴더의 28번은 잘못 배치된 파일 → 스킵
        folder_name = fpath.parent.name
        if folder_name == "2023" and file_type == "28":
            logger.warning(f"  스킵 (2023/28번 = 잘못 배치): {fpath.name}")
            continue

        type_labels = {"27": "FIT예약", "28": "FIT취소", "43": "IB예약", "44": "IB취소"}
        logger.info(f"파싱: [{type_labels.get(file_type, file_type)}] {folder_name}/{fpath.name}")

        rows = parse_file(str(fpath), file_type)
        all_rows.extend(rows)

        file_stats[f"{folder_name}/{fpath.name}"] = {
            'type': file_type,
            'label': type_labels.get(file_type, file_type),
            'rows': len(rows),
        }

    logger.info(f"\n총 파싱 행 수: {len(all_rows):,}")

    # 집계
    logger.info("집계 중...")
    agg = aggregate(all_rows)
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
