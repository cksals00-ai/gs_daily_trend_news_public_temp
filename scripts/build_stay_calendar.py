#!/usr/bin/env python3
"""
build_stay_calendar.py — 투숙일 기준 시즌/요일별 분석 데이터 생성

raw_db txt 파일을 스트리밍 파싱하여 stay_date 단위로 집계:
- 27/43: booking (RN, REV, 리드타임=판매일자-최초입력일자)
- 28/44: cancel (RN, REV, 취소수)

각 stay_date에 시즌(주중/금/토/연휴/골드)·요일(월~일) 라벨 부여.

출력: docs/data/stay_calendar.json
"""
import os, sys, json, re, logging, glob, unicodedata
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# 경로 — 메인 레포 data/raw_db 우선, 없으면 worktree
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
RAW_DB_DIR = PROJECT_DIR / "data" / "raw_db"
HOLIDAYS_PATH = PROJECT_DIR / "data" / "holidays_kr.json"
OUTPUT_PATH = PROJECT_DIR / "docs" / "data" / "stay_calendar.json"

# 분석 대상 stay_date 범위 (요일/시즌별 분석은 2년치면 충분)
MIN_STAY_DATE = "20240101"
MAX_STAY_DATE = "20271231"

DOW_LABELS = ["월", "화", "수", "목", "금", "토", "일"]  # weekday(): 0=월

# 골드 시즌 정의 — (월일 시작, 월일 끝) 튜플 목록
# 한국 리조트 일반적 성수기 정의 기반 (연휴와 중복되면 연휴가 우선)
GOLD_RANGES = [
    ("0125", "0205"),  # 설날 전후 (실제 설은 연휴로 분류)
    ("0725", "0820"),  # 여름 성수기
    ("1224", "0103"),  # 연말연시 (year-wrap)
]


def _in_gold(mmdd):
    for start, end in GOLD_RANGES:
        if start <= end:
            if start <= mmdd <= end:
                return True
        else:  # year wrap (e.g., 1224 ~ 0103)
            if mmdd >= start or mmdd <= end:
                return True
    return False


def detect_file_type(filename):
    name = os.path.basename(filename)
    if name.startswith("27"):
        return "27"
    if name.startswith("28"):
        return "28"
    if name.startswith("43"):
        return "43"
    if name.startswith("44"):
        return "44"
    return None


def _is_retrans(fp):
    return bool(re.search(r"\(\d{8}-\d{8}\)", fp.name))


def _parse_ymd(s):
    try:
        return datetime.strptime(s[:8], "%Y%m%d")
    except (ValueError, TypeError):
        return None


def load_holidays():
    if not HOLIDAYS_PATH.exists():
        logger.warning(f"공휴일 파일 없음: {HOLIDAYS_PATH}")
        return {}
    with open(HOLIDAYS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("holidays", {})


def classify_season(date_str, holidays):
    """date_str: 'YYYYMMDD' → 시즌 라벨 (주중/금/토/연휴/골드)

    우선순위: 연휴 > 골드 > 토 > 금 > 주중(월~목, 일)
    - 연휴: date 가 공휴일 OR (date+1 이 공휴일이고 평일+금/토에 한해)
    - 골드: 정의된 성수기 기간 (연말연시·여름성수기 등)
    - 토 / 금: 요일
    - 그 외: 주중 (일요일 포함 — 리조트 관행)
    """
    d = _parse_ymd(date_str)
    if not d:
        return "주중"
    mmdd = date_str[4:8]
    dow = d.weekday()  # 0=월
    next_d = (d + timedelta(days=1)).strftime("%Y%m%d")

    is_holiday = date_str in holidays
    next_is_holiday = next_d in holidays

    # 연휴: 공휴일 당일 OR 공휴일 전날
    if is_holiday or next_is_holiday:
        return "연휴"
    # 골드: 정의된 성수기 (단, 연휴/공휴일 외)
    if _in_gold(mmdd):
        return "골드"
    # 요일 기반
    if dow == 5:
        return "토"
    if dow == 4:
        return "금"
    return "주중"


def parse_file(filepath, file_type, agg_by_date, min_stay=MIN_STAY_DATE, max_stay=MAX_STAY_DATE,
               min_month=None, max_month=None):
    """단일 파일 스트리밍 파싱 → agg_by_date 업데이트

    agg_by_date[stay_date] = {
        booking_rn, booking_rev, cancel_rn, cancel_rev,
        lead_sum, lead_cnt   (booking 의 리드타임 가중합)
    }
    """
    encodings = ["cp949", "euc-kr", "utf-8"]
    is_cancel = file_type in ("28", "44")

    for enc in encodings:
        try:
            with open(filepath, "r", encoding=enc) as f:
                header_line = f.readline().strip()
                headers = header_line.split(";")
                col_map = {h.strip(): i for i, h in enumerate(headers)}

                idx_selldate = col_map.get("판매일자", -1)
                idx_checkin = col_map.get("입실일자", -1)
                idx_rooms = col_map.get("객실수", -1)
                idx_1night = col_map.get("1박객실료", -1)
                idx_pickup = col_map.get("최초입력일자", -1)
                idx_cancel_date = col_map.get("취소일자", -1)
                idx_member = col_map.get("회원명", -1)
                idx_user = col_map.get("이용자명", -1)
                idx_code_num = col_map.get("변경예약집계코드", col_map.get("예약집계코드", -1))

                seen_hashes = set()
                line_count = 0
                ok = 0

                for line in f:
                    line_count += 1
                    h = hash(line.rstrip("\n\r"))
                    if h in seen_hashes:
                        continue
                    seen_hashes.add(h)

                    parts = line.split(";")
                    plen = len(parts)
                    try:
                        sd = parts[idx_selldate].strip() if 0 <= idx_selldate < plen else ""
                        if len(sd) < 8:
                            sd = parts[idx_checkin].strip() if 0 <= idx_checkin < plen else ""
                            if len(sd) < 8:
                                continue
                        sd = sd[:8]
                        if sd < min_stay or sd > max_stay:
                            continue

                        # 월 필터 (재전송/스냅샷 중복 회피)
                        stay_month = sd[:6]
                        if min_month and stay_month < min_month:
                            continue
                        if max_month and stay_month > max_month:
                            continue

                        # 거래처 중복 제거 (Inbound 제외)
                        code_num = parts[idx_code_num].strip() if 0 <= idx_code_num < plen else ""
                        if 0 <= idx_member < plen and 0 <= idx_user < plen:
                            mem = parts[idx_member].strip()
                            usr = parts[idx_user].strip()
                            if mem and usr and mem == usr and code_num != "58":
                                continue
                        # 매출조정 제거
                        if 0 <= idx_member < plen and "매출조정" in parts[idx_member]:
                            continue
                        if 0 <= idx_user < plen and "매출조정" in parts[idx_user]:
                            continue

                        rooms = 0
                        night_rate = 0
                        if 0 <= idx_rooms < plen:
                            v = parts[idx_rooms].strip()
                            rooms = int(v) if v else 0
                        if 0 <= idx_1night < plen:
                            v = parts[idx_1night].strip()
                            night_rate = int(v) if v else 0

                        rn = rooms if rooms > 0 else 1
                        rev = int(night_rate * rn / 1.1)

                        bucket = agg_by_date[sd]
                        if is_cancel:
                            bucket["cancel_rn"] += rn
                            bucket["cancel_rev"] += rev
                        else:
                            bucket["booking_rn"] += rn
                            bucket["booking_rev"] += rev
                            # 리드타임 (booking 전용)
                            if 0 <= idx_pickup < plen:
                                entry_str = parts[idx_pickup].strip()
                                d_sell = _parse_ymd(sd)
                                d_entry = _parse_ymd(entry_str)
                                if d_sell and d_entry:
                                    lt = (d_sell - d_entry).days
                                    if 0 <= lt <= 365:
                                        bucket["lead_sum"] += lt * rn
                                        bucket["lead_cnt"] += rn
                        ok += 1
                    except Exception:
                        continue
                logger.info(f"  {filepath.name}: {ok:,} rows / {line_count:,} lines")
            return ok
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.error(f"파싱 실패 {filepath}: {e}")
            return 0
    return 0


def collect_files():
    """raw_db 디렉토리에서 27/28/43/44 txt 수집 + 월 필터 부여 (재전송 vs 스냅샷)"""
    if not RAW_DB_DIR.exists():
        logger.error(f"raw_db 디렉토리 없음: {RAW_DB_DIR}")
        return []
    txt_files = sorted(
        Path(dirpath) / fname
        for dirpath, _, filenames in os.walk(RAW_DB_DIR, followlinks=True)
        for fname in filenames
        if fname.lower().endswith(".txt")
    )

    folder_type_files = defaultdict(list)
    for fp in txt_files:
        ft = detect_file_type(fp.name)
        if ft:
            folder_type_files[(str(fp.parent), ft)].append(fp)

    file_month_filter = {}
    for (folder, ft), fps in folder_type_files.items():
        retrans = [fp for fp in fps if _is_retrans(fp)]
        snaps = [fp for fp in fps if not _is_retrans(fp)]
        # parse_raw_db.py와 동일한 분리 로직: 재전송=≤202603, 스냅샷=≥202604
        if retrans and snaps:
            for fp in retrans:
                file_month_filter[fp] = (None, "202603")
            for fp in snaps:
                file_month_filter[fp] = ("202604", None)

    result = []
    for fp in txt_files:
        ft = detect_file_type(fp.name)
        if not ft:
            continue
        min_m, max_m = file_month_filter.get(fp, (None, None))
        result.append((fp, ft, min_m, max_m))
    return result


def build_dates_payload(agg_by_date, holidays):
    """agg_by_date → 정렬된 dates 리스트 (시즌·dow 라벨 포함)"""
    dates = []
    for sd in sorted(agg_by_date.keys()):
        d = _parse_ymd(sd)
        if not d:
            continue
        b = agg_by_date[sd]
        booking_rn = b["booking_rn"]
        cancel_rn = b["cancel_rn"]
        # ADR 분모는 booking_rn (실제 투숙·예약 객실)
        # 매출은 백만원 단위로 환산
        rev_m = round(b["booking_rev"] / 1_000_000, 2)
        adr = int(b["booking_rev"] / booking_rn) if booking_rn > 0 else 0
        # 취소율: 28/44(취소건) / (27/43 + 28/44) → 그동안 예약된 객실 중 취소 비율
        total_ever = booking_rn + cancel_rn
        cancel_rate = round(cancel_rn / total_ever, 4) if total_ever > 0 else 0
        lead_avg = round(b["lead_sum"] / b["lead_cnt"], 1) if b["lead_cnt"] > 0 else None
        dates.append({
            "d": sd,
            "dow": d.weekday(),
            "season": classify_season(sd, holidays),
            "rn": booking_rn,
            "rev_m": rev_m,
            "adr": adr,
            "c_rn": cancel_rn,
            "cancel_rate": cancel_rate,
            "lead_avg": lead_avg,
        })
    return dates


def main():
    logger.info("=" * 60)
    logger.info("투숙 캘린더 빌드 시작")
    logger.info(f"raw_db: {RAW_DB_DIR}")
    logger.info(f"stay_date 범위: {MIN_STAY_DATE}~{MAX_STAY_DATE}")
    logger.info("=" * 60)

    holidays = load_holidays()
    logger.info(f"공휴일 {len(holidays)}건 로드")

    files = collect_files()
    if not files:
        logger.error("raw_db txt 파일을 찾지 못했습니다. 빈 출력 생성.")
        agg_by_date = defaultdict(lambda: {"booking_rn": 0, "booking_rev": 0,
                                            "cancel_rn": 0, "cancel_rev": 0,
                                            "lead_sum": 0, "lead_cnt": 0})
    else:
        agg_by_date = defaultdict(lambda: {"booking_rn": 0, "booking_rev": 0,
                                            "cancel_rn": 0, "cancel_rev": 0,
                                            "lead_sum": 0, "lead_cnt": 0})
        for fp, ft, min_m, max_m in files:
            label = {"27": "FIT예약", "28": "FIT취소", "43": "IB예약", "44": "IB취소"}.get(ft, ft)
            scope = f" [{min_m or '*'}~{max_m or '*'}]" if (min_m or max_m) else ""
            logger.info(f"파싱: {label} {fp.name}{scope}")
            parse_file(fp, ft, agg_by_date, min_month=min_m, max_month=max_m)

    dates = build_dates_payload(agg_by_date, holidays)
    logger.info(f"수집 stay_dates: {len(dates):,}일")

    # 데이터 신뢰 구간: rn>0인 day 가 있는 max stay_date 까지 (그 뒤는 미래)
    max_with_data = ""
    today = datetime.now().strftime("%Y%m%d")
    for r in dates:
        if r["d"] <= today and r["rn"] > 0:
            if r["d"] > max_with_data:
                max_with_data = r["d"]

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "stay_range": {
            "min": MIN_STAY_DATE,
            "max": MAX_STAY_DATE,
            "data_max": max_with_data,
        },
        "dow_labels": DOW_LABELS,
        "season_order": ["주중", "금", "토", "연휴", "골드"],
        "gold_ranges": GOLD_RANGES,
        "dates": dates,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    logger.info(f"✓ 저장: {OUTPUT_PATH} ({OUTPUT_PATH.stat().st_size / 1024:.1f} KB)")


if __name__ == "__main__":
    main()
