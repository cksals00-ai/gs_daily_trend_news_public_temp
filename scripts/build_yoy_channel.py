#!/usr/bin/env python3
"""build_yoy_channel.py — 채널별 동기간 OTB 추출

- 27/43 booking 파일: 최초입력일자 ≤ base_date(연도별 기준일) 인 레코드만 집계
- 28/44 cancel 파일: 최초입력일자 ≤ base_date AND 취소일자 > base_date 인 레코드를 보정값(adjustment)으로 가산
- 결과: by_channel_yoy[year][channel][stay_month] = {orig_rn, orig_rev_m, adj_rn, adj_rev_m, net_rn, net_rev_m}
- 출력: docs/data/db_yoy_channel.json + data/db_yoy_channel.json

기준일: 어제(today-1) 의 MM-DD 를 각 연도에 적용. 2026/05/03 실행 시 base = MM=05 DD=02.
"""
from __future__ import annotations
import os, sys, json, logging
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta

# parse_raw_db 의 헬퍼 재사용
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from parse_raw_db import (  # noqa: E402
    classify_segment, extract_channel, detect_file_type,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─── 경로 (메인 리포의 raw_db 사용) ───
PROJECT_DIR = SCRIPT_DIR.parent.parent.parent.parent  # worktree → main repo
if not (PROJECT_DIR / "data" / "raw_db").exists():
    # fallback: maybe running from main repo
    PROJECT_DIR = SCRIPT_DIR.parent
RAW_DB_DIR = PROJECT_DIR / "data" / "raw_db"

WORKTREE_DIR = SCRIPT_DIR.parent  # 출력은 worktree 기준
OUTPUT_DOCS = WORKTREE_DIR / "docs" / "data" / "db_yoy_channel.json"
OUTPUT_DATA = WORKTREE_DIR / "data" / "db_yoy_channel.json"

HISTORICAL_YEARS = ["2022", "2023", "2024", "2025", "2026"]


def _proc_booking_file(fp: Path, base_date_str: str, by_ch_month):
    """27/43 booking 파일에서 최초입력일자 ≤ base 인 레코드만 (channel, stay_month)별 집계"""
    encodings = ['cp949', 'euc-kr', 'utf-8']
    for enc in encodings:
        try:
            with open(fp, 'r', encoding=enc) as f:
                header = f.readline().strip().split(';')
                col = {h.strip(): i for i, h in enumerate(header)}
                code_col = '변경예약집계코드명' if '변경예약집계코드명' in col else '예약집계명'
                code_num_col = '변경예약집계코드' if '변경예약집계코드' in col else '예약집계코드'
                idx_pickup = col.get('최초입력일자', -1)
                idx_selldate = col.get('판매일자', -1)
                idx_checkin = col.get('입실일자', -1)
                idx_code_num = col.get(code_num_col, -1)
                idx_code = col.get(code_col, -1)
                idx_agent = col.get('AGENT명', -1)
                idx_rooms = col.get('객실수', -1)
                idx_1night = col.get('1박객실료', -1)
                idx_member = col.get('회원명', -1)
                idx_user = col.get('이용자명', -1)
                file_type = detect_file_type(fp.name)
                seen = set()
                ok = 0
                for line in f:
                    s = line.rstrip('\n\r')
                    h = hash(s)
                    if h in seen:
                        continue
                    seen.add(h)
                    parts = line.split(';')
                    plen = len(parts)
                    try:
                        if idx_pickup < 0 or idx_pickup >= plen:
                            continue
                        pickup = parts[idx_pickup].strip()
                        if len(pickup) < 8 or pickup[:8] > base_date_str:
                            continue
                        # 거래처제거
                        if idx_member >= 0 and idx_user >= 0 and idx_member < plen and idx_user < plen:
                            mn = parts[idx_member].strip()
                            un = parts[idx_user].strip()
                            code_num_v = parts[idx_code_num].strip() if 0 <= idx_code_num < plen else ''
                            if mn and un and mn == un and code_num_v != '58':
                                continue
                            if '매출조정' in mn or '매출조정' in un:
                                continue
                        sell = parts[idx_selldate].strip() if 0 <= idx_selldate < plen else ''
                        if len(sell) < 6:
                            sell = parts[idx_checkin].strip() if 0 <= idx_checkin < plen else ''
                            if len(sell) < 6:
                                continue
                        stay_month = sell[:6]
                        code_num = parts[idx_code_num].strip() if 0 <= idx_code_num < plen else ''
                        code_name = parts[idx_code].strip() if 0 <= idx_code < plen else ''
                        agent_nm = parts[idx_agent].strip() if 0 <= idx_agent < plen else ''
                        # OTB 세그먼트 필터 (OTA, G-OTA, Inbound)
                        seg = classify_segment(code_num, code_name, agent_nm, file_type)
                        if seg not in ("OTA", "G-OTA", "Inbound"):
                            continue
                        ch = extract_channel(agent_nm)
                        rooms_s = parts[idx_rooms].strip() if 0 <= idx_rooms < plen else ''
                        rooms = int(rooms_s) if rooms_s else 0
                        rate_s = parts[idx_1night].strip() if 0 <= idx_1night < plen else ''
                        night_rate = int(rate_s) if rate_s else 0
                        rn = rooms if rooms > 0 else 1
                        rev = int(night_rate * rn / 1.1)
                        slot = by_ch_month[ch][stay_month]
                        slot['orig_rn'] += rn
                        slot['orig_rev'] += rev
                        ok += 1
                    except (IndexError, ValueError):
                        continue
                logger.info(f"    booking ok={ok:,} ({fp.name})")
                return
        except UnicodeDecodeError:
            continue
    logger.error(f"  encoding failed: {fp}")


def _proc_cancel_file(fp: Path, base_date_str: str, by_ch_month):
    """28/44 cancel 파일에서 최초입력일자 ≤ base AND 취소일자 > base 인 레코드를 (channel, stay_month) 별로 보정값 가산"""
    encodings = ['cp949', 'euc-kr', 'utf-8']
    for enc in encodings:
        try:
            with open(fp, 'r', encoding=enc) as f:
                header = f.readline().strip().split(';')
                col = {h.strip(): i for i, h in enumerate(header)}
                code_col = '변경예약집계코드명' if '변경예약집계코드명' in col else '예약집계명'
                code_num_col = '변경예약집계코드' if '변경예약집계코드' in col else '예약집계코드'
                idx_pickup = col.get('최초입력일자', -1)
                idx_cancel = col.get('취소일자', -1)
                idx_selldate = col.get('판매일자', -1)
                idx_checkin = col.get('입실일자', -1)
                idx_code_num = col.get(code_num_col, -1)
                idx_code = col.get(code_col, -1)
                idx_agent = col.get('AGENT명', -1)
                idx_rooms = col.get('객실수', -1)
                idx_1night = col.get('1박객실료', -1)
                file_type = detect_file_type(fp.name)
                ok = 0
                for line in f:
                    parts = line.split(';')
                    plen = len(parts)
                    try:
                        if idx_pickup < 0 or idx_pickup >= plen or idx_cancel < 0 or idx_cancel >= plen:
                            continue
                        pickup = parts[idx_pickup].strip()
                        cancel = parts[idx_cancel].strip()
                        if len(pickup) < 8 or len(cancel) < 8:
                            continue
                        if pickup[:8] > base_date_str:
                            continue
                        if cancel[:8] <= base_date_str:
                            continue
                        sell = parts[idx_selldate].strip() if 0 <= idx_selldate < plen else ''
                        if len(sell) < 6:
                            sell = parts[idx_checkin].strip() if 0 <= idx_checkin < plen else ''
                            if len(sell) < 6:
                                continue
                        stay_month = sell[:6]
                        code_num = parts[idx_code_num].strip() if 0 <= idx_code_num < plen else ''
                        code_name = parts[idx_code].strip() if 0 <= idx_code < plen else ''
                        agent_nm = parts[idx_agent].strip() if 0 <= idx_agent < plen else ''
                        seg = classify_segment(code_num, code_name, agent_nm, file_type)
                        if seg not in ("OTA", "G-OTA", "Inbound"):
                            continue
                        ch = extract_channel(agent_nm)
                        rooms_s = parts[idx_rooms].strip() if 0 <= idx_rooms < plen else ''
                        rooms = int(rooms_s) if rooms_s else 0
                        rate_s = parts[idx_1night].strip() if 0 <= idx_1night < plen else ''
                        night_rate = int(rate_s) if rate_s else 0
                        rn = rooms if rooms > 0 else 1
                        # 취소건 보정: 1박객실료 × 객실수 (parse_raw_db parse_yoy_adjustments 와 동일)
                        slot = by_ch_month[ch][stay_month]
                        slot['adj_rn'] += rn
                        slot['adj_rev'] += night_rate * rn
                        ok += 1
                    except (IndexError, ValueError):
                        continue
                logger.info(f"    cancel adj ok={ok:,} ({fp.name})")
                return
        except UnicodeDecodeError:
            continue
    logger.error(f"  encoding failed: {fp}")


def _new_slot():
    return {'orig_rn': 0, 'orig_rev': 0, 'adj_rn': 0, 'adj_rev': 0}


def process_year(year: str, base_date_str: str):
    by_ch_month = defaultdict(lambda: defaultdict(_new_slot))
    year_dir = RAW_DB_DIR / year
    if not year_dir.exists():
        logger.warning(f"  {year} dir 없음: {year_dir}")
        return None
    files = sorted(year_dir.glob("*.txt"))
    booking_files = [f for f in files if detect_file_type(f.name) in ("27", "43")]
    cancel_files = [f for f in files if detect_file_type(f.name) in ("28", "44")]
    logger.info(f"[{year}] base={base_date_str} | booking={len(booking_files)} cancel={len(cancel_files)}")
    for fp in booking_files:
        _proc_booking_file(fp, base_date_str, by_ch_month)
    for fp in cancel_files:
        _proc_cancel_file(fp, base_date_str, by_ch_month)

    # 정리: 백만원 단위 환산, net 계산
    out = {}
    for ch, months in by_ch_month.items():
        if ch == '기타':
            continue
        out[ch] = {}
        for m, v in months.items():
            net_rn = v['orig_rn'] + v['adj_rn']
            net_rev = v['orig_rev'] + v['adj_rev']
            out[ch][m] = {
                'orig_rn': v['orig_rn'],
                'adj_rn': v['adj_rn'],
                'net_rn': net_rn,
                'orig_rev_m': round(v['orig_rev'] / 1_000_000, 2),
                'adj_rev_m': round(v['adj_rev'] / 1_000_000, 2),
                'net_rev_m': round(net_rev / 1_000_000, 2),
            }
    return out


def write_output(payload):
    OUTPUT_DOCS.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_DOCS, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    OUTPUT_DATA.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_DATA, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f"  saved {OUTPUT_DOCS} ({OUTPUT_DOCS.stat().st_size:,}B)")


def main():
    only_years = sys.argv[1:] if len(sys.argv) > 1 else HISTORICAL_YEARS
    yesterday = datetime.now() - timedelta(days=1)
    payload = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'years': {},
    }
    # 진행 중에도 가용 데이터 보이도록 기존 결과 머지
    if OUTPUT_DOCS.exists():
        try:
            with open(OUTPUT_DOCS, 'r', encoding='utf-8') as f:
                existing = json.load(f)
                if isinstance(existing.get('years'), dict):
                    payload['years'].update(existing['years'])
                logger.info(f"  기존 결과 로드: years={list(payload['years'].keys())}")
        except Exception as e:
            logger.warning(f"  기존 결과 로드 실패: {e}")

    for year in only_years:
        try:
            base_dt = yesterday.replace(year=int(year))
        except ValueError:
            base_dt = yesterday.replace(year=int(year), day=28)
        base_date_str = base_dt.strftime('%Y%m%d')
        base_mmdd = base_dt.strftime('%m%d')
        result = process_year(year, base_date_str)
        if result is None:
            continue
        payload['years'][year] = {
            'base_date': base_mmdd,
            'base_date_full': base_date_str,
            'by_channel': result,
        }
        # 매 연도마다 즉시 write (incremental)
        write_output(payload)
        logger.info(f"[{year}] DONE — channels={len(result)}")

    logger.info("ALL DONE")


if __name__ == "__main__":
    main()
