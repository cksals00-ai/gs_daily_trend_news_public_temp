#!/usr/bin/env python3
"""
구글 시트 자동 수집 스크립트 — CSV export URL 활용
주간활동일지 2026 구글 시트에서 DATA 시트를 CSV로 다운로드 → JSON 파싱

사용법:
    python fetch_weekly_activity.py [--date 2026-04-28] [--output daily_summary.json]
"""

import csv
import io
import json
import os
import sys
import logging
from datetime import datetime
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError

# ─── 설정 ─────────────────────────────────────────────────────

# 구글 시트 정보
SPREADSHEET_ID = '1MJNoET5yTLRYejV61ZQuIaHbvYHBHYJ31eMpg7SUDEw'

# 시트별 gid (구글 시트 URL의 #gid= 파라미터)
SHEET_GIDS = {
    'DATA':            '1986408525',
    '개요':             None,   # 필요시 추가
    'cal':             None,
    '25 인플루언서':    None,
    '26 인플루언서':    None,
    '26년 연간PLAN':    None,
}

# CSV export URL 템플릿
CSV_EXPORT_URL = (
    'https://docs.google.com/spreadsheets/d/{sheet_id}'
    '/export?format=csv&gid={gid}'
)

# GS 채널 분류
CHANNEL_CATEGORY_MAP = {
    'Inbound': [
        '카카오톡', '카카오 예약하기', '카카오톡예약하기', '카카오 메이커스',
        '카카오메이커스', '카카오쇼핑라이브', '톡딜', '네이버쇼핑라이브',
    ],
    'OTA': [
        '여기어때', '야놀자', '마이리얼트립', '트립비토즈', '쿠팡',
        '놀유니버스', '놀이의발견', '놀이의 발견', '노랑풍선',
        '하나투어', '티딜', '와디즈',
    ],
    'G-OTA': [
        '11번가', 'G마켓', '지마켓', '옥션', '롯데온', '이베이',
        '인플루언서', '프리즘', '맘맘', '키즈노트', '펫인플루언서',
        'CJ온스타일',
    ],
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# ─── 유틸리티 ─────────────────────────────────────────────────

def classify_channel(channel_name: str) -> str:
    ch = channel_name.strip()
    for gs_cat, keywords in CHANNEL_CATEGORY_MAP.items():
        for kw in keywords:
            if kw in ch:
                return gs_cat
    return 'OTA'


def parse_date_str(val: str) -> Optional[str]:
    """CSV 날짜 문자열 → ISO date"""
    if not val or not val.strip():
        return None
    val = val.strip()
    for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(val, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return val


def fetch_csv(gid: str, timeout: int = 30) -> str:
    """구글 시트에서 CSV 다운로드"""
    url = CSV_EXPORT_URL.format(sheet_id=SPREADSHEET_ID, gid=gid)
    logger.info(f'Fetching: {url}')

    req = Request(url, headers={'User-Agent': 'GS-DailyReport/1.0'})
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            # BOM 제거
            if raw[:3] == b'\xef\xbb\xbf':
                raw = raw[3:]
            return raw.decode('utf-8')
    except URLError as e:
        logger.error(f'CSV 다운로드 실패: {e}')
        raise


def save_csv_cache(csv_text: str, sheet_name: str, cache_dir: str = 'data/cache'):
    """CSV 원본 캐시 저장"""
    os.makedirs(cache_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    path = os.path.join(cache_dir, f'{sheet_name}_{ts}.csv')
    with open(path, 'w', encoding='utf-8') as f:
        f.write(csv_text)
    logger.info(f'캐시 저장: {path}')
    return path


# ─── CSV 파싱 ─────────────────────────────────────────────────

def parse_data_csv(csv_text: str) -> list[dict]:
    """
    DATA 시트 CSV → 기획전 레코드 리스트

    CSV 구조 (14행이 헤더):
      B: 구분, C: 사업장, D: 채널, E: 판매기간(시작), F: 판매기간(종료),
      G: 투숙기간(시작), H: 투숙기간(종료), I: 영업장, J: 상품명,
      K: 상품, L: 노출영역, M: 비고
    """
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)

    # 헤더 행 찾기 — '구분'이 포함된 행
    header_idx = None
    for i, row in enumerate(rows):
        if any('구분' in str(c) for c in row):
            header_idx = i
            break

    if header_idx is None:
        logger.warning('헤더 행을 찾을 수 없습니다. 기본값(13) 사용')
        header_idx = 13  # 0-based for row 14

    records = []
    for row in rows[header_idx + 1:]:
        if len(row) < 12:
            continue

        # B~M 은 index 1~12 (A=0)
        channel = row[3].strip() if len(row) > 3 and row[3].strip() else None
        if not channel:
            continue

        record = {
            'division':     row[1].strip() or None,
            'site':         row[2].strip() or None,
            'channel':      channel,
            'gs_channel':   classify_channel(channel),
            'sale_period': {
                'start': parse_date_str(row[4]) if len(row) > 4 else None,
                'end':   parse_date_str(row[5]) if len(row) > 5 else None,
            },
            'stay_period': {
                'start': parse_date_str(row[6]) if len(row) > 6 else None,
                'end':   parse_date_str(row[7]) if len(row) > 7 else None,
            },
            'branch':       row[8].strip() or None if len(row) > 8 else None,
            'product_name': row[9].strip() or None if len(row) > 9 else None,
            'product':      row[10].strip() or None if len(row) > 10 else None,
            'exposure':     row[11].strip() or None if len(row) > 11 else None,
            'note':         row[12].strip() or None if len(row) > 12 else None,
        }
        records.append(record)

    logger.info(f'파싱된 레코드: {len(records)}건')
    return records


def build_daily_summary(records: list[dict], target_date: str) -> dict:
    """진행중인 기획전 기준 데일리 요약 생성"""
    active = []
    for r in records:
        s = r['sale_period']['start']
        e = r['sale_period']['end']
        if s and e and s <= target_date <= e:
            active.append(r)

    gs_channels = ['Inbound', 'OTA', 'G-OTA']
    by_gs = {ch: [] for ch in gs_channels}
    by_site = {}
    by_division = {}

    for r in active:
        by_gs[r['gs_channel']].append(r)
        by_site.setdefault(r['site'], []).append(r)
        by_division.setdefault(r['division'], []).append(r)

    return {
        'report_date':    target_date,
        'fetched_at':     datetime.now().isoformat(),
        'total_active':   len(active),
        'by_gs_channel':  {k: len(v) for k, v in by_gs.items()},
        'by_site':        {k: len(v) for k, v in sorted(by_site.items())},
        'by_division':    {k: len(v) for k, v in sorted(by_division.items())},
        'active_promotions': active,
    }


# ─── 메인 ─────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description='구글 시트 주간활동일지 자동 수집')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'),
                        help='리포트 기준일 (YYYY-MM-DD)')
    parser.add_argument('--output', default=None,
                        help='출력 JSON 파일 경로')
    parser.add_argument('--cache', action='store_true',
                        help='CSV 원본 캐시 저장')
    parser.add_argument('--csv-file', default=None,
                        help='로컬 CSV 파일 사용 (네트워크 대신)')
    args = parser.parse_args()

    # 1) CSV 가져오기
    gid = SHEET_GIDS['DATA']
    if args.csv_file:
        logger.info(f'로컬 CSV 사용: {args.csv_file}')
        with open(args.csv_file, 'r', encoding='utf-8') as f:
            csv_text = f.read()
    else:
        csv_text = fetch_csv(gid)

    if args.cache:
        save_csv_cache(csv_text, 'DATA')

    # 2) 파싱
    records = parse_data_csv(csv_text)

    # 3) 데일리 요약 생성
    summary = build_daily_summary(records, args.date)

    # 4) 출력
    output_json = json.dumps(summary, ensure_ascii=False, indent=2, default=str)

    if args.output:
        os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(output_json)
        logger.info(f'결과 저장: {args.output}')
    else:
        print(output_json)


if __name__ == '__main__':
    main()
