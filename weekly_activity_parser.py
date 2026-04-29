#!/usr/bin/env python3
"""
주간활동일지 파서 — DATA 시트 → JSON 변환
GS 채널 기획전 데이터를 데일리 리포트용 JSON으로 파싱
"""

import json
import re
from datetime import datetime, date
from typing import Optional

# ─── GS 채널 분류 ─────────────────────────────────────────────
GS_CHANNELS = ['Inbound', 'OTA', 'G-OTA']

# 채널명 → GS 채널 분류 매핑
CHANNEL_CATEGORY_MAP = {
    # Inbound (자사/직접 유입)
    'Inbound': [
        '카카오톡', '카카오 예약하기', '카카오톡예약하기', '카카오 메이커스',
        '카카오메이커스', '카카오쇼핑라이브', '톡딜', '네이버쇼핑라이브',
    ],
    # OTA (Online Travel Agency)
    'OTA': [
        '여기어때', '야놀자', '마이리얼트립', '트립비토즈', '쿠팡',
        '놀유니버스', '놀이의발견', '놀이의 발견', '노랑풍선',
        '하나투어', '티딜', '와디즈',
    ],
    # G-OTA (종합 이커머스 / 인플루언서)
    'G-OTA': [
        '11번가', 'G마켓', '지마켓', '옥션', '롯데온', '이베이',
        '인플루언서', '프리즘', '맘맘', '키즈노트', '펫인플루언서',
        'CJ온스타일',
    ],
}

# 구분 → 사업본부 매핑
DIVISION_MAP = {
    '비발디파크': '비발디파크',
    '한국남부':   '한국남부',
    '한국중부':   '한국중부',
    '아시아퍼시픽': '아시아퍼시픽',
    '전 사업장':  '전사',
}


def classify_channel(channel_name: str) -> str:
    """채널명으로 GS 채널 카테고리 분류"""
    ch = channel_name.strip()
    for gs_cat, keywords in CHANNEL_CATEGORY_MAP.items():
        for kw in keywords:
            if kw in ch:
                return gs_cat
    return 'OTA'  # 기본값


def parse_date(val) -> Optional[str]:
    """datetime 또는 문자열 → ISO 날짜 문자열"""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.strftime('%Y-%m-%d')
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, str):
        val = val.strip()
        for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d'):
            try:
                return datetime.strptime(val, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
    return str(val) if val else None


def safe_str(val) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def parse_data_sheet(ws, max_row: int = 338) -> list[dict]:
    """
    DATA 시트를 파싱하여 기획전 레코드 리스트 반환

    Parameters:
        ws: openpyxl worksheet (DATA 시트)
        max_row: 데이터 마지막 행 (기본 338)

    Returns:
        list of dict — 각 기획전 레코드
    """
    records = []

    for row in ws.iter_rows(min_row=15, max_row=max_row, min_col=2, max_col=13):
        # B~M 열 (구분, 사업장, 채널, 판매시작, 판매종료, 투숙시작, 투숙종료, 영업장, 상품명, 상품, 노출영역, 비고)
        division   = safe_str(row[0].value)   # B: 구분
        site       = safe_str(row[1].value)   # C: 사업장
        channel    = safe_str(row[2].value)   # D: 채널
        sale_start = parse_date(row[3].value) # E: 판매기간(시작)
        sale_end   = parse_date(row[4].value) # F: 판매기간(종료)
        stay_start = parse_date(row[5].value) # G: 투숙기간(시작)
        stay_end   = parse_date(row[6].value) # H: 투숙기간(종료)
        branch     = safe_str(row[7].value)   # I: 영업장
        prod_name  = safe_str(row[8].value)   # J: 상품명
        product    = safe_str(row[9].value)   # K: 상품
        exposure   = safe_str(row[10].value)  # L: 노출영역
        note       = safe_str(row[11].value)  # M: 비고

        if not channel:
            continue

        gs_channel = classify_channel(channel)

        record = {
            'division':       DIVISION_MAP.get(division, division),
            'site':           site,
            'channel':        channel,
            'gs_channel':     gs_channel,
            'sale_period': {
                'start': sale_start,
                'end':   sale_end,
            },
            'stay_period': {
                'start': stay_start,
                'end':   stay_end,
            },
            'branch':         branch,
            'product_name':   prod_name,
            'product':        product,
            'exposure':       exposure,
            'note':           note,
        }
        records.append(record)

    return records


def filter_active_promotions(records: list[dict], target_date: str) -> list[dict]:
    """특정 날짜에 판매 진행 중인 기획전만 필터"""
    active = []
    for r in records:
        s = r['sale_period']['start']
        e = r['sale_period']['end']
        if s and e and s <= target_date <= e:
            active.append(r)
    return active


def build_daily_summary(records: list[dict], target_date: str) -> dict:
    """
    데일리 리포트용 요약 JSON 생성

    Returns:
        {
            "report_date": "2026-04-28",
            "total_active": 15,
            "by_gs_channel": { "Inbound": [...], "OTA": [...], "G-OTA": [...] },
            "by_site": { "비발디": [...], ... },
            "by_division": { "비발디파크": [...], ... }
        }
    """
    active = filter_active_promotions(records, target_date)

    by_gs = {ch: [] for ch in GS_CHANNELS}
    by_site = {}
    by_division = {}

    for r in active:
        gs = r['gs_channel']
        by_gs[gs].append(r)
        by_site.setdefault(r['site'], []).append(r)
        by_division.setdefault(r['division'], []).append(r)

    return {
        'report_date':    target_date,
        'total_active':   len(active),
        'by_gs_channel':  {k: len(v) for k, v in by_gs.items()},
        'by_site':        {k: len(v) for k, v in sorted(by_site.items())},
        'by_division':    {k: len(v) for k, v in sorted(by_division.items())},
        'active_promotions': active,
    }


# ─── CLI 사용 ─────────────────────────────────────────────────
if __name__ == '__main__':
    import sys
    import openpyxl

    xlsx_path = sys.argv[1] if len(sys.argv) > 1 else 'weekly_activity.xlsx'
    target = sys.argv[2] if len(sys.argv) > 2 else datetime.now().strftime('%Y-%m-%d')

    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    ws = wb['DATA']

    records = parse_data_sheet(ws)
    summary = build_daily_summary(records, target)

    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
