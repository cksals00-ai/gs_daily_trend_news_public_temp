#!/usr/bin/env python3
"""
generate_campaign_data.py — 구글시트 CSV → docs/data/campaign_data.json

원본 시트: GS 채널 판매 보고
- 헤더: Key, 구분, 사업장, 채널, 판매기간(시작/종료), 투숙기간(시작/종료),
        KPI (객실수 실), KPI 매출 (백만원), 영업장, 상품명, 상품, 노출영역, 비고
- 위 시트는 publish-to-web CSV URL로 노출 (gid=1818134248)

생성 필드:
- total_campaigns, summer_campaigns
- by_channel_type   (채널 카테고리별 건수)
- by_month          (투숙 시작월 기준)
- channel_by_month  (카테고리 × 월)
- summer_by_channel, summer_by_property, summer_detail
- (보존) influencer_25, influencer_26, annual_plan_summer
  → 위 항목은 다른 시트에서 관리되므로 기존 JSON에서 그대로 머지

기타 카테고리 금지 — 매핑 누락 채널은 원본 채널명을 카테고리로 그대로 사용.
"""

from __future__ import annotations
import json
import sys
import csv
import re
import io
import urllib.request
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# ─── 경로 ───
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DOCS_DATA_DIR = PROJECT_DIR / "docs" / "data"
OUTPUT_JSON = DOCS_DATA_DIR / "campaign_data.json"

# ─── 데이터 소스 ───
CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vTqe7nY8vHYVVnnGR5qrl-uubCABXtmbToAuKWziuaoms14hZ3qlJuQTBWUXDmjCOU-4hd0hp6cpO_O"
    "/pub?gid=1818134248&single=true&output=csv"
)

# ─── 채널 카테고리 매핑 (specific → generic 순서로 매칭) ───
# 매핑 누락 시 원본 채널명을 그대로 카테고리로 사용 (기타 통합 금지)
CATEGORY_RULES = [
    ("인플루언서",          ["인플루언서", "펫인플루언서"]),
    ("카카오쇼핑라이브",    ["카카오쇼핑라이브"]),
    ("카카오메이커스",      ["카카오 메이커스", "카카오메이커스"]),
    ("카카오예약/톡",       ["카카오톡예약", "카카오 예약하기", "카카오 예약", "카카오톡 예약"]),
    ("티딜/톡딜",           ["톡딜", "티딜"]),
    ("CJ온스타일  럭셔리체크인", ["CJ온스타일", "럭셔리체크인"]),
    ("11번가",              ["11번가"]),
    ("와디즈",              ["와디즈"]),
    ("G마켓/옥션",          ["G마켓", "지마켓", "옥션"]),
    ("이베이(종이비행기)",  ["이베이"]),
    ("놀유니버스/놀이의발견", ["놀유니버스", "놀이의 발견", "놀이의발견"]),
    ("여기어때",            ["여기어때"]),
    ("마이리얼트립",        ["마이리얼트립"]),
    ("트립비토즈",          ["트립비토즈"]),
    ("야놀자",              ["야놀자"]),
    ("쿠팡",                ["쿠팡"]),
    ("키즈노트",            ["키즈노트"]),
    ("네이버",              ["네이버"]),
    ("프리즘",              ["프리즘"]),
    ("롯데온",              ["롯데온"]),
    ("여행사",              ["여행사", "하나투어"]),
    ("맘맘",                ["맘맘"]),
]


def categorize_channel(raw: str) -> str:
    """채널 원본 → 카테고리. 매칭 실패 시 원본을 정제하여 그대로 반환."""
    if not raw:
        return ""
    name = raw.strip()
    for category, keywords in CATEGORY_RULES:
        for kw in keywords:
            if kw in name:
                return category
    # fallback: 부가 메타(괄호) 제거 후 그대로 카테고리화
    cleaned = re.sub(r"\(.*?\)", "", name).strip(" ,/x")
    return cleaned or name


def parse_kor_date(s: str):
    """'25.11.29' / '2025-11-29' / '25/11/29' → date 객체. 실패 시 None."""
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    # YY.MM.DD
    m = re.match(r"^(\d{2,4})[\.\-/](\d{1,2})[\.\-/](\d{1,2})$", s)
    if not m:
        return None
    y, mo, d = m.group(1), int(m.group(2)), int(m.group(3))
    if len(y) == 2:
        y = 2000 + int(y)
    else:
        y = int(y)
    try:
        return datetime(y, mo, d).date()
    except ValueError:
        return None


def fetch_csv(url: str) -> list[list[str]]:
    """Google publish-to-web CSV 다운로드 → rows"""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read().decode("utf-8", errors="replace")
    return list(csv.reader(io.StringIO(data)))


def find_header_row(rows: list[list[str]]) -> int:
    """'Key' 와 '채널'이 동시에 들어있는 첫 행 인덱스."""
    for i, r in enumerate(rows):
        cells = [c.strip() for c in r]
        if "Key" in cells and "채널" in cells:
            return i
    raise RuntimeError("CSV에서 헤더 행(Key,채널 포함)을 찾지 못했습니다.")


def is_summer(stay_start, stay_end) -> bool:
    """여름 기획전 판정 — 투숙시작이 2026년 6~8월에 속하면 True
    (투숙기간 자체는 9~10월까지 길게 늘어지는 경우가 많아, 시작 기준이 더 정확)
    """
    if not stay_start:
        return False
    summer_start = datetime(2026, 6, 1).date()
    summer_end   = datetime(2026, 8, 31).date()
    return summer_start <= stay_start <= summer_end


def main():
    print(f"CSV 다운로드: {CSV_URL}")
    rows = fetch_csv(CSV_URL)
    print(f"총 {len(rows)}행 수신")

    hdr_idx = find_header_row(rows)
    headers = [c.strip() for c in rows[hdr_idx]]
    print(f"헤더 행: {hdr_idx} / 컬럼: {headers}")

    def col(name):
        try:
            return headers.index(name)
        except ValueError:
            return -1

    C_KEY  = col("Key")
    C_BUN  = col("구분")
    C_PROP = col("사업장")
    C_CH   = col("채널")
    C_SS   = col("판매기간(시작)")
    C_SE   = col("판매기간(종료)")
    C_TS   = col("투숙기간(시작)")
    C_TE   = col("투숙기간(종료)")
    C_KPI_RN  = col("KPI (객실수 실)")
    C_KPI_REV = col("KPI 매출 (백만원)")
    C_AREA = col("영업장")
    C_PNAME = col("상품명")
    C_PROD = col("상품")
    C_EXPO = col("노출영역")
    C_NOTE = col("비고")

    by_channel_type = defaultdict(int)
    by_month = defaultdict(int)
    channel_by_month = defaultdict(lambda: defaultdict(int))
    summer_by_channel = defaultdict(int)
    summer_by_property = defaultdict(int)
    summer_detail = []

    total = 0
    summer_total = 0

    for r in rows[hdr_idx + 1:]:
        if not r or len(r) <= max(C_CH, C_TS, C_TE):
            continue
        key = (r[C_KEY] or "").strip() if C_KEY >= 0 else ""
        if not key:
            continue
        ch_raw = (r[C_CH] or "").strip() if C_CH >= 0 else ""
        if not ch_raw:
            continue
        cat = categorize_channel(ch_raw)
        ts = parse_kor_date(r[C_TS]) if C_TS >= 0 else None
        te = parse_kor_date(r[C_TE]) if C_TE >= 0 else None

        total += 1
        by_channel_type[cat] += 1

        if ts:
            mkey = f"{ts.year:04d}-{ts.month:02d}"
            by_month[mkey] += 1
            channel_by_month[cat][mkey] += 1

        if is_summer(ts, te):
            summer_total += 1
            summer_by_channel[cat] += 1
            prop = (r[C_PROP] or "").strip() if C_PROP >= 0 else ""
            if prop:
                summer_by_property[prop] += 1
            ss = parse_kor_date(r[C_SS]) if C_SS >= 0 else None
            se = parse_kor_date(r[C_SE]) if C_SE >= 0 else None
            entry = {
                "구분":       (r[C_BUN]  or "").strip() if C_BUN  >= 0 else "",
                "사업장":     (r[C_PROP] or "").strip() if C_PROP >= 0 else "",
                "채널":       ch_raw,
                "판매시작":   ss.isoformat() if ss else "",
                "판매종료":   se.isoformat() if se else "",
                "투숙시작":   ts.isoformat() if ts else "",
                "투숙종료":   te.isoformat() if te else "",
                "영업장":     (r[C_AREA]  or "").strip() if C_AREA  >= 0 else "",
                "상품":       (r[C_PROD]  or "").strip() if C_PROD  >= 0 else "",
            }
            # 상품명·노출영역은 비어있지 않을 때만
            if C_PNAME >= 0 and (r[C_PNAME] or "").strip():
                entry["상품명"] = r[C_PNAME].strip()
            if C_EXPO >= 0 and (r[C_EXPO] or "").strip():
                entry["노출영역"] = r[C_EXPO].strip()
            summer_detail.append(entry)

    # 정렬
    by_channel_type_sorted   = dict(sorted(by_channel_type.items(),   key=lambda x: -x[1]))
    by_month_sorted          = dict(sorted(by_month.items()))
    channel_by_month_sorted  = {k: dict(sorted(v.items())) for k, v in channel_by_month.items()}
    summer_by_channel_sorted = dict(sorted(summer_by_channel.items(), key=lambda x: -x[1]))
    summer_by_property_sorted= dict(sorted(summer_by_property.items(), key=lambda x: -x[1]))
    summer_detail.sort(key=lambda d: (d.get("판매시작") or "", d.get("사업장") or ""))

    output = {
        "total_campaigns":   total,
        "summer_campaigns":  summer_total,
        "by_channel_type":   by_channel_type_sorted,
        "by_month":          by_month_sorted,
        "summer_by_channel": summer_by_channel_sorted,
        "summer_by_property":summer_by_property_sorted,
        "summer_detail":     summer_detail,
        "channel_by_month":  channel_by_month_sorted,
    }

    # ─ 보존 필드(다른 소스에서 관리) ─
    # influencer_25, influencer_26, annual_plan_summer는 본 CSV에 포함되지 않으므로
    # 기존 campaign_data.json에서 머지 (없으면 빈 객체로 둠)
    if OUTPUT_JSON.exists():
        try:
            existing = json.loads(OUTPUT_JSON.read_text(encoding="utf-8"))
            for key in ("influencer_25", "influencer_26", "annual_plan_summer"):
                if key in existing and key not in output:
                    output[key] = existing[key]
        except Exception as e:
            print(f"  기존 JSON 머지 실패(무시): {e}")

    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✓ {OUTPUT_JSON} 생성 완료")
    print(f"  total_campaigns: {total}")
    print(f"  summer_campaigns: {summer_total}")
    print(f"  채널 카테고리: {len(by_channel_type_sorted)}개")
    print(f"  월 분포: {sorted(by_month_sorted.keys())}")


if __name__ == "__main__":
    main()
