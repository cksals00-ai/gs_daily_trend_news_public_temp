#!/usr/bin/env python3
"""
generate_campaign_data.py — 구글시트 CSV → docs/data/campaign_data.json

원본 시트: GS 채널 판매 보고
- 헤더: Key, 구분, 사업장, 채널, 판매기간(시작/종료), 투숙기간(시작/종료),
        KPI (객실수 실), KPI 매출 (백만원), 영업장, 상품명, 상품, 노출영역, 비고
- 위 시트는 publish-to-web CSV URL로 노출 (gid=1818134248)

추가 (2026-05): Key 서브시트 연동
- 각 행의 Key 값(예: "1","2"...)은 같은 워크북 내 서브시트의 시트명
- 서브시트에 패키지코드(86XXXXXX 회원코드) 정의
- pubhtml에서 시트명→gid를 자동 발견, 각 서브시트 CSV를 받아 패키지코드 파싱
- 미발행 시트는 경고 후 스킵 (인프라가 먼저 구축, 데이터는 점진적으로 채워지는 구조)

생성 필드:
- total_campaigns, summer_campaigns
- by_channel_type   (채널 카테고리별 건수)
- by_month          (투숙 시작월 기준)
- channel_by_month  (카테고리 × 월)
- summer_by_channel, summer_by_property, summer_detail
- events            (전체 기획전 행 + Key별 패키지코드 — 실적 매칭용)
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
PUBLISH_ID = (
    "2PACX-1vTqe7nY8vHYVVnnGR5qrl-uubCABXtmbToAuKWziuaoms14hZ3qlJuQTBWUXDmjCOU-4hd0hp6cpO_O"
)
CSV_URL = (
    f"https://docs.google.com/spreadsheets/d/e/{PUBLISH_ID}"
    "/pub?gid=1818134248&single=true&output=csv"
)
PUBHTML_URL = f"https://docs.google.com/spreadsheets/d/e/{PUBLISH_ID}/pubhtml"


def sub_sheet_csv_url(gid: str) -> str:
    return (
        f"https://docs.google.com/spreadsheets/d/e/{PUBLISH_ID}"
        f"/pub?gid={gid}&single=true&output=csv"
    )


# 패키지코드: 86으로 시작하는 7~9자리 숫자 (온북 회원번호 컨벤션)
PKG_CODE_RE = re.compile(r"^86\d{4,7}$")

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


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


def discover_sheet_gids() -> dict[str, str]:
    """pubhtml 인덱스 페이지에서 모든 발행된 시트의 (시트명 → gid) 매핑 추출.
    items.push({name:"X", pageUrl:"...gid=N..."}) 패턴을 파싱.
    """
    try:
        html = fetch_text(PUBHTML_URL)
    except Exception as e:
        print(f"  pubhtml 로드 실패(스킵): {e}")
        return {}
    pat = re.compile(
        r'items\.push\(\{name:\s*"([^"]+)",\s*pageUrl:\s*"([^"]+)"',
        re.IGNORECASE,
    )
    name_to_gid: dict[str, str] = {}
    for name, url in pat.findall(html):
        unescaped = url.replace(r"\/", "/").replace(r"\x3d", "=").replace(r"\x26", "&")
        m = re.search(r"gid=(\d+)", unescaped)
        if m:
            name_to_gid[name.strip()] = m.group(1)
    return name_to_gid


def parse_sub_sheet_codes(rows: list[list[str]]) -> list[str]:
    """Key 서브시트의 행에서 패키지코드(86XXXXXX) 추출.
    구조: 헤더 안내문 → '📦 패키지코드' 마커 → 이후 행에 코드 나열.
    마커를 찾지 못하면 전체 행을 스캔. 중복은 순서 유지하며 dedupe.
    """
    seen: set[str] = set()
    result: list[str] = []
    marker_idx = -1
    for i, row in enumerate(rows):
        joined = " ".join(c.strip() for c in row)
        if "패키지코드" in joined and ("📦" in joined or "붙여넣기" in joined):
            marker_idx = i
            break
    start = marker_idx + 1 if marker_idx >= 0 else 0
    for row in rows[start:]:
        for cell in row:
            v = (cell or "").strip()
            if PKG_CODE_RE.match(v) and v not in seen:
                seen.add(v)
                result.append(v)
    return result


def parse_sub_sheet_meta(rows: list[list[str]]) -> dict[str, str]:
    """서브시트 상단의 '기획전명', '카테고리' 같은 라벨-값 쌍 추출."""
    meta: dict[str, str] = {}
    for row in rows:
        cells = [c.strip() for c in row]
        # 라벨이 보통 column 1, 값이 column 2에 있음
        for i in range(len(cells) - 1):
            label = cells[i]
            value = cells[i + 1]
            if not label or not value:
                continue
            if label in ("기획전명", "카테고리") and label not in meta:
                meta[label] = value
    return meta


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
    events: list[dict] = []  # 전체 기획전 (Key별 패키지코드 매칭용)
    keys_in_order: list[str] = []
    keys_seen: set[str] = set()

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
        ss = parse_kor_date(r[C_SS]) if C_SS >= 0 else None
        se = parse_kor_date(r[C_SE]) if C_SE >= 0 else None

        total += 1
        by_channel_type[cat] += 1

        if ts:
            mkey = f"{ts.year:04d}-{ts.month:02d}"
            by_month[mkey] += 1
            channel_by_month[cat][mkey] += 1

        prop = (r[C_PROP] or "").strip() if C_PROP >= 0 else ""
        bun  = (r[C_BUN]  or "").strip() if C_BUN  >= 0 else ""
        area = (r[C_AREA] or "").strip() if C_AREA >= 0 else ""
        prod = (r[C_PROD] or "").strip() if C_PROD >= 0 else ""
        pname = (r[C_PNAME] or "").strip() if C_PNAME >= 0 else ""
        expo = (r[C_EXPO] or "").strip() if C_EXPO >= 0 else ""
        note = (r[C_NOTE] or "").strip() if C_NOTE >= 0 else ""
        kpi_rn = (r[C_KPI_RN] or "").strip() if C_KPI_RN >= 0 else ""
        kpi_rev = (r[C_KPI_REV] or "").strip() if C_KPI_REV >= 0 else ""

        events.append({
            "key":       key,
            "구분":       bun,
            "사업장":     prop,
            "채널":       ch_raw,
            "채널카테고리": cat,
            "판매시작":   ss.isoformat() if ss else "",
            "판매종료":   se.isoformat() if se else "",
            "투숙시작":   ts.isoformat() if ts else "",
            "투숙종료":   te.isoformat() if te else "",
            "영업장":     area,
            "상품":       prod,
            "상품명":     pname,
            "노출영역":   expo,
            "비고":       note,
            "KPI_RN":     kpi_rn,
            "KPI_REV_M":  kpi_rev,
        })
        if key not in keys_seen:
            keys_seen.add(key)
            keys_in_order.append(key)

        if is_summer(ts, te):
            summer_total += 1
            summer_by_channel[cat] += 1
            if prop:
                summer_by_property[prop] += 1
            entry = {
                "구분":       bun,
                "사업장":     prop,
                "채널":       ch_raw,
                "판매시작":   ss.isoformat() if ss else "",
                "판매종료":   se.isoformat() if se else "",
                "투숙시작":   ts.isoformat() if ts else "",
                "투숙종료":   te.isoformat() if te else "",
                "영업장":     area,
                "상품":       prod,
            }
            # 상품명·노출영역은 비어있지 않을 때만
            if pname:
                entry["상품명"] = pname
            if expo:
                entry["노출영역"] = expo
            summer_detail.append(entry)

    # ─ Key 서브시트에서 패키지코드 매핑 ─
    print(f"pubhtml에서 시트 인덱스 발견 중: {PUBHTML_URL}")
    name_to_gid = discover_sheet_gids()
    print(f"  발행된 시트 {len(name_to_gid)}개 발견")

    key_to_codes: dict[str, list[str]] = {}
    key_to_meta: dict[str, dict[str, str]] = {}
    fetched = 0
    skipped_unpublished = 0
    skipped_empty = 0
    for key in keys_in_order:
        gid = name_to_gid.get(key)
        if not gid:
            skipped_unpublished += 1
            continue
        try:
            sub_text = fetch_text(sub_sheet_csv_url(gid))
            sub_rows = list(csv.reader(io.StringIO(sub_text)))
            codes = parse_sub_sheet_codes(sub_rows)
            meta  = parse_sub_sheet_meta(sub_rows)
            if codes:
                key_to_codes[key] = codes
                fetched += 1
            else:
                skipped_empty += 1
            if meta:
                key_to_meta[key] = meta
        except Exception as e:
            print(f"  Key={key} (gid={gid}) 페치 실패: {e}")
    print(f"  Key 서브시트 결과: {fetched}건 패키지코드 적재 / "
          f"미발행 {skipped_unpublished} / 빈시트 {skipped_empty}")

    # events에 패키지코드 머지 + 동일 Key의 모든 row가 같은 코드를 공유
    pkg_used_by: dict[str, list[str]] = defaultdict(list)  # 코드 → [keys] (중복 추적)
    for ev in events:
        codes = key_to_codes.get(ev["key"], [])
        ev["package_codes"] = codes
        meta = key_to_meta.get(ev["key"], {})
        if meta.get("기획전명") and not ev.get("노출영역"):
            ev["노출영역"] = meta["기획전명"]
        if meta.get("카테고리"):
            ev["서브카테고리"] = meta["카테고리"]
        for c in codes:
            if ev["key"] not in pkg_used_by[c]:
                pkg_used_by[c].append(ev["key"])

    duplicated = {c: keys for c, keys in pkg_used_by.items() if len(keys) > 1}
    if duplicated:
        print(f"  ⚠ 다중 Key에 등록된 패키지코드 {len(duplicated)}건 (실적 합산 시 첫 Key에 귀속):")
        for c, keys in list(duplicated.items())[:10]:
            print(f"    {c} → keys {keys}")

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
        "events":            events,
        "key_to_codes":      key_to_codes,
        "duplicate_codes":   duplicated,
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
    print(f"  events: {len(events)}건 / Key 서브시트 적재: {fetched}건 / "
          f"매핑된 패키지코드: {sum(len(v) for v in key_to_codes.values())}개")


if __name__ == "__main__":
    main()
