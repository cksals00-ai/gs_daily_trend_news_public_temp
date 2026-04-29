#!/usr/bin/env python3
"""
Daily Booking Report PDF 파서
============================
pdfplumber로 Daily_Booking_Report PDF를 파싱하여
사업장별 RNs, OCC%, Budget, 당일변동 등을 추출합니다.

PDF 구조:
  - 4페이지: 당월~+3개월 (예: 4월, 5월, 6월, 7월)
  - 페이지당 25개 사업장 (Grand Total 포함)
  - 각 사업장: RNs행 + OCC%행
  - RNs행: Budget Actual (diff) 누적예약 ▲/▼ 변동 ...일별...
  - OCC행: 전체RN Budget OCC% 전년OCC% 증감 누적OCC% ...일별OCC%...

© 2026 GS팀
"""
import json
import logging
import re
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"

# ── 사업장 매핑 (행 인덱스 고정) ──
# 각 사업장은 PDF에서 고정된 행 위치를 가짐
# RNs 행과 OCC 행이 번갈아 나옴
PROPERTY_MAP = [
    {"name": "Grand Total",     "rns_line": 10, "occ_line": 11, "region": "total"},
    {"name": "소노벨 비발디파크",   "rns_line": 13, "occ_line": 15, "region": "vivaldi"},
    {"name": "소노캄 비발디파크",   "rns_line": 17, "occ_line": 21, "region": "vivaldi"},
    {"name": "소노펫 비발디파크",   "rns_line": 24, "occ_line": 26, "region": "vivaldi"},
    {"name": "소노펠리체 비발디파크", "rns_line": 28, "occ_line": 30, "region": "vivaldi"},
    {"name": "소노펠리체빌리지 비발디파크", "rns_line": 32, "occ_line": 34, "region": "vivaldi"},
    {"name": "소노벨 양평",       "rns_line": 35, "occ_line": 36, "region": "central"},
    {"name": "델피노",           "rns_line": 37, "occ_line": 38, "region": "central"},
    {"name": "쏠비치 양양",       "rns_line": 39, "occ_line": 40, "region": "central"},
    {"name": "쏠비치 삼척",       "rns_line": 41, "occ_line": 42, "region": "central"},
    {"name": "소노벨 단양",       "rns_line": 43, "occ_line": 44, "region": "central"},
    {"name": "소노캄 경주",       "rns_line": 45, "occ_line": 46, "region": "south"},
    {"name": "소노벨 청송",       "rns_line": 47, "occ_line": 48, "region": "south"},
    {"name": "소노벨 천안",       "rns_line": 49, "occ_line": 50, "region": "central"},
    {"name": "소노벨 변산",       "rns_line": 51, "occ_line": 52, "region": "central"},
    {"name": "소노캄 여수",       "rns_line": 53, "occ_line": 54, "region": "south"},
    {"name": "소노캄 거제",       "rns_line": 55, "occ_line": 56, "region": "south"},
    {"name": "쏠비치 진도",       "rns_line": 57, "occ_line": 58, "region": "south"},
    {"name": "소노벨 제주",       "rns_line": 59, "occ_line": 60, "region": "south"},
    {"name": "소노캄 제주",       "rns_line": 61, "occ_line": 62, "region": "south"},
    {"name": "소노캄 고양",       "rns_line": 63, "occ_line": 64, "region": "central"},
    {"name": "소노문 해운대",     "rns_line": 65, "occ_line": 66, "region": "south"},
    {"name": "쏠비치 남해",       "rns_line": 67, "occ_line": 68, "region": "south"},
    {"name": "파나크 영덕",       "rns_line": 70, "occ_line": 72, "region": "south"},
    {"name": "르네블루",          "rns_line": 73, "occ_line": 74, "region": "south"},
    # 팔라티움 해운대는 PDF에 포함되지 않음. OCC는 scripts/parse_palatium_rooms.py가
    # data/palatium_rooma/ 의 사용가능 객실 현황 xlsx에서 직접 산출하여
    # docs/data/palatium_room_availability.json 및 occ_data.json("25.팔라티움")에 반영.
]


def parse_rns_line(line: str) -> dict:
    """RNs 행에서 Budget, Actual, diff, 누적예약, 당일변동 추출

    예시: "Grand Total RNs 183,238 151,133 (12,351) 170,887 ▲ 617 3,904 0 ..."
    또는: "RNs 13,900 16,497 (2,078) 11,822 ▲ 47 31 0 ..."
    """
    result = {
        "budget_rns": 0,
        "ly_actual": 0,
        "vs_budget": 0,
        "actual_rns": 0,
        "daily_change": 0,
        "daily_change_dir": "",
        "total_rn_capacity": 0,
        "budget_per_day": 0,
    }

    # RNs 이후의 숫자 부분만 추출
    rns_idx = line.find("RNs")
    if rns_idx == -1:
        return result

    after_rns = line[rns_idx + 3:].strip()

    # 모든 숫자(콤마 포함), 괄호 숫자, ▲▼ 패턴을 순서대로 추출
    # Budget, LY Actual, (vs Budget), Actual, ▲/▼ daily_change
    tokens = re.findall(r'\([\d,]+\)|▲\s*[\d,]+|▼\s*[\d,]+|[\d,]+', after_rns)

    if len(tokens) < 5:
        return result

    def to_int(s):
        s = s.replace(",", "").replace("(", "").replace(")", "").strip()
        s = re.sub(r'[▲▼]\s*', '', s)
        try:
            return int(s)
        except ValueError:
            return 0

    result["budget_rns"] = to_int(tokens[0])
    result["ly_actual"] = to_int(tokens[1])

    # vs_budget: 괄호면 음수
    vs_str = tokens[2]
    if vs_str.startswith("("):
        result["vs_budget"] = -to_int(vs_str)
    else:
        result["vs_budget"] = to_int(vs_str)

    result["actual_rns"] = to_int(tokens[3])

    # 당일변동
    change_str = tokens[4]
    if "▲" in change_str:
        result["daily_change"] = to_int(change_str)
        result["daily_change_dir"] = "▲"
    elif "▼" in change_str:
        result["daily_change"] = -to_int(change_str)
        result["daily_change_dir"] = "▼"
    else:
        result["daily_change"] = 0
        result["daily_change_dir"] = ""

    return result


def parse_occ_line(line: str) -> dict:
    """OCC 행에서 전체RN용량, Budget/일, OCC%, 전년OCC%, 증감, 누적OCC% 추출

    예시: "348,180 11,606 OCC. 52.6% 50.4% ▼3.5%p 49.1% 33.6% ..."
    또는: "41,970 1,399 OCC. 33.1% 40.3% ▼5.0%p 28.2% 2.2% ..."
    """
    result = {
        "total_rn_capacity": 0,
        "budget_per_day": 0,
        "occ_budget": 0.0,
        "occ_ly": 0.0,
        "occ_yoy_change": 0.0,
        "occ_actual": 0.0,
    }

    # OCC. 이전의 두 숫자: 전체RN용량, Budget/일
    occ_idx = line.find("OCC.")
    if occ_idx == -1:
        return result

    before_occ = line[:occ_idx].strip()
    after_occ = line[occ_idx + 4:].strip()

    # 전체RN용량과 Budget/일
    nums_before = re.findall(r'[\d,]+', before_occ)
    if len(nums_before) >= 2:
        result["total_rn_capacity"] = int(nums_before[-2].replace(",", ""))
        result["budget_per_day"] = int(nums_before[-1].replace(",", ""))

    # OCC% 값들: Budget OCC%, 전년 OCC%, ▲/▼ 증감%p, 누적 OCC%
    # 먼저 %p (yoy change) 패턴을 찾아서 위치 파악
    yoy_pattern = r'[▲▼]\s*[\d.]+%p'
    yoy_match = re.search(yoy_pattern, after_occ)

    if yoy_match:
        before_yoy = after_occ[:yoy_match.start()].strip()
        after_yoy = after_occ[yoy_match.end():].strip()

        # Budget OCC%, 전년 OCC% (증감 이전의 순수 % 값)
        pct_before = re.findall(r'[\d.]+%', before_yoy)
        if len(pct_before) >= 1:
            result["occ_budget"] = float(pct_before[0].replace("%", ""))
        if len(pct_before) >= 2:
            result["occ_ly"] = float(pct_before[1].replace("%", ""))

        # 증감 값
        ch = yoy_match.group()
        val = float(re.search(r'[\d.]+', ch).group())
        if "▼" in ch:
            result["occ_yoy_change"] = -val
        else:
            result["occ_yoy_change"] = val

        # 증감 이후 첫 번째 % 값 = 누적 실적 OCC%
        actual_pct = re.findall(r'[\d.]+%', after_yoy)
        if actual_pct:
            result["occ_actual"] = float(actual_pct[0].replace("%", ""))
    else:
        # fallback: %p 패턴이 없는 경우
        pct_values = re.findall(r'[\d.]+%', after_occ)
        if len(pct_values) >= 1:
            result["occ_budget"] = float(pct_values[0].replace("%", ""))
        if len(pct_values) >= 2:
            result["occ_ly"] = float(pct_values[1].replace("%", ""))
        if len(pct_values) >= 4:
            result["occ_actual"] = float(pct_values[3].replace("%", ""))

    return result


def parse_page(text: str, page_idx: int) -> dict:
    """한 페이지(한 달)의 데이터를 파싱"""
    lines = text.split('\n')

    # 월 추출 (첫 줄에서)
    month_match = re.search(r'(\d+)월', lines[0])
    month = int(month_match.group(1)) if month_match else (page_idx + 4)

    # 보고일 추출
    report_date = ""
    if len(lines) > 1:
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', lines[1])
        if date_match:
            report_date = date_match.group(1)

    # 일수 추출 (헤더 행에서)
    days_in_month = 30
    if len(lines) > 3:
        day_nums = re.findall(r'\d+', lines[3])
        if day_nums:
            days_in_month = int(day_nums[-1])

    properties = []
    for prop_def in PROPERTY_MAP:
        rns_idx = prop_def["rns_line"]
        occ_idx = prop_def["occ_line"]

        if rns_idx >= len(lines) or occ_idx >= len(lines):
            continue

        rns_data = parse_rns_line(lines[rns_idx])
        occ_data = parse_occ_line(lines[occ_idx])

        # Budget 달성률 계산
        budget_ach = 0.0
        if rns_data["budget_rns"] > 0:
            budget_ach = round(rns_data["actual_rns"] / rns_data["budget_rns"] * 100, 1)

        # YoY 변동 (전년 대비)
        yoy_rns = 0
        yoy_pct = 0.0
        if rns_data["ly_actual"] > 0:
            yoy_rns = rns_data["actual_rns"] - rns_data["ly_actual"]
            yoy_pct = round(yoy_rns / rns_data["ly_actual"] * 100, 1)

        prop = {
            "name": prop_def["name"],
            "region": prop_def["region"],
            "budget_rns": rns_data["budget_rns"],
            "ly_actual": rns_data["ly_actual"],
            "vs_budget": rns_data["vs_budget"],
            "actual_rns": rns_data["actual_rns"],
            "daily_change": rns_data["daily_change"],
            "daily_change_dir": rns_data["daily_change_dir"],
            "budget_achievement": budget_ach,
            "yoy_rns": yoy_rns,
            "yoy_pct": yoy_pct,
            "total_rn_capacity": occ_data["total_rn_capacity"],
            "budget_per_day": occ_data["budget_per_day"],
            "occ_budget": occ_data["occ_budget"],
            "occ_ly": occ_data["occ_ly"],
            "occ_yoy_change": occ_data["occ_yoy_change"],
            "occ_actual": occ_data["occ_actual"],
        }
        properties.append(prop)

    return {
        "month": month,
        "year": 2026,
        "month_key": f"2026-{month:02d}",
        "report_date": report_date,
        "days_in_month": days_in_month,
        "properties": properties,
    }


def parse_pdf(pdf_path: str) -> dict:
    """PDF 전체를 파싱하여 구조화된 데이터 반환"""
    try:
        import pdfplumber
    except ImportError:
        logger.error("pdfplumber가 설치되지 않았습니다: pip install pdfplumber")
        sys.exit(1)

    pdf = pdfplumber.open(pdf_path)
    logger.info(f"PDF 로드: {pdf_path} ({len(pdf.pages)}페이지)")

    months_data = []
    for i, page in enumerate(pdf.pages):
        text = page.extract_text()
        if not text:
            continue
        month_data = parse_page(text, i)
        months_data.append(month_data)

        # Grand Total 확인
        gt = next((p for p in month_data["properties"] if p["name"] == "Grand Total"), None)
        if gt:
            logger.info(
                f"  {month_data['month_key']}: "
                f"RNs={gt['actual_rns']:,} (Budget {gt['budget_rns']:,}, 달성 {gt['budget_achievement']:.1f}%), "
                f"OCC={gt['occ_actual']:.1f}%, 당일변동 {gt['daily_change']:+,}"
            )

    # 사업장별로 4개월 데이터를 모아서 요약 구조 생성
    property_summary = {}
    for md in months_data:
        for prop in md["properties"]:
            name = prop["name"]
            if name not in property_summary:
                property_summary[name] = {
                    "name": name,
                    "region": prop["region"],
                    "months": {},
                }
            property_summary[name]["months"][md["month_key"]] = {
                "budget_rns": prop["budget_rns"],
                "ly_actual": prop["ly_actual"],
                "actual_rns": prop["actual_rns"],
                "vs_budget": prop["vs_budget"],
                "daily_change": prop["daily_change"],
                "daily_change_dir": prop["daily_change_dir"],
                "budget_achievement": prop["budget_achievement"],
                "yoy_rns": prop["yoy_rns"],
                "yoy_pct": prop["yoy_pct"],
                "occ_budget": prop["occ_budget"],
                "occ_ly": prop["occ_ly"],
                "occ_yoy_change": prop["occ_yoy_change"],
                "occ_actual": prop["occ_actual"],
                "total_rn_capacity": prop["total_rn_capacity"],
                "budget_per_day": prop["budget_per_day"],
            }

    # 보고일 추출
    report_date = months_data[0]["report_date"] if months_data else ""
    month_keys = [md["month_key"] for md in months_data]

    return {
        "meta": {
            "report_date": report_date,
            "months": month_keys,
            "source": "Daily_Booking_Report",
            "property_count": len(property_summary) - 1,  # Grand Total 제외
        },
        "months_detail": months_data,
        "by_property": property_summary,
    }


def main():
    import glob

    # PDF 파일 찾기 (여러 경로 시도)
    pdf_path = None
    search_paths = [
        str(ROOT / "Daily_Booking_Report_*.pdf"),
        str(ROOT / "data" / "Daily_Booking_Report_*.pdf"),
        str(ROOT.parent / "uploads" / "*Daily_Booking_Report*.pdf"),
    ]

    # 명시적 경로가 주어진 경우
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
    else:
        for pattern in search_paths:
            matches = glob.glob(pattern)
            if matches:
                pdf_path = sorted(matches)[-1]  # 최신 파일
                break

    if not pdf_path or not Path(pdf_path).exists():
        logger.error(f"Daily Booking Report PDF를 찾을 수 없습니다.")
        logger.error(f"사용법: python {Path(__file__).name} <PDF경로>")
        sys.exit(1)

    result = parse_pdf(pdf_path)

    # JSON 저장
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DATA_DIR / "daily_booking.json"
    output_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"✓ 저장 완료: {output_path}")
    logger.info(f"  사업장: {result['meta']['property_count']}개, 월: {result['meta']['months']}")

    # docs/data 에도 동기화
    docs_data_dir = ROOT / "docs" / "data"
    docs_data_dir.mkdir(parents=True, exist_ok=True)
    docs_output = docs_data_dir / "daily_booking.json"
    docs_output.write_text(
        json.dumps(result, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    logger.info(f"✓ docs/data 동기화: {docs_output}")


if __name__ == "__main__":
    main()
