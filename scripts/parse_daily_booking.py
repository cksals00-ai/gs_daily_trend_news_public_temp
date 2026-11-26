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
import fs_utils  # macOS NFD→NFC 유니코드 정규화
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"

# ── 사업장 순서 (테이블 내 RNs/OCC 쌍 순서와 일치) ──
PROPERTY_ORDER = [
    {"name": "Grand Total",                   "region": "total"},
    {"name": "소노벨 비발디파크",               "region": "vivaldi"},
    {"name": "소노캄 비발디파크",               "region": "vivaldi"},
    {"name": "소노펫 비발디파크",               "region": "vivaldi"},
    {"name": "소노펠리체 비발디파크",            "region": "vivaldi"},
    {"name": "소노펠리체빌리지 비발디파크",       "region": "vivaldi"},
    {"name": "소노벨 양평",                    "region": "central"},
    {"name": "델피노",                         "region": "central"},
    {"name": "쏠비치 양양",                    "region": "central"},
    {"name": "쏠비치 삼척",                    "region": "central"},
    {"name": "소노벨 단양",                    "region": "central"},
    {"name": "소노캄 경주",                    "region": "south"},
    {"name": "소노벨 청송",                    "region": "south"},
    {"name": "소노벨 천안",                    "region": "central"},
    {"name": "소노벨 변산",                    "region": "central"},
    {"name": "소노캄 여수",                    "region": "south"},
    {"name": "소노캄 거제",                    "region": "south"},
    {"name": "쏠비치 진도",                    "region": "south"},
    {"name": "소노벨 제주",                    "region": "south"},
    {"name": "소노캄 제주",                    "region": "south"},
    {"name": "소노캄 고양",                    "region": "central"},
    {"name": "소노문 해운대",                  "region": "south"},
    {"name": "쏠비치 남해",                    "region": "south"},
    {"name": "파나크 영덕",                    "region": "south"},
    {"name": "르네블루",                       "region": "south"},
    {"name": "팔라티움 해운대 by sonofelice 해운대",                 "region": "south"},
]


def _safe_str(cell):
    """셀 값을 안전하게 문자열로 변환"""
    return str(cell).strip() if cell else ""


def _to_int(s: str) -> int:
    """콤마/괄호 포함 문자열을 정수로 변환"""
    s = s.replace(",", "").replace("(", "").replace(")", "").strip()
    s = re.sub(r'[▲▼]\s*', '', s)
    try:
        return int(s)
    except ValueError:
        return 0


def _to_float(s: str) -> float:
    """% 포함 문자열을 실수로 변환"""
    s = s.replace("%", "").replace("p", "").strip()
    s = re.sub(r'[▲▼]\s*', '', s)
    try:
        return float(s)
    except ValueError:
        return 0.0


def parse_rns_row(row: list) -> dict:
    """테이블 RNs 행에서 Budget, Actual, vs_budget, daily_change 추출

    Col 4: Budget, Col 5: LY Actual, Col 6: vs Budget, Col 7: Total (actual ▲/▼ change)
    """
    result = {
        "budget_rns": 0, "ly_actual": 0, "vs_budget": 0,
        "actual_rns": 0, "daily_change": 0, "daily_change_dir": "",
    }

    result["budget_rns"] = _to_int(_safe_str(row[4]))
    result["ly_actual"] = _to_int(_safe_str(row[5]))

    # vs_budget: 괄호면 음수
    vs_str = _safe_str(row[6])
    if vs_str.startswith("("):
        result["vs_budget"] = -_to_int(vs_str)
    else:
        result["vs_budget"] = _to_int(vs_str)

    # Total 열: "actual_rns ▲/▼ daily_change" 또는 "actual_rns 0"
    total_str = _safe_str(row[7])
    m = re.match(r'([\d,]+)\s*(▲|▼)\s*([\d,]+)', total_str)
    if m:
        result["actual_rns"] = _to_int(m.group(1))
        result["daily_change_dir"] = m.group(2)
        change_val = _to_int(m.group(3))
        result["daily_change"] = change_val if m.group(2) == "▲" else -change_val
    else:
        # "actual_rns 0" 또는 숫자만 있는 경우
        nums = re.findall(r'[\d,]+', total_str)
        if nums:
            result["actual_rns"] = _to_int(nums[0])

    return result


def parse_capacity(occ_row: list) -> tuple:
    """OCC 행 앞쪽 셀의 '월 RN 용량 일 객실수'(예: '43,369 1,399') → (43369, 1399). 없으면 (0, 0)."""
    for c in occ_row[1:3]:
        m = re.search(r'([\d,]{3,})\s+([\d,]{2,})\s*$', _safe_str(c))
        if m:
            return _to_int(m.group(1)), _to_int(m.group(2))
    return 0, 0


def parse_daily_rns(rns_row: list, days_in_month: int) -> list:
    """RNs 행 일자 셀('1,071 ▼2', '560 0', '58▲ 2') → 날짜별 온북 RN 정수 리스트(1일부터)."""
    out = []
    for d in range(days_in_month):
        idx = 8 + d
        s = _safe_str(rns_row[idx]) if idx < len(rns_row) else ''
        m = re.match(r'\s*([\d,]+)', s)
        out.append(_to_int(m.group(1)) if m else 0)
    return out


def ym_with_year(months_detail: list) -> list:
    """months_detail 월 번호 → 'YYYY-MM' (보고일 연도 기준, 첫 월보다 작은 월은 익년). year 필드는 고정값이라 쓰지 않음."""
    if not months_detail:
        return []
    rd = months_detail[0].get("report_date") or ""
    y0 = int(rd[:4]) if rd[:4].isdigit() else datetime.now().year
    first = months_detail[0]["month"]
    return [f"{y0 + (1 if md['month'] < first else 0)}-{md['month']:02d}" for md in months_detail]


def save_daily_history(result: dict, path) -> int:
    """보고일별 날짜 단위 재고 이력 누적 저장: {보고일: {YYYY-MM: {사업장: {cap, rn[], occ[]}}}}. 같은 보고일은 덮어씀."""
    mds = result.get("months_detail") or []
    if not mds:
        return 0
    rd = mds[0].get("report_date") or ""
    if not rd:
        return 0
    path = Path(path)
    hist = {}
    if path.exists():
        try:
            hist = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            hist = {}
    snap = {}
    for ym, md in zip(ym_with_year(mds), mds):
        snap[ym] = {p["name"]: {"cap": p.get("cap_day", 0), "rn": p.get("daily_rn", []), "occ": p.get("daily_occ", [])}
                    for p in md.get("properties", [])}
    hist[rd] = snap
    hist = dict(sorted(hist.items()))
    path.write_text(json.dumps(hist, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return len(hist)


def parse_occ_row(row: list, days_in_month: int = 31) -> dict:
    """테이블 OCC 행에서 OCC Budget%, LY%, YoY 변동, Actual%, 일별OCC% 추출

    Col 4: OCC Budget%, Col 5: OCC LY%, Col 6: YoY change (%p), Col 7: OCC Actual%
    Col 8~: 일별 OCC% (1일, 2일, ...)
    """
    result = {
        "total_rn_capacity": 0, "budget_per_day": 0,
        "occ_budget": 0.0, "occ_ly": 0.0, "occ_yoy_change": 0.0, "occ_actual": 0.0,
        "daily_occ": [],
    }

    result["occ_budget"] = _to_float(_safe_str(row[4]))
    result["occ_ly"] = _to_float(_safe_str(row[5]))

    # YoY change: "▼3.3%p" 또는 "▲ 6.4%p"
    yoy_str = _safe_str(row[6])
    yoy_val = _to_float(yoy_str)
    if "▼" in yoy_str:
        result["occ_yoy_change"] = -abs(yoy_val)
    else:
        result["occ_yoy_change"] = yoy_val

    result["occ_actual"] = _to_float(_safe_str(row[7]))

    # 일별 OCC% (col 8 이후)
    daily = []
    for d in range(days_in_month):
        col_idx = 8 + d
        if col_idx < len(row):
            daily.append(_to_float(_safe_str(row[col_idx])))
        else:
            daily.append(0.0)
    result["daily_occ"] = daily

    return result


def parse_page(page, page_idx: int) -> dict:
    """pdfplumber page 객체에서 테이블 추출 후 사업장별 데이터 파싱"""
    # 텍스트에서 월/날짜 정보 추출
    text = page.extract_text() or ""
    lines = text.split('\n')

    month_match = re.search(r'(\d+)월', lines[0] if lines else "")
    month = int(month_match.group(1)) if month_match else (page_idx + 4)

    report_date = ""
    if len(lines) > 1:
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', lines[1])
        if date_match:
            report_date = date_match.group(1)

    days_in_month = 30
    # 헤더 행에서 일수 추출
    if len(lines) > 3:
        day_nums = re.findall(r'\b\d{1,2}\b', lines[3])
        if day_nums:
            days_in_month = int(day_nums[-1])

    # 테이블 추출
    tables = page.extract_tables()
    if not tables:
        logger.warning(f"  페이지 {page_idx}: 테이블을 찾을 수 없습니다")
        return {"month": month, "year": 2026, "month_key": f"2026-{month:02d}",
                "report_date": report_date, "days_in_month": days_in_month, "properties": []}

    table = tables[0]

    # 테이블 컬럼 수에서 일수 재계산 (col 8 이후가 일별 데이터)
    if table and len(table[0]) > 8:
        days_in_month = len(table[0]) - 8

    # RNs 행과 OCC 행을 순서대로 수집
    rns_rows = []
    occ_rows = []
    for row in table:
        label = _safe_str(row[3]) if len(row) > 3 else ""
        if label == "RNs":
            rns_rows.append(row)
        elif label == "OCC.":
            occ_rows.append(row)

    # 사업장 순서대로 매핑
    properties = []
    for i, prop_def in enumerate(PROPERTY_ORDER):
        if i >= len(rns_rows) or i >= len(occ_rows):
            break

        rns_data = parse_rns_row(rns_rows[i])
        occ_data = parse_occ_row(occ_rows[i], days_in_month)
        # 날짜별 온북 RN(전 세그) + 보유객실(월 RN 용량·일 객실수) — ADR 가이드(남은 주말/주중 재고)용
        cap = parse_capacity(occ_rows[i])
        occ_data["total_rn_capacity"] = cap[0] or occ_data["total_rn_capacity"]
        occ_data["cap_day"] = cap[1]
        occ_data["daily_rn"] = parse_daily_rns(rns_rows[i], days_in_month)

        # Budget 달성률
        budget_ach = 0.0
        if rns_data["budget_rns"] > 0:
            budget_ach = round(rns_data["actual_rns"] / rns_data["budget_rns"] * 100, 1)

        # YoY 변동
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
            "daily_occ": occ_data.get("daily_occ", []),
            "daily_rn": occ_data.get("daily_rn", []),
            "cap_day": occ_data.get("cap_day", 0),
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
        month_data = parse_page(page, i)
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
            month_entry = {
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
            if "daily_occ" in prop:
                month_entry["daily_occ"] = prop["daily_occ"]
            property_summary[name]["months"][md["month_key"]] = month_entry

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

    # 날짜 단위 재고(온북 RN·OCC·보유객실) 보고일별 이력 누적 → data/daily_occ_history.json (ADR 가이드·향후 전년 동일자 비교)
    try:
        n = save_daily_history(result, DATA_DIR / "daily_occ_history.json")
        logger.info(f"✓ 날짜별 재고 이력 저장: data/daily_occ_history.json (보고일 {n}개)")
    except Exception as e:
        logger.warning(f"날짜별 재고 이력 저장 실패(무시): {e}")

    # 같은 날짜의 Booking Status Report(사업장별 페이지)도 파싱 → docs/data/booking_status_detail.json
    #   (품의 페이지 '전체' 온북 ADR·매출 실제값용). 실패해도 본 파싱 결과에는 영향 없음.
    try:
        bs_pdf = Path(pdf_path).with_name(Path(pdf_path).name.replace("Daily Booking Report", "Booking Status Report"))
        if bs_pdf != Path(pdf_path) and bs_pdf.exists():
            import importlib.util
            spec = importlib.util.spec_from_file_location("parse_booking_status", Path(__file__).with_name("parse_booking_status.py"))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            bs = mod.parse(bs_pdf)
            if len(bs.get("properties", {})) >= 20:
                mod.OUT.write_text(json.dumps(bs, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
                logger.info(f"✓ 사업장별 상세 동기화: {mod.OUT} ({bs.get('report_date')}, {len(bs['properties'])}개)")
            else:
                logger.warning(f"사업장별 상세 파싱 {len(bs.get('properties', {}))}개뿐 — 기존 파일 유지")
        else:
            logger.info(f"Booking Status Report 없음({bs_pdf.name}) — 사업장별 상세 스킵")
    except Exception as e:
        logger.warning(f"사업장별 상세 파싱 실패(무시): {e}")


if __name__ == "__main__":
    main()
