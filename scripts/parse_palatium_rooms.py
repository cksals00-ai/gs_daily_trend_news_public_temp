#!/usr/bin/env python3
"""
팔라티움 해운대 가용객실수 파서
================================
data/palatium_rooma/ 폴더의 "사용가능 객실 현황*.xlsx" 파일들을 파싱하여
일별/월별 OCC(점유율)를 계산합니다.

파일 구조 (각 xlsx, 31일 분량):
  - 1행: 제목
  - 2행: Sales Date : YYYY-MM-DD
  - 3행: 객실정보 / (빈) / MM-DD ... (31개 날짜)
  - 4행: 객실타입 / 객실 수 / 요일 ...
  - 5~17행: 객실 타입별 잔여 객실 수
  - 18행: Total Rooms
  - 19행: Inventory Rooms (= Total - Out of Order)
  - 20행: Sales Available (남은 가용)
  - 21행: Sold Rooms (판매)
  - 22행: Occupancy(%) (= Sold / Inventory × 100)
  - 23행: Out of Order
  - 24행: House Use
  - 25행: Complimentary

각 파일은 31일을 커버하며, 여러 파일을 합쳐 연중 데이터를 구성합니다.
중복 날짜는 Sales Date가 더 최신인 파일로 덮어씁니다.

산출:
  - data/palatium_room_availability.json (일별/월별)
  - docs/data/palatium_room_availability.json (동기화)
  - docs/data/occ_data.json 의 "25.팔라티움" 엔트리 갱신

© 2026 GS팀
"""
import json
import logging
import re
import sys
import warnings
from datetime import date
from pathlib import Path

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
DOCS_DATA_DIR = ROOT / "docs" / "data"
ROOM_DIR = DATA_DIR / "palatium_rooma"

PALATIUM_KEY = "25.팔라티움"


def _to_int(v):
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).replace(",", "").strip()
    if not s:
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def _to_float(v):
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace(",", "").replace("%", "").strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def parse_xlsx(path: Path, default_year: int) -> dict:
    """단일 xlsx 파일을 파싱하여 일별 데이터 dict를 리턴.

    Returns:
        {
            "sales_date": "YYYY-MM-DD",
            "year": int,
            "by_date": {
                "YYYY-MM-DD": {
                    "total": int,
                    "inventory": int,
                    "available": int,
                    "sold": int,
                    "occ": float,
                    "out_of_order": int,
                    "house_use": int,
                    "complimentary": int,
                }
            }
        }
    """
    import openpyxl

    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))

    # Sales Date 추출
    sales_date = ""
    for r in rows[:5]:
        if r and r[0]:
            m = re.search(r"(\d{4}-\d{2}-\d{2})", str(r[0]))
            if m:
                sales_date = m.group(1)
                break

    year = int(sales_date.split("-")[0]) if sales_date else default_year

    # 3행(인덱스 2): 날짜 헤더 — 컬럼 인덱스 2부터 MM-DD
    if len(rows) < 3:
        return {"sales_date": sales_date, "year": year, "by_date": {}}

    date_row = rows[2]
    cols = []  # (col_idx, "YYYY-MM-DD")
    prev_month = None
    yr = year
    for ci, v in enumerate(date_row):
        if not v or not isinstance(v, str):
            continue
        m = re.match(r"^(\d{1,2})-(\d{1,2})$", v.strip())
        if not m:
            continue
        mm, dd = int(m.group(1)), int(m.group(2))
        # 연도 롤오버 처리: 월이 줄어들면 (12 → 01 식) 연도 +1
        if prev_month is not None and mm < prev_month:
            yr += 1
        prev_month = mm
        cols.append((ci, f"{yr:04d}-{mm:02d}-{dd:02d}"))

    # 데이터 행 찾기
    label_to_row = {}
    for r in rows:
        if not r:
            continue
        label = r[0]
        if isinstance(label, str):
            label_to_row[label.strip()] = r

    by_date = {}
    for ci, dkey in cols:
        total = _to_int(label_to_row.get("Total Rooms", (None,) * (ci + 1))[ci]) if "Total Rooms" in label_to_row else 0
        inv = _to_int(label_to_row.get("Inventory Rooms", (None,) * (ci + 1))[ci]) if "Inventory Rooms" in label_to_row else 0
        avail = _to_int(label_to_row.get("Sales Available", (None,) * (ci + 1))[ci]) if "Sales Available" in label_to_row else 0
        sold = _to_int(label_to_row.get("Sold Rooms", (None,) * (ci + 1))[ci]) if "Sold Rooms" in label_to_row else 0
        occ = _to_float(label_to_row.get("Occupancy(%)", (None,) * (ci + 1))[ci]) if "Occupancy(%)" in label_to_row else 0.0
        ooo = _to_int(label_to_row.get("Out of Order", (None,) * (ci + 1))[ci]) if "Out of Order" in label_to_row else 0
        hu = _to_int(label_to_row.get("House Use", (None,) * (ci + 1))[ci]) if "House Use" in label_to_row else 0
        comp = _to_int(label_to_row.get("Complimentary", (None,) * (ci + 1))[ci]) if "Complimentary" in label_to_row else 0
        by_date[dkey] = {
            "total": total,
            "inventory": inv,
            "available": avail,
            "sold": sold,
            "occ": occ,
            "out_of_order": ooo,
            "house_use": hu,
            "complimentary": comp,
        }

    return {"sales_date": sales_date, "year": year, "by_date": by_date}


def aggregate_monthly(by_date: dict) -> dict:
    """일별 데이터를 월별로 집계. 월 OCC = sum(sold) / sum(inventory) × 100"""
    monthly = {}
    for dkey, row in sorted(by_date.items()):
        ym = dkey[:7]  # YYYY-MM
        m = monthly.setdefault(
            ym,
            {
                "days": 0,
                "total_sum": 0,
                "inventory_sum": 0,
                "sold_sum": 0,
                "out_of_order_sum": 0,
                "occ": 0.0,
            },
        )
        m["days"] += 1
        m["total_sum"] += row["total"]
        m["inventory_sum"] += row["inventory"]
        m["sold_sum"] += row["sold"]
        m["out_of_order_sum"] += row["out_of_order"]

    for ym, m in monthly.items():
        if m["inventory_sum"] > 0:
            m["occ"] = round(m["sold_sum"] / m["inventory_sum"] * 100, 1)
    return monthly


def main():
    if not ROOM_DIR.exists():
        logger.error(f"폴더가 없습니다: {ROOM_DIR}")
        sys.exit(1)

    files = sorted(ROOM_DIR.glob("*.xlsx"))
    if not files:
        logger.error(f"xlsx 파일이 없습니다: {ROOM_DIR}")
        sys.exit(1)

    # 모든 파일 파싱 + Sales Date가 더 최신인 것이 우선 (중복 날짜 덮어쓰기)
    files_meta = []
    for f in files:
        parsed = parse_xlsx(f, default_year=date.today().year)
        files_meta.append((f.name, parsed))
        logger.info(
            f"  {f.name}: sales_date={parsed['sales_date']}, "
            f"days={len(parsed['by_date'])}"
        )

    # Sales Date 오름차순 정렬 → 늦은 파일이 마지막에 적용 (덮어쓰기)
    files_meta.sort(key=lambda x: x[1]["sales_date"])

    merged = {}  # YYYY-MM-DD → row
    latest_sales_date = ""
    for fname, parsed in files_meta:
        if parsed["sales_date"] > latest_sales_date:
            latest_sales_date = parsed["sales_date"]
        merged.update(parsed["by_date"])

    # 월별 집계
    monthly = aggregate_monthly(merged)

    # 결과 저장 (data/)
    daily_sorted = dict(sorted(merged.items()))
    monthly_sorted = dict(sorted(monthly.items()))
    result = {
        "_generated": latest_sales_date,
        "_property": "팔라티움 해운대",
        "_property_key": PALATIUM_KEY,
        "_source_files": [f.name for f in files],
        "_note": "OCC(%) = Sold Rooms / Inventory Rooms × 100. Inventory = Total - Out of Order.",
        "monthly": monthly_sorted,
        "daily": daily_sorted,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out1 = DATA_DIR / "palatium_room_availability.json"
    out1.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"✓ 저장: {out1}")

    # docs/data 동기화 (compact)
    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out2 = DOCS_DATA_DIR / "palatium_room_availability.json"
    out2.write_text(
        json.dumps(result, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    logger.info(f"✓ 동기화: {out2}")

    # occ_data.json 갱신 — 25.팔라티움 엔트리만 덮어쓰기
    occ_path = DOCS_DATA_DIR / "occ_data.json"
    if not occ_path.exists():
        logger.warning(f"occ_data.json 없음: {occ_path}")
    else:
        occ = json.loads(occ_path.read_text(encoding="utf-8"))
        target_months = occ.get("_months", [])
        props = occ.setdefault("properties", {})
        pala = props.setdefault(PALATIUM_KEY, {})

        updated = {}
        for ym in target_months:
            if ym in monthly_sorted:
                old = pala.get(ym)
                new = monthly_sorted[ym]["occ"]
                pala[ym] = new
                updated[ym] = (old, new)

        occ_path.write_text(
            json.dumps(occ, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(f"✓ occ_data.json 갱신: {occ_path}")
        for ym, (old, new) in updated.items():
            logger.info(f"  {PALATIUM_KEY} {ym}: {old} → {new}")

    # 요약 출력
    logger.info("")
    logger.info("월별 팔라티움 OCC 요약:")
    for ym, m in monthly_sorted.items():
        logger.info(
            f"  {ym}: OCC={m['occ']:.1f}% "
            f"(Sold {m['sold_sum']:,} / Inventory {m['inventory_sum']:,}, "
            f"days={m['days']})"
        )


if __name__ == "__main__":
    main()
