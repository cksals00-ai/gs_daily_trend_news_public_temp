#!/usr/bin/env python3
"""
build_validation.py
===========================================================
Excel 실시간 실적현황 vs DB(db_aggregated.json) 정합성 검증

규칙
----
1. RN / 매출 비교  : OTA + G-OTA + Inbound 3개 세그먼트만 합산
                     (Excel 실시간 실적현황 동일 기준)
2. OCC 비교        : daily_booking.json(전체 세그먼트) 기준
3. 사업장 매핑     : compare_and_update.py PROPERTY_DEFS
                     (특히 비발디(벨)=소노벨+소노문, 양평=소노휴+소노벨,
                      단양=소노문+소노벨, 경주=소노벨+소노캄)
4. 회색지대 보정   : 고양은 FIT 세그먼트도 포함 (Excel INBOUND/OTA에 섞임)

차이율 평가
-----------
|Δ%| ≤ 1.0  → PASS
1.0 < |Δ%| ≤ 2.0 → INFO
|Δ%| > 2.0 → WARNING

출력
----
docs/data/validation/YYYYMMDD_validation.json
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path

import openpyxl

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
DOCS_DATA = PROJECT_DIR / "docs" / "data"
VALIDATION_DIR = DOCS_DATA / "validation"
DB_JSON = DOCS_DATA / "db_aggregated.json"
DAILY_BOOKING_JSON = DOCS_DATA / "daily_booking.json"
EXCEL_DIR = DATA_DIR / "검증"

# ── PROPERTY_DEFS (compare_and_update.py와 동일, 정답지) ──
# (excel_col, excel_label, code, [db_property_names], region)
# excel_col=None ⇒ Excel 미포함 (팔라티움 등)
PROPERTY_DEFS = [
    (6,  "비발디(벨)",     "01.벨비발디",      "vivaldi", ["소노벨 비발디파크", "소노문 비발디파크"]),
    (9,  "비발디(캄)",     "02.캄비발디",      "vivaldi", ["소노캄 비발디파크"]),
    (18, "비발디(펫)",     "03.펫비발디",      "vivaldi", ["소노펫 비발디파크"]),
    (12, "비발디(펠리체)", "04.펠리체비발디",  "vivaldi", ["소노펠리체 비발디파크"]),
    (15, "비발디(빌리지)", "05.빌리지비발디",  "vivaldi", ["소노펠리체 빌리지 비발디파크"]),
    (21, "양평",           "06.양평",          "central", ["소노휴 양평", "소노벨 양평"]),
    # 델피노: Excel 4컬럼 합산 (문벨/캄/펠리체/빌리지) ↔ DB '델피노' 단일
    ([24, 27, 30, 33], "델피노(합산)", "07.델피노", "central", ["델피노"]),
    (36, "양양",           "08.쏠비치양양",    "central", ["쏠비치 양양"]),
    (39, "삼척",           "09.쏠비치삼척",    "central", ["쏠비치 삼척"]),
    (48, "단양",           "10.소노벨단양",    "central", ["소노문 단양", "소노벨 단양"]),
    (42, "경주",           "11.소노캄경주",    "south",   ["소노벨 경주", "소노캄 경주"]),
    (45, "청송",           "12.소노벨청송",    "central", ["소노벨 청송"]),
    (51, "천안",           "13.소노벨천안",    "central", ["소노벨 천안"]),
    (54, "변산",           "14.소노벨변산",    "central", ["소노벨 변산"]),
    (60, "여수",           "15.소노캄여수",    "south",   ["소노캄 여수"]),
    (57, "거제",           "16.소노캄거제",    "south",   ["소노캄 거제"]),
    (63, "진도",           "17.쏠비치진도",    "south",   ["쏠비치 진도"]),
    (69, "제주(벨)",       "18.소노벨제주",    "apac",    ["소노벨 제주"]),
    (66, "제주(캄)",       "19.소노캄제주",    "apac",    ["소노캄 제주"]),
    (72, "고양",           "20.소노캄고양",    "apac",    ["소노캄 고양"]),
    (75, "해운대",         "21.소노문해운대",  "south",   ["소노문 해운대"]),
    (78, "남해",           "22.쏠비치남해",    "south",   ["쏠비치 남해"]),
    (81, "르네블루",       "23.르네블루",      "central", ["르네블루"]),
    (None, None,           "25.팔라티움",      "south",   []),  # Excel 미포함
]

# ── 기본 비교 세그먼트: OTA + G-OTA + Inbound ──
BASE_SEGMENTS = ["OTA", "G-OTA", "Inbound"]

# ── 사업장별 추가 세그먼트(회색지대) ──
#   고양: Excel의 INBOUND/OTA 항목에 일부 FIT 건이 섞여 있어
#         DB의 FIT을 함께 합산해야 1% 이내로 매칭됨.
SEGMENT_OVERRIDES = {
    "20.소노캄고양": ["OTA", "G-OTA", "Inbound", "FIT"],
}

# ── Excel OTB row 위치 (1~7월) ──
#  월별 [총계_OTB, OTA_OTB, GOTA_OTB, INBOUND_OTB]
#  8월 이후는 OTB 행이 없어 비교 대상에서 제외
OTB_ROWS = {
    1: {"total":  6, "OTA":  12, "G-OTA":  18, "Inbound":  24},
    2: {"total": 30, "OTA":  36, "G-OTA":  42, "Inbound":  48},
    3: {"total": 54, "OTA":  60, "G-OTA":  66, "Inbound":  72},
    4: {"total": 78, "OTA":  84, "G-OTA":  90, "Inbound":  96},
    5: {"total":103, "OTA": 112, "G-OTA": 121, "Inbound": 130},
    6: {"total":139, "OTA": 148, "G-OTA": 157, "Inbound": 166},
    7: {"total":175, "OTA": 184, "G-OTA": 193, "Inbound": 202},
}
COMPARE_MONTHS = sorted(OTB_ROWS.keys())  # [1..7]


def latest_excel() -> Path:
    """data/검증/ 폴더에서 최신 GS실시간실적현황 xlsx 찾기"""
    candidates = sorted(EXCEL_DIR.glob("GS실시간실적현황_*.xlsx"))
    if not candidates:
        raise FileNotFoundError(f"Excel 검증 파일 없음: {EXCEL_DIR}")
    return candidates[-1]


def parse_excel_timestamp(p: Path) -> str:
    m = re.search(r"(\d{14})", p.name)
    if not m:
        return ""
    s = m.group(1)
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]} {s[8:10]}:{s[10:12]}:{s[12:14]}"


def excel_value(ws, row, col) -> float:
    v = ws.cell(row, col).value
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return 0.0


def excel_property_3seg(ws, cols, month_int: int) -> dict:
    """Excel에서 한 사업장(여러 col 합산)의 OTA+GOTA+INBOUND OTB 합산 RN/REV."""
    if not isinstance(cols, list):
        cols = [cols]
    rows = OTB_ROWS[month_int]
    rn_sum, adr_sum, rev_sum = 0.0, 0.0, 0.0
    for c in cols:
        for seg in ("OTA", "G-OTA", "Inbound"):
            r = rows[seg]
            rn_sum  += excel_value(ws, r, c)
            adr_sum += excel_value(ws, r, c + 1)  # ADR 열
            rev_sum += excel_value(ws, r, c + 2)
    return {"rn": int(round(rn_sum)), "rev_m": round(rev_sum, 1)}


def db_property_segments(db, db_props: list, segments: list, month_key: str) -> dict:
    """DB by_property_segment에서 지정된 세그먼트만 합산."""
    rn_total, rev_total = 0, 0.0
    for pname in db_props:
        seg_data = db.get("by_property_segment", {}).get(pname, {})
        for seg in segments:
            m = seg_data.get(seg, {}).get(month_key, {})
            rn_total  += m.get("net_rn", 0) or 0
            rev_total += m.get("net_rev", 0.0) or 0.0
    return {"rn": rn_total, "rev_m": round(rev_total, 1)}


def status_for_pct(pct: float) -> str:
    a = abs(pct)
    if a <= 1.0:
        return "PASS"
    if a <= 2.0:
        return "INFO"
    return "WARNING"


def build_property_record(ws, db, defn) -> dict:
    excel_col, excel_label, code, region, db_props = defn
    segments = SEGMENT_OVERRIDES.get(code, BASE_SEGMENTS)

    monthly = []
    excel_rn_sum, excel_rev_sum = 0, 0.0
    db_rn_sum, db_rev_sum = 0, 0.0

    for m in COMPARE_MONTHS:
        mkey = f"2026{m:02d}"
        if excel_col is None:
            excel_part = {"rn": 0, "rev_m": 0.0}
        else:
            excel_part = excel_property_3seg(ws, excel_col, m)
        db_part = db_property_segments(db, db_props, segments, mkey)

        rn_diff = db_part["rn"] - excel_part["rn"]
        rev_diff = round(db_part["rev_m"] - excel_part["rev_m"], 1)
        rn_pct  = (rn_diff  / excel_part["rn"]    * 100) if excel_part["rn"]    else 0.0
        rev_pct = (rev_diff / excel_part["rev_m"] * 100) if excel_part["rev_m"] else 0.0

        monthly.append({
            "month": f"2026-{m:02d}",
            "excel_rn":     excel_part["rn"],
            "db_rn":        db_part["rn"],
            "rn_diff":      rn_diff,
            "rn_diff_pct":  round(rn_pct, 2),
            "excel_rev_m":  excel_part["rev_m"],
            "db_rev_m":     db_part["rev_m"],
            "rev_diff_m":   rev_diff,
            "rev_diff_pct": round(rev_pct, 2),
            "rn_status":    status_for_pct(rn_pct),
            "rev_status":   status_for_pct(rev_pct),
        })

        excel_rn_sum  += excel_part["rn"]
        excel_rev_sum += excel_part["rev_m"]
        db_rn_sum     += db_part["rn"]
        db_rev_sum    += db_part["rev_m"]

    rn_diff_total  = db_rn_sum - excel_rn_sum
    rev_diff_total = round(db_rev_sum - excel_rev_sum, 1)
    rn_pct_total   = (rn_diff_total  / excel_rn_sum  * 100) if excel_rn_sum  else 0.0
    rev_pct_total  = (rev_diff_total / excel_rev_sum * 100) if excel_rev_sum else 0.0

    return {
        "code":           code,
        "excel_property": excel_label,
        "db_properties":  db_props,
        "region":         region,
        "segments_used":  segments,
        "excel_rn":       excel_rn_sum,
        "db_rn":          db_rn_sum,
        "rn_diff":        rn_diff_total,
        "rn_diff_pct":    round(rn_pct_total, 2),
        "excel_rev_m":    round(excel_rev_sum, 1),
        "db_rev_m":       round(db_rev_sum, 1),
        "rev_diff_m":     rev_diff_total,
        "rev_diff_pct":   round(rev_pct_total, 2),
        "rn_status":      status_for_pct(rn_pct_total),
        "rev_status":     status_for_pct(rev_pct_total),
        "monthly":        monthly,
    }


def attach_occ(prop_records: list) -> None:
    """daily_booking.json (full segment OCC)을 사업장별로 첨부."""
    if not DAILY_BOOKING_JSON.exists():
        return
    db = json.loads(DAILY_BOOKING_JSON.read_text(encoding="utf-8"))

    # PROPERTY_DEFS code→DB display_name 매핑 (daily_booking.json 의 'name' 키)
    DAILY_BK_NAME = {
        "01.벨비발디":      "소노벨 비발디파크",
        "02.캄비발디":      "소노캄 비발디파크",
        "03.펫비발디":      "소노펫 비발디파크",
        "04.펠리체비발디":  "소노펠리체 비발디파크",
        "05.빌리지비발디":  "소노펠리체빌리지 비발디파크",
        "06.양평":          "소노벨 양평",
        "07.델피노":        "델피노",
        "08.쏠비치양양":    "쏠비치 양양",
        "09.쏠비치삼척":    "쏠비치 삼척",
        "10.소노벨단양":    "소노벨 단양",
        "11.소노캄경주":    "소노캄 경주",
        "12.소노벨청송":    "소노벨 청송",
        "13.소노벨천안":    "소노벨 천안",
        "14.소노벨변산":    "소노벨 변산",
        "15.소노캄여수":    "소노캄 여수",
        "16.소노캄거제":    "소노캄 거제",
        "17.쏠비치진도":    "쏠비치 진도",
        "18.소노벨제주":    "소노벨 제주",
        "19.소노캄제주":    "소노캄 제주",
        "20.소노캄고양":    "소노캄 고양",
        "21.소노문해운대":  "소노문 해운대",
        "22.쏠비치남해":    "쏠비치 남해",
        "23.르네블루":      "르네블루",
        "25.팔라티움":      "팔라티움 해운대",
    }

    # months_detail → {month_key: {prop_name: {occ_actual, occ_budget, ...}}}
    occ_by_month = {}
    for md in db.get("months_detail", []):
        mk = md.get("month_key")  # e.g. "2026-04"
        occ_by_month.setdefault(mk, {})
        for p in md.get("properties", []) or []:
            occ_by_month[mk][p.get("name")] = {
                "occ_actual":  p.get("occ_actual"),
                "occ_budget":  p.get("occ_budget"),
                "occ_ly":      p.get("occ_ly"),
            }

    for rec in prop_records:
        bk_name = DAILY_BK_NAME.get(rec["code"])
        if not bk_name:
            continue
        rec["occ_monthly"] = []
        rec["occ_basis"] = "전체 세그먼트 (Daily Booking PDF)"
        for m in COMPARE_MONTHS:
            mk = f"2026-{m:02d}"
            d = occ_by_month.get(mk, {}).get(bk_name, {})
            if d:
                rec["occ_monthly"].append({"month": mk, **d})


def main():
    if not DB_JSON.exists():
        print(f"❌ DB JSON 없음: {DB_JSON}")
        sys.exit(1)
    excel_path = latest_excel()
    print(f"📂 DB JSON: {DB_JSON.name}")
    print(f"📂 Excel:   {excel_path.name}")

    db = json.loads(DB_JSON.read_text(encoding="utf-8"))
    wb = openpyxl.load_workbook(str(excel_path), data_only=True)
    ws = wb["SHEET1"]

    records = [build_property_record(ws, db, d) for d in PROPERTY_DEFS]
    attach_occ(records)

    pass_n = sum(1 for r in records if r["rn_status"] == "PASS")
    info_n = sum(1 for r in records if r["rn_status"] == "INFO")
    warn_n = sum(1 for r in records if r["rn_status"] == "WARNING")

    excel_total_rn  = sum(r["excel_rn"]  for r in records)
    db_total_rn     = sum(r["db_rn"]     for r in records)
    excel_total_rev = round(sum(r["excel_rev_m"] for r in records), 1)
    db_total_rev    = round(sum(r["db_rev_m"]    for r in records), 1)
    total_rn_pct    = round((db_total_rn - excel_total_rn) / excel_total_rn * 100, 2) if excel_total_rn else 0
    total_rev_pct   = round((db_total_rev - excel_total_rev) / excel_total_rev * 100, 2) if excel_total_rev else 0

    out = {
        "validation_date":  datetime.now().strftime("%Y-%m-%d"),
        "validation_time":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "excel_file":       excel_path.name,
        "excel_generated":  parse_excel_timestamp(excel_path),
        "db_generated_at":  db.get("generated_at", ""),
        "comparison_basis": {
            "rn_rev":           "OTA + G-OTA + Inbound (3개 세그먼트) — Excel 실시간 실적현황 동일 기준",
            "rn_rev_overrides": SEGMENT_OVERRIDES,
            "occ":              "전체 세그먼트 — daily_booking.json (Daily Booking PDF)",
            "months":           [f"2026-{m:02d}" for m in COMPARE_MONTHS],
            "snapshot":         "OTB 행 (실시간 예약 누적)",
        },
        "thresholds": {
            "PASS":    "|Δ%| ≤ 1.0",
            "INFO":    "1.0 < |Δ%| ≤ 2.0",
            "WARNING": "|Δ%| > 2.0",
        },
        "summary": {
            "properties_count": len(records),
            "pass_count":       pass_n,
            "info_count":       info_n,
            "warn_count":       warn_n,
            "excel_total_rn":   excel_total_rn,
            "db_total_rn":      db_total_rn,
            "rn_diff":          db_total_rn - excel_total_rn,
            "rn_diff_pct":      total_rn_pct,
            "excel_total_rev_m":excel_total_rev,
            "db_total_rev_m":   db_total_rev,
            "rev_diff_m":       round(db_total_rev - excel_total_rev, 1),
            "rev_diff_pct":     total_rev_pct,
        },
        "by_property": records,
    }

    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    fname = datetime.now().strftime("%Y%m%d") + "_validation.json"
    out_path = VALIDATION_DIR / fname
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    # 콘솔 요약
    print("\n" + "═" * 90)
    print(f"  검증 결과 — {out['validation_date']}  (Excel: {out['excel_generated']})")
    print("═" * 90)
    print(f"  PASS {pass_n} | INFO {info_n} | WARNING {warn_n}  (총 {len(records)} 사업장)")
    print(f"  전체 RN  : Excel {excel_total_rn:,}  vs  DB {db_total_rn:,}   ({total_rn_pct:+.2f}%)")
    print(f"  전체 매출: Excel {excel_total_rev:,}백만  vs  DB {db_total_rev:,}백만  ({total_rev_pct:+.2f}%)")
    print("─" * 90)
    print(f"  {'사업장':<22}{'Excel RN':>10}{'DB RN':>10}{'Δ RN':>10}{'Δ%':>8}  상태")
    print("─" * 90)
    for r in records:
        status = r["rn_status"]
        marker = {"PASS": "✓", "INFO": "i", "WARNING": "!"}[status]
        print(f"  {r['code']:<22}{r['excel_rn']:>10,}{r['db_rn']:>10,}"
              f"{r['rn_diff']:>+10,}{r['rn_diff_pct']:>+7.2f}%  {marker} {status}")
    print("═" * 90)
    print(f"\n✅ {out_path}")


if __name__ == "__main__":
    main()
