#!/usr/bin/env python3
"""
build_inbound_kpi.py
────────────────────
(인바운드)2026_도전목표KPI.xlsx → docs/data/inbound_kpi.json

- '월별 KPI(도전목표)' 시트 → stretch (도전목표)
- '월별 KPI' 시트 → budget (사업계획)
- '도전목표(3개년)' 시트 → 25Y RNs 총합 (ly25_rns_total)

출력 JSON 구조는 gs-sales-report.html의 renderInboundKpiPane()이 기대하는 형식과 일치:
{
  "generated_at": "2026-05-05",
  "totals": {
    "stretch": { "rns_total", "adr", "rev", "monthly": [12] },
    "budget":  { "rns_total", "adr", "rev", "monthly": [12] },
    "ly25_rns_total": number
  },
  "properties": {
    "<사업장>": {
      "stretch_rns_total", "budget_rns_total", "ly_rns_total",
      "stretch_adr", "stretch_rev",
      "budget_adr", "budget_rev",
      "monthly_rns": [ { "stretch", "budget", "ly" } × 12 ],
      "strategy_text": "",
      "issue_text": ""
    }
  },
  "grid": [
    { "property", "country", "agency", "type",
      "stretch": { "total", "monthly": [12] },
      "budget":  { "total", "monthly": [12] }
    }
  ],
  "strategic_agencies_2026": { "<사업장>": ["agency1", ...] }
}
"""

import json, os, sys
from datetime import date
from pathlib import Path

try:
    import openpyxl
except ImportError:
    sys.exit("openpyxl 필요: pip install openpyxl")

# ─── 경로 설정 ───
BASE = Path(__file__).resolve().parent.parent
EXCEL = BASE / "data" / "Inbound" / "(인바운드)2026_도전목표KPI.xlsx"
OUT   = BASE / "docs" / "data" / "inbound_kpi.json"

MONTH_COLS = list(range(10, 22))  # col index 10~21 (0-based) = 1월~12월


def parse_monthly_sheet(wb, sheet_name):
    """월별 KPI 시트를 파싱하여 사업장/국가/거래처 데이터를 반환한다."""
    ws = wb[sheet_name]
    rows = list(ws.iter_rows(min_row=6, max_col=22, values_only=True))  # row 6부터 데이터

    # 첫 행 = TOTAL
    total_row = rows[0]
    total_rns = _num(total_row[3])  # col D
    total_adr = _num(total_row[4])  # col E
    total_rev = _num(total_row[5])  # col F
    total_monthly = [_num(total_row[i]) for i in range(10, 22)]  # col K~V (0-based 10~21) = 1월~12월

    properties = {}
    grid = []
    strategic_agencies = {}

    cur_prop = None
    cur_prop_num = None
    cur_country = None
    cur_prop_adr = None
    cur_prop_rev = None
    cur_prop_rns = None

    for row in rows[1:]:  # skip TOTAL
        col_b = row[1]   # 구분 (순번)
        col_c = row[2]   # 사업장
        col_d = row[3]   # RNs
        col_e = row[4]   # ADR
        col_f = row[5]   # REV
        col_g = row[6]   # 국가
        col_h = row[7]   # 여행사
        col_i = row[8]   # 전략/일반

        # 새 사업장 시작
        if col_b and str(col_b).strip():
            cur_prop_num = str(col_b).strip()
            cur_prop = str(col_c).strip() if col_c else cur_prop
            cur_prop_adr = _num(col_e)
            cur_prop_rev = _num(col_f)
            cur_prop_rns = _num(col_d)
            if cur_prop and cur_prop not in properties:
                properties[cur_prop] = {
                    "rns_total": cur_prop_rns,
                    "adr": cur_prop_adr,
                    "rev": cur_prop_rev,
                    "monthly": [0.0] * 12,
                    "agencies": []
                }

        # 국가 갱신
        if col_g and str(col_g).strip():
            cur_country = str(col_g).strip()

        # 여행사 행 (계 행 제외)
        agency = str(col_h).strip() if col_h else ""
        if not agency or agency == "계":
            # '계' 행 → 국가 소계, skip
            continue

        agency_type = str(col_i).strip() if col_i else "일반"
        total_ag = _num(row[9])  # 계 column (col J)
        monthly = [_num(row[i]) for i in range(10, 22)]  # 1월~12월 (col K~V)

        grid.append({
            "property": cur_prop,
            "country": cur_country or "기타",
            "agency": agency,
            "type": agency_type,
            "total": total_ag,
            "monthly": monthly
        })

        # 전략 여행사 기록
        if agency_type == "전략" and cur_prop:
            strategic_agencies.setdefault(cur_prop, [])
            if agency not in strategic_agencies[cur_prop]:
                strategic_agencies[cur_prop].append(agency)

        # 사업장 월별 합산 (거래처 월별 합)
        if cur_prop and cur_prop in properties:
            for i in range(12):
                properties[cur_prop]["monthly"][i] += monthly[i]

    return {
        "total_rns": total_rns,
        "total_adr": total_adr,
        "total_rev": total_rev,
        "total_monthly": total_monthly,
        "properties": properties,
        "grid": grid,
        "strategic_agencies": strategic_agencies
    }


def get_ly25_total(wb):
    """도전목표(3개년) 시트에서 25Y RNs 총합을 가져온다."""
    try:
        ws = wb['도전목표(3개년)']
        # Row 7, col 9 (25Y) - 0-based index 8
        for row in ws.iter_rows(min_row=7, max_row=7, max_col=12, values_only=True):
            return _num(row[8])  # 25Y column (index 8, col I)
    except Exception:
        pass
    return 0


def _num(v):
    """None이나 빈 문자열을 0으로 변환."""
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def main():
    if not EXCEL.exists():
        sys.exit(f"엑셀 파일 없음: {EXCEL}")

    wb = openpyxl.load_workbook(str(EXCEL), data_only=True, read_only=True)

    # ── Stretch (도전목표)
    stretch = parse_monthly_sheet(wb, '월별 KPI(도전목표)')

    # ── Budget (사업계획)
    budget = parse_monthly_sheet(wb, '월별 KPI')

    # ── LY (25년 실적)
    ly25_total = get_ly25_total(wb)

    wb.close()

    # ── 출력 JSON 구성
    out = {
        "generated_at": date.today().isoformat(),
        "totals": {
            "stretch": {
                "rns_total": round(stretch["total_rns"], 1),
                "adr": round(stretch["total_adr"], 2),
                "rev": round(stretch["total_rev"], 2),
                "monthly": [round(v, 1) for v in stretch["total_monthly"]]
            },
            "budget": {
                "rns_total": round(budget["total_rns"], 1),
                "adr": round(budget["total_adr"], 2),
                "rev": round(budget["total_rev"], 2),
                "monthly": [round(v, 1) for v in budget["total_monthly"]]
            },
            "ly25_rns_total": round(ly25_total, 1)
        },
        "properties": {},
        "grid": [],
        "strategic_agencies_2026": stretch["strategic_agencies"]
    }

    # ── Properties (사업장별)
    all_props = set(list(stretch["properties"].keys()) + list(budget["properties"].keys()))
    for prop in sorted(all_props):
        sp = stretch["properties"].get(prop, {})
        bp = budget["properties"].get(prop, {})
        s_monthly = sp.get("monthly", [0]*12)
        b_monthly = bp.get("monthly", [0]*12)

        out["properties"][prop] = {
            "stretch_rns_total": round(sp.get("rns_total", 0), 1),
            "budget_rns_total": round(bp.get("rns_total", 0), 1),
            "ly_rns_total": 0,  # 사업장별 LY 데이터는 엑셀에 없음
            "stretch_adr": round(sp.get("adr", 0), 2),
            "stretch_rev": round(sp.get("rev", 0), 2),
            "budget_adr": round(bp.get("adr", 0), 2),
            "budget_rev": round(bp.get("rev", 0), 2),
            "monthly_rns": [
                {
                    "stretch": round(s_monthly[i], 1),
                    "budget": round(b_monthly[i], 1),
                    "ly": 0
                }
                for i in range(12)
            ],
            "strategy_text": "",
            "issue_text": ""
        }

    # ── Grid (거래처별 상세)
    # stretch grid를 기준으로 budget을 매핑
    budget_lookup = {}
    for g in budget["grid"]:
        key = (g["property"], g["country"], g["agency"])
        budget_lookup[key] = g

    for g in stretch["grid"]:
        key = (g["property"], g["country"], g["agency"])
        bg = budget_lookup.get(key, {})

        out["grid"].append({
            "property": g["property"],
            "country": g["country"],
            "agency": g["agency"],
            "type": g["type"],
            "stretch": {
                "total": round(g["total"], 1),
                "monthly": [round(v, 1) for v in g["monthly"]]
            },
            "budget": {
                "total": round(bg.get("total", 0), 1),
                "monthly": [round(v, 1) for v in bg.get("monthly", [0]*12)]
            }
        })

    # budget에만 있는 거래처 추가
    stretch_keys = {(g["property"], g["country"], g["agency"]) for g in stretch["grid"]}
    for g in budget["grid"]:
        key = (g["property"], g["country"], g["agency"])
        if key not in stretch_keys:
            out["grid"].append({
                "property": g["property"],
                "country": g["country"],
                "agency": g["agency"],
                "type": g["type"],
                "stretch": {
                    "total": 0,
                    "monthly": [0] * 12
                },
                "budget": {
                    "total": round(g["total"], 1),
                    "monthly": [round(v, 1) for v in g["monthly"]]
                }
            })

    # ── 출력
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"✅ {OUT} 생성 완료")
    print(f"   - 사업장: {len(out['properties'])}개")
    print(f"   - 거래처 grid: {len(out['grid'])}행")
    print(f"   - 전략여행사: {sum(len(v) for v in out['strategic_agencies_2026'].values())}개사")
    print(f"   - Stretch RNs 합계: {out['totals']['stretch']['rns_total']}")
    print(f"   - Budget RNs 합계: {out['totals']['budget']['rns_total']}")


if __name__ == "__main__":
    main()
