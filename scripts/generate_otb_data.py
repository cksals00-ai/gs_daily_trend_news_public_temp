#!/usr/bin/env python3
"""
generate_otb_data.py
BI 26OTB 시트 기준으로 docs/data/otb_data.json 생성
- 사업장 순서: PROPERTY_DEFS (BI 시트 순서)
- 실적: db_aggregated.json (net_rn, net_rev, adr)
- 목표: budget XLSX Grand Total
- 전년 실적: db_aggregated.json 2025년 동월
- 월별 분리 데이터 포함 (월 필터 작동용)
"""
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import openpyxl

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
DOCS_DATA_DIR = PROJECT_DIR / "docs" / "data"
BUDGET_XLSX = DATA_DIR / "raw_db" / "budget" / "★최종★(검토완료)_2026년 객실 사업계획_총량 수립(2차+사업장변경건).xlsx"
DB_JSON = DATA_DIR / "db_aggregated.json"
OUTPUT_JSON = DOCS_DATA_DIR / "otb_data.json"

KST = timezone(timedelta(hours=9))

# BI 26OTB 시트 사업장 순서 (compare_and_update.py PROPERTY_DEFS 동일)
PROPERTY_DEFS = [
    ("소노벨 비발디파크(A,B,C,호텔)",    "01.벨비발디",      "vivaldi", ["소노벨 비발디파크", "소노문 비발디파크"]),
    ("소노캄 비발디파크 (D동)",           "02.캄비발디",      "vivaldi", ["소노캄 비발디파크"]),
    ("소노펫 (D동+E동)",                  "03.펫비발디",      "vivaldi", ["소노펫 비발디파크"]),
    ("소노펠리체 비발디파크",             "04.펠리체비발디",  "vivaldi", ["소노펠리체 비발디파크"]),
    ("소노펠리체 빌리지 비발디파크",      "05.빌리지비발디",  "vivaldi", ["소노펠리체 빌리지 비발디파크"]),
    ("소노벨양평",                        "06.양평",          "central", ["소노휴 양평", "소노벨 양평"]),
    ("델피노",                            "07.델피노",        "central", ["델피노"]),
    ("쏠비치양양",                        "08.쏠비치양양",    "central", ["쏠비치 양양"]),
    ("쏠비치삼척",                        "09.쏠비치삼척",    "central", ["쏠비치 삼척"]),
    ("소노벨단양",                        "10.소노벨단양",    "central", ["소노문 단양", "소노벨 단양"]),
    ("소노캄경주",                        "11.소노캄경주",    "south",   ["소노벨 경주", "소노캄 경주"]),
    ("소노벨청송",                        "12.소노벨청송",    "central", ["소노벨 청송"]),
    ("소노벨천안",                        "13.소노벨천안",    "central", ["소노벨 천안"]),
    ("소노벨변산",                        "14.소노벨변산",    "central", ["소노벨 변산"]),
    ("소노캄여수",                        "15.소노캄여수",    "south",   ["소노캄 여수"]),
    ("소노캄 거제",                       "16.소노캄거제",    "south",   ["소노캄 거제"]),
    ("쏠비치 진도",                       "17.쏠비치진도",    "south",   ["쏠비치 진도"]),
    ("소노벨 제주",                       "18.소노벨제주",    "apac",    ["소노벨 제주"]),
    ("소노캄 제주",                       "19.소노캄제주",    "apac",    ["소노캄 제주"]),
    ("소노캄 고양",                       "20.소노캄고양",    "apac",    ["소노캄 고양"]),
    ("소노문 해운대",                     "21.소노문해운대",  "south",   ["소노문 해운대"]),
    ("쏠비치 남해",                       "22.쏠비치남해",    "south",   ["쏠비치 남해"]),
    ("르네블루 바이 쏠비치",              "23.르네블루",      "central", ["르네블루"]),
]

BUDGET_GRAND_TOTAL_ROWS = [26, 50, 74, 98, 122, 146, 170, 194, 218, 242, 266, 290]
BUDGET_COL_RN  = 6
BUDGET_COL_ADR = 8
BUDGET_COL_REV = 10  # 千원 단위

MONTHS_26 = ["202601","202602","202603","202604","202605","202606",
             "202607","202608","202609","202610","202611","202612"]
MONTHS_25 = ["202501","202502","202503","202504","202505","202506",
             "202507","202508","202509","202510","202511","202512"]
MONTH_LABEL = {
    "202601":"1월","202602":"2월","202603":"3월","202604":"4월",
    "202605":"5월","202606":"6월","202607":"7월","202608":"8월",
    "202609":"9월","202610":"10월","202611":"11월","202612":"12월",
}
BUDGET_MONTH_LABEL = ["1월","2월","3월","4월","5월","6월","7월","8월","9월","10월","11월","12월"]


def load_budget(wb):
    """사업장별 월별 budget {display_name: {"1월":{rn,adr,rev_m},...}}"""
    budgets = {}
    for sheet_name, display_name, region, db_props in PROPERTY_DEFS:
        if sheet_name not in wb.sheetnames:
            budgets[display_name] = {}
            continue
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        monthly = {}
        for i, (row_idx, m_label) in enumerate(zip(BUDGET_GRAND_TOTAL_ROWS, BUDGET_MONTH_LABEL)):
            r = rows[row_idx - 1]
            rn  = float(r[BUDGET_COL_RN  - 1] or 0)
            adr = float(r[BUDGET_COL_ADR - 1] or 0)
            rev_cheonwon = float(r[BUDGET_COL_REV - 1] or 0)
            monthly[m_label] = {
                "rn":  int(round(rn)),
                "adr": round(adr),
                "rev_m": round(rev_cheonwon / 1000, 2),  # 千원 → 百万원
            }
        budgets[display_name] = monthly
    return budgets


def sum_db(db_bp, prop_names, month_key):
    total_rn  = 0
    total_rev = 0.0
    for pname in prop_names:
        m = db_bp.get(pname, {}).get(month_key, {})
        total_rn  += m.get("net_rn",  0)
        total_rev += m.get("net_rev", 0.0)
    adr = round((total_rev * 1000) / total_rn) if total_rn > 0 else 0
    return {"rn": total_rn, "rev_m": round(total_rev, 2), "adr": adr}


def build_month_snapshot(db_bp, budgets, month_idx):
    """특정 월(0=전체, 1~12=해당월)에 대한 byProperty + summary 반환"""
    if month_idx == 0:
        target_keys = MONTHS_26
        last_keys   = MONTHS_25
        bud_labels  = BUDGET_MONTH_LABEL
    else:
        target_keys = [MONTHS_26[month_idx - 1]]
        last_keys   = [MONTHS_25[month_idx - 1]]
        bud_labels  = [BUDGET_MONTH_LABEL[month_idx - 1]]

    props = []
    tot_bud_rn = tot_act_rn = tot_lst_rn = 0
    tot_bud_rev = tot_act_rev = 0.0

    for sheet_name, display_name, region, db_props in PROPERTY_DEFS:
        # Budget 합산
        bud_monthly = budgets.get(display_name, {})
        bud_rn  = sum(bud_monthly.get(l, {}).get("rn",    0) for l in bud_labels)
        bud_adr = (sum(bud_monthly.get(l, {}).get("adr",   0) * bud_monthly.get(l, {}).get("rn", 0)
                       for l in bud_labels) / bud_rn) if bud_rn > 0 else 0
        bud_rev = sum(bud_monthly.get(l, {}).get("rev_m", 0) for l in bud_labels)

        # 실적 합산
        act_rn = act_rev = 0
        for mk in target_keys:
            d = sum_db(db_bp, db_props, mk)
            act_rn  += d["rn"]
            act_rev += d["rev_m"]
        act_adr = round((act_rev * 1000) / act_rn) if act_rn > 0 else 0

        # 전년 합산
        lst_rn = 0
        for mk in last_keys:
            d = sum_db(db_bp, db_props, mk)
            lst_rn += d["rn"]

        rns_ach = round((act_rn / bud_rn * 100), 1) if bud_rn > 0 else 0.0
        rev_ach = round((act_rev / bud_rev * 100), 1) if bud_rev > 0 else 0.0

        props.append({
            "name":            display_name,
            "region":          region,
            "rns_budget":      bud_rn,
            "rns_actual":      act_rn,
            "rns_achievement": rns_ach,
            "rns_last":        lst_rn,
            "adr_budget":      round(bud_adr),
            "adr_actual":      act_adr,
            "rev_budget":      round(bud_rev * 1_000_000),   # 百万 → 원
            "rev_actual":      round(act_rev * 1_000_000),
            "rev_achievement": rev_ach,
            "today_booking":   0,
            "today_cancel":    0,
            "today_net":       0,
        })
        tot_bud_rn  += bud_rn
        tot_act_rn  += act_rn
        tot_lst_rn  += lst_rn
        tot_bud_rev += bud_rev
        tot_act_rev += act_rev

    tot_rns_ach = round(tot_act_rn / tot_bud_rn * 100, 1) if tot_bud_rn > 0 else 0.0
    tot_rev_ach = round(tot_act_rev / tot_bud_rev * 100, 1) if tot_bud_rev > 0 else 0.0
    tot_adr_act = round((tot_act_rev * 1_000_000) / tot_act_rn) if tot_act_rn > 0 else 0
    tot_adr_bud = round((tot_bud_rev * 1_000_000) / tot_bud_rn) if tot_bud_rn > 0 else 0
    tot_yoy = round((tot_act_rn / tot_lst_rn - 1) * 100, 1) if tot_lst_rn > 0 else 0.0

    summary = {
        "rns_budget":      tot_bud_rn,
        "rns_actual":      tot_act_rn,
        "rns_achievement": tot_rns_ach,
        "rns_last":        tot_lst_rn,
        "rns_yoy":         tot_yoy,
        "today_booking":   0,
        "today_cancel":    0,
        "today_net":       0,
        "rev_budget":      round(tot_bud_rev * 1_000_000),
        "rev_actual":      round(tot_act_rev * 1_000_000),
        "rev_achievement": tot_rev_ach,
        "adr_budget":      tot_adr_bud,
        "adr_actual":      tot_adr_act,
        "adr_vs_budget":   round((tot_adr_act / tot_adr_bud - 1) * 100, 1) if tot_adr_bud > 0 else 0.0,
    }
    return {"byProperty": props, "summary": summary}


def build_monthly_chart(db_bp, budgets):
    """Chart용 월별 집계 (전체 사업장 합산)"""
    result = []
    for i, mk in enumerate(MONTHS_26):
        bud_label = BUDGET_MONTH_LABEL[i]
        bud_rn = 0
        act_rn = 0
        lst_rn = 0
        for sheet_name, display_name, region, db_props in PROPERTY_DEFS:
            bud_rn += budgets.get(display_name, {}).get(bud_label, {}).get("rn", 0)
            d = sum_db(db_bp, db_props, mk)
            act_rn += d["rn"]
            d_last = sum_db(db_bp, db_props, MONTHS_25[i])
            lst_rn += d_last["rn"]
        result.append({
            "month": i + 1,
            "label": bud_label,
            "rns_budget": bud_rn,
            "rns_actual": act_rn,
            "rns_last":   lst_rn,
        })
    return result


def main():
    print("db_aggregated.json 로드 중...")
    db = json.loads(DB_JSON.read_text(encoding="utf-8"))
    db_bp = db.get("by_property", {})

    print("Budget XLSX 로드 중...")
    wb = openpyxl.load_workbook(BUDGET_XLSX, read_only=True, data_only=True)
    budgets = load_budget(wb)

    now_kst = datetime.now(KST)

    # 월별 스냅샷 (0=전체, 1~12=각 월)
    all_months = {}
    for m in range(0, 13):
        all_months[str(m)] = build_month_snapshot(db_bp, budgets, m)

    # Chart data
    monthly_chart = build_monthly_chart(db_bp, budgets)

    output = {
        "meta": {
            "refreshTime":  now_kst.strftime("%Y-%m-%d %H:%M KST"),
            "baseDate":     now_kst.strftime("%Y-%m-%d"),
            "dataSource":   "온북 DB + 사업계획 Budget",
        },
        "filters": {
            "months": [{"value": 0, "label": "전체"}] + [
                {"value": i+1, "label": f"{i+1}월"} for i in range(12)
            ],
            "segments": ["전체"],
        },
        # 전체(기본) 스냅샷 (월 필터=전체 상태)
        "summary":    all_months["0"]["summary"],
        "byProperty": all_months["0"]["byProperty"],
        # 월별 분리 데이터 (월 필터 작동용)
        "allMonths":  all_months,
        # Chart
        "monthly":    monthly_chart,
    }

    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✓ {OUTPUT_JSON} 생성 완료")
    print(f"  사업장 수: {len(output['byProperty'])}")
    print(f"  총 목표 RNS: {output['summary']['rns_budget']:,}")
    print(f"  총 실적 RNS: {output['summary']['rns_actual']:,}")
    print(f"  달성률: {output['summary']['rns_achievement']}%")


if __name__ == "__main__":
    main()
