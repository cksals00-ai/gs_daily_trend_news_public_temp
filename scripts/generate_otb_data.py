#!/usr/bin/env python3
"""
generate_otb_data.py
BI 26OTB 시트 기준으로 docs/data/otb_data.json 생성
- 사업장 순서: PROPERTY_DEFS (BI 시트 순서)
- 실적: db_aggregated.json (booking_rn, booking_rev, adr)
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

# 세그먼트별 행 오프셋 (Grand Total 행 기준 상대적 위치)
SEGMENT_KEYS = ["OTA", "G-OTA", "Inbound"]
SEGMENT_ROW_OFFSETS = {"OTA": -6, "G-OTA": -5, "Inbound": -9}

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
    """사업장별 월별 budget 및 세그먼트별 budget 반환
    Returns:
        budgets:     {display_name: {"1월": {rn, adr, rev_m}, ...}}
        seg_budgets: {display_name: {seg: {"1월": {rn, adr, rev_m}, ...}, ...}}
    """
    budgets = {}
    seg_budgets = {}
    for sheet_name, display_name, region, db_props in PROPERTY_DEFS:
        if sheet_name not in wb.sheetnames:
            budgets[display_name] = {}
            seg_budgets[display_name] = {s: {} for s in SEGMENT_KEYS}
            continue
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        monthly = {}
        monthly_seg = {s: {} for s in SEGMENT_KEYS}
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
            # 세그먼트별 행 읽기
            for seg, offset in SEGMENT_ROW_OFFSETS.items():
                seg_r = rows[(row_idx + offset) - 1]
                seg_rn  = float(seg_r[BUDGET_COL_RN  - 1] or 0)
                seg_adr = float(seg_r[BUDGET_COL_ADR - 1] or 0)
                seg_rev = float(seg_r[BUDGET_COL_REV - 1] or 0)
                monthly_seg[seg][m_label] = {
                    "rn":    int(round(seg_rn)),
                    "adr":   round(seg_adr),
                    "rev_m": round(seg_rev / 1000, 2),
                }
        budgets[display_name] = monthly
        seg_budgets[display_name] = monthly_seg
    return budgets, seg_budgets


def sum_db(db_bp, prop_names, month_key):
    total_rn  = 0
    total_rev = 0.0
    for pname in prop_names:
        m = db_bp.get(pname, {}).get(month_key, {})
        total_rn  += m.get("booking_rn",  0)
        total_rev += m.get("booking_rev", 0.0)
    adr = round((total_rev * 1000) / total_rn) if total_rn > 0 else 0
    return {"rn": total_rn, "rev_m": round(total_rev, 2), "adr": adr}


def sum_db_segments(db_bps, prop_names, month_key):
    """by_property_segment에서 OTA+G-OTA+Inbound만 합산"""
    total_rn  = 0
    total_rev = 0.0
    for pname in prop_names:
        prop_segs = db_bps.get(pname, {})
        for seg in SEGMENT_KEYS:
            m = prop_segs.get(seg, {}).get(month_key, {})
            total_rn  += m.get("booking_rn",  0)
            total_rev += m.get("booking_rev", 0.0)
    adr = round((total_rev * 1000) / total_rn) if total_rn > 0 else 0
    return {"rn": total_rn, "rev_m": round(total_rev, 2), "adr": adr}


def sum_seg_budget(seg_budgets, display_name, bud_labels):
    """seg_budgets에서 OTA+G-OTA+Inbound 합산 → budget rn/rev/adr"""
    rn  = 0
    rev = 0.0
    for seg in SEGMENT_KEYS:
        prop_seg = seg_budgets.get(display_name, {}).get(seg, {})
        rn  += sum(prop_seg.get(l, {}).get("rn",    0)    for l in bud_labels)
        rev += sum(prop_seg.get(l, {}).get("rev_m", 0.0)  for l in bud_labels)
    adr = round(rev * 1_000_000 / rn) if rn > 0 else 0
    return {"rn": rn, "rev_m": rev, "adr": adr}


def build_segment_snapshot(db_seg, seg_budgets, month_idx):
    """OTA/G-OTA/Inbound 세그먼트별 budget vs actual 요약"""
    if month_idx == 0:
        target_keys = MONTHS_26
        bud_labels  = BUDGET_MONTH_LABEL
    else:
        target_keys = [MONTHS_26[month_idx - 1]]
        bud_labels  = [BUDGET_MONTH_LABEL[month_idx - 1]]

    result = {}
    for seg in SEGMENT_KEYS:
        # Budget: 전체 사업장 합산
        bud_rn = 0
        bud_rev = 0.0
        for _, display_name, _, _ in PROPERTY_DEFS:
            prop_seg = seg_budgets.get(display_name, {}).get(seg, {})
            bud_rn  += sum(prop_seg.get(l, {}).get("rn",    0) for l in bud_labels)
            bud_rev += sum(prop_seg.get(l, {}).get("rev_m", 0) for l in bud_labels)
        bud_adr = round(bud_rev * 1_000_000 / bud_rn) if bud_rn > 0 else 0

        # Actual: db_seg 집계
        seg_db  = db_seg.get(seg, {})
        act_rn  = sum(seg_db.get(mk, {}).get("booking_rn",  0)   for mk in target_keys)
        act_rev = sum(seg_db.get(mk, {}).get("booking_rev", 0.0) for mk in target_keys)
        act_adr = round(act_rev * 1_000_000 / act_rn) if act_rn > 0 else 0

        rns_ach = round(act_rn  / bud_rn  * 100, 1) if bud_rn  > 0 else 0.0
        rev_ach = round(act_rev / bud_rev * 100, 1) if bud_rev > 0 else 0.0

        result[seg] = {
            "rns_budget":      bud_rn,
            "rns_actual":      act_rn,
            "rns_achievement": rns_ach,
            "rns_last":        0,
            "rns_yoy":         0.0,
            "today_booking":   0,
            "today_cancel":    0,
            "today_net":       0,
            "rev_budget":      round(bud_rev * 1_000_000),
            "rev_actual":      round(act_rev * 1_000_000),
            "rev_achievement": rev_ach,
            "adr_budget":      bud_adr,
            "adr_actual":      act_adr,
            "adr_vs_budget":   round((act_adr / bud_adr - 1) * 100, 1) if bud_adr > 0 else 0.0,
        }
    return result


def build_month_snapshot(db_bp, budgets, month_idx, db_seg=None, seg_budgets=None, db_bps=None):
    """특정 월(0=전체, 1~12=해당월)에 대한 byProperty + summary + segmentData 반환
    실적/목표 모두 OTA+G-OTA+Inbound 세그먼트만 합산.
    """
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
        # Budget: OTA+G-OTA+Inbound 세그먼트만 합산
        if seg_budgets:
            b = sum_seg_budget(seg_budgets, display_name, bud_labels)
            bud_rn  = b["rn"]
            bud_adr = b["adr"]
            bud_rev = b["rev_m"]
        else:
            bud_monthly = budgets.get(display_name, {})
            bud_rn  = sum(bud_monthly.get(l, {}).get("rn",    0) for l in bud_labels)
            bud_adr = (sum(bud_monthly.get(l, {}).get("adr",   0) * bud_monthly.get(l, {}).get("rn", 0)
                           for l in bud_labels) / bud_rn) if bud_rn > 0 else 0
            bud_rev = sum(bud_monthly.get(l, {}).get("rev_m", 0) for l in bud_labels)

        # 실적: by_property_segment → OTA+G-OTA+Inbound만
        act_rn = act_rev = 0
        if db_bps is not None:
            for mk in target_keys:
                d = sum_db_segments(db_bps, db_props, mk)
                act_rn  += d["rn"]
                act_rev += d["rev_m"]
        else:
            for mk in target_keys:
                d = sum_db(db_bp, db_props, mk)
                act_rn  += d["rn"]
                act_rev += d["rev_m"]
        act_adr = round((act_rev * 1000) / act_rn) if act_rn > 0 else 0

        # 전년 합산 (by_property 전체 기준 유지 — 전년도 세그먼트 구분 없음)
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
    seg_data = {}
    if db_seg is not None and seg_budgets is not None:
        seg_data = build_segment_snapshot(db_seg, seg_budgets, month_idx)
    return {"byProperty": props, "summary": summary, "segmentData": seg_data}


def build_monthly_chart(db_bp, budgets, seg_budgets=None, db_bps=None):
    """Chart용 월별 집계 (전체 사업장 합산, OTA+G-OTA+Inbound 기준)"""
    result = []
    for i, mk in enumerate(MONTHS_26):
        bud_label = BUDGET_MONTH_LABEL[i]
        bud_rn = 0
        act_rn = 0
        lst_rn = 0
        for sheet_name, display_name, region, db_props in PROPERTY_DEFS:
            if seg_budgets:
                bud_rn += sum_seg_budget(seg_budgets, display_name, [bud_label])["rn"]
            else:
                bud_rn += budgets.get(display_name, {}).get(bud_label, {}).get("rn", 0)
            if db_bps is not None:
                d = sum_db_segments(db_bps, db_props, mk)
            else:
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


def get_today_summary(db, now_kst):
    """net_daily에서 오늘 데이터를 반환. 없거나 0이면 최근 날짜로 fallback."""
    net_daily = db.get("net_daily", {})
    for days_ago in range(0, 7):
        date_str = (now_kst - timedelta(days=days_ago)).strftime("%Y%m%d")
        if date_str in net_daily:
            entry = net_daily[date_str]
            pickup = entry.get("pickup_rn", 0)
            cancel = entry.get("cancel_rn", 0)
            if pickup > 0 or cancel > 0:
                return pickup, cancel, entry.get("net_rn", 0), date_str
    if net_daily:
        most_recent = max(net_daily.keys())
        entry = net_daily[most_recent]
        return entry.get("pickup_rn", 0), entry.get("cancel_rn", 0), entry.get("net_rn", 0), most_recent
    return 0, 0, 0, None


def get_today_booking_by_props(db, date_str, db_props):
    """pickup_daily_by_property에서 특정 날짜의 사업장별 예약 RN 합산."""
    if not date_str:
        return 0
    pdbp = db.get("pickup_daily_by_property", {})
    return sum(pdbp.get(pname, {}).get(date_str, {}).get("rn", 0) for pname in db_props)


def main():
    print("db_aggregated.json 로드 중...")
    db = json.loads(DB_JSON.read_text(encoding="utf-8"))
    db_bp  = db.get("by_property", {})
    db_seg = db.get("by_segment", {})
    db_bps = db.get("by_property_segment", {})

    print("Budget XLSX 로드 중...")
    wb = openpyxl.load_workbook(BUDGET_XLSX, read_only=True, data_only=True)
    budgets, seg_budgets = load_budget(wb)

    now_kst = datetime.now(KST)

    # 오늘(또는 가장 최근) 예약/취소/순증 데이터
    today_booking, today_cancel, today_net, today_date = get_today_summary(db, now_kst)
    print(f"  오늘 데이터 날짜: {today_date}")
    print(f"  today_booking={today_booking}, today_cancel={today_cancel}, today_net={today_net}")

    # 월별 스냅샷 (0=전체, 1~12=각 월)
    all_months = {}
    for m in range(0, 13):
        all_months[str(m)] = build_month_snapshot(db_bp, budgets, m, db_seg=db_seg, seg_budgets=seg_budgets, db_bps=db_bps)

    # today 데이터를 모든 월 스냅샷에 주입
    for snap in all_months.values():
        snap["summary"]["today_booking"] = today_booking
        snap["summary"]["today_cancel"]  = today_cancel
        snap["summary"]["today_net"]     = today_net
        for prop in snap["byProperty"]:
            db_props = next((d for _, n, _, d in PROPERTY_DEFS if n == prop["name"]), [])
            prop_booking = get_today_booking_by_props(db, today_date, db_props)
            prop["today_booking"] = prop_booking
            prop["today_cancel"]  = 0
            prop["today_net"]     = prop_booking

    # Chart data
    monthly_chart = build_monthly_chart(db_bp, budgets, seg_budgets=seg_budgets, db_bps=db_bps)

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
            "segments": ["전체", "OTA", "G-OTA", "Inbound"],
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
