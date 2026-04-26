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
import calendar
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.request import urlopen

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

# 세그먼트별 행 오프셋 (Grand Total 행 기준 상대적 위치) — 예산이 있는 세그먼트
BUDGET_SEGMENT_KEYS = ["OTA", "G-OTA", "Inbound"]
SEGMENT_KEYS = ["OTA", "G-OTA", "Inbound"]  # 초기값, main()에서 DB 기반 동적 확장
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

# 리드타임 버킷 중간값 (가중평균 계산용)
LEAD_TIME_MIDPOINTS = {
    'same_day': 0, '1_3d': 2, '4_7d': 5.5, '1_2w': 10.5,
    '2_4w': 21, '1_2m': 45, '2m_plus': 75,
}


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
    adr = round((total_rev * 1_000_000) / total_rn) if total_rn > 0 else 0
    return {"rn": total_rn, "rev_m": round(total_rev, 2), "adr": adr}


def sum_db_segments(db_bps, prop_names, month_key):
    """by_property_segment에서 OTA+G-OTA+Inbound만 합산 (예산 세그먼트 기준)"""
    total_rn  = 0
    total_rev = 0.0
    for pname in prop_names:
        prop_segs = db_bps.get(pname, {})
        for seg in BUDGET_SEGMENT_KEYS:
            m = prop_segs.get(seg, {}).get(month_key, {})
            total_rn  += m.get("booking_rn",  0)
            total_rev += m.get("booking_rev", 0.0)
    adr = round((total_rev * 1_000_000) / total_rn) if total_rn > 0 else 0
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


def _fetch_kr_holidays(year):
    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/KR"
    try:
        with urlopen(url, timeout=6) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return []


def _count_weekday_holidays(holidays, year, month):
    from datetime import date as _date
    count = 0
    for h in holidays:
        try:
            d = _date.fromisoformat(h["date"])
            if d.year == year and d.month == month and d.weekday() < 5:
                count += 1
        except Exception:
            pass
    return count


def build_holiday_factors(target_months=(4, 5, 6), cur_year=2026, base_year=2025):
    """올해 vs 작년 평일 공휴일 차이 → FCST 보정 계수 {month_num: factor}
    평일 공휴일 1일당 +3% 조정, ±10% 캡 적용.
    """
    hols_cur  = _fetch_kr_holidays(cur_year)
    hols_base = _fetch_kr_holidays(base_year)
    factors = {}
    for m in target_months:
        delta = (_count_weekday_holidays(hols_cur, cur_year, m)
               - _count_weekday_holidays(hols_base, base_year, m))
        raw = 1.0 + delta * 0.03
        factors[m] = round(max(0.90, min(1.10, raw)), 4)
    return factors


def get_adj_rn(adj_by_prop, prop_names, month_key):
    total = 0
    for pname in prop_names:
        total += adj_by_prop.get(pname, {}).get(month_key, {}).get("adjustment_rn", 0)
    return total


def _calc_fcst(act_rn, act_rev, month_num, now_kst, bud_rn, bud_rev=0):
    """elapsed_ratio 기반 월말 FCST 계산.
    진행 중인 월: (오늘 일자 / 해당월 총일수)로 extrapolate
    완료된 과거 월: ratio=1.0 (실적=FCST)
    미래 월: elapsed_ratio=0 → 실적 그대로 반환
    """
    cur_year  = now_kst.year
    cur_month = now_kst.month

    if cur_year == 2026 and month_num == cur_month:
        days_in_month = calendar.monthrange(cur_year, month_num)[1]
        elapsed_ratio = now_kst.day / days_in_month
    elif month_num < cur_month or cur_year < 2026:
        elapsed_ratio = 1.0
    else:
        elapsed_ratio = 0.0

    if elapsed_ratio == 0.0:
        return None, None, None, None, None

    if elapsed_ratio > 0.1:
        rns_fcst = round(act_rn / elapsed_ratio)
        rev_fcst = act_rev / elapsed_ratio
    else:
        rns_fcst = act_rn
        rev_fcst = act_rev

    adr_fcst     = round(rev_fcst * 1_000_000 / rns_fcst) if rns_fcst > 0 else 0
    fcst_ach     = round(rns_fcst / bud_rn  * 100, 1) if bud_rn  > 0 else 0.0
    rev_fcst_ach = round(rev_fcst / bud_rev * 100, 1) if bud_rev > 0 else 0.0

    return rns_fcst, rev_fcst, adr_fcst, fcst_ach, rev_fcst_ach


def build_yoy_table(db_bp, budgets, seg_budgets, db_bps, adj_by_prop, holiday_factors,
                    months=(4, 5, 6), now_kst=None):
    """사업장별 4·5·6월 YoY 추이 테이블 데이터 생성."""
    if now_kst is None:
        now_kst = datetime.now(KST)
    rows = []
    for sheet_name, display_name, region, db_props in PROPERTY_DEFS:
        month_data = {}
        for m in months:
            mk_26 = f"2026{m:02d}"
            mk_25 = f"2025{m:02d}"
            bud_label = BUDGET_MONTH_LABEL[m - 1]

            act_rn = sum_db(db_bp, db_props, mk_26)["rn"]

            # 전년 보정값 포함
            base_rn = 0
            adj_rn  = 0
            for pname in db_props:
                adj_m = adj_by_prop.get(pname, {}).get(mk_25, {}) if adj_by_prop else {}
                base_rn += adj_m.get("booking_rn", 0)
                adj_rn  += adj_m.get("adjustment_rn", 0)
            if base_rn == 0:
                base_rn = sum_db(db_bp, db_props, mk_25)["rn"]

            if seg_budgets:
                bud_rn = sum_seg_budget(seg_budgets, display_name, [bud_label])["rn"]
            else:
                bud_rn = budgets.get(display_name, {}).get(bud_label, {}).get("rn", 0)

            rns_fcst, _, _, fcst_ach, _ = _calc_fcst(act_rn, 0, m, now_kst, bud_rn)

            yoy = round((act_rn / base_rn - 1) * 100, 1) if base_rn > 0 else None
            month_data[m] = {
                "act_rn":       act_rn,
                "last_rn":      base_rn,
                "yoy":          yoy,
                "bud_rn":       bud_rn,
                "rns_fcst":     rns_fcst,
                "fcst_ach":     fcst_ach,
            }
        rows.append({"name": display_name, "region": region, "months": month_data})
    return rows


def build_segment_snapshot(db_seg, seg_budgets, month_idx):
    """전체 세그먼트별 budget vs actual 요약 (예산 없는 세그먼트는 budget=0)"""
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


def build_month_snapshot(db_bp, budgets, month_idx, db_seg=None, seg_budgets=None, db_bps=None,
                         adj_by_prop=None, holiday_factors=None, lead_time_by_prop=None, now_kst=None):
    """특정 월(0=전체, 1~12=해당월)에 대한 byProperty + summary + segmentData 반환
    실적/목표 모두 OTA+G-OTA+Inbound 세그먼트만 합산.
    """
    if now_kst is None:
        now_kst = datetime.now(KST)

    if month_idx == 0:
        target_keys = MONTHS_26
        last_keys   = MONTHS_25
        bud_labels  = BUDGET_MONTH_LABEL
    else:
        target_keys = [MONTHS_26[month_idx - 1]]
        last_keys   = [MONTHS_25[month_idx - 1]]
        bud_labels  = [BUDGET_MONTH_LABEL[month_idx - 1]]

    props = []
    tot_bud_rn = tot_act_rn = tot_lst_rn = tot_rns_fcst = 0
    tot_bud_rev = tot_act_rev = tot_lst_rev = tot_rev_fcst = 0.0

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
        act_adr = round(act_rev * 1_000_000 / act_rn) if act_rn > 0 else 0

        # 전년 합산 (동기간 보정 반영)
        lst_rn = 0
        for mk in last_keys:
            if adj_by_prop:
                for pname in db_props:
                    adj_m = adj_by_prop.get(pname, {}).get(mk, {})
                    lst_rn += adj_m.get("booking_rn", 0) if adj_m else sum_db(db_bp, [pname], mk)["rn"]
            else:
                d = sum_db(db_bp, db_props, mk)
                lst_rn += d["rn"]

        # 전년 매출 (adj_by_prop에는 booking_rev 없으므로 by_property에서 직접)
        lst_rev = 0.0
        for mk in last_keys:
            lst_rev += sum_db(db_bp, db_props, mk)["rev_m"]
        lst_adr = round(lst_rev * 1_000_000 / lst_rn) if lst_rn > 0 else 0

        rns_ach = round((act_rn / bud_rn * 100), 1) if bud_rn > 0 else 0.0
        rev_ach = round((act_rev / bud_rev * 100), 1) if bud_rev > 0 else 0.0
        rns_yoy = round((act_rn / lst_rn - 1) * 100, 1) if lst_rn > 0 else 0.0

        # 리드타임 분포 (사업장별, 대상 월 합산)
        lead_time = {}
        avg_lead_time = 0.0
        if lead_time_by_prop:
            lt_sum = defaultdict(int)
            for pname in db_props:
                prop_lt = lead_time_by_prop.get(pname, {})
                for mk in target_keys:
                    for bucket, cnt in prop_lt.get(mk, {}).items():
                        lt_sum[bucket] += cnt
            if lt_sum:
                lead_time = dict(lt_sum)
                total_rn = sum(lt_sum.values())
                if total_rn > 0:
                    avg_lead_time = round(
                        sum(cnt * LEAD_TIME_MIDPOINTS.get(b, 0) for b, cnt in lt_sum.items()) / total_rn, 1
                    )

        # FCST: elapsed_ratio 기반
        if month_idx > 0:
            rns_fcst, rev_fcst, adr_fcst, fcst_ach, rev_fcst_ach = _calc_fcst(
                act_rn, act_rev, month_idx, now_kst, bud_rn, bud_rev
            )
        else:
            # 전체 월: 개별 월(1~12월)별 FCST를 합산
            rns_fcst = 0
            rev_fcst = 0.0
            for mi in range(1, 13):
                mk_mi = f"2026{mi:02d}"
                if db_bps is not None:
                    mi_d = sum_db_segments(db_bps, db_props, mk_mi)
                else:
                    mi_d = sum_db(db_bp, db_props, mk_mi)
                mi_rn = mi_d["rn"]
                mi_rev = mi_d["rev_m"]
                if seg_budgets:
                    mi_bud = sum_seg_budget(seg_budgets, display_name, [BUDGET_MONTH_LABEL[mi - 1]])
                    mi_bud_rn = mi_bud["rn"]
                    mi_bud_rev = mi_bud["rev_m"]
                else:
                    mi_bud_rn = budgets.get(display_name, {}).get(BUDGET_MONTH_LABEL[mi - 1], {}).get("rn", 0)
                    mi_bud_rev = budgets.get(display_name, {}).get(BUDGET_MONTH_LABEL[mi - 1], {}).get("rev_m", 0)
                mi_rns_f, mi_rev_f, _, _, _ = _calc_fcst(mi_rn, mi_rev, mi, now_kst, mi_bud_rn, mi_bud_rev)
                if mi_rns_f is not None:
                    rns_fcst += mi_rns_f
                    rev_fcst += mi_rev_f
            adr_fcst = round(rev_fcst * 1_000_000 / rns_fcst) if rns_fcst > 0 else 0
            fcst_ach = round(rns_fcst / bud_rn * 100, 1) if bud_rn > 0 else 0.0
            rev_fcst_ach = round(rev_fcst / bud_rev * 100, 1) if bud_rev > 0 else 0.0

        props.append({
            "name":            display_name,
            "region":          region,
            "rns_budget":      bud_rn,
            "rns_actual":      act_rn,
            "rns_achievement": rns_ach,
            "rns_last":        lst_rn,
            "rns_yoy":         rns_yoy,
            "rns_fcst":        rns_fcst,
            "fcst_achievement": fcst_ach,
            "adr_budget":      round(bud_adr),
            "adr_actual":      act_adr,
            "adr_last":        lst_adr,
            "rev_budget":      round(bud_rev * 1_000_000),
            "rev_actual":      round(act_rev * 1_000_000),
            "rev_last":        round(lst_rev * 1_000_000),
            "rev_achievement": rev_ach,
            "adr_yoy":         round((act_adr / lst_adr - 1) * 100, 1) if lst_adr > 0 else 0.0,
            "rev_yoy":         round((act_rev / lst_rev - 1) * 100, 1) if lst_rev > 0 else 0.0,
            "adr_fcst":        adr_fcst,
            "rev_fcst":        round(rev_fcst * 1_000_000) if rev_fcst is not None else None,
            "adr_fcst_achievement": round(adr_fcst / bud_adr * 100, 1) if (bud_adr > 0 and adr_fcst is not None) else None,
            "rev_fcst_achievement": rev_fcst_ach,
            "today_booking":   0,
            "today_cancel":    0,
            "today_net":       0,
            "today_booking_rev": 0,
            "today_cancel_rev":  0,
            "today_net_rev":     0,
            "lead_time":       lead_time,
            "avg_lead_time":   avg_lead_time,
        })
        tot_bud_rn   += bud_rn
        tot_act_rn   += act_rn
        tot_lst_rn   += lst_rn
        tot_rns_fcst += rns_fcst if rns_fcst is not None else 0
        tot_bud_rev  += bud_rev
        tot_act_rev  += act_rev
        tot_lst_rev  += lst_rev
        tot_rev_fcst += rev_fcst if rev_fcst is not None else 0.0

    # 미래월 판별: month_idx가 단일 월이고 현재월보다 크면 FCST=None
    is_future_month = (month_idx > 0 and month_idx > now_kst.month)

    tot_rns_ach  = round(tot_act_rn  / tot_bud_rn  * 100, 1) if tot_bud_rn  > 0 else 0.0
    tot_rev_ach  = round(tot_act_rev / tot_bud_rev  * 100, 1) if tot_bud_rev  > 0 else 0.0
    tot_adr_act  = round(tot_act_rev * 1_000_000 / tot_act_rn)  if tot_act_rn  > 0 else 0
    tot_adr_bud  = round(tot_bud_rev * 1_000_000 / tot_bud_rn)  if tot_bud_rn  > 0 else 0
    tot_adr_lst  = round(tot_lst_rev * 1_000_000 / tot_lst_rn)  if tot_lst_rn  > 0 else 0
    if is_future_month:
        tot_rns_fcst = None
        tot_rev_fcst = None
        tot_adr_fcst = None
        tot_fcst_ach = None
        tot_rev_fcst_ach = None
    else:
        tot_adr_fcst = round(tot_rev_fcst * 1_000_000 / tot_rns_fcst) if tot_rns_fcst > 0 else 0
        tot_fcst_ach     = round(tot_rns_fcst / tot_bud_rn  * 100, 1) if tot_bud_rn  > 0 else 0.0
        tot_rev_fcst_ach = round(tot_rev_fcst / tot_bud_rev * 100, 1) if tot_bud_rev > 0 else 0.0
    tot_yoy      = round((tot_act_rn  / tot_lst_rn  - 1) * 100, 1) if tot_lst_rn  > 0 else 0.0

    summary = {
        "rns_budget":      tot_bud_rn,
        "rns_actual":      tot_act_rn,
        "rns_achievement": tot_rns_ach,
        "rns_last":        tot_lst_rn,
        "rns_yoy":         tot_yoy,
        "rns_fcst":        tot_rns_fcst,
        "fcst_achievement": tot_fcst_ach,
        "today_booking":   0,
        "today_cancel":    0,
        "today_net":       0,
        "today_booking_rev": 0,
        "today_cancel_rev":  0,
        "today_net_rev":     0,
        "rev_budget":      round(tot_bud_rev * 1_000_000),
        "rev_actual":      round(tot_act_rev * 1_000_000),
        "rev_last":        round(tot_lst_rev * 1_000_000),
        "rev_achievement": tot_rev_ach,
        "rev_yoy":         round((tot_act_rev / tot_lst_rev - 1) * 100, 1) if tot_lst_rev > 0 else 0.0,
        "rev_fcst":        round(tot_rev_fcst * 1_000_000) if tot_rev_fcst is not None else None,
        "rev_fcst_achievement": tot_rev_fcst_ach,
        "adr_budget":      tot_adr_bud,
        "adr_actual":      tot_adr_act,
        "adr_last":        tot_adr_lst,
        "adr_yoy":         round((tot_adr_act / tot_adr_lst - 1) * 100, 1) if tot_adr_lst > 0 else 0.0,
        "adr_fcst":        tot_adr_fcst,
        "adr_fcst_achievement": round(tot_adr_fcst / tot_adr_bud * 100, 1) if (tot_adr_bud > 0 and tot_adr_fcst is not None) else None,
        "adr_vs_budget":   round((tot_adr_act / tot_adr_bud - 1) * 100, 1) if tot_adr_bud > 0 else 0.0,
    }
    seg_data = {}
    if db_seg is not None and seg_budgets is not None:
        seg_data = build_segment_snapshot(db_seg, seg_budgets, month_idx)

    # 세그먼트별 byProperty (각 세그먼트 단독 기준)
    by_prop_seg = {}
    if db_bps is not None and seg_budgets is not None:
        for seg in SEGMENT_KEYS:
            seg_props = []
            for _, display_name, region, db_props in PROPERTY_DEFS:
                sb = seg_budgets.get(display_name, {}).get(seg, {})
                s_bud_rn  = sum(sb.get(l, {}).get("rn",    0) for l in bud_labels)
                s_bud_rev = sum(sb.get(l, {}).get("rev_m", 0) for l in bud_labels)
                s_bud_adr = (sum(sb.get(l, {}).get("adr", 0) * sb.get(l, {}).get("rn", 0)
                                 for l in bud_labels) / s_bud_rn) if s_bud_rn > 0 else 0
                s_act_rn = s_act_rev = 0
                for mk in target_keys:
                    for pname in db_props:
                        m = db_bps.get(pname, {}).get(seg, {}).get(mk, {})
                        s_act_rn  += m.get("booking_rn",  0)
                        s_act_rev += m.get("booking_rev", 0.0)
                s_rns_ach = round((s_act_rn / s_bud_rn * 100), 1) if s_bud_rn > 0 else 0.0
                s_rev_ach = round((s_act_rev / s_bud_rev * 100), 1) if s_bud_rev > 0 else 0.0
                s_act_adr = round((s_act_rev * 1_000_000) / s_act_rn) if s_act_rn > 0 else 0
                seg_props.append({
                    "name": display_name,
                    "region": region,
                    "rns_budget":      s_bud_rn,
                    "rns_actual":      s_act_rn,
                    "rns_achievement": s_rns_ach,
                    "rns_last":        0,
                    "rns_yoy":         0.0,
                    "rns_fcst":        s_act_rn,
                    "fcst_achievement": s_rns_ach,
                    "adr_budget":      round(s_bud_adr),
                    "adr_actual":      s_act_adr,
                    "rev_budget":      round(s_bud_rev * 1_000_000),
                    "rev_actual":      round(s_act_rev * 1_000_000),
                    "rev_achievement": s_rev_ach,
                    "today_booking":   0,
                    "today_cancel":    0,
                    "today_net":       0,
                    "lead_time":       {},
                })
            by_prop_seg[seg] = seg_props

    return {"byProperty": props, "byPropertySegment": by_prop_seg, "summary": summary, "segmentData": seg_data}


def build_monthly_chart(db_bp, budgets, seg_budgets=None, db_bps=None, adj_by_prop=None):
    """Chart용 월별 집계 (전체 사업장 합산, OTA+G-OTA+Inbound 기준)"""
    result = []
    for i, mk in enumerate(MONTHS_26):
        bud_label = BUDGET_MONTH_LABEL[i]
        mk_25 = MONTHS_25[i]
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
            if adj_by_prop:
                for pname in db_props:
                    adj_m = adj_by_prop.get(pname, {}).get(mk_25, {})
                    lst_rn += adj_m.get("booking_rn", 0) if adj_m else sum_db(db_bp, [pname], mk_25)["rn"]
            else:
                d_last = sum_db(db_bp, db_props, mk_25)
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
    """pickup_daily_by_property에서 특정 날짜의 사업장별 예약 RN, REV 합산."""
    if not date_str:
        return 0, 0
    pdbp = db.get("pickup_daily_by_property", {})
    rn  = sum(pdbp.get(pname, {}).get(date_str, {}).get("rn",  0)   for pname in db_props)
    rev = sum(pdbp.get(pname, {}).get(date_str, {}).get("rev", 0.0) for pname in db_props)
    return rn, rev


def get_today_cancel_by_props(db, date_str, db_props):
    """cancel_daily_by_property에서 특정 날짜의 사업장별 취소 RN, REV 합산."""
    if not date_str:
        return 0, 0
    cdbp = db.get("cancel_daily_by_property", {})
    rn  = sum(cdbp.get(pname, {}).get(date_str, {}).get("rn",  0)   for pname in db_props)
    rev = sum(cdbp.get(pname, {}).get(date_str, {}).get("rev", 0.0) for pname in db_props)
    return rn, rev


def get_today_booking_by_props_month(db, date_str, db_props, stay_month):
    """pickup_daily_by_property_month에서 특정 날짜·투숙월의 사업장별 예약 합산."""
    if not date_str:
        return 0, 0
    pdbpm = db.get("pickup_daily_by_property_month", {})
    rn  = sum(pdbpm.get(pname, {}).get(stay_month, {}).get(date_str, {}).get("rn",  0)   for pname in db_props)
    rev = sum(pdbpm.get(pname, {}).get(stay_month, {}).get(date_str, {}).get("rev", 0.0) for pname in db_props)
    return rn, rev


def get_today_cancel_by_props_month(db, date_str, db_props, stay_month):
    """cancel_daily_by_property_month에서 특정 날짜·투숙월의 사업장별 취소 합산."""
    if not date_str:
        return 0, 0
    cdbpm = db.get("cancel_daily_by_property_month", {})
    rn  = sum(cdbpm.get(pname, {}).get(stay_month, {}).get(date_str, {}).get("rn",  0)   for pname in db_props)
    rev = sum(cdbpm.get(pname, {}).get(stay_month, {}).get(date_str, {}).get("rev", 0.0) for pname in db_props)
    return rn, rev


def get_today_summary_by_month(db, now_kst, stay_month):
    """net_daily_by_month에서 특정 투숙월의 오늘 데이터를 반환."""
    net_daily_m = db.get("net_daily_by_month", {}).get(stay_month, {})
    for days_ago in range(0, 7):
        date_str = (now_kst - timedelta(days=days_ago)).strftime("%Y%m%d")
        if date_str in net_daily_m:
            entry = net_daily_m[date_str]
            pickup = entry.get("pickup_rn", 0)
            cancel = entry.get("cancel_rn", 0)
            if pickup > 0 or cancel > 0:
                return pickup, cancel, entry.get("net_rn", 0)
    if net_daily_m:
        most_recent = max(net_daily_m.keys())
        entry = net_daily_m[most_recent]
        return entry.get("pickup_rn", 0), entry.get("cancel_rn", 0), entry.get("net_rn", 0)
    return 0, 0, 0


def main():
    print("db_aggregated.json 로드 중...")
    db = json.loads(DB_JSON.read_text(encoding="utf-8"))
    db_bp  = db.get("by_property", {})
    db_seg = db.get("by_segment", {})
    db_bps = db.get("by_property_segment", {})

    # 전체 세그먼트 동적 탐색 (기타 제외, 예산 세그먼트 우선)
    global SEGMENT_KEYS
    all_segs_set = set()
    for prop_segs in db_bps.values():
        all_segs_set.update(prop_segs.keys())
    all_segs_set.discard("기타")  # 기타 제거
    # 예산 세그먼트 우선, 나머지 가나다 정렬
    other_segs = sorted(all_segs_set - set(BUDGET_SEGMENT_KEYS))
    SEGMENT_KEYS = BUDGET_SEGMENT_KEYS + other_segs
    print(f"  세그먼트 탐색: {len(SEGMENT_KEYS)}개 (기타 제외)")

    print("Budget XLSX 로드 중...")
    wb = openpyxl.load_workbook(BUDGET_XLSX, read_only=True, data_only=True)
    budgets, seg_budgets = load_budget(wb)

    now_kst = datetime.now(KST)

    # YoY 동기간 보정값 로드
    adj_by_prop = {}
    yoy_base_date = ""
    yoy_adj_section = db.get("yoy_adjusted", {})
    if "2025" in yoy_adj_section:
        adj_by_prop   = yoy_adj_section["2025"].get("by_property", {})
        yoy_base_date = yoy_adj_section["2025"].get("base_date_full", "")
    print(f"  YoY 동기간 보정 로드: 사업장 수={len(adj_by_prop)}, base={yoy_base_date}")

    # 사업장별 리드타임 분포
    lead_time_by_prop = db.get("lead_time_by_property", {})

    # 공휴일 보정 계수 (4·5·6월)
    print("  공휴일 보정 계수 계산 중...")
    holiday_factors = build_holiday_factors(target_months=(4, 5, 6), cur_year=2026, base_year=2025)
    print(f"  holiday_factors={holiday_factors}")

    # 오늘(또는 가장 최근) 예약/취소/순증 데이터
    today_booking, today_cancel, today_net, today_date = get_today_summary(db, now_kst)
    print(f"  오늘 데이터 날짜: {today_date}")
    print(f"  today_booking={today_booking}, today_cancel={today_cancel}, today_net={today_net}")

    # 월별 스냅샷 (0=전체, 1~12=각 월)
    all_months = {}
    for m in range(0, 13):
        all_months[str(m)] = build_month_snapshot(
            db_bp, budgets, m,
            db_seg=db_seg, seg_budgets=seg_budgets, db_bps=db_bps,
            adj_by_prop=adj_by_prop, holiday_factors=holiday_factors,
            lead_time_by_prop=lead_time_by_prop, now_kst=now_kst,
        )

    # today 데이터를 월 스냅샷에 주입 (월별로 stay_month 필터 적용)
    for m_str, snap in all_months.items():
        if m_str == "0":
            snap["summary"]["today_booking"] = today_booking
            snap["summary"]["today_cancel"]  = today_cancel
            snap["summary"]["today_net"]     = today_net
        else:
            stay_month = f"2026{int(m_str):02d}"
            m_booking, m_cancel, m_net = get_today_summary_by_month(db, now_kst, stay_month)
            snap["summary"]["today_booking"] = m_booking
            snap["summary"]["today_cancel"]  = m_cancel
            snap["summary"]["today_net"]     = m_net

        for prop in snap["byProperty"]:
            db_props = next((d for _, n, _, d in PROPERTY_DEFS if n == prop["name"]), [])
            if m_str == "0":
                prop_booking, prop_booking_rev = get_today_booking_by_props(db, today_date, db_props)
                prop_cancel,  prop_cancel_rev  = get_today_cancel_by_props(db, today_date, db_props)
            else:
                stay_month = f"2026{int(m_str):02d}"
                prop_booking, prop_booking_rev = get_today_booking_by_props_month(db, today_date, db_props, stay_month)
                prop_cancel,  prop_cancel_rev  = get_today_cancel_by_props_month(db, today_date, db_props, stay_month)
            prop["today_booking"]     = prop_booking
            prop["today_cancel"]      = prop_cancel
            prop["today_net"]         = prop_booking - prop_cancel
            prop["today_booking_rev"] = round(prop_booking_rev * 1_000_000)
            prop["today_cancel_rev"]  = round(prop_cancel_rev  * 1_000_000)
            prop["today_net_rev"]     = round((prop_booking_rev - prop_cancel_rev) * 1_000_000)

        if m_str == "0":
            snap["summary"]["today_booking_rev"] = sum(p["today_booking_rev"] for p in snap["byProperty"])
            snap["summary"]["today_cancel_rev"]  = sum(p["today_cancel_rev"]  for p in snap["byProperty"])
            snap["summary"]["today_net_rev"]     = sum(p["today_net_rev"]     for p in snap["byProperty"])

        for seg_props in snap.get("byPropertySegment", {}).values():
            for prop in seg_props:
                db_props = next((d for _, n, _, d in PROPERTY_DEFS if n == prop["name"]), [])
                if m_str == "0":
                    prop_booking, _ = get_today_booking_by_props(db, today_date, db_props)
                    prop_cancel,  _ = get_today_cancel_by_props(db, today_date, db_props)
                else:
                    stay_month = f"2026{int(m_str):02d}"
                    prop_booking, _ = get_today_booking_by_props_month(db, today_date, db_props, stay_month)
                    prop_cancel,  _ = get_today_cancel_by_props_month(db, today_date, db_props, stay_month)
                prop["today_booking"] = prop_booking
                prop["today_cancel"]  = prop_cancel
                prop["today_net"]     = prop_booking - prop_cancel

    # Chart data (동기간 보정 반영)
    monthly_chart = build_monthly_chart(
        db_bp, budgets, seg_budgets=seg_budgets, db_bps=db_bps, adj_by_prop=adj_by_prop
    )

    # YoY 사업장별 추이 테이블 (4·5·6월)
    yoy_table = build_yoy_table(
        db_bp, budgets, seg_budgets, db_bps, adj_by_prop, holiday_factors,
        months=(4, 5, 6), now_kst=now_kst,
    )

    output = {
        "meta": {
            "refreshTime":  now_kst.strftime("%Y-%m-%d %H:%M KST"),
            "baseDate":     now_kst.strftime("%Y-%m-%d"),
            "yoyBaseDate":  yoy_base_date,
            "dataSource":   "온북 DB + 사업계획 Budget",
        },
        "filters": {
            "months": [{"value": 0, "label": "전체"}] + [
                {"value": i+1, "label": f"{i+1}월"} for i in range(12)
            ],
            "segments": ["전체"] + SEGMENT_KEYS,
        },
        # 전체(기본) 스냅샷 (월 필터=전체 상태)
        "summary":    all_months["0"]["summary"],
        "byProperty": all_months["0"]["byProperty"],
        # 월별 분리 데이터 (월 필터 작동용)
        "allMonths":  all_months,
        # Chart
        "monthly":    monthly_chart,
        # YoY 사업장별 추이
        "yoyTable":   yoy_table,
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
