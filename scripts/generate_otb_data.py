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

import openpyxl

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
DOCS_DATA_DIR = PROJECT_DIR / "docs" / "data"
BUDGET_XLSX = DATA_DIR / "raw_db" / "budget" / "★최종★(검토완료)_2026년 객실 사업계획_총량 수립(2차+사업장변경건).xlsx"
DB_JSON = DATA_DIR / "db_aggregated.json"
RM_FCST_JSON = DATA_DIR / "rm_fcst.json"
HOLIDAYS_KR_JSON = DATA_DIR / "holidays_kr.json"
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


def _load_local_holidays():
    """holidays_kr.json에서 공휴일 데이터 로드. {date_str: {name, type}} 형식."""
    if HOLIDAYS_KR_JSON.exists():
        try:
            data = json.loads(HOLIDAYS_KR_JSON.read_text(encoding="utf-8"))
            return data.get("holidays", {})
        except Exception:
            pass
    return {}


def _count_weekday_holidays_local(holidays_dict, year, month):
    """holidays_kr.json 기반 평일 공휴일 수 계산."""
    from datetime import date as _date
    count = 0
    prefix = f"{year}{month:02d}"
    for date_str, info in holidays_dict.items():
        if not date_str.startswith(prefix):
            continue
        try:
            d = _date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            if d.weekday() < 5:  # 월~금
                count += 1
        except Exception:
            pass
    return count


def build_holiday_factors(target_months=None, cur_year=2026, base_year=2025):
    """올해 vs 작년 평일 공휴일 차이 → FCST 보정 계수 {month_num: factor}
    평일 공휴일 1일당 +3% 조정, ±10% 캡 적용.
    로컬 holidays_kr.json 사용 (외부 API 미사용).
    """
    if target_months is None:
        target_months = tuple(range(1, 13))
    holidays_dict = _load_local_holidays()
    factors = {}
    for m in target_months:
        delta = (_count_weekday_holidays_local(holidays_dict, cur_year, m)
               - _count_weekday_holidays_local(holidays_dict, base_year, m))
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


def _calc_fcst_enhanced(act_rn, month_num, now_kst, bud_rn, db_bp, db_props,
                         holiday_factors=None, db_bps=None):
    """4개년 과거 데이터 + 공휴일/연휴 보정 기반 AI FCST.

    전략:
    1) 과거 4개년(2022~2025) 동월 실적 수집 → 추세(trend) 계산
    2) 진행중 월: 전년 동월 실적 × YoY 성장률(올해 경과 실적 기반) + 휴일보정
       → 단순외삽과 가중평균 (elapsed 비율에 따라 비중 조절)
    3) 미래월: 전년 동월 실적 × 최근 완료 2개월 평균 YoY 성장률 + 휴일보정
    4) 완료월: 실적 = AI FCST

    OTA+G-OTA+Inbound 3세그먼트 기준 (db_bps 우선, 없으면 db_bp fallback).
    """
    cur_year = now_kst.year
    cur_month = now_kst.month
    day = now_kst.day

    # ── elapsed 판별 ──
    if cur_year == 2026 and month_num == cur_month:
        days_in_month = calendar.monthrange(cur_year, month_num)[1]
        elapsed_ratio = day / days_in_month
    elif month_num < cur_month or cur_year < 2026:
        elapsed_ratio = 1.0
    else:
        elapsed_ratio = 0.0

    # 완료 월 → 실적 그대로
    if elapsed_ratio == 1.0:
        fcst_ach = round(act_rn / bud_rn * 100, 1) if bud_rn > 0 else 0.0
        return act_rn, fcst_ach

    # ── helper: 3세그먼트 합산 실적 ──
    def _seg_rn(props, mk):
        if db_bps is not None:
            return sum_db_segments(db_bps, props, mk)["rn"]
        return sum_db(db_bp, props, mk)["rn"]

    # ── 과거 4년 동월 실적 수집 ──
    hist = {}  # {year: rn}
    for yr in [2022, 2023, 2024, 2025]:
        mk = f"{yr}{month_num:02d}"
        rn = _seg_rn(db_props, mk)
        if rn > 0:
            hist[yr] = rn

    # ── 방법 1: 단순 elapsed_ratio 외삽 ──
    if elapsed_ratio > 0.1:
        fcst_simple = round(act_rn / elapsed_ratio)
    else:
        fcst_simple = act_rn

    # ── 방법 2: 과거 패턴 기반 ──
    mk_25 = f"2025{month_num:02d}"
    last_yr_rn = hist.get(2025, 0)
    if last_yr_rn == 0:
        last_yr_rn = _seg_rn(db_props, mk_25)

    if elapsed_ratio > 0 and hist:
        # 진행 중인 월
        if last_yr_rn > 0 and act_rn > 0:
            yoy_growth = act_rn / (last_yr_rn * elapsed_ratio) if elapsed_ratio > 0.1 else 1.0
            fcst_pattern = round(last_yr_rn * yoy_growth)
        elif hist:
            avg_past = sum(hist.values()) / len(hist)
            fcst_pattern = round(avg_past * (act_rn / (avg_past * elapsed_ratio))) if (avg_past > 0 and elapsed_ratio > 0.1) else act_rn
        else:
            fcst_pattern = fcst_simple
    elif elapsed_ratio == 0 and hist:
        # 미래월: 최근 완료 2개월 평균 YoY 성장률 적용
        yoy_ratios = []
        for lookback in range(1, 4):
            rm = cur_month - lookback
            if rm < 1:
                rm += 12
            mk_26_r = f"2026{rm:02d}"
            mk_25_r = f"2025{rm:02d}"
            r26 = _seg_rn(db_props, mk_26_r)
            r25 = _seg_rn(db_props, mk_25_r)
            if r25 > 0 and r26 > 0:
                yoy_ratios.append(r26 / r25)
            if len(yoy_ratios) >= 2:
                break

        if last_yr_rn > 0 and yoy_ratios:
            avg_yoy = sum(yoy_ratios) / len(yoy_ratios)
            fcst_pattern = round(last_yr_rn * avg_yoy)
        elif last_yr_rn > 0:
            fcst_pattern = last_yr_rn  # YoY 데이터 부족 → 전년과 동일 추정
        else:
            # 전년도 없으면 과거 4년 평균의 추세 사용
            vals = [hist[y] for y in sorted(hist)]
            if len(vals) >= 2:
                trend = vals[-1] / vals[-2]
                fcst_pattern = round(vals[-1] * trend)
            else:
                fcst_pattern = vals[-1] if vals else 0
    else:
        fcst_pattern = fcst_simple

    # ── 공휴일/연휴 보정 ──
    hf = holiday_factors.get(month_num, 1.0) if holiday_factors else 1.0
    fcst_pattern = round(fcst_pattern * hf)

    # ── 가중평균: elapsed 비율이 높을수록 단순외삽 신뢰도 ↑ ──
    if elapsed_ratio > 0.5:
        w_simple = min(elapsed_ratio * 1.2, 0.85)
        fcst_final = round(fcst_simple * w_simple + fcst_pattern * (1.0 - w_simple))
    elif elapsed_ratio > 0.1:
        w_simple = elapsed_ratio
        fcst_final = round(fcst_simple * w_simple + fcst_pattern * (1.0 - w_simple))
    elif elapsed_ratio > 0:
        fcst_final = fcst_pattern
    else:
        fcst_final = fcst_pattern

    fcst_ach = round(fcst_final / bud_rn * 100, 1) if bud_rn > 0 else 0.0
    return fcst_final, fcst_ach


def load_rm_fcst():
    """data/rm_fcst.json 로드 → {display_name: {"2026-MM": {rm_fcst_rn, rm_budget_rn}}}"""
    if not RM_FCST_JSON.exists():
        return {}
    try:
        data = json.loads(RM_FCST_JSON.read_text(encoding="utf-8"))
        return data.get("properties", {})
    except Exception:
        return {}


def build_yoy_table(db_bp, budgets, seg_budgets, db_bps, adj_by_prop, holiday_factors,
                    months=(4, 5, 6), now_kst=None, rm_fcst_props=None):
    """사업장별 4·5·6월 YoY 추이 테이블 데이터 생성.
    rm_fcst_props: RM FCST 데이터 (load_rm_fcst() 결과)
    """
    if now_kst is None:
        now_kst = datetime.now(KST)
    if rm_fcst_props is None:
        rm_fcst_props = {}
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

            # AI FCST (4개년 패턴 + 휴일 보정)
            rns_fcst_ai, fcst_ach_ai = _calc_fcst_enhanced(
                act_rn, m, now_kst, bud_rn, db_bp, db_props, holiday_factors,
                db_bps=db_bps)

            # RM FCST (전체 세그먼트 기준 — rm_budget_rn 대비 달성률)
            rm_key = f"2026-{m:02d}"
            rm_entry = rm_fcst_props.get(display_name, {}).get(rm_key, {})
            rm_rn = rm_entry.get("rm_fcst_rn")
            rm_budget = rm_entry.get("rm_budget_rn")
            rm_ach = round(rm_rn / rm_budget * 100, 1) if (rm_rn and rm_budget and rm_budget > 0) else None

            yoy = round((act_rn / base_rn - 1) * 100, 1) if base_rn > 0 else None
            month_data[m] = {
                "act_rn":       act_rn,
                "last_rn":      base_rn,
                "yoy":          yoy,
                "bud_rn":       bud_rn,
                "rns_fcst":     rns_fcst if rns_fcst else (rns_fcst_ai or act_rn),
                "fcst_ach":     fcst_ach if fcst_ach else (fcst_ach_ai or 0),
                "rns_fcst_ai":  rns_fcst_ai,
                "fcst_ach_ai":  fcst_ach_ai,
                "rm_fcst_rn":   rm_rn,
                "rm_budget_rn": rm_budget,
                "rm_fcst_ach":  rm_ach,
            }
        rows.append({"name": display_name, "region": region, "months": month_data})
    return rows


def build_segment_snapshot(db_seg, seg_budgets, month_idx, adj_by_segment=None):
    """전체 세그먼트별 budget vs actual 요약 (예산 없는 세그먼트는 budget=0)
    adj_by_segment: 세그먼트별 동기간 보정 데이터 (2025년)
    """
    if month_idx == 0:
        target_keys = MONTHS_26
        ly_keys     = MONTHS_25
        bud_labels  = BUDGET_MONTH_LABEL
    else:
        target_keys = [MONTHS_26[month_idx - 1]]
        ly_keys     = [MONTHS_25[month_idx - 1]]
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

        # LY: 2025년 동기간 보정 적용
        if adj_by_segment and seg in adj_by_segment:
            seg_adj = adj_by_segment[seg]
            ly_rn  = sum(seg_adj.get(mk, {}).get("booking_rn", 0)  for mk in ly_keys)
            ly_rev_m = sum(seg_adj.get(mk, {}).get("booking_rev_m", 0.0) for mk in ly_keys)
            ly_rev = ly_rev_m  # 이미 백만원 단위
            ly_adr = round(ly_rev * 1_000_000 / ly_rn) if ly_rn > 0 else 0
        else:
            ly_rn   = sum(seg_db.get(mk, {}).get("booking_rn",  0)   for mk in ly_keys)
            ly_rev  = sum(seg_db.get(mk, {}).get("booking_rev", 0.0) for mk in ly_keys)
            ly_adr  = round(ly_rev * 1_000_000 / ly_rn) if ly_rn > 0 else 0

        rns_ach = round(act_rn  / bud_rn  * 100, 1) if bud_rn  > 0 else 0.0
        rev_ach = round(act_rev / bud_rev * 100, 1) if bud_rev > 0 else 0.0
        rns_yoy = round((act_rn / ly_rn - 1) * 100, 1) if ly_rn > 0 else 0.0

        # LY rev: adj_by_segment 있으면 백만원 단위, 없으면 원 단위
        ly_rev_won = round(ly_rev * 1_000_000) if adj_by_segment and seg in adj_by_segment else round(ly_rev * 1_000_000)

        result[seg] = {
            "rns_budget":      bud_rn,
            "rns_actual":      act_rn,
            "rns_achievement": rns_ach,
            "rns_last":        ly_rn,
            "rns_yoy":         rns_yoy,
            "today_booking":   0,
            "today_cancel":    0,
            "today_net":       0,
            "rev_budget":      round(bud_rev * 1_000_000),
            "rev_actual":      round(act_rev * 1_000_000),
            "rev_achievement": rev_ach,
            "rev_last":        ly_rev_won,
            "adr_budget":      bud_adr,
            "adr_actual":      act_adr,
            "adr_last":        ly_adr,
            "adr_vs_budget":   round((act_adr / bud_adr - 1) * 100, 1) if bud_adr > 0 else 0.0,
        }
    return result


def build_month_snapshot(db_bp, budgets, month_idx, db_seg=None, seg_budgets=None, db_bps=None,
                         adj_by_prop=None, adj_by_segment=None, holiday_factors=None, lead_time_by_prop=None, now_kst=None,
                         rm_fcst_props=None):
    """특정 월(0=전체, 1~12=해당월)에 대한 byProperty + summary + segmentData 반환
    실적/목표 모두 OTA+G-OTA+Inbound 세그먼트만 합산.
    """
    if now_kst is None:
        now_kst = datetime.now(KST)
    if rm_fcst_props is None:
        rm_fcst_props = {}

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
    tot_ai_fcst_rn = 0
    tot_rm_fcst_rn = 0
    tot_rm_budget_rn = 0

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

        # 전년 합산 (OTA+G-OTA+Inbound 3개 세그먼트만)
        lst_rn = 0
        lst_rev = 0.0
        if db_bps is not None:
            for mk in last_keys:
                d = sum_db_segments(db_bps, db_props, mk)
                lst_rn  += d["rn"]
                lst_rev += d["rev_m"]
        else:
            for mk in last_keys:
                d = sum_db(db_bp, db_props, mk)
                lst_rn  += d["rn"]
                lst_rev += d["rev_m"]
        # 미래월 LY 동기간 보정 — adj_by_prop의 booking_rn 사용 (orig+adj = 기준일 시점 RN)
        # 매출은 adj_by_prop에 저장되지 않으므로, RN 비율로 비례 환산
        is_future_for_ly = False
        if month_idx == 0:
            # 전체 월: 미래월만 부분 보정 (월별 분리 처리 위해 month_idx=0일 때는 일괄 보정 X — 월별 누계 합산 권장)
            pass
        elif month_idx > now_kst.month:
            is_future_for_ly = True
        if is_future_for_ly and adj_by_prop and lst_rn > 0:
            adj_lst_rn = 0
            for pname in db_props:
                for mk in last_keys:
                    pm = adj_by_prop.get(pname, {}).get(mk, {})
                    # booking_rn = orig + adj (기준일 시점 동기간 RN). 없으면 final 사용.
                    if pm.get("booking_rn") is not None:
                        adj_lst_rn += pm.get("booking_rn", 0)
                    else:
                        adj_lst_rn += 0
            if adj_lst_rn > 0:
                # 매출은 동기간 RN 비율로 환산 (ADR 동일 가정)
                lst_rev = lst_rev * (adj_lst_rn / lst_rn)
                lst_rn = adj_lst_rn
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
        # AI FCST: 항상 독립 계산 (비교 표시용)
        ai_fcst_rn, ai_fcst_ach = 0, 0.0
        # RM FCST: rm_fcst.json에서 로드
        rm_rn_prop = None
        rm_budget_prop = None
        rm_ach_prop = None

        if month_idx > 0:
            rns_fcst, rev_fcst, adr_fcst, fcst_ach, rev_fcst_ach = _calc_fcst(
                act_rn, act_rev, month_idx, now_kst, bud_rn, bud_rev
            )

            # AI FCST 독립 계산
            ai_fcst_rn, ai_fcst_ach = _calc_fcst_enhanced(
                act_rn, month_idx, now_kst, bud_rn, db_bp, db_props,
                holiday_factors, db_bps=db_bps
            )

            # RM FCST 로드 (전체 세그먼트 기준)
            rm_key = f"2026-{month_idx:02d}"
            rm_entry = rm_fcst_props.get(display_name, {}).get(rm_key, {})
            rm_rn_prop = rm_entry.get("rm_fcst_rn")
            rm_budget_prop = rm_entry.get("rm_budget_rn")
            rm_ach_prop = round(rm_rn_prop / rm_budget_prop * 100, 1) if (rm_rn_prop and rm_budget_prop and rm_budget_prop > 0) else None

            # 미래월 fallback: _calc_fcst가 None이면 AI FCST 사용
            if rns_fcst is None:
                if ai_fcst_rn is not None and ai_fcst_rn > 0:
                    rns_fcst = ai_fcst_rn
                    fcst_ach = ai_fcst_ach
                    # 매출 FCST도 전년 기반 추정
                    mk_25 = f"2025{month_idx:02d}"
                    lst_rev_m = sum_db(db_bp, db_props, mk_25)["rev_m"]
                    if lst_rev_m > 0 and lst_rn > 0:
                        rev_fcst = lst_rev_m * (rns_fcst / lst_rn)
                    else:
                        rev_fcst = 0.0
                    adr_fcst = round(rev_fcst * 1_000_000 / rns_fcst) if rns_fcst > 0 else 0
                    rev_fcst_ach = round(rev_fcst / bud_rev * 100, 1) if bud_rev > 0 else 0.0
        else:
            # 전체 월: 개별 월(1~12월)별 FCST를 합산
            rns_fcst = 0
            rev_fcst = 0.0
            ai_fcst_rn = 0
            rm_rn_prop_sum = 0
            rm_budget_prop_sum = 0
            rm_rn_prop_has = False
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

                # AI FCST (월별 합산)
                mi_ai, _ = _calc_fcst_enhanced(
                    mi_rn, mi, now_kst, mi_bud_rn, db_bp, db_props,
                    holiday_factors, db_bps=db_bps
                )
                if mi_ai is not None and mi_ai > 0:
                    ai_fcst_rn += mi_ai

                # RM FCST (월별 합산)
                mi_rm_key = f"2026-{mi:02d}"
                mi_rm_entry = rm_fcst_props.get(display_name, {}).get(mi_rm_key, {})
                mi_rm_rn = mi_rm_entry.get("rm_fcst_rn")
                mi_rm_bud = mi_rm_entry.get("rm_budget_rn")
                if mi_rm_rn is not None:
                    rm_rn_prop_sum += mi_rm_rn
                    rm_rn_prop_has = True
                if mi_rm_bud is not None:
                    rm_budget_prop_sum += mi_rm_bud

                mi_rns_f, mi_rev_f, _, _, _ = _calc_fcst(mi_rn, mi_rev, mi, now_kst, mi_bud_rn, mi_bud_rev)
                if mi_rns_f is not None:
                    rns_fcst += mi_rns_f
                    rev_fcst += mi_rev_f
                else:
                    # 미래월 fallback: AI FCST 사용
                    if mi_ai is not None and mi_ai > 0:
                        rns_fcst += mi_ai
                        mk_25_mi = f"2025{mi:02d}"
                        mi_lst = sum_db(db_bp, db_props, mk_25_mi)
                        if mi_lst["rev_m"] > 0 and mi_lst["rn"] > 0:
                            rev_fcst += mi_lst["rev_m"] * (mi_ai / mi_lst["rn"])
                        else:
                            rev_fcst += mi_bud_rev
            adr_fcst = round(rev_fcst * 1_000_000 / rns_fcst) if rns_fcst > 0 else 0
            fcst_ach = round(rns_fcst / bud_rn * 100, 1) if bud_rn > 0 else 0.0
            rev_fcst_ach = round(rev_fcst / bud_rev * 100, 1) if bud_rev > 0 else 0.0
            ai_fcst_ach = round(ai_fcst_rn / bud_rn * 100, 1) if bud_rn > 0 else 0.0
            rm_rn_prop = rm_rn_prop_sum if rm_rn_prop_has else None
            rm_budget_prop = rm_budget_prop_sum if rm_rn_prop_has else None
            rm_ach_prop = round(rm_rn_prop / rm_budget_prop * 100, 1) if (rm_rn_prop and rm_budget_prop and rm_budget_prop > 0) else None

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
            "ai_fcst_rn":      ai_fcst_rn,
            "ai_fcst_ach":     ai_fcst_ach,
            "rm_fcst_rn":      rm_rn_prop,
            "rm_budget_rn":    rm_budget_prop,
            "rm_fcst_ach":     rm_ach_prop,
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
        tot_ai_fcst_rn += ai_fcst_rn if ai_fcst_rn is not None else 0
        if rm_rn_prop is not None:
            tot_rm_fcst_rn += rm_rn_prop
        if rm_budget_prop is not None:
            tot_rm_budget_rn += rm_budget_prop

    # 미래월 판별: month_idx가 단일 월이고 현재월보다 크면 FCST=None
    is_future_month = (month_idx > 0 and month_idx > now_kst.month)

    tot_rns_ach  = round(tot_act_rn  / tot_bud_rn  * 100, 1) if tot_bud_rn  > 0 else 0.0
    tot_rev_ach  = round(tot_act_rev / tot_bud_rev  * 100, 1) if tot_bud_rev  > 0 else 0.0
    tot_adr_act  = round(tot_act_rev * 1_000_000 / tot_act_rn)  if tot_act_rn  > 0 else 0
    tot_adr_bud  = round(tot_bud_rev * 1_000_000 / tot_bud_rn)  if tot_bud_rn  > 0 else 0
    tot_adr_lst  = round(tot_lst_rev * 1_000_000 / tot_lst_rn)  if tot_lst_rn  > 0 else 0
    if is_future_month:
        # 미래월이라도 enhanced FCST 합산값이 있으면 사용
        if tot_rns_fcst and tot_rns_fcst > 0:
            tot_adr_fcst = round(tot_rev_fcst * 1_000_000 / tot_rns_fcst) if tot_rns_fcst > 0 else 0
            tot_fcst_ach     = round(tot_rns_fcst / tot_bud_rn  * 100, 1) if tot_bud_rn  > 0 else 0.0
            tot_rev_fcst_ach = round(tot_rev_fcst / tot_bud_rev * 100, 1) if tot_bud_rev > 0 else 0.0
        else:
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

    tot_ai_fcst_ach = round(tot_ai_fcst_rn / tot_bud_rn * 100, 1) if tot_bud_rn > 0 else 0.0
    tot_rm_fcst_ach = round(tot_rm_fcst_rn / tot_rm_budget_rn * 100, 1) if (tot_rm_budget_rn > 0 and tot_rm_fcst_rn > 0) else None

    summary = {
        "rns_budget":      tot_bud_rn,
        "rns_actual":      tot_act_rn,
        "rns_achievement": tot_rns_ach,
        "rns_last":        tot_lst_rn,
        "rns_yoy":         tot_yoy,
        "rns_fcst":        tot_rns_fcst,
        "fcst_achievement": tot_fcst_ach,
        "ai_fcst_rn":      tot_ai_fcst_rn,
        "ai_fcst_ach":     tot_ai_fcst_ach,
        "rm_fcst_rn":      tot_rm_fcst_rn if tot_rm_fcst_rn > 0 else None,
        "rm_budget_rn":    tot_rm_budget_rn if tot_rm_budget_rn > 0 else None,
        "rm_fcst_ach":     tot_rm_fcst_ach,
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
        seg_data = build_segment_snapshot(db_seg, seg_budgets, month_idx, adj_by_segment=adj_by_segment)

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
    """OTA+G-OTA+Inbound 3개 세그먼트만 합산한 전일 데이터 반환.
    빌드 시점(now_kst)의 당일 데이터는 미완성이므로 전일(days_ago=1)부터 탐색."""
    nd_seg = db.get("net_daily_by_segment", {})
    for days_ago in range(1, 8):
        date_str = (now_kst - timedelta(days=days_ago)).strftime("%Y%m%d")
        pickup, cancel = 0, 0
        for seg in BUDGET_SEGMENT_KEYS:
            entry = nd_seg.get(seg, {}).get(date_str, {})
            pickup += entry.get("pickup_rn", 0)
            cancel += entry.get("cancel_rn", 0)
        if pickup > 0 or cancel > 0:
            return pickup, cancel, pickup - cancel, date_str
    # fallback: 전체 net_daily에서 최근 날짜 (세그먼트 필터 적용)
    net_daily = db.get("net_daily", {})
    if net_daily:
        most_recent = max(net_daily.keys())
        pickup, cancel = 0, 0
        for seg in BUDGET_SEGMENT_KEYS:
            entry = nd_seg.get(seg, {}).get(most_recent, {})
            pickup += entry.get("pickup_rn", 0)
            cancel += entry.get("cancel_rn", 0)
        return pickup, cancel, pickup - cancel, most_recent
    return 0, 0, 0, None


def get_today_booking_by_props(db, date_str, db_props):
    """pickup_daily_by_property_segment에서 OTA+G-OTA+Inbound만 합산."""
    if not date_str:
        return 0, 0
    pdbps = db.get("pickup_daily_by_property_segment", {})
    rn, rev = 0, 0.0
    for pname in db_props:
        for seg in BUDGET_SEGMENT_KEYS:
            d = pdbps.get(pname, {}).get(seg, {}).get(date_str, {})
            rn  += d.get("rn",  0)
            rev += d.get("rev", 0.0)
    return rn, rev


def get_today_cancel_by_props(db, date_str, db_props):
    """cancel_daily_by_property_segment에서 OTA+G-OTA+Inbound만 합산."""
    if not date_str:
        return 0, 0
    cdbps = db.get("cancel_daily_by_property_segment", {})
    rn, rev = 0, 0.0
    for pname in db_props:
        for seg in BUDGET_SEGMENT_KEYS:
            d = cdbps.get(pname, {}).get(seg, {}).get(date_str, {})
            rn  += d.get("rn",  0)
            rev += d.get("rev", 0.0)
    return rn, rev


def get_today_booking_by_props_month(db, date_str, db_props, stay_month):
    """pickup_daily_by_property_segment_month에서 OTA+G-OTA+Inbound만 합산."""
    if not date_str:
        return 0, 0
    pdbpsm = db.get("pickup_daily_by_property_segment_month", {})
    rn, rev = 0, 0.0
    for pname in db_props:
        for seg in BUDGET_SEGMENT_KEYS:
            d = pdbpsm.get(pname, {}).get(seg, {}).get(stay_month, {}).get(date_str, {})
            rn  += d.get("rn",  0)
            rev += d.get("rev", 0.0)
    return rn, rev


def get_today_cancel_by_props_month(db, date_str, db_props, stay_month):
    """cancel_daily_by_property_segment_month에서 OTA+G-OTA+Inbound만 합산."""
    if not date_str:
        return 0, 0
    cdbpsm = db.get("cancel_daily_by_property_segment_month", {})
    rn, rev = 0, 0.0
    for pname in db_props:
        for seg in BUDGET_SEGMENT_KEYS:
            d = cdbpsm.get(pname, {}).get(seg, {}).get(stay_month, {}).get(date_str, {})
            rn  += d.get("rn",  0)
            rev += d.get("rev", 0.0)
    return rn, rev


def get_today_summary_by_month(db, now_kst, stay_month):
    """OTA+G-OTA+Inbound 3개 세그먼트만 합산, 특정 투숙월의 전일 데이터 반환."""
    pdsm = db.get("pickup_daily_by_segment_month", {})
    cdsm = db.get("cancel_daily_by_segment_month", {})
    for days_ago in range(1, 8):
        date_str = (now_kst - timedelta(days=days_ago)).strftime("%Y%m%d")
        pickup, cancel = 0, 0
        for seg in BUDGET_SEGMENT_KEYS:
            pickup += pdsm.get(seg, {}).get(stay_month, {}).get(date_str, {}).get("rn", 0)
            cancel += cdsm.get(seg, {}).get(stay_month, {}).get(date_str, {}).get("rn", 0)
        if pickup > 0 or cancel > 0:
            return pickup, cancel, pickup - cancel
    # fallback
    net_daily_m = db.get("net_daily_by_month", {}).get(stay_month, {})
    if net_daily_m:
        most_recent = max(net_daily_m.keys())
        pickup, cancel = 0, 0
        for seg in BUDGET_SEGMENT_KEYS:
            pickup += pdsm.get(seg, {}).get(stay_month, {}).get(most_recent, {}).get("rn", 0)
            cancel += cdsm.get(seg, {}).get(stay_month, {}).get(most_recent, {}).get("rn", 0)
        return pickup, cancel, pickup - cancel
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
    adj_by_segment = {}
    yoy_base_date = ""
    yoy_adj_section = db.get("yoy_adjusted", {})
    if "2025" in yoy_adj_section:
        adj_by_prop    = yoy_adj_section["2025"].get("by_property", {})
        adj_by_segment = yoy_adj_section["2025"].get("by_segment", {})
        yoy_base_date  = yoy_adj_section["2025"].get("base_date_full", "")
    print(f"  YoY 동기간 보정 로드: 사업장 수={len(adj_by_prop)}, 세그먼트 수={len(adj_by_segment)}, base={yoy_base_date}")

    # 사업장별 리드타임 분포
    lead_time_by_prop = db.get("lead_time_by_property", {})

    # RM FCST 로드
    print("RM FCST 로드 중...")
    rm_fcst_props = load_rm_fcst()
    print(f"  RM FCST 사업장 수: {len(rm_fcst_props)}")

    # 공휴일 보정 계수 (전월 대상)
    print("  공휴일 보정 계수 계산 중 (로컬 holidays_kr.json)...")
    holiday_factors = build_holiday_factors(target_months=tuple(range(1, 13)), cur_year=2026, base_year=2025)
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
            adj_by_prop=adj_by_prop, adj_by_segment=adj_by_segment,
            holiday_factors=holiday_factors,
            lead_time_by_prop=lead_time_by_prop, now_kst=now_kst,
            rm_fcst_props=rm_fcst_props,
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

        for seg_name, seg_props in snap.get("byPropertySegment", {}).items():
            for prop in seg_props:
                db_props = next((d for _, n, _, d in PROPERTY_DEFS if n == prop["name"]), [])
                # 해당 세그먼트만의 today 데이터 (3세그 합산 X → 개별 세그먼트)
                pdbps = db.get("pickup_daily_by_property_segment", {})
                cdbps = db.get("cancel_daily_by_property_segment", {})
                if m_str == "0":
                    prop_booking, prop_cancel = 0, 0
                    if today_date:
                        for pname in db_props:
                            prop_booking += pdbps.get(pname, {}).get(seg_name, {}).get(today_date, {}).get("rn", 0) or 0
                            prop_cancel  += cdbps.get(pname, {}).get(seg_name, {}).get(today_date, {}).get("rn", 0) or 0
                else:
                    stay_month = f"2026{int(m_str):02d}"
                    pdbpsm = db.get("pickup_daily_by_property_segment_month", {})
                    cdbpsm = db.get("cancel_daily_by_property_segment_month", {})
                    prop_booking, prop_cancel = 0, 0
                    if today_date:
                        for pname in db_props:
                            prop_booking += pdbpsm.get(pname, {}).get(seg_name, {}).get(stay_month, {}).get(today_date, {}).get("rn", 0) or 0
                            prop_cancel  += cdbpsm.get(pname, {}).get(seg_name, {}).get(stay_month, {}).get(today_date, {}).get("rn", 0) or 0
                prop["today_booking"] = prop_booking
                prop["today_cancel"]  = prop_cancel
                prop["today_net"]     = prop_booking - prop_cancel

        # ── segmentData에 today 주입 ──
        for seg, seg_summary in snap.get("segmentData", {}).items():
            if today_date:
                if m_str == "0":
                    # 전체: net_daily_by_segment (전월 합산)
                    nds = db.get("net_daily_by_segment", {})
                    pds = db.get("pickup_daily_by_segment", {})
                    cds = db.get("cancel_daily_by_segment", {})
                    nd_entry = nds.get(seg, {}).get(today_date, {})
                    seg_summary["today_booking"] = nd_entry.get("pickup_rn", 0)
                    seg_summary["today_cancel"]  = nd_entry.get("cancel_rn", 0)
                    seg_summary["today_net"]     = nd_entry.get("net_rn", 0)
                    pd_entry = pds.get(seg, {}).get(today_date, {})
                    cd_entry = cds.get(seg, {}).get(today_date, {})
                    seg_summary["today_booking_rev"] = round(pd_entry.get("rev", 0.0) * 1_000_000)
                    seg_summary["today_cancel_rev"]  = round(cd_entry.get("rev", 0.0) * 1_000_000)
                else:
                    # 월별: pickup/cancel_daily_by_segment_month 사용
                    stay_month = f"2026{int(m_str):02d}"
                    pdsm = db.get("pickup_daily_by_segment_month", {})
                    cdsm = db.get("cancel_daily_by_segment_month", {})
                    pd_entry = pdsm.get(seg, {}).get(stay_month, {}).get(today_date, {})
                    cd_entry = cdsm.get(seg, {}).get(stay_month, {}).get(today_date, {})
                    seg_summary["today_booking"] = pd_entry.get("rn", 0) or 0
                    seg_summary["today_cancel"]  = cd_entry.get("rn", 0) or 0
                    seg_summary["today_net"]     = seg_summary["today_booking"] - seg_summary["today_cancel"]
                    seg_summary["today_booking_rev"] = round((pd_entry.get("rev", 0.0) or 0) * 1_000_000)
                    seg_summary["today_cancel_rev"]  = round((cd_entry.get("rev", 0.0) or 0) * 1_000_000)
                seg_summary["today_net_rev"]     = seg_summary["today_booking_rev"] - seg_summary["today_cancel_rev"]

    # Chart data (동기간 보정 반영)
    monthly_chart = build_monthly_chart(
        db_bp, budgets, seg_budgets=seg_budgets, db_bps=db_bps, adj_by_prop=adj_by_prop
    )

    # YoY 사업장별 추이 테이블 (4·5·6월)
    yoy_table = build_yoy_table(
        db_bp, budgets, seg_budgets, db_bps, adj_by_prop, holiday_factors,
        months=(4, 5, 6), now_kst=now_kst, rm_fcst_props=rm_fcst_props,
    )

    output = {
        "meta": {
            "refreshTime":  now_kst.strftime("%Y-%m-%d %H:%M KST"),
            "baseDate":     now_kst.strftime("%Y-%m-%d"),
            "todayDate":    today_date,
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
        # 투숙일별 일별 데이터 (stay-date.html 용)
        "stayDateDaily": db.get("stay_date_daily", {}),
    }

    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✓ {OUTPUT_JSON} 생성 완료")
    print(f"  사업장 수: {len(output['byProperty'])}")
    print(f"  총 목표 RNS: {output['summary']['rns_budget']:,}")
    print(f"  총 실적 RNS: {output['summary']['rns_actual']:,}")
    print(f"  달성률: {output['summary']['rns_achievement']}%")
    ai_rn = output['summary'].get('ai_fcst_rn')
    rm_rn = output['summary'].get('rm_fcst_rn')
    print(f"  AI FCST RN: {ai_rn:,}" if ai_rn else "  AI FCST RN: N/A")
    print(f"  RM FCST RN: {rm_rn:,}" if rm_rn else "  RM FCST RN: N/A")


if __name__ == "__main__":
    main()
