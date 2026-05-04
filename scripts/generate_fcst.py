#!/usr/bin/env python3
"""
generate_fcst.py
AI FCST 전용 산출 — generate_otb_data.py가 만든 사업장×월 FCST를 받아
다음을 추가로 생성:

1) 사업장×월 FCST + Confidence Interval (otb_data.json에서 추출)
2) 4개년(2022~2025) stay_date_daily 기반 월별 요일·공휴일 가중치
3) 담당자 키인(admin_input.json:fcst_keyin) 적용 → final_fcst
4) FCST 정확도 트래킹 (history snapshot vs 실적 → MAPE / Bias)
5) data/ai_fcst_history.json 에 일별 스냅샷 누적 (롤링 90일)

출력: docs/data/ai_fcst.json
"""
from __future__ import annotations

import calendar
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
DOCS_DATA_DIR = PROJECT_DIR / "docs" / "data"

DB_JSON          = DATA_DIR / "db_aggregated.json"
HOLIDAYS_JSON    = DATA_DIR / "holidays_kr.json"
ADMIN_JSON       = DATA_DIR / "admin_input.json"
FCST_INPUT_JSON  = DATA_DIR / "fcst_input.json"   # fcst-admin.html 직접 export
OTB_JSON         = DOCS_DATA_DIR / "otb_data.json"
HISTORY_JSON     = DATA_DIR / "ai_fcst_history.json"
OUT_JSON         = DOCS_DATA_DIR / "ai_fcst.json"

KST = timezone(timedelta(hours=9))
MODEL_VERSION = "fcst-v1.0"
HISTORY_RETAIN_DAYS = 90


# ─────────────────────────────────────────────────────────────────────
# I/O helpers
# ─────────────────────────────────────────────────────────────────────
def load_json(path: Path, default=None):
    if not path.exists():
        return default if default is not None else {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"  ⚠ {path.name} 로드 실패: {e}", file=sys.stderr)
        return default if default is not None else {}


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────
# Holiday helpers (holidays_kr.json)
# ─────────────────────────────────────────────────────────────────────
def load_holiday_set() -> set[str]:
    """holidays_kr.json → {YYYYMMDD, ...}"""
    h = load_json(HOLIDAYS_JSON, {}).get("holidays", {})
    return set(h.keys())


def is_holiday_or_weekend(d: date, holiday_set: set[str]) -> bool:
    if d.weekday() >= 5:
        return True
    return d.strftime("%Y%m%d") in holiday_set


# ─────────────────────────────────────────────────────────────────────
# 4개년 stay_date_daily 기반 요일·공휴일 가중치
# ─────────────────────────────────────────────────────────────────────
def build_weekday_holiday_pattern(db: dict, holiday_set: set[str]) -> dict:
    """
    stay_date_daily[YYYYMM]['days'] = [1,2,...,30]
    stay_date_daily[YYYYMM]['segments'][seg]['net_rn'] = [n_day1, n_day2, ...]

    월별 요일(0=월~6=일)·공휴일 가중치 계산:
      - 4개년(2022~2025) 동월 stay date 합산 → 요일별 평균 RN
      - 평일 평균을 1.0으로 정규화 → 요일계수 (월~일)
      - 공휴일/연휴 평균을 평일 평균으로 나눠 holiday_uplift

    Returns:
      {month_int: {
          "weekday": [w_mon, w_tue, w_wed, w_thu, w_fri, w_sat, w_sun],
          "holiday_uplift": float,
          "samples": int
      }}
    """
    sd = db.get("stay_date_daily", {})
    pattern = {}

    for m in range(1, 13):
        # 요일별 RN 누적 (4개년 합산)
        wd_sum = [0.0] * 7
        wd_cnt = [0] * 7
        hol_sum = 0.0
        hol_cnt = 0
        weekday_only_sum = 0.0
        weekday_only_cnt = 0

        for yr in (2022, 2023, 2024, 2025):
            mk = f"{yr}{m:02d}"
            entry = sd.get(mk, {})
            days = entry.get("days", [])
            segs = entry.get("segments", {})
            if not days or not segs:
                continue

            # 모든 세그먼트 net_rn 일별 합산
            day_count = len(days)
            daily_total = [0.0] * day_count
            for seg_name, seg_data in segs.items():
                rns = seg_data.get("net_rn", [])
                for i in range(min(day_count, len(rns))):
                    v = rns[i] or 0
                    daily_total[i] += v

            # 일자별 요일·공휴일 분류
            for i, day_num in enumerate(days):
                try:
                    d = date(yr, m, int(day_num))
                except ValueError:
                    continue
                rn = daily_total[i]
                wd = d.weekday()
                wd_sum[wd] += rn
                wd_cnt[wd] += 1

                if is_holiday_or_weekend(d, holiday_set):
                    hol_sum += rn
                    hol_cnt += 1
                else:
                    weekday_only_sum += rn
                    weekday_only_cnt += 1

        # 요일계수: 평일 평균=1.0 정규화
        wd_avg = [(wd_sum[i] / wd_cnt[i]) if wd_cnt[i] > 0 else 0.0 for i in range(7)]
        weekday_avg = (weekday_only_sum / weekday_only_cnt) if weekday_only_cnt > 0 else 0.0
        if weekday_avg > 0:
            wd_factor = [round(v / weekday_avg, 3) for v in wd_avg]
        else:
            wd_factor = [1.0] * 7

        # 공휴일 uplift: 공휴일 평균 / 평일 평균
        hol_avg = (hol_sum / hol_cnt) if hol_cnt > 0 else 0.0
        if weekday_avg > 0 and hol_avg > 0:
            hol_uplift = round(hol_avg / weekday_avg, 3)
        else:
            hol_uplift = 1.0

        pattern[m] = {
            "weekday": wd_factor,
            "holiday_uplift": hol_uplift,
            "samples": sum(wd_cnt),
        }

    return pattern


def expected_month_factor(month_num: int, year: int, pattern: dict, holiday_set: set[str]) -> float:
    """
    해당 월의 (요일분포 + 공휴일분포) 기반 기대치 / 평일기준 일수 비율.
    1.0 = 평균적인 월. 1.05 = 평균보다 5% 높은 수요 분포.
    """
    p = pattern.get(month_num)
    if not p:
        return 1.0
    wd_f = p["weekday"]
    hol_uplift = p["holiday_uplift"]
    days_in_month = calendar.monthrange(year, month_num)[1]

    weighted_sum = 0.0
    for d in range(1, days_in_month + 1):
        try:
            dt = date(year, month_num, d)
        except ValueError:
            continue
        wd = dt.weekday()
        f = wd_f[wd] if wd < len(wd_f) else 1.0
        # 평일 공휴일은 holiday_uplift 적용 (주말은 wd 계수에 이미 반영)
        if wd < 5 and dt.strftime("%Y%m%d") in holiday_set:
            f = max(f, hol_uplift)
        weighted_sum += f

    avg_factor = weighted_sum / days_in_month if days_in_month > 0 else 1.0
    return round(avg_factor, 4)


# ─────────────────────────────────────────────────────────────────────
# otb_data.json에서 사업장×월 AI FCST 추출
# ─────────────────────────────────────────────────────────────────────
def extract_property_fcst(otb: dict) -> dict:
    """
    otb_data.json:yoyTable → {prop_name: {"YYYY-MM": {fcst, lo, hi, actual, budget, region}}}
    """
    out = defaultdict(dict)
    yoy = otb.get("yoyTable", [])
    for row in yoy:
        name = row.get("name", "")
        region = row.get("region", "")
        for m_str, md in (row.get("months") or {}).items():
            try:
                m = int(m_str)
            except ValueError:
                continue
            mk = f"2026-{m:02d}"
            ai_rn = md.get("rns_fcst_ai") or md.get("rns_fcst")
            if ai_rn is None:
                continue
            out[name][mk] = {
                "region": region,
                "ai_fcst_rn": ai_rn,
                "fcst_lo": md.get("fcst_lo", ai_rn),
                "fcst_hi": md.get("fcst_hi", ai_rn),
                "rns_actual": md.get("rns_actual", 0),
                "rns_budget": md.get("rns_budget", 0),
                "rns_last": md.get("rns_last", 0),
                "rm_fcst_rn": md.get("rm_fcst_rn"),
            }
    return dict(out)


# ─────────────────────────────────────────────────────────────────────
# admin_input.json 키인 적용
# ─────────────────────────────────────────────────────────────────────
def parse_admin_keyin(admin: dict) -> dict:
    """
    admin_input.json:fcst_keyin → {prop_name: {month_int: {value, segments, updated_at}}}

    schema:
      "fcst_keyin": {
        "<prop_name>|<month_str>": {
          "value": int,
          "_segments": {"OTA": int, ...},
          "updated_at": iso_string
        },
        ...
      }
    """
    out = defaultdict(dict)
    keyin = (admin or {}).get("fcst_keyin", {}) or {}
    for k, v in keyin.items():
        if not isinstance(v, dict):
            continue
        parts = k.split("|")
        if len(parts) != 2:
            continue
        prop, m_str = parts
        try:
            m = int(m_str)
        except ValueError:
            continue
        val = v.get("value")
        if val is None:
            continue
        out[prop][m] = {
            "value": int(val),
            "segments": v.get("_segments") or {},
            "updated_at": v.get("updated_at"),
            "updated_by": v.get("updated_by"),
        }
    return dict(out)


# ─────────────────────────────────────────────────────────────────────
# History snapshot + accuracy
# ─────────────────────────────────────────────────────────────────────
def append_history(prop_fcst: dict, today: str) -> dict:
    """
    오늘자 스냅샷을 ai_fcst_history.json에 append.
    같은 날짜가 이미 있으면 덮어쓰기. 90일 초과 항목 제거.
    """
    hist = load_json(HISTORY_JSON, {"_description": "AI FCST 일별 스냅샷 (롤링)", "snapshots": []})
    snapshots = hist.get("snapshots", [])

    # 오늘자 항목 제거 (덮어쓰기)
    snapshots = [s for s in snapshots if s.get("date") != today]

    # 압축된 snapshot: prop × month_key → {fcst, lo, hi}
    compact = {}
    for prop, months in prop_fcst.items():
        compact[prop] = {}
        for mk, md in months.items():
            compact[prop][mk] = {
                "fcst": md["ai_fcst_rn"],
                "lo": md["fcst_lo"],
                "hi": md["fcst_hi"],
            }

    snapshots.append({
        "date": today,
        "model": MODEL_VERSION,
        "forecasts": compact,
    })

    # 90일 초과 컷
    cutoff = (datetime.now(KST) - timedelta(days=HISTORY_RETAIN_DAYS)).strftime("%Y-%m-%d")
    snapshots = [s for s in snapshots if s.get("date", "") >= cutoff]
    snapshots.sort(key=lambda s: s.get("date", ""))

    hist["snapshots"] = snapshots
    hist["_updated_at"] = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    write_json(HISTORY_JSON, hist)
    return hist


def compute_accuracy(history: dict, prop_fcst: dict, today: date) -> dict:
    """
    완료된 월(현재월 이전)에 대해 MAPE 계산.
    각 월의 가장 오래된 snapshot의 FCST를 예측값, 현재 actual을 실측값으로 사용.

    Returns:
      {
        "by_property": {prop: {mape, bias, samples, last_evaluated_month, fcst_at_d_minus_30}},
        "overall": {mape, bias, samples}
      }
    """
    snapshots = history.get("snapshots", [])
    if not snapshots:
        return {"by_property": {}, "overall": {"mape": None, "bias": None, "samples": 0}}

    # 현재월 이전(완료된 월) 추출
    closed_mks = []
    for m in range(1, today.month):
        closed_mks.append(f"2026-{m:02d}")

    by_prop = {}
    all_errs = []  # signed pct error

    for prop, months in prop_fcst.items():
        prop_errs = []
        last_eval = None
        d30_fcst = None

        for mk in closed_mks:
            cur_md = months.get(mk)
            if not cur_md:
                continue
            actual = cur_md.get("rns_actual", 0)
            if actual <= 0:
                continue

            # 해당 월의 가장 오래된 스냅샷 (당월 시작 이전 또는 직후)
            mk_year, mk_month = mk.split("-")
            month_start = f"{mk_year}-{mk_month}-01"
            # 월 시작 ±0~30일 사이 가장 빠른 스냅샷
            candidates = [s for s in snapshots
                          if s.get("date", "") >= month_start
                          and prop in s.get("forecasts", {})
                          and mk in s["forecasts"][prop]]
            if not candidates:
                continue
            oldest = min(candidates, key=lambda s: s["date"])
            pred = oldest["forecasts"][prop][mk].get("fcst", 0)
            if pred <= 0:
                continue

            err_pct = (pred - actual) / actual * 100
            prop_errs.append(err_pct)
            all_errs.append(err_pct)
            last_eval = mk
            d30_fcst = pred

        if prop_errs:
            mape = sum(abs(e) for e in prop_errs) / len(prop_errs)
            bias = sum(prop_errs) / len(prop_errs)
            by_prop[prop] = {
                "mape": round(mape, 1),
                "bias": round(bias, 1),
                "samples": len(prop_errs),
                "last_evaluated_month": last_eval,
            }

    overall_mape = (sum(abs(e) for e in all_errs) / len(all_errs)) if all_errs else None
    overall_bias = (sum(all_errs) / len(all_errs)) if all_errs else None
    return {
        "by_property": by_prop,
        "overall": {
            "mape": round(overall_mape, 1) if overall_mape is not None else None,
            "bias": round(overall_bias, 1) if overall_bias is not None else None,
            "samples": len(all_errs),
        },
    }


# ─────────────────────────────────────────────────────────────────────
# Final FCST = max(키인, AI FCST) 가 아니라 키인 우선, 없으면 AI
# ─────────────────────────────────────────────────────────────────────
def merge_keyin(prop_fcst: dict, keyin: dict, today: date) -> dict:
    """각 사업장×월에 manager_keyin/final_fcst 필드 추가."""
    cur_month = today.month
    for prop, months in prop_fcst.items():
        for mk, md in months.items():
            try:
                m = int(mk.split("-")[1])
            except (IndexError, ValueError):
                continue
            ki = (keyin.get(prop) or {}).get(m)
            ki_val = ki["value"] if ki else None
            md["manager_keyin"] = ki_val
            md["manager_keyin_at"] = ki.get("updated_at") if ki else None
            md["manager_keyin_by"] = ki.get("updated_by") if ki else None
            md["manager_keyin_segments"] = ki.get("segments") if ki else None
            # 완료월은 actual 우선, 진행월/미래월은 키인>AI
            if m < cur_month:
                md["final_fcst"] = md.get("rns_actual", 0)
            elif ki_val is not None:
                md["final_fcst"] = ki_val
            else:
                md["final_fcst"] = md["ai_fcst_rn"]

            bud = md.get("rns_budget", 0) or 0
            md["final_ach"] = round(md["final_fcst"] / bud * 100, 1) if bud > 0 else None
            # CI 폭(상대)
            ai = md["ai_fcst_rn"] or 0
            if ai > 0:
                md["ci_width_pct"] = round((md["fcst_hi"] - md["fcst_lo"]) / ai * 100, 1)
            else:
                md["ci_width_pct"] = None
    return prop_fcst


# ─────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("generate_fcst.py — AI FCST 산출")
    print("=" * 60)

    if not OTB_JSON.exists():
        print(f"⚠ {OTB_JSON} 없음 — generate_otb_data.py 먼저 실행 필요", file=sys.stderr)
        sys.exit(1)
    if not DB_JSON.exists():
        print(f"⚠ {DB_JSON} 없음 — parse_raw_db.py 먼저 실행 필요", file=sys.stderr)
        sys.exit(1)

    print(f"  load: {OTB_JSON.name}, {DB_JSON.name}, {HOLIDAYS_JSON.name}")
    otb = load_json(OTB_JSON)
    db = load_json(DB_JSON)
    holiday_set = load_holiday_set()
    admin = load_json(ADMIN_JSON)

    now_kst = datetime.now(KST)
    today = now_kst.date()
    today_str = today.strftime("%Y-%m-%d")

    print(f"  base_date={today_str}, holidays_loaded={len(holiday_set)}")

    # ① 4개년 요일·공휴일 패턴
    print("→ 요일·공휴일 패턴 계산 (2022~2025)")
    pattern = build_weekday_holiday_pattern(db, holiday_set)
    samples_total = sum(p.get("samples", 0) for p in pattern.values())
    print(f"  pattern months={len(pattern)}, samples_total={samples_total:,}")
    for m in (4, 5, 6, 12):
        p = pattern.get(m, {})
        if p:
            wd = p.get("weekday", [])
            print(f"  [{m}월] weekday={wd}, holiday_uplift={p.get('holiday_uplift')}")

    # ② 사업장×월 AI FCST 추출 + month_factor
    print("→ otb_data.json에서 사업장×월 FCST 추출")
    prop_fcst = extract_property_fcst(otb)
    print(f"  properties={len(prop_fcst)}, total entries={sum(len(v) for v in prop_fcst.values())}")

    # 월별 expected_month_factor 계산
    month_factors = {}
    for m in range(1, 13):
        month_factors[m] = expected_month_factor(m, 2026, pattern, holiday_set)
    print(f"  month_factors(2026)={month_factors}")

    # ③ 키인 적용 (admin_input.json 우선, fcst_input.json 폴백)
    print("→ 키인 로드: admin_input.json (우선) + fcst_input.json (폴백)")
    keyin = parse_admin_keyin(admin)
    if not keyin:
        fi = load_json(FCST_INPUT_JSON)
        keyin = parse_admin_keyin(fi)
        if keyin:
            print(f"  └ admin_input.json 비어있음 → fcst_input.json 사용")
    keyin_count = sum(len(v) for v in keyin.values())
    print(f"  keyin entries={keyin_count} (사업장 수={len(keyin)})")
    prop_fcst = merge_keyin(prop_fcst, keyin, today)

    # ④ 스냅샷 누적
    print(f"→ 스냅샷 append: {today_str}")
    history = append_history(prop_fcst, today_str)
    print(f"  history snapshots={len(history.get('snapshots', []))}")

    # ⑤ 정확도 (MAPE/Bias)
    print("→ 정확도 계산 (완료월 vs 최초 스냅샷)")
    accuracy = compute_accuracy(history, prop_fcst, today)
    ov = accuracy["overall"]
    print(f"  overall MAPE={ov['mape']}, Bias={ov['bias']}, samples={ov['samples']}")

    # ⑥ 출력
    output = {
        "meta": {
            "generated_at": now_kst.strftime("%Y-%m-%d %H:%M KST"),
            "base_date": today_str,
            "model_version": MODEL_VERSION,
            "description": "AI FCST 통합 산출 (4개년 패턴 + CI + 정확도 + 키인 반영)",
            "history_count": len(history.get("snapshots", [])),
        },
        "weekday_holiday_pattern": {str(k): v for k, v in pattern.items()},
        "month_factors": {str(k): v for k, v in month_factors.items()},
        "properties": prop_fcst,
        "accuracy": accuracy,
    }
    write_json(OUT_JSON, output)
    print(f"✓ {OUT_JSON} 생성 완료")
    print("=" * 60)


if __name__ == "__main__":
    main()
