#!/usr/bin/env python3
"""
build_weekly_comparison.py — 당일/금주 vs 전주/전년동기간 비교 데이터 생성

데이터 소스: data/db_aggregated.json
  - pickup_daily_by_property / pickup_daily_by_segment (예약파일 기준 들어온 예약)
  - cancel_daily_by_property / cancel_daily_by_segment (취소파일 기준 빠진 예약)
  → net = pickup - cancel  (당일 들어온 순예약)

출력: docs/data/weekly_comparison.json
- today: 당일(가장 최근 데이터일) 단일 일자 결과
- yesterday: 전일
- this_week / prev_week / ly_week: 7일 윈도우(또는 partial WTD) 합계
- by_property / by_segment: 각 차원별 3개 윈도우 비교
- insights: 자동 추출 인사이트 3~5개
"""
from __future__ import annotations
import json, sys, logging
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
DOCS_DATA_DIR = PROJECT_DIR / "docs" / "data"

# Cowork worktree에서 실행 시 부모 저장소에 fallback
if not (DATA_DIR / "db_aggregated.json").exists():
    parent_root = PROJECT_DIR
    while parent_root.parent != parent_root:
        parent_root = parent_root.parent
        if (parent_root / "data" / "db_aggregated.json").exists():
            DATA_DIR = parent_root / "data"
            logger.info(f"  worktree 모드: 부모 데이터 사용 → {DATA_DIR}")
            break

INPUT_PATH = DATA_DIR / "db_aggregated.json"
OUTPUT_PATH = DOCS_DATA_DIR / "weekly_comparison.json"


# ─── 일자 유틸 ───
def to_date(ymd: str) -> datetime:
    return datetime.strptime(ymd, "%Y%m%d")


def to_ymd(d: datetime) -> str:
    return d.strftime("%Y%m%d")


def daterange(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def week_monday(d: datetime) -> datetime:
    """월요일 시작 ISO 주차"""
    return d - timedelta(days=d.weekday())


# ─── 합산 ───
def sum_window(daily_map: dict, dates: list[str]) -> dict:
    rn = 0
    rev = 0.0
    for ymd in dates:
        if ymd in daily_map:
            v = daily_map[ymd]
            rn += v.get("rn", 0)
            rev += v.get("rev", 0)
    return {"rn": rn, "rev": round(rev, 2)}


def sum_window_keyed(keyed_daily_map: dict, dates: list[str]) -> dict:
    """{key: {ymd: {rn, rev}}} → {key: {rn, rev}} 윈도우 합"""
    out = {}
    for key, days in keyed_daily_map.items():
        rn, rev = 0, 0.0
        for ymd in dates:
            if ymd in days:
                v = days[ymd]
                rn += v.get("rn", 0)
                rev += v.get("rev", 0)
        if rn != 0 or rev != 0:
            out[key] = {"rn": rn, "rev": round(rev, 2)}
    return out


def calc_net(pickup: dict, cancel: dict) -> dict:
    """pickup - cancel = net. cancel은 빈 dict일 수 있음"""
    return {
        "pickup_rn": pickup.get("rn", 0),
        "pickup_rev": pickup.get("rev", 0.0),
        "cancel_rn": cancel.get("rn", 0),
        "cancel_rev": cancel.get("rev", 0.0),
        "net_rn": pickup.get("rn", 0) - cancel.get("rn", 0),
        "net_rev": round(pickup.get("rev", 0.0) - cancel.get("rev", 0.0), 2),
    }


def pct_change(curr: float, prev: float) -> float | None:
    """((curr-prev)/prev)*100. prev=0이면 None"""
    if prev == 0:
        return None
    return round((curr - prev) / prev * 100, 1)


# ─── 메인 ───
def build():
    if not INPUT_PATH.exists():
        logger.error(f"입력 없음: {INPUT_PATH}")
        sys.exit(1)

    logger.info(f"읽기: {INPUT_PATH}")
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        agg = json.load(f)

    # 필수 키 확인
    needed = [
        "pickup_daily", "cancel_daily",
        "pickup_daily_by_property", "cancel_daily_by_property",
        "pickup_daily_by_segment", "cancel_daily_by_segment",
    ]
    for k in needed:
        if k not in agg:
            logger.error(f"db_aggregated.json에 키 없음: {k}")
            sys.exit(1)

    pickup_total = agg["pickup_daily"]
    cancel_total = agg["cancel_daily"]
    pickup_prop = agg["pickup_daily_by_property"]
    cancel_prop = agg["cancel_daily_by_property"]
    pickup_seg = agg["pickup_daily_by_segment"]
    cancel_seg = agg["cancel_daily_by_segment"]

    # 가장 최근 데이터 일자. 단, 추출 시점이 그날 새벽이면 partial이므로
    # latest day의 pickup이 직전 7일 평균의 30% 미만이면 latest-1을 "마지막 완전일"로 본다.
    all_dates = sorted(pickup_total.keys())
    if not all_dates:
        logger.error("pickup_daily가 비어있음")
        sys.exit(1)
    raw_latest_ymd = all_dates[-1]
    raw_latest_date = to_date(raw_latest_ymd)
    raw_latest_pu = pickup_total.get(raw_latest_ymd, {}).get("rn", 0)

    # 직전 7일(latest 제외) 평균
    prev7 = [pickup_total[to_ymd(raw_latest_date - timedelta(days=i))].get("rn", 0)
             for i in range(1, 8) if to_ymd(raw_latest_date - timedelta(days=i)) in pickup_total]
    avg_prev7 = sum(prev7) / len(prev7) if prev7 else 0
    is_partial = avg_prev7 > 0 and raw_latest_pu < 0.3 * avg_prev7

    if is_partial:
        latest_ymd = to_ymd(raw_latest_date - timedelta(days=1))
        logger.info(f"  최신 데이터일: {raw_latest_ymd} (partial, RN={raw_latest_pu} vs 7일 평균 {avg_prev7:.0f})")
        logger.info(f"  → 마지막 완전일: {latest_ymd} 사용")
    else:
        latest_ymd = raw_latest_ymd
        logger.info(f"  최신 데이터일(완전): {latest_ymd}")
    latest_date = to_date(latest_ymd)
    partial_today_meta = {
        "is_partial": is_partial,
        "raw_latest_date": raw_latest_ymd,
        "raw_latest_pickup_rn": raw_latest_pu,
    }

    # 윈도우 정의
    # this_week = 월요일 ~ latest_date (Week-To-Date)
    this_mon = week_monday(latest_date)
    this_dates = [to_ymd(d) for d in daterange(this_mon, latest_date)]
    days_in_week = len(this_dates)

    # prev_week = 7일 전 같은 요일 범위 (동일 일수)
    prev_mon = this_mon - timedelta(days=7)
    prev_end = latest_date - timedelta(days=7)
    prev_dates = [to_ymd(d) for d in daterange(prev_mon, prev_end)]

    # ly_week = 364일 전 (동요일 보존)
    ly_mon = this_mon - timedelta(days=364)
    ly_end = latest_date - timedelta(days=364)
    ly_dates = [to_ymd(d) for d in daterange(ly_mon, ly_end)]

    logger.info(f"  this_week: {this_dates[0]} ~ {this_dates[-1]} ({days_in_week}일)")
    logger.info(f"  prev_week: {prev_dates[0]} ~ {prev_dates[-1]}")
    logger.info(f"  ly_week:   {ly_dates[0]} ~ {ly_dates[-1]}")

    # ── 당일/전일 ──
    today_pu = pickup_total.get(latest_ymd, {"rn": 0, "rev": 0.0})
    today_cn = cancel_total.get(latest_ymd, {"rn": 0, "rev": 0.0})
    yest_ymd = to_ymd(latest_date - timedelta(days=1))
    yest_pu = pickup_total.get(yest_ymd, {"rn": 0, "rev": 0.0})
    yest_cn = cancel_total.get(yest_ymd, {"rn": 0, "rev": 0.0})

    # ── 전주 동요일/전년 동요일 (당일 비교용) ──
    prev_day_ymd = to_ymd(latest_date - timedelta(days=7))
    ly_day_ymd = to_ymd(latest_date - timedelta(days=364))
    prev_day_pu = pickup_total.get(prev_day_ymd, {"rn": 0, "rev": 0.0})
    prev_day_cn = cancel_total.get(prev_day_ymd, {"rn": 0, "rev": 0.0})
    ly_day_pu = pickup_total.get(ly_day_ymd, {"rn": 0, "rev": 0.0})
    ly_day_cn = cancel_total.get(ly_day_ymd, {"rn": 0, "rev": 0.0})

    today_block = {
        "date": latest_ymd,
        "this": calc_net(today_pu, today_cn),
        "prev_week_same_day": {
            "date": prev_day_ymd,
            **calc_net(prev_day_pu, prev_day_cn),
        },
        "ly_same_day": {
            "date": ly_day_ymd,
            **calc_net(ly_day_pu, ly_day_cn),
        },
    }
    today_block["wow_pct"] = pct_change(today_block["this"]["net_rn"],
                                        today_block["prev_week_same_day"]["net_rn"])
    today_block["yoy_pct"] = pct_change(today_block["this"]["net_rn"],
                                        today_block["ly_same_day"]["net_rn"])

    yesterday_block = {
        "date": yest_ymd,
        "this": calc_net(yest_pu, yest_cn),
    }

    # ── 주간 합계(전체) ──
    this_pu = sum_window(pickup_total, this_dates)
    this_cn = sum_window(cancel_total, this_dates)
    prev_pu = sum_window(pickup_total, prev_dates)
    prev_cn = sum_window(cancel_total, prev_dates)
    ly_pu = sum_window(pickup_total, ly_dates)
    ly_cn = sum_window(cancel_total, ly_dates)

    week_totals = {
        "this_week": {
            "start": this_dates[0], "end": this_dates[-1], "days": days_in_week,
            **calc_net(this_pu, this_cn),
        },
        "prev_week": {
            "start": prev_dates[0], "end": prev_dates[-1], "days": days_in_week,
            **calc_net(prev_pu, prev_cn),
        },
        "ly_week": {
            "start": ly_dates[0], "end": ly_dates[-1], "days": days_in_week,
            **calc_net(ly_pu, ly_cn),
        },
    }
    week_totals["wow_rn_pct"] = pct_change(week_totals["this_week"]["net_rn"],
                                           week_totals["prev_week"]["net_rn"])
    week_totals["yoy_rn_pct"] = pct_change(week_totals["this_week"]["net_rn"],
                                           week_totals["ly_week"]["net_rn"])
    week_totals["wow_rev_pct"] = pct_change(week_totals["this_week"]["net_rev"],
                                            week_totals["prev_week"]["net_rev"])
    week_totals["yoy_rev_pct"] = pct_change(week_totals["this_week"]["net_rev"],
                                            week_totals["ly_week"]["net_rev"])

    # ── 사업장별 ──
    this_p_pu = sum_window_keyed(pickup_prop, this_dates)
    this_p_cn = sum_window_keyed(cancel_prop, this_dates)
    prev_p_pu = sum_window_keyed(pickup_prop, prev_dates)
    prev_p_cn = sum_window_keyed(cancel_prop, prev_dates)
    ly_p_pu = sum_window_keyed(pickup_prop, ly_dates)
    ly_p_cn = sum_window_keyed(cancel_prop, ly_dates)

    all_props = sorted(set(this_p_pu.keys()) | set(prev_p_pu.keys()) | set(ly_p_pu.keys())
                       | set(this_p_cn.keys()) | set(prev_p_cn.keys()) | set(ly_p_cn.keys()))
    by_property = []
    for prop in all_props:
        if prop == "미분류":
            continue
        this_n = calc_net(this_p_pu.get(prop, {}), this_p_cn.get(prop, {}))
        prev_n = calc_net(prev_p_pu.get(prop, {}), prev_p_cn.get(prop, {}))
        ly_n = calc_net(ly_p_pu.get(prop, {}), ly_p_cn.get(prop, {}))
        by_property.append({
            "property": prop,
            "this_net_rn": this_n["net_rn"],
            "this_net_rev": this_n["net_rev"],
            "prev_net_rn": prev_n["net_rn"],
            "prev_net_rev": prev_n["net_rev"],
            "ly_net_rn": ly_n["net_rn"],
            "ly_net_rev": ly_n["net_rev"],
            "wow_rn_pct": pct_change(this_n["net_rn"], prev_n["net_rn"]),
            "yoy_rn_pct": pct_change(this_n["net_rn"], ly_n["net_rn"]),
            "wow_rev_pct": pct_change(this_n["net_rev"], prev_n["net_rev"]),
            "yoy_rev_pct": pct_change(this_n["net_rev"], ly_n["net_rev"]),
        })
    # 금주 net_rn 내림차순
    by_property.sort(key=lambda x: x["this_net_rn"], reverse=True)

    # ── 세그먼트별 ──
    this_s_pu = sum_window_keyed(pickup_seg, this_dates)
    this_s_cn = sum_window_keyed(cancel_seg, this_dates)
    prev_s_pu = sum_window_keyed(pickup_seg, prev_dates)
    prev_s_cn = sum_window_keyed(cancel_seg, prev_dates)
    ly_s_pu = sum_window_keyed(pickup_seg, ly_dates)
    ly_s_cn = sum_window_keyed(cancel_seg, ly_dates)

    all_segs = sorted(set(this_s_pu.keys()) | set(prev_s_pu.keys()) | set(ly_s_pu.keys())
                      | set(this_s_cn.keys()) | set(prev_s_cn.keys()) | set(ly_s_cn.keys()))
    by_segment = []
    for seg in all_segs:
        if not seg:
            continue
        this_n = calc_net(this_s_pu.get(seg, {}), this_s_cn.get(seg, {}))
        prev_n = calc_net(prev_s_pu.get(seg, {}), prev_s_cn.get(seg, {}))
        ly_n = calc_net(ly_s_pu.get(seg, {}), ly_s_cn.get(seg, {}))
        by_segment.append({
            "segment": seg,
            "this_net_rn": this_n["net_rn"],
            "this_net_rev": this_n["net_rev"],
            "prev_net_rn": prev_n["net_rn"],
            "prev_net_rev": prev_n["net_rev"],
            "ly_net_rn": ly_n["net_rn"],
            "ly_net_rev": ly_n["net_rev"],
            "wow_rn_pct": pct_change(this_n["net_rn"], prev_n["net_rn"]),
            "yoy_rn_pct": pct_change(this_n["net_rn"], ly_n["net_rn"]),
            "wow_rev_pct": pct_change(this_n["net_rev"], prev_n["net_rev"]),
            "yoy_rev_pct": pct_change(this_n["net_rev"], ly_n["net_rev"]),
        })
    by_segment.sort(key=lambda x: x["this_net_rn"], reverse=True)

    # ── 인사이트 자동 생성 ──
    insights = []

    # 1) 전체 금주 vs 전주
    if week_totals["wow_rn_pct"] is not None:
        wow = week_totals["wow_rn_pct"]
        delta = week_totals["this_week"]["net_rn"] - week_totals["prev_week"]["net_rn"]
        sign = "▲" if wow > 0 else "▼"
        tone = "positive" if wow > 0 else "negative"
        insights.append({
            "tone": tone,
            "title": f"금주 온북 {sign} {abs(wow)}% (전주比)",
            "detail": f"금주 {week_totals['this_week']['net_rn']:,}실 vs 전주 {week_totals['prev_week']['net_rn']:,}실 (Δ{delta:+,}실)"
        })

    # 2) 전체 금주 vs LY
    if week_totals["yoy_rn_pct"] is not None:
        yoy = week_totals["yoy_rn_pct"]
        delta = week_totals["this_week"]["net_rn"] - week_totals["ly_week"]["net_rn"]
        sign = "▲" if yoy > 0 else "▼"
        tone = "positive" if yoy > 0 else "negative"
        insights.append({
            "tone": tone,
            "title": f"금주 온북 {sign} {abs(yoy)}% (전년 동기간比)",
            "detail": f"금주 {week_totals['this_week']['net_rn']:,}실 vs 전년 {week_totals['ly_week']['net_rn']:,}실 (Δ{delta:+,}실)"
        })

    # 3) 사업장 — 최대 상승 (WoW, |this_net_rn|≥30)
    rising = [p for p in by_property
              if p["wow_rn_pct"] is not None and abs(p["this_net_rn"]) >= 30 and p["wow_rn_pct"] > 0]
    rising.sort(key=lambda x: x["wow_rn_pct"], reverse=True)
    if rising:
        top = rising[0]
        insights.append({
            "tone": "positive",
            "title": f"급등 사업장: {top['property']} ▲ {top['wow_rn_pct']}% (전주比)",
            "detail": f"금주 {top['this_net_rn']:,}실 vs 전주 {top['prev_net_rn']:,}실 (Δ{top['this_net_rn']-top['prev_net_rn']:+,}실)"
        })

    # 4) 사업장 — 최대 하락 (WoW)
    falling = [p for p in by_property
               if p["wow_rn_pct"] is not None and abs(p["prev_net_rn"]) >= 30 and p["wow_rn_pct"] < 0]
    falling.sort(key=lambda x: x["wow_rn_pct"])
    if falling:
        bot = falling[0]
        insights.append({
            "tone": "negative",
            "title": f"급락 사업장: {bot['property']} ▼ {abs(bot['wow_rn_pct'])}% (전주比)",
            "detail": f"금주 {bot['this_net_rn']:,}실 vs 전주 {bot['prev_net_rn']:,}실 (Δ{bot['this_net_rn']-bot['prev_net_rn']:+,}실)"
        })

    # 5) 세그먼트 — OTA/G-OTA/Inbound 중 가장 큰 변동
    focus_segs = [s for s in by_segment if s["segment"] in ("OTA", "G-OTA", "Inbound")
                  and s["wow_rn_pct"] is not None]
    if focus_segs:
        focus_segs.sort(key=lambda x: abs(x["wow_rn_pct"]), reverse=True)
        s = focus_segs[0]
        sign = "▲" if s["wow_rn_pct"] > 0 else "▼"
        tone = "positive" if s["wow_rn_pct"] > 0 else "negative"
        insights.append({
            "tone": tone,
            "title": f"{s['segment']} 세그먼트 {sign} {abs(s['wow_rn_pct'])}% (전주比)",
            "detail": f"금주 {s['this_net_rn']:,}실 vs 전주 {s['prev_net_rn']:,}실"
        })

    # 6) 당일 vs 전주 동요일
    if today_block["wow_pct"] is not None:
        wow = today_block["wow_pct"]
        sign = "▲" if wow > 0 else "▼"
        tone = "positive" if wow > 0 else "negative"
        insights.append({
            "tone": tone,
            "title": f"당일({latest_ymd[4:6]}/{latest_ymd[6:8]}) 픽업 {sign} {abs(wow)}% (전주 동요일比)",
            "detail": f"당일 {today_block['this']['net_rn']:,}실 vs 전주 {today_block['prev_week_same_day']['net_rn']:,}실"
        })

    # 결과 저장
    result = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_generated_at": agg.get("generated_at", ""),
        "latest_data_date": latest_ymd,
        "partial_today": partial_today_meta,
        "today": today_block,
        "yesterday": yesterday_block,
        "week_totals": week_totals,
        "by_property": by_property,
        "by_segment": by_segment,
        "insights": insights[:6],
    }

    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info(f"✓ 저장: {OUTPUT_PATH} ({OUTPUT_PATH.stat().st_size:,} bytes)")
    logger.info(f"  사업장 {len(by_property)}개 / 세그먼트 {len(by_segment)}개 / 인사이트 {len(result['insights'])}개")


if __name__ == "__main__":
    build()
