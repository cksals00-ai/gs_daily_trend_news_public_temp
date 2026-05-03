#!/usr/bin/env python3
"""
generate_daily_insights.py
==========================
db_aggregated.json + otb_data.json + campaign_data.json 실데이터를 기반으로
영업기획 관리자 페이지(admin.html / strategy-keyin.html)에서 후보로 노출할
데일리 인사이트 3~6개를 자동 생성한다.

생성되는 인사이트 유형:
  a) yoy_pickup       전년 동기간 대비 픽업 추이 (수치화)
  b) trend_change     예약 추이 가속/감속/정체 판단
  c) surge            어제 급등 사업장 (평소 대비 2배 이상) + 매칭 기획전
  d) risk             위험 사업장 조기 경고
  e) campaign         기획전 효과 분석

출력: docs/data/daily_insights.json

각 인사이트:
  {
    "id":          고유 ID,
    "type":        yoy_pickup|trend_change|surge|risk|campaign,
    "severity":    info|positive|warning|negative,
    "source":      DB|OTB|CAMPAIGN,
    "title":       짧은 제목 (UI 카드 헤더용),
    "body":        영업기획이 그대로 활용 가능한 한 줄 (HTML <strong> 허용),
    "text":        body 의 plain 동의어 (admin_input.json daily_insights 호환),
    "data_points": 핵심 수치 dict (PR/리뷰용)
  }

실행:
  python3 scripts/generate_daily_insights.py
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DOCS_DATA = ROOT / "docs" / "data"
KST = timezone(timedelta(hours=9))

DB_PATH = DOCS_DATA / "db_aggregated.json"
OTB_PATH = DOCS_DATA / "otb_data.json"
CAMPAIGN_PATH = DOCS_DATA / "campaign_data.json"
OUT_PATH = DOCS_DATA / "daily_insights.json"


def _short_name(prop_name: str) -> str:
    """'07.델피노' → '델피노', '소노벨 비발디파크' → '소노벨 비발디파크'."""
    if "." in prop_name and prop_name.split(".")[0].isdigit():
        return prop_name.split(".", 1)[1].strip()
    return prop_name.strip()


# OTB byProperty short name → db_aggregated by_property full name
# OTB 는 '01.벨비발디' 형식 short, DB 는 '소노벨 비발디파크' 형식 full.
# 부분 일치/키워드 매칭이 어려운 약식 prefix 가 많아 명시 매핑.
OTB_TO_DB_NAME = {
    "벨비발디": "소노벨 비발디파크",
    "캄비발디": "소노캄 비발디파크",
    "펫비발디": "소노펫 비발디파크",
    "펠리체비발디": "소노펠리체 비발디파크",
    "빌리지비발디": "소노펠리체 빌리지 비발디파크",
    "양평": "소노휴 양평",
    "델피노": "델피노",
    "쏠비치양양": "쏠비치 양양",
    "쏠비치삼척": "쏠비치 삼척",
    "소노벨단양": "소노문 단양",  # OTB '소노벨단양' 이 DB '소노문 단양' 과 동일 사업장
    "소노캄경주": "소노벨 경주",
    "소노벨청송": "소노벨 청송",
    "소노벨천안": "소노벨 천안",
    "소노벨변산": "소노벨 변산",
    "소노캄여수": "소노캄 여수",
    "소노캄거제": "소노캄 거제",
    "쏠비치진도": "쏠비치 진도",
    "소노벨제주": "소노벨 제주",
    "소노캄제주": "소노캄 제주",
    "소노캄고양": "소노캄 고양",
    "소노문해운대": "소노문 해운대",
    "쏠비치남해": "쏠비치 남해",
    "르네블루": "르네블루",
}


def _db_name_for(otb_name: str) -> str | None:
    """OTB byProperty 의 name → db_aggregated by_property 의 key. 없으면 None."""
    return OTB_TO_DB_NAME.get(_short_name(otb_name))


def _campaign_keyword_match(prop_short: str, campaign_label: str,
                             match_wildcard: bool = True) -> bool:
    """사업장 short name 과 기획전 사업장 컬럼 간 부분 일치 매칭.

    match_wildcard=True 면 '전체' 캠페인을 모든 사업장에 매칭. False 면 약식 키워드만.
    """
    if not prop_short or not campaign_label:
        return False
    if campaign_label == "전체":
        return match_wildcard
    # campaign 사업장 컬럼은 '경주', '양양', '비발디', '르네블루' 등 약식
    return campaign_label in prop_short or prop_short in campaign_label


def _kst_today() -> datetime:
    return datetime.now(KST)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"필수 파일 없음: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


# ════════════════════════════════════════════════════════════════════
#  a) YOY PICKUP TREND — 전년 동기간 대비 픽업 추이
# ════════════════════════════════════════════════════════════════════
def insight_yoy_pickup(net_daily: dict, base_date: datetime) -> dict | None:
    """최근 14일 net_rn 합계 vs 전년 동기 14일 net_rn 합계."""
    days = 14
    cur_keys = [(base_date - timedelta(days=i)).strftime("%Y%m%d") for i in range(days)]
    prev_keys = [(base_date - timedelta(days=i + 364)).strftime("%Y%m%d") for i in range(days)]

    cur_sum = sum(net_daily.get(k, {}).get("net_rn", 0) for k in cur_keys)
    prev_sum = sum(net_daily.get(k, {}).get("net_rn", 0) for k in prev_keys)
    cur_pickup = sum(net_daily.get(k, {}).get("pickup_rn", 0) for k in cur_keys)

    if prev_sum <= 0 or cur_sum == 0:
        return None

    delta = cur_sum - prev_sum
    pct = delta / prev_sum * 100
    arrow = "▲" if delta >= 0 else "▼"
    trend = "회복세" if delta >= 0 else "둔화"
    severity = "positive" if pct >= 5 else ("warning" if pct < -5 else "info")

    body = (
        f"최근 14일 순증 <strong>{cur_sum:,}실</strong> · "
        f"전년 동기 {prev_sum:,}실 대비 {arrow} <strong>{abs(pct):.1f}%</strong> ({delta:+,}실) — {trend}."
    )
    return {
        "id": "auto-yoy-pickup-14d",
        "type": "yoy_pickup",
        "severity": severity,
        "source": "DB",
        "title": "전년 동기간 픽업 추이 (14일)",
        "body": body,
        "text": body,
        "data_points": {
            "window_days": days,
            "current_net_rn": cur_sum,
            "prev_year_net_rn": prev_sum,
            "current_pickup_rn": cur_pickup,
            "delta_rn": delta,
            "pct_change": round(pct, 1),
            "current_period": [cur_keys[-1], cur_keys[0]],
            "prev_period": [prev_keys[-1], prev_keys[0]],
        },
    }


# ════════════════════════════════════════════════════════════════════
#  b) TREND CHANGE — 예약 추이 가속/감속/정체
# ════════════════════════════════════════════════════════════════════
def insight_trend_change(net_daily: dict, base_date: datetime) -> dict | None:
    """최근 7일 일평균 net vs 직전 7일 일평균 net."""
    recent_keys = [(base_date - timedelta(days=i)).strftime("%Y%m%d") for i in range(7)]
    prior_keys = [(base_date - timedelta(days=i + 7)).strftime("%Y%m%d") for i in range(7)]

    recent_vals = [net_daily.get(k, {}).get("net_rn", 0) for k in recent_keys]
    prior_vals = [net_daily.get(k, {}).get("net_rn", 0) for k in prior_keys]
    recent_avg = sum(recent_vals) / len(recent_vals) if recent_vals else 0
    prior_avg = sum(prior_vals) / len(prior_vals) if prior_vals else 0

    if prior_avg <= 0:
        return None

    ratio = recent_avg / prior_avg
    pct = (ratio - 1) * 100

    if ratio >= 1.15:
        verdict = "가속"
        severity = "positive"
    elif ratio <= 0.85:
        verdict = "감속"
        severity = "warning"
    else:
        verdict = "정체"
        severity = "info"

    sign = "+" if pct >= 0 else ""
    body = (
        f"예약 추이 <strong>{verdict}</strong> — "
        f"최근 7일 일평균 <strong>{recent_avg:,.0f}실</strong>, "
        f"직전 7일 {prior_avg:,.0f}실 대비 {sign}{pct:.1f}%."
    )
    return {
        "id": "auto-trend-change-7d",
        "type": "trend_change",
        "severity": severity,
        "source": "DB",
        "title": "예약 추이 (7일 일평균)",
        "body": body,
        "text": body,
        "data_points": {
            "recent_avg": round(recent_avg, 1),
            "prior_avg": round(prior_avg, 1),
            "ratio": round(ratio, 3),
            "pct_change": round(pct, 1),
            "verdict": verdict,
            "recent_window": [recent_keys[-1], recent_keys[0]],
            "prior_window": [prior_keys[-1], prior_keys[0]],
        },
    }


# ════════════════════════════════════════════════════════════════════
#  c) SURGE — 어제 급등 사업장 (평소 대비 2배 이상) + 매칭 기획전
# ════════════════════════════════════════════════════════════════════
def insight_surge(by_property: list, summer_detail: list,
                  db_by_property: dict, base_date: datetime) -> dict | None:
    """어제 today_net 이 직전 30일(= 직전월 net_rn / 30) 대비 2배 이상인 사업장 탐지.

    baseline = 직전월 net_rn / 30 (사업장의 평소 일평균 booking 페이스).
    db_aggregated.by_property[name][prev_month_key].net_rn 사용.
    """
    if not by_property:
        return None

    prev_month = base_date.month - 1 if base_date.month > 1 else 12
    prev_year = base_date.year if base_date.month > 1 else base_date.year - 1
    prev_key = f"{prev_year}{prev_month:02d}"

    candidates = []  # (prop, ratio, baseline_daily) for ratio >= 2.0
    all_with_baseline = []  # (prop, ratio, baseline_daily) — for fallback ranking
    for p in by_property:
        today_net = p.get("today_net") or 0
        if today_net <= 0:
            continue
        db_name = _db_name_for(p["name"])
        if not db_name:
            continue
        prop_monthly = db_by_property.get(db_name, {})
        prev_net = prop_monthly.get(prev_key, {}).get("net_rn")
        if not prev_net or prev_net <= 0:
            continue
        baseline_daily = prev_net / 30.0
        if baseline_daily < 5:  # 너무 작은 모수는 노이즈
            continue
        ratio = today_net / baseline_daily
        all_with_baseline.append((p, ratio, baseline_daily))
        if ratio >= 2.0:
            candidates.append((p, ratio, baseline_daily))

    surges = candidates
    fallback_mode = not surges
    if fallback_mode:
        # 2배 이상 급등이 없으면 평소 대비 ratio 가 가장 높은 사업장 1개를 INFO 로 노출
        if not all_with_baseline:
            return None
        all_with_baseline.sort(key=lambda x: x[1], reverse=True)
        surges = all_with_baseline[:1]

    surges.sort(key=lambda x: x[1], reverse=True)
    top, top_ratio, top_baseline = surges[0]
    top_short = _short_name(top["name"])
    today_net = top["today_net"]

    # 이번달/다음달 active 기획전 매칭
    next_month = base_date.month + 1 if base_date.month < 12 else 1
    cur_prefix = f"{base_date.year}-{base_date.month:02d}"
    next_prefix = f"{base_date.year if next_month > 1 else base_date.year + 1}-{next_month:02d}"
    matched = []
    for c in summer_detail:
        sale_start = c.get("판매시작") or ""
        if not (sale_start.startswith(cur_prefix) or sale_start.startswith(next_prefix)):
            continue
        if _campaign_keyword_match(top_short, c.get("사업장", "")):
            channel = c.get("채널", "").strip()
            product = (c.get("상품", "") or "").strip()
            if product:
                # 상품 ":" 등 노이즈 제거 + 길이 제한
                product = product.replace("\n", " ")[:24]
                matched.append(f"{channel} — {product}")
            else:
                matched.append(channel)

    if fallback_mode:
        severity = "info"
        title = "어제 픽업 상대 TOP 사업장"
        body = (
            f"어제 평소 대비 2배 이상 급등 사업장 없음 · "
            f"상대 TOP <strong>{top_short}</strong> 순증 <strong>{today_net:,}실</strong> "
            f"(직전월 일평균 {top_baseline:.0f}실 대비 {top_ratio:.2f}배)"
        )
        if matched:
            body += f" — 매칭 기획전 {len(matched)}건 ({matched[0]})."
        else:
            body += "."
    else:
        severity = "positive"
        title = "어제 급등 사업장"
        body = (
            f"<strong>{top_short}</strong> 어제 순증 <strong>{today_net:,}실</strong> · "
            f"직전월 일평균 {top_baseline:.0f}실 대비 <strong>{top_ratio:.1f}배</strong> 급등"
        )
        if matched:
            body += f" — 매칭 기획전 {len(matched)}건 ({matched[0]})."
        else:
            body += " — 매칭 기획전 없음, 자연 수요/이벤트 점검 필요."

        if len(surges) > 1:
            others = ", ".join(_short_name(s[0]["name"]) for s in surges[1:3])
            body += f" 동시 급등: {others}."

    return {
        "id": "auto-surge-yesterday",
        "type": "surge",
        "severity": severity,
        "source": "OTB",
        "title": title,
        "body": body,
        "text": body,
        "data_points": {
            "fallback_mode": fallback_mode,
            "threshold_ratio": 2.0,
            "top_property": top_short,
            "today_net": today_net,
            "baseline_daily_avg": round(top_baseline, 1),
            "baseline_source": f"db_aggregated.by_property[{prev_key}].net_rn / 30",
            "ratio": round(top_ratio, 2),
            "matched_campaigns": matched,
            "other_surges": [
                {"name": _short_name(s[0]["name"]),
                 "today_net": s[0]["today_net"],
                 "ratio": round(s[1], 2)}
                for s in surges[1:5]
            ],
        },
    }


# ════════════════════════════════════════════════════════════════════
#  d) RISK — 위험 사업장 조기 경고
# ════════════════════════════════════════════════════════════════════
def insight_risk(by_property: list) -> dict | None:
    """달성률 낮음 + FCST 달성률 낮음 + 어제 net 약함 → 위험."""
    risk_props = []
    for p in by_property:
        ach = p.get("rns_achievement")
        fcst_ach = p.get("fcst_achievement")
        rns_fcst = p.get("rns_fcst") or 0
        today_net = p.get("today_net") or 0
        if ach is None or fcst_ach is None:
            continue
        # FCST 자체가 0 인 사업장(신규/미반영)은 위험 비교 대상 제외 — 데이터 노이즈
        if rns_fcst <= 0:
            continue
        # 위험 조건: 달성률 35% 미만 AND FCST 달성률 90% 미만
        if ach < 35 and fcst_ach < 90:
            # risk score: lower is worse — combine ach + fcst
            score = ach + fcst_ach
            risk_props.append((p, score, today_net))

    if not risk_props:
        return None

    # 가장 위험한 것부터 정렬 (score 오름차순)
    risk_props.sort(key=lambda x: x[1])
    top_risks = risk_props[:3]
    top, top_score, top_today = top_risks[0]
    top_short = _short_name(top["name"])
    others_label = ", ".join(_short_name(r[0]["name"]) for r in top_risks[1:])

    body_parts = [
        f"<strong>{top_short}</strong> 달성률 <strong>{top['rns_achievement']:.1f}%</strong> · "
        f"FCST 달성 <strong>{top['fcst_achievement']:.1f}%</strong>"
    ]
    if top_today < 0:
        body_parts.append(f"어제 순증 {top_today:+,}실 (취소 우세)")
    elif top_today == 0:
        body_parts.append("어제 신규 픽업 없음")
    else:
        body_parts.append(f"어제 순증 {top_today:,}실")
    body = " · ".join(body_parts) + " — 즉시 가격/프로모션 점검 권장"
    if others_label:
        body += f". 동반 위험: {others_label}."
    else:
        body += "."

    return {
        "id": "auto-risk-early-warning",
        "type": "risk",
        "severity": "negative",
        "source": "OTB",
        "title": "위험 사업장 조기 경고",
        "body": body,
        "text": body,
        "data_points": {
            "top_risk": top_short,
            "rns_achievement": top["rns_achievement"],
            "fcst_achievement": top["fcst_achievement"],
            "today_net": top_today,
            "others": [
                {"name": _short_name(r[0]["name"]),
                 "rns_achievement": r[0]["rns_achievement"],
                 "fcst_achievement": r[0]["fcst_achievement"]}
                for r in top_risks[1:]
            ],
        },
    }


# ════════════════════════════════════════════════════════════════════
#  e) CAMPAIGN — 기획전 효과 분석
# ════════════════════════════════════════════════════════════════════
def insight_campaign(by_property: list, summer_detail: list,
                     base_date: datetime) -> dict | None:
    """이번달 active 기획전 보유 여부 vs 사업장 픽업 페이스."""
    if not summer_detail:
        return None

    cur_year = base_date.year
    cur_month = base_date.month
    next_month = cur_month + 1 if cur_month < 12 else 1

    # 이번달/다음달 진행되는 기획전이 있는 사업장 set
    # 사업장별 매칭은 '전체' 와일드카드 제외(개별 캠페인 효과만 비교).
    specific_campaign_props: set[str] = set()
    campaign_count = 0
    wildcard_campaigns = 0
    for c in summer_detail:
        s = (c.get("판매시작") or "").strip()
        if not s:
            continue
        try:
            sd = datetime.strptime(s, "%Y-%m-%d")
        except ValueError:
            continue
        if sd.year == cur_year and sd.month in (cur_month, next_month):
            campaign_count += 1
            label = c.get("사업장", "").strip()
            if label == "전체":
                wildcard_campaigns += 1
            elif label:
                specific_campaign_props.add(label)

    if campaign_count == 0:
        return None

    # 사업장별 covered/uncovered + 어제 픽업 비교 (개별 캠페인 기준)
    covered = []
    uncovered = []
    for p in by_property:
        short = _short_name(p["name"])
        today_net = p.get("today_net") or 0
        ach = p.get("rns_achievement") or 0
        is_covered = any(
            _campaign_keyword_match(short, c, match_wildcard=False)
            for c in specific_campaign_props
        )
        item = {"name": short, "today_net": today_net, "rns_achievement": ach}
        (covered if is_covered else uncovered).append(item)

    if not covered:
        return None

    avg_cov = sum(p["today_net"] for p in covered) / len(covered)
    avg_unc = sum(p["today_net"] for p in uncovered) / len(uncovered) if uncovered else 0

    # 효과 판정
    if avg_unc > 0:
        lift = (avg_cov - avg_unc) / avg_unc * 100
    else:
        lift = None

    if lift is not None and lift >= 15:
        verdict, severity = "유의미한 lift 관측", "positive"
    elif lift is not None and lift <= -15:
        verdict, severity = "기획전 효과 미흡 — 노출/소재 점검 필요", "warning"
    else:
        verdict, severity = "효과 중립 — 추가 데이터 누적 필요", "info"

    wildcard_note = f" (전체대상 {wildcard_campaigns}건 포함)" if wildcard_campaigns else ""
    body = (
        f"이번달/익월 기획전 <strong>{campaign_count}건</strong>{wildcard_note} 진행 중 · "
        f"개별타겟 사업장 {len(covered)}곳 어제 평균 순증 <strong>{avg_cov:.0f}실</strong>"
    )
    if uncovered:
        sign = "+" if avg_cov >= avg_unc else "-"
        body += (
            f" vs 미커버 {len(uncovered)}곳 {avg_unc:.0f}실 "
            f"({sign}{abs(avg_cov - avg_unc):.0f}실 차이)"
        )
        if lift is not None:
            body += f" · {verdict}."
    else:
        body += f" — {verdict}."

    return {
        "id": "auto-campaign-effect",
        "type": "campaign",
        "severity": severity,
        "source": "CAMPAIGN",
        "title": "기획전 효과 분석",
        "body": body,
        "text": body,
        "data_points": {
            "active_campaign_count": campaign_count,
            "wildcard_campaigns": wildcard_campaigns,
            "specific_campaign_props": sorted(specific_campaign_props),
            "covered_props": [p["name"] for p in covered],
            "uncovered_props": [p["name"] for p in uncovered],
            "avg_today_net_covered": round(avg_cov, 1),
            "avg_today_net_uncovered": round(avg_unc, 1),
            "lift_pct": round(lift, 1) if lift is not None else None,
            "verdict": verdict,
        },
    }


# ════════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════════
def main() -> None:
    logger.info("=" * 60)
    logger.info("데일리 인사이트 생성 시작")
    logger.info("=" * 60)

    db = _load_json(DB_PATH)
    otb = _load_json(OTB_PATH)
    campaign = _load_json(CAMPAIGN_PATH)

    net_daily: dict = db.get("net_daily", {})
    db_by_property: dict = db.get("by_property", {})
    by_property: list = otb.get("byProperty", [])
    summer_detail: list = campaign.get("summer_detail", [])

    # base_date = otb 의 baseDate 우선 (실데이터 일관성), 없으면 today
    base_date_str = otb.get("meta", {}).get("baseDate")
    if base_date_str:
        try:
            base_date = datetime.strptime(base_date_str, "%Y-%m-%d").replace(tzinfo=KST)
        except ValueError:
            base_date = _kst_today()
    else:
        base_date = _kst_today()

    logger.info(f"기준일: {base_date.strftime('%Y-%m-%d')}")

    candidates: list[dict] = []
    builders = (
        ("yoy_pickup",   lambda: insight_yoy_pickup(net_daily, base_date)),
        ("trend_change", lambda: insight_trend_change(net_daily, base_date)),
        ("surge",        lambda: insight_surge(by_property, summer_detail, db_by_property, base_date)),
        ("risk",         lambda: insight_risk(by_property)),
        ("campaign",     lambda: insight_campaign(by_property, summer_detail, base_date)),
    )

    for label, fn in builders:
        try:
            ins = fn()
            if ins:
                candidates.append(ins)
                logger.info(f"  ✓ {label}: {ins['body'][:80]}")
            else:
                logger.info(f"  · {label}: 데이터 부족으로 스킵")
        except Exception as e:
            logger.warning(f"  ✗ {label} 생성 실패: {e}")

    if not (3 <= len(candidates) <= 6):
        logger.warning(f"인사이트 개수 {len(candidates)} — 3~6 권장 범위 밖")

    output = {
        "generated_at": _kst_today().isoformat(),
        "generator": "scripts/generate_daily_insights.py",
        "base_date": base_date.strftime("%Y-%m-%d"),
        "insight_count": len(candidates),
        "insights": candidates,
    }
    OUT_PATH.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("=" * 60)
    logger.info(f"✓ {OUT_PATH.relative_to(ROOT)} 저장 ({len(candidates)}개 인사이트)")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
