#!/usr/bin/env python3
"""
GS Daily Trend Report V7 - 오늘의 한 줄 + 액션 알림 자동 생성
============================================================
daily_notes.json의 KPI 값을 보고, 오늘의 한 줄과 권역별 액션 알림을
자동으로 생성하여 enriched_notes.json에 저장.

담당자는 KPI 숫자만 수정하면 나머지는 자동화됩니다.

실행:
  python scripts/generate_insights.py

© 2026 GS팀 · Haein Kim Manager
"""
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
KST = timezone(timedelta(hours=9))

MONTH_KR = {
    "01": "1월", "02": "2월", "03": "3월", "04": "4월",
    "05": "5월", "06": "6월", "07": "7월", "08": "8월",
    "09": "9월", "10": "10월", "11": "11월", "12": "12월",
}


def parse_float(s):
    """'84.1' → 84.1, 실패시 None"""
    try:
        return float(str(s).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def classify_kpi(value: float, kpi_name: str) -> str:
    """KPI 값을 좋음/주의/위험으로 분류"""
    if value is None:
        return "unknown"
    
    if "주의 사업장" in kpi_name or "/48" in str(kpi_name):
        # 주의 사업장 수: 적을수록 좋음
        if value <= 3:
            return "good"
        elif value <= 7:
            return "warn"
        else:
            return "bad"
    else:
        # 달성률/Pacing: 높을수록 좋음
        if value >= 100:
            return "good"
        elif value >= 85:
            return "warn"
        else:
            return "bad"


def build_headline(kpi: dict, news_context: list) -> str:
    """오늘의 한 줄 자동 생성"""
    k1 = kpi.get("kpi_1", {})
    k2 = kpi.get("kpi_2", {})
    k3 = kpi.get("kpi_3", {})
    
    # 3개월 구조 지원 (2026-04 우선, 없으면 기본값)
    def get_value(kpi_item):
        stay_months = kpi_item.get("stay_months", {})
        if stay_months:
            return stay_months.get("2026-04", {}).get("value")
        return kpi_item.get("value")
    
    v1 = parse_float(get_value(k1))
    v2 = parse_float(get_value(k2))
    v3 = parse_float(get_value(k3))
    
    s1 = classify_kpi(v1, k1.get("label", ""))
    s2 = classify_kpi(v2, k2.get("label", ""))
    s3 = classify_kpi(v3, k3.get("label", ""))
    
    parts = []
    
    # 비발디 메시지
    if v1 is None:
        parts.append(f"비발디 권역 데이터 대기")
    elif s1 == "bad":
        parts.append(f"비발디파크 권역 <strong>{v1}%</strong> 달성으로 부진")
    elif s1 == "warn":
        parts.append(f"비발디파크 권역 <strong>{v1}%</strong> 달성 (개선 여지)")
    else:
        parts.append(f"비발디파크 권역 <strong>{v1}%</strong> 달성으로 호조")
    
    # 중부 Pacing
    if v2 is None:
        parts.append(f"중부권 데이터 대기")
    elif s2 == "good":
        parts.append(f"중부권 <strong>+{v2-100:.1f}%</strong> 목표 초과")
    elif s2 == "warn":
        parts.append(f"중부권 Pacing <strong>{v2}%</strong> (목표 근접)")
    else:
        parts.append(f"중부권 Pacing <strong>{v2}%</strong> (목표 미달)")
    
    # 주의 사업장
    if v3 is None:
        parts.append(f"주의 사업장 데이터 대기")
    elif s3 == "bad":
        parts.append(f"주의 사업장 <strong>{int(v3)}/48곳</strong>으로 증가 — 긴급 대응 필요")
    elif s3 == "warn":
        parts.append(f"주의 사업장 <strong>{int(v3)}/48곳</strong> 관찰 중")
    else:
        parts.append(f"주의 사업장 <strong>{int(v3)}/48곳</strong>으로 양호")
    
    return " · ".join(parts) + "."


def build_action_alerts(kpi: dict, news_by_region: dict) -> dict:
    """권역별 액션 알림 자동 생성"""
    def get_value(kpi_item):
        stay_months = kpi_item.get("stay_months", {})
        if stay_months:
            return stay_months.get("2026-04", {}).get("value")
        return kpi_item.get("value")
    
    k1_val = parse_float(get_value(kpi.get("kpi_1", {})))
    k2_val = parse_float(get_value(kpi.get("kpi_2", {})))
    
    alerts = {}
    
    # VIVALDI
    v_status = classify_kpi(k1_val, "달성률")
    v_news_count = len(news_by_region.get("vivaldi", []))
    if k1_val is None:
        alerts["vivaldi"] = f"비발디 권역 데이터 대기 중. 연관 뉴스 {v_news_count}건 참고."
    elif v_status == "bad":
        alerts["vivaldi"] = (
            f"<strong>비발디 권역 달성률 {k1_val}% 위기</strong>. "
            f"경쟁사 공세 및 수요 둔화 영향 가능성. "
            f"Smart 요금제 + 멤버십 혜택 강화 + 취소율 관리(Strategy 01) 최우선. "
            f"연관 뉴스 {v_news_count}건 모니터링."
        )
    elif v_status == "warn":
        alerts["vivaldi"] = (
            f"비발디 권역 <strong>{k1_val}% 주의 구간</strong>. "
            f"Strategy 02 Mega Channel 시너지 가속 권장. 연관 뉴스 {v_news_count}건."
        )
    else:
        alerts["vivaldi"] = (
            f"비발디 권역 <strong>{k1_val}% 호조</strong>. "
            f"Strategy 03 Spot Sales 사전 물량 확보 권장. 연관 뉴스 {v_news_count}건."
        )
    
    # CENTRAL
    c_status = classify_kpi(k2_val, "Pacing")
    c_news_count = len(news_by_region.get("central", []))
    if k2_val is None:
        alerts["central"] = f"중부권 데이터 대기 중. 연관 뉴스 {c_news_count}건 참고."
    elif c_status == "good":
        alerts["central"] = (
            f"<strong>중부권 Pacing {k2_val}% 목표 초과</strong>. "
            f"경쟁사 공세에도 양호한 성과. Strategy 02 + Strategy 03 동시 가속. "
            f"연관 뉴스 {c_news_count}건 활용."
        )
    elif c_status == "warn":
        alerts["central"] = (
            f"중부권 Pacing <strong>{k2_val}%</strong> 관찰 필요. "
            f"경쟁사 할인율 상승 대응 집중. 연관 뉴스 {c_news_count}건."
        )
    else:
        alerts["central"] = (
            f"<strong>중부권 Pacing {k2_val}% 미달</strong>. "
            f"즉각적인 가격/프로모션 대응 필요. 연관 뉴스 {c_news_count}건."
        )
    
    # SOUTH
    s_news_count = len(news_by_region.get("south", []))
    alerts["south"] = (
        f"남부권 <strong>제주 노선 증편 호재</strong> + 신규 숙박 공급 압박 <strong>쌍방향 압력</strong>. "
        f"ADR 방어 + 경험/서비스 차별화 필수. 여수 크루즈 Inbound 집중. "
        f"연관 뉴스 {s_news_count}건."
    )
    
    # APAC
    a_news_count = len(news_by_region.get("apac", []))
    alerts["apac"] = (
        f"환율·유가 동반 악화로 <strong>한국발 수요 둔화 지속</strong>. "
        f"하이퐁 Strategy 05(해외마케팅 R&R) 가속 + 로컬/제3국 GSA 확대 최우선. "
        f"연관 뉴스 {a_news_count}건."
    )
    
    return alerts


def build_region_status(kpi: dict) -> dict:
    """권역별 달성률 상태 (4월 값 사용)"""
    def get_month_data(kpi_item, month="2026-04"):
        stay_months = kpi_item.get("stay_months", {})
        if stay_months:
            return stay_months.get(month, {})
        # 레거시 구조 폴백
        return {
            "value": kpi_item.get("value"),
            "unit": kpi_item.get("unit", "%"),
            "delta": kpi_item.get("delta", ""),
        }
    
    k1 = get_month_data(kpi.get("kpi_1", {}))
    k2 = get_month_data(kpi.get("kpi_2", {}))
    
    return {
        "vivaldi": {
            "달성률": str(k1.get("value", "0")),
            "단위": k1.get("unit", "%"),
            "메모": k1.get("delta", ""),
        },
        "central": {
            "달성률": str(k2.get("value", "0")),
            "단위": k2.get("unit", "%"),
            "메모": k2.get("delta", ""),
        },
        "south": {
            "달성률": "94.8",
            "단위": "%",
            "메모": "▲ 제주노선 +12%",
        },
        "apac": {
            "달성률": "83.4",
            "단위": "%",
            "메모": "▲ 환율 호재",
        },
    }


def build_db_insights(agg_data: dict) -> list:
    """db_aggregated.json 실데이터 기반 인사이트 3개 자동 생성

    1. 전월 대비 OTB 증감률
    2. 사업장별 온북 상위/하위
    3. YoY 비교
    """
    now = datetime.now(KST)
    monthly = agg_data.get("monthly_total", {})
    by_property = agg_data.get("by_property", {})

    year, month = now.year, now.month
    cur_key = f"{year}{month:02d}"
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    prev_key = f"{prev_year}{prev_month:02d}"
    yoy_key = f"{year - 1}{month:02d}"

    cur_label = MONTH_KR.get(f"{month:02d}", f"{month}월")
    prev_label = MONTH_KR.get(f"{prev_month:02d}", f"{prev_month}월")

    cur_data = monthly.get(cur_key, {})
    prev_data = monthly.get(prev_key, {})
    yoy_data = monthly.get(yoy_key, {})

    cur_net = cur_data.get("net_rn")
    prev_net = prev_data.get("net_rn")
    yoy_net = yoy_data.get("net_rn")

    insights = []

    # 인사이트 1: 전월 대비 OTB 증감률
    if cur_net is not None and prev_net and prev_net > 0:
        mom_pct = (cur_net - prev_net) / prev_net * 100
        arrow = "▲" if mom_pct >= 0 else "▼"
        trend = "증가" if mom_pct >= 0 else "감소"
        text = (
            f"{cur_label} 온북 <strong>{cur_net:,}RN</strong> · "
            f"전월({prev_label} {prev_net:,}RN) 대비 {arrow} <strong>{abs(mom_pct):.1f}%</strong> {trend}."
        )
    elif cur_net is not None:
        text = f"{cur_label} 온북 <strong>{cur_net:,}RN</strong> 집계 완료. 전월 비교 데이터 미확인."
    else:
        text = f"{cur_label} 온북 집계 중 — 데이터 확인 필요."
    insights.append({"text": text, "auto": True, "source": "monthly_total/mom"})

    # 인사이트 2: 사업장별 온북 상위/하위
    prop_nets = {}
    for prop_name, prop_monthly in by_property.items():
        nd = prop_monthly.get(cur_key, {}).get("net_rn")
        if nd is not None:
            prop_nets[prop_name] = nd

    if prop_nets:
        sorted_props = sorted(prop_nets.items(), key=lambda x: x[1], reverse=True)
        total_net = sum(prop_nets.values()) or 1
        top_name, top_val = sorted_props[0]
        bot_name, bot_val = sorted_props[-1]
        top_share = top_val / total_net * 100
        bot_share = bot_val / total_net * 100
        text = (
            f"사업장별 {cur_label} 온북 상위 <strong>{top_name} {top_val:,}RN</strong> ({top_share:.1f}%) · "
            f"하위 <strong>{bot_name} {bot_val:,}RN</strong> ({bot_share:.1f}%) — "
            f"전체 {len(prop_nets)}개 사업장 집계."
        )
    else:
        text = f"{cur_label} 사업장별 온북 집계 중."
    insights.append({"text": text, "auto": True, "source": "by_property"})

    # 인사이트 3: YoY 비교
    if cur_net is not None and yoy_net and yoy_net > 0:
        yoy_pct = (cur_net - yoy_net) / yoy_net * 100
        arrow = "▲" if yoy_pct >= 0 else "▼"
        trend = "성장" if yoy_pct >= 0 else "감소"
        text = (
            f"전년 동월({year - 1}/{month:02d}) 대비 {arrow} <strong>{abs(yoy_pct):.1f}%</strong> {trend} · "
            f"올해 {cur_net:,}RN vs 전년 {yoy_net:,}RN."
        )
    elif cur_net is not None:
        text = f"{cur_label} 온북 {cur_net:,}RN · 전년 동월 데이터 미집계 또는 비교 불가."
    else:
        text = f"전년 동월 비교 불가 — 현재 데이터 수집 중."
    insights.append({"text": text, "auto": True, "source": "monthly_total/yoy"})

    return insights


def main():
    logger.info("=" * 60)
    logger.info("인사이트 자동 생성 시작")
    logger.info("=" * 60)
    
    # 1. daily_notes.json (KPI만) 로드
    notes_file = DATA_DIR / "daily_notes.json"
    if not notes_file.exists():
        logger.error(f"daily_notes.json 없음: {notes_file}")
        return
    
    with open(notes_file, "r", encoding="utf-8") as f:
        notes = json.load(f)
    
    kpi = notes.get("executive_kpi", {})
    logger.info(f"✓ KPI 로드: K1={kpi.get('kpi_1',{}).get('value')}, "
                f"K2={kpi.get('kpi_2',{}).get('value')}, "
                f"K3={kpi.get('kpi_3',{}).get('value')}")
    
    # 2. db_aggregated.json 로드 (있으면)
    agg_file = DATA_DIR / "db_aggregated.json"
    agg_data = {}
    if agg_file.exists():
        with open(agg_file, "r", encoding="utf-8") as f:
            agg_data = json.load(f)
        logger.info(f"✓ db_aggregated 로드: monthly_total {len(agg_data.get('monthly_total', {}))}개월")
    else:
        logger.warning("db_aggregated.json 없음 - DB 인사이트 스킵")

    # 3. news_latest.json 로드 (있으면)
    news_file = DATA_DIR / "news_latest.json"
    news_by_region = {"vivaldi": [], "central": [], "south": [], "apac": [], "general": []}
    news_top = []
    if news_file.exists():
        with open(news_file, "r", encoding="utf-8") as f:
            news_data = json.load(f)
        news_by_region = news_data.get("by_region", news_by_region)
        news_top = news_data.get("top_news", [])
        logger.info(f"✓ 뉴스 로드: {len(news_top)} TOP / 전체 {news_data.get('total_count', 0)}건")
    else:
        logger.warning("뉴스 파일 없음 - 기본값 사용")
    
    # 4. 자동 생성
    now = datetime.now(KST)
    day_map = {0: "월요일", 1: "화요일", 2: "수요일", 3: "목요일", 4: "금요일", 5: "토요일", 6: "일요일"}

    # db_aggregated 기반 인사이트 3개 생성
    db_headlines = build_db_insights(agg_data) if agg_data else []
    # KPI 기반 헤드라인 (fallback용)
    kpi_headline = build_headline(kpi, news_top)
    # today_headline.text = 첫 번째 DB 인사이트 (없으면 KPI 기반)
    default_headline_text = db_headlines[0]["text"] if db_headlines else kpi_headline

    enriched = {
        "_generated_at": now.isoformat(),
        "_generator": "scripts/generate_insights.py (auto-generated from db_aggregated + KPI)",

        "report_date": now.strftime("%Y-%m-%d"),
        "report_day_kst": day_map[now.weekday()],

        "today_headline": {
            "text": default_headline_text,
        },
        "today_headlines": db_headlines,

        "executive_kpi": kpi,
        "region_status": build_region_status(kpi),
        "action_alerts": build_action_alerts(kpi, news_by_region),
    }

    # 5. enriched_notes.json 저장
    output_file = DATA_DIR / "enriched_notes.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    logger.info("=" * 60)
    logger.info(f"✓ 자동 생성 완료")
    logger.info(f"  DB 인사이트: {len(db_headlines)}개")
    for i, h in enumerate(db_headlines, 1):
        logger.info(f"  [{i}] {h['text'][:80]}...")
    logger.info(f"  저장: {output_file}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
