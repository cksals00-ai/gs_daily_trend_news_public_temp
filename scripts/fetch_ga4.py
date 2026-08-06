#!/usr/bin/env python3
"""
GA4 웹 애널리틱스 자동 수집 — Google Analytics Data API (GA4)
=============================================================

소노호텔앤리조트 통합 GA4 속성에서 최근 12개월 지표를 수집해
docs/data/ga4_latest.json 으로 저장한다. (파이프라인이 커밋·배포)

인증 (아래 중 하나 — 위에서부터 우선 적용):
  A) OAuth 사용자 인증 (관리자 권한 불필요, 내 구글 계정 사용) ← 이 프로젝트 채택
       - GA4_OAUTH_CLIENT_ID
       - GA4_OAUTH_CLIENT_SECRET
       - GA4_OAUTH_REFRESH_TOKEN   (scripts/ga4_oauth_setup.py 로 1회 발급)
  B) 서비스계정 키(JSON 문자열)      : GA4_SA_JSON
  C) 서비스계정 키 파일 경로          : GOOGLE_APPLICATION_CREDENTIALS

속성 지정:
  - 환경변수 GA4_PROPERTY_ID (기본 433673272 = 소노호텔앤리조트 통합)

의존성:
  pip install google-analytics-data          # 수집(런타임)
  pip install google-auth-oauthlib           # 토큰 최초 발급(로컬 1회)만 필요

동작 원칙(파이프라인 안전):
  - 자격증명이 없거나 API 오류면 기존 JSON을 보존하고 exit 0 (배포를 깨지 않음).
  - collect_powerbi.py 와 동일하게 optional 스텝으로 동작.

사용법:
  python scripts/fetch_ga4.py [--property 433673272] [--output docs/data/ga4_latest.json]
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("fetch_ga4")

KST = timezone(timedelta(hours=9))

DEFAULT_PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "433673272")
DEFAULT_OUTPUT = "docs/data/ga4_latest.json"

# 최근 12개월(어제까지). 오늘은 데이터가 미완성이라 제외.
DATE_START = "365daysAgo"
DATE_END = "yesterday"
# 직전 동일기간(전년 동기 비교용 델타)
PREV_START = "729daysAgo"
PREV_END = "366daysAgo"


def _load_client():
    """서비스계정 자격증명으로 GA4 Data API 클라이언트 생성. 없으면 None."""
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
    except ImportError:
        logger.warning("google-analytics-data 미설치 — `pip install google-analytics-data`. 스킵.")
        return None

    SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
    oauth_rt = os.environ.get("GA4_OAUTH_REFRESH_TOKEN", "").strip()
    oauth_id = os.environ.get("GA4_OAUTH_CLIENT_ID", "").strip()
    oauth_secret = os.environ.get("GA4_OAUTH_CLIENT_SECRET", "").strip()

    # 로컬 편의: env 가 없으면 ga4_oauth_setup.py 가 만든 토큰 파일에서 읽음
    # (CI 에서는 env(GitHub Secrets)가 우선. 이 파일은 .gitignore 로 커밋 방지.)
    if not (oauth_rt and oauth_id and oauth_secret):
        for p in ("ga4_oauth_token.json", os.path.join(os.path.dirname(__file__), "..", "ga4_oauth_token.json")):
            if os.path.exists(p):
                try:
                    with open(p, encoding="utf-8") as f:
                        tok = json.load(f)
                    oauth_id = oauth_id or tok.get("client_id", "")
                    oauth_secret = oauth_secret or tok.get("client_secret", "")
                    oauth_rt = oauth_rt or tok.get("refresh_token", "")
                    logger.info("자격증명: 로컬 토큰 파일 (%s)", p)
                    break
                except Exception:  # noqa: BLE001
                    pass
    sa_json = os.environ.get("GA4_SA_JSON", "").strip()
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

    try:
        # A) OAuth 사용자 인증 (refresh token) — google-auth 가 자동 갱신
        if oauth_rt and oauth_id and oauth_secret:
            from google.oauth2.credentials import Credentials
            creds = Credentials(
                token=None,
                refresh_token=oauth_rt,
                client_id=oauth_id,
                client_secret=oauth_secret,
                token_uri="https://oauth2.googleapis.com/token",
                scopes=SCOPES,
            )
            logger.info("자격증명: OAuth 사용자 (refresh token)")
            return BetaAnalyticsDataClient(credentials=creds)
        # B) 서비스계정 (JSON 문자열)
        if sa_json:
            from google.oauth2 import service_account
            info = json.loads(sa_json)
            creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            logger.info("자격증명: GA4_SA_JSON (env)")
            return BetaAnalyticsDataClient(credentials=creds)
        # C) 서비스계정 (파일 경로)
        if key_path and os.path.exists(key_path):
            logger.info("자격증명: GOOGLE_APPLICATION_CREDENTIALS (%s)", key_path)
            return BetaAnalyticsDataClient()
    except Exception as e:  # noqa: BLE001
        logger.warning("자격증명 로드 실패: %s", e)
        return None

    logger.warning("GA4 자격증명 없음 (OAuth/서비스계정) — 스킵.")
    return None


def _run(client, property_id, dimensions, metrics, date_start, date_end,
         order_by_metric=None, desc=True, limit=None):
    """runReport 래퍼 → [{dim/metric: value}] 리스트로 반환."""
    from google.analytics.data_v1beta.types import (
        DateRange, Dimension, Metric, RunReportRequest, OrderBy,
    )
    kwargs = dict(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=date_start, end_date=date_end)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
    )
    if order_by_metric:
        kwargs["order_bys"] = [
            OrderBy(metric=OrderBy.MetricOrderBy(metric_name=order_by_metric), desc=desc)
        ]
    if limit:
        kwargs["limit"] = limit
    resp = client.run_report(RunReportRequest(**kwargs), timeout=120.0)

    rows = []
    for r in resp.rows:
        row = {}
        for i, d in enumerate(dimensions):
            row[d] = r.dimension_values[i].value
        for i, m in enumerate(metrics):
            raw = r.metric_values[i].value
            try:
                row[m] = float(raw) if ("." in raw or "e" in raw.lower()) else int(raw)
            except (ValueError, AttributeError):
                row[m] = raw
        rows.append(row)
    return rows


def _totals(client, property_id, date_start, date_end):
    """기간 전체 요약 지표(단일 행)."""
    metrics = [
        "activeUsers", "newUsers", "sessions", "screenPageViews",
        "engagementRate", "averageSessionDuration", "eventCount",
        "bounceRate", "sessionsPerUser",
    ]
    rows = _run(client, property_id, [], metrics, date_start, date_end)
    return rows[0] if rows else {m: 0 for m in metrics}


# GA itemCategory(동/건물 단위) → 내부 사업장 크로스워크
def _property_key(name):
    n = (name or "").replace(" ", "")
    if "비발디" in n:
        if "빌리지" in n: return ("05", "빌리지비발디")
        if "펠리체" in n: return ("04", "펠리체비발디")
        if "펫" in n: return ("03", "펫비발디")
        if "캄" in n: return ("02", "캄비발디")
        return ("01", "벨비발디")
    for kw, key, lab in [
        ("양평", "06", "양평"), ("델피노", "07", "델피노"), ("양양", "08", "쏠비치양양"),
        ("삼척", "09", "쏠비치삼척"), ("단양", "10", "소노벨단양"), ("경주", "11", "소노캄경주"),
        ("청송", "12", "소노벨청송"), ("천안", "13", "소노벨천안"), ("변산", "14", "소노벨변산"),
        ("여수", "15", "소노캄여수"), ("거제", "16", "소노캄거제"), ("진도", "17", "쏠비치진도"),
        ("고양", "20", "소노캄고양"), ("해운대", "21", "소노문해운대"), ("소노문", "21", "소노문해운대"),
        ("남해", "22", "쏠비치남해"), ("르네블루", "23", "르네블루")]:
        if kw in n:
            return (key, lab)
    if "제주" in n:
        return ("19", "소노캄제주") if "캄" in n else ("18", "소노벨제주")
    return None


def build_homepage(rows):
    """GA 이커머스(itemCategory × 예약월) → 내부 사업장별 자사웹 실적.
    주의: GA는 예약(거래)일 기준이며 투숙월이 아님."""
    props = {}
    unmapped_rev = 0.0
    for r in rows:
        km = _property_key(r.get("itemCategory"))
        rev = r.get("itemRevenue", 0) or 0
        rn = r.get("itemsPurchased", 0) or 0
        ym = r.get("yearMonth")
        if km is None:
            unmapped_rev += rev
            continue
        key = f"{km[0]}.{km[1]}"
        p = props.setdefault(key, {"key": key, "name": km[1], "revenue": 0, "bookings": 0, "monthly": {}})
        p["revenue"] += rev
        p["bookings"] += rn
        if ym:
            p["monthly"][ym] = p["monthly"].get(ym, 0) + rev
    plist = sorted(props.values(), key=lambda x: -x["revenue"])
    total = sum(p["revenue"] for p in plist)
    for p in plist:
        p["rev_share"] = round(p["revenue"] / total * 100, 1) if total else 0
        p["revenue"] = round(p["revenue"])
    return {"note": "GA 이커머스 기준(예약일). 투숙월 아님.", "total_revenue": round(total),
            "unmapped_revenue": round(unmapped_rev), "properties": plist}


def build_funnel(client, property_id):
    """구매 퍼널(고객여정) + 기기별 전환 + 정량 기회."""
    fe = _run(client, property_id, ["eventName", "deviceCategory"], ["eventCount"], DATE_START, DATE_END, limit=400)
    du = _run(client, property_id, ["deviceCategory"], ["activeUsers"], DATE_START, DATE_END)
    agg = {}
    for r in fe:
        agg.setdefault(r["eventName"], {})[r["deviceCategory"]] = r.get("eventCount", 0)
    users = {r["deviceCategory"]: r.get("activeUsers", 0) for r in du}

    def tot(e):
        return sum(agg.get(e, {}).values())

    def mob(e):
        t = tot(e)
        return round(agg.get(e, {}).get("mobile", 0) / t * 100) if t else None

    selects = sum(tot(e) for e in ["select_room", "select_package", "select_activity", "select_dining", "select_ticket"])
    steps = [
        {"key": "list", "label": "숙소 리스트 조회", "count": tot("view_item_list"), "mobile_pct": mob("view_item_list")},
        {"key": "select", "label": "상품 선택", "count": selects, "mobile_pct": None},
        {"key": "booking", "label": "예약 버튼", "count": tot("click_pkg_booking_btn"), "mobile_pct": mob("click_pkg_booking_btn")},
        {"key": "purchase", "label": "구매 완료", "count": tot("purchase"), "mobile_pct": mob("purchase")},
    ]
    pur = agg.get("purchase", {})
    dev = {}
    for d in ["mobile", "desktop", "tablet"]:
        u, p = users.get(d, 0), pur.get(d, 0)
        dev[d] = {"users": u, "purchases": p, "rate": round(p / u * 100, 2) if u else None}
    uplift = None
    if dev["mobile"]["users"] and dev["desktop"]["rate"] and dev["mobile"]["rate"]:
        uplift = round((dev["desktop"]["rate"] - dev["mobile"]["rate"]) / 100 * dev["mobile"]["users"])
    mix = [{"name": n, "count": tot(e)} for e, n in
           [("select_room", "객실"), ("select_package", "패키지"), ("select_activity", "액티비티"),
            ("select_dining", "다이닝"), ("select_ticket", "티켓")]]
    mix.sort(key=lambda x: -x["count"])
    conv = {
        "booking_to_purchase": round(tot("purchase") / tot("click_pkg_booking_btn") * 100, 1) if tot("click_pkg_booking_btn") else None,
        "form_completion": round(tot("form_submit") / tot("form_start") * 100, 1) if tot("form_start") else None,
        "select_room_mobile_pct": mob("select_room"),
    }
    # 채널별 전환율 (구매 ÷ 세션)
    channel_conv = []
    try:
        cc = _run(client, property_id, ["sessionDefaultChannelGroup"],
                  ["sessions", "ecommercePurchases"], DATE_START, DATE_END,
                  order_by_metric="sessions", limit=12)
        for r in cc:
            s = r.get("sessions", 0) or 0
            if s >= 1000:
                p = r.get("ecommercePurchases", 0) or 0
                channel_conv.append({"channel": r["sessionDefaultChannelGroup"], "sessions": s,
                                     "purchases": p, "cvr": round(p / s * 100, 2)})
        channel_conv.sort(key=lambda x: -x["cvr"])
    except Exception:  # noqa: BLE001
        pass
    # 요일별 예약 (GA dayOfWeek: 0=일 … 6=토)
    weekday = []
    try:
        wk = _run(client, property_id, ["dayOfWeek"], ["ecommercePurchases", "sessions"], DATE_START, DATE_END)
        wm = {r["dayOfWeek"]: r for r in wk}
        weekday = [{"dow": d, "purchases": wm.get(str(d), {}).get("ecommercePurchases", 0),
                    "sessions": wm.get(str(d), {}).get("sessions", 0)} for d in range(7)]
    except Exception:  # noqa: BLE001
        pass
    # 요일 × 시간대 히트맵 (예약 밀도)
    heat = [[0] * 24 for _ in range(7)]
    try:
        hr = _run(client, property_id, ["dayOfWeek", "hour"], ["ecommercePurchases"], DATE_START, DATE_END, limit=300)
        for x in hr:
            try:
                heat[int(x["dayOfWeek"])][int(x["hour"])] = x.get("ecommercePurchases", 0)
            except (ValueError, IndexError):
                pass
    except Exception:  # noqa: BLE001
        pass
    return {"steps": steps, "device": dev, "mobile_uplift": uplift, "product_mix": mix,
            "conversions": conv, "channel_conv": channel_conv, "weekday": weekday, "heatmap": heat}


def build_actions(sec, out_path=None):
    """인사이트를 '실행 가능한 처방'으로 변환 — 우선순위·레버·근거·기대효과·실행법.
    대시보드 최상단 액션 플랜용."""
    import os as _os
    import json as _json
    A = []
    fn = sec.get("funnel", {})
    rt = sec.get("retention", {})
    monthly = sec.get("monthly", [])
    countries = sec.get("countries", [])
    dev = fn.get("device", {})
    cur_ym = datetime.now(KST).strftime("%Y%m")

    # 1) 모바일 결제 UX (최대 정량 기회)
    up = fn.get("mobile_uplift")
    mr = (dev.get("mobile") or {}).get("rate")
    dr = (dev.get("desktop") or {}).get("rate")
    if up and mr and dr:
        A.append({"priority": 1, "lever": "UX·전환", "title": "모바일 결제 퍼널 개선",
                  "why": f"모바일 전환 {mr:.1f}% vs 데스크톱 {dr:.1f}% (모바일 72% 트래픽)",
                  "impact": f"이론 상한 +{_fmt(up)} 구매/년", "level": "warn",
                  "how": "모바일 객실선택→결제 단계 단축 · 자동입력 · 간편결제 도입"})

    # 2) 재방문 CRM
    nw, rr = rt.get("new"), rt.get("returning")
    if nw and rr and rr.get("cvr_session") and nw.get("cvr_session"):
        tp = (nw["purchases"] + rr["purchases"]) or 1
        rs = rr["purchases"] / tp * 100
        ratio = rr["cvr_session"] / nw["cvr_session"]
        A.append({"priority": 2, "lever": "CRM·리텐션", "title": "재방문 유도(리마케팅·멤버십) 강화",
                  "why": f"재방문 전환 {ratio:.1f}배 · 구매의 {rs:.0f}%가 재방문", "level": "action",
                  "impact": "재방문율↑ = 매출 직접 상승", "how": "이탈 리마케팅 · 멤버 전용 쿠폰 · 재구매 주기 알림(CRM)"})

    # 3) 채널 예산 재배분
    cc = fn.get("channel_conv", [])
    if len(cc) >= 3:
        best = cc[0]
        worst = min(cc, key=lambda c: c["cvr"])
        if worst["cvr"] < best["cvr"] / 2:
            A.append({"priority": 3, "lever": "미디어 예산", "title": "저전환 유료 예산 → 고전환 채널 재배분",
                      "why": f"{worst['channel']} 전환 {worst['cvr']}% vs {best['channel']} {best['cvr']}%", "level": "action",
                      "impact": "동일 예산 대비 구매 수 증가", "how": f"{worst['channel']} 등 저전환 유료 축소 → SEO·리타겟·Direct 강화"})

    # 4) 비수기 캠페인 (연중 최저 달)
    seas = [m for m in monthly if m.get("yearMonth") != cur_ym]
    if len(seas) >= 4:
        trough = min(seas, key=lambda m: m.get("sessions", 0))
        tym = trough["yearMonth"]
        A.append({"priority": 4, "lever": "캠페인", "title": "비수기 수요창출 캠페인 집행",
                  "why": f"연중 최저 {tym[:4]}-{tym[4:]} 트래픽 저점 (성수기엔 수요 이미 충분)", "level": "action",
                  "impact": "저점 매출 방어 · 증분 효과 최대", "how": "비수기(3월·가을 어깨철) 타겟 유료+프로모, 성수기는 재고·요금 관리"})

    # 5) 사업장 웹 육성 (웹강도지수 — otb_data.json 있으면 정량)
    hp = sec.get("homepage_by_property", {})
    props = hp.get("properties", [])
    otb_path = _os.path.join(_os.path.dirname(out_path or "docs/data/x"), "otb_data.json")
    weak = []
    try:
        if props and _os.path.exists(otb_path):
            otb = _json.load(open(otb_path, encoding="utf-8"))
            bud = {p["name"]: (p.get("rns_budget") or 0) for p in otb.get("byProperty", [])}
            totb = sum(bud.values()) or 1
            totw = sum(p["revenue"] for p in props) or 1
            idx = []
            for p in props:
                bs = bud.get(p["key"], 0) / totb * 100
                ws = p["revenue"] / totw * 100
                if bs > 0.5:
                    idx.append((ws / bs * 100, p["name"]))
            idx.sort()
            weak = [n.split(".")[-1] for _, n in idx[:3]]
    except Exception:  # noqa: BLE001
        weak = []
    if weak:
        A.append({"priority": 5, "lever": "사업장 활성화", "title": f"웹 과소 사업장 육성: {', '.join(weak)}",
                  "why": "규모 대비 자사웹 판매가 약한 사업장(웹강도지수 하위)", "level": "action",
                  "impact": "약한 사업장 웹 매출 정상화", "how": "해당 사업장 상품 노출·SEO·프로모 우선 배치 / 도심 비즈니스는 채널전략 분리"})

    # 6) 국가·인바운드
    if countries:
        tc = sum(x.get("activeUsers", 0) for x in countries) or 1
        intl = sum(x.get("activeUsers", 0) for x in countries if x.get("country") != "South Korea")
        top_intl = [x["country"] for x in countries if x.get("country") != "South Korea"][:3]
        A.append({"priority": 6, "lever": "국가 전략", "title": "인바운드 타겟 결정",
                  "why": f"해외 {intl/tc*100:.0f}%뿐 · 상위 {', '.join(top_intl)}", "level": "info",
                  "impact": "백지 시장 진입 or 국내 집중 택1", "how": "목표면 일본어/영문 랜딩+현지 채널, 아니면 국내 집중 유지"})

    # 7) 측정 정합 (UTM)
    cmm = sec.get("channel_month", {})
    if cmm.get("unassigned") and cmm.get("total"):
        una = sum(cmm["unassigned"]) / (sum(cmm["total"]) or 1) * 100
        if una >= 3:
            A.append({"priority": 7, "lever": "측정·운영", "title": "UTM 태깅 표준화",
                      "why": f"Unassigned {una:.0f}% — 캠페인 성과 미귀속", "level": "info",
                      "impact": "캠페인 ROI 측정 복구", "how": "모든 캠페인 링크 UTM 규칙 통일 · 자동화"})

    return A


def build_retention(client, property_id):
    """신규 vs 재방문 — 구성·전환·매출."""
    r = _run(client, property_id, ["newVsReturning"],
             ["activeUsers", "sessions", "ecommercePurchases", "purchaseRevenue"], DATE_START, DATE_END)
    out = {}
    for x in r:
        k = x.get("newVsReturning")
        if k in ("new", "returning"):
            s = x.get("sessions", 0) or 0
            p = x.get("ecommercePurchases", 0) or 0
            out[k] = {"users": x.get("activeUsers", 0), "sessions": s, "purchases": p,
                      "revenue": x.get("purchaseRevenue", 0),
                      "cvr_session": round(p / s * 100, 2) if s else None}
    return out


def build_by_month(client, property_id):
    """각 지표를 월별로 쪼개 저장 → 대시보드 월 선택기용.
    반환: {"months":[ym...], "data":{ym:{channels,devices,countries,top_pages}}}"""
    from collections import defaultdict
    by = defaultdict(lambda: {"channels": [], "devices": [], "countries": [], "top_pages": []})

    def pull(dims, mets, sort, section, topn=None):
        try:
            rows = _run(client, property_id, dims, mets, DATE_START, DATE_END,
                        order_by_metric=sort, limit=2000)
        except Exception as e:  # noqa: BLE001
            logger.warning("  · by_month %s 실패: %s", section, e)
            return
        grp = defaultdict(list)
        for r in rows:
            ym = r.get("yearMonth")
            if ym:
                grp[ym].append({k: v for k, v in r.items() if k != "yearMonth"})
        for ym, lst in grp.items():
            lst.sort(key=lambda x: -(x.get(sort, 0) or 0))
            by[ym][section] = lst[:topn] if topn else lst

    pull(["yearMonth", "sessionDefaultChannelGroup"], ["sessions", "activeUsers", "engagementRate"],
         "sessions", "channels", topn=15)
    pull(["yearMonth", "deviceCategory"], ["activeUsers", "sessions"], "activeUsers", "devices")
    pull(["yearMonth", "country"], ["activeUsers", "sessions"], "activeUsers", "countries", topn=12)
    pull(["yearMonth", "pagePath"], ["screenPageViews", "activeUsers"], "screenPageViews", "top_pages", topn=15)

    months = sorted(by.keys())
    return {"months": months, "data": {m: by[m] for m in months}}


def _fmt(n):
    try:
        return f"{int(round(n)):,}"
    except (TypeError, ValueError):
        return str(n)


def build_insights(sec):
    """수집된 섹션에서 해석 인사이트(문장)를 자동 생성.
    각 인사이트: {level: warn|action|good|info, icon, title, body}"""
    out = []
    summ = sec.get("summary", {})
    cur = summ.get("current", {})
    dp = summ.get("delta_pct", {})
    monthly = sec.get("monthly", [])
    channels = sec.get("channels", [])
    devices = sec.get("devices", [])
    countries = sec.get("countries", [])
    pages = {p.get("pagePath"): p for p in sec.get("top_pages", [])}

    # (1) 데이터 신뢰성 — 추적 변경(신규/PV 급증 vs 사용자 정체) 탐지
    step = None
    for i in range(1, len(monthly)):
        prev = monthly[i - 1].get("newUsers", 0) or 0
        curv = monthly[i].get("newUsers", 0) or 0
        if prev > 0 and curv >= prev * 4 and curv > 100000:
            step = monthly[i].get("yearMonth")
            break
    du, dnu, dpv = dp.get("activeUsers"), dp.get("newUsers"), dp.get("screenPageViews")
    if step and dnu is not None and dnu > 300 and (du is None or abs(du) < 40):
        ym = f"{step[:4]}-{step[4:]}"
        out.append({
            "level": "warn", "icon": "⚠️", "title": f"YoY 해석 주의 — 추적 변경({ym})",
            "body": (f"신규 {dnu:+.0f}%·PV {dpv:+.0f}%인데 사용자 {du:+.1f}%. "
                     f"{ym} GA4 측정 변경 흔적 → 그 전 과소집계. 신뢰구간 {ym}↑, YoY 비교 지양 필요.")})
        reliable = [m for m in monthly if m.get("yearMonth", "") >= step]
    else:
        reliable = monthly[:]

    # 현재(부분) 월 제외 후 계절성
    cur_ym = datetime.now(KST).strftime("%Y%m")
    seas = [m for m in reliable if m.get("yearMonth") != cur_ym]
    if len(seas) >= 3:
        peak = max(seas, key=lambda m: m.get("sessions", 0))
        trough = min(seas, key=lambda m: m.get("sessions", 0))
        pym, tym = peak["yearMonth"], trough["yearMonth"]
        out.append({
            "level": "info", "icon": "📈", "title": f"성수기 {pym[:4]}-{pym[4:]} · 비수기 {tym[:4]}-{tym[4:]}",
            "body": (f"세션 성수기 {_fmt(peak['sessions'])} vs 비수기 {_fmt(trough['sessions'])}. "
                     f"성수기=재고·요금 관리, 비수기=유료·프로모 집중 필요.")})

    # (2) 채널 집중도 / 유료·태깅
    if channels:
        tot = sum(c.get("sessions", 0) for c in channels) or 1
        share = {c["sessionDefaultChannelGroup"]: c.get("sessions", 0) / tot * 100 for c in channels}
        do = share.get("Direct", 0) + share.get("Organic Search", 0)
        paid = sum(v for k, v in share.items() if k.startswith("Paid") or k == "Display")
        una = share.get("Unassigned", 0)
        os_ch = next((c for c in channels if c["sessionDefaultChannelGroup"] == "Organic Search"), None)
        body = f"Direct+Organic {do:.0f}% · 유료 {paid:.1f}%뿐."
        if os_ch:
            body += f" 오가닉 검색 참여율 {os_ch.get('engagementRate',0)*100:.0f}% 최상 → SEO 강화 최우선."
        out.append({"level": "action", "icon": "🧭", "title": f"브랜드·오가닉 {do:.0f}% 의존, 유료 미가동", "body": body})
        if una >= 3:
            out.append({
                "level": "action", "icon": "🏷️", "title": "UTM 태깅 누락 — 성과 유실",
                "body": f"Unassigned {una:.1f}%. 캠페인 성과 미귀속 → UTM 표준화 필요."})

    # (3) 모바일 비중
    if devices:
        tu = sum(x.get("activeUsers", 0) for x in devices) or 1
        mob = next((x for x in devices if x.get("deviceCategory") == "mobile"), None)
        if mob:
            pct = mob["activeUsers"] / tu * 100
            out.append({
                "level": "info", "icon": "📱", "title": f"모바일 {pct:.0f}% — 모바일 우선 설계",
                "body": f"사용자 {pct:.0f}% 모바일. 전환 개선은 모바일 객실→결제 집중 필요."})

    # (4) 예약 퍼널 이탈 (모바일 객실 → 결제)
    room = pages.get("/reserve/room/mo")
    pay = pages.get("/reserve/room/mo/payment")
    if room and pay and room.get("activeUsers"):
        rate = pay["activeUsers"] / room["activeUsers"] * 100
        lvl = "action" if rate < 50 else "good"
        out.append({
            "level": lvl, "icon": "🛒", "title": f"예약 퍼널 객실→결제 {rate:.0f}%",
            "body": (f"모바일 객실 {_fmt(room['activeUsers'])}명 → 결제 {_fmt(pay['activeUsers'])}명. "
                     f"반복조회 {room.get('screenPageViews',0)/max(room['activeUsers'],1):.0f}회 → 매진·마찰 여부 RM 교차확인 필요.")})

    # (5) 참여 품질
    er = cur.get("engagementRate")
    br = cur.get("bounceRate")
    if er is not None:
        out.append({
            "level": "good", "icon": "✅", "title": "참여 품질 양호",
            "body": (f"참여율 {er*100:.0f}%·이탈 {(br or 0)*100:.0f}%·체류 "
                     f"{int((cur.get('averageSessionDuration') or 0)//60)}분. 고관여 예약 트래픽.")})

    # (6) 해외 비중
    if countries:
        tc = sum(x.get("activeUsers", 0) for x in countries) or 1
        intl = sum(x.get("activeUsers", 0) for x in countries if x.get("country") != "South Korea")
        pct = intl / tc * 100
        out.append({
            "level": "info", "icon": "🌏", "title": f"해외 {pct:.0f}% — 인바운드 판단 지점",
            "body": (f"국내 {100-pct:.0f}% 집중, 해외 {pct:.0f}%뿐. "
                     f"인바운드=백지 기회 or 국내 집중 유지 판단 필요.")})

    # (7) 월별 이슈 자동 감지
    cmm = sec.get("channel_month", {})
    ymL = lambda y: f"{y[:4]}-{y[4:]}"

    # 7a) 신뢰구간 내 참여 품질 최저 달
    q = [m for m in seas if m.get("engagementRate") is not None]
    if len(q) >= 3:
        worst = min(q, key=lambda m: m["engagementRate"])
        wym = worst["yearMonth"]
        out.append({
            "level": "action", "icon": "📉", "title": f"참여 최저 달 {ymL(wym)}",
            "body": (f"참여율 {worst['engagementRate']*100:.0f}%·이탈 "
                     f"{(worst.get('bounceRate') or 0)*100:.0f}%로 최저. 유입·랜딩 점검 필요.")})

    # 7b) UTM 태깅 누락 급등 달 (Unassigned 월 점유율 ≥ 8%)
    if cmm.get("unassigned") and cmm.get("total"):
        months = cmm["months"]; una = cmm["unassigned"]; tot = cmm["total"]
        bad = [months[i] for i in range(len(months))
               if tot[i] and una[i] / tot[i] >= 0.08 and months[i] != cur_ym]
        if bad:
            out.append({
                "level": "action", "icon": "🏷️", "title": "캠페인 달 UTM 누락 반복",
                "body": (f"Unassigned 8%↑: {', '.join(ymL(m) for m in bad)}. "
                         f"성수기 무태깅 집행 → 표준화 필요.")})

    # 7c) 유료 예산 타이밍 엇박 (연중 최저 달에 유료 축소)
    if cmm.get("paid") and seas:
        months = cmm["months"]; paid = cmm["paid"]
        trough = min(seas, key=lambda m: m.get("sessions", 0))
        tym = trough["yearMonth"]
        if tym in months:
            ti = months.index(tym)
            avgp = sum(paid) / len(paid) if paid else 0
            if avgp and paid[ti] < avgp * 0.6:
                out.append({
                    "level": "action", "icon": "⏱️", "title": "유료 예산 타이밍 엇박",
                    "body": (f"최저 {ymL(tym)}에 유료 {paid[ti]/avgp*100:.0f}%로 축소. "
                             f"비수기로 예산 이동 필요.")})

    # (8) 고객여정(퍼널) 심층 — 정량 기회
    fn = sec.get("funnel", {})
    if fn:
        dev = fn.get("device", {})
        mr = (dev.get("mobile") or {}).get("rate")
        dr = (dev.get("desktop") or {}).get("rate")
        up = fn.get("mobile_uplift")
        if mr and dr and up and dr > mr:
            out.append({
                "level": "warn", "icon": "🔻", "title": f"모바일 전환 열위 — 연 +{_fmt(up)} 기회",
                "body": (f"전환 모바일 {mr:.1f}% vs 데스크톱 {dr:.1f}%. "
                         f"데스크톱 수준시 모바일 +{_fmt(up)} 구매/년. 모바일 결제 UX 최우선.")})
        cv = fn.get("conversions", {})
        b2p = cv.get("booking_to_purchase")
        if b2p is not None:
            lvl = "good" if b2p >= 70 else "action"
            out.append({
                "level": lvl, "icon": "🎯", "title": f"예약버튼→구매 {b2p:.0f}%",
                "body": (f"클릭 후 마감률 {b2p:.0f}%. "
                         + ("마감 강함 → 상단 유입·객실 노출 확대가 지렛대." if b2p >= 70 else "마감 이탈 큼 → 결제 단계 점검 필요."))})
        fc = cv.get("form_completion")
        if fc is not None and fc < 60:
            out.append({
                "level": "action", "icon": "📝", "title": f"폼 완료율 {fc:.0f}% — 절반 이탈",
                "body": f"폼 시작→완료 {fc:.0f}%. 입력 항목 단축·자동완성 필요."})
        srm = cv.get("select_room_mobile_pct")
        if srm is not None and srm < 15:
            out.append({
                "level": "action", "icon": "🧩", "title": "모바일 객실선택 계측 공백",
                "body": f"객실선택 이벤트 모바일 {srm:.0f}%뿐 — 여정 추적 공백. 모바일 select_room 태깅 필요."})
        mix = fn.get("product_mix", [])
        if len(mix) >= 2 and mix[0]["count"]:
            top2 = " · ".join(f"{m['name']}" for m in mix[:3])
            out.append({
                "level": "info", "icon": "🧺", "title": f"상품 선택 1위 {mix[0]['name']}",
                "body": f"선택 볼륨 {top2} 순. 객실 외 상품 교차판매 여지."})
        # 채널별 전환율 — 최고 vs 유료 낭비
        cc = fn.get("channel_conv", [])
        if len(cc) >= 3:
            best = cc[0]
            paid_lo = min((c for c in cc if c["channel"].startswith("Paid") or c["channel"] == "Display"),
                          key=lambda c: c["cvr"], default=None)
            body = f"전환 1위 {best['channel']} {best['cvr']:.1f}%."
            if paid_lo and paid_lo["cvr"] < best["cvr"] / 2:
                body += f" 반면 {paid_lo['channel']} {paid_lo['cvr']:.1f}%로 트래픽 낭비 — 타게팅·소재 재점검 필요."
            out.append({
                "level": "action" if paid_lo and paid_lo["cvr"] < 1.5 else "info",
                "icon": "💱", "title": "채널별 전환율 편차 큼", "body": body})
        # 요일 예약 패턴
        wd = fn.get("weekday", [])
        if len(wd) == 7 and sum(w["purchases"] for w in wd):
            names = ["일", "월", "화", "수", "목", "금", "토"]
            pk = max(wd, key=lambda w: w["purchases"])
            lo = min(wd, key=lambda w: w["purchases"])
            out.append({
                "level": "info", "icon": "🗓️", "title": f"예약 최다 {names[pk['dow']]}요일 · 최저 {names[lo['dow']]}요일",
                "body": f"주중 예약 집중, {names[lo['dow']]}요일 최저. 주중 프로모·재고 노출 강화 여지."})
        # 요일×시간대 피크
        hm = fn.get("heatmap")
        if hm and any(any(r) for r in hm):
            names = ["일", "월", "화", "수", "목", "금", "토"]
            pk = max(((hm[d][h], d, h) for d in range(7) for h in range(24)), key=lambda x: x[0])
            tot = sum(sum(r) for r in hm) or 1
            biz = sum(hm[d][h] for d in range(7) for h in range(9, 18)) / tot * 100
            out.append({
                "level": "info", "icon": "⏰", "title": f"예약 피크 {names[pk[1]]}요일 {pk[2]}시",
                "body": f"업무시간(9~18시) 예약 {biz:.0f}% 집중. 광고 노출·CS 인력 이 시간대 배치 효율적."})

    # (9) 리텐션 — 재방문 전환 우위
    rt = sec.get("retention", {})
    nw, rr = rt.get("new"), rt.get("returning")
    if nw and rr and nw.get("cvr_session") and rr.get("cvr_session"):
        tp = (nw["purchases"] + rr["purchases"]) or 1
        rshare = rr["purchases"] / tp * 100
        ratio = rr["cvr_session"] / nw["cvr_session"] if nw["cvr_session"] else 0
        out.append({
            "level": "action", "icon": "🔁", "title": f"재방문이 구매 {rshare:.0f}% — 리텐션이 핵심",
            "body": (f"세션 전환 재방문 {rr['cvr_session']:.1f}% vs 신규 {nw['cvr_session']:.1f}%({ratio:.1f}배). "
                     f"재방문이 구매의 {rshare:.0f}%. CRM·리마케팅이 성장 지렛대.")})

    return out


def collect(property_id):
    client = _load_client()
    if client is None:
        return None

    logger.info("GA4 속성 %s — 최근 12개월(%s~%s) 수집 시작", property_id, DATE_START, DATE_END)

    data = {"property_id": property_id, "sections": {}}
    sec = data["sections"]

    # 1) 요약 지표 (+ 전년 동기 델타)
    try:
        cur = _totals(client, property_id, DATE_START, DATE_END)
        prev = _totals(client, property_id, PREV_START, PREV_END)
        deltas = {}
        for k, v in cur.items():
            pv = prev.get(k, 0)
            try:
                deltas[k] = round(((v - pv) / pv) * 100, 1) if pv else None
            except TypeError:
                deltas[k] = None
        sec["summary"] = {"current": cur, "previous": prev, "delta_pct": deltas}
        logger.info("  ✔ 요약: 사용자 %s · 세션 %s · 조회수 %s",
                    cur.get("activeUsers"), cur.get("sessions"), cur.get("screenPageViews"))
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 요약 실패: %s", e)

    # 2) 월별 추이 (+ 품질지표: 참여율·이탈률·체류·이벤트)
    try:
        rows = _run(client, property_id, ["yearMonth"],
                    ["activeUsers", "sessions", "screenPageViews", "newUsers",
                     "engagementRate", "bounceRate", "averageSessionDuration", "eventCount"],
                    DATE_START, DATE_END)
        rows.sort(key=lambda r: r.get("yearMonth", ""))
        sec["monthly"] = rows
        logger.info("  ✔ 월별 추이: %d개월", len(rows))
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 월별 추이 실패: %s", e)

    # 3) 채널(트래픽 소스)
    try:
        sec["channels"] = _run(
            client, property_id, ["sessionDefaultChannelGroup"],
            ["sessions", "activeUsers", "engagementRate"],
            DATE_START, DATE_END, order_by_metric="sessions", limit=15)
        logger.info("  ✔ 채널: %d개", len(sec["channels"]))
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 채널 실패: %s", e)

    # 3.5) 채널 × 월 (채널 믹스 추이) — 상위 6개 채널 + 기타로 피벗
    try:
        cm = _run(client, property_id, ["yearMonth", "sessionDefaultChannelGroup"],
                  ["sessions"], DATE_START, DATE_END)
        months = sorted({r["yearMonth"] for r in cm})
        by_ch = {}
        for r in cm:
            by_ch.setdefault(r["sessionDefaultChannelGroup"], {})[r["yearMonth"]] = r.get("sessions", 0)
        totals = {ch: sum(v.values()) for ch, v in by_ch.items()}
        top_ch = sorted(totals, key=totals.get, reverse=True)[:6]
        series = [{"channel": ch, "sessions": [by_ch[ch].get(m, 0) for m in months]} for ch in top_ch]
        etc = [sum(by_ch[ch].get(m, 0) for ch in by_ch if ch not in top_ch) for m in months]
        if any(etc):
            series.append({"channel": "기타", "sessions": etc})
        paid_m = [sum(by_ch[ch].get(m, 0) for ch in by_ch
                      if ch.startswith("Paid") or ch == "Display") for m in months]
        una_m = [by_ch.get("Unassigned", {}).get(m, 0) for m in months]
        total_m = [sum(by_ch[ch].get(m, 0) for ch in by_ch) for m in months]
        sec["channel_month"] = {"months": months, "series": series,
                                "paid": paid_m, "unassigned": una_m, "total": total_m}
        logger.info("  ✔ 채널×월: %d개월 × %d채널", len(months), len(series))
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 채널×월 실패: %s", e)

    # 4) 인기 페이지 — 대형 속성 504 방지: 경로만(제목 제외), 지난 90일로 범위 축소 + 재시도
    try:
        try:
            pages = _run(
                client, property_id, ["pagePath"],
                ["screenPageViews", "activeUsers"],
                DATE_START, DATE_END, order_by_metric="screenPageViews", limit=20)
        except Exception as e1:  # noqa: BLE001
            logger.warning("  · 인기 페이지 12개월 실패(%s) → 최근 90일로 재시도", e1)
            pages = _run(
                client, property_id, ["pagePath"],
                ["screenPageViews", "activeUsers"],
                "90daysAgo", DATE_END, order_by_metric="screenPageViews", limit=20)
        sec["top_pages"] = pages
        logger.info("  ✔ 인기 페이지: %d개", len(sec["top_pages"]))
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 인기 페이지 실패: %s", e)

    # 5) 기기
    try:
        sec["devices"] = _run(
            client, property_id, ["deviceCategory"],
            ["activeUsers", "sessions"],
            DATE_START, DATE_END, order_by_metric="activeUsers")
        logger.info("  ✔ 기기: %d개", len(sec["devices"]))
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 기기 실패: %s", e)

    # 6) 국가/지역
    try:
        sec["countries"] = _run(
            client, property_id, ["country"],
            ["activeUsers", "sessions"],
            DATE_START, DATE_END, order_by_metric="activeUsers", limit=12)
        logger.info("  ✔ 국가: %d개", len(sec["countries"]))
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 국가 실패: %s", e)

    # 6.3) 고객여정(구매 퍼널) + 기기별 전환
    try:
        sec["funnel"] = build_funnel(client, property_id)
        up = sec["funnel"].get("mobile_uplift")
        logger.info("  ✔ 고객여정 퍼널 (모바일 전환기회 +%s)", _fmt(up) if up else "—")
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 퍼널 실패: %s", e)

    # 6.4) 신규 vs 재방문 (리텐션)
    try:
        sec["retention"] = build_retention(client, property_id)
        logger.info("  ✔ 리텐션(신규/재방문)")
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 리텐션 실패: %s", e)

    # 6.5) 월별 상세 (월 선택기용) — 채널·기기·국가·페이지를 월별로
    try:
        sec["by_month"] = build_by_month(client, property_id)
        logger.info("  ✔ 월별 상세: %d개월", len(sec["by_month"]["months"]))
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 월별 상세 실패: %s", e)

    # 7) 홈페이지(자사웹) 사업장별 실적 — 이커머스 itemCategory 크로스워크
    try:
        prop = _run(client, property_id, ["itemCategory", "yearMonth"],
                    ["itemRevenue", "itemsPurchased"], DATE_START, DATE_END)
        sec["homepage_by_property"] = build_homepage(prop)
        logger.info("  ✔ 홈페이지 사업장별: %d개 사업장 (미매핑 %.1f억)",
                    len(sec["homepage_by_property"]["properties"]),
                    sec["homepage_by_property"]["unmapped_revenue"] / 1e8)
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 홈페이지 사업장별 실패: %s", e)

    if not sec:
        logger.warning("수집된 섹션 없음 — 저장 스킵.")
        return None

    # 7) 자동 인사이트 (해석 로직)
    try:
        data["insights"] = build_insights(sec)
        logger.info("  ✔ 자동 인사이트: %d건", len(data["insights"]))
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 인사이트 생성 실패: %s", e)
        data["insights"] = []

    # 7.5) 액션 플랜 (처방)
    try:
        data["actions"] = build_actions(sec, DEFAULT_OUTPUT)
        logger.info("  ✔ 액션 플랜: %d건", len(data["actions"]))
    except Exception as e:  # noqa: BLE001
        logger.warning("  ✗ 액션 생성 실패: %s", e)
        data["actions"] = []

    data["meta"] = {
        "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
        "generated_iso": datetime.now(KST).isoformat(),
        "date_range": {"start": DATE_START, "end": DATE_END, "label": "최근 12개월"},
        "source": "Google Analytics Data API (GA4)",
        "property_id": property_id,
    }
    return data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--property", default=DEFAULT_PROPERTY_ID)
    ap.add_argument("--output", default=DEFAULT_OUTPUT)
    args = ap.parse_args()

    data = collect(args.property)
    if data is None:
        logger.info("GA4 수집 결과 없음 — 기존 %s 보존.", args.output)
        return 0  # 파이프라인을 깨지 않음

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("저장 완료 → %s", args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
