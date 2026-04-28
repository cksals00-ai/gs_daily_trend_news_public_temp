#!/usr/bin/env python3
"""
GS Daily Trend Report V7 - 뉴스 자동 수집 스크립트
============================================================
Google News RSS에서 호텔/리조트/항공/OTA/여행/레저 관련 뉴스를 수집.
소노/SONO 언급된 뉴스는 제외 (자사 언급 회피).

동작:
  1. 키워드별 Google News RSS 호출
  2. 소노/SONO/대명/Daemyung 언급 필터링 (제외)
  3. 권역/카테고리별 분류
  4. data/news_latest.json 저장

실행:
  python scripts/collect_news.py

© 2026 GS팀 · Haein Kim Manager
"""
import json
import logging
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_FILE = DATA_DIR / "news_latest.json"
KST = timezone(timedelta(hours=9))

# ─────────────────────────────────────────────
# 키워드 정의 (카테고리별)
# ─────────────────────────────────────────────
KEYWORDS = {
    "호텔/리조트": [
        "호텔 객실", "리조트 패키지", "호텔 신규 오픈", "리조트 프로모션",
        "호텔업계", "한화리조트", "롯데호텔", "신라호텔", "휘닉스 평창", "하이원리조트",
    ],
    "항공/공항": [
        "대한항공", "아시아나항공", "제주항공", "티웨이항공", "유류할증료",
        "국제선 증편", "제주노선", "공항 이용객",
    ],
    "OTA/여행": [
        "야놀자", "여기어때", "트립닷컴", "아고다", "Booking.com",
        "OTA 여행", "온라인 여행", "여행 플랫폼",
    ],
    "관광/지역": [
        "강원 관광", "제주 관광", "여수 관광", "남해 관광", "지역 축제",
        "벚꽃 축제", "봄꽃 축제", "외국인 관광객", "인바운드 관광",
    ],
    "레저/휴양": [
        "골프장", "워터파크", "스키리조트", "캠핑", "글램핑",
    ],
    "거시지표": [
        "원달러 환율", "국제유가", "WTI 유가",
    ],
}

# ─────────────────────────────────────────────
# 제외 키워드 (자사 + 부정 키워드)
# ─────────────────────────────────────────────
EXCLUDE_KEYWORDS = [
    "소노", "SONO", "Sono", "sono",
    "대명", "Daemyung",
    "비발디파크", "Vivaldi Park",
    "쏠비치", "솔비치", "Solbeach", "Sol Beach",
    "델피노",
    "오션캐슬",
]

# 권역 매핑 (제주·고양은 APAC, 2026-04-21 변경)
REGION_MAP = {
    "비발디": "vivaldi",
    "한화리조트": "vivaldi",
    "휘닉스": "central",
    "하이원": "central",
    "웰리힐리": "central",
    "강원": "central",
    "양평": "central",
    "양양": "central",
    "삼척": "central",
    "단양": "central",
    "청송": "central",
    "변산": "central",
    "천안": "central",
    "여수": "south",
    "남해": "south",
    "거제": "south",
    "진도": "south",
    "경주": "south",
    "해운대": "south",
    "부산": "south",
    "신라호텔": "south",
    "롯데호텔": "south",
    "제주": "apac",
    "고양": "apac",
    "히든클리프": "apac",
    "해비치": "apac",
    "유류할증료": "apac",
    "환율": "apac",
    "유가": "apac",
    "국제선": "apac",
    "외국인": "apac",
    "인바운드": "apac",
    "아웃바운드": "apac",
    "대한항공": "apac",
    "아시아나": "apac",
    "제주항공": "apac",
    "하와이": "apac",
    "괌": "apac",
    "하이퐁": "apac",
    "베트남": "apac",
}


def fetch_google_news(query: str, limit: int = 5) -> list[dict]:
    """Google News RSS에서 뉴스 가져오기"""
    encoded = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl=ko&gl=KR&ceid=KR:ko"
    
    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0 (compatible; GSReportBot/1.0)'}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            xml_data = resp.read().decode('utf-8')
        
        root = ET.fromstring(xml_data)
        items = []
        for item in root.findall(".//item")[:limit]:
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            pub_date = item.findtext("pubDate", "").strip()
            source_el = item.find("source")
            source = source_el.text.strip() if source_el is not None else ""
            
            if not title or not link:
                continue
            
            # HTML 태그 제거
            title = re.sub(r'<[^>]+>', '', title)
            
            items.append({
                "title": title,
                "link": link,
                "source": source,
                "pub_date": pub_date,
            })
        
        return items
    except Exception as e:
        logger.warning(f"Google News 호출 실패 ({query}): {e}")
        return []


def is_stale_by_pub_date(pub_date_str: str, ref: datetime | None = None) -> bool:
    """pub_date(RFC 2822) 기준으로 오래된 기사인지 판별.

    제외 조건 (하나라도 해당되면 True):
      1. 발행 연도가 현재 연도와 다름
      2. 발행일이 현재 날짜 기준 3개월(90일) 이전
    """
    if not pub_date_str:
        return False  # 날짜 없으면 일단 통과
    from email.utils import parsedate_to_datetime
    try:
        pub_dt = parsedate_to_datetime(pub_date_str)
        now = ref or datetime.now(KST)
        # 조건 1: 연도 불일치
        if pub_dt.year != now.year:
            return True
        # 조건 2: 3개월(90일) 이전
        if (now - pub_dt).days > 90:
            return True
        return False
    except (ValueError, TypeError):
        return False  # 파싱 실패 시 통과


def is_excluded(title: str) -> bool:
    """자사/소노 언급 여부 검사"""
    return any(kw in title for kw in EXCLUDE_KEYWORDS)


def detect_region(title: str) -> str:
    """제목에서 권역 자동 감지"""
    for kw, region in REGION_MAP.items():
        if kw in title:
            return region
    return "general"


def detect_category_emoji(category: str) -> str:
    """카테고리별 이모지"""
    return {
        "호텔/리조트": "📰",
        "항공/공항": "✈️",
        "OTA/여행": "🌐",
        "관광/지역": "📰",
        "레저/휴양": "🏖️",
        "거시지표": "📊",
    }.get(category, "📰")


def load_existing_news() -> list:
    """기존 news_latest.json에서 이전 기사 목록 로드 (by_category 단일 소스, 제목 기준 dedup)"""
    if not OUTPUT_FILE.exists():
        return []
    try:
        data = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
        seen = set()
        existing = []
        # by_category만 단일 소스로 사용 (top_news/by_region은 동일 기사의 뷰이므로 제외)
        for cat_data in data.get("by_category", {}).values():
            if not isinstance(cat_data, dict):
                continue
            for art in cat_data.get("articles", []):
                if not isinstance(art, dict):
                    continue
                key = art.get("title", "")[:50]
                if key and key not in seen:
                    seen.add(key)
                    existing.append(art)
        return existing
    except (json.JSONDecodeError, KeyError):
        return []


def purge_old_articles(articles: list, max_hours: int = 48) -> list:
    """collected_at 기준 48시간 초과 기사 삭제"""
    now = datetime.now(KST)
    cutoff = now - timedelta(hours=max_hours)
    kept = []
    removed = 0
    for art in articles:
        collected = art.get("collected_at", "")
        if collected:
            try:
                dt = datetime.fromisoformat(collected)
                if dt < cutoff:
                    removed += 1
                    continue
            except (ValueError, TypeError):
                pass
        kept.append(art)
    if removed:
        logger.info(f"  🗑 {removed}건 삭제 (48시간 초과)")
    return kept


def is_new_article(collected_at: str) -> bool:
    """오늘 00:00~11:00 KST 사이에 수집된 기사인지 판별"""
    now = datetime.now(KST)
    try:
        dt = datetime.fromisoformat(collected_at)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_11am = now.replace(hour=11, minute=0, second=0, microsecond=0)
        return today_start <= dt <= today_11am
    except (ValueError, TypeError):
        return False


def main():
    logger.info("=" * 60)
    logger.info("뉴스 자동 수집 시작")
    logger.info("=" * 60)

    now = datetime.now(KST)
    now_iso = now.isoformat()

    # 기존 기사 로드 + 48시간 초과 삭제
    existing_articles = load_existing_news()
    existing_articles = purge_old_articles(existing_articles, max_hours=48)
    # 기존 기사 중 pub_date 기준 오래된 기사도 제거
    before_stale = len(existing_articles)
    existing_articles = [a for a in existing_articles if not is_stale_by_pub_date(a.get("pub_date", ""), now)]
    stale_removed = before_stale - len(existing_articles)
    if stale_removed:
        logger.info(f"  ⏳ 기존 기사 중 {stale_removed}건 제거 (pub_date 오래됨)")
    existing_titles = {art.get("title", "")[:50] for art in existing_articles}
    logger.info(f"기존 기사: {len(existing_articles)}건 (48시간 이내, 최신)")

    new_news = []
    seen_titles = set(existing_titles)

    for category, queries in KEYWORDS.items():
        logger.info(f"카테고리: {category} ({len(queries)}개 쿼리)")
        emoji = detect_category_emoji(category)

        for query in queries:
            news_items = fetch_google_news(query, limit=3)

            for item in news_items:
                title = item["title"]

                # 중복 제거 (기존 + 신규 모두)
                title_key = title[:50]
                if title_key in seen_titles:
                    continue
                seen_titles.add(title_key)

                # 자사 언급 제외
                if is_excluded(title):
                    logger.debug(f"  ❌ 제외 (자사): {title[:50]}")
                    continue

                # 오래된 기사 제외 (연도 불일치 또는 3개월 이전)
                if is_stale_by_pub_date(item.get("pub_date", ""), now):
                    logger.info(f"  ⏳ 제외 (오래된 기사): {title[:50]} | pub_date={item.get('pub_date','')}")
                    continue

                region = detect_region(title)

                new_news.append({
                    "title": title,
                    "link": item["link"],
                    "source": item["source"],
                    "pub_date": item["pub_date"],
                    "category": category,
                    "category_emoji": emoji,
                    "region": region,
                    "query": query,
                    "collected_at": now_iso,
                    "is_new": True,
                })

        logger.info(f"  → 신규 수집: {len(new_news)}건")

    # 기존 기사의 is_new 플래그 재계산 (오늘 00:00~11:00 수집분만 NEW)
    for art in existing_articles:
        art["is_new"] = is_new_article(art.get("collected_at", ""))

    # 신규 + 기존 병합
    all_news = new_news + existing_articles
    logger.info(f"전체 기사: {len(all_news)}건 (신규 {len(new_news)} + 기존 {len(existing_articles)})")

    # 권역별 정렬 (신규 먼저, 그 다음 권역순)
    region_order = {"vivaldi": 1, "central": 2, "south": 3, "apac": 4, "general": 5}
    all_news.sort(key=lambda x: (0 if x.get("is_new") else 1, region_order.get(x["region"], 9), x["category"]))

    # 권역별 그룹화 (각 권역 최대 8건)
    by_region = {"vivaldi": [], "central": [], "south": [], "apac": [], "general": []}
    for item in all_news:
        region = item["region"]
        if len(by_region[region]) < 8:
            by_region[region].append(item)

    # 상단 노출용 TOP 12 (신규 우선, 전체에서 골고루)
    top_news = []
    for region in ["vivaldi", "central", "south", "apac", "general"]:
        top_news.extend(by_region[region][:3])
    top_news = top_news[:12]

    # build.py 호환: 카테고리별 그룹화 (by_category)
    category_order = ["호텔/리조트", "OTA/여행", "종합여행사", "항공/공항", "관광/지역", "레저/휴양", "거시지표", "업계동향", "IT/플랫폼"]
    by_category: dict = {}
    for art in all_news:
        cat = art.get("category", "기타")
        if cat not in by_category:
            by_category[cat] = {"emoji": art.get("category_emoji", "📰"), "articles": []}
        entry = dict(art)
        entry["tag"] = art.get("query", cat)  # build.py의 [tag] 표시용
        by_category[cat]["articles"].append(entry)

    # build.py 호환: featured 뉴스 (신규 기사 중 상위 2건)
    featured_candidates = [a for a in all_news if a.get("is_new")][:4] or all_news[:4]
    featured = []
    for art in featured_candidates[:2]:
        featured.append({
            "headline": art.get("title", ""),
            "summary": art.get("title", ""),
            "source": art.get("source", ""),
            "link": art.get("link", "#"),
            "category": art.get("category", ""),
            "category_emoji": art.get("category_emoji", "📰"),
            "region": art.get("region", "general"),
            "tag": art.get("query", ""),
            "impact": "high" if art.get("is_new") else "medium",
            "is_new": art.get("is_new", False),
        })

    output = {
        "collected_at": now_iso,
        "total_count": len(all_news),
        "new_count": len(new_news),
        "top_news": top_news,
        "by_region": by_region,
        "by_category": by_category,
        "featured": featured,
        "categories": list(KEYWORDS.keys()),
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    logger.info("=" * 60)
    logger.info(f"✓ 뉴스 수집 완료: {len(all_news)}건 (신규 {len(new_news)}, TOP {len(top_news)} 선정)")
    logger.info(f"  권역별: 비발디={len(by_region['vivaldi'])}, 중부={len(by_region['central'])}, "
                f"남부={len(by_region['south'])}, APAC={len(by_region['apac'])}, 일반={len(by_region['general'])}")
    logger.info(f"  저장: {OUTPUT_FILE}")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"치명적 오류: {e}", exc_info=True)
        # 오류여도 빈 결과 저장 (다른 빌드 단계 진행 위함)
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "collected_at": datetime.now(KST).isoformat(),
                "total_count": 0,
                "top_news": [],
                "by_region": {"vivaldi": [], "central": [], "south": [], "apac": [], "general": []},
                "by_category": {},
                "featured": [],
                "error": str(e),
            }, f, ensure_ascii=False, indent=2)
        sys.exit(0)  # 빌드 계속 진행
