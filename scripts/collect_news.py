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

# 권역 매핑
REGION_MAP = {
    "비발디": "vivaldi",
    "한화리조트": "vivaldi",
    "휘닉스": "central",
    "하이원": "central",
    "강원": "central",
    "양평": "central",
    "양양": "central",
    "고양": "central",
    "제주": "south",
    "여수": "south",
    "남해": "south",
    "거제": "south",
    "신라호텔": "south",
    "롯데호텔": "south",
    "유류할증료": "apac",
    "환율": "apac",
    "유가": "apac",
    "국제선": "apac",
    "외국인": "apac",
    "인바운드": "apac",
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
        "호텔/리조트": "🏨",
        "항공/공항": "✈️",
        "OTA/여행": "🌐",
        "관광/지역": "🌸",
        "레저/휴양": "🏖️",
        "거시지표": "📊",
    }.get(category, "📰")


def main():
    logger.info("=" * 60)
    logger.info("뉴스 자동 수집 시작")
    logger.info("=" * 60)
    
    all_news = []
    seen_titles = set()
    
    for category, queries in KEYWORDS.items():
        logger.info(f"카테고리: {category} ({len(queries)}개 쿼리)")
        emoji = detect_category_emoji(category)
        
        for query in queries:
            news_items = fetch_google_news(query, limit=3)
            
            for item in news_items:
                title = item["title"]
                
                # 중복 제거
                title_key = title[:50]
                if title_key in seen_titles:
                    continue
                seen_titles.add(title_key)
                
                # 자사 언급 제외
                if is_excluded(title):
                    logger.debug(f"  ❌ 제외 (자사): {title[:50]}")
                    continue
                
                region = detect_region(title)
                
                all_news.append({
                    "title": title,
                    "link": item["link"],
                    "source": item["source"],
                    "pub_date": item["pub_date"],
                    "category": category,
                    "category_emoji": emoji,
                    "region": region,
                    "query": query,
                })
        
        logger.info(f"  → 누적 수집: {len(all_news)}건")
    
    # 권역별 정렬 (vivaldi → central → south → apac → general)
    region_order = {"vivaldi": 1, "central": 2, "south": 3, "apac": 4, "general": 5}
    all_news.sort(key=lambda x: (region_order.get(x["region"], 9), x["category"]))
    
    # 권역별 그룹화 (각 권역 최대 6건)
    by_region = {"vivaldi": [], "central": [], "south": [], "apac": [], "general": []}
    for item in all_news:
        region = item["region"]
        if len(by_region[region]) < 6:
            by_region[region].append(item)
    
    # 상단 노출용 TOP 12 (전체에서 골고루)
    top_news = []
    for region in ["vivaldi", "central", "south", "apac", "general"]:
        top_news.extend(by_region[region][:3])
    top_news = top_news[:12]
    
    output = {
        "collected_at": datetime.now(KST).isoformat(),
        "total_count": len(all_news),
        "top_news": top_news,
        "by_region": by_region,
        "categories": list(KEYWORDS.keys()),
    }
    
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    logger.info("=" * 60)
    logger.info(f"✓ 뉴스 수집 완료: {len(all_news)}건 (TOP {len(top_news)} 선정)")
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
                "error": str(e),
            }, f, ensure_ascii=False, indent=2)
        sys.exit(0)  # 빌드 계속 진행
