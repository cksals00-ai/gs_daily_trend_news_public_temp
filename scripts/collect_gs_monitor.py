#!/usr/bin/env python3
"""
GS Monitor 자동 수집기
============================================================
https://cksals00-ai.github.io/sono-competitor-crawler/ 에서
경쟁사 프로모션 데이터를 자동으로 가져와 competitors.json 생성

전략 (우선순위 순):
  1. 같은 organization의 sono-competitor-crawler 레포 raw JSON 직접 다운로드
     (가장 빠르고 정확함)
  2. GitHub Pages HTML 페이지 파싱
  3. 실패 시 기존 competitors.json 유지

© 2026 GS팀 · Haein Kim Manager
"""
import json
import logging
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
KST = timezone(timedelta(hours=9))

# GS Monitor 데이터 소스 후보 (우선순위 순)
GS_MONITOR_SOURCES = [
    # 1. JSON 데이터 파일이 노출된 경우 (가장 일반적인 GitHub Pages 정적 사이트 패턴)
    "https://cksals00-ai.github.io/sono-competitor-crawler/data/latest.json",
    "https://cksals00-ai.github.io/sono-competitor-crawler/data/competitors.json",
    "https://cksals00-ai.github.io/sono-competitor-crawler/data.json",
    "https://cksals00-ai.github.io/sono-competitor-crawler/competitors.json",
    
    # 2. GitHub raw - main 브랜치
    "https://raw.githubusercontent.com/cksals00-ai/sono-competitor-crawler/main/data/latest.json",
    "https://raw.githubusercontent.com/cksals00-ai/sono-competitor-crawler/main/data/competitors.json",
    "https://raw.githubusercontent.com/cksals00-ai/sono-competitor-crawler/main/data.json",
    
    # 3. HTML 페이지 자체 (파싱 시도)
    "https://cksals00-ai.github.io/sono-competitor-crawler/",
]

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
TIMEOUT = 15

# 권역 매핑 (브랜드명 키워드 → 권역)
REGION_MAP = {
    "한화": "vivaldi",
    "휘닉스": "central",
    "하이원": "central",
    "웰리힐리": "central",
    "강원": "central",
    "양양": "central",
    "삼척": "central",
    "단양": "central",
    "양평": "central",
    "여수": "south",
    "남해": "south",
    "거제": "south",
    "진도": "south",
    "경주": "south",
    "해운대": "south",
    "부산": "south",
    "히든클리프": "apac",
    "해비치": "apac",
    "신라": "apac",
    "롯데": "apac",  # 롯데는 부산/제주 모두 - 일단 apac로 (제주 위주로 가정)
    "그랜드하얏트": "apac",
    "파라다이스": "apac",
    "제주": "apac",
    "고양": "apac",
    "하와이": "apac",
    "괌": "apac",
    "하이퐁": "apac",
}


def detect_region(brand: str, title: str = "") -> str:
    """브랜드명/타이틀에서 권역 자동 탐지"""
    full_text = f"{brand} {title}"
    for keyword, region in REGION_MAP.items():
        if keyword in full_text:
            return region
    return "general"


def detect_threat(discount_pct: int) -> str:
    """할인율 기반 위협도"""
    if discount_pct >= 30:
        return "high"
    elif discount_pct >= 20:
        return "medium"
    return "low"


def fetch_url(url: str, timeout: int = TIMEOUT) -> tuple[bytes, dict]:
    """URL fetch (UA 설정 + 타임아웃)"""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read(), dict(resp.headers)


def try_json_endpoint(url: str) -> list:
    """JSON 엔드포인트 시도"""
    try:
        body, headers = fetch_url(url)
        content_type = headers.get("Content-Type", "")
        text = body.decode("utf-8", errors="ignore")
        
        # JSON 파싱 시도
        data = json.loads(text)
        
        # 다양한 형식 대응
        if isinstance(data, list):
            return data  # 직접 list
        elif isinstance(data, dict):
            # 일반적인 키 후보
            for key in ("competitors", "data", "results", "items", "promotions", "list"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            # dict 자체가 데이터인 경우
            if any(k in data for k in ("brand", "name", "title")):
                return [data]
        return []
    except (URLError, HTTPError, json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.debug(f"JSON 시도 실패 ({url}): {type(e).__name__}")
        return []


def parse_html_page(url: str) -> list:
    """HTML 페이지 파싱 (정적 GitHub Pages 사이트)"""
    try:
        body, _ = fetch_url(url)
        html = body.decode("utf-8", errors="ignore")
        
        # 패턴 1: <script>로 임베드된 JSON 데이터 찾기
        # 예: const competitors = [...];
        json_patterns = [
            r'const\s+competitors?\s*=\s*(\[[^;]+\])',
            r'window\.competitors?\s*=\s*(\[[^;]+\])',
            r'window\.__DATA__\s*=\s*(\{[^;]+\})',
            r'<script[^>]*type=["\']application/json["\'][^>]*>([^<]+)</script>',
            r'data-competitors\s*=\s*["\'](\[[^"\']+\])["\']',
        ]
        for pattern in json_patterns:
            matches = re.findall(pattern, html, re.DOTALL)
            for match in matches:
                try:
                    data = json.loads(match)
                    if isinstance(data, list) and data:
                        logger.info(f"✓ HTML 안 JSON 패턴 발견 ({len(data)}건)")
                        return data
                    elif isinstance(data, dict) and "competitors" in data:
                        return data["competitors"]
                except json.JSONDecodeError:
                    continue
        
        # 패턴 2: HTML 카드 구조 파싱
        # 예: <div class="competitor-card"> ... </div>
        cards = re.findall(
            r'<div[^>]*class=["\'][^"\']*(?:competitor|promotion|deal)[^"\']*["\'][^>]*>(.*?)</div>\s*</div>',
            html, re.DOTALL
        )
        if cards:
            logger.info(f"✓ HTML 카드 패턴 발견 ({len(cards)}건)")
            results = []
            for card in cards[:20]:
                # 브랜드명, 타이틀, 할인율 추출
                brand_m = re.search(r'<(?:h\d|strong)[^>]*>([^<]+)</', card)
                discount_m = re.search(r'-?(\d+)\s*%', card)
                if brand_m:
                    results.append({
                        "brand": brand_m.group(1).strip(),
                        "discount_pct": int(discount_m.group(1)) if discount_m else 0,
                        "raw_html": card[:500],
                    })
            return results
        
        return []
    except (URLError, HTTPError, UnicodeDecodeError) as e:
        logger.debug(f"HTML 시도 실패 ({url}): {type(e).__name__}")
        return []


def normalize_competitor(raw: dict, source_url: str = "") -> dict:
    """다양한 형식의 raw 데이터를 표준 형식으로 정규화"""
    brand = raw.get("brand") or raw.get("name") or raw.get("hotel") or raw.get("company") or ""
    title = raw.get("title") or raw.get("promotion") or raw.get("description") or raw.get("name") or ""
    
    # discount: 다양한 키 대응
    discount = raw.get("discount_pct") or raw.get("discount") or raw.get("discount_rate") or raw.get("max_discount") or 0
    if isinstance(discount, str):
        m = re.search(r'(\d+)', discount)
        discount = int(m.group(1)) if m else 0
    
    # 권역 자동 탐지
    region = raw.get("region") or detect_region(brand, title)
    
    # 위협도
    threat = raw.get("threat_level") or detect_threat(discount)
    
    # 링크
    link = raw.get("link") or raw.get("url") or raw.get("homepage") or "#"
    
    # 기간
    period = raw.get("period") or raw.get("date_range") or raw.get("validity") or "수집된 정보 참고"
    
    # 채널
    channel = raw.get("channel") or raw.get("channels") or raw.get("sales_channel") or "자사 + OTA"
    if isinstance(channel, list):
        channel = " + ".join(channel)
    
    # 상세
    detail = raw.get("detail") or raw.get("description") or raw.get("summary") or raw.get("note") or ""
    
    return {
        "brand": str(brand).strip(),
        "region": region,
        "title": str(title).strip()[:120],
        "period": str(period).strip()[:60],
        "discount_pct": int(discount),
        "channel": str(channel).strip()[:80],
        "detail": str(detail).strip()[:200],
        "link": str(link),
        "threat_level": threat,
    }


def main():
    logger.info("=" * 60)
    logger.info("GS Monitor 자동 수집 시작")
    logger.info("=" * 60)
    
    raw_competitors = []
    successful_source = None
    
    # 우선순위 순으로 시도
    for url in GS_MONITOR_SOURCES:
        logger.info(f"시도: {url}")
        
        if url.endswith(".json") or "raw.githubusercontent" in url:
            data = try_json_endpoint(url)
        else:
            data = parse_html_page(url)
        
        if data:
            logger.info(f"✓ 성공: {len(data)}건 수집")
            raw_competitors = data
            successful_source = url
            break
    
    # 데이터 정규화
    if raw_competitors:
        competitors = [normalize_competitor(c, successful_source) for c in raw_competitors]
        # 빈 brand 필터
        competitors = [c for c in competitors if c["brand"]]
        logger.info(f"✓ 정규화 완료: {len(competitors)}건")
    else:
        logger.warning("⚠ 모든 소스 실패. 기존 competitors.json 유지.")
        # 기존 파일 유지하고 종료
        existing_path = DATA_DIR / "competitors.json"
        if existing_path.exists():
            logger.info(f"✓ 기존 competitors.json 유지 ({existing_path})")
            sys.exit(0)
        else:
            logger.error("기존 파일도 없음. 빈 competitors.json 생성")
            competitors = []
    
    # 권역별 요약 생성
    from collections import Counter
    region_counts = Counter(c["region"] for c in competitors)
    summary = {}
    for region in ("vivaldi", "central", "south", "apac"):
        region_comps = [c for c in competitors if c["region"] == region]
        if region_comps:
            avg = sum(c["discount_pct"] for c in region_comps) / len(region_comps)
            top = max(region_comps, key=lambda x: x["discount_pct"])
            summary[region] = {
                "count": len(region_comps),
                "avg_discount": round(avg),
                "threat": f"{top['brand']} 등 {len(region_comps)}개사",
                "top_threat": f"{top['brand']} -{top['discount_pct']}%",
            }
        else:
            summary[region] = {
                "count": 0,
                "avg_discount": 0,
                "threat": "수집된 경쟁사 없음",
                "top_threat": "-",
            }
    
    # 저장
    now = datetime.now(KST)
    output = {
        "_instruction": "GS Monitor에서 자동 수집됨. 수기 수정 시 다음 자동 수집에서 덮어쓰여짐.",
        "_updated_at": now.strftime("%Y-%m-%d %H:%M:%S KST"),
        "_source": successful_source or "manual",
        "_auto_collected": True,
        "competitors": competitors,
        "summary_by_region": summary,
    }
    
    out_path = DATA_DIR / "competitors.json"
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    
    logger.info("=" * 60)
    logger.info(f"✓ 저장 완료: {out_path}")
    logger.info(f"  소스: {successful_source}")
    logger.info(f"  경쟁사: {len(competitors)}개")
    for region, s in summary.items():
        logger.info(f"  {region}: {s['count']}개 · 평균 -{s['avg_discount']}%")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
