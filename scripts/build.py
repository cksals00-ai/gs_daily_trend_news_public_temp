#!/usr/bin/env python3
"""
GS Daily Trend Report V7 - HTML 빌드 (최종 통합본)
============================================================
실행 순서: news → insights → build
  1. scripts/collect_news.py  → data/news_latest.json
  2. scripts/generate_insights.py → data/enriched_notes.json
  3. scripts/build.py (본 스크립트) → docs/index.html

© 2026 GS팀 · Haein Kim Manager
"""
import html as html_module
import json
import logging
import re
import sys
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
DOCS_DIR = ROOT / "docs"

HTML_FILE = DOCS_DIR / "index.html"
KST = timezone(timedelta(hours=9))


def load_json(path: Path, default=None):
    if not path.exists():
        logger.warning(f"파일 없음: {path.name}")
        return default or {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"JSON 파싱 실패 ({path.name}): {e}")
        return default or {}


def apply_tpl(html: str, selector: str, new_text: str) -> str:
    """data-tpl-XXX 속성을 가진 요소의 innerHTML을 교체."""
    if not new_text and new_text != "":
        return html
    pattern = re.compile(
        rf'(<[^>]*\bdata-tpl-{re.escape(selector)}\b[^>]*>)([^<]*?)(</)',
        re.DOTALL
    )
    new_html, n = pattern.subn(
        lambda m: m.group(1) + str(new_text) + m.group(3),
        html,
        count=1
    )
    if n == 0:
        logger.debug(f"템플릿 슬롯 '{selector}' 미발견")
    return new_html


def escape_html(text: str) -> str:
    """HTML 특수문자 이스케이프"""
    return html_module.escape(text, quote=True)


def build_news_html(news_data: dict) -> str:
    """뉴스 데이터 → HTML 섹션 생성"""
    top_news = news_data.get("top_news", [])
    
    if not top_news:
        return '<div style="padding:40px;text-align:center;color:var(--ink-faint);font-family:var(--mono);font-size:12px;">뉴스 수집 대기 중... (GitHub Actions에서 자동 수집)</div>'
    
    region_labels = {
        "vivaldi": "비발디",
        "central": "중부",
        "south": "남부",
        "apac": "APAC",
        "general": "일반",
    }
    region_colors = {
        "vivaldi": "#a83e4f",
        "central": "#2c5f7c",
        "south": "#2d7a3f",
        "apac": "#5c4a7c",
        "general": "#8a8a8a",
    }
    
    items_html = []
    for item in top_news[:12]:
        title = escape_html(item.get("title", ""))[:120]
        link = item.get("link", "#")
        source = escape_html(item.get("source", ""))
        category = item.get("category", "")
        emoji = item.get("category_emoji", "📰")
        region = item.get("region", "general")
        region_label = region_labels.get(region, region)
        region_color = region_colors.get(region, "#8a8a8a")
        
        items_html.append(f'''
    <a href="{link}" target="_blank" rel="noopener noreferrer" class="news-item" style="text-decoration:none;color:inherit;display:block;">
      <div class="news-item-cat" style="color:{region_color};">● {region_label} · {emoji} {category}</div>
      <p class="news-item-text">{title}</p>
      <div class="news-item-tags">
        <span class="news-tag">{source}</span>
      </div>
    </a>''')
    
    return "\n".join(items_html)


def inject_news_section(html: str, news_data: dict) -> str:
    """뉴스 섹션을 HTML에 주입"""
    news_html = build_news_html(news_data)
    
    # <!-- NEWS_INJECT_START --> ... <!-- NEWS_INJECT_END --> 구간을 치환
    pattern = re.compile(
        r'(<!-- NEWS_INJECT_START -->)(.*?)(<!-- NEWS_INJECT_END -->)',
        re.DOTALL
    )
    if pattern.search(html):
        html = pattern.sub(
            lambda m: m.group(1) + "\n" + news_html + "\n" + m.group(3),
            html
        )
        logger.info("✓ 뉴스 섹션 주입 완료")
    else:
        logger.warning("NEWS_INJECT_START/END 마커 미발견 - 뉴스 주입 스킵")
    
    return html


def main():
    logger.info("=" * 60)
    logger.info("V7 대시보드 빌드 (최종 통합)")
    logger.info("=" * 60)
    
    # 1. 데이터 로드
    enriched = load_json(DATA_DIR / "enriched_notes.json")
    notes = load_json(DATA_DIR / "daily_notes.json")
    news_data = load_json(DATA_DIR / "news_latest.json")
    
    # enriched가 없으면 notes 폴백
    data = enriched if enriched else notes
    if not data:
        logger.error("데이터 없음 - 빌드 중단")
        sys.exit(1)
    
    # 2. HTML 템플릿 로드
    if not HTML_FILE.exists():
        logger.error(f"HTML 템플릿 없음: {HTML_FILE}")
        sys.exit(1)
    
    html = HTML_FILE.read_text(encoding="utf-8")
    logger.info(f"✓ HTML 템플릿 로드 ({len(html):,} bytes)")
    
    # 3. 주입
    now = datetime.now(KST)
    day_map = {0: "MON", 1: "TUE", 2: "WED", 3: "THU", 4: "FRI", 5: "SAT", 6: "SUN"}
    
    report_date = data.get("report_date", now.strftime("%Y-%m-%d"))
    try:
        dt = datetime.strptime(report_date, "%Y-%m-%d")
        display_date = dt.strftime("%Y.%m.%d")
        timestamp = f"{display_date} {day_map[dt.weekday()]} 08:00 KST"
    except ValueError:
        display_date = report_date
        timestamp = now.strftime("%Y.%m.%d %H:%M KST")
    
    html = apply_tpl(html, "date", display_date)
    html = apply_tpl(html, "timestamp", timestamp)
    
    # 🔥 오늘의 한 줄 (자동 생성된 값)
    headline = data.get("today_headline", {})
    if headline.get("text"):
        html = apply_tpl(html, "headline", headline["text"])
    
    html = apply_tpl(html, "updated_by", f"by {data.get('_updated_by', 'GS팀 · Haein Kim Manager')}")
    
    # 임원 KPI 3개
    kpi = data.get("executive_kpi", {})
    for idx in (1, 2, 3):
        k = kpi.get(f"kpi_{idx}", {})
        html = apply_tpl(html, f"kpi{idx}-label", k.get("label", ""))
        html = apply_tpl(html, f"kpi{idx}-value", str(k.get("value", "")))
        html = apply_tpl(html, f"kpi{idx}-unit", k.get("unit", ""))
        html = apply_tpl(html, f"kpi{idx}-delta", k.get("delta", ""))
    
    # 권역 상태
    regions = data.get("region_status", {})
    for key in ("vivaldi", "central", "south", "apac"):
        r = regions.get(key, {})
        html = apply_tpl(html, f"region-{key}-value", str(r.get("달성률", "")))
        html = apply_tpl(html, f"region-{key}-delta", r.get("메모", ""))
    
    # 액션 알림 (자동 생성된 값)
    actions = data.get("action_alerts", {})
    for key in ("vivaldi", "central", "south", "apac"):
        text = actions.get(key, "")
        if text:
            html = apply_tpl(html, f"action-{key}", text)
    
    # 빌드 메타
    build_meta = now.strftime("Auto-Built %Y-%m-%d %H:%M KST")
    html = apply_tpl(html, "build", build_meta)
    
    # 4. 뉴스 섹션 주입
    html = inject_news_section(html, news_data)
    
    # 5. HTML 저장
    HTML_FILE.write_text(html, encoding="utf-8")
    
    logger.info("=" * 60)
    logger.info(f"✓ 빌드 완료: {HTML_FILE}")
    logger.info(f"  크기: {len(html):,} bytes")
    logger.info(f"  빌드 시각: {build_meta}")
    logger.info(f"  오늘의 한 줄: {headline.get('text', '')[:60]}...")
    logger.info(f"  뉴스: {len(news_data.get('top_news', []))}건 주입")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
