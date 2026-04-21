#!/usr/bin/env python3
"""
GS Daily Trend Report V7 - HTML 빌드 (최종 통합본)
============================================================
실행 순서: news → competitors → insights → build

주요 기능:
  1. 3개월 KPI (4월 진하게, 5·6월 연하게) 주입
  2. 주요 OTA 실적 테이블 (객실수 + YoY)
  3. 경쟁사 동향 카드 + 링크
  4. 업계 뉴스 (Google News RSS)
  5. 오늘의 한 줄 + 권역 액션 (자동 생성)

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

# 3개월 기준
STAY_MONTHS = ["2026-04", "2026-05", "2026-06"]


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
    if new_text is None:
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
    return html_module.escape(str(text), quote=True)


# ─────────────────────────────────────────────
# KPI 주입 (3개월 병렬)
# ─────────────────────────────────────────────
def inject_kpi_3months(html: str, kpi_data: dict) -> str:
    """3개월 KPI 값 주입 (kpi1-m0, kpi1-m1, kpi1-m2, ...)"""
    for idx in (1, 2, 3):
        kpi = kpi_data.get(f"kpi_{idx}", {})
        html = apply_tpl(html, f"kpi{idx}-label", kpi.get("label", ""))
        
        stay_months = kpi.get("stay_months", {})
        for m_idx, month_key in enumerate(STAY_MONTHS):
            m_data = stay_months.get(month_key, {})
            html = apply_tpl(html, f"kpi{idx}-m{m_idx}-value", str(m_data.get("value", "-")))
            html = apply_tpl(html, f"kpi{idx}-m{m_idx}-unit", m_data.get("unit", ""))
            html = apply_tpl(html, f"kpi{idx}-m{m_idx}-delta", m_data.get("delta", ""))
    return html


# ─────────────────────────────────────────────
# 주요 OTA 테이블 렌더링
# ─────────────────────────────────────────────
def render_ota_rows(ota_data: dict) -> str:
    """주요 OTA 실적 테이블 tbody HTML 생성"""
    channels = ota_data.get("channels", [])
    if not channels:
        return '<tr><td colspan="9" style="padding:40px;text-align:center;color:var(--ink-faint);font-family:var(--mono);font-size:11px;">OTA 데이터 없음</td></tr>'
    
    rows_html = []
    for ch in channels:
        rank = ch.get("rank", "-")
        name = escape_html(ch.get("name", ""))
        tier = ch.get("tier", "")
        
        # Tier 색상
        tier_colors = {
            "글로벌": "#2d7a3f",
            "국내": "#b8332c",
            "신규": "#c9772c",
        }
        tier_color = tier_colors.get(tier, "#8a8a8a")
        
        # 월별 데이터
        def format_month(m_data, emphasis="secondary"):
            if not m_data:
                return "-", "-"
            rns = m_data.get("rns", 0)
            yoy = m_data.get("yoy_pct", 0)
            
            rns_str = f"{rns:,}"
            
            # YoY 색상 + 화살표
            if yoy > 0:
                yoy_color = "#2d7a3f"
                yoy_arrow = "▲"
            elif yoy < 0:
                yoy_color = "#b8332c"
                yoy_arrow = "▼"
            else:
                yoy_color = "#8a8a8a"
                yoy_arrow = "▬"
            
            yoy_str = f'<span style="color:{yoy_color};font-weight:700;">{yoy_arrow} {abs(yoy):.0f}%</span>'
            return rns_str, yoy_str
        
        # 4월 (진하게)
        m0_rns, m0_yoy = format_month(ch.get("2026-04"), "primary")
        # 5월, 6월 (연하게)
        m1_rns, m1_yoy = format_month(ch.get("2026-05"), "secondary")
        m2_rns, m2_yoy = format_month(ch.get("2026-06"), "secondary")
        
        row = f'''
        <tr style="border-bottom:1px solid var(--rule);transition:background 0.15s;" onmouseover="this.style.background='var(--bg-hover)'" onmouseout="this.style.background='transparent'">
          <td style="padding:12px 14px;font-family:var(--mono);font-size:11px;color:var(--ink-faint);font-weight:700;">#{rank:02d}</td>
          <td style="padding:12px 14px;font-family:var(--serif);font-size:15px;font-weight:700;color:var(--ink);">{name}</td>
          <td style="padding:12px 14px;text-align:center;"><span style="font-family:var(--mono);font-size:9px;padding:3px 8px;background:{tier_color}15;color:{tier_color};border-radius:2px;font-weight:700;letter-spacing:0.05em;">{tier}</span></td>
          <!-- APR (진하게) -->
          <td style="padding:12px 10px;text-align:right;font-family:var(--mono);font-size:14px;color:var(--ink);font-weight:800;background:rgba(184,137,63,0.04);border-left:2px solid var(--gold);">{m0_rns}</td>
          <td style="padding:12px 10px;text-align:right;font-family:var(--mono);font-size:11px;font-weight:700;background:rgba(184,137,63,0.04);">{m0_yoy}</td>
          <!-- MAY (연하게) -->
          <td style="padding:12px 10px;text-align:right;font-family:var(--mono);font-size:12px;color:var(--ink-muted);opacity:0.75;">{m1_rns}</td>
          <td style="padding:12px 10px;text-align:right;font-family:var(--mono);font-size:10px;opacity:0.75;">{m1_yoy}</td>
          <!-- JUN (연하게) -->
          <td style="padding:12px 10px;text-align:right;font-family:var(--mono);font-size:12px;color:var(--ink-muted);opacity:0.75;">{m2_rns}</td>
          <td style="padding:12px 10px;text-align:right;font-family:var(--mono);font-size:10px;opacity:0.75;">{m2_yoy}</td>
        </tr>'''
        rows_html.append(row)
    
    return "\n".join(rows_html)


def inject_ota_table(html: str, ota_data: dict) -> str:
    """OTA 테이블 tbody를 주입"""
    rows = render_ota_rows(ota_data)
    pattern = re.compile(
        r'(<tbody\s+data-tpl-major-ota-rows[^>]*>)(.*?)(</tbody>)',
        re.DOTALL
    )
    new_html, n = pattern.subn(
        lambda m: m.group(1) + rows + m.group(3),
        html,
        count=1
    )
    if n > 0:
        logger.info(f"✓ OTA 테이블 주입: {len(ota_data.get('channels', []))}개 채널")
    else:
        logger.warning("OTA 테이블 tbody 미발견")
    return new_html


# ─────────────────────────────────────────────
# 경쟁사 카드 렌더링
# ─────────────────────────────────────────────
def render_competitor_cards(comp_data: dict) -> str:
    """경쟁사 프로모션 카드 HTML 생성"""
    competitors = comp_data.get("competitors", [])
    if not competitors:
        return '<div style="padding:40px;text-align:center;color:var(--ink-faint);font-family:var(--mono);font-size:12px;grid-column:1/-1;">경쟁사 데이터 수집 중...</div>'
    
    # 권역별 색상
    region_colors = {
        "vivaldi": "#a83e4f",
        "central": "#2c5f7c",
        "south": "#2d7a3f",
        "apac": "#5c4a7c",
    }
    region_labels = {
        "vivaldi": "비발디",
        "central": "중부",
        "south": "남부",
        "apac": "APAC",
    }
    
    # 위협도 배지
    threat_styles = {
        "high": ("🔴 HIGH", "#b8332c", "#f5e7e6"),
        "medium": ("🟡 MED", "#c9772c", "#faf0e3"),
        "low": ("🟢 LOW", "#2d7a3f", "#e8f3eb"),
    }
    
    cards_html = []
    for comp in competitors:
        brand = escape_html(comp.get("brand", ""))
        title = escape_html(comp.get("title", ""))
        period = escape_html(comp.get("period", ""))
        discount = comp.get("discount_pct", 0)
        channel = escape_html(comp.get("channel", ""))
        detail = escape_html(comp.get("detail", ""))
        link = comp.get("link", "#")
        region = comp.get("region", "general")
        threat = comp.get("threat_level", "low")
        
        r_color = region_colors.get(region, "#8a8a8a")
        r_label = region_labels.get(region, region)
        t_label, t_color, t_bg = threat_styles.get(threat, ("-", "#8a8a8a", "#f0f0f0"))
        
        card = f'''
    <a href="{link}" target="_blank" rel="noopener noreferrer" style="text-decoration:none;color:inherit;display:block;background:var(--bg-card);border:1px solid var(--rule);border-left:3px solid {r_color};border-radius:0 4px 4px 0;padding:16px 18px;transition:all 0.2s;" onmouseover="this.style.boxShadow='0 8px 18px rgba(0,0,0,0.08)';this.style.transform='translateY(-2px)'" onmouseout="this.style.boxShadow='none';this.style.transform='translateY(0)'">
      <!-- 헤더 -->
      <div style="display:flex;justify-content:space-between;align-items:start;gap:8px;margin-bottom:8px;">
        <div>
          <div style="font-family:var(--mono);font-size:9px;letter-spacing:0.1em;color:{r_color};font-weight:700;margin-bottom:2px;">● {r_label}</div>
          <div style="font-family:var(--serif);font-size:16px;font-weight:800;color:var(--ink);">{brand}</div>
        </div>
        <div style="text-align:right;">
          <div style="font-family:var(--serif);font-size:22px;font-weight:800;color:var(--negative);line-height:1;">-{discount}%</div>
          <div style="font-family:var(--mono);font-size:8px;padding:2px 6px;background:{t_bg};color:{t_color};border-radius:2px;font-weight:700;margin-top:4px;display:inline-block;">{t_label}</div>
        </div>
      </div>
      
      <!-- 타이틀 -->
      <div style="font-family:var(--sans);font-size:13px;color:var(--ink-soft);line-height:1.5;margin-bottom:8px;min-height:40px;">{title}</div>
      
      <!-- 기간 -->
      <div style="font-family:var(--mono);font-size:10px;color:var(--ink-muted);margin-bottom:6px;">📅 {period}</div>
      
      <!-- 상세 -->
      <div style="font-family:var(--sans);font-size:11px;color:var(--ink-muted);line-height:1.4;padding:8px 10px;background:var(--bg-soft);border-radius:3px;margin-bottom:6px;">{detail}</div>
      
      <!-- 판매 채널 -->
      <div style="display:flex;justify-content:space-between;align-items:center;font-family:var(--mono);font-size:9px;color:var(--ink-faint);padding-top:6px;border-top:1px dashed var(--rule);">
        <span>{channel}</span>
        <span style="color:var(--gold);font-weight:700;">🔗 Link ↗</span>
      </div>
    </a>'''
        cards_html.append(card)
    
    return "\n".join(cards_html)


def inject_competitor_section(html: str, comp_data: dict) -> str:
    """경쟁사 카드 주입"""
    cards = render_competitor_cards(comp_data)
    pattern = re.compile(
        r'(<!-- COMP_INJECT_START -->)(.*?)(<!-- COMP_INJECT_END -->)',
        re.DOTALL
    )
    new_html, n = pattern.subn(
        lambda m: m.group(1) + "\n" + cards + "\n" + m.group(3),
        html,
        count=1
    )
    if n > 0:
        logger.info(f"✓ 경쟁사 카드 주입: {len(comp_data.get('competitors', []))}개")
    else:
        logger.warning("경쟁사 INJECT 마커 미발견")
    
    # 총 경쟁사 개수 + 평균 할인율 계산
    comps = comp_data.get("competitors", [])
    if comps:
        avg_discount = sum(c.get("discount_pct", 0) for c in comps) / len(comps)
        summary_text = f"총 {len(comps)}개 경쟁사 · 평균 할인율 {avg_discount:.0f}%"
        new_html = apply_tpl(new_html, "comp-count", summary_text)
    
    return new_html


# ─────────────────────────────────────────────
# 뉴스 섹션 렌더링
# ─────────────────────────────────────────────
def build_news_html(news_data: dict) -> str:
    top_news = news_data.get("top_news", [])
    if not top_news:
        return '<div style="padding:40px;text-align:center;color:var(--ink-faint);font-family:var(--mono);font-size:12px;grid-column:1/-1;">뉴스 수집 대기 중... (GitHub Actions에서 자동 수집)</div>'
    
    region_labels = {"vivaldi": "비발디", "central": "중부", "south": "남부", "apac": "APAC", "general": "일반"}
    region_colors = {"vivaldi": "#a83e4f", "central": "#2c5f7c", "south": "#2d7a3f", "apac": "#5c4a7c", "general": "#8a8a8a"}
    
    items_html = []
    for item in top_news[:20]:
        title = escape_html(item.get("title", ""))[:120]
        link = item.get("link", "#")
        source = escape_html(item.get("source", ""))
        category = item.get("category", "")
        emoji = item.get("category_emoji", "📰")
        region = item.get("region", "general")
        r_label = region_labels.get(region, region)
        r_color = region_colors.get(region, "#8a8a8a")
        
        items_html.append(f'''
    <a href="{link}" target="_blank" rel="noopener noreferrer" class="news-item" data-region="{region}" style="text-decoration:none;color:inherit;display:block;">
      <div class="news-item-cat" style="color:{r_color};">● {r_label} · {emoji} {category}</div>
      <p class="news-item-text">{title}</p>
      <div class="news-item-tags">
        <span class="news-tag">{source}</span>
        <span class="news-tag" style="background:{r_color}15;color:{r_color};">🔗 링크 ↗</span>
      </div>
    </a>''')
    
    return "\n".join(items_html)


def inject_news_section(html: str, news_data: dict) -> str:
    news_html = build_news_html(news_data)
    pattern = re.compile(
        r'(<!-- NEWS_INJECT_START -->)(.*?)(<!-- NEWS_INJECT_END -->)',
        re.DOTALL
    )
    new_html, n = pattern.subn(
        lambda m: m.group(1) + "\n" + news_html + "\n" + m.group(3),
        html,
        count=1
    )
    if n > 0:
        logger.info(f"✓ 뉴스 섹션 주입: {len(news_data.get('top_news', []))}건")
    
    # 권역별 카운트 주입
    by_region = news_data.get("by_region", {})
    top_news = news_data.get("top_news", [])
    
    # top_news에서 권역별 카운트
    region_counts = {"vivaldi": 0, "central": 0, "south": 0, "apac": 0, "general": 0}
    for item in top_news:
        r = item.get("region", "general")
        if r in region_counts:
            region_counts[r] += 1
    
    new_html = apply_tpl(new_html, "news-count-all", str(len(top_news)))
    for key in ("vivaldi", "central", "south", "apac"):
        new_html = apply_tpl(new_html, f"news-count-{key}", str(region_counts[key]))
    
    return new_html


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("V7 대시보드 빌드 (최종 통합)")
    logger.info("=" * 60)
    
    # 데이터 로드
    enriched = load_json(DATA_DIR / "enriched_notes.json")
    notes = load_json(DATA_DIR / "daily_notes.json")
    news_data = load_json(DATA_DIR / "news_latest.json")
    comp_data = load_json(DATA_DIR / "competitors.json")
    
    data = enriched if enriched else notes
    if not data:
        logger.error("데이터 없음")
        sys.exit(1)
    
    # HTML 로드
    if not HTML_FILE.exists():
        logger.error(f"HTML 템플릿 없음: {HTML_FILE}")
        sys.exit(1)
    
    html = HTML_FILE.read_text(encoding="utf-8")
    logger.info(f"✓ HTML 로드 ({len(html):,} bytes)")
    
    # 날짜
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
    
    # 오늘의 한 줄
    headline = data.get("today_headline", {})
    if headline.get("text"):
        html = apply_tpl(html, "headline", headline["text"])
    
    html = apply_tpl(html, "updated_by", f"by {data.get('_updated_by', 'GS팀 · Haein Kim Manager')}")
    
    # KPI (3개월 병렬)
    kpi_data = notes.get("executive_kpi", {})  # 원본 daily_notes에서 가져옴
    html = inject_kpi_3months(html, kpi_data)
    
    # 권역 상태
    regions = data.get("region_status", {})
    for key in ("vivaldi", "central", "south", "apac"):
        r = regions.get(key, {})
        html = apply_tpl(html, f"region-{key}-value", str(r.get("달성률", "")))
        html = apply_tpl(html, f"region-{key}-delta", r.get("메모", ""))
    
    # 액션 알림
    actions = data.get("action_alerts", {})
    for key in ("vivaldi", "central", "south", "apac"):
        text = actions.get(key, "")
        if text:
            html = apply_tpl(html, f"action-{key}", text)
    
    # OTA 테이블
    ota_data = notes.get("major_ota_performance", {})
    html = inject_ota_table(html, ota_data)
    
    # 경쟁사 카드
    html = inject_competitor_section(html, comp_data)
    
    # 뉴스
    html = inject_news_section(html, news_data)
    
    # 빌드 메타
    build_meta = now.strftime("Auto-Built %Y-%m-%d %H:%M KST")
    html = apply_tpl(html, "build", build_meta)
    
    # 저장
    HTML_FILE.write_text(html, encoding="utf-8")
    
    logger.info("=" * 60)
    logger.info(f"✓ 빌드 완료")
    logger.info(f"  크기: {len(html):,} bytes")
    logger.info(f"  시각: {build_meta}")
    logger.info(f"  OTA 채널: {len(ota_data.get('channels', []))}개")
    logger.info(f"  경쟁사: {len(comp_data.get('competitors', []))}개")
    logger.info(f"  뉴스: {len(news_data.get('top_news', []))}건")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
