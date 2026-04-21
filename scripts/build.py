#!/usr/bin/env python3
"""
GS Daily Trend Report V7 - HTML 빌드 (최종 통합)
============================================================
주요 기능:
  1. 외부 링크 주입 (Power BI / GS Monitor)
  2. 3개월 KPI (4월 진하게, 5·6월 연하게)
  3. 주요 OTA TOP 4 테이블 (객실수 + YoY)
  4. 권역별 사업장 실적 매트릭스 (RNS/ADR/REV/달성률)
  5. 풍부한 뉴스 카드 (출처/요약/카테고리/링크 명확)
  6. 경쟁사 동향 카드
  7. 자동 생성 헤드라인 + 액션 알림

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
STAY_MONTHS = ["2026-04", "2026-05", "2026-06"]
MONTH_LABELS = {"2026-04": "4월", "2026-05": "5월", "2026-06": "6월"}


def load_json(path: Path, default=None):
    if not path.exists():
        return default or {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        logger.error(f"JSON 파싱 실패 ({path.name}): {e}")
        return default or {}


def apply_tpl(html: str, selector: str, new_text) -> str:
    if new_text is None:
        return html
    pattern = re.compile(
        rf'(<[^>]*\bdata-tpl-{re.escape(selector)}\b[^>]*>)([^<]*?)(</)',
        re.DOTALL
    )
    new_html, n = pattern.subn(
        lambda m: m.group(1) + str(new_text) + m.group(3),
        html, count=1
    )
    return new_html


def replace_block(html: str, attr_name: str, inner_html: str) -> str:
    """data-tpl-XXX 속성을 가진 div 안의 전체 내용을 통째 교체"""
    pattern = re.compile(
        rf'(<div\s+data-tpl-{re.escape(attr_name)}[^>]*>)(.*?)(</div>(?:\s*<!--[^-]*-->)?\s*(?=<h3|<div|<section|<!--))',
        re.DOTALL
    )
    new_html, n = pattern.subn(
        lambda m: m.group(1) + inner_html + '</div>',
        html, count=1
    )
    if n > 0:
        logger.info(f"✓ 블록 교체: {attr_name}")
    return new_html


def escape_html(text) -> str:
    return html_module.escape(str(text), quote=True)


# ─────────────────────────────────────────────
# 외부 링크 주입
# ─────────────────────────────────────────────
def inject_external_links(html: str, links: dict) -> str:
    bi = links.get("bi_dashboard", {})
    monitor = links.get("gs_monitor", {})
    palatium = links.get("palatium_dashboard", {})
    
    if bi.get("url"):
        html = re.sub(
            r'(<a[^>]*data-tpl-link-bi[^>]*href=")[^"]+(")',
            lambda m: m.group(1) + bi["url"] + m.group(2),
            html, count=1
        )
    if monitor.get("url"):
        html = re.sub(
            r'(<a[^>]*data-tpl-link-monitor[^>]*href=")[^"]+(")',
            lambda m: m.group(1) + monitor["url"] + m.group(2),
            html, count=1
        )
    if palatium.get("url"):
        html = re.sub(
            r'(<a[^>]*href=")[^"]+("[^>]*data-tpl-link-palatium)',
            lambda m: m.group(1) + palatium["url"] + m.group(2),
            html, count=1
        )
    logger.info(f"✓ 외부 링크 주입")
    return html


# ─────────────────────────────────────────────
# KPI 3개월 주입
# ─────────────────────────────────────────────
def inject_kpi_3months(html: str, kpi_data: dict) -> str:
    for idx in (1, 2, 3):
        kpi = kpi_data.get(f"kpi_{idx}", {})
        html = apply_tpl(html, f"kpi{idx}-label", kpi.get("label", ""))
        for m_idx, month_key in enumerate(STAY_MONTHS):
            m_data = kpi.get("stay_months", {}).get(month_key, {})
            html = apply_tpl(html, f"kpi{idx}-m{m_idx}-value", str(m_data.get("value", "-")))
            html = apply_tpl(html, f"kpi{idx}-m{m_idx}-unit", m_data.get("unit", ""))
            html = apply_tpl(html, f"kpi{idx}-m{m_idx}-delta", m_data.get("delta", ""))
    return html


# ─────────────────────────────────────────────
# OTA 테이블 (TOP 4)
# ─────────────────────────────────────────────
def render_ota_rows(ota_data: dict) -> str:
    channels = ota_data.get("channels", [])[:4]  # TOP 4만
    if not channels:
        return '''<tr>
          <td colspan="9" style="padding:60px 40px;text-align:center;">
            <div style="font-family:'Pretendard Variable',sans-serif;font-size:14px;color:var(--ink-muted);font-weight:600;margin-bottom:6px;">⏳ 데이터 추후 적용</div>
            <div style="font-family:'Pretendard Variable',sans-serif;font-size:12px;color:var(--ink-faint);">Power BI 자동 수집 작업 진행 중 · 완료 후 자동 표시</div>
          </td>
        </tr>'''
    
    rows = []
    for ch in channels:
        rank = ch.get("rank", "-")
        name = escape_html(ch.get("name", ""))
        tier = ch.get("tier", "")
        # tier 변환: 글로벌→GOTA, 국내→OTA
        tier_display = {"글로벌": "GOTA", "국내": "OTA", "신규": "NEW"}.get(tier, tier)
        tier_colors = {"글로벌": "#6db58a", "국내": "#d97a7a", "신규": "#e6b970"}
        tier_color = tier_colors.get(tier, "#8a8a8a")
        
        def fmt_month(m_data):
            if not m_data:
                return "-", "-"
            rns = m_data.get("rns", 0)
            yoy = m_data.get("yoy_pct", 0)
            rns_str = f"{rns:,}"
            if yoy > 0:
                yoy_str = f'<span style="color:#2d7a3f;font-weight:700;">▲ {abs(yoy):.0f}%</span>'
            elif yoy < 0:
                yoy_str = f'<span style="color:#b8332c;font-weight:700;">▼ {abs(yoy):.0f}%</span>'
            else:
                yoy_str = f'<span style="color:#8a8a8a;font-weight:700;">▬ 0%</span>'
            return rns_str, yoy_str
        
        m0_rns, m0_yoy = fmt_month(ch.get("2026-04"))
        m1_rns, m1_yoy = fmt_month(ch.get("2026-05"))
        m2_rns, m2_yoy = fmt_month(ch.get("2026-06"))
        
        rows.append(f'''
        <tr style="border-bottom:1px solid var(--rule);transition:background 0.15s;" 
            onmouseover="this.style.background='var(--bg-hover)'" 
            onmouseout="this.style.background='transparent'">
          <td style="padding:14px 14px;font-family:var(--mono);font-size:11px;color:var(--ink-faint);font-weight:700;">#{rank:02d}</td>
          <td style="padding:14px 14px;font-family:var(--serif);font-size:16px;font-weight:700;color:var(--ink);">{name}</td>
          <td style="padding:14px 14px;text-align:center;"><span style="font-family:var(--mono);font-size:10px;padding:4px 10px;background:{tier_color}20;color:{tier_color};border-radius:3px;font-weight:800;letter-spacing:0.08em;border:1px solid {tier_color}40;">{tier_display}</span></td>
          <td style="padding:14px 10px;text-align:right;font-family:var(--mono);font-size:15px;color:var(--ink);font-weight:800;background:rgba(184,137,63,0.04);border-left:2px solid var(--gold);">{m0_rns}</td>
          <td style="padding:14px 10px;text-align:right;font-family:var(--mono);font-size:11px;font-weight:700;background:rgba(184,137,63,0.04);">{m0_yoy}</td>
          <td style="padding:14px 10px;text-align:right;font-family:var(--mono);font-size:13px;color:var(--ink-muted);opacity:0.75;">{m1_rns}</td>
          <td style="padding:14px 10px;text-align:right;font-family:var(--mono);font-size:10px;opacity:0.75;">{m1_yoy}</td>
          <td style="padding:14px 10px;text-align:right;font-family:var(--mono);font-size:13px;color:var(--ink-muted);opacity:0.75;">{m2_rns}</td>
          <td style="padding:14px 10px;text-align:right;font-family:var(--mono);font-size:10px;opacity:0.75;">{m2_yoy}</td>
        </tr>''')
    
    return "\n".join(rows)


def inject_ota_table(html: str, ota_data: dict) -> str:
    rows = render_ota_rows(ota_data)
    pattern = re.compile(
        r'(<tbody\s+data-tpl-major-ota-rows[^>]*>)(.*?)(</tbody>)',
        re.DOTALL
    )
    new_html, n = pattern.subn(
        lambda m: m.group(1) + rows + m.group(3),
        html, count=1
    )
    if n > 0:
        logger.info(f"✓ OTA 테이블 주입: TOP {len(ota_data.get('channels', [])[:4])}")
    return new_html


# ─────────────────────────────────────────────
# 사업장별 실적 매트릭스 (NEW)
# ─────────────────────────────────────────────
def render_property_matrix(properties: list, region_color: str) -> str:
    """사업장별 4/5/6월 RNS·ADR·REV·달성률 매트릭스 카드"""
    if not properties:
        return '<div style="padding:30px;text-align:center;color:var(--ink-faint);font-family:var(--mono);font-size:11px;">사업장 데이터 없음</div>'
    
    cards = []
    for prop in properties:
        name = escape_html(prop.get("name", ""))
        
        # 각 월 데이터 추출
        months_html = []
        for m_idx, month_key in enumerate(STAY_MONTHS):
            m = prop.get(month_key, {})
            rns = m.get("rns", 0)
            adr = m.get("adr", 0)
            rev = m.get("rev", 0)
            ach = m.get("achievement", 0)
            
            # 달성률 색상
            if ach >= 100:
                ach_color = "#2d7a3f"
                ach_bg = "#e8f3eb"
                ach_icon = "▲"
            elif ach >= 85:
                ach_color = "#c9772c"
                ach_bg = "#faf0e3"
                ach_icon = "▬"
            else:
                ach_color = "#b8332c"
                ach_bg = "#f5e7e6"
                ach_icon = "▼"
            
            # 4월 진하게, 5·6월 연하게
            opacity = "1" if m_idx == 0 else "0.55"
            border = f"border-right:1px solid var(--rule);" if m_idx < 2 else ""
            month_label = MONTH_LABELS[month_key]
            is_primary = m_idx == 0
            
            # 달성률 바 (가로 진행률)
            ach_bar_width = min(ach, 130)
            
            months_html.append(f'''
        <div style="padding:12px 14px;{border}opacity:{opacity};">
          <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px;">
            <div style="font-family:var(--mono);font-size:10px;color:var(--ink-muted);font-weight:{'800' if is_primary else '500'};letter-spacing:0.1em;">
              {month_label} {'· 당월' if is_primary else '· 전망'}
            </div>
            <div style="font-family:var(--serif);font-size:{'18px' if is_primary else '14px'};font-weight:{'800' if is_primary else '600'};color:{ach_color};">
              {ach:.1f}%
            </div>
          </div>
          
          <!-- 달성률 바 -->
          <div style="height:4px;background:var(--bg-soft);border-radius:2px;margin-bottom:8px;overflow:hidden;">
            <div style="height:100%;width:{min(ach_bar_width, 100)}%;background:{ach_color};"></div>
          </div>
          
          <!-- 3개 지표 -->
          <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;font-size:10px;">
            <div>
              <div style="font-family:var(--mono);font-size:8px;color:var(--ink-faint);margin-bottom:1px;letter-spacing:0.05em;">RNS</div>
              <div style="font-family:var(--mono);font-size:{'13px' if is_primary else '11px'};font-weight:{'800' if is_primary else '500'};color:var(--ink);">{rns:,}</div>
            </div>
            <div>
              <div style="font-family:var(--mono);font-size:8px;color:var(--ink-faint);margin-bottom:1px;letter-spacing:0.05em;">ADR</div>
              <div style="font-family:var(--mono);font-size:{'13px' if is_primary else '11px'};font-weight:{'800' if is_primary else '500'};color:var(--ink);">₩{adr}K</div>
            </div>
            <div>
              <div style="font-family:var(--mono);font-size:8px;color:var(--ink-faint);margin-bottom:1px;letter-spacing:0.05em;">REV</div>
              <div style="font-family:var(--mono);font-size:{'13px' if is_primary else '11px'};font-weight:{'800' if is_primary else '500'};color:var(--ink);">₩{rev}M</div>
            </div>
          </div>
        </div>''')
        
        cards.append(f'''
    <div style="background:var(--bg-card);border:1px solid var(--rule);border-left:4px solid {region_color};border-radius:0 4px 4px 0;overflow:hidden;">
      <!-- 사업장명 -->
      <div style="padding:14px 16px 10px;background:linear-gradient(to right, {region_color}08, transparent);border-bottom:1px solid var(--rule);">
        <div style="font-family:var(--serif);font-size:16px;font-weight:800;color:var(--ink);">{name}</div>
      </div>
      <!-- 3개월 -->
      <div style="display:grid;grid-template-columns:1.3fr 1fr 1fr;">
        {''.join(months_html)}
      </div>
    </div>''')
    
    return f'''
    <!-- 범례 -->
    <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:var(--bg-soft);border-radius:4px;margin-bottom:12px;font-family:var(--mono);font-size:10px;color:var(--ink-muted);flex-wrap:wrap;gap:10px;">
      <div>
        <strong style="color:var(--ink);">📊 RNS</strong> 객실수 · 
        <strong style="color:var(--ink);">ADR</strong> 평균객단가(천원) · 
        <strong style="color:var(--ink);">REV</strong> 매출(백만원) · 
        <strong style="color:var(--ink);">달성률</strong> = 실적/목표
      </div>
      <div style="display:flex;gap:12px;">
        <span><span style="color:#2d7a3f;">●</span> 호조(≥100%)</span>
        <span><span style="color:#c9772c;">●</span> 보통(85~99%)</span>
        <span><span style="color:#b8332c;">●</span> 부진(&lt;85%)</span>
      </div>
    </div>
    
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:12px;">
      {''.join(cards)}
    </div>
    '''


def inject_property_matrix(html: str, prop_data: dict) -> str:
    """권역별 사업장 실적 매트릭스 주입 (구버전 - 매트릭스 UI 삭제됨, 사용 안 함)"""
    return html


def inject_signal_cards(html: str, prop_data: dict) -> str:
    """
    BI 자동 수집 데이터로 사업장 신호등 카드 자동 생성
    → Power BI 성공 시 '데이터 추후 적용' → 실데이터로 교체
    """
    # _status 체크
    status = prop_data.get("_status", "")
    if status != "auto_synced":
        logger.info(f"  신호등 주입 스킵 (status={status} - '데이터 추후 적용' 유지)")
        return html
    
    import re
    updated_regions = 0
    
    for region_key in ("vivaldi", "central", "south", "apac"):
        properties = prop_data.get(region_key, [])
        if not properties:
            continue
        
        # 신호등 카드 HTML 생성
        cards_html = []
        for prop in properties:
            name = escape_html(prop.get("name", ""))
            m4 = prop.get("2026-04", {})
            achievement = m4.get("achievement", 0)
            yoy_pct = m4.get("yoy_pct", 0)
            
            # 달성률에 따른 색상
            if achievement >= 100:
                dot_color = "green"
                value_class = "up"
                value_color = "var(--positive)"
            elif achievement >= 85:
                dot_color = "yellow"
                value_class = ""
                value_color = "var(--warning)"
            else:
                dot_color = "red"
                value_class = "down"
                value_color = "var(--negative)"
            
            # YoY 표시
            if yoy_pct > 0:
                yoy_text = f"▲ 전년比 +{yoy_pct:.1f}%"
                yoy_color = "var(--positive)"
            elif yoy_pct < 0:
                yoy_text = f"▼ 전년比 {yoy_pct:.1f}%"
                yoy_color = "var(--negative)"
            else:
                yoy_text = "▬ 전년比 ±0%"
                yoy_color = "var(--ink-faint)"
            
            cards_html.append(
                f'    <div class="signal-card">\n'
                f'      <div class="signal-dot {dot_color}"></div>\n'
                f'      <div>\n'
                f'        <div class="signal-name">{name}</div>\n'
                f'        <div class="signal-sub" style="font-family:\'Pretendard Variable\',sans-serif;font-weight:700;font-size:12px;color:var(--ink-muted);">4월 달성 {achievement:.1f}%</div>\n'
                f'        <div style="font-family:\'JetBrains Mono\',monospace;font-size:11px;font-weight:700;margin-top:2px;color:{yoy_color};">{yoy_text}</div>\n'
                f'      </div>\n'
                f'      <div class="signal-value {value_class}">{achievement:.1f}%</div>\n'
                f'    </div>'
            )
        
        cards_block = "\n".join(cards_html)
        
        # 마커 사이 영역 교체
        pattern = re.compile(
            f'(<!-- SIGNAL_INJECT_START_{region_key} -->)(.*?)(<!-- SIGNAL_INJECT_END_{region_key} -->)',
            re.DOTALL
        )
        new_html, n = pattern.subn(
            lambda m: m.group(1) + "\n" + cards_block + "\n    " + m.group(3),
            html, count=1
        )
        if n > 0:
            html = new_html
            updated_regions += 1
            logger.info(f"  ✓ {region_key}: {len(properties)}개 신호등 주입")
    
    if updated_regions > 0:
        logger.info(f"✓ BI 자동 수집 데이터로 사업장 신호등 {updated_regions}개 권역 갱신")
    
    return html


def _LEGACY_inject_property_matrix(html: str, prop_data: dict) -> str:
    for region_key, color in region_colors.items():
        properties = prop_data.get(region_key, [])
        matrix_html = render_property_matrix(properties, color)
        
        # data-tpl-property-matrix-XXX 안의 내용 교체
        pattern = re.compile(
            rf'(<div\s+data-tpl-property-matrix-{region_key}[^>]*>)(.*?)(</div>\s*(?=\s*<h3|\s*<!--))',
            re.DOTALL
        )
        new_html, n = pattern.subn(
            lambda m: m.group(1) + matrix_html + '</div>',
            html, count=1
        )
        if n > 0:
            html = new_html
            logger.info(f"✓ {region_key} 사업장 실적 매트릭스 주입: {len(properties)}개")
        else:
            logger.warning(f"✗ {region_key} 매트릭스 마커 미발견")
    
    return html


# ─────────────────────────────────────────────
# 경쟁사 카드
# ─────────────────────────────────────────────
def render_competitor_cards(comp_data: dict) -> str:
    competitors = comp_data.get("competitors", [])
    if not competitors:
        return '<div style="padding:40px;text-align:center;color:var(--ink-faint);grid-column:1/-1;">경쟁사 데이터 없음</div>'
    
    region_colors = {"vivaldi": "#a83e4f", "central": "#2c5f7c", "south": "#2d7a3f", "apac": "#5c4a7c"}
    region_labels = {"vivaldi": "비발디", "central": "중부", "south": "남부", "apac": "APAC"}
    threat_styles = {
        "high": ("🔴 HIGH", "#b8332c", "#f5e7e6"),
        "medium": ("🟡 MED", "#c9772c", "#faf0e3"),
        "low": ("🟢 LOW", "#2d7a3f", "#e8f3eb"),
    }
    
    cards = []
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
        
        cards.append(f'''
    <a href="{link}" target="_blank" rel="noopener noreferrer" 
       style="text-decoration:none;color:inherit;display:block;background:var(--bg-card);border:1px solid var(--rule);border-left:3px solid {r_color};border-radius:0 4px 4px 0;padding:16px 18px;transition:all 0.2s;"
       onmouseover="this.style.boxShadow='0 8px 18px rgba(0,0,0,0.4)';this.style.transform='translateY(-2px)'" 
       onmouseout="this.style.boxShadow='none';this.style.transform='translateY(0)'">
      <!-- 헤더: 권역 + 브랜드 + 할인율 -->
      <div style="display:flex;justify-content:space-between;align-items:start;gap:8px;margin-bottom:10px;">
        <div style="flex:1;">
          <div style="font-family:'Noto Sans KR',sans-serif;font-size:11px;letter-spacing:0.1em;color:{r_color};font-weight:800;margin-bottom:3px;">● {r_label}</div>
          <div style="font-family:'Noto Sans KR',sans-serif;font-size:17px;font-weight:800;color:var(--ink);line-height:1.3;">{brand}</div>
        </div>
        <!-- 할인율 박스 (의미 명확하게) -->
        <div style="text-align:center;background:rgba(229,115,115,0.12);border:1px solid rgba(229,115,115,0.4);padding:6px 10px;border-radius:4px;min-width:80px;">
          <div style="font-family:'JetBrains Mono',monospace;font-size:9px;color:var(--negative);letter-spacing:0.1em;font-weight:700;margin-bottom:2px;">최대 할인율</div>
          <div style="font-family:'Noto Sans KR',sans-serif;font-size:22px;font-weight:900;color:var(--negative);line-height:1;">{discount}<span style="font-size:13px;font-weight:700;">%</span></div>
          <div style="font-family:'JetBrains Mono',monospace;font-size:8px;padding:2px 5px;background:{t_bg};color:{t_color};border-radius:2px;font-weight:700;margin-top:4px;display:inline-block;">{t_label}</div>
        </div>
      </div>
      
      <!-- 프로모션 제목 -->
      <div style="font-family:'Noto Sans KR',sans-serif;font-size:13.5px;color:var(--ink-soft);line-height:1.5;margin-bottom:10px;font-weight:600;">
        🎯 {title}
      </div>
      
      <!-- 기간 -->
      <div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--ink-muted);margin-bottom:8px;font-weight:700;">
        📅 {period}
      </div>
      
      <!-- 상세 내용 -->
      <div style="font-family:'Noto Sans KR',sans-serif;font-size:12px;color:var(--ink-muted);line-height:1.5;padding:8px 12px;background:var(--bg-soft);border-radius:3px;margin-bottom:8px;font-weight:500;">
        💡 {detail}
      </div>
      
      <!-- 푸터: 채널 + 링크 -->
      <div style="display:flex;justify-content:space-between;align-items:center;font-family:'JetBrains Mono',monospace;font-size:10px;color:var(--ink-faint);padding-top:8px;border-top:1px dashed var(--rule);font-weight:700;">
        <span>📡 {channel}</span>
        <span style="color:var(--gold);font-weight:800;">🔗 자사 사이트 ↗</span>
      </div>
    </a>''')
    return "\n".join(cards)


def inject_competitor_section(html: str, comp_data: dict) -> str:
    cards = render_competitor_cards(comp_data)
    pattern = re.compile(
        r'(<!-- COMP_INJECT_START -->)(.*?)(<!-- COMP_INJECT_END -->)',
        re.DOTALL
    )
    new_html, n = pattern.subn(
        lambda m: m.group(1) + "\n" + cards + "\n" + m.group(3),
        html, count=1
    )
    if n > 0:
        logger.info(f"✓ 경쟁사 카드 주입: {len(comp_data.get('competitors', []))}개")
    
    comps = comp_data.get("competitors", [])
    if comps:
        avg = sum(c.get("discount_pct", 0) for c in comps) / len(comps)
        new_html = apply_tpl(new_html, "comp-count", f"총 {len(comps)}개 경쟁사 · 평균 할인율 {avg:.0f}%")
    
    return new_html


# ─────────────────────────────────────────────
# 뉴스 카드 (가독성 개선)
# ─────────────────────────────────────────────
def build_news_html(news_data: dict) -> str:
    """Daily News Monitoring 형식 - 카테고리별 그룹 + Articles 카운트"""
    by_category = news_data.get("by_category", {})
    if not by_category:
        return '<div style="padding:40px;text-align:center;color:var(--ink-faint);font-family:\'Pretendard Variable\',sans-serif;">카테고리별 뉴스 데이터 없음</div>'
    
    region_colors = {"vivaldi": "#d97a7a", "central": "#6ba3c4", "south": "#6db58a", "apac": "#a892c8", "general": "#7a7e85"}
    region_labels = {"vivaldi": "비발디", "central": "중부", "south": "남부", "apac": "APAC", "general": "일반"}
    
    # 카테고리 순서
    category_order = ["호텔/리조트", "OTA/여행", "종합여행사", "항공/공항", "관광/지역", "레저/휴양", "거시지표", "업계동향", "IT/플랫폼"]
    
    sections = []
    for cat_name in category_order:
        if cat_name not in by_category:
            continue
        cat_data = by_category[cat_name]
        emoji = cat_data.get("emoji", "📰")
        articles = cat_data.get("articles", [])
        if not articles:
            continue
        
        # 카테고리 헤더
        article_count = len(articles)
        section_html = f'''
  <!-- ===== {cat_name} ===== -->
  <div class="news-category-section" style="margin-bottom:24px;">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:var(--bg-soft);border-left:3px solid var(--gold);border-radius:0 4px 4px 0;margin-bottom:6px;">
      <div style="display:flex;align-items:center;gap:8px;">
        <span style="font-size:18px;">{emoji}</span>
        <span style="font-family:'Pretendard Variable',sans-serif;font-size:15px;font-weight:900;color:var(--ink);letter-spacing:-0.01em;">{escape_html(cat_name)}</span>
      </div>
      <span style="font-family:'Pretendard Variable',sans-serif;font-size:11px;color:var(--gold);font-weight:800;letter-spacing:0.05em;">{article_count} Articles</span>
    </div>
    <div>'''
        
        for art in articles:
            tag = escape_html(art.get("tag", ""))
            title = escape_html(art.get("title", ""))
            source = escape_html(art.get("source", ""))
            link = art.get("link", "#")
            region = art.get("region", "general")
            r_color = region_colors.get(region, "#7a7e85")
            r_label = region_labels.get(region, region)
            is_new = art.get("is_new", False)
            new_badge = '<span style="font-family:var(--mono);font-size:9px;padding:2px 6px;background:#ff4757;color:#fff;border-radius:2px;font-weight:900;letter-spacing:0.08em;animation:pulse-new 2s ease-in-out infinite;">NEW</span>' if is_new else ''

            section_html += f'''
      <a href="{link}" target="_blank" rel="noopener noreferrer" class="news-item" data-region="{region}"
         style="text-decoration:none;color:inherit;display:block;padding:12px 14px;border-bottom:1px solid var(--rule);transition:all 0.15s;"
         onmouseover="this.style.background='var(--bg-soft)';this.style.paddingLeft='18px'"
         onmouseout="this.style.background='transparent';this.style.paddingLeft='14px'">
        <div style="display:flex;justify-content:space-between;align-items:start;gap:14px;">
          <div style="flex:1;min-width:0;">
            <div style="display:flex;align-items:center;gap:6px;margin-bottom:4px;flex-wrap:wrap;">
              <span style="font-family:'Pretendard Variable',sans-serif;font-size:9.5px;padding:2px 7px;background:{r_color}22;color:{r_color};border-radius:2px;font-weight:800;letter-spacing:0.05em;">{r_label}</span>
              {new_badge}
              <span style="font-family:'Pretendard Variable',sans-serif;font-size:10px;color:var(--ink-faint);font-weight:700;">[{tag}]</span>
            </div>
            <div style="font-family:'Pretendard Variable',sans-serif;font-size:14px;font-weight:700;color:var(--ink-soft);line-height:1.5;letter-spacing:-0.01em;">{title}</div>
          </div>
          <div style="text-align:right;flex-shrink:0;display:flex;flex-direction:column;align-items:flex-end;gap:4px;min-width:80px;">
            <span style="font-family:'Pretendard Variable',sans-serif;font-size:11px;color:var(--gold);font-weight:700;">{source}</span>
            <span style="font-family:'JetBrains Mono',monospace;font-size:10px;color:var(--ink-faint);">↗</span>
          </div>
        </div>
      </a>'''
        
        section_html += '\n    </div>\n  </div>'
        sections.append(section_html)
    
    return "\n".join(sections)


def render_featured_news(featured_list: list) -> str:
    """오늘의 주요기사 - 큰 카드 2개"""
    if not featured_list:
        return '<div style="padding:30px;text-align:center;color:var(--ink-faint);grid-column:1/-1;">Featured 뉴스 없음</div>'
    
    region_colors = {"vivaldi": "#d97a7a", "central": "#6ba3c4", "south": "#6db58a", "apac": "#a892c8", "general": "#c9a063"}
    impact_colors = {"high": "var(--negative)", "medium": "var(--warning)", "low": "var(--positive)"}
    impact_labels = {"high": "🔴 HIGH IMPACT", "medium": "🟡 MED", "low": "🟢 LOW"}
    
    cards = []
    for item in featured_list[:2]:
        headline = escape_html(item.get("headline", ""))
        summary = escape_html(item.get("summary", ""))[:200]
        source = escape_html(item.get("source", ""))
        link = item.get("link", "#")
        category = escape_html(item.get("category", ""))
        emoji = item.get("category_emoji", "📰")
        region = item.get("region", "general")
        tag = escape_html(item.get("tag", ""))
        image_emoji = item.get("image_emoji", "📰")
        impact = item.get("impact", "medium")
        is_new = item.get("is_new", False)
        r_color = region_colors.get(region, "#c9a063")
        i_color = impact_colors.get(impact, "var(--warning)")
        i_label = impact_labels.get(impact, "")
        new_badge_feat = '<span style="font-family:var(--mono);font-size:9px;padding:3px 7px;background:#ff4757;color:#fff;border-radius:2px;font-weight:900;letter-spacing:0.08em;animation:pulse-new 2s ease-in-out infinite;">NEW</span>' if is_new else ''
        
        cards.append(f'''
    <a href="{link}" target="_blank" rel="noopener noreferrer" class="news-item" data-region="{region}" 
       style="text-decoration:none;color:inherit;display:flex;background:var(--bg-card);border:1px solid var(--rule);border-left:4px solid {r_color};border-radius:0 6px 6px 0;overflow:hidden;transition:all 0.2s;"
       onmouseover="this.style.boxShadow='0 12px 24px rgba(0,0,0,0.4)';this.style.transform='translateY(-2px)'"
       onmouseout="this.style.boxShadow='none';this.style.transform='translateY(0)'">
      
      <!-- 이미지 영역 (이모지 placeholder) -->
      <div style="flex-shrink:0;width:130px;background:linear-gradient(135deg,{r_color}25,{r_color}08);display:flex;align-items:center;justify-content:center;font-size:60px;border-right:1px solid var(--rule);">
        {image_emoji}
      </div>
      
      <!-- 본문 -->
      <div style="flex:1;padding:16px 20px;display:flex;flex-direction:column;justify-content:space-between;min-width:0;">
        <div>
          <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;flex-wrap:wrap;">
            <span style="font-family:'Pretendard Variable',sans-serif;font-size:10px;padding:3px 8px;background:{r_color};color:#0d0d0d;border-radius:2px;font-weight:900;letter-spacing:0.05em;">{emoji} {category}</span>
            <span style="font-family:'Pretendard Variable',sans-serif;font-size:9px;padding:3px 7px;background:{i_color}22;color:{i_color};border-radius:2px;font-weight:800;letter-spacing:0.05em;">{i_label}</span>
            <span style="font-family:'Pretendard Variable',sans-serif;font-size:10px;color:var(--ink-faint);font-weight:700;">[{tag}]</span>
            {new_badge_feat}
          </div>
          <div style="font-family:'Pretendard Variable',sans-serif;font-size:15px;font-weight:900;color:var(--ink);line-height:1.4;letter-spacing:-0.01em;margin-bottom:8px;">{headline}</div>
          <div style="font-family:'Pretendard Variable',sans-serif;font-size:12px;color:var(--ink-muted);line-height:1.55;font-weight:500;">{summary}</div>
        </div>
        <div style="display:flex;justify-content:space-between;align-items:center;margin-top:10px;padding-top:8px;border-top:1px dashed var(--rule);">
          <span style="font-family:'Pretendard Variable',sans-serif;font-size:11px;color:var(--gold);font-weight:800;">📰 {source}</span>
          <span style="font-family:'Pretendard Variable',sans-serif;font-size:10px;color:var(--ink-faint);font-weight:700;">원문 보기 ↗</span>
        </div>
      </div>
    </a>''')
    
    return "\n".join(cards)


def inject_news_section(html: str, news_data: dict) -> str:
    # 1. 카테고리별 리스트 주입
    news_html = build_news_html(news_data)
    pattern = re.compile(r'(<!-- NEWS_INJECT_START -->)(.*?)(<!-- NEWS_INJECT_END -->)', re.DOTALL)
    new_html, n = pattern.subn(
        lambda m: m.group(1) + "\n" + news_html + "\n" + m.group(3),
        html, count=1
    )
    if n > 0:
        total = sum(len(c.get("articles", [])) for c in news_data.get("by_category", {}).values())
        logger.info(f"✓ 카테고리별 뉴스 주입: {len(news_data.get('by_category', {}))}개 카테고리, {total}건")
    
    # 2. Featured 뉴스 주입 (마커 기반 - 누적 방지)
    featured_html = render_featured_news(news_data.get("featured", []))
    feat_pattern = re.compile(
        r'(<!-- FEATURED_INJECT_START -->)(.*?)(<!-- FEATURED_INJECT_END -->)',
        re.DOTALL
    )
    new_html, fn = feat_pattern.subn(
        lambda m: m.group(1) + "\n" + featured_html + "\n" + m.group(3),
        new_html, count=1
    )
    if fn > 0:
        logger.info(f"✓ Featured 뉴스 주입: {len(news_data.get('featured', []))}건")
    
    # 3. 권역별 카운트 (모든 카테고리 합산)
    counts = {"vivaldi": 0, "central": 0, "south": 0, "apac": 0, "general": 0}
    total_count = 0
    for cat_data in news_data.get("by_category", {}).values():
        for art in cat_data.get("articles", []):
            r = art.get("region", "general")
            if r in counts:
                counts[r] += 1
            total_count += 1
    # Featured도 카운트에 포함
    for f in news_data.get("featured", []):
        r = f.get("region", "general")
        if r in counts:
            counts[r] += 1
        total_count += 1
    
    new_html = apply_tpl(new_html, "news-count-all", str(total_count))
    for k in counts:
        new_html = apply_tpl(new_html, f"news-count-{k}", str(counts[k]))
    
    # 4. 오늘 날짜
    from datetime import datetime, timezone, timedelta
    KST = timezone(timedelta(hours=9))
    now = datetime.now(KST)
    day_kr = ['월', '화', '수', '목', '금', '토', '일'][now.weekday()]
    new_html = apply_tpl(new_html, "news-date", f"{now.strftime('%Y.%m.%d')} ({day_kr})")
    
    return new_html


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
# 주간 리포트 주입
# ─────────────────────────────────────────────
def inject_weekly_report(html: str, weekly: dict) -> str:
    """data/weekly_report.json → 주간 리포트 카드 데이터 주입"""
    if not weekly:
        return html

    # 주간 날짜
    html = apply_tpl(html, "weekly-date", weekly.get("_week", ""))

    # Daily OTB
    otb = weekly.get("daily_otb", {})
    if otb:
        date_str = otb.get("date", "")
        if date_str:
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                html = apply_tpl(html, "otb-date", f"{dt.month}/{dt.day}")
            except ValueError:
                html = apply_tpl(html, "otb-date", date_str)
        html = apply_tpl(html, "otb-net", f'{otb.get("net_otb", 0):,}')
        html = apply_tpl(html, "otb-booking", f'{otb.get("booking_rns", 0):,}')
        html = apply_tpl(html, "otb-cancel", f'{otb.get("cancel_rns", 0):,}')

    # 전략 카드 (최대 2개)
    strategies = weekly.get("weekly_strategies", [])
    for s_idx, strat in enumerate(strategies[:2]):
        prefix = f"strategy{s_idx + 1}"
        html = apply_tpl(html, f"{prefix}-subtitle", strat.get("subtitle", ""))
        html = apply_tpl(html, f"{prefix}-rate", str(strat.get("achievement_rate", "-")))

        channels = strat.get("channels", [])
        for c_idx, ch in enumerate(channels[:3]):
            html = apply_tpl(html, f"{prefix}-ch{c_idx}-rns", f'{ch.get("rns", 0):,}')
            html = apply_tpl(html, f"{prefix}-ch{c_idx}-rate", f'{ch.get("achievement_rate", 0)}%')

    logger.info("✓ 주간 리포트 주입")
    return html


def main():
    logger.info("=" * 60)
    logger.info("V7 대시보드 빌드 (TOP 4 + 사업장 매트릭스 + 뉴스 개선)")
    logger.info("=" * 60)
    
    enriched = load_json(DATA_DIR / "enriched_notes.json")
    notes = load_json(DATA_DIR / "daily_notes.json")
    news_data = load_json(DATA_DIR / "news_latest.json")
    comp_data = load_json(DATA_DIR / "competitors.json")
    weekly_data = load_json(DATA_DIR / "weekly_report.json")
    
    data = enriched if enriched else notes
    if not data:
        logger.error("데이터 없음")
        sys.exit(1)
    
    if not HTML_FILE.exists():
        logger.error(f"HTML 템플릿 없음: {HTML_FILE}")
        sys.exit(1)
    
    html = HTML_FILE.read_text(encoding="utf-8")
    logger.info(f"✓ HTML 로드 ({len(html):,} bytes)")
    
    # 외부 링크
    html = inject_external_links(html, notes.get("external_links", {}))
    
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
    
    headline = data.get("today_headline", {})
    if headline.get("text"):
        html = apply_tpl(html, "headline", headline["text"])
    html = apply_tpl(html, "updated_by", f"by {data.get('_updated_by', 'GS팀 · Haein Kim Manager')}")
    
    # KPI
    html = inject_kpi_3months(html, notes.get("executive_kpi", {}))
    
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
    
    # OTA TOP 4
    html = inject_ota_table(html, notes.get("major_ota_performance", {}))
    
    # 사업장 신호등 (BI 자동 수집 데이터)
    html = inject_signal_cards(html, notes.get("property_performance", {}))
    
    # 경쟁사
    html = inject_competitor_section(html, comp_data)
    
    # 주간 리포트
    html = inject_weekly_report(html, weekly_data)

    # 뉴스
    html = inject_news_section(html, news_data)
    
    # 빌드 메타
    build_meta = now.strftime("Auto-Built %Y-%m-%d %H:%M KST")
    html = apply_tpl(html, "build", build_meta)
    
    HTML_FILE.write_text(html, encoding="utf-8")
    
    logger.info("=" * 60)
    logger.info(f"✓ 빌드 완료 · 크기: {len(html):,} bytes · 시각: {build_meta}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
