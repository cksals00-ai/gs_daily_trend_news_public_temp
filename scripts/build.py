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
import subprocess
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
OTB_FILE = DOCS_DIR / "otb.html"
KST = timezone(timedelta(hours=9))

# ─────────────��───────────────────────────────
# 데일리 인사���트 문구 자동 생성
# ─────────���───────────────────────────────────
def generate_daily_insights(otb_data: dict, weekly_data: dict, rm_fcst: dict) -> list:
    """OTB/주간/RM FCST 데이터 기반 3~5개 인사��트 문구를 자동 생성한다."""
    insights = []
    summary = otb_data.get("summary", {})
    by_property = otb_data.get("byProperty", [])
    monthly = otb_data.get("monthly", [])
    now = datetime.now(KST)
    base_month = now.month if now.day >= 2 else (now.month - 1 if now.month > 1 else 12)

    # ── 1. 당월 OTB 달성률 + 전일 순증 인사이트 ──
    cur_month = next((m for m in monthly if m.get("month") == base_month), None)
    if cur_month and summary:
        ach = summary.get("rns_achievement")
        today_net = summary.get("today_net", 0)
        # 전일 순증이 높은 TOP 2 사업장
        top_props = sorted(
            [p for p in by_property if (p.get("today_net") or 0) > 0],
            key=lambda p: p.get("today_net", 0), reverse=True
        )[:2]
        top_names = "/".join(p["name"].split(".")[-1] for p in top_props) if top_props else ""
        sign = "+" if today_net >= 0 else ""
        text = f"{base_month}월 OTB {ach:.1f}% 달성, 전일 대비 {sign}{today_net:,}실 순증"
        if top_names:
            text += f". {top_names} 견인 중"
        insights.append({
            "id": "auto-otb-achievement",
            "type": "positive" if (ach or 0) >= 40 else "warning",
            "source": "OTB",
            "text": text,
        })

    # ── 2. FCST 갭 인사이트 (Budget vs FCST) ──
    fcst_ach = summary.get("fcst_achievement")
    if fcst_ach is not None:
        gap = round(fcst_ach - 100, 1)
        # 차월(M+1) 찾기
        next_month_idx = base_month + 1 if base_month < 12 else 1
        next_m = next((m for m in monthly if m.get("month") == next_month_idx), None)
        if next_m:
            next_budget = next_m.get("rns_budget", 0)
            next_actual = next_m.get("rns_actual", 0)
            next_ach = round(next_actual / next_budget * 100, 1) if next_budget else 0
            if next_ach < 30:
                sign = "+" if gap >= 0 else ""
                text = f"{next_month_idx}월 FCST 갭 확대 주의 — 목표 대비 FCST {sign}{gap}%, 기획전 보강 필요"
                insights.append({
                    "id": "auto-fcst-gap",
                    "type": "negative" if gap < -3 else "warning",
                    "source": "FCST",
                    "text": text,
                })
            elif fcst_ach >= 103:
                text = f"연간 FCST {fcst_ach:.1f}% 달성 전망, Budget 초과 페이싱 유지 중"
                insights.append({
                    "id": "auto-fcst-gap",
                    "type": "positive",
                    "source": "FCST",
                    "text": text,
                })

    # ── 3. TOP/BOTTOM 사업장 인사이트 ──
    active_props = [p for p in by_property if (p.get("rns_actual") or 0) > 0]
    if active_props:
        top = max(active_props, key=lambda p: p.get("rns_achievement", 0))
        bot = min(active_props, key=lambda p: p.get("rns_achievement", 0))
        top_name = top["name"].split(".")[-1]
        bot_name = bot["name"].split(".")[-1]
        top_ach = top.get("rns_achievement", 0)
        bot_ach = bot.get("rns_achievement", 0)
        if top_ach - bot_ach > 10:
            text = f"사업장 편차 주의 — TOP {top_name} {top_ach:.1f}% vs BOTTOM {bot_name} {bot_ach:.1f}%"
            insights.append({
                "id": "auto-prop-spread",
                "type": "warning",
                "source": "OTB",
                "text": text,
            })

    # ── 4. ADR YoY 인사이트 ──
    adr_yoy = summary.get("adr_yoy")
    adr_actual = summary.get("adr_actual")
    if adr_yoy is not None and adr_actual:
        sign = "+" if adr_yoy >= 0 else ""
        text = f"평균 ADR {adr_actual:,.0f}원, YoY {sign}{adr_yoy:.1f}%"
        if adr_yoy > 3:
            text += " — 단가 상승세 지속"
        elif adr_yoy < -3:
            text += " — 단가 하락 모니터링 필요"
        insights.append({
            "id": "auto-adr-yoy",
            "type": "positive" if adr_yoy > 0 else "negative",
            "source": "OTB",
            "text": text,
        })

    # ── 5. RM FCST vs Budget 권역별 갭 ──
    regions = rm_fcst.get("regions", {})
    if regions:
        target_key = f"{now.year}-{base_month:02d}"
        for region_name, months_data in regions.items():
            m_data = months_data.get(target_key, {})
            budget_rn = m_data.get("budget_rn", 0)
            rm_rn = m_data.get("rm_fcst_rn", 0)
            if budget_rn and rm_rn:
                gap_pct = round((rm_rn - budget_rn) / budget_rn * 100, 1)
                if gap_pct < -5:
                    text = f"{region_name} RM전망 Budget 대비 {gap_pct:+.1f}% ({rm_rn:,}실 vs {budget_rn:,}실)"
                    insights.append({
                        "id": f"auto-rm-{region_name}",
                        "type": "negative",
                        "source": "RM FCST",
                        "text": text,
                    })
                    break  # 가장 심한 것만

    # 최대 5개로 제한
    return insights[:5]


def build_admin_suggestions(otb_data: dict, weekly_data: dict, rm_fcst: dict,
                            news_data: dict = None, comp_data: dict = None) -> dict:
    """admin_suggestions.json에 저장할 전체 구조를 생성한다."""
    insights = generate_daily_insights(otb_data, weekly_data, rm_fcst)

    # 기존 전략 옵션 유지 (weekly_report의 strategies)
    strategies = weekly_data.get("weekly_strategies", [])
    strategy_options = []
    for s in strategies:
        opt = dict(s)
        opt["_current"] = True
        strategy_options.append(opt)

    return {
        "generated_at": datetime.now(KST).isoformat(),
        "insights": insights,
        "strategy_options": strategy_options,
        "current_strategies": strategies,
    }


def _calc_target_months():
    """매월 2일부터 다음 3개월로 롤링. 1일은 전월 마감 실적 확인용으로 이전 3개월 유지."""
    now = datetime.now(KST)
    # 2일 이전이면 이전달 기준, 2일 이후면 당월 기준
    base_month = now.month if now.day >= 2 else (now.month - 1 if now.month > 1 else 12)
    base_year = now.year if not (now.day < 2 and now.month == 1) else now.year - 1
    months = []
    labels = {}
    for i in range(3):
        m = base_month + i
        y = base_year
        if m > 12:
            m -= 12
            y += 1
        key = f"{y}-{m:02d}"
        months.append(key)
        labels[key] = f"{m}월"
    return months, labels

STAY_MONTHS, MONTH_LABELS = _calc_target_months()


def load_json(path: Path, default=None):
    if not path.exists():
        return default or {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        logger.error(f"JSON 파싱 실패 ({path.name}): {e}")
        return default or {}


def apply_tpl(html: str, selector: str, new_text) -> str:
    """data-tpl-{selector} element 의 inner text 또는 inner HTML 을 new_text 로 교체.

    Inner HTML 에 <strong> 등 nested tag 가 있어도 동작하도록 backref(\\2) 로 같은 tag 의 close 매칭.
    new_text 는 HTML 가능 (예: <strong>아고다</strong>).
    """
    if new_text is None:
        return html
    # 1차: inner HTML 에 < 없는 단순 케이스 (기존 호환)
    simple_pattern = re.compile(
        rf'(<[^>]*\bdata-tpl-{re.escape(selector)}\b[^>]*>)([^<]*?)(</)',
        re.DOTALL
    )
    new_html, n = simple_pattern.subn(
        lambda m: m.group(1) + str(new_text) + m.group(3),
        html, count=1
    )
    if n > 0:
        return new_html
    # 2차: inner HTML 에 nested tag 포함 케이스 (backref 로 같은 tag close 매칭)
    nested_pattern = re.compile(
        rf'(<(\w+)[^>]*\bdata-tpl-{re.escape(selector)}\b[^>]*>)(.*?)(</\2>)',
        re.DOTALL
    )
    new_html, n = nested_pattern.subn(
        lambda m: m.group(1) + str(new_text) + m.group(4),
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
              {month_label} {'· 당월' if is_primary else '· FCST'}
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

# ─────────────────────────────────────────────
# 권역 카드 주간 온북 추이 (사업장별·월별)
# ─────────────────────────────────────────────
# PROPERTY_DEFS: (display_name, region, [db_property_names])
_PROP_REGION_MAP = [
    ("벨비발디",      "vivaldi", ["소노벨 비발디파크", "소노문 비발디파크"]),
    ("캄비발디",      "vivaldi", ["소노캄 비발디파크"]),
    ("펫비발디",      "vivaldi", ["소노펫 비발디파크"]),
    ("펠리체비발디",  "vivaldi", ["소노펠리체 비발디파크"]),
    ("빌리지비발디",  "vivaldi", ["소노펠리체 빌리지 비발디파크"]),
    ("양평",          "central", ["소노휴 양평", "소노벨 양평"]),
    ("델피노",        "central", ["델피노"]),
    ("쏠비치양양",    "central", ["쏠비치 양양"]),
    ("쏠비치삼척",    "central", ["쏠비치 삼척"]),
    ("소노벨단양",    "central", ["소노문 단양", "소노벨 단양"]),
    ("소노캄경주",    "south",   ["소노벨 경주", "소노캄 경주"]),
    ("소노벨청송",    "central", ["소노벨 청송"]),
    ("소노벨천안",    "central", ["소노벨 천안"]),
    ("소노벨변산",    "central", ["소노벨 변산"]),
    ("소노캄여수",    "south",   ["소노캄 여수"]),
    ("소노캄거제",    "south",   ["소노캄 거제"]),
    ("쏠비치진도",    "south",   ["쏠비치 진도"]),
    ("소노벨제주",    "apac",    ["소노벨 제주"]),
    ("소노캄제주",    "apac",    ["소노캄 제주"]),
    ("소노캄고양",    "apac",    ["소노캄 고양"]),
    ("소노문해운대",  "south",   ["소노문 해운대"]),
    ("쏠비치남해",    "south",   ["쏠비치 남해"]),
    ("르네블루",      "central", ["르네블루"]),
]


def build_weekly_onbook(db_agg: dict) -> dict:
    """
    db_aggregated.json에서 사업장별·투숙월별 주간 순예약(온북) 추이 계산.
    반환: { region: { display_name: { "202604": [{"w":"4/7","rn":120}, ...], ... } } }
    """
    from datetime import datetime as _dt
    pickup_pm = db_agg.get("pickup_daily_by_property_month", {})
    cancel_pm = db_agg.get("cancel_daily_by_property_month", {})

    result = {"vivaldi": {}, "central": {}, "south": {}, "apac": {}}

    for disp, region, db_props in _PROP_REGION_MAP:
        prop_months = {}
        for stay_m in ["202604", "202605", "202606"]:
            # 일별 net pickup 합산 (여러 db_prop 합산)
            daily_net = {}
            for dbp in db_props:
                for d, v in pickup_pm.get(dbp, {}).get(stay_m, {}).items():
                    daily_net[d] = daily_net.get(d, 0) + v.get("rn", 0)
                for d, v in cancel_pm.get(dbp, {}).get(stay_m, {}).items():
                    daily_net[d] = daily_net.get(d, 0) - v.get("rn", 0)

            if not daily_net:
                continue

            # 주간 집계 (월~일 기준)
            weekly = {}
            for d_str, rn in sorted(daily_net.items()):
                try:
                    dt = _dt.strptime(d_str[:8], "%Y%m%d")
                except ValueError:
                    continue
                iso_yr, iso_wk, _ = dt.isocalendar()
                wk_key = f"{iso_yr}W{iso_wk:02d}"
                # 주차 라벨: 해당 주의 월요일 날짜
                mon = dt - timedelta(days=dt.weekday())
                wk_label = f"{mon.month}/{mon.day}"
                if wk_key not in weekly:
                    weekly[wk_key] = {"l": wk_label, "rn": 0}
                weekly[wk_key]["rn"] += rn

            if weekly:
                # 최근 8주만 표시
                sorted_weeks = [
                    {"w": v["l"], "rn": v["rn"]}
                    for _, v in sorted(weekly.items())
                ]
                prop_months[stay_m] = sorted_weeks[-8:]

        if prop_months:
            result[region][disp] = prop_months

    return result


def inject_weekly_onbook(html: str, db_agg: dict) -> str:
    """주간 온북 추이 JSON 주입 (이미 주입된 경우 교체)"""
    weekly_data = build_weekly_onbook(db_agg)
    weekly_json = json.dumps(weekly_data, ensure_ascii=False)
    replacement = f"const WEEKLY_ONBOOK = {weekly_json};"
    # 이미 주입된 경우 교체
    pattern = re.compile(r'const WEEKLY_ONBOOK = \{.*?\};', re.DOTALL)
    if pattern.search(html):
        html = pattern.sub(replacement, html, count=1)
    else:
        html = html.replace("/*__WEEKLY_ONBOOK__*/", replacement)
    logger.info(f"✓ 주간 온북 추이 주입: {sum(len(v) for v in weekly_data.values())}개 사업장")
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
            rns_check = m4.get("rns", 0)
            target_check = m4.get("target_rns", 0)

            # 달성률에 따른 색상 (목표 미입력 시 중립)
            if target_check == 0 and rns_check > 0:
                dot_color = "yellow"
                value_class = ""
                value_color = "var(--ink-muted)"
            elif achievement >= 100:
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
            
            # 목표 미입력 시 중립 표시 (achievement=0 but rns>0)
            rns = m4.get("rns", 0)
            target_rns = m4.get("target_rns", 0)
            if target_rns == 0 and rns > 0:
                sub_text = f"{rns:,}실 (목표 미입력)"
                display_value = "—"
                value_class = ""
            else:
                sub_text = f"4월 달성 {achievement:.1f}%"
                display_value = f"{achievement:.1f}%"

            cards_html.append(
                f'    <div class="signal-card">\n'
                f'      <div class="signal-dot {dot_color}"></div>\n'
                f'      <div>\n'
                f'        <div class="signal-name">{name}</div>\n'
                f'        <div class="signal-sub" style="font-family:\'Pretendard Variable\',sans-serif;font-weight:700;font-size:12px;color:var(--ink-muted);">{sub_text}</div>\n'
                f'        <div style="font-family:\'JetBrains Mono\',monospace;font-size:11px;font-weight:700;margin-top:2px;color:{yoy_color};">{yoy_text}</div>\n'
                f'      </div>\n'
                f'      <div class="signal-value {value_class}">{display_value}</div>\n'
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
NEWS_VISIBLE_LIMIT = 10  # PC 기준 초기 노출 기사 수 (모바일은 클라이언트 JS에서 5개로 조정)


def _is_stale_by_pub_date(pub_date_str: str) -> bool:
    """pub_date(RFC 2822) 기준 오래된 기사 여부 (빌드 단계 필터).

    제외 조건:
      1. 발행 연도가 현재 연도와 다름
      2. 발행일이 현재 날짜 기준 3개월(90일) 이전
    """
    if not pub_date_str:
        return False
    from email.utils import parsedate_to_datetime
    try:
        pub_dt = parsedate_to_datetime(pub_date_str)
        now = datetime.now(KST)
        if pub_dt.year != now.year:
            return True
        if (now - pub_dt).days > 90:
            return True
        return False
    except (ValueError, TypeError):
        return False


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
    seen_titles: set[str] = set()
    for cat_name in category_order:
        if cat_name not in by_category:
            continue
        cat_data = by_category[cat_name]
        raw_articles = cat_data.get("articles", [])
        # 제목 기준 최종 dedup (top_news/by_region 교차 중복 포함)
        articles = []
        for a in raw_articles:
            key = a.get("title", "")[:50]
            if key and key not in seen_titles:
                seen_titles.add(key)
                # 오래된 기사 제외 (연도 불일치 또는 3개월 이전)
                if _is_stale_by_pub_date(a.get("pub_date", "")):
                    continue
                articles.append(a)
        if not articles:
            continue

        # 카테고리 헤더 (id는 클라이언트 JS 더보기 토글에 사용)
        cat_id = "cat-" + cat_name.replace("/", "-").replace(" ", "-")
        article_count = len(articles)
        section_html = f'''
  <!-- ===== {cat_name} ===== -->
  <div class="news-category-section" id="{cat_id}" style="margin-bottom:24px;">
    <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:var(--bg-soft);border-left:3px solid var(--gold);border-radius:0 4px 4px 0;margin-bottom:6px;">
      <div style="display:flex;align-items:center;gap:8px;">
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
    # 오래된 기사 필터링
    featured_list = [f for f in featured_list if not _is_stale_by_pub_date(f.get("pub_date", ""))]
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
        region = item.get("region", "general")
        tag = escape_html(item.get("tag", ""))
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

      <!-- 본문 -->
      <div style="flex:1;padding:16px 20px;display:flex;flex-direction:column;justify-content:space-between;min-width:0;">
        <div>
          <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;flex-wrap:wrap;">
            <span style="font-family:'Pretendard Variable',sans-serif;font-size:10px;padding:3px 8px;background:{r_color};color:#0d0d0d;border-radius:2px;font-weight:900;letter-spacing:0.05em;">{category}</span>
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
    
    # 3. 권역별 카운트 (모든 카테고리 합산, 오래된 기사 제외)
    counts = {"vivaldi": 0, "central": 0, "south": 0, "apac": 0, "general": 0}
    total_count = 0
    for cat_data in news_data.get("by_category", {}).values():
        for art in cat_data.get("articles", []):
            if _is_stale_by_pub_date(art.get("pub_date", "")):
                continue
            r = art.get("region", "general")
            if r in counts:
                counts[r] += 1
            total_count += 1
    # Featured도 카운트에 포함 (오래된 기사 제외)
    for f in news_data.get("featured", []):
        if _is_stale_by_pub_date(f.get("pub_date", "")):
            continue
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
def inject_weekly_report(html: str, weekly: dict, agg_data: dict = None, otb_data: dict = None, admin_data: dict = None) -> str:
    """data/weekly_report.json + otb_data.json → 주간 리포트 카드 데이터 주입"""
    if not weekly:
        return html

    # 주간 날짜
    html = apply_tpl(html, "weekly-date", weekly.get("_week", ""))

    # 예약 기준일 = 빌드 전날 (자동 갱신)
    now = datetime.now(KST)
    yesterday = now - timedelta(days=1)
    basis_date = f"{yesterday.month}/{yesterday.day}"
    basis_full = f"~ {yesterday.month}/{yesterday.day} 예약 기준"

    # 섹션 헤더 예약 기준일
    html = apply_tpl(html, "weekly-basis", basis_full)

    # Daily OTB — otb_data.json의 allMonths에서 당월/익월/익익월 자동 추출
    html = apply_tpl(html, "otb-date", basis_date)
    if otb_data:
        all_months = otb_data.get("allMonths", {})
        cur_month = now.month
        month_keys = [str(cur_month + i) for i in range(3)]

        def _ach_class(v):
            if v is None:
                return "ach-low"
            return "ach-high" if v >= 90 else "ach-mid" if v >= 75 else "ach-low"

        # admin FCST key-in 오버라이드 집계
        admin_fcst_keyin = (admin_data or {}).get("fcst_keyin", {})

        for m_idx, mkey in enumerate(month_keys):
            s = all_months.get(mkey, {}).get("summary", {})
            actual   = s.get("rns_actual")
            ach      = s.get("rns_achievement")
            fcst     = s.get("rns_fcst")
            fcst_ach = s.get("fcst_achievement")

            # admin key-in FCST 오버라이드
            # yoyTable의 사업장별 자동 FCST를 기본값으로, 키인이 있는 사업장만 대체 후 합산
            bud_rn = s.get("rns_budget", 0) or 0
            if admin_fcst_keyin and otb_data:
                yoy_table = otb_data.get("yoyTable", [])
                # 해당 월에 키인이 하나라도 있는지 확인
                keyin_for_month = {}
                for akey, aval in admin_fcst_keyin.items():
                    parts = akey.split("|")
                    if len(parts) == 2:
                        prop_name, keyin_month = parts
                        if keyin_month == mkey or keyin_month == "all":
                            v = aval.get("value") if isinstance(aval, dict) else None
                            if v is not None:
                                keyin_for_month[prop_name] = v
                if keyin_for_month:
                    # 사업장별로: 키인 있으면 키인값, 없으면 자동 FCST 사용
                    fcst_total = 0
                    for row in yoy_table:
                        pname = row.get("name", "")
                        md = (row.get("months") or {}).get(mkey, {})
                        auto_fcst = md.get("rns_fcst") or md.get("rns_fcst_ai") or 0
                        fcst_total += keyin_for_month.get(pname, auto_fcst)
                    fcst = fcst_total
                    fcst_ach = round(fcst_total / bud_rn * 100, 1) if bud_rn > 0 else None
            t_net    = s.get("today_net", 0) or 0
            t_book   = s.get("today_booking", 0) or 0
            t_cancel = s.get("today_cancel", 0) or 0

            html = apply_tpl(html, f"otb-m{m_idx}-actual",   f"{actual:,}"         if actual   is not None else "—")
            html = apply_tpl(html, f"otb-m{m_idx}-ach",      f"{ach:.1f}%"         if ach      is not None else "—")
            html = apply_tpl(html, f"otb-m{m_idx}-fcst",     f"{fcst:,}"           if fcst     is not None else "—")
            html = apply_tpl(html, f"otb-m{m_idx}-fcst-ach", f"{fcst_ach:.1f}%"    if fcst_ach is not None else "—")
            net_sign = "+" if t_net >= 0 else ""
            html = apply_tpl(html, f"otb-m{m_idx}-today-net", f"{net_sign}{t_net:,}")

            html = html.replace(f"OTB_M{m_idx}_ACH_CLASS",  _ach_class(ach))
            html = html.replace(f"OTB_M{m_idx}_FACH_CLASS", _ach_class(fcst_ach))
            net_clr = "var(--positive)" if t_net >= 0 else "var(--negative)"
            html = html.replace(f"OTB_M{m_idx}_NET_CLR", net_clr)

        # 월 라벨 동적 주입 (하드코딩 방지)
        for m_idx, mkey in enumerate(month_keys):
            m_label = f"{int(mkey)}월"
            html = apply_tpl(html, f"otb-m{m_idx}-label", m_label)

        # 당월 인사이트 주입
        cur_snap = all_months.get(str(cur_month), {})
        cur_s = cur_snap.get("summary", {})
        cur_props = cur_snap.get("byProperty", [])

        # YoY
        yoy_val = cur_s.get("rns_yoy", 0) or 0
        yoy_sign = "+" if yoy_val >= 0 else ""
        yoy_clr = "var(--positive)" if yoy_val >= 0 else "var(--negative)"
        html = apply_tpl(html, "otb-yoy", f"{yoy_sign}{yoy_val:.1f}%")
        html = html.replace("OTB_YOY_CLR", yoy_clr)

        # 목표 갭
        gap = (cur_s.get("rns_budget", 0) or 0) - (cur_s.get("rns_actual", 0) or 0)
        if gap > 0:
            gap_txt = f"부족 {gap:,}실"
            gap_clr = "var(--negative)"
        else:
            gap_txt = f"초과 {abs(gap):,}실"
            gap_clr = "var(--positive)"
        html = apply_tpl(html, "otb-gap", gap_txt)
        html = html.replace("OTB_GAP_CLR", gap_clr)

        # 월별 갭 데이터 JSON 주입 (JS 월탭 연동용)
        import json as _json_gap
        gap_by_month = {}
        for mkey in month_keys:
            ms = all_months.get(mkey, {}).get("summary", {})
            gap_by_month[mkey] = {
                "budget": ms.get("rns_budget", 0) or 0,
                "actual": ms.get("rns_actual", 0) or 0
            }
        gap_json = _json_gap.dumps(gap_by_month, ensure_ascii=False)
        html = html.replace("/*__OTB_GAP_BY_MONTH__*/", f"var _OTB_GAP_BY_MONTH = {gap_json};")

        # 매출 달성률
        rev_ach = cur_s.get("rev_achievement")
        html = apply_tpl(html, "otb-rev-ach", f"{rev_ach:.1f}%" if rev_ach is not None else "—")
        html = html.replace("OTB_REVACH_CLASS", _ach_class(rev_ach))

        # Top/Bottom 사업장 — 3개월분 JSON 주입
        import json as _json
        tb_by_month = {}
        for m_idx, mkey in enumerate(month_keys):
            m_snap = all_months.get(mkey, {})
            m_props = m_snap.get("byProperty", [])
            if m_props:
                s_props = sorted(m_props, key=lambda x: x.get("rns_yoy", 0), reverse=True)
                t3 = [{"n": p["name"].split(".",1)[-1], "a": p.get("rns_yoy", 0)} for p in s_props[:3]]
                b3 = [{"n": p["name"].split(".",1)[-1], "a": p.get("rns_yoy", 0)} for p in s_props[-3:] if p.get("rns_yoy", 0) < 0]
                tb_by_month[mkey] = {"top": t3, "bot": b3}
            else:
                tb_by_month[mkey] = {"top": [], "bot": []}
        # 당월 기본 표시
        cur_tb = tb_by_month.get(str(cur_month), {"top": [], "bot": []})
        def _fmt_tb(p): return f'{p["n"]} {"+" if p["a"] >= 0 else ""}{p["a"]}% YoY'
        top_items = " / ".join(_fmt_tb(p) for p in cur_tb["top"]) or "—"
        bot_items = " / ".join(_fmt_tb(p) for p in cur_tb["bot"]) or "—"
        html = apply_tpl(html, "otb-top3", top_items)
        html = apply_tpl(html, "otb-bot3", bot_items)
        # JSON 주입 (JS 토글용) — 기존 값 or 플레이스홀더 모두 대체
        import re as _re
        tb_json = _json.dumps(tb_by_month, ensure_ascii=False)
        tb_replacement = f"const TB_BY_MONTH = {tb_json};"
        if "/*__TB_BY_MONTH__*/" in html:
            html = html.replace("/*__TB_BY_MONTH__*/", tb_replacement)
        else:
            html = _re.sub(r'const TB_BY_MONTH\s*=\s*\{.*?\};', tb_replacement, html, count=1, flags=_re.DOTALL)
        # 월 라벨 주입
        month_labels_json = _json.dumps({mkey: f"{int(mkey)}월" for mkey in month_keys}, ensure_ascii=False)
        ml_replacement = f"const TB_MONTH_LABELS = {month_labels_json};"
        if "/*__TB_MONTH_LABELS__*/" in html:
            html = html.replace("/*__TB_MONTH_LABELS__*/", ml_replacement)
        else:
            html = _re.sub(r'const TB_MONTH_LABELS\s*=\s*\{.*?\};', ml_replacement, html, count=1)

        logger.info("✓ Daily OTB (otb_data.json allMonths) 주입")

    # 전략 카드 (최대 2개)
    strategies = weekly.get("weekly_strategies", [])
    for s_idx, strat in enumerate(strategies[:2]):
        prefix = f"strategy{s_idx + 1}"
        n = s_idx + 1
        html = apply_tpl(html, f"{prefix}-subtitle", strat.get("subtitle", ""))

        rate = strat.get("achievement_rate", 0)
        html = apply_tpl(html, f"{prefix}-rate", f"{rate}%")

        # 게이지 색상 및 오프셋 계산
        if rate >= 100:
            gauge_color = "#4ecdc4"
        elif rate >= 85:
            gauge_color = "#f0a500"
        else:
            gauge_color = "#ff6b6b"
        arc_length = 125.66
        gauge_offset = arc_length * (1 - rate / 100)
        html = html.replace(f"STRAT{n}_GCOLOR", gauge_color)
        html = html.replace(f"STRAT{n}_GOFFSET", f"{gauge_offset:.2f}")

        # KPI (객실수 / ADR / 매출)
        rns = strat.get("rns", 0)
        html = apply_tpl(html, f"{prefix}-rns", f'{rns:,}')
        html = apply_tpl(html, f"{prefix}-adr", str(strat.get("adr", 0)))
        html = apply_tpl(html, f"{prefix}-rev", str(strat.get("rev", 0)))

        channels = strat.get("channels", [])
        for c_idx, ch in enumerate(channels[:3]):
            html = apply_tpl(html, f"{prefix}-ch{c_idx}-rns", f'{ch.get("rns", 0):,}')
            html = apply_tpl(html, f"{prefix}-ch{c_idx}-rate", f'{ch.get("achievement_rate", 0)}%')

        # ── A안: 목표→현재→부족 진행바 ──
        target_rns = round(rns * 100 / rate) if rate > 0 else rns
        shortage_rns = max(0, target_rns - rns)
        bar_pct = min(100, rate)
        html = html.replace(f"STRAT{n}_TARGET_RNS", f"{target_rns:,}")
        html = html.replace(f"STRAT{n}_CUR_RNS", f"{rns:,}")
        html = html.replace(f"STRAT{n}_SHORTAGE_RNS", f"{shortage_rns:,}")
        html = html.replace(f"STRAT{n}_BAR_PCT", f"{bar_pct:.1f}")

        # ── B안: 워터폴 채널 흐름 ──
        ch_rns = [ch.get("rns", 0) for ch in channels[:3]]
        wf_final = rns + sum(ch_rns)
        # flex 값: 최소 0 보장
        wf_vals = [rns] + ch_rns + [0] * (3 - len(ch_rns))
        for wi, wv in enumerate(wf_vals):
            html = html.replace(f"STRAT{n}_WF{wi}", str(max(0, wv)))
        html = html.replace(f"STRAT{n}_WF_FINAL", f"{wf_final:,}")

    logger.info("✓ 주간 리포트 주입")
    return html


# ─────────────────────────────────────────────
# 당일 분석 교차검증
# ─────────────────────────────────────────────
def _validate_daily_analysis_consistency(da: dict) -> dict:
    """당일 분석 데이터 정합성 검증.

    규칙:
      1. 사업장별(OTA+G-OTA+Inbound) NET ≥ 채널별(OTA+G-OTA) NET — 채널별이 사업장별보다 크면 안 됨.
      2. 사업장별 NET - 채널별 NET ≒ Inbound NET (채널은 비례추정이라 ±오차 허용).

    위반 시 빌드 로그에 ERROR 출력하고, 검증 결과를 dict로 반환해
    docs/data/daily_analysis_validation.json·data-check.html에서 표시 가능하게 함.
    """
    def _net(d):
        return sum((v.get("pickup", 0) or 0) - (v.get("cancel", 0) or 0) for v in d.values())

    prop_net = _net(da.get("byProperty", {}))
    chan_net = _net(da.get("byChannel", {}))
    seg = da.get("bySegment", {}) or {}
    ib = (seg.get("Inbound", {}) or {}).get("net", 0) or 0

    # 채널 분배는 by_property_channel 비례 추정이라 정확히 일치하지 않음. 절대값 5% 또는 100실 둘 중 큰 값 허용.
    expected_diff = ib  # 사업장별 - 채널별 = Inbound (이상)
    actual_diff = prop_net - chan_net
    tolerance = max(100, abs(prop_net) * 0.05)
    diff_ok = abs(actual_diff - expected_diff) <= tolerance
    inequality_ok = prop_net >= chan_net  # 강한 룰

    result = {
        "prop_net": prop_net,
        "chan_net": chan_net,
        "inbound_net": ib,
        "expected_diff": expected_diff,
        "actual_diff": actual_diff,
        "tolerance": round(tolerance, 1),
        "inequality_ok": inequality_ok,
        "diff_ok": diff_ok,
        "passed": inequality_ok and diff_ok,
    }

    if not inequality_ok:
        logger.error(
            f"✗ 당일분석 교차검증 실패: 사업장별 NET({prop_net}) < 채널별 NET({chan_net}). "
            f"채널별이 사업장별보다 클 수 없음 (Inbound·기타 세그먼트가 채널 합계에 섞여있을 가능성)."
        )
    elif not diff_ok:
        logger.warning(
            f"⚠ 당일분석 교차검증 경고: 사업장별-채널별({actual_diff}) ≠ Inbound({ib}), "
            f"오차 {abs(actual_diff - expected_diff):.0f} > 허용 {tolerance:.0f}"
        )
    else:
        logger.info(
            f"✓ 당일분석 교차검증 통과: 사업장별 NET={prop_net}, 채널별 NET={chan_net}, "
            f"차이={actual_diff} (Inbound={ib}, 허용±{tolerance:.0f})"
        )

    return result


# ─────────────────────────────────────────────
# 인사이트 패널 데이터 주입
# ─────────────────────────────────────────────
def inject_insight_panel_data(html: str, otb_data: dict, agg_data: dict, now: datetime) -> str:
    """전일 픽업 상세 / 주간 픽업 트렌드 / 4개년 비교 데이터를 JS 상수로 주입"""
    import json as _json
    from datetime import timedelta as _td

    cur_month = now.month
    all_months = otb_data.get("allMonths", {})
    nd = agg_data.get("net_daily", {})
    mt = agg_data.get("monthly_total", {})
    today_date = otb_data.get("meta", {}).get("todayDate", "")  # e.g. "20260426"

    # ── 1) 세그먼트별 전일 픽업 ──
    main_segs = ["OTA", "G-OTA", "Inbound"]
    seg_today = {}
    # "summary" = 전체 합산 (구 "0" 키는 별칭으로 호환)
    full_seg = (all_months.get("summary") or all_months.get("0") or {}).get("segmentData", {})
    for seg in main_segs:
        s = full_seg.get(seg, {})
        seg_today[seg] = {
            "booking": s.get("today_booking", 0) or 0,
            "cancel": s.get("today_cancel", 0) or 0,
            "net": s.get("today_net", 0) or 0,
            "net_rev": round((s.get("today_net_rev", 0) or 0) / 1e8, 1),  # 억 단위
        }

    # ── 2) 투숙월별 전일 픽업 분포 ──
    stay_month_today = {}
    for mi in range(cur_month, min(cur_month + 4, 13)):
        mkey = str(mi)
        ms = all_months.get(mkey, {}).get("summary", {})
        t_net = ms.get("today_net", 0) or 0
        t_book = ms.get("today_booking", 0) or 0
        t_cancel = ms.get("today_cancel", 0) or 0
        if t_book != 0 or t_cancel != 0 or t_net != 0:
            stay_month_today[f"{mi}월"] = {"booking": t_book, "cancel": t_cancel, "net": t_net}

    # ── 3) 주간 픽업 트렌드 (일별 net_rn, 14일) ──
    all_keys = sorted(nd.keys())
    keys14 = all_keys[-14:] if len(all_keys) >= 14 else all_keys
    daily_trend = []
    for k in keys14:
        v = nd.get(k, {})
        daily_trend.append({
            "date": k,
            "label": f"{k[4:6]}/{k[6:8]}",
            "pickup": v.get("pickup_rn", 0) or 0,
            "cancel": v.get("cancel_rn", 0) or 0,
            "net": v.get("net_rn", 0) or 0,
        })

    # 주간 세그먼트 추이 (stayDateDaily에서 당월 세그먼트별 최근 14일 net_rn)
    sdd = otb_data.get("stayDateDaily", {})
    cur_month_key = f"{now.year}{cur_month:02d}"
    seg_weekly = {}
    if cur_month_key in sdd:
        sdd_seg = sdd[cur_month_key].get("segments", {})
        for seg in main_segs:
            arr = sdd_seg.get(seg, {}).get("net_rn", [])
            seg_weekly[seg] = arr[-14:] if len(arr) >= 14 else arr

    # ── 3b) 투숙월별 주간 픽업 트렌드 (4/5/6월 탭용) ──
    compare_months = list(range(cur_month, min(cur_month + 3, 13)))
    ndm = agg_data.get("net_daily_by_month", {})
    daily_trend_by_month = {}
    for mi in compare_months:
        mkey = f"{now.year}{mi:02d}"
        md = ndm.get(mkey, {})
        if not md:
            continue
        mkeys = sorted(md.keys())
        mkeys14 = mkeys[-14:] if len(mkeys) >= 14 else mkeys
        mtrend = []
        for k in mkeys14:
            v = md.get(k, {})
            mtrend.append({
                "date": k,
                "label": f"{k[4:6]}/{k[6:8]}",
                "pickup": v.get("pickup_rn", 0) or 0,
                "cancel": v.get("cancel_rn", 0) or 0,
                "net": v.get("net_rn", 0) or 0,
            })
        daily_trend_by_month[f"{mi}월"] = mtrend

    # ── 3c) 전년 동기간(YoY) 일별 픽업 트렌드 ──
    # 같은 요일 매칭: 52주(364일) 전 날짜를 전년 동기간으로 사용
    # + 공휴일/연휴 제외 + 시즌 매칭 검증
    def _ly_date(dt_str: str) -> str:
        """YYYYMMDD → 364일(52주) 전 날짜 (같은 요일 보장)"""
        from datetime import datetime as _dt
        d = _dt.strptime(dt_str, "%Y%m%d")
        ly = d - _td(days=364)
        return ly.strftime("%Y%m%d")

    # 한국 공휴일 데이터 로드
    holidays_path = DATA_DIR / "holidays_kr.json"
    holidays_data = load_json(holidays_path, {})
    kr_holidays = holidays_data.get("holidays", {})
    season_cfg = holidays_data.get("seasons", {})

    def _is_holiday(dt_str: str) -> tuple:
        """공휴일 여부 판별. (True/False, 공휴일명)"""
        h = kr_holidays.get(dt_str)
        if h:
            return True, h.get("name", "공휴일")
        return False, ""

    def _parse_period(period_str: str) -> tuple:
        """'YYYYMMDD-YYYYMMDD' → (start_date, end_date)"""
        from datetime import datetime as _dt
        parts = period_str.split("-")
        return _dt.strptime(parts[0], "%Y%m%d"), _dt.strptime(parts[1], "%Y%m%d")

    def _get_season(dt_str: str) -> str:
        """날짜의 시즌 판별: 'peak', 'shoulder', 'off'"""
        from datetime import datetime as _dt
        dt = _dt.strptime(dt_str, "%Y%m%d")
        yr = dt.year
        for season_type in ["peak", "shoulder"]:
            scfg = season_cfg.get(season_type, {})
            for yr_check in [yr - 1, yr, yr + 1]:
                periods = scfg.get(f"periods_{yr_check}", [])
                for p in periods:
                    try:
                        s, e = _parse_period(p)
                        if s <= dt <= e:
                            return season_type
                    except Exception:
                        continue
        return "off"

    _season_label = {"peak": "성수기", "shoulder": "준성수기", "off": "비수기"}

    def _check_yoy_match(cur_date: str, ly_date: str) -> tuple:
        """
        YoY 매칭 유효성 검사.
        Returns: (is_valid, exclusion_reason)
        - is_valid=True: 매칭 가능
        - is_valid=False, exclusion_reason: 제외 사유 문자열
        """
        # 1) 올해 날짜가 공휴일/연휴인지 확인
        cur_is_hol, cur_hol_name = _is_holiday(cur_date)
        if cur_is_hol:
            return False, f"{cur_hol_name}"

        # 2) 전년 매칭 날짜가 공휴일/연휴인지 확인
        ly_is_hol, ly_hol_name = _is_holiday(ly_date)
        if ly_is_hol:
            return False, f"전년 동기간 {ly_hol_name}"

        # 3) 시즌 매칭 확인
        cur_season = _get_season(cur_date)
        ly_season = _get_season(ly_date)
        if cur_season != ly_season:
            return False, f"시즌 불일치 (금년 {_season_label[cur_season]} ↔ 전년 {_season_label[ly_season]})"

        return True, ""

    # 전체 일별 트렌드 전년 매칭 (공휴일/시즌 제외 적용)
    daily_trend_ly = []
    yoy_exclusions_all = []  # 전체 기간 제외 사유
    for item in daily_trend:
        ly_key = _ly_date(item["date"])
        is_valid, reason = _check_yoy_match(item["date"], ly_key)
        if is_valid:
            ly_val = nd.get(ly_key, {})
            daily_trend_ly.append({
                "date": ly_key,
                "label": f"{ly_key[4:6]}/{ly_key[6:8]}",
                "pickup": ly_val.get("pickup_rn", 0) or 0,
                "cancel": ly_val.get("cancel_rn", 0) or 0,
                "net": ly_val.get("net_rn", 0) or 0,
            })
        else:
            # 제외 시 null 데이터로 표시
            daily_trend_ly.append({
                "date": ly_key,
                "label": f"{ly_key[4:6]}/{ly_key[6:8]}",
                "pickup": None,
                "cancel": None,
                "net": None,
                "excluded": True,
            })
            day_label = f"{item['date'][4:6]}/{item['date'][6:8]}"
            yoy_exclusions_all.append(f"{day_label} - {reason}으로 제외")

    # 투숙월별 전년 매칭 (공휴일/시즌 제외 적용)
    daily_trend_by_month_ly = {}
    yoy_exclusions_by_month = {}  # 월별 제외 사유
    for mi in compare_months:
        mlabel = f"{mi}월"
        cur_mtrend = daily_trend_by_month.get(mlabel, [])
        if not cur_mtrend:
            continue
        ly_mkey = f"{now.year - 1}{mi:02d}"  # 전년 같은 투숙월
        ly_md = ndm.get(ly_mkey, {})
        mtrend_ly = []
        month_exclusions = []
        for item in cur_mtrend:
            ly_bk_date = _ly_date(item["date"])  # 같은 요일 매칭
            is_valid, reason = _check_yoy_match(item["date"], ly_bk_date)
            if is_valid:
                ly_val = ly_md.get(ly_bk_date, {})
                mtrend_ly.append({
                    "date": ly_bk_date,
                    "label": f"{ly_bk_date[4:6]}/{ly_bk_date[6:8]}",
                    "pickup": ly_val.get("pickup_rn", 0) or 0,
                    "cancel": ly_val.get("cancel_rn", 0) or 0,
                    "net": ly_val.get("net_rn", 0) or 0,
                })
            else:
                mtrend_ly.append({
                    "date": ly_bk_date,
                    "label": f"{ly_bk_date[4:6]}/{ly_bk_date[6:8]}",
                    "pickup": None,
                    "cancel": None,
                    "net": None,
                    "excluded": True,
                })
                day_label = f"{item['date'][4:6]}/{item['date'][6:8]}"
                month_exclusions.append(f"{day_label} - {reason}으로 제외")
        daily_trend_by_month_ly[mlabel] = mtrend_ly
        if month_exclusions:
            yoy_exclusions_by_month[mlabel] = month_exclusions

    # ── 4) 4개년 동기간 비교 (월별 OTB) ──
    years_compare = {}
    for yr in [2022, 2023, 2024, 2025, 2026]:
        yr_data = {}
        for mo in compare_months:
            key = f"{yr}{mo:02d}"
            if key in mt:
                v = mt[key]
                yr_data[f"{mo}월"] = {
                    "booking_rn": v.get("booking_rn", 0),
                    "net_rn": v.get("net_rn", 0),
                    "net_rev": round(v.get("net_rev", 0), 1),
                }
        if yr_data:
            years_compare[str(yr)] = yr_data

    # 4개년 동기간 — OTB 스냅샷 (otb_data의 yoyTable에서 사업장별 수치)
    yoy_summary = {}
    for yr in [2022, 2023, 2024, 2025, 2026]:
        for mo in compare_months:
            mkey = f"{yr}{mo:02d}"
            if mkey in mt:
                if str(yr) not in yoy_summary:
                    yoy_summary[str(yr)] = {}
                yoy_summary[str(yr)][f"{mo}월"] = mt[mkey].get("booking_rn", 0)

    # ── 5) 당일 분석: 사업장별·채널별 전일 숫자 (투숙월 4/5/6 탭용) ──
    pdbp = agg_data.get("pickup_daily_by_property", {})
    cdbp = agg_data.get("cancel_daily_by_property", {})
    pdbpm = agg_data.get("pickup_daily_by_property_month", {})
    cdbpm = agg_data.get("cancel_daily_by_property_month", {})
    pds = agg_data.get("pickup_daily_by_segment", {})
    cds = agg_data.get("cancel_daily_by_segment", {})
    pdbps = agg_data.get("pickup_daily_by_property_segment", {})
    cdbps = agg_data.get("cancel_daily_by_property_segment", {})
    pdbpsm = agg_data.get("pickup_daily_by_property_segment_month", {})
    cdbpsm = agg_data.get("cancel_daily_by_property_segment_month", {})

    # 전일 = today_date (빌드 기준일)
    daily_analysis = {"byProperty": {}, "bySegment": {}, "byPropertyMonth": {}, "bySegmentMonth": {},
                      "byChannel": {}, "byChannelMonth": {}, "byPropertySegment": {}}

    # (a) 사업장별 전일 전체
    for prop in sorted(pdbp.keys()):
        p_val = pdbp[prop].get(today_date, {})
        c_val = cdbp.get(prop, {}).get(today_date, {})
        p_rn = p_val.get("rn", 0) or 0
        c_rn = c_val.get("rn", 0) or 0
        p_rev = p_val.get("rev", 0) or 0
        c_rev = c_val.get("rev", 0) or 0
        if p_rn or c_rn:
            daily_analysis["byProperty"][prop] = {
                "pickup": p_rn, "cancel": c_rn, "net": p_rn - c_rn,
                "rev": round(p_rev - c_rev, 1),
                "rev_pickup": round(p_rev, 1), "rev_cancel": round(c_rev, 1),
            }

    # (a-2) 사업장×세그먼트별 전일 전체 — OTA/G-OTA/Inbound만 필터링
    ALLOWED_SEGMENTS = {"OTA", "G-OTA", "Inbound"}
    all_ps_props = sorted(set(list(pdbps.keys()) + list(cdbps.keys())))
    for prop in all_ps_props:
        p_segs = pdbps.get(prop, {})
        c_segs = cdbps.get(prop, {})
        all_segs = sorted(set(list(p_segs.keys()) + list(c_segs.keys())))
        prop_seg_data = {}
        for seg in all_segs:
            if seg not in ALLOWED_SEGMENTS:
                continue
            p_val = p_segs.get(seg, {}).get(today_date, {})
            c_val = c_segs.get(seg, {}).get(today_date, {})
            p_rn = p_val.get("rn", 0) or 0
            c_rn = c_val.get("rn", 0) or 0
            p_rev = p_val.get("rev", 0) or 0
            c_rev = c_val.get("rev", 0) or 0
            if p_rn or c_rn:
                prop_seg_data[seg] = {
                    "pickup": p_rn, "cancel": c_rn, "net": p_rn - c_rn,
                    "rev": round(p_rev - c_rev, 1),
                    "rev_pickup": round(p_rev, 1), "rev_cancel": round(c_rev, 1),
                }
        # Inbound가 데이터에 존재하는 사업장이면 0이라도 항상 표시
        for seg in ALLOWED_SEGMENTS:
            if seg not in prop_seg_data:
                # 해당 사업장에 이 세그먼트 데이터가 원본에 존재하면 0으로 포함
                if seg in p_segs or seg in c_segs:
                    prop_seg_data[seg] = {
                        "pickup": 0, "cancel": 0, "net": 0,
                        "rev": 0, "rev_pickup": 0, "rev_cancel": 0,
                    }
        if prop_seg_data:
            daily_analysis["byPropertySegment"][prop] = prop_seg_data

    # (a-3) byProperty를 OTA/G-OTA/Inbound 3개 세그먼트 합계로 재계산
    filtered_by_property = {}
    for prop, segs in daily_analysis["byPropertySegment"].items():
        tot_p, tot_c, tot_rp, tot_rc = 0, 0, 0.0, 0.0
        for seg_data in segs.values():
            tot_p += seg_data.get("pickup", 0)
            tot_c += seg_data.get("cancel", 0)
            tot_rp += seg_data.get("rev_pickup", 0)
            tot_rc += seg_data.get("rev_cancel", 0)
        if tot_p or tot_c:
            filtered_by_property[prop] = {
                "pickup": tot_p, "cancel": tot_c, "net": tot_p - tot_c,
                "rev": round(tot_rp - tot_rc, 1),
                "rev_pickup": round(tot_rp, 1), "rev_cancel": round(tot_rc, 1),
            }
    daily_analysis["byProperty"] = filtered_by_property

    # (b) 세그먼트별 전일 전체
    for seg in sorted(pds.keys()):
        p_val = pds[seg].get(today_date, {})
        c_val = cds.get(seg, {}).get(today_date, {})
        p_rn = p_val.get("rn", 0) or 0
        c_rn = c_val.get("rn", 0) or 0
        if p_rn or c_rn:
            daily_analysis["bySegment"][seg] = {"pickup": p_rn, "cancel": c_rn, "net": p_rn - c_rn}

    # (c-0) 세그먼트별 × 투숙월 전일
    pdsm = agg_data.get("pickup_daily_by_segment_month", {})
    cdsm = agg_data.get("cancel_daily_by_segment_month", {})
    compare_mkeys = {f"{now.year}{mi:02d}" for mi in compare_months}
    for mi in compare_months:
        mkey = f"{now.year}{mi:02d}"
        mlabel = f"{mi}월"
        month_segs = {}
        all_segs = sorted(set(list(pdsm.keys()) + list(cdsm.keys())))
        for seg in all_segs:
            p_val = pdsm.get(seg, {}).get(mkey, {}).get(today_date, {})
            c_val = cdsm.get(seg, {}).get(mkey, {}).get(today_date, {})
            p_rn = p_val.get("rn", 0) or 0
            c_rn = c_val.get("rn", 0) or 0
            if p_rn or c_rn:
                month_segs[seg] = {"pickup": p_rn, "cancel": c_rn, "net": p_rn - c_rn}
        if month_segs:
            daily_analysis["bySegmentMonth"][mlabel] = month_segs
    # "기타" = compare_months(4~6월) 이외 투숙월 합산
    etc_segs = {}
    all_segs = sorted(set(list(pdsm.keys()) + list(cdsm.keys())))
    for seg in all_segs:
        seg_months = set(pdsm.get(seg, {}).keys()) | set(cdsm.get(seg, {}).keys())
        for mkey in seg_months:
            if mkey in compare_mkeys or not mkey.startswith(str(now.year)):
                continue
            p_val = pdsm.get(seg, {}).get(mkey, {}).get(today_date, {})
            c_val = cdsm.get(seg, {}).get(mkey, {}).get(today_date, {})
            p_rn = p_val.get("rn", 0) or 0
            c_rn = c_val.get("rn", 0) or 0
            if p_rn or c_rn:
                prev = etc_segs.get(seg, {"pickup": 0, "cancel": 0, "net": 0})
                prev["pickup"] += p_rn; prev["cancel"] += c_rn; prev["net"] += p_rn - c_rn
                etc_segs[seg] = prev
    if etc_segs:
        daily_analysis["bySegmentMonth"]["기타"] = etc_segs

    # (b-2) 거래처(채널)별 전일 — OTA + G-OTA 채널만 (Inbound·기타 세그먼트 채널 제외)
    # 채널→세그먼트 매핑(`by_channel_segment`)에서 OTA/G-OTA로 분류된 채널만 포함.
    # 사업장별(OTA+G-OTA+Inbound) ≥ 채널별(OTA+G-OTA) 보장.
    pdbc = agg_data.get("pickup_daily_by_channel", {})
    cdbc = agg_data.get("cancel_daily_by_channel", {})
    bcs_map = agg_data.get("by_channel_segment", {})
    # by_channel_segment 구조: {channel: {segment: {month: data}}} (dict) 또는 {channel: [segments]} (list)
    # dict인 경우 키가 세그먼트 이름, list인 경우 원소가 세그먼트 이름
    OTA_GOTA_CHANNELS = set()
    for ch, segs in bcs_map.items():
        if isinstance(segs, list):
            if any(s in ("OTA", "G-OTA") for s in segs):
                OTA_GOTA_CHANNELS.add(ch)
        elif isinstance(segs, dict):
            if any(s in ("OTA", "G-OTA") for s in segs.keys()):
                OTA_GOTA_CHANNELS.add(ch)
    for ch in sorted(set(list(pdbc.keys()) + list(cdbc.keys()))):
        if ch not in OTA_GOTA_CHANNELS:
            continue
        p_val = pdbc.get(ch, {}).get(today_date, {})
        c_val = cdbc.get(ch, {}).get(today_date, {})
        p_rn = p_val.get("rn", 0) or 0
        c_rn = c_val.get("rn", 0) or 0
        p_rev = p_val.get("rev", 0) or 0
        c_rev = c_val.get("rev", 0) or 0
        if p_rn or c_rn:
            daily_analysis["byChannel"][ch] = {
                "pickup": p_rn, "cancel": c_rn, "net": p_rn - c_rn,
                "rev": round(p_rev - c_rev, 1),
                "rev_pickup": round(p_rev, 1), "rev_cancel": round(c_rev, 1),
            }

    # (b-3) 거래처(채널)별 × 투숙월 전일 — OTA + G-OTA 채널만
    pdbcm = agg_data.get("pickup_daily_by_channel_month", {})
    cdbcm = agg_data.get("cancel_daily_by_channel_month", {})
    for mi in compare_months:
        mkey = f"{now.year}{mi:02d}"
        mlabel = f"{mi}월"
        month_chs = {}
        all_chs = sorted(set(list(pdbcm.keys()) + list(cdbcm.keys())))
        for ch in all_chs:
            if ch not in OTA_GOTA_CHANNELS:
                continue
            p_val = pdbcm.get(ch, {}).get(mkey, {}).get(today_date, {})
            c_val = cdbcm.get(ch, {}).get(mkey, {}).get(today_date, {})
            p_rn = p_val.get("rn", 0) or 0
            c_rn = c_val.get("rn", 0) or 0
            p_rev = p_val.get("rev", 0) or 0
            c_rev = c_val.get("rev", 0) or 0
            if p_rn or c_rn:
                month_chs[ch] = {
                    "pickup": p_rn, "cancel": c_rn, "net": p_rn - c_rn,
                    "rev": round(p_rev - c_rev, 1),
                    "rev_pickup": round(p_rev, 1), "rev_cancel": round(c_rev, 1),
                }
        if month_chs:
            daily_analysis["byChannelMonth"][mlabel] = month_chs
    # "기타" 채널별
    etc_chs = {}
    all_chs_etc = sorted(set(list(pdbcm.keys()) + list(cdbcm.keys())))
    for ch in all_chs_etc:
        ch_months_p = set(pdbcm.get(ch, {}).keys())
        ch_months_c = set(cdbcm.get(ch, {}).keys())
        for mkey in ch_months_p | ch_months_c:
            if mkey in compare_mkeys or not mkey.startswith(str(now.year)):
                continue
            p_val = pdbcm.get(ch, {}).get(mkey, {}).get(today_date, {})
            c_val = cdbcm.get(ch, {}).get(mkey, {}).get(today_date, {})
            p_rn = p_val.get("rn", 0) or 0; c_rn = c_val.get("rn", 0) or 0
            p_rev = p_val.get("rev", 0) or 0; c_rev = c_val.get("rev", 0) or 0
            if p_rn or c_rn:
                prev = etc_chs.get(ch, {"pickup": 0, "cancel": 0, "net": 0, "rev": 0.0, "rev_pickup": 0.0, "rev_cancel": 0.0})
                prev["pickup"] += p_rn; prev["cancel"] += c_rn; prev["net"] += p_rn - c_rn
                prev["rev"] += round(p_rev - c_rev, 1); prev["rev_pickup"] += p_rev; prev["rev_cancel"] += c_rev
                etc_chs[ch] = prev
    if etc_chs:
        # round rev values
        for ch in etc_chs:
            etc_chs[ch]["rev"] = round(etc_chs[ch]["rev"], 1)
            etc_chs[ch]["rev_pickup"] = round(etc_chs[ch]["rev_pickup"], 1)
            etc_chs[ch]["rev_cancel"] = round(etc_chs[ch]["rev_cancel"], 1)
        # daily_analysis["byChannelMonth"]["기타"] = etc_chs

    # (c) 사업장별 × 투숙월 전일 — OTA/G-OTA/Inbound 3개 세그먼트만 합산
    all_psm_props = sorted(set(list(pdbpsm.keys()) + list(cdbpsm.keys())))
    for mi in compare_months:
        mkey = f"{now.year}{mi:02d}"
        mlabel = f"{mi}월"
        month_props = {}
        for prop in all_psm_props:
            tot_p, tot_c, tot_rp, tot_rc = 0, 0, 0.0, 0.0
            for seg in ALLOWED_SEGMENTS:
                p_val = pdbpsm.get(prop, {}).get(seg, {}).get(mkey, {}).get(today_date, {})
                c_val = cdbpsm.get(prop, {}).get(seg, {}).get(mkey, {}).get(today_date, {})
                tot_p += p_val.get("rn", 0) or 0
                tot_c += c_val.get("rn", 0) or 0
                tot_rp += p_val.get("rev", 0) or 0
                tot_rc += c_val.get("rev", 0) or 0
            if tot_p or tot_c:
                month_props[prop] = {
                    "pickup": tot_p, "cancel": tot_c, "net": tot_p - tot_c,
                    "rev": round(tot_rp - tot_rc, 1),
                    "rev_pickup": round(tot_rp, 1), "rev_cancel": round(tot_rc, 1),
                }
        if month_props:
            daily_analysis["byPropertyMonth"][mlabel] = month_props
    # "기타" 사업장별
    etc_props = {}
    for prop in all_psm_props:
        prop_months_p = set()
        prop_months_c = set()
        for seg in ALLOWED_SEGMENTS:
            prop_months_p |= set(pdbpsm.get(prop, {}).get(seg, {}).keys())
            prop_months_c |= set(cdbpsm.get(prop, {}).get(seg, {}).keys())
        for mkey in prop_months_p | prop_months_c:
            if mkey in compare_mkeys or not mkey.startswith(str(now.year)):
                continue
            for seg in ALLOWED_SEGMENTS:
                p_val = pdbpsm.get(prop, {}).get(seg, {}).get(mkey, {}).get(today_date, {})
                c_val = cdbpsm.get(prop, {}).get(seg, {}).get(mkey, {}).get(today_date, {})
                p_rn = p_val.get("rn", 0) or 0; c_rn = c_val.get("rn", 0) or 0
                p_rev = p_val.get("rev", 0) or 0; c_rev = c_val.get("rev", 0) or 0
                if p_rn or c_rn:
                    prev = etc_props.get(prop, {"pickup": 0, "cancel": 0, "net": 0, "rev": 0.0, "rev_pickup": 0.0, "rev_cancel": 0.0})
                    prev["pickup"] += p_rn; prev["cancel"] += c_rn; prev["net"] += p_rn - c_rn
                    prev["rev"] += p_rev - c_rev; prev["rev_pickup"] += p_rev; prev["rev_cancel"] += c_rev
                    etc_props[prop] = prev
    if etc_props:
        for prop in etc_props:
            etc_props[prop]["rev"] = round(etc_props[prop]["rev"], 1)
            etc_props[prop]["rev_pickup"] = round(etc_props[prop]["rev_pickup"], 1)
            etc_props[prop]["rev_cancel"] = round(etc_props[prop]["rev_cancel"], 1)
        daily_analysis["byPropertyMonth"]["기타"] = etc_props

    # (c-2) 사업장별 × 세그먼트 × 투숙월 전일 (서브행용)
    daily_analysis["byPropertySegmentMonth"] = {}
    for mi in compare_months:
        mkey = f"{now.year}{mi:02d}"
        mlabel = f"{mi}월"
        month_prop_segs = {}
        for prop in all_psm_props:
            prop_segs = {}
            for seg in ALLOWED_SEGMENTS:
                p_val = pdbpsm.get(prop, {}).get(seg, {}).get(mkey, {}).get(today_date, {})
                c_val = cdbpsm.get(prop, {}).get(seg, {}).get(mkey, {}).get(today_date, {})
                p_rn = p_val.get("rn", 0) or 0; c_rn = c_val.get("rn", 0) or 0
                p_rev = p_val.get("rev", 0) or 0; c_rev = c_val.get("rev", 0) or 0
                if p_rn or c_rn:
                    prop_segs[seg] = {
                        "pickup": p_rn, "cancel": c_rn, "net": p_rn - c_rn,
                        "rev": round(p_rev - c_rev, 1),
                        "rev_pickup": round(p_rev, 1), "rev_cancel": round(c_rev, 1),
                    }
            if prop_segs:
                month_prop_segs[prop] = prop_segs
        if month_prop_segs:
            daily_analysis["byPropertySegmentMonth"][mlabel] = month_prop_segs
    # "기타" 사업장별 세그먼트
    etc_prop_segs = {}
    for prop in all_psm_props:
        prop_segs = {}
        for seg in ALLOWED_SEGMENTS:
            seg_months = set(pdbpsm.get(prop, {}).get(seg, {}).keys()) | set(cdbpsm.get(prop, {}).get(seg, {}).keys())
            for mkey in seg_months:
                if mkey in compare_mkeys or not mkey.startswith(str(now.year)):
                    continue
                p_val = pdbpsm.get(prop, {}).get(seg, {}).get(mkey, {}).get(today_date, {})
                c_val = cdbpsm.get(prop, {}).get(seg, {}).get(mkey, {}).get(today_date, {})
                p_rn = p_val.get("rn", 0) or 0; c_rn = c_val.get("rn", 0) or 0
                p_rev = p_val.get("rev", 0) or 0; c_rev = c_val.get("rev", 0) or 0
                if p_rn or c_rn:
                    prev = prop_segs.get(seg, {"pickup": 0, "cancel": 0, "net": 0, "rev": 0.0, "rev_pickup": 0.0, "rev_cancel": 0.0})
                    prev["pickup"] += p_rn; prev["cancel"] += c_rn; prev["net"] += p_rn - c_rn
                    prev["rev"] += p_rev - c_rev; prev["rev_pickup"] += p_rev; prev["rev_cancel"] += c_rev
                    prop_segs[seg] = prev
        if prop_segs:
            for seg in prop_segs:
                prop_segs[seg]["rev"] = round(prop_segs[seg]["rev"], 1)
                prop_segs[seg]["rev_pickup"] = round(prop_segs[seg]["rev_pickup"], 1)
                prop_segs[seg]["rev_cancel"] = round(prop_segs[seg]["rev_cancel"], 1)
            etc_prop_segs[prop] = prop_segs
    if etc_prop_segs:
        daily_analysis["byPropertySegmentMonth"]["기타"] = etc_prop_segs

    # ── 5-X) 당일 분석 교차검증: 사업장별(OTA+G-OTA+Inbound) ≥ 채널별(OTA+G-OTA) ──
    # 사업장별 NET = 채널별 NET + Inbound NET 이어야 정합 (채널 분배는 비례 추정이라 ±오차 허용)
    daily_analysis["validation"] = _validate_daily_analysis_consistency(daily_analysis)

    # ── 6) 거래처별 주간 점유율 데이터 (최근 8주) ──
    from datetime import timedelta
    pdbc_full = agg_data.get("pickup_daily_by_channel", {})
    cdbc_full = agg_data.get("cancel_daily_by_channel", {})
    pdbcm_full = agg_data.get("pickup_daily_by_channel_month", {})
    cdbcm_full = agg_data.get("cancel_daily_by_channel_month", {})

    def _build_channel_weekly(p_data, c_data, is_month_keyed=False, month_key=None):
        """일별 채널 데이터를 주간 집계하여 거래처별 점유율 계산."""
        from collections import defaultdict
        # 모든 날짜 수집
        all_dates = set()
        for ch in list(p_data.keys()) + list(c_data.keys()):
            if is_month_keyed:
                dates_dict = p_data.get(ch, {}).get(month_key, {})
                all_dates.update(dates_dict.keys())
                dates_dict = c_data.get(ch, {}).get(month_key, {})
                all_dates.update(dates_dict.keys())
            else:
                all_dates.update(p_data.get(ch, {}).keys())
                all_dates.update(c_data.get(ch, {}).keys())

        if not all_dates:
            return []

        # 날짜 → ISO 주번호(월요일 시작) 매핑
        def date_to_week(d_str):
            """YYYYMMDD → (year, week_num) 월요일 시작"""
            from datetime import datetime
            dt = datetime.strptime(d_str, "%Y%m%d")
            iso = dt.isocalendar()
            return (iso[0], iso[1])

        def week_start_label(d_str):
            from datetime import datetime
            dt = datetime.strptime(d_str, "%Y%m%d")
            # 해당 날짜의 월요일
            monday = dt - timedelta(days=dt.weekday())
            return monday.strftime("%m/%d")

        # 주별 채널별 net 합산
        weekly_ch = defaultdict(lambda: defaultdict(int))  # {week_key: {channel: net}}
        week_labels = {}  # {week_key: label}

        all_channels = sorted(set(list(p_data.keys()) + list(c_data.keys())))
        for ch in all_channels:
            if is_month_keyed:
                p_dates = p_data.get(ch, {}).get(month_key, {})
                c_dates = c_data.get(ch, {}).get(month_key, {})
            else:
                p_dates = p_data.get(ch, {})
                c_dates = c_data.get(ch, {})

            for d in set(list(p_dates.keys()) + list(c_dates.keys())):
                if len(d) != 8:
                    continue
                p_rn = (p_dates.get(d, {}).get("rn", 0) or 0)
                c_rn = (c_dates.get(d, {}).get("rn", 0) or 0)
                net = p_rn - c_rn
                wk = date_to_week(d)
                weekly_ch[wk][ch] += net
                if wk not in week_labels:
                    week_labels[wk] = week_start_label(d)

        if not weekly_ch:
            return []

        # 최근 8주만
        sorted_weeks = sorted(weekly_ch.keys())[-8:]

        # 상위 6채널 + 기타 결정 (전체 주간 합계 기준, "기타" 원본 채널 제외)
        total_by_ch = defaultdict(int)
        for wk in sorted_weeks:
            for ch, net in weekly_ch[wk].items():
                if ch != "기타":
                    total_by_ch[ch] += abs(net)
        top_channels = sorted(total_by_ch.keys(), key=lambda c: total_by_ch[c], reverse=True)[:6]
        top_set = set(top_channels)

        # 주간 결과 배열 생성
        result = []
        for wk in sorted_weeks:
            entry = {"label": week_labels.get(wk, ""), "channels": {}}
            for ch in top_channels:
                v = weekly_ch[wk].get(ch, 0)
                entry["channels"][ch] = v
            # 기타 = 원본 "기타" + 상위 6에 안 든 나머지
            etc_val = sum(v for ch, v in weekly_ch[wk].items() if ch not in top_set)
            entry["channels"]["기타"] = etc_val
            entry["total"] = sum(entry["channels"].values())
            result.append(entry)
        return result

    channel_weekly_share = {
        "all": _build_channel_weekly(pdbc_full, cdbc_full),
    }
    for mi in compare_months:
        mkey = f"{now.year}{mi:02d}"
        mlabel = f"{mi}월"
        channel_weekly_share[mlabel] = _build_channel_weekly(pdbcm_full, cdbcm_full, is_month_keyed=True, month_key=mkey)

    # ── 7) 동기간(YoY) OTB 비교 ──
    # 4월(당월): yoy_adjusted (투숙일 기준 온북, 코드 27+43)
    # 5·6월(미래월): 예약일 기준 동기간 Net (pickup - cancel, 예약일 ≤ 기준일)
    yoy_adj = agg_data.get("yoy_adjusted", {})
    _cy_yr = str(now.year)
    _ly_yr = str(now.year - 1)
    _cy_yoy = yoy_adj.get(_cy_yr, {})
    _ly_yoy = yoy_adj.get(_ly_yr, {})
    yoy_otb = {}

    # 예약일 기준 동기간 비교용 cutoff (CY/LY 동일 월일)
    _cy_cutoff = f"{now.year}{now.month:02d}{now.day:02d}"      # e.g. 20260429
    _ly_cutoff = f"{now.year - 1}{now.month:02d}{now.day:02d}"  # e.g. 20250429

    def _safe_pct(cv, lv):
        """전년 대비 증감률 (%) — 분모 0 방어"""
        return round((cv - lv) / lv * 100, 1) if lv else None

    def _booking_date_net(pdbpm_src, cdbpm_src, month_key, cutoff):
        """예약일 기준 동기간 Net: pickup - cancel (예약일 ≤ cutoff) 를 사업장별로 계산.
        Returns (total_rn, total_rev_raw, {prop: {rn, rev}})"""
        prop_net = {}
        all_props = sorted(set(list(pdbpm_src.keys()) + list(cdbpm_src.keys())))
        total_rn = 0
        total_rev = 0.0
        for prop in all_props:
            p_month = pdbpm_src.get(prop, {}).get(month_key, {})
            c_month = cdbpm_src.get(prop, {}).get(month_key, {})
            p_rn = sum(v.get("rn", 0) for dt, v in p_month.items() if dt <= cutoff)
            c_rn = sum(v.get("rn", 0) for dt, v in c_month.items() if dt <= cutoff)
            p_rev = sum(v.get("rev", 0) for dt, v in p_month.items() if dt <= cutoff)
            c_rev = sum(v.get("rev", 0) for dt, v in c_month.items() if dt <= cutoff)
            net_rn = p_rn - c_rn
            net_rev = p_rev - c_rev
            if net_rn or net_rev:
                prop_net[prop] = {"rn": net_rn, "rev": net_rev}
            total_rn += net_rn
            total_rev += net_rev
        return total_rn, total_rev, prop_net

    for mi in compare_months:
        cy_mkey = f"{now.year}{mi:02d}"
        ly_mkey = f"{now.year - 1}{mi:02d}"

        if mi == cur_month:
            # ── 당월(4월): yoy_adjusted 투숙일 기준 (실적 확정) ──
            cy_m = _cy_yoy.get("by_month", {}).get(cy_mkey, {})
            ly_m = _ly_yoy.get("by_month", {}).get(ly_mkey, {})
            cy_rn = cy_m.get("booking_rn", 0) or 0
            ly_rn = ly_m.get("booking_rn", 0) or 0
            cy_rev = cy_m.get("booking_rev_m", 0) or 0
            ly_rev = ly_m.get("booking_rev_m", 0) or 0
            cy_adr = round(cy_rev * 1e6 / cy_rn / 1000, 1) if cy_rn else 0
            ly_adr = round(ly_rev * 1e6 / ly_rn / 1000, 1) if ly_rn else 0

            _bp_yoy = {}
            _cy_props = _cy_yoy.get("by_property", {})
            _ly_props = _ly_yoy.get("by_property", {})
            for prop in sorted(set(list(_cy_props.keys()) + list(_ly_props.keys()))):
                c_rn = _cy_props.get(prop, {}).get(cy_mkey, {}).get("booking_rn", 0) or 0
                l_rn = _ly_props.get(prop, {}).get(ly_mkey, {}).get("booking_rn", 0) or 0
                if c_rn or l_rn:
                    _bp_yoy[prop] = {"cy": c_rn, "ly": l_rn, "pct": _safe_pct(c_rn, l_rn)}
        else:
            # ── 미래월(5·6월): 예약일 기준 동기간 Net (pickup - cancel) ──
            cy_rn, cy_rev_raw, cy_bp = _booking_date_net(pdbpm, cdbpm, cy_mkey, _cy_cutoff)
            ly_rn, ly_rev_raw, ly_bp = _booking_date_net(pdbpm, cdbpm, ly_mkey, _ly_cutoff)
            cy_rev = round(cy_rev_raw / 1_000_000, 2) if cy_rev_raw else 0
            ly_rev = round(ly_rev_raw / 1_000_000, 2) if ly_rev_raw else 0
            cy_adr = round(cy_rev_raw / cy_rn / 1000, 1) if cy_rn else 0
            ly_adr = round(ly_rev_raw / ly_rn / 1000, 1) if ly_rn else 0

            _bp_yoy = {}
            all_bp = sorted(set(list(cy_bp.keys()) + list(ly_bp.keys())))
            for prop in all_bp:
                c_rn = cy_bp.get(prop, {}).get("rn", 0)
                l_rn = ly_bp.get(prop, {}).get("rn", 0)
                if c_rn or l_rn:
                    _bp_yoy[prop] = {"cy": c_rn, "ly": l_rn, "pct": _safe_pct(c_rn, l_rn)}

        mlabel = f"{mi}월"
        yoy_otb[mlabel] = {
            "cy_rn": cy_rn, "ly_rn": ly_rn, "rn_pct": _safe_pct(cy_rn, ly_rn),
            "cy_rev": round(cy_rev, 1), "ly_rev": round(ly_rev, 1), "rev_pct": _safe_pct(cy_rev, ly_rev),
            "cy_adr": cy_adr, "ly_adr": ly_adr, "adr_pct": _safe_pct(cy_adr, ly_adr),
            "byProperty": _bp_yoy,
        }

    # "전체" = 비교 대상 월 합산
    _a_cy_rn = sum(yoy_otb.get(f"{mi}월", {}).get("cy_rn", 0) for mi in compare_months)
    _a_ly_rn = sum(yoy_otb.get(f"{mi}월", {}).get("ly_rn", 0) for mi in compare_months)
    _a_cy_rev = sum(yoy_otb.get(f"{mi}월", {}).get("cy_rev", 0) for mi in compare_months)
    _a_ly_rev = sum(yoy_otb.get(f"{mi}월", {}).get("ly_rev", 0) for mi in compare_months)
    _a_cy_adr = round(_a_cy_rev * 1e6 / _a_cy_rn / 1000, 1) if _a_cy_rn else 0
    _a_ly_adr = round(_a_ly_rev * 1e6 / _a_ly_rn / 1000, 1) if _a_ly_rn else 0
    _a_bp = {}
    for mi in compare_months:
        for prop, pv in yoy_otb.get(f"{mi}월", {}).get("byProperty", {}).items():
            if prop not in _a_bp:
                _a_bp[prop] = {"cy": 0, "ly": 0}
            _a_bp[prop]["cy"] += pv.get("cy", 0)
            _a_bp[prop]["ly"] += pv.get("ly", 0)
    for prop in _a_bp:
        _a_bp[prop]["pct"] = _safe_pct(_a_bp[prop]["cy"], _a_bp[prop]["ly"])
    yoy_otb["all"] = {
        "cy_rn": _a_cy_rn, "ly_rn": _a_ly_rn, "rn_pct": _safe_pct(_a_cy_rn, _a_ly_rn),
        "cy_rev": round(_a_cy_rev, 1), "ly_rev": round(_a_ly_rev, 1), "rev_pct": _safe_pct(_a_cy_rev, _a_ly_rev),
        "cy_adr": _a_cy_adr, "ly_adr": _a_ly_adr, "adr_pct": _safe_pct(_a_cy_adr, _a_ly_adr),
        "byProperty": _a_bp,
    }
    logger.info(f"✓ YoY OTB 비교 계산: {len(yoy_otb)}개 탭 (전체+{len(compare_months)}개월), "
                f"사업장 {len(_a_bp)}개")

    insight_blob = {
        "todayDate": today_date,
        "todayLabel": f"{today_date[4:6]}/{today_date[6:]}" if len(today_date) == 8 else "",
        "segToday": seg_today,
        "stayMonthToday": stay_month_today,
        "dailyTrend": daily_trend,
        "dailyTrendLY": daily_trend_ly,
        "dailyTrendByMonth": daily_trend_by_month,
        "dailyTrendByMonthLY": daily_trend_by_month_ly,
        "segWeekly": seg_weekly,
        "yearsCompare": years_compare,
        "yoySummary": yoy_summary,
        "compareMonths": [f"{m}월" for m in compare_months] + (["기타"] if daily_analysis["byPropertyMonth"].get("기타") or daily_analysis["bySegmentMonth"].get("기타") else []),
        "dailyAnalysis": daily_analysis,
        "channelWeeklyShare": channel_weekly_share,
        "yoyExclusions": yoy_exclusions_all,
        "yoyExclusionsByMonth": yoy_exclusions_by_month,
        "yoyOtb": yoy_otb,
    }

    js_const = f"const INSIGHT_DATA = {_json.dumps(insight_blob, ensure_ascii=False)};"
    pattern = re.compile(r'const INSIGHT_DATA = \{.*?\};', re.DOTALL)
    if pattern.search(html):
        html = pattern.sub(js_const, html)
    else:
        html = html.replace("/*__INSIGHT_DATA__*/", js_const)

    # 당일분석 교차검증 결과를 JSON으로 영속화 → data-check.html이 fetch
    validation = daily_analysis.get("validation", {})
    if validation:
        try:
            v_path = DOCS_DIR / "data" / "daily_analysis_validation.json"
            v_path.write_text(
                _json.dumps({
                    "today_date": today_date,
                    "generated_at": now.strftime("%Y-%m-%d %H:%M KST"),
                    **validation,
                }, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"✗ daily_analysis_validation.json 저장 실패: {e}")

    logger.info(f"✓ 인사이트 패널 데이터 주입 (seg={len(seg_today)}, daily={len(daily_trend)}, years={len(years_compare)})")
    return html


def _apply_common_injections(html: str, notes: dict, data: dict, comp_data: dict, weekly_data: dict, now: datetime, agg_data: dict = None, admin_data: dict = None, otb_data: dict = None) -> str:
    """index.html과 otb.html 공통 주입 함수"""
    day_map = {0: "MON", 1: "TUE", 2: "WED", 3: "THU", 4: "FRI", 5: "SAT", 6: "SUN"}
    report_date = data.get("report_date", now.strftime("%Y-%m-%d"))
    try:
        dt = datetime.strptime(report_date, "%Y-%m-%d")
        display_date = dt.strftime("%Y.%m.%d")
        timestamp = f"{display_date} {day_map[dt.weekday()]} 08:00 KST"
    except ValueError:
        display_date = report_date
        timestamp = now.strftime("%Y.%m.%d %H:%M KST")

    html = inject_external_links(html, notes.get("external_links", {}))
    html = apply_tpl(html, "date", display_date)
    html = apply_tpl(html, "timestamp", timestamp)
    # 헤드라인: admin_input.selected_headline(1-indexed)로 today_headlines 배열에서 선택
    # 선택 없으면(0) today_headlines[0], 배열 없으면 today_headline.text fallback
    headlines = data.get("today_headlines", [])
    admin_sel = int((admin_data or {}).get("selected_headline", 0))
    if headlines and 1 <= admin_sel <= len(headlines):
        headline_text = headlines[admin_sel - 1].get("text", "")
    elif headlines:
        headline_text = headlines[0].get("text", "")
    else:
        headline_text = data.get("today_headline", {}).get("text", "")
    if headline_text:
        html = apply_tpl(html, "headline", headline_text)
    html = apply_tpl(html, "updated_by", f"by {data.get('_updated_by', 'GS팀 · Haein Kim Manager')}")
    html = inject_kpi_3months(html, notes.get("executive_kpi", {}))
    actions = data.get("action_alerts", {})
    for key in ("vivaldi", "central", "south", "apac"):
        text = actions.get(key, "")
        if text:
            html = apply_tpl(html, f"action-{key}", text)
    html = inject_ota_table(html, notes.get("major_ota_performance", {}))
    html = inject_signal_cards(html, notes.get("property_performance", {}))
    if agg_data:
        html = inject_weekly_onbook(html, agg_data)
    html = inject_competitor_section(html, comp_data)
    html = inject_weekly_report(html, weekly_data, agg_data, otb_data=otb_data, admin_data=admin_data)
    if otb_data:
        rm_fcst_data = load_json(DOCS_DIR / "data" / "rm_fcst.json")
        html = inject_yoy_property_table(html, otb_data, rm_fcst_data)
    if otb_data and agg_data:
        html = inject_insight_panel_data(html, otb_data, agg_data, now)
    build_meta = now.strftime("Auto-Built %Y-%m-%d %H:%M KST")
    html = apply_tpl(html, "build", build_meta)
    return html


# ─────────────────────────────────────────────
# YoY 사업장별 추이 테이블
# ─────────────────────────────────────────────
REGION_COLORS = {
    "vivaldi": "#d97a7a",
    "central": "#6ba3c4",
    "south":   "#7ab891",
    "apac":    "#a892c8",
}
REGION_LABELS = {
    "vivaldi": "비발디",
    "central": "중부",
    "south":   "남부",
    "apac":    "APAC",
}

def render_yoy_property_table(yoy_table: list, base_date: str, rm_fcst_data: dict = None) -> str:
    if not yoy_table:
        return "<p style='color:#888;font-size:12px;'>YoY 데이터 없음</p>"

    base_disp = f"{base_date[:4]}.{base_date[4:6]}.{base_date[6:]}" if len(base_date) == 8 else base_date
    months = [int(m.split('-')[1]) for m in STAY_MONTHS]
    month_labels = {int(m.split('-')[1]): f"{int(m.split('-')[1])}월" for m in STAY_MONTHS}
    rm_props = (rm_fcst_data or {}).get("properties", {})

    def arrow(yoy):
        if yoy is None:
            return "—"
        if yoy >= 3:
            return f'<span style="color:#4caf89;font-weight:700;">▲ {yoy:+.1f}%</span>'
        elif yoy <= -3:
            return f'<span style="color:#e05555;font-weight:700;">↓ {yoy:+.1f}%</span>'
        else:
            return f'<span style="color:#b0a060;font-weight:700;">→ {yoy:+.1f}%</span>'

    SEG_COLORS = {"OTA": "#6ba3c4", "G-OTA": "#7ab891", "Inbound": "#a892c8"}

    rows_html = ""
    for row in yoy_table:
        is_segment = row.get("is_segment", False)
        region = row.get("region", "")
        color  = REGION_COLORS.get(region, "#888")
        cells  = ""
        for m in months:
            md = row.get("months", {}).get(str(m), {})
            act   = md.get("act_rn", 0)
            last  = md.get("last_rn", 0)
            yoy   = md.get("yoy")
            arrow_html = arrow(yoy)

            if is_segment:
                # 세그먼트 행: 간결하게 실적/전년/YoY만
                bud = md.get("bud_rn", 0)
                bud_html = f'<div style="font-size:9px;color:#666;">목표 {bud:,}</div>' if bud > 0 else ""
                cells += (
                    f'<td style="padding:4px 10px;border-bottom:1px solid #2a2a2a;vertical-align:top;background:rgba(255,255,255,0.02);">'
                    f'<div style="font-size:11px;">{act:,}실</div>'
                    f'<div style="font-size:10px;color:#777;">전년 {last:,}실</div>'
                    f'<div style="font-size:11px;margin-top:2px;">{arrow_html}</div>'
                    f'{bud_html}'
                    f'</td>'
                )
            else:
                # 사업장 행: 풀 정보 (FCST, RM 포함)
                fcst  = md.get("rns_fcst", act)
                fach  = md.get("fcst_ach", 0.0)
                bud   = md.get("bud_rn", 0)
                if bud > 0 and fcst is not None and fach is not None:
                    fcst_html = (
                        f'<div style="font-size:10px;color:#a0a0c0;margin-top:2px;">'
                        f'FCST: {fcst:,}실 (목표대비 {fach:.1f}%)</div>'
                    )
                else:
                    fcst_html = ""
                rm_key = f"2026-{m:02d}"
                rm_entry = rm_props.get(row.get("name", ""), {}).get(rm_key, {})
                # RM FCST: 세그먼트 합(OTA+G-OTA+Inbound) 사용, 총량 사용 금지
                rm_segments = rm_entry.get("segments", {})
                if rm_segments:
                    seg_sum = sum(
                        s.get("rm_fcst_rn", 0) or 0
                        for s in rm_segments.values()
                    )
                    rm_rn = seg_sum if seg_sum > 0 else None
                else:
                    rm_rn = md.get("rm_fcst_rn")  # yoyTable 원본 폴백
                if rm_rn is not None and bud > 0:
                    rm_ach = round(rm_rn / bud * 100, 1)
                    rm_html = (
                        f'<div style="font-size:10px;color:#e8a256;margin-top:1px;">'
                        f'RM: {rm_rn:,}실 ({rm_ach:.1f}%)</div>'
                    )
                elif rm_rn is not None:
                    rm_html = (
                        f'<div style="font-size:10px;color:#e8a256;margin-top:1px;">'
                        f'RM: {rm_rn:,}실</div>'
                    )
                else:
                    rm_html = ""
                cells += (
                    f'<td style="padding:8px 10px;border-bottom:1px solid #333;vertical-align:top;">'
                    f'<div style="font-size:12px;">{act:,}실</div>'
                    f'<div style="font-size:11px;color:#888;">전년 {last:,}실</div>'
                    f'<div style="font-size:12px;margin-top:3px;">{arrow_html}</div>'
                    f'{fcst_html}'
                    f'{rm_html}'
                    f'</td>'
                )

        if is_segment:
            seg_name = row["name"]
            seg_color = SEG_COLORS.get(seg_name, "#888")
            rows_html += (
                f'<tr>'
                f'<td style="padding:4px 10px 4px 28px;border-bottom:1px solid #2a2a2a;white-space:nowrap;background:rgba(255,255,255,0.02);">'
                f'<span style="display:inline-block;width:5px;height:5px;border-radius:50%;background:{seg_color};margin-right:5px;vertical-align:middle;"></span>'
                f'<span style="font-size:11px;color:#aaa;">{seg_name}</span>'
                f'</td>'
                f'{cells}</tr>'
            )
        else:
            rows_html += (
                f'<tr>'
                f'<td style="padding:8px 10px;border-bottom:1px solid #333;white-space:nowrap;">'
                f'<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:{color};margin-right:6px;vertical-align:middle;"></span>'
                f'<span style="font-size:12px;font-weight:600;">{row["name"]}</span>'
                f'</td>'
                f'{cells}</tr>'
            )

    header_cells = "".join(
        f'<th style="padding:8px 10px;font-family:var(--mono,monospace);font-size:11px;'
        f'font-weight:600;letter-spacing:0.08em;border-bottom:2px solid #555;text-align:left;">'
        f'{month_labels[m]}</th>'
        for m in months
    )

    return (
        f'<div style="overflow-x:auto;">'
        f'<p style="font-family:monospace;font-size:10.5px;color:#888;margin-bottom:8px;">'
        f'동기간 보정 기준일: {base_disp} &nbsp;·&nbsp; ▲ 전년 동기 대비 개선 / → 유사 / ↓ 부진</p>'
        f'<table style="width:100%;border-collapse:collapse;font-family:var(--sans,sans-serif);">'
        f'<thead><tr>'
        f'<th style="padding:8px 10px;font-family:var(--mono,monospace);font-size:11px;'
        f'font-weight:600;letter-spacing:0.08em;border-bottom:2px solid #555;text-align:left;">사업장</th>'
        f'{header_cells}'
        f'</tr></thead>'
        f'<tbody>{rows_html}</tbody>'
        f'</table></div>'
    )


def inject_yoy_property_table(html: str, otb_data: dict, rm_fcst_data: dict = None) -> str:
    yoy_table = otb_data.get("yoyTable", [])
    base_date = otb_data.get("meta", {}).get("yoyBaseDate", "")

    # base_date가 비어있거나 현재 연도와 다르면 빌드 날짜(KST 오늘)로 폴백
    now_kst = datetime.now(KST)
    current_year = now_kst.strftime("%Y")
    if not base_date or not base_date.startswith(current_year):
        base_date = now_kst.strftime("%Y%m%d")
        logger.info(f"⚠ yoyBaseDate 폴백 → {base_date} (현재 연도 {current_year} 기준)")

    table_html = render_yoy_property_table(yoy_table, base_date, rm_fcst_data)
    pattern = re.compile(
        r'(<!-- YOY_PROP_TABLE_START -->)(.*?)(<!-- YOY_PROP_TABLE_END -->)',
        re.DOTALL
    )
    new_html, n = pattern.subn(
        lambda m: m.group(1) + "\n" + table_html + "\n" + m.group(3),
        html
    )
    if n > 0:
        logger.info(f"✓ YoY 사업장별 추이 테이블 주입: {len(yoy_table)}개 사업장")
    else:
        logger.warning("✗ YOY_PROP_TABLE 마커 미발견")
    return new_html


# ─────────────────────────────────────────────
# 패키지 트렌드 데이터 주입
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
# Daily Booking Report 섹션
# ─────────────────────────────────────────────
BOOKING_REGION_COLORS_MAP = {
    "vivaldi": "#d97a7a", "central": "#6ba3c4", "south": "#7ab891", "apac": "#a892c8",
}


def _occ_heatmap_color(occ: float) -> str:
    if occ >= 90:
        return "rgba(76,175,111,0.35)"
    elif occ >= 70:
        return "rgba(76,175,111,0.20)"
    elif occ >= 50:
        return "rgba(232,162,86,0.20)"
    elif occ >= 30:
        return "rgba(232,162,86,0.12)"
    elif occ > 0:
        return "rgba(229,115,115,0.15)"
    else:
        return "rgba(122,126,133,0.08)"


def _occ_text_color(occ: float) -> str:
    if occ >= 70:
        return "#4caf6f"
    elif occ >= 50:
        return "#e8a256"
    elif occ >= 30:
        return "#e0c070"
    elif occ > 0:
        return "#e57373"
    else:
        return "#7a7e85"


def _gauge_svg(pct: float, label: str, size: int = 90) -> str:
    import math
    clamped = max(0, min(pct, 150))
    angle = clamped / 150 * 180
    if pct >= 100:
        color = "#4caf6f"
    elif pct >= 80:
        color = "#e8a256"
    elif pct >= 60:
        color = "#e0c070"
    else:
        color = "#e57373"
    r = size * 0.38
    cx, cy = size / 2, size * 0.55
    start_x = cx - r
    start_y = cy
    end_angle_rad = math.radians(angle - 180)
    end_x = cx + r * math.cos(end_angle_rad)
    end_y = cy + r * math.sin(end_angle_rad)
    large_arc = 1 if angle > 180 else 0
    return (
        f'<svg width="{size}" height="{int(size*0.65)}" viewBox="0 0 {size} {int(size*0.65)}">'
        f'<path d="M {cx-r},{cy} A {r},{r} 0 0 1 {cx+r},{cy}" '
        f'fill="none" stroke="#333" stroke-width="5" stroke-linecap="round"/>'
        f'<path d="M {start_x},{start_y} A {r},{r} 0 {large_arc} 1 {end_x:.1f},{end_y:.1f}" '
        f'fill="none" stroke="{color}" stroke-width="5" stroke-linecap="round"/>'
        f'<text x="{cx}" y="{cy-2}" text-anchor="middle" '
        f'font-family="var(--mono,monospace)" font-size="12" font-weight="800" fill="{color}">'
        f'{pct:.0f}%</text>'
        f'<text x="{cx}" y="{cy+10}" text-anchor="middle" '
        f'font-family="var(--sans,sans-serif)" font-size="7" fill="#a8acb3">{label}</text>'
        f'</svg>'
    )


def render_daily_booking_section(booking_data: dict) -> str:
    if not booking_data:
        return ""
    meta = booking_data.get("meta", {})
    by_prop = booking_data.get("by_property", {})
    report_date = meta.get("report_date", "")
    months = meta.get("months", [])
    if not by_prop or not months:
        return ""

    gt = by_prop.get("Grand Total", {})
    gt_months = gt.get("months", {})

    # Grand Total 요약 카드
    gt_cards = ""
    for mk in months:
        md = gt_months.get(mk, {})
        m_label = f"{int(mk.split('-')[1])}월"
        rns = md.get("actual_rns", 0)
        budget = md.get("budget_rns", 0)
        ach = md.get("budget_achievement", 0)
        occ = md.get("occ_actual", 0)
        occ_chg = md.get("occ_yoy_change", 0)
        daily_chg = md.get("daily_change", 0)
        ach_color = "#4caf6f" if ach >= 90 else "#e8a256" if ach >= 70 else "#e57373"
        occ_dir_color = "#4caf6f" if occ_chg >= 0 else "#e57373"
        occ_dir = "▲" if occ_chg >= 0 else "▼"
        daily_dir = "▲" if daily_chg > 0 else "▼" if daily_chg < 0 else ""
        daily_color = "#4caf6f" if daily_chg > 0 else "#e57373" if daily_chg < 0 else "#7a7e85"
        is_current = (mk == months[0])
        opacity = "1" if is_current else "0.6"
        border = f"border:1px solid {ach_color}40;" if is_current else "border:1px solid var(--rule);"
        gt_cards += f'''
      <div style="flex:1;min-width:160px;background:var(--bg-card);{border}border-radius:6px;padding:14px 16px;opacity:{opacity};">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
          <span style="font-family:var(--mono);font-size:13px;font-weight:800;color:var(--ink);letter-spacing:0.08em;">{m_label}</span>
          <span style="font-family:var(--mono);font-size:10px;padding:2px 6px;background:{ach_color}20;color:{ach_color};border-radius:3px;font-weight:800;">{ach:.0f}%</span>
        </div>
        <div style="font-family:var(--mono);font-size:20px;font-weight:900;color:var(--ink);margin-bottom:4px;">{rns:,}<span style="font-size:11px;color:var(--ink-muted);font-weight:600;margin-left:4px;">/ {budget:,}</span></div>
        <div style="display:flex;gap:12px;align-items:center;">
          <span style="font-family:var(--mono);font-size:11px;color:{_occ_text_color(occ)};font-weight:700;">OCC {occ:.1f}%</span>
          <span style="font-family:var(--mono);font-size:10px;color:{occ_dir_color};font-weight:700;">{occ_dir}{abs(occ_chg):.1f}%p</span>
          <span style="font-family:var(--mono);font-size:10px;color:{daily_color};font-weight:700;">{daily_dir}{abs(daily_chg):,}</span>
        </div>
      </div>'''

    # OCC 히트맵
    props_for_heatmap = [(n, d) for n, d in by_prop.items() if n != "Grand Total"]
    month_headers = ""
    for mk in months:
        m_label = f"{int(mk.split('-')[1])}월"
        month_headers += (
            f'<th style="padding:8px 10px;font-family:var(--mono);font-size:11px;'
            f'font-weight:700;letter-spacing:0.08em;color:var(--ink-muted);text-align:center;'
            f'border-bottom:2px solid var(--rule-strong);">{m_label} OCC</th>'
        )
    heatmap_rows = ""
    for name, pdata in props_for_heatmap:
        r_color = BOOKING_REGION_COLORS_MAP.get(pdata.get("region", ""), "#888")
        ms = pdata.get("months", {})
        cells = ""
        for mk in months:
            md = ms.get(mk, {})
            occ = md.get("occ_actual", 0)
            occ_chg = md.get("occ_yoy_change", 0)
            bg = _occ_heatmap_color(occ)
            txt_color = _occ_text_color(occ)
            chg_html = ""
            if occ_chg != 0:
                chg_dir = "▲" if occ_chg > 0 else "▼"
                chg_color = "#4caf6f" if occ_chg > 0 else "#e57373"
                chg_html = f'<div style="font-size:9px;color:{chg_color};font-weight:700;">{chg_dir}{abs(occ_chg):.1f}%p</div>'
            cells += (
                f'<td style="padding:6px 8px;text-align:center;background:{bg};'
                f'border-bottom:1px solid var(--rule);vertical-align:middle;">'
                f'<div style="font-family:var(--mono);font-size:13px;font-weight:800;color:{txt_color};">{occ:.1f}%</div>'
                f'{chg_html}</td>'
            )
        heatmap_rows += (
            f'<tr style="transition:background 0.15s;" '
            f'onmouseover="this.style.background=\'var(--bg-hover)\'" '
            f'onmouseout="this.style.background=\'transparent\'">'
            f'<td style="padding:6px 10px;border-bottom:1px solid var(--rule);white-space:nowrap;">'
            f'<span style="display:inline-block;width:5px;height:5px;border-radius:50%;background:{r_color};margin-right:6px;vertical-align:middle;"></span>'
            f'<span style="font-family:var(--sans);font-size:12px;font-weight:600;color:var(--ink-soft);">{escape_html(name)}</span></td>'
            f'{cells}</tr>'
        )

    # Budget 달성률 게이지
    gauge_cards = ""
    for mk in months:
        m_label = f"{int(mk.split('-')[1])}월"
        md = gt_months.get(mk, {})
        ach = md.get("budget_achievement", 0)
        gauge_cards += f'<div style="text-align:center;">{_gauge_svg(ach, m_label, 90)}</div>'

    # YoY 사업장별 비교
    current_month = months[0] if months else ""
    yoy_list = []
    for name, pdata in props_for_heatmap:
        md = pdata.get("months", {}).get(current_month, {})
        yoy_list.append((name, pdata.get("region", ""), md))
    yoy_list.sort(key=lambda x: x[2].get("yoy_pct", 0), reverse=True)

    yoy_rows = ""
    for name, region, md in yoy_list:
        r_color = BOOKING_REGION_COLORS_MAP.get(region, "#888")
        actual = md.get("actual_rns", 0)
        ly = md.get("ly_actual", 0)
        yoy_pct = md.get("yoy_pct", 0)
        ach = md.get("budget_achievement", 0)
        daily_chg = md.get("daily_change", 0)
        if yoy_pct > 3:
            yoy_color, yoy_icon = "#4caf6f", "▲"
        elif yoy_pct < -3:
            yoy_color, yoy_icon = "#e57373", "▼"
        else:
            yoy_color, yoy_icon = "#e0c070", "→"
        ach_color = "#4caf6f" if ach >= 90 else "#e8a256" if ach >= 70 else "#e57373"
        daily_html = ""
        if daily_chg != 0:
            d_dir = "▲" if daily_chg > 0 else "▼"
            d_color = "#4caf6f" if daily_chg > 0 else "#e57373"
            daily_html = f'<span style="font-family:var(--mono);font-size:10px;color:{d_color};font-weight:700;">{d_dir}{abs(daily_chg):,}</span>'
        yoy_rows += (
            f'<tr style="border-bottom:1px solid var(--rule);transition:background 0.15s;" '
            f'onmouseover="this.style.background=\'var(--bg-hover)\'" '
            f'onmouseout="this.style.background=\'transparent\'">'
            f'<td style="padding:8px 10px;white-space:nowrap;">'
            f'<span style="display:inline-block;width:5px;height:5px;border-radius:50%;background:{r_color};margin-right:6px;vertical-align:middle;"></span>'
            f'<span style="font-size:12px;font-weight:600;">{escape_html(name)}</span></td>'
            f'<td style="padding:8px 10px;text-align:right;font-family:var(--mono);font-size:13px;font-weight:800;">{actual:,}</td>'
            f'<td style="padding:8px 10px;text-align:right;font-family:var(--mono);font-size:11px;color:var(--ink-muted);">{ly:,}</td>'
            f'<td style="padding:8px 10px;text-align:center;">'
            f'<span style="font-family:var(--mono);font-size:12px;color:{yoy_color};font-weight:800;">{yoy_icon} {yoy_pct:+.1f}%</span></td>'
            f'<td style="padding:8px 10px;text-align:center;">'
            f'<span style="font-family:var(--mono);font-size:11px;color:{ach_color};font-weight:700;">{ach:.0f}%</span></td>'
            f'<td style="padding:8px 10px;text-align:center;">{daily_html}</td>'
            f'</tr>'
        )

    cm_label = f"{int(current_month.split('-')[1])}월" if current_month else ""

    section_html = f'''
<section id="sec-daily-booking" style="max-width:1640px;margin:0 auto;padding:20px 28px 12px;">
  <div style="border-top:2px solid var(--ink);padding-top:20px;margin-bottom:20px;display:flex;justify-content:space-between;align-items:end;gap:20px;flex-wrap:wrap;">
    <div>
      <div style="font-family:var(--mono);font-size:12.5px;letter-spacing:0.2em;color:var(--ink-muted);margin-bottom:6px;">DAILY BOOKING REPORT &middot; {report_date}</div>
      <h2 style="font-family:var(--serif);font-weight:800;font-size:clamp(24px,3.2vw,34px);line-height:1.05;color:var(--ink);">Daily Booking <em style="font-family:var(--script);font-style:normal;font-weight:400;color:var(--gold);padding-left:10px;">예약 현황</em></h2>
    </div>
    <div style="font-family:var(--mono);font-size:12px;color:var(--ink-muted);letter-spacing:0.1em;text-align:right;">
      {meta.get("property_count", 0)}개 사업장 &middot; 4개월 예약 현황<br>
      <span style="color:var(--gold);font-weight:700;">투숙일 기준 (27+43)</span>
    </div>
  </div>
  <div style="display:flex;gap:12px;margin-bottom:24px;flex-wrap:wrap;">{gt_cards}
  </div>
  <div style="background:var(--bg-card);border:1px solid var(--rule);border-radius:6px;padding:16px 20px;margin-bottom:24px;">
    <div style="font-family:var(--mono);font-size:11px;color:var(--ink-muted);font-weight:700;letter-spacing:0.08em;margin-bottom:12px;">BUDGET ACHIEVEMENT &mdash; 전사 달성률</div>
    <div style="display:flex;justify-content:space-around;align-items:center;flex-wrap:wrap;gap:8px;">{gauge_cards}</div>
  </div>
  <div style="background:var(--bg-card);border:1px solid var(--rule);border-radius:6px;padding:16px 0;margin-bottom:24px;overflow-x:auto;">
    <div style="padding:0 20px 10px;font-family:var(--mono);font-size:11px;color:var(--ink-muted);font-weight:700;letter-spacing:0.08em;">OCCUPANCY HEATMAP &mdash; 사업장별 OCC%</div>
    <table style="width:100%;border-collapse:collapse;font-family:var(--sans);min-width:600px;">
      <thead><tr>
        <th style="padding:8px 10px;font-family:var(--mono);font-size:11px;font-weight:700;letter-spacing:0.08em;color:var(--ink-muted);text-align:left;border-bottom:2px solid var(--rule-strong);">사업장</th>
        {month_headers}
      </tr></thead>
      <tbody>{heatmap_rows}</tbody>
    </table>
  </div>
  <div style="background:var(--bg-card);border:1px solid var(--rule);border-radius:6px;padding:16px 0;overflow-x:auto;">
    <div style="padding:0 20px 10px;font-family:var(--mono);font-size:11px;color:var(--ink-muted);font-weight:700;letter-spacing:0.08em;">YoY COMPARISON &mdash; {cm_label} 사업장별 전년 동기 대비</div>
    <table style="width:100%;border-collapse:collapse;font-family:var(--sans);min-width:600px;">
      <thead><tr style="border-bottom:2px solid var(--rule-strong);">
        <th style="padding:8px 10px;font-family:var(--mono);font-size:11px;font-weight:700;letter-spacing:0.08em;color:var(--ink-muted);text-align:left;">사업장</th>
        <th style="padding:8px 10px;font-family:var(--mono);font-size:11px;font-weight:700;letter-spacing:0.08em;color:var(--ink-muted);text-align:right;">금년</th>
        <th style="padding:8px 10px;font-family:var(--mono);font-size:11px;font-weight:700;letter-spacing:0.08em;color:var(--ink-muted);text-align:right;">전년</th>
        <th style="padding:8px 10px;font-family:var(--mono);font-size:11px;font-weight:700;letter-spacing:0.08em;color:var(--ink-muted);text-align:center;">YoY</th>
        <th style="padding:8px 10px;font-family:var(--mono);font-size:11px;font-weight:700;letter-spacing:0.08em;color:var(--ink-muted);text-align:center;">달성률</th>
        <th style="padding:8px 10px;font-family:var(--mono);font-size:11px;font-weight:700;letter-spacing:0.08em;color:var(--ink-muted);text-align:center;">당일</th>
      </tr></thead>
      <tbody>{yoy_rows}</tbody>
    </table>
  </div>
</section>
'''
    return section_html


def inject_daily_booking(html: str, booking_data: dict) -> str:
    if not booking_data:
        logger.info("  Daily Booking 데이터 없음 - 스킵")
        return html
    section_html = render_daily_booking_section(booking_data)
    if not section_html:
        return html
    pattern = re.compile(
        r'(<!-- DAILY_BOOKING_START -->)(.*?)(<!-- DAILY_BOOKING_END -->)',
        re.DOTALL
    )
    new_html, n = pattern.subn(
        lambda m: m.group(1) + "\n" + section_html + "\n" + m.group(3),
        html, count=1
    )
    if n > 0:
        prop_count = booking_data.get("meta", {}).get("property_count", 0)
        m_count = len(booking_data.get("meta", {}).get("months", []))
        logger.info(f"✓ Daily Booking 섹션 주입: {prop_count}개 사업장, {m_count}개월")
    else:
        logger.warning("✗ DAILY_BOOKING 마커 미발견")
    return new_html


def inject_package_data(html: str, pkg_data: dict) -> str:
    """package_series_trend.json → <!-- PKG_DATA_START/END --> 사이에 주입"""
    if not pkg_data:
        logger.info("  패키지 데이터 없음 — 스킵")
        return html
    json_str = json.dumps(pkg_data, ensure_ascii=False, separators=(',', ':'))
    script_tag = f"<script>const PKG_TREND_DATA = {json_str};</script>"
    pattern = re.compile(
        r'(<!-- PKG_DATA_START -->)(.*?)(<!-- PKG_DATA_END -->)',
        re.DOTALL
    )
    new_html, n = pattern.subn(
        lambda m: m.group(1) + "\n" + script_tag + "\n" + m.group(3),
        html, count=1
    )
    if n > 0:
        total = pkg_data.get("meta", {}).get("total_series", 0)
        top = pkg_data.get("meta", {}).get("top_series_count", 0)
        logger.info(f"✓ 패키지 트렌드 데이터 주입: 전체 {total:,}계열, TOP {top} 표시")
    else:
        logger.warning("✗ PKG_DATA 마커 미발견")
    return new_html


def main():
    logger.info("=" * 60)
    logger.info("V7 대시보드 빌드 (index.html + otb.html)")
    logger.info("=" * 60)

    enriched = load_json(DATA_DIR / "enriched_notes.json")
    notes = load_json(DATA_DIR / "daily_notes.json")
    news_data = load_json(DATA_DIR / "news_latest.json")
    comp_data = load_json(DATA_DIR / "competitors.json")
    weekly_data = load_json(DATA_DIR / "weekly_report.json")
    pkg_data = load_json(DATA_DIR / "package_series_trend.json")
    booking_data = load_json(DATA_DIR / "daily_booking.json")
    # ── 채널(거래처)별 일별 데이터 패치 (byChannel 생성) ──
    patch_script = Path(__file__).resolve().parent / "patch_channel_daily.py"
    if patch_script.exists():
        try:
            result = subprocess.run(
                [sys.executable, str(patch_script)],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode == 0:
                logger.info(f"✓ patch_channel_daily 완료: {result.stdout.strip().splitlines()[-1] if result.stdout.strip() else 'OK'}")
            else:
                logger.warning(f"✗ patch_channel_daily 실패: {result.stderr.strip()}")
        except Exception as e:
            logger.warning(f"✗ patch_channel_daily 실행 오류: {e}")

    agg_data = load_json(DATA_DIR / "db_aggregated.json")
    admin_data = load_json(DATA_DIR / "admin_input.json")
    otb_data = load_json(DOCS_DIR / "data" / "otb_data.json")
    rm_fcst = load_json(DOCS_DIR / "data" / "rm_fcst.json")

    # ── admin_suggestions.json 자동 생성 (데���리 인사이트 문구) ──
    if otb_data:
        try:
            suggestions = build_admin_suggestions(
                otb_data, weekly_data, rm_fcst,
                news_data=news_data, comp_data=comp_data,
            )
            suggestions_path = DOCS_DIR / "admin_suggestions.json"
            suggestions_path.write_text(
                json.dumps(suggestions, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.info(f"✓ admin_suggestions.json 생성 ({len(suggestions.get('insights', []))}개 인사이트)")

            # admin_input.json에도 daily_insights 필드 동기화
            admin_data["daily_insights"] = suggestions.get("insights", [])
            (DATA_DIR / "admin_input.json").write_text(
                json.dumps(admin_data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            # docs/data/에도 admin_input.json 동기화 (index.html에서 fetch용)
            docs_admin_path = DOCS_DIR / "data" / "admin_input.json"
            docs_admin_path.parent.mkdir(parents=True, exist_ok=True)
            docs_admin_path.write_text(
                json.dumps(admin_data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.info("✓ admin_input.json daily_insights 동기화 완료")
        except Exception as e:
            logger.warning(f"✗ admin_suggestions.json 생성 실패: {e}")

    data = enriched if enriched else notes
    if not data:
        logger.error("데이터 없음")
        sys.exit(1)

    if not HTML_FILE.exists():
        logger.error(f"HTML 템플릿 없음: {HTML_FILE}")
        sys.exit(1)

    now = datetime.now(KST)

    # ── index.html 빌드 ──
    html = HTML_FILE.read_text(encoding="utf-8")
    logger.info(f"✓ index.html 로드 ({len(html):,} bytes)")
    html = _apply_common_injections(html, notes, data, comp_data, weekly_data, now, agg_data, admin_data, otb_data=otb_data)
    html = inject_news_section(html, news_data)
    html = inject_daily_booking(html, booking_data)
    html = inject_package_data(html, pkg_data)
    HTML_FILE.write_text(html, encoding="utf-8")
    logger.info(f"✓ index.html 빌드 완료 ({len(html):,} bytes)")

    # ── otb.html 빌드 ──
    if OTB_FILE.exists():
        otb_html = OTB_FILE.read_text(encoding="utf-8")
        logger.info(f"✓ otb.html 로드 ({len(otb_html):,} bytes)")
        otb_html = _apply_common_injections(otb_html, notes, data, comp_data, weekly_data, now, otb_data=otb_data)
        OTB_FILE.write_text(otb_html, encoding="utf-8")
        logger.info(f"✓ otb.html 빌드 완료 ({len(otb_html):,} bytes)")
    else:
        logger.info("  otb.html 없음 - 스킵")

    # ── docs/data/db_aggregated.json에 net_daily·monthly_total 동기화 ──
    # 프론트엔드 JS가 docs/data/db_aggregated.json을 fetch하므로,
    # 원본(data/db_aggregated.json)의 net_daily·monthly_total을 항상 동기화해야 함
    docs_agg_path = DOCS_DIR / "data" / "db_aggregated.json"
    if agg_data and docs_agg_path.exists():
        try:
            docs_agg = load_json(docs_agg_path)
            synced_keys = []
            for key in ("net_daily", "monthly_total", "pickup_daily", "net_daily_by_month",
                         "by_segment", "by_region_segment", "by_property_segment", "meta",
                         "yoy_adjusted",
                         "pickup_daily_by_channel", "cancel_daily_by_channel",
                         "pickup_daily_by_channel_month", "cancel_daily_by_channel_month",
                         "by_channel_segment",
                         "net_daily_by_segment", "net_daily_by_month_seg",
                         "product_detail"):
                if key in agg_data:
                    docs_agg[key] = agg_data[key]
                    synced_keys.append(key)
            docs_agg["generated_at"] = agg_data.get("generated_at", "")
            docs_agg_path.write_text(
                json.dumps(docs_agg, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            logger.info(f"✓ docs/data/db_aggregated.json 동기화: {', '.join(synced_keys)}")
        except Exception as e:
            logger.warning(f"✗ docs/data 동기화 실패: {e}")

    # ── docs/data/package_series_trend.json 동기화 ──
    # 프론트엔드(product-detail.html)가 by_category·by_year_ranking 등을 fetch하므로,
    # parse_package_trend.py가 생성한 원본(data/)을 항상 docs/data/에 동기화해야 함
    src_pkg_path = DATA_DIR / "package_series_trend.json"
    dst_pkg_path = DOCS_DIR / "data" / "package_series_trend.json"
    if src_pkg_path.exists():
        try:
            import shutil
            dst_pkg_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src_pkg_path), str(dst_pkg_path))
            logger.info(f"✓ docs/data/package_series_trend.json 동기화 완료")
        except Exception as e:
            logger.warning(f"✗ package_series_trend.json 동기화 실패: {e}")

    # ── docs/data/rm_fcst.json 동기화 ──
    # fcst-admin.html이 RM FCST 열에서 사용 (사업장×월별 Revenue Meeting 전망 RN).
    # scripts/parse_rm_fcst.py가 최신 PDF로부터 data/rm_fcst.json을 생성한다.
    src_rm_path = DATA_DIR / "rm_fcst.json"
    dst_rm_path = DOCS_DIR / "data" / "rm_fcst.json"
    if src_rm_path.exists():
        try:
            import shutil
            dst_rm_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src_rm_path), str(dst_rm_path))
            logger.info(f"✓ docs/data/rm_fcst.json 동기화 완료")
        except Exception as e:
            logger.warning(f"✗ rm_fcst.json 동기화 실패: {e}")

    # ── 해외사업장 데이터 파싱 (overseas_data.json) ──
    overseas_script = Path(__file__).resolve().parent / "parse_overseas.py"
    if overseas_script.exists():
        try:
            result = subprocess.run(
                [sys.executable, str(overseas_script)],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode == 0:
                logger.info(f"✓ parse_overseas 완료")
            else:
                logger.warning(f"✗ parse_overseas 실패: {result.stderr.strip()}")
        except Exception as e:
            logger.warning(f"✗ parse_overseas 실행 오류: {e}")

    build_meta = now.strftime("Auto-Built %Y-%m-%d %H:%M KST")
    logger.info("=" * 60)
    logger.info(f"✓ 전체 빌드 완료 · {build_meta}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
