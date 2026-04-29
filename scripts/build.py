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
# 권역 카드 월별 실적/목표/달성률 주입
# ─────────────────────────────────────────────
def inject_region_monthly(html: str, prop_data: dict) -> str:
    """권역 카드에 월별 실적/목표/달성률 3행 표 주입"""
    month_labels = {"2026-04": "4월", "2026-05": "5월", "2026-06": "6월"}

    for region_key in ("vivaldi", "central", "south", "apac"):
        properties = prop_data.get(region_key, [])

        # 월별 집계
        totals = {}
        for month_key in STAY_MONTHS:
            actual = sum(p.get(month_key, {}).get("rns", 0) for p in properties)
            target = sum(p.get(month_key, {}).get("target_rns", 0) for p in properties)
            if target > 0:
                rate = actual / target * 100
            else:
                rate = None
            totals[month_key] = {"actual": actual, "target": target, "rate": rate}

        # HTML 행 생성
        rows = []
        for m_idx, month_key in enumerate(STAY_MONTHS):
            label = month_labels[month_key]
            d = totals[month_key]
            actual_str = f"{d['actual']:,}"
            target_str = f"{d['target']:,}" if d["target"] > 0 else "—"
            if d["rate"] is not None:
                rate_val = d["rate"]
                if rate_val >= 100:
                    rate_color = "#6dd396"
                elif rate_val >= 85:
                    rate_color = "#e6b960"
                else:
                    rate_color = "#e08580"
                rate_str = f"{rate_val:.1f}%"
            else:
                rate_color = "rgba(255,255,255,0.35)"
                rate_str = "—%"

            # 당월(4월) 진하게, 나머지 연하게
            opacity = "1" if m_idx == 0 else "0.55"
            font_weight = "700" if m_idx == 0 else "400"

            rows.append(
                f'<div style="display:flex;justify-content:space-between;align-items:center;'
                f'font-family:var(--mono);font-size:10.5px;line-height:1.7;opacity:{opacity};">'
                f'<span style="color:rgba(255,255,255,0.55);min-width:22px;font-weight:{font_weight};">{label}</span>'
                f'<span style="color:rgba(255,255,255,0.85);flex:1;text-align:right;padding-right:6px;'
                f'font-weight:{font_weight};">{actual_str}<span style="font-size:9px;opacity:0.6;">실</span>'
                f'&nbsp;/&nbsp;{target_str}<span style="font-size:9px;opacity:0.6;">실</span></span>'
                f'<span style="color:{rate_color};min-width:38px;text-align:right;font-weight:700;">{rate_str}</span>'
                f'</div>'
            )

        inner_html = "\n          ".join(rows)
        marker_key = region_key.upper()
        pattern = re.compile(
            rf'(<!-- REGION_MONTHLY_{marker_key}_START -->)(.*?)(<!-- REGION_MONTHLY_{marker_key}_END -->)',
            re.DOTALL
        )
        new_html, n = pattern.subn(
            lambda m: m.group(1) + "\n          " + inner_html + "\n          " + m.group(3),
            html, count=1
        )
        if n > 0:
            html = new_html
            logger.info(f"✓ 권역 월별 주입: {region_key}")
        else:
            logger.warning(f"✗ 권역 마커 미발견: {region_key}")

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
                s_props = sorted(m_props, key=lambda x: x.get("rns_achievement", 0), reverse=True)
                t3 = [{"n": p["name"].split(".",1)[-1], "a": p.get("rns_achievement", 0)} for p in s_props[:3]]
                b3 = [{"n": p["name"].split(".",1)[-1], "a": p.get("rns_achievement", 0)} for p in s_props[-3:] if p.get("rns_achievement", 0) < 100]
                tb_by_month[mkey] = {"top": t3, "bot": b3}
            else:
                tb_by_month[mkey] = {"top": [], "bot": []}
        # 당월 기본 표시
        cur_tb = tb_by_month.get(str(cur_month), {"top": [], "bot": []})
        top_items = " / ".join(f'{p["n"]} {p["a"]}%' for p in cur_tb["top"]) or "—"
        bot_items = " / ".join(f'{p["n"]} {p["a"]}%' for p in cur_tb["bot"]) or "—"
        html = apply_tpl(html, "otb-top3", top_items)
        html = apply_tpl(html, "otb-bot3", bot_items)
        # JSON 주입 (JS 토글용)
        tb_json = _json.dumps(tb_by_month, ensure_ascii=False)
        html = html.replace("/*__TB_BY_MONTH__*/", f"const TB_BY_MONTH = {tb_json};")
        # 월 라벨 주입
        month_labels_json = _json.dumps({mkey: f"{int(mkey)}월" for mkey in month_keys}, ensure_ascii=False)
        html = html.replace("/*__TB_MONTH_LABELS__*/", f"const TB_MONTH_LABELS = {month_labels_json};")

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
    full_seg = all_months.get("0", {}).get("segmentData", {})
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

    # (b-2) 거래처(채널)별 전일 전체 (매출 포함, Inbound 제외)
    pdbc = agg_data.get("pickup_daily_by_channel", {})
    cdbc = agg_data.get("cancel_daily_by_channel", {})
    # Inbound 세그먼트의 전일 총 pickup/cancel 계산 (채널 총합에서 차감용)
    _ib_p_val = pds.get("Inbound", {}).get(today_date, {})
    _ib_c_val = cds.get("Inbound", {}).get(today_date, {})
    _ib_total_p_rn = _ib_p_val.get("rn", 0) or 0
    _ib_total_c_rn = _ib_c_val.get("rn", 0) or 0
    _ib_total_p_rev = _ib_p_val.get("rev", 0) or 0
    _ib_total_c_rev = _ib_c_val.get("rev", 0) or 0
    # 채널별 합계 먼저 계산
    _ch_total_p = sum((pdbc.get(ch, {}).get(today_date, {}).get("rn", 0) or 0) for ch in pdbc)
    _ch_total_c = sum((cdbc.get(ch, {}).get(today_date, {}).get("rn", 0) or 0) for ch in cdbc)
    for ch in sorted(set(list(pdbc.keys()) + list(cdbc.keys()))):
        p_val = pdbc.get(ch, {}).get(today_date, {})
        c_val = cdbc.get(ch, {}).get(today_date, {})
        p_rn = p_val.get("rn", 0) or 0
        c_rn = c_val.get("rn", 0) or 0
        p_rev = p_val.get("rev", 0) or 0
        c_rev = c_val.get("rev", 0) or 0
        # Inbound 비율 차감 (채널 비율 기반)
        if _ib_total_p_rn and _ch_total_p:
            ib_ratio_p = p_rn / _ch_total_p
            p_rn -= round(_ib_total_p_rn * ib_ratio_p)
            p_rev -= _ib_total_p_rev * ib_ratio_p
        if _ib_total_c_rn and _ch_total_c:
            ib_ratio_c = c_rn / _ch_total_c
            c_rn -= round(_ib_total_c_rn * ib_ratio_c)
            c_rev -= _ib_total_c_rev * ib_ratio_c
        if p_rn or c_rn:
            daily_analysis["byChannel"][ch] = {
                "pickup": p_rn, "cancel": c_rn, "net": p_rn - c_rn,
                "rev": round(p_rev - c_rev, 1),
                "rev_pickup": round(p_rev, 1), "rev_cancel": round(c_rev, 1),
            }

    # (b-3) 거래처(채널)별 × 투숙월 전일 (매출 포함, Inbound 제외)
    pdbcm = agg_data.get("pickup_daily_by_channel_month", {})
    cdbcm = agg_data.get("cancel_daily_by_channel_month", {})
    for mi in compare_months:
        mkey = f"{now.year}{mi:02d}"
        mlabel = f"{mi}월"
        month_chs = {}
        all_chs = sorted(set(list(pdbcm.keys()) + list(cdbcm.keys())))
        # Inbound 세그먼트 월별 전일 (차감용)
        _ib_mp = pdsm.get("Inbound", {}).get(mkey, {}).get(today_date, {})
        _ib_mc = cdsm.get("Inbound", {}).get(mkey, {}).get(today_date, {})
        _ib_mp_rn = _ib_mp.get("rn", 0) or 0
        _ib_mc_rn = _ib_mc.get("rn", 0) or 0
        _ib_mp_rev = _ib_mp.get("rev", 0) or 0
        _ib_mc_rev = _ib_mc.get("rev", 0) or 0
        # 월별 채널 합계
        _chm_total_p = sum((pdbcm.get(ch, {}).get(mkey, {}).get(today_date, {}).get("rn", 0) or 0) for ch in all_chs)
        _chm_total_c = sum((cdbcm.get(ch, {}).get(mkey, {}).get(today_date, {}).get("rn", 0) or 0) for ch in all_chs)
        for ch in all_chs:
            p_val = pdbcm.get(ch, {}).get(mkey, {}).get(today_date, {})
            c_val = cdbcm.get(ch, {}).get(mkey, {}).get(today_date, {})
            p_rn = p_val.get("rn", 0) or 0
            c_rn = c_val.get("rn", 0) or 0
            p_rev = p_val.get("rev", 0) or 0
            c_rev = c_val.get("rev", 0) or 0
            # Inbound 비율 차감
            if _ib_mp_rn and _chm_total_p:
                ib_r = p_rn / _chm_total_p
                p_rn -= round(_ib_mp_rn * ib_r)
                p_rev -= _ib_mp_rev * ib_r
            if _ib_mc_rn and _chm_total_c:
                ib_r = c_rn / _chm_total_c
                c_rn -= round(_ib_mc_rn * ib_r)
                c_rev -= _ib_mc_rev * ib_r
            if p_rn or c_rn:
                month_chs[ch] = {
                    "pickup": p_rn, "cancel": c_rn, "net": p_rn - c_rn,
                    "rev": round(p_rev - c_rev, 1),
                    "rev_pickup": round(p_rev, 1), "rev_cancel": round(c_rev, 1),
                }
        if month_chs:
            daily_analysis["byChannelMonth"][mlabel] = month_chs

    # (c) 사업장별 × 투숙월 전일
    for mi in compare_months:
        mkey = f"{now.year}{mi:02d}"
        mlabel = f"{mi}월"
        month_props = {}
        for prop in sorted(pdbpm.keys()):
            p_val = pdbpm[prop].get(mkey, {}).get(today_date, {})
            c_val = cdbpm.get(prop, {}).get(mkey, {}).get(today_date, {})
            p_rn = p_val.get("rn", 0) or 0
            c_rn = c_val.get("rn", 0) or 0
            p_rev = p_val.get("rev", 0) or 0
            c_rev = c_val.get("rev", 0) or 0
            if p_rn or c_rn:
                month_props[prop] = {
                    "pickup": p_rn, "cancel": c_rn, "net": p_rn - c_rn,
                    "rev": round(p_rev - c_rev, 1),
                    "rev_pickup": round(p_rev, 1), "rev_cancel": round(c_rev, 1),
                }
        if month_props:
            daily_analysis["byPropertyMonth"][mlabel] = month_props

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

    # ── 7) 동기간(YoY) OTB 비교 — yoy_adjusted 기준 (투숙일 기준 온북) ──
    yoy_adj = agg_data.get("yoy_adjusted", {})
    _cy_yr = str(now.year)
    _ly_yr = str(now.year - 1)
    _cy_yoy = yoy_adj.get(_cy_yr, {})
    _ly_yoy = yoy_adj.get(_ly_yr, {})
    yoy_otb = {}

    def _safe_pct(cv, lv):
        """전년 대비 증감률 (%) — 분모 0 방어"""
        return round((cv - lv) / lv * 100, 1) if lv else None

    for mi in compare_months:
        cy_mkey = f"{now.year}{mi:02d}"
        ly_mkey = f"{now.year - 1}{mi:02d}"
        cy_m = _cy_yoy.get("by_month", {}).get(cy_mkey, {})
        ly_m = _ly_yoy.get("by_month", {}).get(ly_mkey, {})
        cy_rn = cy_m.get("booking_rn", 0) or 0
        ly_rn = ly_m.get("booking_rn", 0) or 0
        cy_rev = cy_m.get("booking_rev_m", 0) or 0
        ly_rev = ly_m.get("booking_rev_m", 0) or 0
        cy_adr = round(cy_rev * 1e6 / cy_rn / 1000, 1) if cy_rn else 0
        ly_adr = round(ly_rev * 1e6 / ly_rn / 1000, 1) if ly_rn else 0

        # 사업장별 RN YoY (투숙일 기준 온북)
        _bp_yoy = {}
        _cy_props = _cy_yoy.get("by_property", {})
        _ly_props = _ly_yoy.get("by_property", {})
        for prop in sorted(set(list(_cy_props.keys()) + list(_ly_props.keys()))):
            c_rn = _cy_props.get(prop, {}).get(cy_mkey, {}).get("booking_rn", 0) or 0
            l_rn = _ly_props.get(prop, {}).get(ly_mkey, {}).get("booking_rn", 0) or 0
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
        "compareMonths": [f"{m}월" for m in compare_months],
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
    html = inject_region_monthly(html, notes.get("property_performance", {}))
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
        html = inject_yoy_property_table(html, otb_data)
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

def render_yoy_property_table(yoy_table: list, base_date: str) -> str:
    if not yoy_table:
        return "<p style='color:#888;font-size:12px;'>YoY 데이터 없음</p>"

    base_disp = f"{base_date[:4]}.{base_date[4:6]}.{base_date[6:]}" if len(base_date) == 8 else base_date
    months = [4, 5, 6]
    month_labels = {4: "4월", 5: "5월", 6: "6월"}

    def arrow(yoy):
        if yoy is None:
            return "—"
        if yoy >= 3:
            return f'<span style="color:#4caf89;font-weight:700;">▲ {yoy:+.1f}%</span>'
        elif yoy <= -3:
            return f'<span style="color:#e05555;font-weight:700;">↓ {yoy:+.1f}%</span>'
        else:
            return f'<span style="color:#b0a060;font-weight:700;">→ {yoy:+.1f}%</span>'

    rows_html = ""
    for row in yoy_table:
        region = row.get("region", "")
        color  = REGION_COLORS.get(region, "#888")
        cells  = ""
        for m in months:
            md = row.get("months", {}).get(str(m), {})
            act   = md.get("act_rn", 0)
            last  = md.get("last_rn", 0)
            yoy   = md.get("yoy")
            fcst  = md.get("rns_fcst", act)
            fach  = md.get("fcst_ach", 0.0)
            bud   = md.get("bud_rn", 0)
            arrow_html = arrow(yoy)
            if bud > 0 and fcst is not None and fach is not None:
                fcst_html = (
                    f'<div style="font-size:10px;color:#a0a0c0;margin-top:2px;">'
                    f'FCST: {fcst:,}실 (목표대비 {fach:.1f}%)</div>'
                )
            else:
                fcst_html = ""
            cells += (
                f'<td style="padding:8px 10px;border-bottom:1px solid #333;vertical-align:top;">'
                f'<div style="font-size:12px;">{act:,}실</div>'
                f'<div style="font-size:11px;color:#888;">전년 {last:,}실</div>'
                f'<div style="font-size:12px;margin-top:3px;">{arrow_html}</div>'
                f'{fcst_html}'
                f'</td>'
            )
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


def inject_yoy_property_table(html: str, otb_data: dict) -> str:
    yoy_table = otb_data.get("yoyTable", [])
    base_date = otb_data.get("meta", {}).get("yoyBaseDate", "")

    # base_date가 비어있거나 현재 연도와 다르면 빌드 날짜(KST 오늘)로 폴백
    now_kst = datetime.now(KST)
    current_year = now_kst.strftime("%Y")
    if not base_date or not base_date.startswith(current_year):
        base_date = now_kst.strftime("%Y%m%d")
        logger.info(f"⚠ yoyBaseDate 폴백 → {base_date} (현재 연도 {current_year} 기준)")

    table_html = render_yoy_property_table(yoy_table, base_date)
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
                         "by_segment", "by_region_segment", "by_property_segment", "meta"):
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
