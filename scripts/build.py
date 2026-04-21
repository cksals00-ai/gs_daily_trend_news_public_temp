#!/usr/bin/env python3
"""
GS팀 데일리 트렌드 리포트 V7 - HTML 빌드 스크립트
============================================================
매일 daily_notes.json의 수기 입력 내용을 HTML에 자동 반영.

동작:
  1. data/daily_notes.json 읽기 (담당자 수기 입력)
  2. data/powerbi_latest.json 읽기 (Power BI 자동 수집)
  3. docs/index.html 템플릿에 주입
  4. docs/index.html 덮어쓰기

실행:
  python scripts/build.py

© 2026 GS팀 · Haein Kim Manager
"""
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

# ─────────────────────────────────────────────
# 경로 설정
# ─────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
DOCS_DIR = ROOT / "docs"

NOTES_FILE = DATA_DIR / "daily_notes.json"
POWERBI_FILE = DATA_DIR / "powerbi_latest.json"
HTML_FILE = DOCS_DIR / "index.html"
KST = timezone(timedelta(hours=9))


def load_json(path: Path, default=None):
    """JSON 파일 안전하게 로드"""
    if not path.exists():
        logger.warning(f"파일 없음: {path.name} → 기본값 사용")
        return default or {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"JSON 파싱 실패 ({path.name}): {e}")
        return default or {}


def apply_tpl(html: str, selector: str, new_text: str) -> str:
    """
    data-tpl-XXX 속성을 가진 요소의 innerHTML을 교체.
    예: data-tpl-headline → <div data-tpl-headline>...</div>
    """
    if not new_text:
        return html
    # <tag data-tpl-XXX ...>OLD</tag> → <tag data-tpl-XXX ...>NEW</tag>
    # re.escape()로 new_text의 특수문자 이스케이프 (그룹 참조 오류 방지)
    pattern = re.compile(
        rf'(<[^>]*\bdata-tpl-{re.escape(selector)}\b[^>]*>)([^<]*?)(</)',
        re.DOTALL
    )
    # lambda로 치환해서 백슬래시 문제 회피
    new_html, n = pattern.subn(
        lambda m: m.group(1) + new_text + m.group(3),
        html,
        count=1
    )
    if n == 0:
        logger.debug(f"템플릿 슬롯 '{selector}' 미발견 (HTML에 추가 필요)")
    return new_html


def main():
    logger.info("=" * 60)
    logger.info("V7 대시보드 빌드 시작")
    logger.info("=" * 60)

    # 1. 수기 입력 데이터 로드
    notes = load_json(NOTES_FILE)
    if not notes:
        logger.error("daily_notes.json 없음 또는 비어있음. 빌드 중단.")
        sys.exit(1)
    
    logger.info(f"✓ daily_notes.json 로드 (작성자: {notes.get('_updated_by', 'unknown')})")
    logger.info(f"  리포트 날짜: {notes.get('report_date', 'unknown')}")

    # 2. Power BI 자동 수집 데이터 로드 (있으면)
    powerbi = load_json(POWERBI_FILE)
    if powerbi:
        logger.info(f"✓ powerbi_latest.json 로드 (수집: {powerbi.get('collected_at', 'unknown')})")
    
    # 3. HTML 템플릿 로드
    if not HTML_FILE.exists():
        logger.error(f"HTML 템플릿 없음: {HTML_FILE}")
        sys.exit(1)
    
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()
    
    logger.info(f"✓ HTML 템플릿 로드 ({len(html):,} bytes)")

    # ─────────────────────────────────────────────
    # 4. 수기 입력 내용 주입
    # ─────────────────────────────────────────────
    now_kst = datetime.now(KST)
    
    # 날짜/시간
    report_date = notes.get("report_date", now_kst.strftime("%Y-%m-%d"))
    day_map = {0: "MON", 1: "TUE", 2: "WED", 3: "THU", 4: "FRI", 5: "SAT", 6: "SUN"}
    try:
        dt = datetime.strptime(report_date, "%Y-%m-%d")
        day_abbr = day_map[dt.weekday()]
        display_date = dt.strftime("%Y.%m.%d")
        timestamp = f"{display_date} {day_abbr} 08:00 KST"
    except ValueError:
        display_date = report_date
        timestamp = now_kst.strftime("%Y.%m.%d %H:%M KST")
    
    html = apply_tpl(html, "date", display_date)
    html = apply_tpl(html, "timestamp", timestamp)
    
    # 🔥 오늘의 한 줄
    headline = notes.get("today_headline", {})
    if headline.get("text"):
        html = apply_tpl(html, "headline", headline["text"])
    
    # 작성자
    updated_by = notes.get("_updated_by", "GS팀 · Haein Kim Manager")
    html = apply_tpl(html, "updated_by", f"by {updated_by}")
    
    # 임원 KPI 3개
    kpi = notes.get("executive_kpi", {})
    for idx in (1, 2, 3):
        k = kpi.get(f"kpi_{idx}", {})
        html = apply_tpl(html, f"kpi{idx}-label", k.get("label", ""))
        html = apply_tpl(html, f"kpi{idx}-value", str(k.get("value", "")))
        html = apply_tpl(html, f"kpi{idx}-unit", k.get("unit", ""))
        html = apply_tpl(html, f"kpi{idx}-delta", k.get("delta", ""))
    
    # 권역 상태 (4개)
    regions = notes.get("region_status", {})
    for key in ("vivaldi", "central", "south", "apac"):
        r = regions.get(key, {})
        html = apply_tpl(html, f"region-{key}-value", str(r.get("달성률", r.get("value", ""))))
        html = apply_tpl(html, f"region-{key}-delta", r.get("메모", r.get("delta", "")))
    
    # 액션 알림 (4개)
    actions = notes.get("action_alerts", {})
    for key in ("vivaldi", "central", "south", "apac"):
        text = actions.get(key, "")
        if text:
            html = apply_tpl(html, f"action-{key}", text)
    
    # 빌드 메타
    build_meta = now_kst.strftime("Auto-Built %Y-%m-%d %H:%M KST")
    html = apply_tpl(html, "build", build_meta)
    
    # ─────────────────────────────────────────────
    # 5. Power BI 데이터 주입 (있으면)
    # ─────────────────────────────────────────────
    if powerbi and powerbi.get("top5_by_revenue"):
        logger.info(f"✓ Power BI TOP 5 채널 감지: {len(powerbi['top5_by_revenue'])}개")
        # TODO: 향후 채널 TOP 5를 HTML에 반영
    
    # ─────────────────────────────────────────────
    # 6. HTML 저장
    # ─────────────────────────────────────────────
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    
    logger.info("=" * 60)
    logger.info(f"✓ 빌드 완료: {HTML_FILE}")
    logger.info(f"  크기: {len(html):,} bytes")
    logger.info(f"  빌드 시각: {build_meta}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
