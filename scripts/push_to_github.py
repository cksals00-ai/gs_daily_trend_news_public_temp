#!/usr/bin/env python3
"""
GS Daily Trend Report V7 - GitHub 자동 푸시 스크립트
============================================================
전체 파이프라인을 실행하고 GitHub에 자동 커밋·푸시합니다.
크롤러처럼 동작하여 매일 자동 배포됩니다.

실행 순서:
  1. collect_news.py      → data/news_latest.json
  2. generate_insights.py → data/enriched_notes.json
  3. build.py             → docs/index.html
  4. git add → commit → push

사용법:
  python scripts/push_to_github.py                  # 전체 실행
  python scripts/push_to_github.py --skip-news      # 뉴스 수집 건너뛰기
  python scripts/push_to_github.py --dry-run        # 푸시 없이 빌드만
  
환경변수 (GitHub Actions 전용):
  GITHUB_TOKEN: 푸시 권한 토큰

© 2026 GS팀 · Haein Kim Manager
"""
import argparse
import logging
import os
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
SCRIPTS_DIR = ROOT / "scripts"
KST = timezone(timedelta(hours=9))


def run_cmd(cmd: list, cwd=None, check=True, capture=False) -> subprocess.CompletedProcess:
    """명령 실행"""
    logger.info(f"$ {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd or ROOT,
            check=check,
            capture_output=capture,
            text=True,
        )
        if capture and result.stdout:
            logger.info(result.stdout.strip())
        return result
    except subprocess.CalledProcessError as e:
        logger.error(f"명령 실패 (code {e.returncode}): {' '.join(cmd)}")
        if e.stdout:
            logger.error(f"stdout: {e.stdout}")
        if e.stderr:
            logger.error(f"stderr: {e.stderr}")
        raise


def run_step(name: str, script: str, skip: bool = False) -> bool:
    """각 단계 실행"""
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"[STEP] {name}")
    logger.info("=" * 60)
    
    if skip:
        logger.info(f"⏭  스킵됨 ({script})")
        return True
    
    try:
        run_cmd([sys.executable, str(SCRIPTS_DIR / script)])
        return True
    except subprocess.CalledProcessError:
        logger.error(f"❌ {name} 실패")
        return False


def git_commit_push(dry_run: bool = False) -> bool:
    """Git 커밋 & 푸시"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("[STEP] Git 커밋 & 푸시")
    logger.info("=" * 60)
    
    # 1) 변경 상태 확인
    result = run_cmd(["git", "status", "--porcelain"], capture=True)
    if not result.stdout.strip():
        logger.info("ℹ  변경사항 없음 - 커밋 스킵")
        return True
    
    logger.info(f"변경 감지:\n{result.stdout}")
    
    # 2) git user 설정 (GitHub Actions용)
    try:
        run_cmd(["git", "config", "user.email", "action@github.com"], check=False)
        run_cmd(["git", "config", "user.name", "GS Auto-Bot"], check=False)
    except Exception:
        pass
    
    # 3) 파일 추가 - 모든 변경 사항 포함 (.github, scripts, data, docs 등)
    run_cmd(["git", "add", "data/", "docs/", "scripts/", ".github/"])
    
    # 4) 커밋 메시지 생성
    now = datetime.now(KST)
    commit_msg = f"chore(auto): daily update {now.strftime('%Y-%m-%d %H:%M')} KST"
    
    # 5) 커밋
    try:
        run_cmd(["git", "commit", "-m", commit_msg])
    except subprocess.CalledProcessError:
        logger.info("ℹ  커밋할 변경 없음 (이미 커밋됨)")
        return True
    
    # 6) 푸시
    if dry_run:
        logger.info("🔍 DRY-RUN 모드 - 푸시 스킵")
        run_cmd(["git", "log", "-1", "--oneline"], capture=True)
        return True
    
    try:
        run_cmd(["git", "push"])
        logger.info("✓ GitHub에 푸시 완료")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"푸시 실패: {e}")
        logger.info("💡 로컬 환경이라면 `git pull --rebase` 후 다시 실행하거나 수동 push 하세요")
        return False


def main():
    parser = argparse.ArgumentParser(description="V7 자동 파이프라인 + GitHub 푸시")
    parser.add_argument("--skip-news", action="store_true", help="뉴스 수집 건너뛰기")
    parser.add_argument("--skip-insights", action="store_true", help="인사이트 생성 건너뛰기")
    parser.add_argument("--skip-powerbi", action="store_true", help="Power BI 수집 건너뛰기")
    parser.add_argument("--dry-run", action="store_true", help="푸시 없이 빌드만")
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("🚀 V7 자동 파이프라인 시작")
    logger.info(f"   시각: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}")
    logger.info(f"   작업 디렉토리: {ROOT}")
    logger.info(f"   Dry Run: {args.dry_run}")
    logger.info("=" * 60)
    
    steps_passed = True
    
    # STEP 1: Power BI 수집
    if not run_step(
        "Power BI 데이터 수집",
        "collect_powerbi.py",
        skip=args.skip_powerbi,
    ):
        logger.warning("⚠  Power BI 수집 실패 - 기존 데이터로 진행")
    
    # STEP 2: 뉴스 수집
    if not run_step(
        "뉴스 자동 수집 (Google News RSS)",
        "collect_news.py",
        skip=args.skip_news,
    ):
        logger.warning("⚠  뉴스 수집 실패 - 기존 데이터로 진행")
    
    # STEP 2.5: GS Monitor 경쟁사 수집
    if not run_step(
        "경쟁사 동향 수집 (GS Monitor)",
        "collect_gs_monitor.py",
    ):
        logger.warning("⚠  GS Monitor 수집 실패 - 기존 데이터로 진행")
    
    # STEP 3: 인사이트 자동 생성
    if not run_step(
        "오늘의 한 줄 + 액션 자동 생성",
        "generate_insights.py",
        skip=args.skip_insights,
    ):
        steps_passed = False
    
    # STEP 4: HTML 빌드
    if not run_step("HTML 빌드", "build.py"):
        logger.error("HTML 빌드 실패 - 파이프라인 중단")
        sys.exit(1)
    
    # STEP 5: Git 푸시
    if not git_commit_push(dry_run=args.dry_run):
        logger.error("Git 푸시 실패")
        sys.exit(1)
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ 전체 파이프라인 완료")
    logger.info("=" * 60)
    if not args.dry_run:
        logger.info("🌐 GitHub Pages가 1~2분 내에 자동 갱신됩니다")
        logger.info("    https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/")


if __name__ == "__main__":
    main()
