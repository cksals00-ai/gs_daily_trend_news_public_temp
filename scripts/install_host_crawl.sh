#!/bin/bash
# install_host_crawl.sh — com.gs.daily-crawl launchd 작업 설치/재설치
# ============================================================================
# 사용법:
#   ./scripts/install_host_crawl.sh            # 설치(또는 재설치)
#   ./scripts/install_host_crawl.sh --uninstall  # 제거
#   ./scripts/install_host_crawl.sh --run-now    # 설치 후 즉시 1회 실행(테스트)
# ============================================================================
set -e

PROJECT_ROOT="$HOME/Projects/gs_daily_trend_news_public_temp"
LABEL="com.gs.daily-crawl"
SRC_PLIST="$PROJECT_ROOT/scripts/$LABEL.plist"
DEST_PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"

uninstall() {
  echo "▶ 기존 작업 제거..."
  launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
  rm -f "$DEST_PLIST"
  echo "  ✅ 제거 완료 ($LABEL)"
}

if [ "$1" = "--uninstall" ]; then
  uninstall
  exit 0
fi

echo "▶ 래퍼 스크립트 실행권한 부여..."
chmod +x "$PROJECT_ROOT/scripts/host_daily_crawl.sh"

echo "▶ plist 설치: $DEST_PLIST"
mkdir -p "$HOME/Library/LaunchAgents"
cp -f "$SRC_PLIST" "$DEST_PLIST"

echo "▶ 기존 로드 해제 (있으면)..."
launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true

echo "▶ 로드(bootstrap)..."
launchctl bootstrap "$DOMAIN" "$DEST_PLIST"
launchctl enable "$DOMAIN/$LABEL"

echo ""
echo "✅ 설치 완료 — 매일 05:00 (호스트 로컬타임) 자동 실행"
echo "   상태 확인 : launchctl print $DOMAIN/$LABEL | grep -E 'state|program'"
echo "   로그       : tail -f $PROJECT_ROOT/logs/host_daily_crawl.log"
echo "   제거       : $0 --uninstall"

if [ "$1" = "--run-now" ]; then
  echo ""
  echo "▶ 즉시 1회 실행 (kickstart)..."
  launchctl kickstart -k "$DOMAIN/$LABEL"
  echo "  실행 시작됨 — 로그를 확인하세요: tail -f $PROJECT_ROOT/logs/host_daily_crawl.log"
fi
