#!/bin/bash
# 🔍 진단 + 강제 푸시 스크립트
# 워크플로우 파일 누락 문제 해결용

echo "=========================================="
echo "🔍 GitHub 동기화 상태 진단"
echo "=========================================="
echo ""

cd ~/Desktop/gs_daily_trend_news_public_temp || {
    echo "❌ 폴더 없음: ~/Desktop/gs_daily_trend_news_public_temp"
    exit 1
}

# 1. 워크플로우 파일 존재 확인
echo "[1/4] 워크플로우 파일 존재 여부"
if [ -f .github/workflows/deploy.yml ]; then
    echo "  ✓ .github/workflows/deploy.yml 존재 ($(wc -l < .github/workflows/deploy.yml)줄)"
else
    echo "  ❌ deploy.yml 누락 - ZIP에서 다시 압축 해제 필요"
    echo ""
    echo "    실행: unzip -o ~/Downloads/v7_dashboard_deploy.zip -d ."
    exit 1
fi

# 2. Git 상태
echo ""
echo "[2/4] Git 추적 상태"
git ls-files .github/workflows/ 2>/dev/null | head -5
if git ls-files .github/workflows/deploy.yml | grep -q deploy.yml; then
    echo "  ✓ deploy.yml이 Git에 추적됨"
else
    echo "  ⚠ deploy.yml이 Git에 추적 안 됨 - 강제 add 필요"
fi

# 3. 원격 저장소와 비교
echo ""
echo "[3/4] 원격 저장소 동기화 상태"
git fetch origin main 2>&1 | tail -3
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)
if [ "$LOCAL" = "$REMOTE" ]; then
    echo "  ✓ 로컬과 원격 동기화됨"
else
    echo "  ⚠ 로컬과 원격 다름"
    echo "    로컬:  $LOCAL"
    echo "    원격:  $REMOTE"
fi

# 4. 강제 add + push
echo ""
echo "[4/4] 강제 add + commit + push"
echo "  실행 중..."

git add -A .github/ scripts/ data/ docs/

# 변경 있는지 확인
if git diff --staged --quiet; then
    echo "  ℹ 스테이지된 변경 없음"
    
    # 그래도 강제로 빈 커밋 푸시 (워크플로우 트리거)
    echo "  → 빈 커밋으로 워크플로우 강제 트리거"
    git commit --allow-empty -m "trigger: force workflow run $(date +%H:%M)"
else
    echo "  → 변경 발견. 커밋 진행"
    git commit -m "fix: ensure all workflow + script files are pushed $(date +%H:%M)"
fi

echo ""
echo "  → 푸시 진행"
git push origin main

echo ""
echo "=========================================="
echo "✅ 완료. 다음 단계:"
echo "=========================================="
echo ""
echo "1. https://github.com/cksals00-ai/gs_daily_trend_news_public_temp/actions"
echo "   접속해서 좌측 사이드바에 '🚀 Auto Build & Deploy' 워크플로우가 보이는지 확인"
echo ""
echo "2. 안 보이면 GitHub Pages 설정 변경 필요:"
echo "   https://github.com/cksals00-ai/gs_daily_trend_news_public_temp/settings/pages"
echo "   → Source를 'GitHub Actions'로 변경"
echo ""
echo "3. 보이면 '🚀 Auto Build & Deploy' 클릭 → 'Run workflow' 버튼으로 즉시 실행"
echo ""
