#!/bin/bash
cd ~/Desktop/gs_daily_trend_news_public_temp
rm -f .git/index.lock .git/HEAD.lock
git add -A
git commit -m "fix: OTB FCST 버그 + 합계목표 GS 61205 + 예약추이 페이스 + 상품상세 월탭 + weekly GS필터"
git pull --no-rebase || {
  git checkout --theirs docs/admin_suggestions.json docs/index.html 2>/dev/null
  git add -A
  git commit --no-edit
}
git push
echo ""
echo "✅ 푸시 완료! 이 창을 닫아도 됩니다."
read -p "아무 키나 누르세요..."
