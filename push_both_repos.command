#!/bin/zsh
cd ~/Desktop/gs_daily_trend_news_public_temp
rm -f .git/index.lock .git/HEAD.lock
git add -A
git commit -m "fix: 마감 거래처 G-OTA매핑 + 취소율분석 + 국가별인바운드 + 캘린더사업장필터"
git pull --no-rebase || {
  git checkout --theirs docs/admin_suggestions.json docs/index.html 2>/dev/null
  git add -A
  git commit --no-edit
}
git push
echo ""
echo "✅ 푸시 완료!"
read -p "아무 키나 누르세요..."
