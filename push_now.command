#!/bin/bash
cd ~/Desktop/gs_daily_trend_news_public_temp
echo "📄 Daily Booking PDF 파싱 시작..."
python3 scripts/parse_daily_booking.py 2>&1
echo ""
echo "✅ PDF 파싱 완료!"
echo ""
echo "📦 Git 푸시 진행..."
rm -f .git/index.lock .git/HEAD.lock
git add -A
git commit -m "data: Daily Booking 2026.05.06 파싱 + 마감 거래처매핑/취소율/국가별/캘린더필터"
git pull --no-rebase || {
  git checkout --theirs docs/admin_suggestions.json docs/index.html 2>/dev/null
  git add -A
  git commit --no-edit
}
git push
echo ""
echo "✅ 파싱 + 푸시 완료! 이 창을 닫아도 됩니다."
read -p "아무 키나 누르세요..."
