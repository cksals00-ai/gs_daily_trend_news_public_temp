#!/bin/bash
cd ~/Desktop/gs_daily_trend_news_public_temp
rm -f .git/index.lock .git/HEAD.lock 2>/dev/null || true
find .git/objects -name 'tmp_obj_*' -delete 2>/dev/null || true
git add -A
DATE_KST=$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M')
git commit -m "data: 온북 데이터 갱신 ${DATE_KST} KST

- otb_data.json refreshTime 갱신
- inbound_enriched.json 업데이트
- validation 20260526 추가
- index.html / otb.html 재빌드"
git pull --no-rebase origin main || {
  git checkout --theirs docs/admin_suggestions.json docs/index.html docs/otb.html docs/data/otb_data.json 2>/dev/null
  git add -A
  git commit --no-edit
}
git push origin main
echo ""
echo "✅ 커밋 & 푸시 완료!"
echo "아무 키나 누르면 닫힙니다..."
read -n1
