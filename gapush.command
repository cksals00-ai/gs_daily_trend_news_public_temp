#!/bin/zsh
# GA 대시보드 원클릭 푸시 — 더블클릭하면 add+commit+pull(rebase)+push 자동 실행
cd "$(dirname "$0")"
echo "📦 $(pwd)"
rm -f .git/index.lock .git/HEAD.lock 2>/dev/null
git add -A
git commit -m "update: GA 대시보드 $(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M') KST" || echo "ℹ 변경 없음"
echo "⬇️  원격 병합(rebase)..."
git pull --rebase -X theirs origin main || {
  echo "⚠ rebase 충돌 — 자동해소 시도"; git rebase --abort 2>/dev/null
  git pull --no-rebase -X theirs origin main
}
echo "⬆️  푸시..."
git push && echo "✅ 완료!" || echo "❌ 푸시 실패 — 로그 확인"
read -n1 -p "닫으려면 아무 키나 누르세요..."
