#!/bin/zsh
# Auto-generated push script — pushes both Sono GS repos to GitHub.
# Created by Claude. Safe to delete after use.

set -e
echo "================================================================"
echo "  Pushing Sono GS repos to GitHub"
echo "================================================================"

REPO1="$HOME/Desktop/gs_daily_trend_news_public_temp"
REPO2="$HOME/Projects/sono-competitor-crawler"

for REPO in "$REPO1" "$REPO2"; do
  echo ""
  echo "--- $REPO ---"
  cd "$REPO"
  echo "Branch: $(git branch --show-current)"
  echo "Pending: $(git log --oneline origin/main..HEAD | wc -l | tr -d ' ') commit(s)"
  git push origin main
  echo "Push OK."
done

echo ""
echo "================================================================"
echo "  Both pushes complete. You can close this window."
echo "================================================================"
echo ""
echo "Press any key to close..."
read -k1
