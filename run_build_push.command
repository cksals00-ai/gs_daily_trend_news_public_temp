#!/bin/bash
cd ~/Desktop/gs_daily_trend_news_public_temp
echo "=== parse_raw_db.py ==="
python3 scripts/parse_raw_db.py
echo "=== generate_otb_data.py ==="
python3 scripts/generate_otb_data.py
echo "=== build.py ==="
python3 scripts/build.py
echo "=== git add + commit + push ==="
git add -A
git commit -m "fix: 동기간보정+기타제거+YoY TOP/BOTTOM+해외숨김"
git push origin main
echo ""
echo "✅ 완료! 이 창을 닫아도 됩니다."
read -p "Press Enter to close..."
