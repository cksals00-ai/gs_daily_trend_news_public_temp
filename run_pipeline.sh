#\!/bin/bash
cd /sessions/vibrant-dazzling-goodall/mnt/gs_daily_trend_news_public_temp

echo "=== Starting Step 1: parse_raw_db.py ==="
python3 scripts/parse_raw_db.py 2>&1 | tail -20
echo "---"
python3 scripts/parse_raw_db.py > /tmp/s1.log 2>&1
RC1=$?
echo "RETURN CODE: $RC1"
tail -5 /tmp/s1.log
