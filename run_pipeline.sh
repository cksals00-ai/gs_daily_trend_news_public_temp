#!/bin/bash
cd ~/Desktop/gs_daily_trend_news_public_temp

echo "=== Step 1: parse_raw_db.py ==="
python3 scripts/parse_raw_db.py
RC1=$?
echo "RETURN CODE: $RC1"

echo "=== Step 2: generate_otb_data.py ==="
python3 scripts/generate_otb_data.py
RC2=$?
echo "RETURN CODE: $RC2"

echo "=== Step 3: build.py ==="
python3 scripts/build.py
RC3=$?
echo "RETURN CODE: $RC3"

echo ""
echo "=== 결과 ==="
echo "  parse_raw_db:       exit=$RC1"
echo "  generate_otb_data:  exit=$RC2"
echo "  build:              exit=$RC3"
