#!/bin/bash
set -e
cd /sessions/happy-trusting-cray/mnt/gs_daily_trend_news_public_temp

echo "===== STEP 1: parse_raw_db.py ====="
python scripts/parse_raw_db.py 2>&1 | tail -20
echo "STEP1_DONE"

echo "===== STEP 2: compare_and_update.py ====="
python scripts/compare_and_update.py 2>&1 | tail -20
echo "STEP2_DONE"

echo "===== STEP 3: generate_otb_data.py ====="
python scripts/generate_otb_data.py 2>&1 | tail -20
echo "STEP3_DONE"

echo "===== STEP 4: generate_insights.py ====="
python scripts/generate_insights.py 2>&1 | tail -30
echo "STEP4_DONE"

echo "===== STEP 5: build.py ====="
python scripts/build.py 2>&1 | tail -20
echo "STEP5_DONE"

echo "===== ALL PIPELINE DONE ====="
