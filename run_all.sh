#!/bin/bash
cd "$(dirname "$0")"

echo "=== Step 1: parse_raw_db.py ===" > /tmp/run_all.log
python3 scripts/parse_raw_db.py >> /tmp/run_all.log 2>&1
echo "STEP1_EXIT: $?" >> /tmp/run_all.log

echo "=== Step 2: generate_otb_data.py ===" >> /tmp/run_all.log
python3 scripts/generate_otb_data.py >> /tmp/run_all.log 2>&1
echo "STEP2_EXIT: $?" >> /tmp/run_all.log

echo "=== Step 3: generate_fcst.py ===" >> /tmp/run_all.log
python3 scripts/generate_fcst.py >> /tmp/run_all.log 2>&1
echo "STEP3_EXIT: $?" >> /tmp/run_all.log

echo "=== Step 4: generate_campaign_data.py ===" >> /tmp/run_all.log
python3 scripts/generate_campaign_data.py >> /tmp/run_all.log 2>&1
echo "STEP4_EXIT: $?" >> /tmp/run_all.log

echo "=== Step 5: build_validation.py ===" >> /tmp/run_all.log
python3 scripts/build_validation.py >> /tmp/run_all.log 2>&1
echo "STEP5_EXIT: $?" >> /tmp/run_all.log

echo "=== Step 6: build.py ===" >> /tmp/run_all.log
python3 scripts/build.py >> /tmp/run_all.log 2>&1
echo "STEP6_EXIT: $?" >> /tmp/run_all.log

echo "ALL_DONE" > /tmp/run_all_done.txt
