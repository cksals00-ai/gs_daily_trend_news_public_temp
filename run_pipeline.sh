#\!/bin/bash
cd /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp
echo "=== Step 1: parse_raw_db.py ===" > /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt
python3 scripts/parse_raw_db.py >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt 2>&1
echo "RC1=$?" >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt

echo "=== Step 2: compare_and_update.py ===" >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt
python3 scripts/compare_and_update.py >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt 2>&1
echo "RC2=$?" >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt

echo "=== Step 3: generate_otb_data.py ===" >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt
python3 scripts/generate_otb_data.py >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt 2>&1
echo "RC3=$?" >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt

echo "=== Step 4: generate_insights.py ===" >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt
python3 scripts/generate_insights.py >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt 2>&1
echo "RC4=$?" >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt

echo "=== Step 5: build.py ===" >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt
python3 scripts/build.py >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt 2>&1
echo "RC5=$?" >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt

echo "=== DONE ===" >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt
date >> /sessions/zealous-inspiring-dijkstra/mnt/gs_daily_trend_news_public_temp/pipeline_log.txt
