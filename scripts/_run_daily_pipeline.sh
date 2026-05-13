#!/bin/bash
# 자동 실행 래퍼 스크립트 (cowork 스케줄러용)
# 단계별 실행 + 로그를 _pipeline_run.log 에 기록

set -o pipefail

PROJECT_ROOT="$HOME/Desktop/gs_daily_trend_news_public_temp"
LOG_FILE="$PROJECT_ROOT/_pipeline_run.log"
STATUS_FILE="$PROJECT_ROOT/_pipeline_status.json"

cd "$PROJECT_ROOT" || { echo "[FATAL] cannot cd to $PROJECT_ROOT" > "$LOG_FILE"; exit 99; }

# 로그 초기화
{
  echo "=== Pipeline run started at $(date '+%Y-%m-%d %H:%M:%S %Z') ==="
  echo "PWD: $(pwd)"
  echo "Python: $(which python3) $(python3 --version 2>&1)"
} > "$LOG_FILE" 2>&1

write_status() {
  cat > "$STATUS_FILE" <<EOF
{"stage": "$1", "status": "$2", "exit_code": $3, "ts": "$(date '+%Y-%m-%d %H:%M:%S')"}
EOF
}

write_status "init" "running" 0

# .git lock 정리
rm -f "$PROJECT_ROOT/.git/index.lock" "$PROJECT_ROOT/.git/HEAD.lock" 2>>"$LOG_FILE"

# 1단계: 변경 감지
echo "" >> "$LOG_FILE"
echo "=== STAGE 1: check_raw_db_changes.py (--wait --interval 600 --max-wait 7200) ===" >> "$LOG_FILE"
write_status "check_raw_db_changes" "running" 0
python3 scripts/check_raw_db_changes.py --wait --interval 600 --max-wait 7200 >> "$LOG_FILE" 2>&1
RC=$?
echo "exit=$RC" >> "$LOG_FILE"

if [ $RC -eq 1 ]; then
  echo "[INFO] No changes detected within max-wait. Skipping build." >> "$LOG_FILE"
  write_status "check_raw_db_changes" "no_changes" 1
  echo "=== Pipeline finished (no changes) at $(date '+%Y-%m-%d %H:%M:%S %Z') ===" >> "$LOG_FILE"
  exit 0
fi
if [ $RC -ne 0 ]; then
  echo "[ERROR] check_raw_db_changes failed (exit=$RC)" >> "$LOG_FILE"
  write_status "check_raw_db_changes" "error" $RC
  exit $RC
fi

run_stage() {
  STAGE_NAME="$1"
  CMD="$2"
  echo "" >> "$LOG_FILE"
  echo "=== STAGE: $STAGE_NAME ($(date '+%H:%M:%S')) ===" >> "$LOG_FILE"
  write_status "$STAGE_NAME" "running" 0
  eval "$CMD" >> "$LOG_FILE" 2>&1
  RC=$?
  echo "exit=$RC" >> "$LOG_FILE"
  if [ $RC -ne 0 ]; then
    echo "[ERROR] $STAGE_NAME failed (exit=$RC)" >> "$LOG_FILE"
    write_status "$STAGE_NAME" "error" $RC
    exit $RC
  fi
  write_status "$STAGE_NAME" "done" 0
}

run_stage "parse_raw_db"       "python3 scripts/parse_raw_db.py"
run_stage "compare_and_update" "python3 scripts/compare_and_update.py"
run_stage "generate_otb_data"  "python3 scripts/generate_otb_data.py"
run_stage "generate_insights"  "python3 scripts/generate_insights.py"
run_stage "build"              "python3 scripts/build.py"

# Git commit + push (lock 재정리)
rm -f "$PROJECT_ROOT/.git/index.lock" "$PROJECT_ROOT/.git/HEAD.lock" 2>>"$LOG_FILE"

echo "" >> "$LOG_FILE"
echo "=== STAGE: git ($(date '+%H:%M:%S')) ===" >> "$LOG_FILE"
write_status "git" "running" 0
git add -A >> "$LOG_FILE" 2>&1
COMMIT_MSG="chore(auto): daily update $(date '+%Y-%m-%d %H:%M') KST [skip ci]"
git commit -m "$COMMIT_MSG" >> "$LOG_FILE" 2>&1
COMMIT_RC=$?
if [ $COMMIT_RC -ne 0 ]; then
  # nothing to commit 인 경우 (rc=1) 는 통과
  if ! git diff --cached --quiet 2>/dev/null; then
    echo "[ERROR] git commit failed" >> "$LOG_FILE"
    write_status "git_commit" "error" $COMMIT_RC
    exit $COMMIT_RC
  fi
  echo "[INFO] nothing to commit" >> "$LOG_FILE"
fi

# push (실패 시 rebase 재시도 한 번)
git push origin main >> "$LOG_FILE" 2>&1
PUSH_RC=$?
if [ $PUSH_RC -ne 0 ]; then
  echo "[WARN] push failed, trying pull --rebase then push again" >> "$LOG_FILE"
  git pull --rebase origin main >> "$LOG_FILE" 2>&1
  git push origin main >> "$LOG_FILE" 2>&1
  PUSH_RC=$?
fi

if [ $PUSH_RC -ne 0 ]; then
  echo "[ERROR] git push failed (exit=$PUSH_RC)" >> "$LOG_FILE"
  write_status "git_push" "error" $PUSH_RC
  exit $PUSH_RC
fi

write_status "done" "success" 0
echo "" >> "$LOG_FILE"
echo "=== Pipeline finished SUCCESS at $(date '+%Y-%m-%d %H:%M:%S %Z') ===" >> "$LOG_FILE"
exit 0
