#!/bin/bash
# host_daily_crawl.sh — 호스트(macOS) 전용 크롤링 → 대시보드 빌드 → git push 자동화
# ============================================================================
# 배경: Claude 스케줄 에이전트(샌드박스)는 프록시 403 으로 외부망/푸시가 모두 차단됨.
#       호스트 Mac 터미널은 네트워크가 정상이므로 launchd 로 여기서 직접 실행한다.
#
# 동작: deploy.yml(GitHub Actions full-build) 의 네트워크 단계를 호스트에서 미러링.
#   1) collect_news.py        — Google News RSS (네트워크)
#   2) collect_gs_monitor.py  — 경쟁사 모니터 (네트워크, 실패 시 기존 유지)
#   3) collect_powerbi.py     — Power BI 실적 (네트워크, 실패 시 기존 유지)
#   4) generate_insights.py   — KPI → 인사이트 (로컬)
#   5) gen_sdd_by_prop.py     — 사업장별 SDD (로컬, optional)
#   6) generate_campaign_performance.py — raw_db 27/28 txt 있을 때만
#   6.7) generate_campaign_data.py — 구글시트 기획전 동기화 (published CSV 직접 fetch, Chrome 불필요)
#   7) build.py               — HTML 전체 빌드
#   8) git add data/ docs/ → commit → push (rebase 재시도 포함)
#
# 원칙:
#   - 더미 데이터 절대 금지: 크롤러 실패 시 기존 JSON 그대로 유지 (덮어쓰지 않음)
#   - 야놀자 자동 크롤링 안 함 (이 스크립트는 야놀자를 건드리지 않음)
#   - 크롤러는 best-effort(실패해도 계속), 빌드/푸시는 fatal(실패 시 중단)
#
# 수동 실행:  ./scripts/host_daily_crawl.sh
# 자동 실행:  launchd (com.gs.daily-crawl.plist), 매일 05:00 KST
# ============================================================================

set -o pipefail

PROJECT_ROOT="$HOME/Projects/gs_daily_trend_news_public_temp"
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/host_daily_crawl.log"
STATUS_FILE="$PROJECT_ROOT/_host_crawl_status.json"
PY="$(command -v python3)"

cd "$PROJECT_ROOT" || { echo "[FATAL] cannot cd to $PROJECT_ROOT"; exit 99; }
mkdir -p "$LOG_DIR"

# ── 로그 로테이션 (5MB 초과 시 .1 로 보관) ──────────────────────────────
if [ -f "$LOG_FILE" ] && [ "$(stat -f%z "$LOG_FILE" 2>/dev/null || echo 0)" -gt 5242880 ]; then
  mv -f "$LOG_FILE" "$LOG_FILE.1"
fi

log() { echo "$(date '+%Y-%m-%d %H:%M:%S %Z')  $*" | tee -a "$LOG_FILE"; }

write_status() {
  # $1=stage $2=status $3=exit_code
  cat > "$STATUS_FILE" <<EOF
{"stage": "$1", "status": "$2", "exit_code": $3, "ts": "$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S')"}
EOF
}

log "============================================================"
log "=== host_daily_crawl 시작 ==="
log "PROJECT_ROOT: $PROJECT_ROOT"
log "Python      : $PY ($($PY --version 2>&1))"
write_status "init" "running" 0

# ── git lock 정리 ────────────────────────────────────────────────────────
rm -f "$PROJECT_ROOT/.git/index.lock" "$PROJECT_ROOT/.git/HEAD.lock" 2>>"$LOG_FILE"

# ── best-effort 크롤러: 실패해도 파이프라인 계속 (기존 데이터 유지) ──────
run_crawl() {
  local label="$1" script="$2"
  log "--- [crawl] $label ---"
  write_status "$label" "running" 0
  if [ ! -f "$script" ]; then
    log "    ⚠ $script 없음 — 스킵"
    return 0
  fi
  if "$PY" "$script" >>"$LOG_FILE" 2>&1; then
    log "    ✅ $label 완료"
    write_status "$label" "done" 0
  else
    local rc=$?
    log "    ⚠ $label 실패 (exit=$rc) — 기존 데이터 유지하고 계속"
    write_status "$label" "soft_fail" "$rc"
  fi
  return 0
}

# ── fatal 단계: 실패 시 중단 ──────────────────────────────────────────────
run_fatal() {
  local label="$1" script="$2"
  log "--- [build] $label ---"
  write_status "$label" "running" 0
  if "$PY" "$script" >>"$LOG_FILE" 2>&1; then
    log "    ✅ $label 완료"
    write_status "$label" "done" 0
  else
    local rc=$?
    log "    ❌ $label 실패 (exit=$rc) — 파이프라인 중단"
    write_status "$label" "error" "$rc"
    exit "$rc"
  fi
}

# ── [1~3] 크롤링 (네트워크) ───────────────────────────────────────────────
run_crawl "collect_news"       "scripts/collect_news.py"
run_crawl "collect_gs_monitor" "scripts/collect_gs_monitor.py"
run_crawl "collect_powerbi"    "scripts/collect_powerbi.py"

# ── [4] 인사이트 (필수) ───────────────────────────────────────────────────
run_fatal "generate_insights"  "scripts/generate_insights.py"

# ── [5] 사업장별 SDD (optional — 실패해도 계속) ──────────────────────────
run_crawl "gen_sdd_by_prop"    "scripts/gen_sdd_by_prop.py"

# ── [6] 기획전 실적: raw_db 27/28 txt 있을 때만 (없으면 기존 유지) ────────
log "--- [build] generate_campaign_performance (조건부) ---"
TXT_COUNT=$(find data/raw_db -maxdepth 2 -type f \( -name '27.*.txt' -o -name '28.*.txt' \) 2>/dev/null | wc -l | tr -d ' ')
if [ "$TXT_COUNT" -gt 0 ]; then
  log "    raw_db 27/28 txt ${TXT_COUNT}개 발견 → 집계 실행"
  run_crawl "campaign_performance" "scripts/generate_campaign_performance.py"
else
  log "    ⚠ raw_db 27/28 txt 없음 — 기획전 실적 스킵 (기존 데이터 유지)"
fi

# ── [6.5] 권역별 가격 분석 재생성 (경쟁사 금액 크롤 CSV → competitor_analysis.json) ──
#   sono-competitor-crawler 레포의 최신 sono_competitor_prices CSV를 읽어
#   GS 매출 리포트·freshness 가 소비하는 권역별 가격 분석 JSON을 생성한다.
#   best-effort: 실패해도 기존 competitor_analysis.json 유지하고 빌드 계속.
CRAWLER_ROOT="$HOME/Projects/sono-competitor-crawler"
CRAWLER_PY="$CRAWLER_ROOT/venv/bin/python"
log "--- [build] competitor_analysis (권역별 가격 분석) ---"
write_status "competitor_analysis" "running" 0
if [ -x "$CRAWLER_PY" ] && [ -f "$CRAWLER_ROOT/build_competitor_analysis.py" ]; then
  if ( cd "$CRAWLER_ROOT" && "$CRAWLER_PY" build_competitor_analysis.py \
        --out "$PROJECT_ROOT/docs/data/competitor_analysis.json" \
        --out "$PROJECT_ROOT/data/competitor_analysis.json" ) >>"$LOG_FILE" 2>&1; then
    log "    ✅ competitor_analysis 완료"
    write_status "competitor_analysis" "done" 0
  else
    log "    ⚠ competitor_analysis 실패 — 기존 데이터 유지하고 계속"
    write_status "competitor_analysis" "soft_fail" 1
  fi
else
  log "    ⚠ crawler venv/스크립트 없음 — 스킵 (기존 competitor_analysis.json 유지)"
fi

# ── [6.7] 기획전 시트 동기화 (구글시트 published CSV 직접 fetch — Chrome 불필요) ──
#   "GS 채널 판매 보고" publish-to-web 시트를 urllib로 직접 받아
#   기획전 패키지코드(86XXXXXX)·이벤트를 docs/data/campaign_data.json 에 동기화.
#   호스트는 풀 네트워크라 무인 fetch 가능. fetch 실패 시 스크립트가 쓰기 전에
#   중단되어 기존 campaign_data.json 이 그대로 보존됨 → best-effort(run_crawl).
run_crawl "campaign_data_sync" "scripts/generate_campaign_data.py"

# ── [7] HTML 빌드 (필수) ──────────────────────────────────────────────────
run_fatal "build" "scripts/build.py"

# ── [8] git commit + push ─────────────────────────────────────────────────
log "--- [git] commit + push ---"
write_status "git" "running" 0
rm -f "$PROJECT_ROOT/.git/index.lock" "$PROJECT_ROOT/.git/HEAD.lock" 2>>"$LOG_FILE"

# 크롤·빌드 산출물만 스테이징 (입력 xlsx/txt·로그는 .gitignore 가 거름)
git add data/ docs/ >>"$LOG_FILE" 2>&1

if git diff --cached --quiet; then
  log "    ℹ 변경사항 없음 — 커밋/푸시 스킵"
  write_status "done" "no_changes" 0
  log "=== host_daily_crawl 종료 (변경 없음) ==="
  exit 0
fi

DATE_KST=$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M')
git commit -m "chore(auto): host crawl ${DATE_KST} KST [skip ci]" >>"$LOG_FILE" 2>&1

# push (실패 시 rebase -X theirs 재시도 — 산출물 충돌은 방금 빌드한 로컬본 우선)
PUSH_OK=0
for i in 1 2 3; do
  if git push origin main >>"$LOG_FILE" 2>&1; then
    PUSH_OK=1
    break
  fi
  log "    ⚠ push 시도 $i 실패 — origin 갱신 후 재시도"
  if ! git pull --rebase -X theirs origin main >>"$LOG_FILE" 2>&1; then
    log "    ⚠ rebase 충돌 자동해소 실패 — abort"
    git rebase --abort >>"$LOG_FILE" 2>&1 || true
  fi
done

if [ "$PUSH_OK" -ne 1 ]; then
  log "    ❌ git push 최종 실패"
  write_status "git_push" "error" 1
  exit 1
fi

# 최종 동기화 확인
git fetch origin >>"$LOG_FILE" 2>&1
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)
if [ "$LOCAL" != "$REMOTE" ]; then
  log "    ❌ 푸시 후 로컬($LOCAL) != 리모트($REMOTE)"
  write_status "git_push" "out_of_sync" 1
  exit 1
fi

log "    ✅ push 완료 — GitHub Pages 배포 트리거됨"
write_status "done" "success" 0
log "=== host_daily_crawl 종료 (성공) ==="
exit 0
