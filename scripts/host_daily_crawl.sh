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
#   3.5) fetch_ga4.py         — GA4 웹 애널리틱스 (OAuth 토큰 로컬전용, 실패 시 기존 유지)
#   4) generate_insights.py   — KPI → 인사이트 (로컬)
#   5) gen_sdd_by_prop.py     — 사업장별 SDD (로컬, optional)
#   6) generate_campaign_performance.py — raw_db 27/28 txt 있을 때만
#   6.7) generate_campaign_data.py — 구글시트 기획전 동기화 (published CSV 직접 fetch, Chrome 불필요)
#   7) build.py               — HTML 전체 빌드
#   8) git add data/ docs/ → commit → fetch·merge(산출물 --ours 자동해소)·push 재시도
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

# 하위 스텝 soft-fail 누적 — 최종 status 에 반드시 드러나게 한다.
# (예전엔 마지막 write_status "done" "success" 가 앞선 soft_fail 을 덮어써서
#  기획전 동기화가 며칠째 실패해도 status=success 로만 보였다.)
SOFT_FAILS=()

write_status() {
  # $1=stage $2=status $3=exit_code
  local sf_json="" first=1
  for s in "${SOFT_FAILS[@]}"; do
    [ $first -eq 1 ] && first=0 || sf_json="$sf_json, "
    sf_json="$sf_json\"$s\""
  done
  local overall="$2"
  # 하위 스텝이 하나라도 실패했으면 최종 성공을 partial 로 강등
  if [ "$1" = "done" ] && [ "$2" = "success" ] && [ ${#SOFT_FAILS[@]} -gt 0 ]; then
    overall="partial"
  fi
  cat > "$STATUS_FILE" <<EOF
{"stage": "$1", "status": "$overall", "exit_code": $3, "ts": "$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S')", "soft_failures": [$sf_json], "stale_sources": [$STALE_JSON]}
EOF
}

# 산출물 신선도 점검 결과 (아래 check_freshness 가 채움)
STALE_JSON=""

log "============================================================"
log "=== host_daily_crawl 시작 ==="
log "PROJECT_ROOT: $PROJECT_ROOT"
log "Python      : $PY ($($PY --version 2>&1))"
write_status "init" "running" 0

# ── git lock 정리 (stale 락 자동정리: index/HEAD/config/refs 등 모든 *.lock) ──
#   push 실패의 흔한 원인이 죽은 프로세스가 남긴 stale 락이므로 시작 시 싹 제거.
find "$PROJECT_ROOT/.git" -maxdepth 3 -name '*.lock' -type f -delete 2>>"$LOG_FILE" || true

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
    SOFT_FAILS+=("$label")
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
# GA4 웹 애널리틱스 — OAuth 토큰(ga4_oauth_token.json, gitignore·로컬전용)으로 수집.
# CI엔 토큰이 없어 호스트 데몬만 갱신 가능. 실패 시 기존 docs/data/ga4_latest.json 보존(soft-fail).
run_crawl "fetch_ga4"          "scripts/fetch_ga4.py"

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
    SOFT_FAILS+=("competitor_analysis")
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

# ── [6.9] 부킹/RM PDF 자동 파싱 (회귀 안전·이중커밋 없음) ─────────────────
#   사용자가 update_daily_booking.sh / update_rm_fcst.sh 를 매일 수동 실행하는 걸
#   깜빡하면 며칠씩 stale 이 된다(이번 사건의 근본원인). 여기서 최신 PDF 를
#   자동 반영하되:
#     - 파일명 날짜가 "이미 반영된 보고일보다 엄격히 최신"일 때만 파싱(회귀 방지)
#     - best-effort: 파싱 실패해도 크롤 중단 안 함(SOFT_FAILS 기록)
#     - 별도 commit/push 없음 — 바로 뒤 build + 크롤 자체의 단일 commit 이 흡수
#   수동 스크립트(update_*.sh)는 그대로 유효(회귀가드·검증·강제옵션 포함).
auto_parse_pdf() {
  # $1=label $2=parser $3=pdf_glob_dir $4=pdf_iname $5=current_date(ISO or empty)
  local label="$1" parser="$2" dir="$3" iname="$4" cur="$5"
  [ -f "$parser" ] || { log "    ⚠ $label: $parser 없음 — 스킵"; return 0; }
  [ -d "$dir" ]    || { log "    ⚠ $label: $dir 없음 — 스킵"; return 0; }
  #   파일명 어디든 있는 YYYY.MM.DD 를 정렬키로 뽑는다(날짜 뒤 접미사 파일도 처리).
  #   키 없는(날짜 없는) 파일은 제외.
  local pdf
  pdf="$(find "$dir" -maxdepth 1 -type f -iname "$iname" 2>/dev/null \
        | sed -E 's/.*_([0-9]{4}\.[0-9]{2}\.[0-9]{2}).*/\1\t&/' \
        | grep -E '^[0-9]{4}\.[0-9]{2}\.[0-9]{2}\t' \
        | sort | tail -1 | cut -f2-)"
  [ -n "$pdf" ] || { log "    ⚠ $label: 날짜 있는 PDF 없음 — 스킵(기존 유지)"; return 0; }
  local pdate
  pdate="$(basename "$pdf" | sed -E 's/.*_([0-9]{4})\.([0-9]{2})\.([0-9]{2}).*/\1-\2-\3/')"
  if [ -n "$cur" ] && [ -n "$pdate" ] && [ ! "$pdate" \> "$cur" ]; then
    log "    ✅ $label: 최신 PDF($pdate) ≤ 반영본($cur) — 재파싱 불필요"
    return 0
  fi
  log "    → $label: 신규 PDF 파싱 ($pdate > ${cur:-없음}) — $(basename "$pdf")"
  if "$PY" "$parser" "$pdf" >>"$LOG_FILE" 2>&1; then
    log "    ✅ $label: $pdate 반영 완료"
  else
    local rc=$?
    log "    ⚠ $label: 파싱 실패(exit=$rc) — 기존 데이터 유지"
    SOFT_FAILS+=("$label")
  fi
  return 0
}

log "--- [parse] 부킹/RM PDF 자동 반영 ---"
BK_CUR="$("$PY" -c "import json;d=json.load(open('data/daily_booking.json'));md=d.get('months_detail') or [{}];print(md[0].get('report_date','') or '')" 2>/dev/null)"
RM_CUR="$("$PY" -c "import json;print((json.load(open('data/rm_fcst.json')).get('_snapshot_date','') or '').replace('.','-'))" 2>/dev/null)"
auto_parse_pdf "auto_booking" "scripts/parse_daily_booking.py" \
  "data/Daily Booking Report PDF" "Daily Booking Report_*.pdf" "$BK_CUR"
auto_parse_pdf "auto_rm_fcst"  "scripts/parse_rm_fcst.py" \
  "data/RM자료" "Revenue*Meeting_*.pdf" "$RM_CUR"

# ── [7] HTML 빌드 (필수) ──────────────────────────────────────────────────
run_fatal "build" "scripts/build.py"

# ── [7.5] 산출물 신선도 점검 (소스 미수신 조용한 정지 감지) ──────────────
#   Daily Booking / RM FCST 는 사용자가 PDF 를 드롭해야 갱신된다. 소스가 안 들어오면
#   파이프라인은 정상 종료하므로, 며칠씩 stale 인 것을 아무도 모른 채 지나간다.
#   → 기준일수 초과 시 로그 경고 + status.stale_sources 에 기록.
#   주의: daily_booking 의 meta.report_date 는 갱신되지 않는 stale 필드다(항상 옛날짜).
#         신뢰 가능한 최신 보고일은 months_detail[0].report_date (update 스크립트 회귀가드도 이 값 사용).
log "--- [check] 산출물 신선도 ---"
STALE_JSON=$("$PY" - <<'PYEOF' 2>>"$LOG_FILE"
import json, datetime, pathlib
today = datetime.date.today()
stale = []
def age(d):
    return (today - d).days
try:
    d = json.load(open('data/daily_booking.json'))
    md = d.get('months_detail') or [{}]
    rpt = md[0].get('report_date') or ''
    dd = datetime.date.fromisoformat(rpt)
    if age(dd) > 2:
        stale.append(f"daily_booking:{rpt}({age(dd)}일)")
except Exception as e:
    stale.append(f"daily_booking:read_error({e})")
try:
    r = json.load(open('data/rm_fcst.json'))
    sd = datetime.date.fromisoformat(r['_snapshot_date'].replace('.', '-'))
    if age(sd) > 8:   # RM 은 주간 회의 자료 → 8일 기준
        stale.append(f"rm_fcst:{r['_snapshot_date']}({age(sd)}일)")
except Exception as e:
    stale.append(f"rm_fcst:read_error({e})")
for s in stale:
    print(f"STALE {s}", file=__import__('sys').stderr)
print(", ".join(f'"{s}"' for s in stale))
PYEOF
)
if [ -n "$STALE_JSON" ]; then
  log "    ⚠ stale 산출물: $STALE_JSON — 소스 PDF 미수신 가능성(사용자 확인 필요)"
else
  log "    ✅ 산출물 신선도 정상"
fi

# ── [8] git commit + push (안전 자동화: 잔재정리→main보정→fetch·rebase→push) ──
log "--- [git] commit + push ---"

write_status "git" "running" 0
cleanup_locks() { find "$PROJECT_ROOT/.git" -maxdepth 3 -name '*.lock' -type f -delete 2>>"$LOG_FILE" || true; }
cleanup_locks

# 공용 git 안전 계층 — daily_update.sh 와 동일 로직(잔재정리·main보정·rebase push)
# shellcheck source=git_safe.sh
GSN_GIT_OUT="$LOG_FILE"
source "$PROJECT_ROOT/scripts/git_safe.sh"
gsn_log() { log "$*"; }

# 커밋 전 상태 보정: 중단된 rebase 잔재·detached HEAD 를 여기서 흡수한다.
#   (2026-08-04: 잔재 rebase + detached HEAD 로 크롤 커밋이 브랜치 밖에 쌓였던 사고)
gsn_git_heal_state
if gsn_git_ensure_main; then :; else
  log "    ❌ main 브랜치 확보 실패 — 커밋하지 않고 중단 (산출물은 워킹트리에 보존)"
  write_status "git" "branch_error" 1
  log "=== host_daily_crawl 종료 (브랜치 오류) ==="
  exit 1
fi

# 크롤·빌드 산출물만 스테이징 (입력 xlsx/txt·로그는 .gitignore 가 거름)
#   _host_crawl_status.json 은 크롤이 소유하는 상태파일 → 함께 커밋해 최신 ts 를 원격에 반영.
git add data/ docs/ _host_crawl_status.json >>"$LOG_FILE" 2>&1

if git diff --cached --quiet; then
  log "    ℹ 변경사항 없음 — 커밋 스킵 (원격 fast-forward 만 확인)"
  _gsn_git "$GSN_GIT_T_FETCH" "fetch origin main" fetch origin main || true
  _gsn_git "$GSN_GIT_T_LOCAL" "merge --ff-only" merge --ff-only origin/main || true
  write_status "done" "no_changes" 0
  log "=== host_daily_crawl 종료 (변경 없음) ==="
  exit 0
fi

DATE_KST=$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M')
git commit -m "chore(auto): host crawl ${DATE_KST} KST [skip ci]" >>"$LOG_FILE" 2>&1

# push: fetch → rebase origin/main(생성물 충돌=재빌드본 우선 자동해소) → push, 최대 3회.
#   충돌 해소·잔재정리·main 보정은 모두 scripts/git_safe.sh 가 담당(force-push 없음).
PUSH_OK=0
if gsn_git_sync_push; then prc=0; else prc=$?; fi
case "$prc" in
  0) PUSH_OK=1 ;;
  2) log "    ❌ 코드(data/·docs/ 밖) 충돌 — 자동 rebase 불가, 수동 확인 필요 (로컬 커밋은 보존됨)"
     write_status "git_push" "merge_conflict" 1
     log "=== host_daily_crawl 종료 (충돌) ==="
     exit 1 ;;
  3) log "    ❌ git 구조 문제(브랜치·HEAD) — 자동 진행 불가 (로컬 커밋은 refs/gsn-backup/* 에 보존)"
     write_status "git_push" "branch_error" 1
     log "=== host_daily_crawl 종료 (브랜치 오류) ==="
     exit 1 ;;
esac

if [ "$PUSH_OK" -ne 1 ]; then
  log "    ❌ git push 최종 실패 (3회) — 로컬 커밋은 보존됨, 다음 05:00 실행에서 재시도"
  write_status "git_push" "error" 1
  log "=== host_daily_crawl 종료 (push 실패) ==="
  exit 1
fi

# 최종 동기화 확인
_gsn_git "$GSN_GIT_T_FETCH" "fetch origin" fetch origin || true
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)
if [ "$LOCAL" != "$REMOTE" ]; then
  log "    ⚠ 푸시 후 로컬($LOCAL) != 리모트($REMOTE) — origin 이 더 앞섬(다른 세션). 다음 실행에서 정리됨"
  write_status "git_push" "out_of_sync" 1
  exit 1
fi

log "    ✅ push 완료 — GitHub Pages 배포 트리거됨"
write_status "done" "success" 0
log "=== host_daily_crawl 종료 (성공) ==="
exit 0
