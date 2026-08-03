#!/bin/bash
# daily_update.sh — 영업기획팀 매일 데이터 업데이트 파이프라인 (수동 실행용)
# 사용법:
#   cd ~/Desktop/gs_daily_trend_news_public_temp
#   ./daily_update.sh
#
# 사전 준비:
#   1) data/raw_db/ 폴더에 오늘자 온북 txt (27.*.txt, 28.*.txt) 넣기
#   2) data/palatium_db/ 폴더에 팔라티움 예약정보조회/사업계획 xlsx 넣기
#   3) data/palatium_rooma/ 폴더에 "사용가능 객실 현황*.xlsx" 넣기 (선택)
#
# 자동 스케줄러용 _run_daily_pipeline.sh 와는 별도 파일.

set -e
set -o pipefail
cd "$(dirname "$0")"

# ── 색상 (가독성) ────────────────────────────────────────────
BOLD='\033[1m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ── 에러 핸들러: 어느 단계에서 실패했는지 표시 ───────────────
CURRENT_STAGE="init"
on_error() {
    EC=$?
    echo ""
    echo -e "${RED}=========================================="
    echo -e "  ❌ 실패: [${CURRENT_STAGE}]  (exit=${EC})"
    echo -e "==========================================${NC}"
    echo -e "${RED}  위 출력에서 에러 메시지를 확인 후 재실행하세요.${NC}"
    echo ""
    exit "$EC"
}
trap on_error ERR

# ── .git lock 정리 ───────────────────────────────────────────
find .git -maxdepth 3 -name '*.lock' -type f -delete 2>/dev/null || true

# ── 헬퍼 함수 ────────────────────────────────────────────────
print_header() {
    echo ""
    echo -e "${BOLD}${BLUE}==========================================${NC}"
    echo -e "${BOLD}${BLUE}  $1${NC}"
    echo -e "${BOLD}${BLUE}==========================================${NC}"
}

# 빠른 스텝: 마지막 몇 줄만 표시 (출력 절약)
run_quick() {
    local label="$1"
    local script="$2"
    CURRENT_STAGE="$label"
    echo ""
    echo -e "${BOLD}[${label}]${NC}"
    if [ ! -f "$script" ]; then
        echo -e "  ${YELLOW}⚠ $script 없음 — 스킵${NC}"
        return 0
    fi
    python3 "$script" 2>&1 | tail -5
    echo -e "  ${GREEN}✅ 완료${NC}"
}

# 느린 스텝: 전체 출력을 그대로 스트리밍 (진행률 확인)
run_streaming() {
    local label="$1"
    local script="$2"
    CURRENT_STAGE="$label"
    echo ""
    echo -e "${BOLD}[${label}] (시간 소요 — 진행률 스트리밍)${NC}"
    python3 "$script"
    echo -e "  ${GREEN}✅ 완료${NC}"
}

# 조건부 스텝: 패턴에 매칭되는 입력 파일이 있을 때만 실행
run_if_input() {
    local label="$1"
    local script="$2"
    local glob_dir="$3"
    local glob_pat="$4"
    CURRENT_STAGE="$label"
    echo ""
    echo -e "${BOLD}[${label}]${NC}"
    local match
    match=$(find "$glob_dir" -maxdepth 1 -name "$glob_pat" 2>/dev/null | head -1)
    if [ -z "$match" ]; then
        echo -e "  ${YELLOW}⚠ ${glob_dir}/${glob_pat} 없음 — 스킵${NC}"
        return 0
    fi
    echo "  ✓ 입력: $(basename "$match")"
    python3 "$script" 2>&1 | tail -8
    echo -e "  ${GREEN}✅ 완료${NC}"
}

# ── 시작 ─────────────────────────────────────────────────────
print_header "GS팀 일별 데이터 업데이트 파이프라인"
echo "  시작 : $(date '+%Y-%m-%d %H:%M:%S')"
echo "  PWD  : $(pwd)"
echo "  Py   : $(python3 --version 2>&1)"

# ── [1/12] 입력 파일 확인 ─────────────────────────────────────
CURRENT_STAGE="1/12 check_inputs"
echo ""
echo -e "${BOLD}[1/12] 입력 파일 확인...${NC}"

REF_FILE="docs/data/db_aggregated.json"
NEW_TXT=0
if [ -f "$REF_FILE" ]; then
    TXT_27=$(find data/raw_db -maxdepth 2 -name '27.*.txt' -newer "$REF_FILE" 2>/dev/null | head -1)
    TXT_28=$(find data/raw_db -maxdepth 2 -name '28.*.txt' -newer "$REF_FILE" 2>/dev/null | head -1)
    if [ -n "$TXT_27" ] || [ -n "$TXT_28" ]; then
        echo -e "  ${GREEN}✅ 새로운 온북 txt 발견${NC}"
        [ -n "$TXT_27" ] && echo "     27: $(basename "$TXT_27")"
        [ -n "$TXT_28" ] && echo "     28: $(basename "$TXT_28")"
        NEW_TXT=1
    else
        echo -e "  ${YELLOW}⚠ 새로운 온북 txt 없음 — 기존 데이터로 진행${NC}"
    fi
else
    echo -e "  ${YELLOW}⚠ $REF_FILE 없음 — 신규 빌드로 진행${NC}"
fi

PALAT_DB=$(find data/palatium_db -maxdepth 1 -name '예약정보조회*.xlsx' 2>/dev/null | head -1)
PALAT_BIZ=$(find data/palatium_db -maxdepth 1 -name '*사업계획*.xlsx' 2>/dev/null | head -1)
PALAT_ROOM=$(find data/palatium_rooma -maxdepth 1 -name '*객실*.xlsx' 2>/dev/null | head -1)
[ -n "$PALAT_DB"   ] && echo "  ✓ 팔라티움 예약 DB     : $(basename "$PALAT_DB")"
[ -n "$PALAT_BIZ"  ] && echo "  ✓ 팔라티움 사업계획     : $(basename "$PALAT_BIZ")"
[ -n "$PALAT_ROOM" ] && echo "  ✓ 팔라티움 객실 현황    : $(basename "$PALAT_ROOM")"

# ── [2/12] 팔라티움 객실 가용성 ───────────────────────────────
# parse_palatium_db.py 가 palatium_room_availability.json 을 읽으므로 선행 실행
run_if_input "2/12 parse_palatium_rooms" \
    "scripts/parse_palatium_rooms.py" \
    "data/palatium_rooma" "*객실*.xlsx"

# ── [3/12] 온북 파싱 (느림 — 전체 출력) ───────────────────────
# parse_raw_db.py 는 종료 직전 parse_palatium_db.py 도 호출하지만,
# 실패 시 logger.warning 만 띄우고 silent 이므로 아래 [4/12] 에서 명시 재실행한다.
run_streaming "3/12 parse_raw_db" "scripts/parse_raw_db.py"

# ── [4/12] 팔라티움 예약 DB (명시적 실행, 에러 시 중단) ─────
# data/palatium_db/예약정보조회*.xlsx 있을 때만 실행
run_if_input "4/12 parse_palatium_db" \
    "scripts/parse_palatium_db.py" \
    "data/palatium_db" "예약정보조회*.xlsx"

# ── [5/12] 동월 대비 예약 비율 ────────────────────────────────
# db_aggregated.json (3/12), rm_fcst.json, raw_db 27번 txt 필요
run_quick "5/12 build_same_month_ratio"     "scripts/build_same_month_ratio.py"

# ── [6/12] ~ [12/12] 후속 집계 ───────────────────────────────
run_quick "6/12 compare_and_update"         "scripts/compare_and_update.py"
run_quick "7/12 generate_otb_data"          "scripts/generate_otb_data.py"
# 7b — 7월 동기간 픽업(데일리 보고 리포트): 온북 갱신과 함께 자동 갱신. raw_db 직접 net.
#       Desktop 사본은 자동 파이프라인에선 생략(PICKUP_NO_DESKTOP). 산출물은 git add -A 로 함께 커밋/푸시.
echo ""; echo -e "${BOLD}[7b/12 build_july_pickup]${NC}"
if [ -f scripts/build_july_pickup_tracker.py ]; then
    PICKUP_NO_DESKTOP=1 python3 scripts/build_july_pickup_tracker.py 2>&1 | tail -5
    echo -e "  ${GREEN}✅ 완료${NC}"
else
    echo -e "  ${YELLOW}⚠ scripts/build_july_pickup_tracker.py 없음 — 스킵${NC}"
fi
run_quick "8/12 generate_insights"          "scripts/generate_insights.py"
run_quick "9/12 generate_campaign_perf"     "scripts/generate_campaign_performance.py"
run_quick "10/12 parse_campaign86"          "scripts/parse_campaign86.py"
run_quick "10b/12 campaign_history"         "scripts/build_campaign_history.py"
# 10c — 숙세페 여름편 실적 슬라이스(gs-salefesta.html §5). campaign_history.json 산출 이후에 실행해야 함.
run_quick "10c/12 salefesta_perf"           "scripts/build_salefesta_perf.py"
# 10d — 스페셜(최성수기)/연휴 전략상품 실적 팔로업(index.html). raw_db(3/12 parse_raw_db 이후 최신)만 읽는
#       독립 산출물 → db/otb/gz 무접촉. special_period.json 은 아래 git 단계에서 함께 커밋됨.
run_quick "10d/12 special_period"           "scripts/build_special_period.py"
run_quick "11/12 generate_chat_index"       "scripts/generate_chat_index.py"

# ── [12/12] HTML 빌드 ──────────────────────────────────────────
run_quick "12/12 build"                     "scripts/build.py"

# ── Git 커밋 & 푸시 (안전 자동화: 잔재 정리 → main 보정 → fetch·rebase → push) ──
#   scripts/git_safe.sh 로 host_daily_crawl.sh 와 로직을 공유한다:
#   - 커밋 전: 미완 rebase/merge 잔재 정리 + detached HEAD → main 복귀(커밋 보존)
#   - 산출물(data/·docs/) 충돌 → 방금 빌드한 재빌드본 우선 자동해소
#   - data/·docs/ 밖(손으로 쓴 소스·스크립트 등) 충돌 → rebase 되돌린 뒤 중단(수동 확인)
#   - push 거부(다른 세션·호스트 크롤이 원격 선점) 시 fetch→rebase→재push 최대 3회(force 금지)
CURRENT_STAGE="git"
print_header "Git 커밋 & 푸시"

# shellcheck source=scripts/git_safe.sh
source "scripts/git_safe.sh"
gsn_log() { echo -e "$*"; }

# 커밋 전 상태 보정: 중단된 rebase 잔재·detached HEAD 를 여기서 흡수한다.
#   (2026-08-04: 잔재 rebase + detached HEAD 로 daily 커밋이 브랜치 밖에 고립됐던 사고)
gsn_git_heal_state
if gsn_git_ensure_main; then :; else
    echo -e "${RED}    ❌ main 브랜치 확보 실패 — 커밋하지 않고 중단${NC}"
    echo -e "${RED}       빌드 산출물은 워킹트리에 그대로 있습니다(유실 없음).${NC}"
    exit 1
fi

git add -A
# _host_crawl_status.json 은 호스트 크롤이 소유하는 상태파일 → daily_update 는 stale 본을
# 커밋해 원격 최신 ts 를 되돌리면 안 됨(주말 status ts 롤백 원인). 스테이지에서 제외.
git reset -q -- _host_crawl_status.json 2>/dev/null || true

# 안전 push: scripts/git_safe.sh 의 gsn_git_sync_push 위임
#   fetch → rebase origin/main(생성물 충돌=재빌드본 우선 자동해소) → push, 최대 3회.
#   반환  0=push성공  1=3회 실패  2=코드충돌(중단要)  3=구조적 중단(브랜치 등).
#
#   DEPLOY_ALREADY_TRIGGERED: 과거 merge 방식에선 발산 흡수용 merge 커밋([skip ci] 없음)이
#   deploy.yml push 트리거를 발동시켜 중복 빌드를 막을 필요가 있었다. rebase 방식에선
#   merge 커밋이 생기지 않아 push 되는 커밋이 전부 [skip ci] → 배포가 트리거되지 않는다.
#   따라서 항상 0 으로 두고 아래 trigger_deploy() 가 배포를 책임진다.
DEPLOY_ALREADY_TRIGGERED=0
safe_push() {
    local rc
    if gsn_git_sync_push; then rc=0; else rc=$?; fi
    return "$rc"
}

# 즉시 배포 트리거: 데이터 커밋엔 [skip ci]가 있어 push 배포가 스킵됨.
#   docs/.deploy-trigger 를 갱신([skip ci] 없음)해 deploy.yml 의 docs/** push 트리거를
#   즉시 발동 → 4h cron 안 기다리고 바로 GitHub Pages 빌드/배포. (실패해도 데이터는 이미
#   반영됐으므로 경고만; 다음 cron·실행에서 배포됨.)
trigger_deploy() {
    local TRIG_TS
    TRIG_TS=$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S KST')
    printf 'deploy trigger\n%s\n' "$TRIG_TS" > docs/.deploy-trigger
    git add docs/.deploy-trigger
    git commit -m "chore(deploy): trigger Pages build ${TRIG_TS} (no skip-ci)" >/dev/null
    if safe_push; then
        echo -e "${GREEN}🚀 즉시 배포 트리거 완료 — GitHub Actions 빌드 시작됨${NC}"
    else
        echo -e "${YELLOW}⚠ 배포 트리거 push 실패 — 데이터는 이미 반영됨(다음 cron/실행에서 배포)${NC}"
    fi
}

if git diff --cached --quiet; then
    echo -e "${YELLOW}⚠ 변경사항 없음 — 커밋/푸시 스킵${NC}"
    # 원격이 앞서 있으면 fast-forward 만 맞춰둠 (다음 실행 발산 예방)
    _gsn_git "$GSN_GIT_T_FETCH" "fetch origin main" fetch origin main >/dev/null 2>&1 || true
    _gsn_git "$GSN_GIT_T_LOCAL" "merge --ff-only" merge --ff-only origin/main >/dev/null 2>&1 || true
else
    STAT=$(git diff --cached --stat | tail -1)
    DATE_KST=$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M')
    # [skip ci] 로 GitHub Actions 이중 실행 방지
    git commit -m "chore(auto): daily update ${DATE_KST} KST [skip ci]"

    # 이번 데이터 push 한정으로 트리거 발동 여부 초기화
    DEPLOY_ALREADY_TRIGGERED=0
    if safe_push; then prc=0; else prc=$?; fi
    if [ "$prc" -eq 2 ]; then
        echo -e "${RED}    ❌ 코드(data/·docs/ 밖) 충돌 — 자동 rebase 불가, 수동 확인 필요${NC}"
        echo -e "${RED}       (rebase 는 되돌렸고 방금 만든 로컬 커밋은 보존되어 있습니다)${NC}"
        exit 1
    fi
    if [ "$prc" -eq 3 ]; then
        echo -e "${RED}    ❌ git 구조 문제(브랜치·HEAD) — 자동 진행 불가, 수동 확인 필요${NC}"
        echo -e "${RED}       (로컬 커밋은 보존, 필요 시 refs/gsn-backup/* 에서 복구 가능)${NC}"
        exit 1
    fi
    if [ "$prc" -ne 0 ]; then
        echo -e "${RED}    ❌ git push 최종 실패(3회) — 로컬 커밋은 보존됨, 잠시 후 재실행하세요${NC}"
        exit 1
    fi

    echo ""
    echo -e "${GREEN}✅ 푸시 완료${NC}"
    echo "   $STAT"

    # 데이터 push 성공 직후 즉시 배포 트리거 — 단, 중복 빌드 방지:
    #   rebase 방식에선 발산을 흡수해도 merge 커밋이 생기지 않아 push 되는 커밋이
    #   전부 [skip ci] → deploy.yml push 트리거가 안 돈다. 따라서 항상 아래
    #   trigger_deploy 가 no-skip-ci 트리거 커밋을 올려 1빌드를 보장한다.
    #   (플래그는 merge 방식으로 되돌릴 경우를 위해 분기만 남겨둠)
    if [ "$DEPLOY_ALREADY_TRIGGERED" -eq 1 ]; then
        echo -e "${GREEN}🚀 이미 merge 커밋(no [skip ci])으로 빌드 트리거됨 — 트리거 커밋 생략 (중복 빌드 방지)${NC}"
    else
        trigger_deploy
    fi
fi

print_header "완료: $(date '+%Y-%m-%d %H:%M:%S')"
