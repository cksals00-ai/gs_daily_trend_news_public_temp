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
rm -f .git/index.lock .git/HEAD.lock 2>/dev/null || true

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
run_quick "8/12 generate_insights"          "scripts/generate_insights.py"
run_quick "9/12 generate_campaign_perf"     "scripts/generate_campaign_performance.py"
run_quick "10/12 parse_campaign86"          "scripts/parse_campaign86.py"
run_quick "11/12 generate_chat_index"       "scripts/generate_chat_index.py"

# ── [12/12] HTML 빌드 ──────────────────────────────────────────
run_quick "12/12 build"                     "scripts/build.py"

# ── Git 커밋 & 푸시 (안전 자동화: 원격 발산 시 fetch→merge→산출물 자동해소→재push) ──
#   scripts/host_daily_crawl.sh 의 git 단계와 동일 원칙:
#   - 산출물(data/·docs/) 충돌 → 방금 빌드한 로컬본(--ours) 우선 자동해소
#   - data/·docs/ 밖(손으로 쓴 소스·스크립트 등) 충돌 → merge abort 후 중단(수동 확인)
#   - push 거부(다른 세션·호스트 크롤이 원격 선점) 시 fetch→merge→재push 최대 3회
CURRENT_STAGE="git"
print_header "Git 커밋 & 푸시"

rm -f .git/index.lock .git/HEAD.lock 2>/dev/null || true
git add -A

# 산출물 충돌 자동해소: data/·docs/ 는 --ours(로컬 빌드본), 그 외는 중단要
resolve_generated_conflicts() {   # 0=해소됨, 2=데이터외 충돌(중단要)
    local U BAD
    U="$(git diff --name-only --diff-filter=U 2>/dev/null)"
    [ -z "$U" ] && return 0
    BAD="$(printf '%s\n' "$U" | grep -vE '^(data|docs)/' || true)"
    if [ -n "$BAD" ]; then
        echo -e "${RED}    ❌ data/·docs/ 밖 충돌 — 자동해소 불가:${NC}"
        printf '%s\n' "$BAD" | sed 's/^/        /'
        git merge --abort 2>/dev/null || true
        return 2
    fi
    printf '%s\n' "$U" | while IFS= read -r f; do
        [ -z "$f" ] && continue
        git checkout --ours -- "$f" >/dev/null 2>&1 || true
        git add -- "$f" >/dev/null 2>&1 || true
        echo "    충돌→ours(로컬 빌드본): $f"
    done
    if ! git commit --no-edit >/dev/null 2>&1; then
        echo -e "${RED}    ⚠ merge 커밋 실패 — merge abort${NC}"
        git merge --abort 2>/dev/null || true
        return 2
    fi
    return 0
}

# 안전 push: 거부되면 fetch→merge(--no-edit)→산출물 충돌 자동해소→재push (최대 3회)
#   반환  0=push성공  1=3회 실패  2=소스충돌(중단要). 데이터 커밋·트리거 커밋 공용.
safe_push() {
    local attempt rc
    for attempt in 1 2 3; do
        rm -f .git/index.lock .git/HEAD.lock 2>/dev/null || true
        if git push origin main; then
            return 0
        fi
        echo -e "${YELLOW}    ⚠ push 시도 ${attempt} 실패(원격 발산) — origin fetch 후 merge 재시도${NC}"
        if ! git fetch origin main; then
            echo -e "${YELLOW}    ⚠ fetch 실패(네트워크?) — 다음 시도${NC}"
            continue
        fi
        if git merge --no-edit origin/main; then
            echo -e "${GREEN}    ✅ merge 클린 — 재push${NC}"
        else
            # set -e + ERR trap 하에서 함수 반환값을 안전하게 받기 위해 if 로 가드
            if resolve_generated_conflicts; then rc=0; else rc=$?; fi
            [ "$rc" -eq 2 ] && return 2
            echo -e "${GREEN}    ✅ 산출물 충돌 자동해소 완료 — 재push${NC}"
        fi
    done
    return 1
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
    git fetch origin main >/dev/null 2>&1 || true
    git merge --ff-only origin/main >/dev/null 2>&1 || true
else
    STAT=$(git diff --cached --stat | tail -1)
    DATE_KST=$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M')
    # [skip ci] 로 GitHub Actions 이중 실행 방지
    git commit -m "chore(auto): daily update ${DATE_KST} KST [skip ci]"

    if safe_push; then prc=0; else prc=$?; fi
    if [ "$prc" -eq 2 ]; then
        echo -e "${RED}    ❌ 자동 merge 불가(소스 충돌 등) — 수동 확인 필요${NC}"
        echo -e "${RED}       (방금 만든 로컬 커밋은 보존되어 있습니다)${NC}"
        exit 1
    fi
    if [ "$prc" -ne 0 ]; then
        echo -e "${RED}    ❌ git push 최종 실패(3회) — 로컬 커밋은 보존됨, 잠시 후 재실행하세요${NC}"
        exit 1
    fi

    echo ""
    echo -e "${GREEN}✅ 푸시 완료${NC}"
    echo "   $STAT"

    # 데이터 push 성공 직후 즉시 배포 트리거
    trigger_deploy
fi

print_header "완료: $(date '+%Y-%m-%d %H:%M:%S')"
