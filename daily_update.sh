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

# ── [1/8] 입력 파일 확인 ─────────────────────────────────────
CURRENT_STAGE="1/8 check_inputs"
echo ""
echo -e "${BOLD}[1/8] 입력 파일 확인...${NC}"

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

# ── [2/9] 팔라티움 객실 가용성 ───────────────────────────────
# parse_palatium_db.py 가 palatium_room_availability.json 을 읽으므로 선행 실행
run_if_input "2/9 parse_palatium_rooms" \
    "scripts/parse_palatium_rooms.py" \
    "data/palatium_rooma" "*객실*.xlsx"

# ── [3/9] 온북 파싱 (느림 — 전체 출력) ───────────────────────
# parse_raw_db.py 는 종료 직전 parse_palatium_db.py 도 호출하지만,
# 실패 시 logger.warning 만 띄우고 silent 이므로 아래 [4/9] 에서 명시 재실행한다.
run_streaming "3/9 parse_raw_db" "scripts/parse_raw_db.py"

# ── [4/9] 팔라티움 예약 DB (명시적 실행, 에러 시 중단) ─────
# data/palatium_db/예약정보조회*.xlsx 있을 때만 실행
run_if_input "4/9 parse_palatium_db" \
    "scripts/parse_palatium_db.py" \
    "data/palatium_db" "예약정보조회*.xlsx"

# ── [5/9] ~ [8/9] 후속 집계 ──────────────────────────────────
run_quick "5/9 compare_and_update"          "scripts/compare_and_update.py"
run_quick "6/9 generate_otb_data"           "scripts/generate_otb_data.py"
run_quick "7/9 generate_insights"           "scripts/generate_insights.py"
run_quick "8/10 generate_campaign_perf"     "scripts/generate_campaign_performance.py"
run_quick "9/10 parse_campaign86"           "scripts/parse_campaign86.py"

# ── [10/10] HTML 빌드 ──────────────────────────────────────────
run_quick "10/10 build"                     "scripts/build.py"

# ── Git 커밋 & 푸시 ─────────────────────────────────────────
CURRENT_STAGE="git"
print_header "Git 커밋 & 푸시"

rm -f .git/index.lock .git/HEAD.lock 2>/dev/null || true
git add -A

if git diff --cached --quiet; then
    echo -e "${YELLOW}⚠ 변경사항 없음 — 커밋/푸시 스킵${NC}"
else
    STAT=$(git diff --cached --stat | tail -1)
    DATE_KST=$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M')
    # [skip ci] 로 GitHub Actions 이중 실행 방지
    git commit -m "chore(auto): daily update ${DATE_KST} KST [skip ci]"
    git push origin main
    echo ""
    echo -e "${GREEN}✅ 푸시 완료 — GitHub Pages 배포 시작됨${NC}"
    echo "   $STAT"
fi

print_header "완료: $(date '+%Y-%m-%d %H:%M:%S')"
