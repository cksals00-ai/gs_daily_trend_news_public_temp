#!/usr/bin/env bash
# =============================================================================
# update_daily_booking.sh — Daily Booking Report PDF → 리포트 반영 (수동/매일 실행용)
# =============================================================================
# 하는 일:
#   1) data/ 안에서 "가장 최신" Daily Booking Report PDF 자동 탐지 (파일명 날짜 기준)
#   2) scripts/parse_daily_booking.py 로 파싱 → data/daily_booking.json,
#      docs/data/daily_booking.json 갱신
#   3) 변경이 없으면(같은 PDF 재실행) 빌드·커밋 생략 (멱등)
#   4) 변경이 있으면 scripts/build.py 로 docs/index.html 에 당일 부킹 섹션 주입
#   5) 핵심 수치(Grand Total) 출력해 검증
#   6) git: lock 정리 → 커밋 → pull(--no-rebase --autostash, 생성파일 충돌 자동 --ours)
#      → push (push 거부 시 1회 pull 후 재시도)
#
# 사용법:
#   cd ~/Projects/gs_daily_trend_news_public_temp     # (또는 ~/Desktop/...)
#   ./scripts/update_daily_booking.sh                 # 평소: 이것만 치면 됨
#   ./scripts/update_daily_booking.sh --force         # 같은 PDF라도 강제 재빌드
#   ./scripts/update_daily_booking.sh --no-push       # 커밋만, 푸시 생략
#   ./scripts/update_daily_booking.sh /경로/특정.pdf  # 특정 PDF 명시 지정
#
# 멱등성: 같은 PDF를 두 번 돌려도 안전 (두 번째는 "이미 반영됨" 으로 스킵).
# =============================================================================

set -uo pipefail   # set -e 는 쓰지 않음 — 단계별로 직접 에러 처리(die)함

# ── 인자 파싱 ────────────────────────────────────────────────
FORCE=0
DO_PUSH=1
EXPLICIT_PDF=""
for arg in "$@"; do
    case "$arg" in
        --force)   FORCE=1 ;;
        --no-push) DO_PUSH=0 ;;
        -h|--help)
            sed -n '2,30p' "$0"; exit 0 ;;
        *.pdf|*.PDF) EXPLICIT_PDF="$arg" ;;
        *) echo "알 수 없는 인자: $arg (무시)" ;;
    esac
done

# ── 색상 ─────────────────────────────────────────────────────
if [ -t 1 ]; then
    BOLD='\033[1m'; RED='\033[0;31m'; GREEN='\033[0;32m'
    YELLOW='\033[0;33m'; BLUE='\033[0;34m'; NC='\033[0m'
else
    BOLD=''; RED=''; GREEN=''; YELLOW=''; BLUE=''; NC=''
fi

log()  { echo -e "${BLUE}▶${NC} $*"; }
ok()   { echo -e "${GREEN}✅${NC} $*"; }
warn() { echo -e "${YELLOW}⚠ ${NC} $*"; }
die()  { echo -e "\n${RED}❌ 실패: $*${NC}\n   위 출력에서 원인 확인 후 재실행하세요." >&2; exit 1; }

# ── 리포지토리 루트로 이동 ───────────────────────────────────
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" || die "리포지토리 경로 확인 실패"
cd "$REPO" || die "cd $REPO 실패"

PARSER="scripts/parse_daily_booking.py"
BUILDER="scripts/build.py"
DATA_JSON="data/daily_booking.json"
DOCS_JSON="docs/data/daily_booking.json"
INDEX_HTML="docs/index.html"

command -v python3 >/dev/null 2>&1 || die "python3 가 없습니다"
[ -f "$PARSER" ]  || die "$PARSER 없음"
[ -f "$BUILDER" ] || die "$BUILDER 없음"

echo -e "${BOLD}${BLUE}========================================================${NC}"
echo -e "${BOLD}${BLUE}  Daily Booking Report → 리포트 반영${NC}"
echo -e "${BOLD}${BLUE}========================================================${NC}"
echo "  시작 : $(date '+%Y-%m-%d %H:%M:%S')"
echo "  PWD  : $REPO"
echo "  Py   : $(python3 --version 2>&1)"

# ── git lock 잔여 정리(헬퍼) ─────────────────────────────────
cleanup_locks() { rm -f "$REPO/.git/index.lock" "$REPO/.git/HEAD.lock" 2>/dev/null || true; }
cleanup_locks

# =============================================================================
# [1/6] 최신 Daily Booking Report PDF 탐지
# =============================================================================
log "[1/6] 최신 Daily Booking Report PDF 탐지..."

if [ -n "$EXPLICIT_PDF" ]; then
    [ -f "$EXPLICIT_PDF" ] || die "지정한 PDF가 없습니다: $EXPLICIT_PDF"
    PDF="$EXPLICIT_PDF"
    ok "명시 지정: $PDF"
else
    # 후보 디렉터리(루트 + data/ + data/Daily Booking Report PDF/)에서
    # 'Daily Booking Report_YYYY.MM.DD.pdf' 만 수집 (Booking Status Report 제외).
    # 파일명 날짜(YYYY.MM.DD)는 사전식 정렬 = 시간순 → 가장 마지막이 최신.
    PDF="$(
        find "$REPO" "$REPO/data" "$REPO/data/Daily Booking Report PDF" \
             -maxdepth 1 -type f -name "Daily Booking Report_*.pdf" 2>/dev/null \
        | while IFS= read -r f; do
              d=$(basename "$f" | sed -E 's/^Daily Booking Report_([0-9]{4}\.[0-9]{2}\.[0-9]{2})\.pdf$/\1/')
              # 날짜 추출 실패한(패턴 불일치) 파일은 건너뜀
              [ "$d" != "$(basename "$f")" ] && printf '%s\t%s\n' "$d" "$f"
          done \
        | sort -u | tail -1 | cut -f2-
    )"
    [ -n "$PDF" ] || die "Daily Booking Report PDF를 찾지 못했습니다 (data/ 또는 data/Daily Booking Report PDF/ 에 'Daily Booking Report_YYYY.MM.DD.pdf' 배치 필요)"
    ok "최신 PDF: $(basename "$PDF")"
fi
PDF_DATE="$(basename "$PDF" | sed -E 's/^Daily Booking Report_([0-9]{4}\.[0-9]{2}\.[0-9]{2})\.pdf$/\1/' | tr '.' '-')"

# =============================================================================
# [2/6] 파싱 → daily_booking.json 갱신
# =============================================================================
log "[2/6] 파싱: parse_daily_booking.py"
python3 "$PARSER" "$PDF" || die "파싱 실패 ($PARSER)"
[ -f "$DATA_JSON" ] || die "$DATA_JSON 가 생성되지 않았습니다"

# 파싱 결과 핵심 수치 추출(검증 + 멱등 판정용 해시)
read -r RPT_DATE GT_RNS GT_BUD GT_ACH GT_OCC GT_CHG MONTHS_CSV <<EOF
$(python3 - "$DATA_JSON" <<'PY'
import json, sys
d = json.load(open(sys.argv[1], encoding="utf-8"))
md = d.get("months_detail") or []
m0 = md[0] if md else {}
gt = next((p for p in m0.get("properties", []) if p["name"] == "Grand Total"), {})
print(
    m0.get("report_date", "?"),
    gt.get("actual_rns", 0),
    gt.get("budget_rns", 0),
    gt.get("budget_achievement", 0),
    gt.get("occ_actual", 0),
    gt.get("daily_change", 0),
    ",".join(d.get("meta", {}).get("months", [])),
)
PY
)
EOF

echo "  ──────────────────────────────────────────────"
echo "  보고일(months_detail[0]) : $RPT_DATE   (PDF 파일명 날짜: $PDF_DATE)"
echo "  대상 월                  : $MONTHS_CSV"
echo -e "  ${BOLD}Grand Total (1번째 월)${NC}"
echo "    온북 RNs   : $GT_RNS"
echo "    Budget     : $GT_BUD   (달성 ${GT_ACH}%)"
echo "    OCC        : ${GT_OCC}%"
echo "    당일변동   : $GT_CHG"
echo "  ──────────────────────────────────────────────"

if [ "$RPT_DATE" != "$PDF_DATE" ]; then
    warn "PDF 파일명 날짜($PDF_DATE)와 보고일($RPT_DATE) 불일치 — 파일명 또는 PDF 내용 확인 권장(계속 진행)"
fi
[ "${GT_RNS:-0}" != "0" ] || die "Grand Total 온북이 0 — 파싱 비정상(테이블 구조 변경 의심). 빌드/커밋 중단"

# =============================================================================
# [3/6] 멱등 판정 — 커밋된(HEAD) JSON 과 동일하면 빌드/커밋 생략
# =============================================================================
log "[3/6] 변경 여부 확인 (HEAD 대비)..."
NEW_HASH="$(python3 -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$DATA_JSON")"
HEAD_HASH="$(git show "HEAD:$DATA_JSON" 2>/dev/null | python3 -c "import hashlib,sys;print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())" 2>/dev/null || echo "none")"

if [ "$NEW_HASH" = "$HEAD_HASH" ] && [ "$FORCE" -ne 1 ]; then
    ok "오늘 PDF는 이미 리포트에 반영되어 있습니다 (HEAD 와 동일) — 빌드/커밋 생략"
    SKIP_BUILD=1
else
    [ "$FORCE" -eq 1 ] && warn "--force: 변경 없어도 재빌드합니다" || ok "신규/변경된 부킹 데이터 — 빌드 진행"
    SKIP_BUILD=0
fi

# =============================================================================
# [4/6] 빌드 — docs/index.html 에 당일 부킹 섹션 주입
# =============================================================================
if [ "$SKIP_BUILD" -eq 0 ]; then
    log "[4/6] 빌드: build.py (당일 부킹 섹션 주입, ~30초)"
    python3 "$BUILDER" 2>&1 | tail -6 || die "빌드 실패 ($BUILDER)"
    # 주입 검증: 방금 파싱한 온북 수치가 index.html 에 실제로 박혔는지 확인
    GT_RNS_FMT="$(printf "%'d" "$GT_RNS" 2>/dev/null || echo "$GT_RNS")"
    if grep -qF "$GT_RNS_FMT" "$INDEX_HTML" 2>/dev/null || grep -qF "$GT_RNS" "$INDEX_HTML" 2>/dev/null; then
        ok "index.html 에 온북 $GT_RNS 반영 확인"
    else
        warn "index.html 에서 온북 수치($GT_RNS)를 못 찾음 — 표기 포맷 차이일 수 있음(계속)"
    fi
    ok "빌드 완료"
else
    log "[4/6] 빌드 스킵 (변경 없음)"
fi

# =============================================================================
# [5/6] git 커밋 (이 스크립트가 책임지는 파일만 명시 pathspec)
# =============================================================================
log "[5/6] git 커밋..."
cleanup_locks

# 이 스크립트가 소유하는 산출물만 add (build.py 부산물·동시 작업 파일은 건드리지 않음)
COMMIT_PATHS=("$PDF" "$DATA_JSON" "$DOCS_JSON" "$INDEX_HTML")
git add -- "${COMMIT_PATHS[@]}" 2>/dev/null || true

COMMITTED=0
if git diff --cached --quiet; then
    warn "스테이징된 변경 없음 — 커밋 생략"
else
    git diff --cached --stat | sed 's/^/    /'
    git commit -m "chore(daily-booking): ${PDF_DATE} 부킹 리포트 반영 (GT 온북 ${GT_RNS}, 달성 ${GT_ACH}%) [skip ci]" \
        || die "git commit 실패"
    COMMITTED=1
    ok "커밋 완료"
fi

# =============================================================================
# [6/6] pull → push (lock·충돌·동시푸시 자동 처리)
# =============================================================================
# 이 스크립트가 생성/소유하는 파일인지 판정 (충돌 시 --ours 허용 대상)
is_owned_generated() {
    case "$1" in
        "$DATA_JSON"|"$DOCS_JSON"|"$INDEX_HTML") return 0 ;;
        "Daily Booking Report PDF"/*|data/"Daily Booking Report PDF"/*|*"Daily Booking Report_"*.pdf) return 0 ;;
        *) return 1 ;;
    esac
}

# 머지/스태시-pop 충돌을 자동 처리: 소유 생성파일은 --ours, 그 외는 중단
resolve_conflicts_or_die() {
    local unmerged
    unmerged="$(git diff --name-only --diff-filter=U 2>/dev/null)"
    [ -z "$unmerged" ] && return 0
    local f bad=0
    while IFS= read -r f; do
        [ -z "$f" ] && continue
        if is_owned_generated "$f"; then
            git checkout --ours -- "$f" 2>/dev/null && git add -- "$f" 2>/dev/null \
                && echo "    충돌 자동해결(--ours): $f"
        else
            warn "비-소유 파일 충돌: $f (자동해결 안 함)"
            bad=1
        fi
    done <<< "$unmerged"
    [ "$bad" -eq 1 ] && die "수동 해결이 필요한 충돌이 있습니다. 위 파일을 직접 정리 후 재실행하세요."
    # 머지 진행 중이면 마무리 커밋
    [ -f "$REPO/.git/MERGE_HEAD" ] && { git commit --no-edit || die "머지 커밋 실패"; }
    return 0
}

sync_pull() {
    log "    pull --no-rebase --autostash origin main"
    cleanup_locks
    if git pull --no-rebase --autostash origin main; then
        return 0
    fi
    # 실패 → 충돌 처리 시도
    resolve_conflicts_or_die
    # autostash 가 충돌로 남아있을 수 있음 → 정리 시도(부산물은 재생성되므로 안전)
    if git stash list 2>/dev/null | grep -q 'autostash'; then
        git stash drop 2>/dev/null || true
        warn "autostash 잔여 stash 정리함(부산물은 다음 빌드 시 재생성)"
    fi
    return 0
}

if [ "$DO_PUSH" -ne 1 ]; then
    warn "[6/6] --no-push: 푸시 생략 (로컬 커밋만)"
    echo ""
    git log -1 --format='    로컬 HEAD: %h %s' 2>/dev/null
else
    log "[6/6] git pull & push..."
    cleanup_locks

    # 변경이 전혀 없으면(이미 반영 + 백로그 없음) 빠른 종료 판단
    AHEAD="$(git rev-list --count origin/main..HEAD 2>/dev/null || echo 0)"
    if [ "$COMMITTED" -eq 0 ] && [ "${AHEAD:-0}" = "0" ]; then
        ok "이미 origin/main 과 동기화됨 — 푸시할 것 없음"
    else
        sync_pull
        cleanup_locks
        if git push origin main; then
            ok "푸시 완료 — GitHub Pages 배포 시작됨"
        else
            warn "푸시 거부됨(원격이 갱신됨?) — 1회 재시도"
            sync_pull
            cleanup_locks
            if git push origin main; then
                ok "재시도 푸시 완료"
            else
                die "푸시 실패. 네트워크/권한 확인 후 수동 실행:
     cd \"$REPO\"
     rm -f .git/index.lock .git/HEAD.lock
     git pull --no-rebase --autostash origin main && git push origin main"
            fi
        fi
    fi
fi

echo ""
echo -e "${BOLD}${GREEN}========================================================${NC}"
echo -e "${BOLD}${GREEN}  완료: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
echo -e "${BOLD}${GREEN}========================================================${NC}"
echo "  PDF        : $(basename "$PDF")  (보고일 $RPT_DATE)"
echo "  Grand Total: 온북 $GT_RNS / Budget $GT_BUD (달성 ${GT_ACH}%) / OCC ${GT_OCC}% / 당일 $GT_CHG"
[ "$SKIP_BUILD" -eq 1 ] && echo "  (변경 없어 빌드/커밋 생략됨)"
exit 0
