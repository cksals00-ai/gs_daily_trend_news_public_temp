#!/usr/bin/env bash
# =============================================================================
# update_daily_booking.sh — Daily Booking Report PDF → 리포트 반영 (수동/매일 실행용)
# =============================================================================
# 하는 일:
#   1) data/ 안에서 "가장 최신" Daily Booking Report PDF 자동 탐지 (파일명 날짜 기준)
#      + 회귀 방지 가드(이미 반영된 보고일보다 과거 PDF면 거부)
#   2) scripts/parse_daily_booking.py 로 파싱 → data/daily_booking.json,
#      docs/data/daily_booking.json 갱신
#   3) 변경이 없으면(같은 PDF 재실행) 빌드·커밋 생략 (멱등)
#   4) 변경이 있으면 scripts/build.py 로 docs/index.html 에 당일 부킹 섹션 주입
#   5) 핵심 수치(Grand Total) 출력해 검증 + 이 스크립트가 소유한 파일만 커밋
#   6) git: lock 정리 → 커밋 → pull(--no-rebase --autostash, 생성파일 충돌 자동 --ours)
#      → push (push 거부 시 재시도)
#
# 사용법 (daily_update.sh 와 같은 방식 — repo 루트에서 실행):
#   cd ~/Desktop/gs_daily_trend_news_public_temp     # (= ~/Projects/... 심볼릭)
#   ./update_daily_booking.sh                        # 평소: 이것만 치면 됨
#   ./update_daily_booking.sh --force                # 같은/과거 PDF라도 강제 재빌드
#   ./update_daily_booking.sh --no-push              # 커밋만, 푸시 생략
#   ./update_daily_booking.sh /경로/특정.pdf         # 특정 PDF 명시 지정
#
# 입력 PDF 위치: data/Daily Booking Report PDF/Daily Booking Report_YYYY.MM.DD.pdf
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
        -h|--help) sed -n '2,33p' "$0"; exit 0 ;;
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
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" || die "리포지토리 경로 확인 실패"
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

cleanup_locks() { rm -f "$REPO/.git/index.lock" "$REPO/.git/HEAD.lock" 2>/dev/null || true; }
cleanup_locks

# =============================================================================
# [1/6] 최신 Daily Booking Report PDF 탐지 + 회귀 방지 가드
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
              [ "$d" != "$(basename "$f")" ] && printf '%s\t%s\n' "$d" "$f"
          done \
        | sort -u | tail -1 | cut -f2-
    )"
    [ -n "$PDF" ] || die "Daily Booking Report PDF를 찾지 못했습니다 ('data/Daily Booking Report PDF/' 에 'Daily Booking Report_YYYY.MM.DD.pdf' 배치 필요)"
    ok "최신 PDF: $(basename "$PDF")"
fi
PDF_DATE="$(basename "$PDF" | sed -E 's/^Daily Booking Report_([0-9]{4}\.[0-9]{2}\.[0-9]{2})\.pdf$/\1/' | tr '.' '-')"

# 회귀 방지: 이미 반영된(HEAD) 보고일보다 과거 PDF면 거부(오늘 PDF 누락 시 옛수치 복귀 방지)
HEAD_RPT="$(git show "HEAD:$DATA_JSON" 2>/dev/null | python3 -c "import json,sys
try:
    d=json.load(sys.stdin); md=d.get('months_detail') or [{}]; print(md[0].get('report_date','') or '')
except Exception: print('')" 2>/dev/null)"
if [ -n "$HEAD_RPT" ] && [ -n "$PDF_DATE" ] && [ "$PDF_DATE" \< "$HEAD_RPT" ] && [ "$FORCE" -ne 1 ]; then
    die "탐지된 PDF($PDF_DATE)가 이미 반영된 보고일($HEAD_RPT)보다 과거입니다.
   오늘자 PDF가 'data/Daily Booking Report PDF/' 에 없을 수 있습니다(파일명: Daily Booking Report_YYYY.MM.DD.pdf).
   옛 수치로 되돌리지 않으려고 중단했습니다. 오늘 PDF를 넣고 재실행하거나,
   의도적으로 과거 PDF를 반영하려면 --force 를 붙이세요."
fi

# =============================================================================
# [2/6] 파싱 → daily_booking.json 갱신
# =============================================================================
log "[2/6] 파싱: parse_daily_booking.py"
python3 "$PARSER" "$PDF" || die "파싱 실패 ($PARSER)"
[ -f "$DATA_JSON" ] || die "$DATA_JSON 가 생성되지 않았습니다"

read -r RPT_DATE GT_RNS GT_BUD GT_ACH GT_OCC GT_CHG MONTHS_CSV <<EOF
$(python3 - "$DATA_JSON" <<'PY'
import json, sys
d = json.load(open(sys.argv[1], encoding="utf-8"))
md = d.get("months_detail") or []
m0 = md[0] if md else {}
gt = next((p for p in m0.get("properties", []) if p["name"] == "Grand Total"), {})
print(m0.get("report_date","?"), gt.get("actual_rns",0), gt.get("budget_rns",0),
      gt.get("budget_achievement",0), gt.get("occ_actual",0), gt.get("daily_change",0),
      ",".join(d.get("meta",{}).get("months",[])))
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

[ "$RPT_DATE" = "$PDF_DATE" ] || warn "PDF 파일명 날짜($PDF_DATE)와 보고일($RPT_DATE) 불일치 — 확인 권장(계속)"
[ "${GT_RNS:-0}" != "0" ] || die "Grand Total 온북이 0 — 파싱 비정상(테이블 구조 변경 의심). 중단"

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
    # 주입 검증: 콤마 포맷 포함해 온북 수치가 index.html 에 박혔는지 확인
    GT_FMT="$(python3 -c "print(f'{int(\"$GT_RNS\"):,}')" 2>/dev/null || echo "$GT_RNS")"
    if grep -qF "$GT_FMT" "$INDEX_HTML" 2>/dev/null || grep -qF "$GT_RNS" "$INDEX_HTML" 2>/dev/null; then
        ok "index.html 에 온북 $GT_FMT 반영 확인"
    else
        warn "index.html 에서 온북 수치($GT_FMT)를 못 찾음 — 표기 포맷 차이일 수 있음(계속)"
    fi
    ok "빌드 완료"
else
    log "[4/6] 빌드 스킵 (변경 없음)"
fi

# =============================================================================
# 공용: 미해결(unmerged) 인덱스 정리 — 공유 작업폴더의 동시 git 작업 대비
# =============================================================================
# 규칙: 소스코드(.py/.sh/.js/.ts/.mjs/.cjs) 충돌이면 절대 자동진행 안 함(중단).
#       머지/리베이스 진행 중이면 다른 작업이 살아있는 것 → 잠시 후 재실행 안내.
#       그 외(생성물이 멈춘 채 unmerged)면 HEAD(--ours) 기준 정리(생성물은 재생성됨).
resolve_unmerged_generated() {
    local U; U="$(git diff --name-only --diff-filter=U 2>/dev/null)"
    [ -z "$U" ] && return 0
    if echo "$U" | grep -qE '\.(py|sh|js|ts|mjs|cjs)$'; then
        die "소스코드 충돌(unmerged)이 있어 자동 진행 불가:
$(echo "$U" | sed 's/^/     /')
   수동 해결(git add/rm) 후 재실행하세요."
    fi
    if [ -f "$REPO/.git/MERGE_HEAD" ] || [ -d "$REPO/.git/rebase-merge" ] || [ -d "$REPO/.git/rebase-apply" ]; then
        die "다른 git 작업(머지/리베이스)이 진행 중입니다(공유 작업폴더). 잠시 후 재실행하세요.
$(echo "$U" | sed 's/^/     /')"
    fi
    warn "기존 미해결(unmerged) 생성파일을 HEAD 기준으로 정리(생성물은 빌드시 재생성):"
    echo "$U" | while IFS= read -r f; do
        [ -z "$f" ] && continue
        git checkout --ours -- "$f" 2>/dev/null || git checkout HEAD -- "$f" 2>/dev/null || git rm -f -- "$f" 2>/dev/null
        git add -- "$f" 2>/dev/null
        echo "     정리: $f"
    done
    return 0
}

# =============================================================================
# [5/6] git 커밋 (이 스크립트가 책임지는 파일만 명시 pathspec)
# =============================================================================
log "[5/6] git 커밋..."
cleanup_locks
resolve_unmerged_generated   # 동시 작업이 남긴 unmerged 로 commit 이 막히지 않도록 선정리

COMMIT_PATHS=("$PDF" "$DATA_JSON" "$DOCS_JSON" "$INDEX_HTML")
git add -- "${COMMIT_PATHS[@]}" 2>/dev/null || true

COMMITTED=0
if git diff --cached --quiet -- "${COMMIT_PATHS[@]}"; then
    warn "스테이징된(소유) 변경 없음 — 커밋 생략"
else
    git diff --cached --stat -- "${COMMIT_PATHS[@]}" | sed 's/^/    /'
    git commit -m "chore(daily-booking): ${PDF_DATE} 부킹 리포트 반영 (GT 온북 ${GT_RNS}, 달성 ${GT_ACH}%)" \
        -- "${COMMIT_PATHS[@]}" || die "git commit 실패"
    COMMITTED=1
    ok "커밋 완료"
fi

# =============================================================================
# [6/6] pull → push (lock·충돌·동시푸시 자동 처리)
# =============================================================================
is_owned() {
    case "$1" in
        "$DATA_JSON"|"$DOCS_JSON"|"$INDEX_HTML") return 0 ;;
        *"Daily Booking Report_"*.pdf) return 0 ;;
        *) return 1 ;;
    esac
}

sync_pull() {
    cleanup_locks
    log "    pull --no-rebase --autostash origin main"
    git pull --no-rebase --autostash origin main >/tmp/_dbk_pull.$$ 2>&1
    # 충돌 처리: 소유파일은 --ours(내 최신), 그 외 생성물은 --theirs(원격) → 재생성됨, 소스는 중단
    local U; U="$(git diff --name-only --diff-filter=U 2>/dev/null)"
    if [ -n "$U" ]; then
        if echo "$U" | grep -qE '\.(py|sh|js|ts|mjs|cjs)$'; then
            cat /tmp/_dbk_pull.$$ 2>/dev/null; rm -f /tmp/_dbk_pull.$$
            die "소스코드 충돌로 자동 머지 불가:
$(echo "$U" | sed 's/^/     /')"
        fi
        echo "$U" | while IFS= read -r f; do
            [ -z "$f" ] && continue
            if is_owned "$f"; then git checkout --ours -- "$f" 2>/dev/null; echo "    충돌→ours: $f"
            else git checkout --theirs -- "$f" 2>/dev/null; echo "    충돌→theirs(재생성): $f"; fi
            git add -- "$f" 2>/dev/null
        done
        [ -f "$REPO/.git/MERGE_HEAD" ] && { git commit --no-edit >/dev/null 2>&1 || true; }
    fi
    # autostash 가 충돌로 남았으면 정리(생성물 부산물 → 다음 빌드시 재생성)
    git stash list 2>/dev/null | grep -q 'autostash' && git stash drop >/dev/null 2>&1
    rm -f /tmp/_dbk_pull.$$
}

if [ "$DO_PUSH" -ne 1 ]; then
    warn "[6/6] --no-push: 푸시 생략 (로컬 커밋만)"
    git log -1 --format='    로컬 HEAD: %h %s' 2>/dev/null
else
    log "[6/6] git pull & push..."
    cleanup_locks
    AHEAD="$(git rev-list --count origin/main..HEAD 2>/dev/null || echo 0)"
    if [ "$COMMITTED" -eq 0 ] && [ "${AHEAD:-0}" = "0" ]; then
        ok "이미 origin/main 과 동기화됨 — 푸시할 것 없음"
    else
        PUSHED=0
        for attempt in 1 2 3 4; do
            sync_pull
            cleanup_locks
            if git push origin main >/tmp/_dbk_push.$$ 2>&1; then
                ok "푸시 완료 (attempt $attempt) — GitHub Pages 배포 시작됨"; PUSHED=1; rm -f /tmp/_dbk_push.$$; break
            else
                warn "푸시 거부/실패 (attempt $attempt) — 재시도"; tail -2 /tmp/_dbk_push.$$ 2>/dev/null | sed 's/^/      /'; rm -f /tmp/_dbk_push.$$
            fi
        done
        [ "$PUSHED" -eq 1 ] || die "푸시 실패(반복). 네트워크/권한 확인 후 수동:
     cd \"$REPO\"
     rm -f .git/index.lock .git/HEAD.lock
     git pull --no-rebase --autostash origin main && git push origin main"
    fi
fi

echo ""
echo -e "${BOLD}${GREEN}========================================================${NC}"
echo -e "${BOLD}${GREEN}  완료: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
echo -e "${BOLD}${GREEN}========================================================${NC}"
echo "  PDF        : $(basename "$PDF")  (보고일 $RPT_DATE)"
echo "  Grand Total: 온북 $GT_RNS / Budget $GT_BUD (달성 ${GT_ACH}%) / OCC ${GT_OCC}% / 당일 $GT_CHG"
[ "${SKIP_BUILD:-0}" -eq 1 ] && echo "  (변경 없어 빌드/커밋 생략됨)"
exit 0
