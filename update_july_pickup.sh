#!/usr/bin/env bash
# =============================================================================
# update_july_pickup.sh — 7월 동기간 픽업 일자별 관리 리포트 갱신 (수동/매일 실행용)
# =============================================================================
# 하는 일:
#   1) scripts/build_july_pickup_tracker.py 실행
#      → raw_db(온라인영업팀 27/28/43/44) 최신 라이브 스냅샷에서 직접 net 산출
#        (이중계상 회피), 기준일은 스냅샷 파일명에서 자동 추출(날짜 하드코딩 없음)
#      → docs/data/july_pickup.json (대시보드 fetch)
#      → docs/data/july_pickup.xlsx (다운로드용, 소노위크 보고 양식)
#      → ~/Desktop/7월동기간_픽업_일자별관리_YYYYMMDD.xlsx (로컬 사본)
#   2) 회귀 가드: 6개 사업장·온북>0 확인. 스냅샷 기준일이 이미 반영된 것보다
#      과거면 거부(옛 스냅샷으로 되돌리기 방지)
#   3) 멱등: 데이터(json)가 직전 커밋과 동일하면 커밋/푸시 생략(xlsx는 HEAD로 복원)
#   4) 이 스크립트가 책임지는 파일만 명시 pathspec 커밋
#   5) git: lock 정리 → 커밋 → pull(--no-rebase --autostash, 소유=ours/생성물=theirs)
#      → push (거부 시 재시도). push 가 GitHub Actions 빌드·배포(Pages) 트리거.
#
# 사용법 (repo 루트에서):
#   ./update_july_pickup.sh              # 평소: 이것만
#   ./update_july_pickup.sh --force      # 변경 없어도 강제 커밋·푸시
#   ./update_july_pickup.sh --no-push    # 커밋만, 푸시 생략
#
# 멱등성: 같은 스냅샷으로 두 번 돌려도 안전(두 번째는 "이미 반영됨" 스킵).
# =============================================================================

set -uo pipefail

# ── 인자 ─────────────────────────────────────────────────────
FORCE=0; DO_PUSH=1
for arg in "$@"; do
    case "$arg" in
        --force)   FORCE=1 ;;
        --no-push) DO_PUSH=0 ;;
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
        *) echo "알 수 없는 인자: $arg (무시)" ;;
    esac
done

# ── 색상/로그 ────────────────────────────────────────────────
if [ -t 1 ]; then BOLD='\033[1m'; RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'; BLUE='\033[0;34m'; NC='\033[0m'
else BOLD=''; RED=''; GREEN=''; YELLOW=''; BLUE=''; NC=''; fi
log()  { echo -e "${BLUE}▶${NC} $*"; }
ok()   { echo -e "${GREEN}✅${NC} $*"; }
warn() { echo -e "${YELLOW}⚠ ${NC} $*"; }
die()  { echo -e "\n${RED}❌ 실패: $*${NC}\n   위 출력에서 원인 확인 후 재실행하세요." >&2; exit 1; }

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" || die "리포지토리 경로 확인 실패"
cd "$REPO" || die "cd $REPO 실패"

BUILDER="scripts/build_july_pickup_tracker.py"
DOCS_JSON="docs/data/july_pickup.json"
DOCS_XLSX="docs/data/july_pickup.xlsx"

command -v python3 >/dev/null 2>&1 || die "python3 가 없습니다"
[ -f "$BUILDER" ] || die "$BUILDER 없음"

echo -e "${BOLD}${BLUE}========================================================${NC}"
echo -e "${BOLD}${BLUE}  7월 동기간 픽업 일자별 관리 리포트 → 갱신${NC}"
echo -e "${BOLD}${BLUE}========================================================${NC}"
echo "  시작 : $(date '+%Y-%m-%d %H:%M:%S')"
echo "  PWD  : $REPO"
echo "  Py   : $(python3 --version 2>&1)"

cleanup_locks() { rm -f "$REPO/.git/index.lock" "$REPO/.git/HEAD.lock" 2>/dev/null || true; }
cleanup_locks

# =============================================================================
# [1/5] 빌드 — raw_db 직접 net 산출 → json + xlsx + Desktop 사본
# =============================================================================
log "[1/5] 빌드: build_july_pickup_tracker.py (raw_db 파싱, ~10초)"
python3 "$BUILDER" 2>&1 | sed 's/^/    /' || die "빌드 실패 ($BUILDER)"
[ -f "$DOCS_JSON" ] || die "$DOCS_JSON 가 생성되지 않았습니다"
[ -f "$DOCS_XLSX" ] || die "$DOCS_XLSX 가 생성되지 않았습니다"

# =============================================================================
# [2/5] 회귀 가드 + 핵심 수치 추출
# =============================================================================
log "[2/5] 검증: 6개 사업장·온북·갭"
read -r DATA_DATE ASOF26 ASOF25 NPROP V26 V25 GAP NBEHIND <<EOF
$(python3 - "$DOCS_JSON" <<'PY'
import json, sys
d = json.load(open(sys.argv[1], encoding="utf-8"))
m = d["meta"]; s = d["summary"]; FIT = m.get("fit_segments", m["segments"])
def sel(rec): return sum(rec.get(k, 0) for k in FIT)
v26 = sum(sel(x["on26"]) for x in s)
v25 = sum(sel(x["on25"]) for x in s)
behind = sum(1 for x in s if sel(x["on26"]) - sel(x["on25"]) < 0)
print(m["data_date"], m["asof26"], m["asof25"], len(s), v26, v25, v26 - v25, behind)
PY
)
EOF
echo "  ──────────────────────────────────────────────"
echo "  스냅샷 기준일 : $DATA_DATE   (동기간 완전일 $ASOF26 vs 전년 $ASOF25)"
echo "  대상 사업장   : ${NPROP}개"
echo -e "  ${BOLD}6개 합계 동기간 온북${NC} : 26년 ${V26}실 / 25년 ${V25}실 / 갭 ${GAP}실 (전년미달 ${NBEHIND}개)"
echo "  ──────────────────────────────────────────────"

[ "${NPROP:-0}" = "6" ] || die "대상 사업장이 6개가 아닙니다(${NPROP}) — 파싱/매핑 비정상. 중단"
[ "${V26:-0}" != "0" ] || die "26년 동기간 온북이 0 — raw_db 파싱 비정상(스냅샷 누락 의심). 중단"

# 회귀 방지: 스냅샷 기준일이 이미 반영된(HEAD) 것보다 과거면 거부
HEAD_DATE="$(git show "HEAD:$DOCS_JSON" 2>/dev/null | python3 -c "import json,sys
try: print(json.load(sys.stdin)['meta']['data_date'])
except Exception: print('')" 2>/dev/null)"
if [ -n "$HEAD_DATE" ] && [ "$DATA_DATE" \< "$HEAD_DATE" ] && [ "$FORCE" -ne 1 ]; then
    git checkout -- "$DOCS_JSON" "$DOCS_XLSX" 2>/dev/null || true
    die "탐지된 스냅샷($DATA_DATE)이 이미 반영된 기준일($HEAD_DATE)보다 과거입니다.
   최신 27 라이브 스냅샷이 data/raw_db/2026/ 에 없을 수 있습니다.
   옛 수치로 되돌리지 않으려고 중단했습니다. (의도적이면 --force)"
fi

# =============================================================================
# [3/5] 멱등 판정 — 커밋된(HEAD) json 과 동일하면 커밋/푸시 생략
#       (xlsx 는 zip mtime 으로 매번 바이트가 달라지므로 json 해시로 판정)
# =============================================================================
log "[3/5] 변경 여부 확인 (HEAD 대비, json 기준)..."
NEW_HASH="$(python3 -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$DOCS_JSON")"
HEAD_HASH="$(git show "HEAD:$DOCS_JSON" 2>/dev/null | python3 -c "import hashlib,sys;print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())" 2>/dev/null || echo "none")"
if [ "$NEW_HASH" = "$HEAD_HASH" ] && [ "$FORCE" -ne 1 ]; then
    ok "데이터가 직전 커밋과 동일합니다 (스냅샷 $DATA_DATE 이미 반영) — 커밋/푸시 생략"
    # 데이터 동일하면 xlsx 타임스탬프 차이만 남으므로 작업트리 깨끗하게 HEAD 로 복원
    git checkout -- "$DOCS_XLSX" 2>/dev/null || true
    SKIP_COMMIT=1
else
    [ "$FORCE" -eq 1 ] && warn "--force: 변경 없어도 커밋합니다" || ok "신규/변경된 픽업 데이터 — 커밋 진행"
    SKIP_COMMIT=0
fi

# =============================================================================
# 공용: 미해결(unmerged) 정리 — 공유 작업폴더의 동시 git 작업 대비
# =============================================================================
resolve_unmerged_generated() {
    local U; U="$(git diff --name-only --diff-filter=U 2>/dev/null)"
    [ -z "$U" ] && return 0
    if echo "$U" | grep -qE '\.(py|sh|js|ts|mjs|cjs)$'; then
        die "소스코드 충돌(unmerged):
$(echo "$U" | sed 's/^/     /')
   수동 해결(git add/rm) 후 재실행하세요."
    fi
    if [ -f "$REPO/.git/MERGE_HEAD" ] || [ -d "$REPO/.git/rebase-merge" ] || [ -d "$REPO/.git/rebase-apply" ]; then
        die "다른 git 작업(머지/리베이스) 진행 중(공유 작업폴더). 잠시 후 재실행."
    fi
    warn "기존 미해결(unmerged) 생성파일을 HEAD 기준으로 정리:"
    echo "$U" | while IFS= read -r f; do
        [ -z "$f" ] && continue
        git checkout --ours -- "$f" 2>/dev/null || git checkout HEAD -- "$f" 2>/dev/null || git rm -f -- "$f" 2>/dev/null
        git add -- "$f" 2>/dev/null; echo "     정리: $f"
    done
    return 0
}

# =============================================================================
# [4/5] git 커밋 (소유 파일만)
# =============================================================================
COMMITTED=0
if [ "$SKIP_COMMIT" -eq 1 ]; then
    log "[4/5] 커밋 스킵 (변경 없음)"
else
    log "[4/5] git 커밋..."
    cleanup_locks
    resolve_unmerged_generated
    COMMIT_PATHS=("$DOCS_JSON" "$DOCS_XLSX")
    git add -- "${COMMIT_PATHS[@]}" 2>/dev/null || true
    if git diff --cached --quiet -- "${COMMIT_PATHS[@]}"; then
        warn "스테이징된(소유) 변경 없음 — 커밋 생략"
    else
        git diff --cached --stat -- "${COMMIT_PATHS[@]}" | sed 's/^/    /'
        git commit -m "chore(daily-pickup): ${DATA_DATE} 7월 동기간 픽업 갱신 (6개 합계 갭 ${GAP}실, 전년미달 ${NBEHIND}개)" \
            -- "${COMMIT_PATHS[@]}" || die "git commit 실패"
        COMMITTED=1; ok "커밋 완료"
    fi
fi

# =============================================================================
# [5/5] pull → push (lock·충돌·동시푸시 자동 처리). push = Pages 빌드·배포 트리거
# =============================================================================
is_owned() { case "$1" in "$DOCS_JSON"|"$DOCS_XLSX") return 0 ;; *) return 1 ;; esac; }

sync_pull() {
    cleanup_locks
    log "    pull --no-rebase --autostash origin main"
    git pull --no-rebase --autostash origin main >/tmp/_jpk_pull.$$ 2>&1
    local U; U="$(git diff --name-only --diff-filter=U 2>/dev/null)"
    if [ -n "$U" ]; then
        if echo "$U" | grep -qE '\.(py|sh|js|ts|mjs|cjs)$'; then
            cat /tmp/_jpk_pull.$$ 2>/dev/null; rm -f /tmp/_jpk_pull.$$
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
    git stash list 2>/dev/null | grep -q 'autostash' && git stash drop >/dev/null 2>&1
    rm -f /tmp/_jpk_pull.$$
}

if [ "$DO_PUSH" -ne 1 ]; then
    warn "[5/5] --no-push: 푸시 생략 (로컬 커밋만)"
    git log -1 --format='    로컬 HEAD: %h %s' 2>/dev/null
else
    log "[5/5] git pull & push..."
    cleanup_locks
    AHEAD="$(git rev-list --count origin/main..HEAD 2>/dev/null || echo 0)"
    if [ "$COMMITTED" -eq 0 ] && [ "${AHEAD:-0}" = "0" ]; then
        ok "이미 origin/main 과 동기화됨 — 푸시할 것 없음"
    else
        PUSHED=0
        for attempt in 1 2 3 4; do
            sync_pull; cleanup_locks
            if git push origin main >/tmp/_jpk_push.$$ 2>&1; then
                ok "푸시 완료 (attempt $attempt) — GitHub Pages 배포 시작됨"; PUSHED=1; rm -f /tmp/_jpk_push.$$; break
            else
                warn "푸시 거부/실패 (attempt $attempt) — 재시도"; tail -2 /tmp/_jpk_push.$$ 2>/dev/null | sed 's/^/      /'; rm -f /tmp/_jpk_push.$$
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
echo "  스냅샷 기준일 : $DATA_DATE  (동기간 $ASOF26 vs 전년 $ASOF25)"
echo "  6개 합계 갭   : ${GAP}실 (26년 ${V26} / 25년 ${V25}, 전년미달 ${NBEHIND}개)"
echo "  대시보드      : 즐겨찾기 → 데일리 보고 리포트 (엑셀 다운로드 포함)"
[ "${SKIP_COMMIT:-0}" -eq 1 ] && echo "  (변경 없어 커밋/푸시 생략됨)"
exit 0
