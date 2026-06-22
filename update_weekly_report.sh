#!/usr/bin/env bash
# =============================================================================
# update_weekly_report.sh — 세일즈마케팅 주간업무 PDF → 마감보고서 주간탭 반영 (수동 실행용)
# =============================================================================
# 하는 일:
#   1) data/weekly report/ 안에서 "가장 최신" 주간업무 PDF 자동 탐지(mtime 기준, 한글 NFD/NFC 정규화)
#      + 회귀 방지 가드(이미 반영된 주차일자보다 과거 PDF면 거부)
#   2) scripts/build_weekly_business.py 로 파싱(세그먼트+사업장 페이지 자동탐지)
#      → docs/gs-closing-report.html 의 WEEKLY_BIZ 마커 사이 시각화 4종 주입
#   3) 변경이 없으면(같은 PDF 재실행) 커밋 생략 (멱등)
#   4) 소유 파일(docs/gs-closing-report.html)만 명시 pathspec 커밋
#   5) git: lock 정리 → 커밋 → pull(--no-rebase --autostash, 생성물 충돌 자동 해소) → push(재시도)
#
# 사용법 (repo 루트에서 실행 — update_rm_fcst.sh / update_daily_booking.sh 와 동일):
#   cd ~/Projects/gs_daily_trend_news_public_temp
#   ./update_weekly_report.sh                  # 평소: 이것만 — 최신 주간업무 PDF 자동 반영
#   ./update_weekly_report.sh --force          # 같은/과거 PDF라도 강제 재빌드
#   ./update_weekly_report.sh --no-push        # 커밋만, 푸시 생략
#   ./update_weekly_report.sh "data/weekly report/(6월 4주)세일즈마케팅 주간업무 260622.pdf"  # 특정 PDF 지정
#
# 입력 PDF 위치: data/weekly report/  (…주간업무….pdf — 파일명 안 바꿔도 됨)
#   * 이 폴더의 *.pdf 는 .gitignore — 커밋되지 않음(공개 repo 경량화 + 데몬 git clean 삭제 방지).
# 멱등성: 같은 PDF를 두 번 돌려도 안전 (두 번째는 "변경 없음"으로 커밋 스킵).
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

BUILDER="scripts/build_weekly_business.py"
PDF_DIR="data/weekly report"
CLOSING_HTML="docs/gs-closing-report.html"

command -v python3 >/dev/null 2>&1 || die "python3 가 없습니다"
[ -f "$BUILDER" ]      || die "$BUILDER 없음"
[ -f "$CLOSING_HTML" ] || die "$CLOSING_HTML 없음"
[ -d "$PDF_DIR" ]      || die "$PDF_DIR 디렉터리 없음 ('…주간업무….pdf' 를 여기에 두세요)"

echo -e "${BOLD}${BLUE}========================================================${NC}"
echo -e "${BOLD}${BLUE}  세일즈마케팅 주간업무 PDF → 마감보고서 주간탭 반영${NC}"
echo -e "${BOLD}${BLUE}========================================================${NC}"
echo "  시작 : $(date '+%Y-%m-%d %H:%M:%S')"
echo "  PWD  : $REPO"
echo "  Py   : $(python3 --version 2>&1)"

cleanup_locks() { rm -f "$REPO/.git/index.lock" "$REPO/.git/HEAD.lock" 2>/dev/null || true; }
cleanup_locks

# 파일명에서 날짜 추출(YYMMDD/YYYYMMDD → YYYY.MM.DD). NFC 정규화 후 처리.
pdf_date() {
    python3 - "$1" <<'PY'
import sys, re, os, unicodedata
b = unicodedata.normalize("NFC", os.path.basename(sys.argv[1]))
m = re.search(r'(20\d{6})', b) or re.search(r'(?<!\d)(\d{6})(?!\d)', b)
if not m:
    print(""); raise SystemExit
d = m.group(1)
if len(d) == 6: d = "20" + d
print(f"{d[:4]}.{d[4:6]}.{d[6:8]}")
PY
}

# =============================================================================
# [1/5] 최신 주간업무 PDF 탐지 + 회귀 방지 가드
# =============================================================================
log "[1/5] 최신 주간업무 PDF 탐지..."
if [ -n "$EXPLICIT_PDF" ]; then
    [ -f "$EXPLICIT_PDF" ] || die "지정한 PDF가 없습니다: $EXPLICIT_PDF"
    PDF="$EXPLICIT_PDF"
    ok "명시 지정: $(basename "$PDF")"
else
    # macOS 파일명은 NFD 저장 → Python 에서 NFC 정규화 후 '주간업무' 매칭, mtime 최신 1개.
    PDF="$(python3 - "$PDF_DIR" <<'PY'
import sys, os, glob, unicodedata
d = sys.argv[1]
c = [p for p in glob.glob(os.path.join(d, "*.pdf"))
     if "주간업무" in unicodedata.normalize("NFC", os.path.basename(p))]
print(max(c, key=os.path.getmtime) if c else "")
PY
)"
    [ -n "$PDF" ] || die "주간업무 PDF를 찾지 못했습니다 ('$PDF_DIR/' 에 '…주간업무….pdf' 배치 필요)"
    ok "최신 PDF: $(basename "$PDF")"
fi

PDF_DATE="$(pdf_date "$PDF")"
[ -n "$PDF_DATE" ] || warn "PDF 파일명에서 날짜를 읽지 못함 — 회귀가드 건너뜀(계속)"

# 회귀 방지: HEAD 의 closing-report 에 이미 반영된 '기준 날짜'보다 과거 PDF면 거부.
if [ -n "$PDF_DATE" ]; then
    HEAD_DATE="$(git show "HEAD:$CLOSING_HTML" 2>/dev/null | python3 -c '
import sys, re
s = sys.stdin.read()
i, j = s.find("WEEKLY_BIZ_INJECT_START"), s.find("WEEKLY_BIZ_INJECT_END")
b = s[i:j] if 0 <= i < j else ""
m = re.search(r"(\d{4})\.(\d{2})\.(\d{2}) 기준", b)
print(".".join(m.groups()) if m else "")' 2>/dev/null)"
    if [ -n "$HEAD_DATE" ] && [ "$PDF_DATE" \< "$HEAD_DATE" ] && [ "$FORCE" -ne 1 ]; then
        die "탐지된 PDF($PDF_DATE)가 이미 반영된 주간탭 기준일($HEAD_DATE)보다 과거입니다.
   최신 주간업무 PDF가 '$PDF_DIR/' 에 없을 수 있습니다.
   옛 수치로 되돌리지 않으려고 중단했습니다. 최신 PDF를 넣고 재실행하거나,
   의도적으로 과거 PDF를 반영하려면 --force 를 붙이세요."
    fi
fi

# =============================================================================
# [2/5] 빌드 — 파싱 + closing-report.html 주입
# =============================================================================
log "[2/5] 빌드: build_weekly_business.py \"$(basename "$PDF")\""
BUILD_JSON="$(python3 "$BUILDER" "$PDF" 2>/tmp/_wkr_err.$$)" \
    || { cat /tmp/_wkr_err.$$ >&2; rm -f /tmp/_wkr_err.$$; \
         die "빌드 실패 ($BUILDER) — PDF 레이아웃 변경 의심(파서 보정 필요)"; }
rm -f /tmp/_wkr_err.$$

# 결과 요약/검증 (week_label, warnings, 파싱 건수). ensure_ascii=False 로 한글 그대로 출력.
eval "$(python3 - <<PY
import json
d = json.loads(r'''$BUILD_JSON''')
w = d.get("warnings", [])
pu = d.get("pages_used") or {}
pages = "당월 p%s · 누계 p%s" % (pu.get("당월","?"), pu.get("누계","?")) if pu else "?"
def q(s): return json.dumps(str(s), ensure_ascii=False)   # 큰따옴표로 안전 래핑
print("WK_LABEL=%s" % q(d.get("week_label","")))
print("WK_WARN=%s"  % q("; ".join(w)))
print("WK_NPROP=%d" % d.get("n_property", 0))
print("WK_NSEG=%d"  % d.get("n_segment", 0))
print("WK_PAGES=%s" % q(pages))
PY
)"
echo "  ──────────────────────────────────────────────"
echo "  주차 라벨   : ${WK_LABEL}"
echo "  사용 페이지  : ${WK_PAGES}"
echo "  파싱 결과   : 사업장 ${WK_NPROP}/22 · 세그먼트 ${WK_NSEG}/3"
echo "  ──────────────────────────────────────────────"
[ "${WK_NPROP:-0}" -ge 15 ] 2>/dev/null || die "사업장 파싱 ${WK_NPROP}개(예상 22) — 파싱 비정상. 중단"
[ "${WK_NSEG:-0}"  -ge 3  ] 2>/dev/null || die "세그먼트 파싱 ${WK_NSEG}개(예상 3) — 파싱 비정상. 중단"
if [ -n "${WK_WARN}" ]; then
    warn "파서 경고: ${WK_WARN}"
    warn "  → PDF 레이아웃이 일부 바뀌었을 수 있습니다. 주간탭 확인 권장(계속 진행)."
else
    ok "파싱 정상 (warnings 없음)"
fi

# =============================================================================
# [3/5] 멱등 판정 — closing-report.html 에 실제 변경이 있나
# =============================================================================
log "[3/5] 변경 여부 확인 ($CLOSING_HTML)..."
if git diff --quiet -- "$CLOSING_HTML" && [ "$FORCE" -ne 1 ]; then
    ok "이 주간업무 PDF는 이미 주간탭에 반영되어 있습니다 — 커밋 생략"
    SKIP_COMMIT=1
else
    [ "$FORCE" -eq 1 ] && warn "--force: 변경 없어도 진행" || ok "주간탭 변경 감지 — 커밋 진행"
    SKIP_COMMIT=0
fi

# =============================================================================
# 공용: 미해결(unmerged) 인덱스 정리 — 공유 작업폴더의 동시 git 작업 대비
# =============================================================================
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
    warn "기존 미해결(unmerged) 생성파일을 HEAD 기준으로 정리:"
    echo "$U" | while IFS= read -r f; do
        [ -z "$f" ] && continue
        git checkout --ours -- "$f" 2>/dev/null || git checkout HEAD -- "$f" 2>/dev/null || git rm -f -- "$f" 2>/dev/null
        git add -- "$f" 2>/dev/null
        echo "     정리: $f"
    done
    return 0
}

# =============================================================================
# [4/5] git 커밋 (이 스크립트가 책임지는 파일만: closing-report.html)
# =============================================================================
log "[4/5] git 커밋..."
cleanup_locks
resolve_unmerged_generated

COMMITTED=0
if [ "${SKIP_COMMIT:-0}" -eq 1 ]; then
    warn "변경 없음 — 커밋 생략"
else
    git add -- "$CLOSING_HTML" 2>/dev/null || true
    if git diff --cached --quiet -- "$CLOSING_HTML"; then
        warn "스테이징된 변경 없음 — 커밋 생략"
    else
        git diff --cached --stat -- "$CLOSING_HTML" | sed 's/^/    /'
        git commit -m "chore(weekly-business): ${WK_LABEL} 주간업무 시각화 반영 (사업장 ${WK_NPROP} · 세그먼트 ${WK_NSEG}) [skip ci]" \
            -- "$CLOSING_HTML" || die "git commit 실패"
        COMMITTED=1
        ok "커밋 완료"
    fi
fi

# =============================================================================
# [5/5] pull → push (lock·충돌·동시푸시 자동 처리)
# =============================================================================
sync_pull() {
    cleanup_locks
    log "    pull --no-rebase --autostash origin main"
    git pull --no-rebase --autostash origin main >/tmp/_wkr_pull.$$ 2>&1
    # 충돌: 소유파일(closing-report.html)은 --ours(내 최신 주입본), 그 외 생성물은 --theirs(원격·재생성), 소스는 중단
    local U; U="$(git diff --name-only --diff-filter=U 2>/dev/null)"
    if [ -n "$U" ]; then
        if echo "$U" | grep -qE '\.(py|sh|js|ts|mjs|cjs)$'; then
            cat /tmp/_wkr_pull.$$ 2>/dev/null; rm -f /tmp/_wkr_pull.$$
            die "소스코드 충돌로 자동 머지 불가:
$(echo "$U" | sed 's/^/     /')"
        fi
        echo "$U" | while IFS= read -r f; do
            [ -z "$f" ] && continue
            if [ "$f" = "$CLOSING_HTML" ]; then git checkout --ours -- "$f" 2>/dev/null; echo "    충돌→ours: $f"
            else git checkout --theirs -- "$f" 2>/dev/null; echo "    충돌→theirs(재생성): $f"; fi
            git add -- "$f" 2>/dev/null
        done
        [ -f "$REPO/.git/MERGE_HEAD" ] && { git commit --no-edit >/dev/null 2>&1 || true; }
    fi
    git stash list 2>/dev/null | grep -q 'autostash' && git stash drop >/dev/null 2>&1
    rm -f /tmp/_wkr_pull.$$
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
            sync_pull
            cleanup_locks
            if git push origin main >/tmp/_wkr_push.$$ 2>&1; then
                ok "푸시 완료 (attempt $attempt) — GitHub Pages 배포 시작됨"; PUSHED=1; rm -f /tmp/_wkr_push.$$; break
            else
                warn "푸시 거부/실패 (attempt $attempt) — 재시도"; tail -2 /tmp/_wkr_push.$$ 2>/dev/null | sed 's/^/      /'; rm -f /tmp/_wkr_push.$$
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
echo "  PDF       : $(basename "$PDF")"
echo "  주차       : ${WK_LABEL}"
echo "  파싱       : 사업장 ${WK_NPROP}/22 · 세그먼트 ${WK_NSEG}/3"
[ "${SKIP_COMMIT:-0}" -eq 1 ] && echo "  (변경 없어 커밋/푸시 생략됨)"
exit 0
