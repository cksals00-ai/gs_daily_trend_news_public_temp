#!/usr/bin/env bash
# =============================================================================
# update_rm_fcst.sh — Revenue Meeting PDF → RM FCST 리포트 반영 (수동/매일 실행용)
# =============================================================================
# 하는 일:
#   1) data/RM자료/ 안에서 "가장 최신" Revenue Meeting PDF 자동 탐지
#      (파일명 날짜 YYYY.MM.DD 기준, 공백형 "Revenue Meeting_" · 언더스코어형 "Revenue_Meeting_" 모두)
#      + 회귀 방지 가드(이미 반영된 스냅샷일자보다 과거 PDF면 거부)
#   2) scripts/parse_rm_fcst.py 로 파싱 → data/rm_fcst.json, docs/data/rm_fcst.json 갱신(스냅샷 누적)
#   3) 변경이 없으면(같은 PDF·같은 수치 재실행) 빌드·커밋 생략 (멱등)
#   4) 변경이 있으면 scripts/build.py 실행
#      → RM_FCST_정리.xlsx · 소사업_예상매출.xlsx · 세일즈마케팅 PPT 재생성(build_rm_fcst_excel 자동호출)
#      → docs/index.html 에 RM FCST 숫자 반영
#   5) 핵심 수치(6/7/8월 RM FCST 합계·세그먼트) 출력해 검증 + 소유 파일만 커밋
#   6) git: lock 정리 → 커밋 → pull(--no-rebase --autostash, 생성파일 충돌 자동 --ours)
#      → push (push 거부 시 재시도)
#
# 사용법 (repo 루트에서 실행 — update_daily_booking.sh 와 동일한 방식):
#   cd ~/Projects/gs_daily_trend_news_public_temp
#   ./update_rm_fcst.sh                  # 평소: 이것만 치면 됨 (최신 RM PDF 자동 반영)
#   ./update_rm_fcst.sh --force          # 같은/과거 PDF라도 강제 재빌드
#   ./update_rm_fcst.sh --no-push        # 커밋만, 푸시 생략
#   ./update_rm_fcst.sh "/경로/Revenue Meeting_YYYY.MM.DD.pdf"   # 특정 PDF 명시 지정(공백 OK)
#
# 입력 PDF 위치: data/RM자료/Revenue Meeting_YYYY.MM.DD.pdf
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
        -h|--help) sed -n '2,37p' "$0"; exit 0 ;;
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

PARSER="scripts/parse_rm_fcst.py"
BUILDER="scripts/build.py"
PDF_DIR="data/RM자료"
DATA_JSON="data/rm_fcst.json"
DOCS_JSON="docs/data/rm_fcst.json"
INDEX_HTML="docs/index.html"
OTB_HTML="docs/otb.html"   # RM FCST YoY 사업장 테이블이 함께 주입되는 페이지
# build.py 의 build_rm_fcst_excel 자동호출이 재생성하는, 이 스크립트가 책임지는 산출물
GEN_OUTPUTS=(
    "data/RM_FCST_정리.xlsx"        "docs/data/RM_FCST_정리.xlsx"
    "data/소사업_예상매출.xlsx"      "docs/data/소사업_예상매출.xlsx"
    "data/세일즈마케팅_예상매출현황.pptx" "docs/data/세일즈마케팅_예상매출현황.pptx"
)

command -v python3 >/dev/null 2>&1 || die "python3 가 없습니다"
[ -f "$PARSER" ]  || die "$PARSER 없음"
[ -f "$BUILDER" ] || die "$BUILDER 없음"
[ -d "$PDF_DIR" ] || die "$PDF_DIR 디렉터리 없음"

echo -e "${BOLD}${BLUE}========================================================${NC}"
echo -e "${BOLD}${BLUE}  Revenue Meeting PDF → RM FCST 반영${NC}"
echo -e "${BOLD}${BLUE}========================================================${NC}"
echo "  시작 : $(date '+%Y-%m-%d %H:%M:%S')"
echo "  PWD  : $REPO"
echo "  Py   : $(python3 --version 2>&1)"

cleanup_locks() { rm -f "$REPO/.git/index.lock" "$REPO/.git/HEAD.lock" 2>/dev/null || true; }
cleanup_locks

# rm_fcst.json 에서 안정적 시그니처(휘발성 _extracted_at 제외) 추출:
#   "<source_pdf>|<snapshot_date>|YM:grand_rn:grand_rev,..." 형태.
# stdin 으로 json 을 받아 한 줄 출력. 파싱 실패 시 빈 줄.
# (heredoc 가 아니라 python3 -c 사용 — heredoc 는 stdin 을 가로채 파이프 입력을 못 읽음)
rm_signature() {
    python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    print(""); sys.exit()
props = d.get("properties", {})
months = sorted({ym for p in props.values() for ym in p})
parts = []
for ym in months:
    rn  = sum(p[ym].get("rm_fcst_rn", 0)      for p in props.values() if ym in p)
    rev = sum(p[ym].get("rm_fcst_rev_mil", 0) for p in props.values() if ym in p)
    parts.append("%s:%d:%d" % (ym, rn, rev))
print("%s|%s|%s" % (d.get("_source_pdf",""), d.get("_snapshot_date",""), ",".join(parts)))
'
}

# =============================================================================
# [1/6] 최신 Revenue Meeting PDF 탐지 + 회귀 방지 가드
# =============================================================================
log "[1/6] 최신 Revenue Meeting PDF 탐지..."
if [ -n "$EXPLICIT_PDF" ]; then
    [ -f "$EXPLICIT_PDF" ] || die "지정한 PDF가 없습니다: $EXPLICIT_PDF"
    PDF="$EXPLICIT_PDF"
    ok "명시 지정: $(basename "$PDF")"
else
    # 공백형/언더스코어형 모두 수집 → 파일명 날짜(YYYY.MM.DD)로 정렬 → 최신 1개.
    PDF="$(
        find "$REPO/$PDF_DIR" -maxdepth 1 -type f -iname "Revenue*Meeting_*.pdf" 2>/dev/null \
        | while IFS= read -r f; do
              d=$(basename "$f" | sed -nE 's/.*([0-9]{4}\.[0-9]{2}\.[0-9]{2}).*/\1/p')
              [ -n "$d" ] && printf '%s\t%s\n' "$d" "$f"
          done \
        | sort -u | tail -1 | cut -f2-
    )"
    [ -n "$PDF" ] || die "Revenue Meeting PDF를 찾지 못했습니다 ('$PDF_DIR/' 에 'Revenue Meeting_YYYY.MM.DD.pdf' 배치 필요)"
    ok "최신 PDF: $(basename "$PDF")"
fi
PDF_DATE="$(basename "$PDF" | sed -nE 's/.*([0-9]{4})\.([0-9]{2})\.([0-9]{2}).*/\1.\2.\3/p')"
[ -n "$PDF_DATE" ] || die "PDF 파일명에서 날짜(YYYY.MM.DD)를 읽지 못했습니다: $(basename "$PDF")"

# 회귀 방지: HEAD 에 반영된 스냅샷일자보다 과거 PDF면 거부(오늘 PDF 누락 시 옛수치 복귀 방지)
HEAD_SNAP="$(git show "HEAD:$DATA_JSON" 2>/dev/null | python3 -c "import json,sys
try: print(json.load(sys.stdin).get('_snapshot_date','') or '')
except Exception: print('')" 2>/dev/null)"
if [ -n "$HEAD_SNAP" ] && [ "$PDF_DATE" \< "$HEAD_SNAP" ] && [ "$FORCE" -ne 1 ]; then
    die "탐지된 PDF($PDF_DATE)가 이미 반영된 스냅샷일자($HEAD_SNAP)보다 과거입니다.
   오늘자 Revenue Meeting PDF가 '$PDF_DIR/' 에 없을 수 있습니다.
   옛 수치로 되돌리지 않으려고 중단했습니다. 오늘 PDF를 넣고 재실행하거나,
   의도적으로 과거 PDF를 반영하려면 --force 를 붙이세요."
fi

# =============================================================================
# [2/6] 파싱 → rm_fcst.json 갱신
# =============================================================================
log "[2/6] 파싱: parse_rm_fcst.py \"$(basename "$PDF")\""
python3 "$PARSER" "$PDF" || die "파싱 실패 ($PARSER) — unmapped 사업장/테이블 구조 변경 의심"
[ -f "$DATA_JSON" ] || die "$DATA_JSON 가 생성되지 않았습니다"

# [2/6b] 홈페이지 세그(주요지표+픽업) 파싱 → docs/data/homepage_rm.json (GA 대시보드용)
log "[2/6b] 홈페이지 세그 파싱: parse_homepage_rm.py"
python3 scripts/parse_homepage_rm.py "$PDF" || echo "  ⚠ 홈페이지 RM 파싱 실패 — 기존 homepage_rm.json 유지 (무시하고 계속)"

# 핵심 수치 출력 + 검증 (소스 PDF 명에 공백이 있으므로 탭 구분으로 안전하게 읽음)
IFS=$'\t' read -r SRC_PDF SNAP_DATE RN6 REV6 RN7 REV7 RN8 REV8 SEGCSV <<EOF
$(python3 - "$DATA_JSON" <<'PY'
import json, sys
d = json.load(open(sys.argv[1], encoding="utf-8"))
props = d.get("properties", {})
def tot(ym, key):
    return sum(p[ym].get(key, 0) for p in props.values() if ym in p)
def seg(ym, s, key):
    return sum(p[ym].get("segments", {}).get(s, {}).get(key, 0) for p in props.values() if ym in p)
months = d.get("_months_covered", [])
# 최신 스냅샷이 다루는 3개월(보통 당월~+2): 첫 3개
m = (months + ["", "", ""])[:3]
def g(ym):
    return (tot(ym, "rm_fcst_rn") if ym else 0, tot(ym, "rm_fcst_rev_mil") if ym else 0)
(rn6, rev6), (rn7, rev7), (rn8, rev8) = g(m[0]), g(m[1]), g(m[2])
# 세그먼트 합(첫 월): OTA/G-OTA/Inbound RN
segs = "/".join(f"{s}={seg(m[0], s, 'rm_fcst_rn')}" for s in ("OTA", "G-OTA", "Inbound"))
print("\t".join(str(x) for x in (
    d.get("_source_pdf", "?"), d.get("_snapshot_date", "?"),
    rn6, rev6, rn7, rev7, rn8, rev8, segs)))
PY
)
EOF

MONTHS_CSV="$(python3 -c "import json;print(', '.join(json.load(open('$DATA_JSON',encoding='utf-8')).get('_months_covered',[])))")"
echo "  ──────────────────────────────────────────────"
echo "  소스 PDF   : $SRC_PDF   (스냅샷일자: $SNAP_DATE / 파일명 날짜: $PDF_DATE)"
echo "  대상 월     : $MONTHS_CSV"
echo -e "  ${BOLD}RM FCST 합계 (Grand Total)${NC}"
printf "    %s월  : RN %s실 / 매출 %s백만원\n" "$(echo "$MONTHS_CSV" | cut -d, -f1 | tr -d ' ')" "$RN6" "$REV6"
printf "    %s월  : RN %s실 / 매출 %s백만원\n" "$(echo "$MONTHS_CSV" | cut -d, -f2 | tr -d ' ')" "$RN7" "$REV7"
printf "    %s월  : RN %s실 / 매출 %s백만원\n" "$(echo "$MONTHS_CSV" | cut -d, -f3 | tr -d ' ')" "$RN8" "$REV8"
echo "    세그먼트(첫 월 RN): $SEGCSV"
echo "  ──────────────────────────────────────────────"

# 파싱 정상성 검증: 스냅샷일자=PDF날짜, 첫 월 RN>0
[ "$SNAP_DATE" = "$PDF_DATE" ] || warn "스냅샷일자($SNAP_DATE)와 PDF 파일명 날짜($PDF_DATE) 불일치 — 확인 권장(계속)"
[ "${RN6:-0}" -gt 0 ] 2>/dev/null || die "첫 월 RM FCST RN 이 0/비정상 — 파싱 실패 의심(테이블 구조 변경?). 중단"

# =============================================================================
# [3/6] 멱등 판정 — 커밋된(HEAD) JSON 과 (휘발 타임스탬프 제외) 내용 동일하면 빌드/커밋 생략
# =============================================================================
log "[3/6] 변경 여부 확인 (HEAD 대비, _extracted_at 제외)..."
NEW_SIG="$(rm_signature < "$DATA_JSON")"
HEAD_SIG="$(git show "HEAD:$DATA_JSON" 2>/dev/null | rm_signature 2>/dev/null || echo "")"
if [ -n "$NEW_SIG" ] && [ "$NEW_SIG" = "$HEAD_SIG" ] && [ "$FORCE" -ne 1 ]; then
    ok "이 RM 스냅샷은 이미 리포트에 반영되어 있습니다 (HEAD 와 동일 수치) — 빌드/커밋 생략"
    # JSON 의 _extracted_at 만 바뀐 working tree 변경은 되돌려 깨끗하게 유지
    git checkout -- "$DATA_JSON" "$DOCS_JSON" 2>/dev/null || true
    SKIP_BUILD=1
else
    [ "$FORCE" -eq 1 ] && warn "--force: 변경 없어도 재빌드합니다" || ok "신규/변경된 RM FCST 데이터 — 빌드 진행"
    SKIP_BUILD=0
fi

# =============================================================================
# [4/6] 빌드 — RM_FCST_정리.xlsx·소사업·PPT 재생성 + index.html 반영
# =============================================================================
if [ "$SKIP_BUILD" -eq 0 ]; then
    log "[4/6] 빌드: build.py (build_rm_fcst_excel 자동호출 포함, ~40초)"
    python3 "$BUILDER" 2>&1 | grep -iE 'build_rm_fcst|RM FCST|소사업|rm_fcst.json|index.html 빌드|error|fail|✗' | tail -10
    [ "${PIPESTATUS[0]}" -eq 0 ] || die "빌드 실패 ($BUILDER)"
    # 산출물 검증: RM 엑셀이 갱신됐는지(소스 PDF 가 메타에 박혔는지) 확인
    if python3 - <<PY 2>/dev/null
import openpyxl, sys
wb = openpyxl.load_workbook("data/RM_FCST_정리.xlsx")
ws = wb["메타정보"] if "메타정보" in wb.sheetnames else wb[wb.sheetnames[-1]]
hit = any("$SNAP_DATE" in str(c) or "$SRC_PDF" in str(c)
          for row in ws.iter_rows(values_only=True) for c in row if c)
sys.exit(0 if hit else 1)
PY
    then ok "RM_FCST_정리.xlsx 에 스냅샷 $SNAP_DATE 반영 확인"
    else warn "RM_FCST_정리.xlsx 메타에서 스냅샷 미확인 — 표기 차이일 수 있음(계속)"; fi
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

COMMIT_PATHS=("$PDF" "$DATA_JSON" "$DOCS_JSON" "$INDEX_HTML" "$OTB_HTML" "${GEN_OUTPUTS[@]}")
git add -- "${COMMIT_PATHS[@]}" 2>/dev/null || true

COMMITTED=0
if git diff --cached --quiet -- "${COMMIT_PATHS[@]}"; then
    warn "스테이징된(소유) 변경 없음 — 커밋 생략"
else
    git diff --cached --stat -- "${COMMIT_PATHS[@]}" | sed 's/^/    /'
    git commit -m "chore(rm-fcst): ${SNAP_DATE} RM FCST 반영 (${MONTHS_CSV} / 첫월 온북 ${RN6}실 ${REV6}백만원) [skip ci]" \
        -- "${COMMIT_PATHS[@]}" || die "git commit 실패"
    COMMITTED=1
    ok "커밋 완료"
fi

# =============================================================================
# [6/6] pull → push (lock·충돌·동시푸시 자동 처리)
# =============================================================================
is_owned() {
    case "$1" in
        "$DATA_JSON"|"$DOCS_JSON"|"$INDEX_HTML"|"$OTB_HTML") return 0 ;;
        "data/RM_FCST_정리.xlsx"|"docs/data/RM_FCST_정리.xlsx") return 0 ;;
        "data/소사업_예상매출.xlsx"|"docs/data/소사업_예상매출.xlsx") return 0 ;;
        "data/세일즈마케팅_예상매출현황.pptx"|"docs/data/세일즈마케팅_예상매출현황.pptx") return 0 ;;
        *Revenue*Meeting_*.pdf) return 0 ;;
        *) return 1 ;;
    esac
}

sync_pull() {
    cleanup_locks
    log "    pull --no-rebase --autostash origin main"
    git pull --no-rebase --autostash origin main >/tmp/_rmf_pull.$$ 2>&1
    # 충돌 처리: 소유파일은 --ours(내 최신), 그 외 생성물은 --theirs(원격) → 재생성됨, 소스는 중단
    local U; U="$(git diff --name-only --diff-filter=U 2>/dev/null)"
    if [ -n "$U" ]; then
        if echo "$U" | grep -qE '\.(py|sh|js|ts|mjs|cjs)$'; then
            cat /tmp/_rmf_pull.$$ 2>/dev/null; rm -f /tmp/_rmf_pull.$$
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
    rm -f /tmp/_rmf_pull.$$
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
            if git push origin main >/tmp/_rmf_push.$$ 2>&1; then
                ok "푸시 완료 (attempt $attempt) — GitHub Pages 배포 시작됨"; PUSHED=1; rm -f /tmp/_rmf_push.$$; break
            else
                warn "푸시 거부/실패 (attempt $attempt) — 재시도"; tail -2 /tmp/_rmf_push.$$ 2>/dev/null | sed 's/^/      /'; rm -f /tmp/_rmf_push.$$
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
echo "  PDF       : $(basename "$PDF")  (스냅샷 $SNAP_DATE)"
echo "  대상 월    : $MONTHS_CSV"
echo "  RM FCST   : 첫월 온북 ${RN6}실 / ${REV6}백만원 · 세그먼트 ${SEGCSV}"
[ "${SKIP_BUILD:-0}" -eq 1 ] && echo "  (변경 없어 빌드/커밋 생략됨)"
exit 0
