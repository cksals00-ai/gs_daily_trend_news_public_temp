#!/usr/bin/env bash
# =============================================================================
# update_exec_segment.sh — 주간 세일즈마케팅 리포트 PDF → "전체 세그 분석 자료" 반영
# =============================================================================
# 하는 일:
#   1) data/weekly report/ 안에서 "가장 최신" 주간회의 PDF 자동 탐지
#      (파일명 날짜 YYYYMMDD 기준, "*주간회의*.pdf")
#   2) scripts/parse_exec_segment.py 로 파싱 → docs/data/exec_segment.json 갱신
#      · 좌표기반 컬럼 바인딩 + 4종 교차검증(세그=사업장 합계 / 소계합=합계 / 권역합=합계)
#   3) 회귀 가드: 교차검증 경고가 있으면 커밋 거부(die)
#   4) 변경 없으면(같은 PDF·같은 수치) 커밋 생략 (멱등)
#   5) 소유 파일(docs/data/exec_segment.json)만 커밋 → git-safe push
#      · push 거부 시 fetch→merge, 생성파일 충돌은 --ours 자동해소 후 재push
#
# 렌더링은 100% 데이터 기반이므로(docs/gs-segment-analysis.html 이 JSON fetch),
# JSON 만 갱신하면 페이지가 자동으로 최신 수치로 표시된다. (HTML 하드코딩 없음)
#
# 사용법 (repo 루트에서):
#   ./update_exec_segment.sh                       # 최신 주간회의 PDF 자동 반영
#   ./update_exec_segment.sh --no-push             # 커밋만
#   ./update_exec_segment.sh "/경로/주간회의.pdf"   # 특정 PDF 명시(공백 OK)
# =============================================================================
set -uo pipefail

DO_PUSH=1
EXPLICIT_PDF=""
for arg in "$@"; do
    case "$arg" in
        --no-push) DO_PUSH=0 ;;
        -h|--help) sed -n '2,26p' "$0"; exit 0 ;;
        *.pdf|*.PDF) EXPLICIT_PDF="$arg" ;;
        *) echo "알 수 없는 인자: $arg (무시)" ;;
    esac
done

if [ -t 1 ]; then
    BOLD='\033[1m'; RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'; BLUE='\033[0;34m'; NC='\033[0m'
else
    BOLD=''; RED=''; GREEN=''; YELLOW=''; BLUE=''; NC=''
fi
log()  { echo -e "${BLUE}▶${NC} $*"; }
ok()   { echo -e "${GREEN}✅${NC} $*"; }
warn() { echo -e "${YELLOW}⚠ ${NC} $*"; }
die()  { echo -e "\n${RED}❌ 실패: $*${NC}" >&2; exit 1; }

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" || die "리포지토리 경로 확인 실패"
cd "$REPO" || die "cd 실패"

PARSER="scripts/parse_exec_segment.py"
DROP_DIR="data/weekly report"
DOCS_JSON="docs/data/exec_segment.json"

command -v python3 >/dev/null 2>&1 || die "python3 없음"
[ -f "$PARSER" ] || die "$PARSER 없음"
[ -d "$DROP_DIR" ] || die "$DROP_DIR 디렉터리 없음"

echo -e "${BOLD}${BLUE}=== 전체 세그 분석 자료 반영 ===${NC}"
echo "  시작 : $(date '+%Y-%m-%d %H:%M:%S')"

# ── 소스 PDF 결정 ─────────────────────────────────────────────
if [ -n "$EXPLICIT_PDF" ]; then
    [ -f "$EXPLICIT_PDF" ] || die "지정 PDF 없음: $EXPLICIT_PDF"
    SRC="$EXPLICIT_PDF"
else
    # 파일명 날짜(YYYYMMDD) 최신 우선 (파서와 동일한 규칙)
    SRC="$(ls -1 "$DROP_DIR"/*주간회의*.pdf 2>/dev/null \
        | while IFS= read -r f; do
              d="$(echo "$f" | grep -oE '20[0-9]{6}' | head -1)"; echo "${d:-00000000}|$f";
          done | sort -r | head -1 | cut -d'|' -f2-)"
    [ -n "$SRC" ] || die "주간회의 PDF를 찾지 못함: $DROP_DIR/*주간회의*.pdf"
fi
log "소스 PDF: $SRC"

# 데이터 해시(생성시각 등 휘발성 meta 제외) — 멱등 판정용
data_hash() {
    python3 - "$1" <<'PY' 2>/dev/null || echo none
import json,sys,hashlib
try:
    d=json.load(open(sys.argv[1],encoding="utf-8"))
except Exception:
    print("none"); sys.exit()
m=d.get("meta",{})
for k in ("generated_at",): m.pop(k,None)
print(hashlib.md5(json.dumps(d,ensure_ascii=False,sort_keys=True).encode()).hexdigest())
PY
}

# ── 파싱 (교차검증 포함) ──────────────────────────────────────
BEFORE_HASH="$(data_hash "$DOCS_JSON")"
OUT="$(python3 "$PARSER" --pdf "$SRC" 2>&1)" || { echo "$OUT"; die "파서 실행 실패"; }
echo "$OUT"

# 회귀 가드: 교차검증 경고 발생 시 거부
if echo "$OUT" | grep -q '⚠'; then
    die "교차검증 경고 발생 — 양식/파싱 확인 필요(커밋 거부)"
fi
echo "$OUT" | grep -q '교차검증 통과' || die "교차검증 통과 로그 없음(커밋 거부)"

# 필수 키 존재 확인
python3 - "$DOCS_JSON" <<'PY' || die "산출 JSON 구조 이상"
import json,sys
d=json.load(open(sys.argv[1],encoding="utf-8"))
assert d.get("months") and len(d["months"])>=2, "months<2"
assert d.get("overseas") and d["overseas"].get("sections"), "overseas 누락"
mk=sorted(d["months"])[0]
st=d["months"][mk]["seg_total"]["rns"]["실적"]
pt=d["months"][mk]["prop_total"]["rns"]["실적"]
assert st==pt, f"세그≠사업장 합계 {st}!={pt}"
print(f"  검증 OK · {mk} 세그=사업장 합계 RNs={st:,}")
PY

AFTER_HASH="$(data_hash "$DOCS_JSON")"
if [ "$BEFORE_HASH" = "$AFTER_HASH" ]; then
    ok "변경 없음(동일 수치) — 커밋 스킵 (멱등)"
    git checkout -- "$DOCS_JSON" 2>/dev/null || true  # 생성시각만 바뀐 재기록 되돌림
    exit 0
fi

# ── git: 소유 파일만 커밋 → git-safe push ─────────────────────
rm -f .git/index.lock 2>/dev/null || true
git add "$DOCS_JSON" || die "git add 실패"
if git diff --cached --quiet; then
    ok "스테이지 변경 없음 — 스킵"; exit 0
fi
TS="$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M')"
git commit -m "chore(segment-analysis): 전체 세그 분석 자료 갱신 ${TS} KST [skip ci]" || die "commit 실패"
ok "커밋 완료"

[ "$DO_PUSH" -eq 1 ] || { warn "--no-push: push 생략"; exit 0; }

safe_push() {
    local attempt
    for attempt in 1 2 3; do
        rm -f .git/index.lock 2>/dev/null || true
        git push origin main && return 0
        warn "push 시도 ${attempt} 실패 — fetch 후 merge 재시도"
        git fetch origin main || { warn "fetch 실패 — 다음 시도"; continue; }
        if git merge --no-edit origin/main; then
            ok "merge 클린 — 재push"
        else
            # 생성 산출물 충돌은 우리 것(--ours)으로 자동해소
            git checkout --ours -- "$DOCS_JSON" 2>/dev/null || true
            git add "$DOCS_JSON" 2>/dev/null || true
            git commit --no-edit 2>/dev/null || true
            ok "산출물 충돌 --ours 자동해소 — 재push"
        fi
    done
    return 1
}
if safe_push; then ok "push 완료 🚀"; else die "push 실패(원격 발산 지속) — 수동 확인 필요"; fi
