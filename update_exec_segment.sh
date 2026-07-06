#!/usr/bin/env bash
# =============================================================================
# update_exec_segment.sh — "전체 세그 분석 자료" 갱신 (수동/터미널 실행용)
# =============================================================================
# 하는 일:
#   1) 소스 폴더(data/weekly_total)의 월간 리포트 PDF들을 스캔
#      · 확정(마감) 리포트 + 예상(주간) 리포트를 양식 시그니처로 자동 식별
#      · 시장·트랜드 등 비대상 문서는 자동 skip(전체 죽이지 않음)
#   2) scripts/build_exec_segment.py 실행 → data/exec_segment.json (+docs 사본)
#      · 월별 세그·사업장·해외. 확정>예상 우선. 그룹+COMP=합계 교차검증.
#   3) 회귀 가드: 산출 실패/검증경고 시 커밋 거부(과거 자료로 되돌리지 않음)
#   4) 변경 없으면(동일 수치) 커밋 생략(멱등)
#   5) 소유 파일만 커밋 → git-safe push (safe_push, concurrency)
#
# ⚠️ 독립 산출물 원칙:
#   · 전체 build.py 실행 안 함. db_aggregated/otb_data/*.gz 무접촉.
#   · git add 는 이 페이지 소유 파일만(명시적). `git add -A` 금지.
#
# 사용법 (repo 루트에서):
#   ./update_exec_segment.sh                 # 최신 리포트 반영(폴더 스캔)
#   ./update_exec_segment.sh --no-push       # 커밋만
#   ./update_exec_segment.sh --src /경로      # 다른 소스 폴더 지정
#
# 리포트 추가 방법: data/weekly_total/ 에 PDF를 넣고 이 스크립트를 실행.
#   (같은 달의 예상 리포트가 여러 주차면 최신 주차 자동 채택)
# =============================================================================
set -uo pipefail

DO_PUSH=1
SRC_DIR="data/weekly_total"
for arg in "$@"; do
    case "$arg" in
        --no-push) DO_PUSH=0 ;;
        --src) : ;;  # 다음 토큰이 값
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
        /*|./*) SRC_DIR="$arg" ;;
        *) : ;;
    esac
done
# --src VALUE 형태 처리
prev=""
for arg in "$@"; do
    if [ "$prev" = "--src" ]; then SRC_DIR="$arg"; fi
    prev="$arg"
done

if [ -t 1 ]; then
    BOLD='\033[1m'; RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'; BLUE='\033[0;34m'; NC='\033[0m'
else BOLD=''; RED=''; GREEN=''; YELLOW=''; BLUE=''; NC=''; fi
log(){ echo -e "${BLUE}▶${NC} $*"; }
ok(){ echo -e "${GREEN}✅${NC} $*"; }
warn(){ echo -e "${YELLOW}⚠ ${NC} $*"; }
die(){ echo -e "\n${RED}❌ 실패: $*${NC}" >&2; exit 1; }

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" || die "리포지토리 경로 확인 실패"
cd "$REPO" || die "cd 실패"

BUILDER="scripts/build_exec_segment.py"
DATA_JSON="data/exec_segment.json"
DOCS_JSON="docs/data/exec_segment.json"
# 이 페이지가 소유(=커밋 대상)하는 파일. db/otb/gz 등 공용파일은 절대 포함하지 않는다.
OWNED=("$DATA_JSON" "$DOCS_JSON")

command -v python3 >/dev/null 2>&1 || die "python3 없음"
[ -f "$BUILDER" ] || die "$BUILDER 없음"
[ -d "$SRC_DIR" ] || die "소스 폴더 없음: $SRC_DIR"

echo -e "${BOLD}${BLUE}=== 전체 세그 분석 자료 갱신 ===${NC}"
echo "  시작 : $(date '+%Y-%m-%d %H:%M:%S')  ·  소스: $SRC_DIR"

# 데이터 해시(생성시각 제외) — 멱등 판정
data_hash(){ python3 - "$1" <<'PY' 2>/dev/null || echo none
import json,sys,hashlib
try: d=json.load(open(sys.argv[1],encoding="utf-8"))
except Exception: print("none"); sys.exit()
d.get("meta",{}).pop("generated_at",None)
print(hashlib.md5(json.dumps(d,ensure_ascii=False,sort_keys=True).encode()).hexdigest())
PY
}
BEFORE="$(data_hash "$DOCS_JSON")"

# ── 빌드(파싱·검증) ──
OUT="$(python3 "$BUILDER" --src "$SRC_DIR" 2>&1)" || { echo "$OUT"; die "빌더 실행 실패(산출물 미변경)"; }
echo "$OUT" | sed -n '/파일 매핑/,/저장:/p'

# 회귀 가드
echo "$OUT" | grep -q '⚠' && die "교차검증 경고 발생 — 양식/파싱 확인(커밋 거부)"
echo "$OUT" | grep -q '저장:' || die "산출 저장 로그 없음(커밋 거부)"
python3 - "$DOCS_JSON" <<'PY' || die "산출 JSON 구조 이상"
import json,sys
d=json.load(open(sys.argv[1],encoding="utf-8"))
ms=d.get("months",{})
assert ms, "months 비어있음"
for mk,e in ms.items():
    t=(e.get("seg_total") or {}).get("v")
    assert t, f"{mk} 합계 없음"
print(f"  검증 OK · {len(ms)}개월 · {', '.join(sorted(ms))}")
PY

AFTER="$(data_hash "$DOCS_JSON")"
if [ "$BEFORE" = "$AFTER" ]; then
    ok "변경 없음(동일 수치) — 커밋 스킵(멱등)"
    for f in "${OWNED[@]}"; do git checkout -- "$f" 2>/dev/null || true; done
    exit 0
fi

# ── git: 소유 파일만 스테이징(명시적) ──
rm -f .git/index.lock 2>/dev/null || true
STAGED=0
for f in "${OWNED[@]}"; do
    [ -f "$f" ] || continue
    git add -- "$f" || die "git add 실패: $f"
    STAGED=1
done
[ "$STAGED" -eq 1 ] || { ok "스테이징 대상 없음"; exit 0; }

# 안전점검: db/otb/gz가 스테이징에 섞이지 않았는지
if git diff --cached --name-only | grep -Eq 'db_aggregated|otb_data|\.gz$'; then
    die "공용 데이터가 스테이징에 포함됨 — 중단(수동 확인)"
fi
echo "  스테이징:"; git diff --cached --name-only | sed 's/^/    /'

TS="$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M')"
git commit -m "chore(segment-analysis): 전체 세그 분석 자료 월별 갱신 ${TS} KST [skip ci]" || die "commit 실패"
ok "커밋 완료"

[ "$DO_PUSH" -eq 1 ] || { warn "--no-push: push 생략"; exit 0; }

safe_push(){
    local a
    for a in 1 2 3; do
        rm -f .git/index.lock 2>/dev/null || true
        git push origin main && return 0
        warn "push 시도 ${a} 실패 — fetch 후 merge 재시도"
        git fetch origin main || { warn "fetch 실패 — 다음 시도"; continue; }
        if git merge --no-edit origin/main; then
            ok "merge 클린 — 재push"
        else
            # 산출물 충돌: 데이터 파일은 원격(theirs) 우선? 아니오 — 우리 최신 파싱이 정본이므로 ours.
            for f in "${OWNED[@]}"; do git checkout --ours -- "$f" 2>/dev/null || true; git add -- "$f" 2>/dev/null || true; done
            git commit --no-edit 2>/dev/null || true
            ok "산출물 충돌 --ours 자동해소 — 재push"
        fi
    done
    return 1
}
if safe_push; then ok "push 완료 🚀"; else die "push 실패(원격 발산 지속) — 수동 확인"; fi
