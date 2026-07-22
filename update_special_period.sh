#!/usr/bin/env bash
# =============================================================================
# update_special_period.sh — 스페셜(최성수기)/연휴 전략상품 실적 팔로업 갱신
#                            (수동/터미널 실행용)
# =============================================================================
# 하는 일:
#   1) scripts/build_special_period.py 실행
#      · 원천 data/raw_db(2026 현재 온북 · 2025 확정실적)만 read-only 파싱
#      · 대상 기간 투숙일자별 × 사업장/세그먼트/채널 → data/special_period.json (+docs 사본)
#      · db_aggregated.stay_date_daily 와 세그먼트 net_rn 자동 대조(불일치 시 산출 중단)
#   2) 회귀 가드: 산출 실패/대조검증 실패 시 커밋 거부(과거 자료로 되돌리지 않음)
#   3) 변경 없으면(동일 수치) 커밋 생략(멱등)
#   4) 소유 파일만 커밋 → git-safe push (safe_push, concurrency)
#
# ⚠️ 독립 산출물 원칙:
#   · 전체 build.py 실행 안 함. db_aggregated/otb_data/*.gz 무접촉.
#   · git add 는 이 페이지 소유 파일만(명시적). `git add -A` 금지.
#
# 참고: 자동 파이프라인(daily_update.sh 10d)에서도 매일 재산출되므로,
#       온북이 늘면 자동 반영된다. 이 스크립트는 즉시 갱신이 필요할 때만 쓴다.
#
# 사용법 (repo 루트에서):
#   ./update_special_period.sh              # 최신 온북 반영 + push
#   ./update_special_period.sh --no-push    # 커밋만
# =============================================================================
set -uo pipefail

DO_PUSH=1
for arg in "$@"; do
    case "$arg" in
        --no-push) DO_PUSH=0 ;;
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
    esac
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

BUILDER="scripts/build_special_period.py"
DATA_JSON="data/special_period.json"
DOCS_JSON="docs/data/special_period.json"
HIST_DATA="data/special_period_history.json"
HIST_DOCS="docs/data/special_period_history.json"
# 이 페이지가 소유(=커밋 대상)하는 파일. db/otb/gz 등 공용파일은 절대 포함하지 않는다.
# 히스토리(일자별 온북 스냅샷)는 데일리 증감 표기의 근거이므로 함께 커밋한다.
OWNED=("$DATA_JSON" "$DOCS_JSON" "$HIST_DATA" "$HIST_DOCS")

command -v python3 >/dev/null 2>&1 || die "python3 없음"
[ -f "$BUILDER" ] || die "$BUILDER 없음"
[ -d "data/raw_db" ] || die "원천 data/raw_db 없음"

echo -e "${BOLD}${BLUE}=== 스페셜/연휴 전략상품 실적 팔로업 갱신 ===${NC}"
echo "  시작 : $(date '+%Y-%m-%d %H:%M:%S')"

# 데이터 해시(생성시각·파일명 제외) — 멱등 판정
data_hash(){ python3 - "$1" <<'PY' 2>/dev/null || echo none
import json,sys,hashlib
try: d=json.load(open(sys.argv[1],encoding="utf-8"))
except Exception: print("none"); sys.exit()
d.get("meta",{}).pop("generated_at",None)
for per in d.get("periods",[]): per.pop("_files",None)
print(hashlib.md5(json.dumps(d,ensure_ascii=False,sort_keys=True).encode()).hexdigest())
PY
}
BEFORE="$(data_hash "$DOCS_JSON")"

# ── 1) 빌드 (대조검증 실패 시 exit≠0 → 커밋 안 함) ──
log "빌드 실행: $BUILDER"
python3 "$BUILDER" || die "빌드/대조검증 실패 — 산출물 미기록(과거 자료 보존)"

# ── 2) 회귀 가드 + 검증 ──
python3 - "$DOCS_JSON" <<'PY' || die "산출물 검증 실패"
import json,sys
d=json.load(open(sys.argv[1],encoding="utf-8"))
ps=d.get("periods",[])
assert ps, "periods 비어있음"
for p in ps:
    s=p.get("summary",{})
    assert s.get("rn") is not None, f"{p.get('key')} summary 없음"
    # 정합: 사업장합 == 세그합 == summary
    psum=sum(r["rn"] for r in p.get("by_property",[]))
    ssum=sum(r["rn"] for r in p.get("by_segment",[]))
    assert psum==ssum==s["rn"], f"{p.get('key')} 정합 실패 prop={psum} seg={ssum} sum={s['rn']}"
print(f"  검증 OK · {len(ps)}개 기간 · "+", ".join(f"{p['key']}(RN {p['summary']['rn']:,})" for p in ps))
PY

AFTER="$(data_hash "$DOCS_JSON")"
if [ "$BEFORE" = "$AFTER" ]; then
    ok "변경 없음(동일 수치) — 커밋 스킵(멱등)"
    for f in "${OWNED[@]}"; do git checkout -- "$f" 2>/dev/null || true; done
    exit 0
fi

# ── 3) git: 소유 파일만 스테이징(명시적) ──
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
git commit -m "chore(special-period): 스페셜/연휴 전략상품 실적 팔로업 갱신 ${TS} KST [skip ci]" || die "commit 실패"
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
            # 산출물 충돌: 우리 최신 파싱이 정본이므로 ours.
            for f in "${OWNED[@]}"; do git checkout --ours -- "$f" 2>/dev/null || true; git add -- "$f" 2>/dev/null || true; done
            git commit --no-edit 2>/dev/null || true
            ok "산출물 충돌 --ours 자동해소 — 재push"
        fi
    done
    return 1
}
if safe_push; then ok "push 완료 🚀"; else die "push 실패(원격 발산 지속) — 수동 확인"; fi
