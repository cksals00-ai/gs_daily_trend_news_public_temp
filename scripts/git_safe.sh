#!/bin/bash
# scripts/git_safe.sh — daily_update.sh · host_daily_crawl.sh 공용 git 안전 계층
#
# 배경(2026-08-04 사고):
#   중단된 rebase(.git/rebase-merge)가 남은 채 데몬이 detached HEAD 위에 그대로
#   커밋 → 로컬 daily 빌드가 브랜치 밖에 고립되고 push 만 실패했다. 수동 복구가
#   필요했다. 이 파일은 그 경로를 스크립트가 스스로 넘어가게 만든다.
#
# 제공 함수 (호출측에서 source 후 사용):
#   gsn_git_heal_state    미완 rebase/merge/cherry-pick 잔재 정리 (현재 커밋 보존)
#   gsn_git_ensure_main   detached/타브랜치 → main 으로 보정 (커밋 유실 없음)
#   gsn_git_sync_push     fetch → rebase origin/main → push, 최대 3회 (force 금지)
#   반환값: 0=성공  1=3회 실패  2=코드충돌 등 자동해소 불가(중단要)  3=구조적 중단
#
# 로깅: 호출측이 source 후 gsn_log() 를 재정의하면 그쪽으로 나간다(호스트=LOG_FILE).
# git 원문 출력: GSN_GIT_OUT (기본 stdout, 호스트는 LOG_FILE 지정).
#
# 원칙:
#   · force-push 금지 — 어떤 경로로도 --force/-f 를 쓰지 않는다.
#   · 로컬 커밋 보존 — 자동복구가 커밋을 버리는 경우는 없다. 애매하면 refs/gsn-backup/*
#     로 고정해두고 중단한다.
#   · 코드(data/·docs/ 밖) 충돌은 절대 자동해소하지 않는다 — 중단하고 사람에게 넘긴다.

[ -n "${GSN_GIT_SAFE_LOADED:-}" ] && return 0
GSN_GIT_SAFE_LOADED=1

# ── 비대화식 강제 (source 시점에 export → 호출측 자체 git 명령에도 적용) ──────
export GIT_TERMINAL_PROMPT=0     # 자격증명 프롬프트 금지 (걸리면 즉시 실패)
export GIT_EDITOR=true           # 에디터가 뜨며 멈추는 일 없음
export GIT_MERGE_AUTOEDIT=no     # merge 커밋 메시지 편집기 금지
export GIT_PAGER=cat             # 페이저 대기 금지
export GIT_ADVICE_DETACHEDHEAD=0

# ── 명령별 타임아웃(초) — 환경변수로 덮어쓸 수 있음 ─────────────────────────
#   push 는 db_aggregated.json.gz(14MB) 등이 있어 넉넉히 잡는다.
GSN_GIT_T_FETCH="${GSN_GIT_T_FETCH:-180}"
GSN_GIT_T_REBASE="${GSN_GIT_T_REBASE:-300}"
GSN_GIT_T_PUSH="${GSN_GIT_T_PUSH:-600}"
GSN_GIT_T_LOCAL="${GSN_GIT_T_LOCAL:-180}"
GSN_GIT_OUT="${GSN_GIT_OUT:-/dev/stdout}"

if ! declare -f gsn_log >/dev/null 2>&1; then
    gsn_log() { echo "$*"; }
fi

# ── 타임아웃 래퍼 (macOS 엔 coreutils timeout 이 없다) ───────────────────────
#   git 을 백그라운드로 띄우고 초 단위로 감시 → 초과 시 TERM→KILL. 반환 124=타임아웃.
_gsn_git() {   # _gsn_git <초> <라벨> <git 인자...>
    local secs="$1" label="$2"; shift 2
    local pid rc waited=0
    git "$@" >>"$GSN_GIT_OUT" 2>&1 &
    pid=$!
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$waited" -ge "$secs" ]; then
            kill -TERM "$pid" 2>/dev/null || true
            sleep 2
            kill -KILL "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
            gsn_log "    ⏱ 타임아웃 ${secs}s 초과 — git ${label} 중단"
            return 124
        fi
        sleep 1
        waited=$((waited + 1))
    done
    if wait "$pid"; then rc=0; else rc=$?; fi
    return "$rc"
}

_gsn_gitdir() { git rev-parse --git-dir 2>/dev/null || echo ".git"; }

_gsn_rebase_in_progress() {
    local d; d="$(_gsn_gitdir)"
    [ -d "$d/rebase-merge" ] || [ -d "$d/rebase-apply" ]
}

# 현재 커밋을 백업 ref 로 고정 — 어떤 자동복구 경로로도 커밋이 사라지지 않게.
_gsn_backup_ref() {   # _gsn_backup_ref <태그> [sha]
    local tag="$1" sha="${2:-}"
    [ -z "$sha" ] && sha="$(git rev-parse HEAD 2>/dev/null || true)"
    [ -z "$sha" ] && return 0
    git update-ref "refs/gsn-backup/${tag}-$(date '+%Y%m%d-%H%M%S')" "$sha" 2>/dev/null || true
    # 무한 증식 방지: 최신 20개만 유지
    git for-each-ref --format='%(refname)' --sort=-refname 'refs/gsn-backup/*' 2>/dev/null \
        | tail -n +21 | while IFS= read -r r; do
            [ -n "$r" ] && git update-ref -d "$r" 2>/dev/null || true
        done
    return 0
}

# ── 충돌 자동해소 — 산출물만. 코드는 손대지 않는다 ──────────────────────────
#
#   ⚠️ ours/theirs 의미가 merge 와 rebase 에서 뒤집힌다. 이걸 틀리면 "재빌드본 우선"이
#      정확히 반대로 동작해 방금 만든 빌드를 원격 구본으로 덮어쓴다.
#        merge  : :2(--ours)  = 로컬(방금 빌드본)   :3(--theirs) = origin
#        rebase : :2(--ours)  = origin(=onto)       :3(--theirs) = 재생되는 로컬 커밋(빌드본)
#      → "생성물은 재빌드본 우선" 이므로  merge → --ours,  rebase → --theirs.
_gsn_resolve_conflicts() {   # $1=merge|rebase   0=해소됨, 2=코드충돌(중단要)
    local mode="$1" U BAD pick_local local_stage remote_stage
    U="$(git diff --name-only --diff-filter=U 2>/dev/null || true)"
    [ -z "$U" ] && return 0

    BAD="$(printf '%s\n' "$U" | grep -vE '^(data/|docs/|_host_crawl_status\.json$)' || true)"
    if [ -n "$BAD" ]; then
        gsn_log "    ❌ data/·docs/ 밖(코드·스크립트) 충돌 — 자동해소 금지:"
        printf '%s\n' "$BAD" | sed 's/^/        /' | while IFS= read -r l; do gsn_log "$l"; done
        return 2
    fi

    if [ "$mode" = "rebase" ]; then
        pick_local="--theirs"; local_stage=3; remote_stage=2
    else
        pick_local="--ours";   local_stage=2; remote_stage=3
    fi

    printf '%s\n' "$U" | while IFS= read -r f; do
        [ -z "$f" ] && continue
        if [ "$f" = "_host_crawl_status.json" ]; then
            # 상태파일은 ts(ISO 문자열) 최신본 우선 — 롤백 방지
            L_TS="$(git show ":${local_stage}:$f" 2>/dev/null | sed -n 's/.*"ts": *"\([^"]*\)".*/\1/p')"
            R_TS="$(git show ":${remote_stage}:$f" 2>/dev/null | sed -n 's/.*"ts": *"\([^"]*\)".*/\1/p')"
            if [ -n "$R_TS" ] && { [ -z "$L_TS" ] || [[ "$R_TS" > "$L_TS" ]]; }; then
                git checkout "--$([ "$remote_stage" = 2 ] && echo ours || echo theirs)" -- "$f" >/dev/null 2>&1 || true
            else
                git checkout "$pick_local" -- "$f" >/dev/null 2>&1 || true
            fi
            git add -- "$f" >/dev/null 2>&1 || true
            continue
        fi
        # 한쪽에서 삭제된 파일이면 checkout 이 실패한다 → 워킹트리 내용 그대로 add
        git checkout "$pick_local" -- "$f" >/dev/null 2>&1 || true
        git add -- "$f" >/dev/null 2>&1 || git rm -f -q -- "$f" >/dev/null 2>&1 || true
    done
    gsn_log "    ↳ 생성물 충돌 자동해소($mode: 재빌드본=$pick_local): $(printf '%s\n' "$U" | wc -l | tr -d ' ')개"

    if [ "$mode" = "rebase" ]; then
        # 해소 결과가 onto 와 동일하면 남길 변경이 없다 → --skip 이 정답
        if git diff --cached --quiet 2>/dev/null; then
            _gsn_git "$GSN_GIT_T_REBASE" "rebase --skip" rebase --skip && return 0
            return 2
        fi
        _gsn_git "$GSN_GIT_T_REBASE" "rebase --continue" rebase --continue && return 0
        return 2
    fi
    _gsn_git "$GSN_GIT_T_LOCAL" "commit --no-edit" commit --no-edit >/dev/null 2>&1 && return 0
    return 2
}

# rebase 는 커밋마다 멈출 수 있다 → 끝나거나 실패할 때까지 해소+continue 반복
_gsn_rebase_drive() {   # 0=완료, 2=중단要
    local guard=0 rc
    while _gsn_rebase_in_progress; do
        guard=$((guard + 1))
        if [ "$guard" -gt 20 ]; then
            gsn_log "    ❌ rebase 해소 반복 20회 초과 — 중단"
            return 2
        fi
        if _gsn_resolve_conflicts rebase; then rc=0; else rc=$?; fi
        [ "$rc" -ne 0 ] && return 2
    done
    return 0
}

# ── [1] 미완 rebase/merge 잔재 자동 정리 (현재 커밋 보존) ────────────────────
gsn_git_heal_state() {
    local d head_sha orig_head
    d="$(_gsn_gitdir)"
    find "$d" -maxdepth 3 -name '*.lock' -type f -delete 2>/dev/null || true

    if _gsn_rebase_in_progress; then
        head_sha="$(git rev-parse HEAD 2>/dev/null || true)"
        orig_head="$(cat "$d/rebase-merge/orig-head" 2>/dev/null || cat "$d/rebase-apply/orig-head" 2>/dev/null || true)"
        _gsn_backup_ref "heal" "$head_sha"
        # rebase --abort 는 HEAD 를 orig-head 로 되돌린다. 중단된 rebase 위에 새 커밋이
        # 쌓여 있으면(데몬이 detached HEAD 에 그대로 커밋한 2026-08-04 사례) 그 커밋을
        # 통째로 버린다 → 현재 커밋이 orig-head 에 포함될 때만 abort, 아니면 --quit.
        if [ -n "$orig_head" ] && [ -n "$head_sha" ] \
           && git merge-base --is-ancestor "$head_sha" "$orig_head" 2>/dev/null; then
            gsn_log "    🔧 중단된 rebase 잔재 — 위에 쌓인 커밋 없음 → rebase --abort"
            _gsn_git "$GSN_GIT_T_LOCAL" "rebase --abort" rebase --abort || true
        else
            gsn_log "    🔧 중단된 rebase 잔재 — 위에 커밋 존재 → rebase --quit (현재 커밋 보존)"
            _gsn_git "$GSN_GIT_T_LOCAL" "rebase --quit" rebase --quit || true
        fi
    fi

    if [ -f "$d/MERGE_HEAD" ]; then
        gsn_log "    🔧 미완 merge(MERGE_HEAD) 잔재 — merge --abort (로컬 커밋은 보존)"
        _gsn_backup_ref "heal"
        _gsn_git "$GSN_GIT_T_LOCAL" "merge --abort" merge --abort || true
    fi
    if [ -f "$d/CHERRY_PICK_HEAD" ]; then
        gsn_log "    🔧 미완 cherry-pick 잔재 — cherry-pick --abort"
        _gsn_git "$GSN_GIT_T_LOCAL" "cherry-pick --abort" cherry-pick --abort || true
    fi
    if [ -f "$d/REVERT_HEAD" ]; then
        gsn_log "    🔧 미완 revert 잔재 — revert --abort"
        _gsn_git "$GSN_GIT_T_LOCAL" "revert --abort" revert --abort || true
    fi

    # 잔재 정리 후에도 unmerged 가 남으면 알리기만 한다(빌드 산출물을 임의로 되돌리지 않음)
    if [ -n "$(git diff --name-only --diff-filter=U 2>/dev/null || true)" ]; then
        gsn_log "    ⚠ unmerged 파일이 남아 있음 — 이후 충돌 해소 단계에서 처리"
    fi
    return 0
}

# ── [2] 항상 main 에서 커밋 (detached HEAD 방지) ────────────────────────────
gsn_git_ensure_main() {   # 0=main 확보, 3=중단要
    local br head_sha main_sha
    br="$(git symbolic-ref --quiet --short HEAD 2>/dev/null || true)"
    [ "$br" = "main" ] && return 0

    if [ -n "$br" ]; then
        gsn_log "    ❌ 현재 브랜치가 main 이 아님($br) — 자동 전환하지 않고 중단"
        gsn_log "       작업 중인 브랜치를 건드리지 않기 위함. 'git checkout main' 후 재실행하세요."
        return 3
    fi

    head_sha="$(git rev-parse HEAD 2>/dev/null || true)"
    [ -z "$head_sha" ] && { gsn_log "    ❌ HEAD 를 읽을 수 없음 — 중단"; return 3; }
    _gsn_backup_ref "detached" "$head_sha"
    gsn_log "    🔧 detached HEAD($(git rev-parse --short HEAD)) 감지 — main 으로 복귀(현재 커밋 보존)"

    # checkout -B 는 워킹트리를 건드리지 않고 main ref 만 현재 커밋으로 옮긴다.
    # → 방금 만든 빌드 산출물(미커밋)이 안전하다. 'checkout main'(트리 교체)은 쓰지 않는다.
    if git rev-parse --verify --quiet main >/dev/null 2>&1; then
        main_sha="$(git rev-parse main)"
        if ! git merge-base --is-ancestor "$main_sha" "$head_sha" 2>/dev/null; then
            # main 에만 있는 커밋이 있다 → 버리지 않고 백업 ref 로 고정한 뒤 알린다.
            _gsn_backup_ref "main-diverged" "$main_sha"
            gsn_log "    ⚠ main($(git rev-parse --short main))에만 있는 커밋이 있어 refs/gsn-backup/ 에 보존함"
            gsn_log "       복구가 필요하면: git merge \$(git for-each-ref --format='%(refname)' 'refs/gsn-backup/main-diverged-*' | tail -1)"
        fi
    fi

    if _gsn_git "$GSN_GIT_T_LOCAL" "checkout -B main" checkout -B main; then
        gsn_log "    ✅ main 복귀 완료 ($(git rev-parse --short HEAD))"
        return 0
    fi
    gsn_log "    ❌ main 복귀 실패 — 중단(커밋은 refs/gsn-backup/ 에 보존됨)"
    return 3
}

# ── [3] fetch → rebase origin/main → push (최대 3회, force 금지) ────────────
gsn_git_sync_push() {   # 0=성공 1=3회실패 2=코드충돌(중단要) 3=구조적 중단
    local attempt rc
    for attempt in 1 2 3; do
        gsn_git_heal_state
        if gsn_git_ensure_main; then :; else return 3; fi

        if ! _gsn_git "$GSN_GIT_T_FETCH" "fetch origin main" fetch origin main; then
            gsn_log "    ⚠ fetch 실패(네트워크?) — 시도 ${attempt}/3"
            continue
        fi

        if git merge-base --is-ancestor origin/main HEAD 2>/dev/null; then
            gsn_log "    ℹ origin/main 이 이미 로컬에 포함됨 — rebase 생략"
        else
            gsn_log "    ↻ rebase origin/main (시도 ${attempt}/3)"
            # --autostash: _host_crawl_status.json 등 미스테이징 변경이 있어도 rebase 가 거부되지 않게
            if _gsn_git "$GSN_GIT_T_REBASE" "rebase origin/main" rebase --autostash origin/main; then
                gsn_log "    ✅ rebase 클린"
            else
                if _gsn_rebase_drive; then rc=0; else rc=$?; fi
                if [ "$rc" -ne 0 ]; then
                    gsn_log "    ❌ 자동해소 불가 — rebase 되돌리고 중단(로컬 커밋 보존)"
                    _gsn_backup_ref "pre-abort"
                    _gsn_git "$GSN_GIT_T_LOCAL" "rebase --abort" rebase --abort \
                        || _gsn_git "$GSN_GIT_T_LOCAL" "rebase --quit" rebase --quit || true
                    return 2
                fi
                gsn_log "    ✅ rebase 충돌 자동해소 완료(생성물=재빌드본 우선)"
            fi
        fi

        # force 금지 — 일반 push 만. 거부되면 위로 돌아가 fetch·rebase 재시도.
        if _gsn_git "$GSN_GIT_T_PUSH" "push origin main" push origin main; then
            gsn_log "    ✅ push 성공 ($(git rev-parse --short HEAD))"
            return 0
        fi
        gsn_log "    ⚠ push 시도 ${attempt}/3 실패(원격 선점 추정) — fetch·rebase 후 재시도"
    done

    gsn_log "    ❌ push 최종 실패(3회) — 로컬 커밋은 보존됨(force-push 하지 않음)"
    return 1
}
