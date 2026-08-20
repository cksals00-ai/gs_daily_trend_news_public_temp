#!/bin/bash
# update_action_plan.sh — 인플루언서 시트(라이브) → 실적 → Action Plan 페이지
set -uo pipefail
cd "$(dirname "$0")" || exit 1
DO_PUSH=1; for a in "$@"; do [ "$a" = "--no-push" ] && DO_PUSH=0; done
python3 scripts/action_plan_perf.py || { echo "실적 생성 실패 — 기존 결과 유지"; exit 1; }
python3 - <<'PY' || exit 1
import json,sys
d=json.load(open("docs/data/action_plan_performance.json",encoding="utf-8"))
m=d.get("meta",{})
if m.get("rows_with_perf",0)==0:
    sys.exit("실적 0건 — 커밋 거부 (시트 발행/온북 파일 확인)")
print(f"검증 통과: {m['rows_with_perf']}/{m['rows_with_codes']}행, RN {m['total_rn']:,}")
PY
git add docs/data/action_plan_performance.json docs/action_plan_dashboard.html \
        data/action_plan_paste.csv scripts/action_plan_perf.py update_action_plan.sh 2>/dev/null
git diff --cached --quiet && { echo "변경 없음 — 커밋 생략"; exit 0; }
git commit -q -m "chore(action-plan): 인플루언서 실적 갱신 $(date +%Y-%m-%d)" && echo "커밋 완료"
if [ "$DO_PUSH" = "1" ]; then
  git pull --no-rebase --autostash -q origin main
  git push -q origin main && echo "푸시 완료" || echo "푸시 실패 — 나중에 git push"
fi
