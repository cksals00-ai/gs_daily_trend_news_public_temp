#!/bin/bash
# 인바운드 실적 파일 원클릭 생성.
#   ./scripts/inbound_reports/run.sh 2025 2026        # 지정 연도
#   ./scripts/inbound_reports/run.sh                   # 기본: 작년(전체)+올해(전월까지)
#   ./scripts/inbound_reports/run.sh --strategy 2026   # 전략방안 롤포워드도 함께
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
source scripts/py_env.sh

DIR=scripts/inbound_reports
do_strategy=0; years=()
for a in "$@"; do
  if [ "$a" = "--strategy" ]; then do_strategy=1; else years+=("$a"); fi
done

# 기본 연도: 작년 + 올해(현재 전월까지 마감으로 간주)
if [ ${#years[@]} -eq 0 ]; then
  Y=$(date +%Y); PM=$(printf '%02d' $(( $(date +%-m) - 1 )))
  [ "$PM" = "00" ] && PM=12
  echo "▶ 기본 실행: $((Y-1)) 전체 + $Y 1~${PM}월"
  python3 "$DIR/build_actuals.py" --year $((Y-1))
  python3 "$DIR/build_actuals.py" --year "$Y" --max-month "$PM"
  [ "$do_strategy" = 1 ] && python3 "$DIR/rollforward_strategy.py" --base-year "$Y" --max-month "$PM"
else
  for y in "${years[@]}"; do
    python3 "$DIR/build_actuals.py" --year "$y"
    [ "$do_strategy" = 1 ] && python3 "$DIR/rollforward_strategy.py" --base-year "$y"
  done
fi
# 도전목표 전략방안: 대시보드 inline 데이터 + 호스팅 xlsx + ~/Downloads 사본 재생성
python3 "$DIR/build_dojeon.py"
echo "✅ 완료 — ~/Downloads 확인"
