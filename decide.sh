#!/bin/bash
# =============================================================================
# decide.sh — 결정 로그 한 줄 추가 / 결과 채우기
# =============================================================================
#   ./decide.sh                    대화형으로 새 결정 기록
#   ./decide.sh --result <id>      기존 결정의 '실제결과' 채우기
#   ./decide.sh --list             최근 10건 보기 (id 확인용)
#   ./decide.sh --no-push          커밋만, 푸시 생략
#
#   저장 위치: docs/data/decision_log.json
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")" || exit 1
JSON="docs/data/decision_log.json"
DO_PUSH=1
MODE="add"; TARGET=""

for a in "$@"; do
  case "$a" in
    --no-push) DO_PUSH=0 ;;
    --list)    MODE="list" ;;
    --result)  MODE="result" ;;
    -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
    *)         [ "$MODE" = "result" ] && [ -z "$TARGET" ] && TARGET="$a" ;;
  esac
done

[ -f "$JSON" ] || { echo "없음: $JSON"; exit 1; }

if [ "$MODE" = "list" ]; then
  python3 - "$JSON" <<'PY'
import json,sys
d=json.load(open(sys.argv[1],encoding="utf-8"))
for e in sorted([x for x in d["entries"] if x["id"]!="SAMPLE"],
                key=lambda x:x["date"],reverse=True)[:10]:
    mark="OK " if (e.get("result") or "").strip() else "대기"
    print(f'[{mark}] {e["id"]}  {e["date"]}  {e["decision"][:52]}')
PY
  exit 0
fi

ask(){ printf "\033[1;33m%s\033[0m\n> " "$1"; IFS= read -r REPLY_; echo "$REPLY_"; }

if [ "$MODE" = "result" ]; then
  [ -n "$TARGET" ] || { echo "id를 지정하세요.  ./decide.sh --list 로 확인"; exit 1; }
  RESULT=$(ask "실제결과 — 숫자로 (${TARGET})")
  [ -n "$RESULT" ] || { echo "취소"; exit 1; }
  python3 - "$JSON" "$TARGET" "$RESULT" <<'PY'
import json,sys,datetime
p,tid,res=sys.argv[1],sys.argv[2],sys.argv[3]
d=json.load(open(p,encoding="utf-8"))
hit=[e for e in d["entries"] if e["id"]==tid]
if not hit: sys.exit(f"id 없음: {tid}")
hit[0]["result"]=res
hit[0]["result_date"]=datetime.date.today().isoformat()
d["updated"]=datetime.date.today().isoformat()
json.dump(d,open(p,"w",encoding="utf-8"),ensure_ascii=False,indent=2)
print(f"결과 기록됨: {tid}")
PY
  MSG="chore(decision-log): 결과 기록 $TARGET"
else
  echo "── 새 결정 기록 (빈 줄로 두면 나중에 채울 수 있어요) ──"
  SRC=$(ask  "어느 화면을 보고 알았나요? (예: gs-map / 경쟁사 모니터)")
  OBS=$(ask  "무엇을 봤나요? 숫자와 기준을 함께")
  DEC=$(ask  "그래서 무엇을 했나요?")
  OWN=$(ask  "담당자")
  EXP=$(ask  "기대효과 — 반드시 숫자로 (예: 해당 주 예약 +15건)")
  TAG=$(ask  "태그 (쉼표 구분, 예: 가격,비발디)")
  [ -n "$DEC" ] || { echo "결정 내용이 비어 취소합니다."; exit 1; }
  python3 - "$JSON" "$SRC" "$OBS" "$DEC" "$OWN" "$EXP" "$TAG" <<'PY'
import json,sys,datetime
p,src,obs,dec,own,exp,tag=sys.argv[1:8]
d=json.load(open(p,encoding="utf-8"))
today=datetime.date.today().isoformat()
n=sum(1 for e in d["entries"] if e.get("date")==today)+1
eid=f'{today.replace("-","")}-{n:02d}'
d["entries"].insert(0,{"id":eid,"date":today,"source":src,"observation":obs,
  "decision":dec,"owner":own,"expected":exp,"result":"","result_date":"",
  "tags":[t.strip() for t in tag.split(",") if t.strip()]})
d["updated"]=today
json.dump(d,open(p,"w",encoding="utf-8"),ensure_ascii=False,indent=2)
print(f"기록됨: {eid}")
PY
  MSG="chore(decision-log): 결정 추가 $(date +%Y-%m-%d)"
fi

git add "$JSON" docs/decision-log.html 2>/dev/null
git diff --cached --quiet && { echo "변경 없음 — 커밋 생략"; exit 0; }
git commit -q -m "$MSG" && echo "커밋 완료"
if [ "$DO_PUSH" = "1" ]; then
  git pull --rebase -q origin main 2>/dev/null
  git push -q origin main && echo "푸시 완료" || echo "푸시 실패 — 나중에 ./decide.sh --no-push 후 수동 푸시"
fi
