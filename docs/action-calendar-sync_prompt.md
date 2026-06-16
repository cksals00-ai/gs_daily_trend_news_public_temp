# action-calendar-sync — 교체용 프롬프트 (검증 전용)

> 배경: 기획전 구글시트 동기화·빌드·커밋·푸시는 이제 **호스트 launchd 잡**
> `com.gs.daily-crawl`(`scripts/host_daily_crawl.sh`, 매일 05:00 KST, build.py 직전
> `generate_campaign_data.py`로 시트 동기화 포함)이 전담한다.
> 샌드박스 스케줄 task `action-calendar-sync`는 프록시 403으로 외부망·git push가
> 모두 차단되므로, **네트워크 없이 호스트 잡 결과를 점검·리포트하는 역할**로 전환한다.
> (참고 메모: campaign_data 동기화는 호스트가 담당 / 데몬 git 경합 주의)

---

## 1) routine 프롬프트 (그대로 복붙)

```
역할: 매일 아침, 호스트 일일 크롤 잡(launchd com.gs.daily-crawl, 매일 05:00 KST)이
정상 수행됐는지 "로컬 신호만으로" 점검하고 한 줄 리포트를 내는 검증 전용 작업.

⚠️ 제약 (반드시 준수):
- 네트워크 접속 금지(구글시트 fetch·git fetch/pull/push·ls-remote 일절 안 함).
- Chrome/브라우저 MCP 사용 금지.
- 데이터 재생성·빌드·git add/commit/push 금지. 읽기 전용으로만 점검.
- 더미 데이터 생성 금지.
이유: 이 작업은 샌드박스에서 돌아 외부망이 프록시 403으로 차단됨. 실제 시트 동기화·
빌드·푸시는 호스트 launchd 잡(scripts/host_daily_crawl.sh, build.py 직전
generate_campaign_data.py로 구글시트 기획전 동기화 포함)이 전담한다.

대상 디렉터리: ~/Projects/gs_daily_trend_news_public_temp

점검 단계:
1. _host_crawl_status.json 읽기 → stage/status/exit_code/ts 확인.
   status=="success" 이고 ts가 오늘(KST)이면 정상.
2. git log -8 --format='%h %ci %s' 로 오늘자 "host crawl ... KST" 커밋 존재 확인.
3. docs/data/campaign_data.json 의 mtime이 오늘인지, 그리고 내부 events 수와
   package_codes 매핑 수가 비어있지 않은지 확인(기획전 시트 동기화 반영 여부).
4. 위 중 하나라도 이상(어제·오류 status / 오늘 커밋 없음 / campaign_data 미갱신)이면
   PROBLEM으로 판정하고, 어느 stage가 error였는지와 권장 조치
   ("호스트 터미널에서 ./scripts/host_daily_crawl.sh 수동 실행")를 명시.

출력: 결과를 채팅/알림으로 짧게 보고(✅ 정상 / ⚠️ PROBLEM + 원인 + 권장조치).
SCHEDULED_TASK_REPORT 파일 작성이나 커밋은 하지 않는다(푸시 불가 환경이므로).
```

## 2) 적용 방법

1. claude.ai(또는 데스크톱 앱)에서 자동화/스케줄(Automations · Routines · Scheduled tasks)
   목록을 연다.
2. **action-calendar-sync** 항목을 찾아 프롬프트를 위 §1 내용으로 **교체**.
3. 스케줄은 **매일 06:00 KST 유지**(호스트 05:00 실행 뒤 점검).
4. 저장. (비활성하지 않으므로 task는 그대로 살아 매일 의미있게 동작)

## 3) task를 못 찾을 때

이 routine은 로컬 파일·이 repo·Claude Code CLI의 scheduled-tasks 목록에는 없고
**서버측(claude.ai 계정)에 저장**돼 있다. 찾는 곳:

- **claude.ai** 웹 → 좌측/설정의 **Automations** 또는 **Scheduled / Routines** 메뉴
  (계정은 cksals00@gmail.com).
- 매일 커밋 author가 `action-calendar-sync <action-calendar-sync@auto.local>` 인
  자동 커밋을 만들던 그 작업이다.
- 끝내 안 보이면, 그냥 두어도 됨 — 실제 데이터 갱신은 호스트 잡이 하므로 기능상 문제
  없고, 이 task는 매일 무해한 "push 차단" 리포트만 낸다. 발견되는 시점에 위 프롬프트로
  교체하면 된다.

## 4) 현재 정상화 상태 (참고)

- 시트 동기화: `host_daily_crawl.sh` 의 `run_crawl "campaign_data_sync"` 단계
  (commit 9dd0891, 2026-06-16) — Chrome 없이 published CSV 직접 fetch.
- git push: 호스트 잡이 lock 정리 → add data/ docs/ → commit → push(rebase -X theirs
  재시도 + 푸시 후 LOCAL==REMOTE 동기검증)까지 수행. 정상.
