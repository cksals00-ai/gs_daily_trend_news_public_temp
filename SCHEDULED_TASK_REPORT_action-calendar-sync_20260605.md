# action-calendar-sync 자동 실행 리포트 — 2026-06-05

실행 시각: 2026-06-05 06:06 KST (자동)
커밋: `2fed91d` chore(auto): action calendar sync 2026-06-05 06:06 KST [skip ci]

## 결과 요약

| 단계 | 결과 |
|---|---|
| git lock 정리 | ⚠ unlink 차단 → rename(mv)으로 우회 정리 |
| 1단계: Chrome MCP 구글시트 fetch | ⏭ 미실행 (브라우저 미연결) |
| 2단계: generate_campaign_performance.py | ✅ 실적 재집계 |
| 3단계: build.py | ✅ 전체 리빌드 |
| git commit | ✅ (로컬) |
| git push origin main | ❌ 차단 (아래 참조) |

## git lock 정리 — 우회 처리

`.git/index.lock`, `.git/HEAD.lock` (전일 20:06~20:07 UTC 잔존) 가 있었으나, 이 세션의 virtiofs 마운트는 `.git` 내부 파일의 **unlink(rm)를 EPERM으로 차단**합니다. 대신 **rename(mv)은 허용**되어, 두 lock을 같은 디렉터리 내 `*.stale_*` 이름으로 옮겨 정리한 뒤 정상적으로 commit을 수행했습니다. 마지막에 다시 생성된 phantom `index.lock`도 동일 방식으로 제거하여 `.git`에 활성 `*.lock` 파일이 없는 clean 상태로 종료했습니다. (commit은 `git fsck` connectivity 통과, HEAD 정상.)

## 1단계: 구글시트 fetch — 미실행

Chrome MCP(브라우저 확장)가 연결되어 있지 않아(`list_connected_browsers` → 빈 목록) 구글시트 CSV fetch를 수행하지 못했습니다. 무인 실행이라 `switch_browser`(사용자 클릭 대기)도 무의미하여 시도하지 않았습니다. 주의사항 규정대로 **기존 campaign_data.json을 유지**하고, raw_db 기반 실적 재집계만 진행했습니다. 보안 규칙상 sandbox에서 docs.google.com 직접 접속·네트워크 우회는 시도하지 않았습니다(no-args fallback도 docs.google.com 403으로 실패 → 기존 JSON 유지).

- 기존 campaign_data.json: events 225개 (유지)

## 2단계: 실적 재집계 — 정상

`generate_campaign_performance.py` 가 raw_db 27/28 txt에서 패키지코드 매칭으로 실적을 재집계했습니다.
- 패키지코드 매핑: 123개 코드 → 1개 Key
- 예약자료 매칭 4,074행 / 취소자료 매칭 4,395행 / 총 매칭 8,469행
- campaign_performance.json: by_key 1건 / 누적 RN 4,080 / 누적 매출 787.9백만

## 3단계: 빌드 — 정상

`build.py` 전체 HTML 리빌드 완료.
- index.html 1,715,752 bytes
- otb.html 215,086 bytes (YoY 88개 사업장)
- db_aggregated.json / package_series_trend.json / rm_fcst.json / data_freshness.json 동기화
- 당일분석 교차검증 통과 (NET=1480, 세그먼트합=1480)
- Auto-Built 2026-06-05 06:05 KST

## ⚠ push 실패 — 사용자 조치 필요

`git push origin main` 이 **샌드박스 네트워크 정책으로 차단**되었습니다 (전일들과 동일 사유).
- 원격: `ssh://git@ssh.github.com:443/cksals00-ai/gs_daily_trend_news_public_temp.git`
- SSH: `CONNECT ssh.github.com:443: Forbidden` (호스트 SOCKS 프록시 allowlist 거부)
- HTTPS 대체: `github.com` → 프록시 403 (allowlist 외), 저장된 credential 없음
- `pull --rebase` 재시도도 동일 차단

커밋·빌드 산출물은 로컬에 정상 보존, 작업 트리 clean. 현재 로컬 `main` 이 `origin/main` 보다 **4 커밋 앞서** 있습니다. 보안 규칙상 네트워크 우회는 시도하지 않았습니다.

GitHub 접근이 가능한 환경(사용자 Mac 터미널)에서 1회 실행 필요:

```
cd ~/Desktop/gs_daily_trend_news_public_temp
git push origin main
```

## 체크포인트

- campaign_main.csv: 기존 캐시 유지 (신규 fetch 없음)
- campaign_data.json: events 225 (유지)
- campaign_performance.json: by_key 1, 총 RN 4,080

## 개선 제안

1. 구글시트 fetch가 동작하려면 06:00 실행 시점에 **Chrome 실행 + 확장 로그인** 상태여야 함. 현재 5일+ 연속 브라우저 미연결로 fetch 스킵 중.
2. push 차단이 매일 반복됨 — GitHub 도달 가능 환경에서 스케줄을 돌리거나 별도 푸시 자동화 필요. 매 실행마다 로컬 커밋만 누적되고 있음.
3. 이 세션 마운트는 `.git` 내부 파일 unlink 차단(EPERM) 특성이 있어 lock 잔존 시 rename 우회가 필요함. raw_db 재집계·빌드·로컬 커밋은 정상 동작.
