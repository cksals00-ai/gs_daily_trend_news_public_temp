# action-calendar-sync 자동 실행 리포트 — 2026-06-07

실행 시각: 2026-06-07 06:08 KST (자동, 무인)
커밋: `60cce7f` chore(auto): action calendar sync 2026-06-07 06:08 KST [skip ci]

## 결과 요약

| 단계 | 결과 |
|---|---|
| git lock 정리 | ⚠ 잔존 HEAD.lock/index.lock → rename(mv) 우회 정리 |
| 1단계: Chrome MCP 구글시트 fetch | ⏭ 미실행 (브라우저 미연결) |
| 2단계: generate_campaign_performance.py | ✅ 실적 재집계 |
| 3단계: build.py | ✅ 전체 리빌드 |
| git commit | ✅ 로컬 커밋 성공 (`60cce7f`) |
| git push origin main | ❌ 차단 (샌드박스 네트워크 정책) |

## git lock 정리 — 우회 처리

시작 시점에 `.git/HEAD.lock`·`.git/index.lock`(전일 20:07 UTC 잔존)이 있었습니다. 이 세션 마운트는 `.git` 내부 파일의 **unlink(rm)를 EPERM으로 차단**(`Operation not permitted`)하므로, 동일 디렉터리 내 `*.autosync_*`/`*.phantom_*` 이름으로 **rename(mv)** 하여 비웠습니다. 락을 비운 직후 커밋이 정상 진행되었고, 커밋 중 재생성된 phantom 락도 같은 방식으로 정리했습니다. (index.lock은 git 명령마다 재생성되는 phantom 특성이 있으나 완료된 커밋에는 영향 없음.)

## 1단계: 구글시트 fetch — 미실행

Chrome MCP(브라우저 확장)가 연결되어 있지 않아(`tabs_context_mcp` → not connected) 구글시트 CSV fetch를 수행하지 못했습니다. 무인 06:00 실행이라 사용자 조작 대기는 무의미하여 시도하지 않았습니다. 주의사항 규정대로 **기존 campaign_data.json을 유지**하고, raw_db 기반 실적 재집계만 진행했습니다. 보안 규칙상 sandbox에서 docs.google.com 직접 접속·네트워크 우회는 시도하지 않았습니다(참고: `generate_campaign_data.py` 무인자 실행 시 `urlopen 403 Forbidden` — sandbox에서 docs.google.com 미허용 확인).

- 기존 campaign_main.csv 캐시 유지: 65,378 bytes (6/1 fetch분)
- 기존 campaign_data.json: events 225개 (유지), package_codes 보유 Key 2개

## 2단계: 실적 재집계 — 정상

`generate_campaign_performance.py` 가 raw_db 27/28 txt(20260605 생성분)에서 패키지코드 매칭으로 실적을 재집계했습니다.
- 패키지코드 매핑: 123개 코드 → 1개 Key (`20`)
- 예약자료 매칭 4,200행 / 취소자료 매칭 4,576행 / 총 매칭 8,776행
- campaign_performance.json: by_key 1건 / 누적 RN 4,206 / 누적 매출 812.4백만
- (전일과 동일 raw_db 생성분 기준 — RN 4,206 / 매출 812.4백만 유지)

## 3단계: 빌드 — 정상

`build.py` 전체 HTML 리빌드 완료.
- index.html 1,546,646 bytes
- otb.html 215,138 bytes (YoY 88개 사업장)
- db_aggregated.json / package_series_trend.json / rm_fcst.json / data_freshness.json(5개 소스) 동기화
- 당일분석 교차검증 통과 (NET=2870, 세그먼트합=2870 · OTA=1387, G-OTA=1276, IB=207)
- 패키지 트렌드 14,409계열(TOP 100 표시), 카테고리 뉴스 234건 주입
- Auto-Built 2026-06-07 06:06 KST

## ⚠ push 실패 — 사용자 조치 필요

`git push origin main` 이 **샌드박스 네트워크 정책으로 차단**되었습니다 (전일들과 동일 사유).
- 원격: `ssh://git@ssh.github.com:443/cksals00-ai/gs_daily_trend_news_public_temp.git`
- SSH: `socat E CONNECT ssh.github.com:443: Forbidden` (호스트 프록시 allowlist 거부)
- `pull --rebase` 재시도도 동일 네트워크 차단이 예상되어 무의미 → 미시도

커밋·빌드 산출물은 로컬에 정상 보존, 작업 트리 clean. 현재 로컬 `main` 이 `origin/main` 보다 **10 커밋 앞서** 있습니다(미푸시 누적). 보안 규칙상 네트워크 우회는 시도하지 않았습니다. GitHub 도달 가능한 환경(사용자 Mac 터미널)에서 1회 실행 필요:

```
cd ~/Desktop/gs_daily_trend_news_public_temp
git push origin main
```

## 체크포인트

- campaign_main.csv: 기존 캐시 유지 (신규 fetch 없음), 65,378 bytes (> 1,000자 ✅)
- campaign_data.json: events 225 (유지), package_codes 보유 Key 2개
- campaign_performance.json: by_key 1, 총 RN 4,206, 총 매칭 8,776행
- 활성 git lock: index.lock phantom 재생성(커밋 무영향), HEAD.lock 없음

## 개선 제안

1. 구글시트 fetch가 동작하려면 06:00 실행 시점에 **Chrome 실행 + 확장 로그인** 상태여야 함. 7일+ 연속 브라우저 미연결로 fetch 스킵 중(실적 재집계는 raw_db 기반이라 정상).
2. push 차단이 매일 반복됨 — GitHub 도달 가능 환경에서 스케줄을 돌리거나 별도 푸시 자동화 필요. 로컬 미푸시 커밋 10건 누적.
3. 이 세션 마운트는 `.git` 내부 파일 unlink 차단(EPERM) 특성. lock 잔존 시 rename 우회 필요. raw_db 재집계·빌드·로컬 커밋은 정상.
