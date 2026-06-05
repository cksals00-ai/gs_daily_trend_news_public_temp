# action-calendar-sync 자동 실행 리포트 — 2026-06-06

실행 시각: 2026-06-06 06:04 KST (자동)
커밋: `6348e7f` chore(auto): action calendar sync 2026-06-06 06:04 KST [skip ci]

## 결과 요약

| 단계 | 결과 |
|---|---|
| git lock 정리 | ⚠ 잔존 HEAD.lock/index.lock → rename(mv)으로 우회 정리 |
| 1단계: Chrome MCP 구글시트 fetch | ⏭ 미실행 (브라우저 미연결) |
| 2단계: generate_campaign_performance.py | ✅ 실적 재집계 |
| 3단계: build.py | ✅ 전체 리빌드 |
| git commit | ✅ (로컬) |
| git push origin main | ❌ 차단 (아래 참조) |

## git lock 정리 — 우회 처리

시작 시점에 `.git/HEAD.lock`(전일 20:08 UTC 잔존)이 있었고, 커밋 과정에서 phantom `HEAD.lock`/`index.lock`이 재생성됐습니다. 이 세션 마운트는 `.git` 내부 파일의 **unlink(rm)를 EPERM으로 차단**하므로(`/tmp`로의 cross-device rename도 불가), 동일 디렉터리 내 `*.stale_*`/`*.phantom_*` 이름으로 **rename(mv)** 하여 정리했습니다. 종료 시 `.git`에 활성 `*.lock` 없음(clean). HEAD 정상(`6348e7f`).

## 1단계: 구글시트 fetch — 미실행

`list_connected_browsers` → 빈 목록. Chrome MCP(브라우저 확장)가 연결되어 있지 않아 구글시트 CSV fetch를 수행하지 못했습니다. 무인 실행이라 `switch_browser`(사용자 클릭 대기)는 무의미하여 시도하지 않았습니다. 주의사항 규정대로 **기존 campaign_data.json을 유지**(events 225개)하고, raw_db 기반 실적 재집계만 진행했습니다. 보안 규칙상 sandbox에서 docs.google.com 직접 접속·네트워크 우회는 시도하지 않았습니다.

- 기존 campaign_main.csv 캐시 유지: 65,378 bytes (6/1 fetch분)
- 기존 campaign_data.json: events 225개 (유지)

## 2단계: 실적 재집계 — 정상

`generate_campaign_performance.py` 가 raw_db 27/28 txt(20260605 생성분)에서 패키지코드 매칭으로 실적을 재집계했습니다.
- 패키지코드 매핑: 123개 코드 → 1개 Key
- 예약자료 매칭 4,200행 / 취소자료 매칭 4,576행 / 총 매칭 8,776행
- campaign_performance.json: by_key 1건(Key `20`) / 누적 RN 4,206 / 누적 매출 812.4백만
- (전일 대비 RN 4,080 → 4,206 +126, 매출 787.9 → 812.4백만)

## 3단계: 빌드 — 정상

`build.py` 전체 HTML 리빌드 완료.
- index.html 1,767,101 bytes
- otb.html 215,133 bytes (YoY 88개 사업장)
- db_aggregated.json / package_series_trend.json / rm_fcst.json / data_freshness.json(5개 소스) 동기화
- 당일분석 교차검증 통과 (NET=2870, 세그먼트합=2870 · OTA=1387, G-OTA=1276, IB=207)
- 패키지 트렌드 14,409계열, 카테고리 뉴스 359건 주입
- Auto-Built 2026-06-06 06:04 KST

## ⚠ push 실패 — 사용자 조치 필요

`git push origin main` 이 **샌드박스 네트워크 정책으로 차단**되었습니다 (전일들과 동일 사유).
- 원격: `ssh://git@ssh.github.com:443/cksals00-ai/gs_daily_trend_news_public_temp.git`
- SSH: `socat E CONNECT ssh.github.com:443: Forbidden` (호스트 프록시 allowlist 거부)
- `pull --rebase` 재시도도 동일 네트워크 차단이 예상되어 무의미 → 미시도

커밋·빌드 산출물은 로컬에 정상 보존, 작업 트리 clean. 현재 로컬 `main` 이 `origin/main` 보다 **4 커밋 앞서** 있습니다(미푸시 누적):
- `6348e7f` action calendar sync (오늘 본 작업)
- `ba17a9f` rm-fcst + booking update (6/5 20:08)
- `a8a96b4` host crawl (6/6 05:02)
- `abe49cf` trend regional update (6/6 02:08)

보안 규칙상 네트워크 우회는 시도하지 않았습니다. GitHub 도달 가능한 환경(사용자 Mac 터미널)에서 1회 실행 필요:

```
cd ~/Desktop/gs_daily_trend_news_public_temp
git push origin main
```

## 체크포인트

- campaign_main.csv: 기존 캐시 유지 (신규 fetch 없음), 65,378 bytes
- campaign_data.json: events 225 (유지)
- campaign_performance.json: by_key 1, 총 RN 4,206
- 활성 git lock: 없음 (clean)

## 개선 제안

1. 구글시트 fetch가 동작하려면 06:00 실행 시점에 **Chrome 실행 + 확장 로그인** 상태여야 함. 6일+ 연속 브라우저 미연결로 fetch 스킵 중. 실적 재집계는 raw_db 기반이라 정상 동작.
2. push 차단이 매일 반복됨 — GitHub 도달 가능 환경에서 스케줄을 돌리거나 별도 푸시 자동화 필요. 로컬 미푸시 커밋이 4건 누적됨.
3. 이 세션 마운트는 `.git` 내부 파일 unlink 차단(EPERM) 특성이 있어 lock 잔존 시 rename 우회가 필요. raw_db 재집계·빌드·로컬 커밋은 정상.
