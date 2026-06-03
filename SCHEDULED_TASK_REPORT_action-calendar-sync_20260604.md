# action-calendar-sync 자동 실행 리포트 — 2026-06-04

실행 시각: 2026-06-04 06:04 KST (자동)
커밋: `141af3f` chore(auto): action calendar sync 2026-06-04 06:04 KST [skip ci]

## 결과 요약

| 단계 | 결과 |
|---|---|
| git lock 정리 | ✅ index.lock / HEAD.lock 제거 |
| 1단계: Chrome MCP 구글시트 fetch | ⏭ 미실행 (브라우저 미연결) |
| 2단계: generate_campaign_performance.py | ✅ 실적 재집계 |
| 3단계: build.py | ✅ 전체 리빌드 |
| git commit | ✅ (로컬) |
| git push origin main | ❌ 차단 (아래 참조) |

## 1단계: 구글시트 fetch — 미실행

Chrome MCP(브라우저 확장)가 연결되어 있지 않아(`list_connected_browsers` → 빈 목록) 구글시트 CSV fetch를 수행하지 못했습니다. 주의사항 규정대로 **기존 campaign_data.json을 유지**하고, raw_db 기반 실적 재집계만 진행했습니다. 보안 규칙상 sandbox에서 docs.google.com 직접 접속·네트워크 우회는 시도하지 않았습니다.

- 기존 campaign_data.json: events 225개, package_codes 보유 key 4개 (유지)

## 2단계: 실적 재집계 — 정상

`generate_campaign_performance.py` 가 raw_db 27/28 txt에서 패키지코드 매칭으로 실적을 재집계했습니다.
- 패키지코드 매핑: 123개 코드 → 1개 Key
- 예약자료 매칭 3,964행 / 취소자료 매칭 4,240행 / 총 매칭 8,204행
- campaign_performance.json: by_key 1건 / 누적 RN 3,970 / 누적 매출 766.6백만

## 3단계: 빌드 — 정상

`build.py` 전체 HTML 리빌드 완료.
- index.html 1,671,367 bytes
- otb.html 215,074 bytes (YoY 88개 사업장)
- db_aggregated.json / package_series_trend.json / rm_fcst.json / data_freshness.json 동기화
- Auto-Built 2026-06-04 06:04 KST

## ⚠ push 실패 — 사용자 조치 필요

`git push origin main` 이 **샌드박스 네트워크 정책으로 차단**되었습니다 (전일들과 동일 사유).
- 원격: `ssh://git@ssh.github.com:443/cksals00-ai/gs_daily_trend_news_public_temp.git`
- SSH: `CONNECT ssh.github.com:443: Forbidden`
- 대체 시도(github.com:22): `Temporary failure in name resolution`

커밋·빌드 산출물은 로컬에 정상 보존, 작업 트리 clean. 현재 로컬 `main` 이 `origin/main` 보다 **5 커밋 앞서** 있습니다. 보안 규칙상 네트워크 우회는 시도하지 않았습니다.

GitHub 접근이 가능한 환경(사용자 Mac 터미널)에서 1회 실행 필요:

```
cd ~/Desktop/gs_daily_trend_news_public_temp
git push origin main
```

## 체크포인트

- campaign_main.csv: 65,378 chars (기존 캐시 유지)
- campaign_data.json: events 225, package_codes key 4 (유지)
- campaign_performance.json: by_key 1, 총 RN 3,970

## 개선 제안

1. 구글시트 fetch가 동작하려면 06:00 실행 시점에 **Chrome 실행 + 확장 로그인** 상태여야 함. 현재 4일 연속 브라우저 미연결로 fetch 스킵 중.
2. push 차단이 매일 반복됨 — GitHub 도달 가능 환경에서 스케줄을 돌리거나 별도 푸시 자동화 필요.
