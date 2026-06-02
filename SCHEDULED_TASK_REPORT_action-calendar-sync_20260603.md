# action-calendar-sync 자동 실행 리포트 — 2026-06-03

실행 시각: 2026-06-03 06:04 KST (자동 스케줄)
커밋: `1de7b46` chore(auto): action calendar sync 2026-06-03 06:04 KST [skip ci]

## 결과 요약

| 단계 | 상태 |
|---|---|
| git 락 정리 | ✅ index.lock / HEAD.lock 없음 (정상) |
| 구글시트 CSV fetch (Chrome MCP) | ❌ Chrome 미연결 → 기존 JSON 유지 (폴백 규칙 적용) |
| campaign_data.json 생성 | ⏭ 건너뜀 (기존 유지) |
| campaign_performance.json 재집계 | ✅ raw_db 기반, 정상 |
| build.py 전체 리빌드 | ✅ |
| git commit | ✅ (로컬) |
| git push origin main | ❌ 차단 (아래 참조) |

## 1단계: 구글시트 fetch — 미실행

Chrome MCP(브라우저 확장)가 이 실행 환경에 **연결되어 있지 않음** (`list_connected_browsers` → 빈 목록, `tabs_context_mcp` 재시도 실패). 
주의사항 규칙대로 **기존 campaign_data.json을 그대로 유지**하고 시트 fetch는 건너뛰었습니다. 더미 데이터는 생성하지 않았습니다.

## 2단계: 파이썬 재집계 (raw_db 기반, 네트워크 불필요 → 정상 실행)

`generate_campaign_performance.py`
- 실적 적재 Key: **1건**
- 누적 RN: **3,836** (전일 3,743 → +93)
- 누적 매출: **742.8백만** (전일 727.3 → +15.5)
- 매칭 행: 7,863 (예약 3,830 / 취소 4,033)
- raw_db 소스 일자: **20260602** (가용 최신본, 전일 20260601 대비 갱신됨)

## 3단계: 빌드 — 정상

`build.py` 전체 리빌드 성공 · "Auto-Built 2026-06-03 06:04 KST"
- index.html (1,656,209 bytes), otb.html (215,061 bytes)
- db_aggregated.json · package_series_trend.json · rm_fcst.json · data_freshness.json 동기화
- 당일분석 교차검증 통과 (NET=2367, 세그먼트합=2367)
- 패키지 트렌드 14,409계열, YoY 사업장 88개, 카테고리 뉴스 299건

## ⚠ push 실패 — 사용자 조치 필요

`git push origin main` 이 **샌드박스 네트워크 정책으로 차단**되었습니다 (전일과 동일 사유).
- SSH: `ssh://git@ssh.github.com:443/...` → `CONNECT ssh.github.com:443: Forbidden`
- HTTPS: `github.com` → 프록시 `HTTP 403`

커밋·빌드 산출물은 로컬에 정상 보존되어 있고 작업 트리는 clean 합니다. 현재 로컬 `main` 이 `origin/main` 보다 **3 커밋 앞서** 있습니다. 보안 규칙상 네트워크 우회는 시도하지 않았습니다.

GitHub 접근이 가능한 환경(사용자 Mac 터미널)에서 아래 1회 실행이 필요합니다:

```
cd ~/Desktop/gs_daily_trend_news_public_temp
git push origin main
```

## 다음 자동 실행 개선 제안

구글시트 fetch가 동작하려면 06:00 스케줄 실행 시점에 **Chrome이 실행 + 확장 로그인 상태**여야 합니다. 또한 push 차단이 매일 반복되고 있어, GitHub 도달이 가능한 환경에서 스케줄을 돌리거나 별도 푸시 자동화가 필요합니다.
