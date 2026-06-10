# action-calendar-sync 자동 실행 리포트 — 2026-06-11

실행 시각: 2026-06-11 06:05 KST (자동 스케줄, Cowork 샌드박스)
커밋: `405b3ce` chore(auto): action calendar sync 2026-06-11 06:05 KST [skip ci]

## 결과 요약

| 단계 | 상태 |
|---|---|
| git 락 정리 | ✅ index.lock / HEAD.lock 제거 (mv 우회) |
| 구글시트 CSV fetch (Chrome MCP) | ⚠️ Chrome MCP 미연결 → 브라우저 fetch 불가 |
| campaign_data.json 생성 | ⏭️ 기존 JSON 유지 (fallback 규칙) |
| campaign_performance.json 재집계 | ✅ raw_db 기반, 네트워크 불필요 |
| build.py 전체 리빌드 | ✅ index.html 1,831,438 bytes |
| git commit | ✅ (로컬) |
| git push origin main | ❌ 차단 (아래 참조) |

## 1단계: 구글시트 fetch — Chrome MCP 미연결

`list_connected_browsers` 결과 연결된 브라우저 0개 → 작업지시서의 Chrome MCP fetch 경로 사용 불가. 샌드박스 bash/curl의 docs.google.com 직접 접속도 프록시 정책상 차단됨 (`HTTP 403 from proxy after CONNECT`).

작업지시서 fallback 규칙("Chrome MCP 연결 안 될 때 → 기존 JSON 유지, campaign_performance.py만 실행")에 따라 **campaign_data.json은 기존본(events 225건, package_codes 보유 4건) 유지**. 더미 데이터 생성 금지 원칙 준수 — 불완전 CSV로 덮어쓰지 않음.

## 2단계: 파이썬 재집계

`generate_campaign_data.py`
- 미실행 (기존 JSON 유지). events 225건 / package_codes 보유 4건 (기존본 그대로)

`generate_campaign_performance.py` — **정상 실행 (raw_db 기반)**
- by_key: **1건** (Key=20), 누적 RN **4,625**, 누적 매출 **891.9백만** (ADR 192,837원)
- 매칭 행: **9,916** (예약 4,618 / 취소 5,298)
- raw_db 소스 일자: 20260610 (가용 최신본, 27.예약 / 28.취소 자료)
- 패키지코드 매핑: 123개 코드 → 1개 Key
- 전일 대비: RN 4,554 → 4,625 (+71), 매출 879.5 → 891.9백만 (+12.4)

## 3단계: 빌드 + 푸시

`build.py` — ✅ 전체 리빌드 완료
- index.html 1,831,438 bytes / otb.html 220,342 bytes
- 당일분석 교차검증 통과 (사업장별 NET=3,182 = 세그먼트합 3,182 · OTA 1,609 / G-OTA 1,560 / IB 13)
- 카테고리별 뉴스 348건, YoY 88개 사업장, 패키지 트렌드 14,409계열 (TOP 100 표시)
- db_aggregated.json.gz 생성 (86MB → 13.5MB), rm_fcst / overseas / data_freshness 동기화
- 빌드 스탬프: Auto-Built 2026-06-11 06:04 KST

git commit — ✅ 로컬 커밋 `405b3ce` (17 files, +4,511 / -4,464)

git push origin main — ❌ **차단됨**
- 원격: `ssh://git@ssh.github.com:443/cksals00-ai/gs_daily_trend_news_public_temp.git`
- 샌드박스 프록시가 GitHub egress 차단: SSH(ssh.github.com:443) 및 HTTPS(github.com:443) 모두 `403 Forbidden after CONNECT`
- 네트워크 정책 문제이므로 `pull --rebase` 재시도로 해결 불가
- **조치 필요: 호스트(Mac) 측 git 또는 LaunchAgent가 다음 실행 시 로컬 커밋을 푸시함. 또는 수동 `git push origin main` 1회 실행.** 현재 origin/main 대비 미푸시 커밋 8건 대기 중.

## 환경 특이사항

- 마운트된 `.git` 파일시스템이 **unlink(삭제) 금지**(`Operation not permitted`) — 모든 git 명령이 index.lock / HEAD.lock을 남김. lock을 `mv`로 격리하여 우회(커밋/레퍼런스 갱신은 rename 기반이라 정상 동작). 리포트 작성 시점 0바이트 lock 잔존은 다음 git 작업에 영향 없음.
- 커밋 메시지 시각은 `TZ=Asia/Seoul`로 KST 정상 표기.

## 체크포인트

- campaign_performance.json: by_key 1건 / 총 RN 4,625 ✅
- campaign_data.json: events 225건 (기존 유지) ✅
- 빌드 산출물 정상 생성 (index.html 1.83MB) ✅
- 로컬 커밋 완료 / 원격 푸시는 호스트 측 후속 처리 필요 ⚠️
