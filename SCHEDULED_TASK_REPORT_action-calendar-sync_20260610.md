# action-calendar-sync 자동 실행 리포트 — 2026-06-10

실행 시각: 2026-06-10 06:08 KST (자동 스케줄, Cowork 샌드박스)
커밋: `c48e528` chore(auto): action calendar sync ... [skip ci]

## 결과 요약

| 단계 | 상태 |
|---|---|
| git 락 정리 | ✅ index.lock / HEAD.lock 제거 (mv 우회) |
| 구글시트 CSV fetch (Chrome MCP) | ⚠️ Chrome MCP 미연결 → 브라우저 fetch 불가 |
| campaign_data.json 생성 | ⏭️ 기존 JSON 유지 (fallback 규칙) |
| campaign_performance.json 재집계 | ✅ raw_db 기반, 네트워크 불필요 |
| build.py 전체 리빌드 | ✅ index.html 1,631,823 bytes |
| git commit | ✅ (로컬) |
| git push origin main | ❌ 차단 (아래 참조) |

## 1단계: 구글시트 fetch — Chrome MCP 미연결

`list_connected_browsers` 결과 연결된 브라우저 0개 → 작업지시서의 Chrome MCP fetch 경로 사용 불가.

참고로 승인된 web_fetch 도구로 퍼블릭 published CSV(gid 1818134248) 접근은 성공했고, **기준일자 2026-06-10 최신본**이 확인됨. 다만 web_fetch는 결과를 파일로 저장하는 경로가 없고(65KB 분량은 표시 단계에서 잘림), 샌드박스 bash/python의 docs.google.com 직접 접속은 프록시 정책상 차단(`Tunnel connection failed: 403 Forbidden`)되어 전체 CSV를 캐시 파일로 안전하게 확보할 수 없었음.

작업지시서 fallback 규칙("Chrome MCP 연결 안 될 때 → 기존 JSON 유지, campaign_performance.py만 실행")에 따라 **campaign_data.json은 기존본(2026-06-08 생성, events 225건) 유지**. 더미 데이터 생성 금지 원칙 준수 — 불완전 CSV로 덮어쓰지 않음.

## 2단계: 파이썬 재집계

`generate_campaign_data.py`
- 미실행 (기존 JSON 유지). events 225건 / package_codes 보유 Key 4건 (기존본 그대로)

`generate_campaign_performance.py` — **정상 실행 (raw_db 기반)**
- by_key: **1건**, 누적 RN **4,554**, 누적 매출 **879.5백만**
- 매칭 행: 9,695 (예약 4,547 / 취소 5,148)
- raw_db 소스 일자: 20260609 (가용 최신본)
- 패키지코드 매핑: 123개 코드 → 1개 Key

## 3단계: 빌드 + 푸시

`build.py` — ✅ 전체 리빌드 완료
- index.html 1,631,823 bytes / otb.html 216,852 bytes
- 당일분석 교차검증 통과 (사업장별 NET=2839 = 세그먼트합 2839)
- 카테고리별 뉴스 287건, YoY 88개 사업장, 패키지 트렌드 14,409계열
- 빌드 스탬프: Auto-Built 2026-06-10 06:06 KST

git commit — ✅ 로컬 커밋 `c48e528` (22 files, +4229 / -4177)

git push origin main — ❌ **차단됨**
- 원격: `ssh://git@ssh.github.com:443/cksals00-ai/...`
- 샌드박스 프록시가 GitHub egress 차단: SSH(ssh.github.com:443) 및 HTTPS(github.com:443) 모두 `403 Forbidden after CONNECT`
- 네트워크 정책 문제이므로 `pull --rebase` 재시도로 해결 불가
- **조치 필요: 호스트(Mac) 측 git 또는 host_daily_crawl(LaunchAgent)이 다음 실행 시 로컬 커밋을 푸시함. 또는 수동 `git push origin main` 1회 실행.**

## 환경 특이사항

- 마운트된 `.git` 파일시스템이 **unlink(삭제) 금지**(`Operation not permitted`) — 모든 git 명령이 index.lock을 남김. 각 git 작업 직전 lock을 `mv`로 `.git/_lock_trash/`에 격리하여 우회. 커밋/레퍼런스 갱신은 rename 기반이라 정상 동작.
- 커밋 메시지 시각이 UTC(21:08)로 표기됨 — 샌드박스 TZ가 UTC라 `date` 출력이 KST가 아님(빌드 내부 스탬프는 06:06 KST로 정상). 라벨링상 경미한 불일치.

## 체크포인트

- campaign_performance.json: by_key 1건 / 총 RN 4,554 ✅
- campaign_data.json: events 225건 (기존 유지) ✅
- 빌드 산출물 정상 생성 ✅
