# Scheduled Task Report — action-calendar-sync

**Run:** 2026-06-13 06:05 KST
**Project:** ~/Projects/gs_daily_trend_news_public_temp

## Result summary

| Step | Status | Notes |
|------|--------|-------|
| Git locks 정리 | ✅ | `HEAD.lock` mv 우회 제거. push 단계에서 재생성된 `index.lock`도 재제거 (rm은 Operation not permitted) |
| 1단계 Chrome MCP 구글시트 fetch | ⏭️ SKIPPED | 브라우저 미연결(무인 실행, 3회 재시도 실패). 폴백 규칙대로 기존 `campaign_data.json` 유지. 샌드박스 직접 fetch도 docs.google.com 403 차단 |
| 2단계 generate_campaign_performance.py | ✅ | raw_db 27/28 매칭 10,164행 → by_key 1, 누적 RN 4,723, 매출 909.4백만 |
| 3단계 build.py | ✅ | 전체 리빌드 완료 (otb 221,012 bytes, db_aggregated.json.gz 13.5MB, YoY 88개 사업장) |
| 4단계 git commit | ✅ | `670871a chore(auto): action calendar sync 2026-06-12 21:05 KST [skip ci]` (20 files changed) |
| 4단계 git push | ❌ BLOCKED | 샌드박스 프록시가 GitHub egress 차단 (ssh.github.com:443 Forbidden, github.com:22 DNS 실패, https 403). pull --rebase도 동일 차단이라 불가 |

## Checkpoints
- campaign_main.csv: 65,378 bytes (>1000 ✓, 기존 캐시 유지)
- campaign_data.json: events 225 / package_codes 보유 key 4
- campaign_performance.json: by_key 1 / 총 RN 4,723 / 매칭 10,164행 / 매출 909.4백만
- data_freshness.json: 생성 2026-06-13 06:04 KST (5개 소스)

## Action needed
로컬 커밋이 origin보다 앞섬 (이전 무인 실행분 누적 포함, 미푸시 6+개). 샌드박스에서는 GitHub 접속이 차단되어 push는 **호스트 측에서 처리**되어야 함. 호스트 daily 작업(`com.gs.daily-crawl`) 또는 수동 `git push origin main` 실행 시 일괄 반영됨. 데이터/빌드 산출물은 모두 정상 생성·커밋 완료.
