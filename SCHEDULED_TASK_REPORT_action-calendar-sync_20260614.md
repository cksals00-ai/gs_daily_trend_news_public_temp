# Scheduled Task Report — action-calendar-sync

**Run:** 2026-06-14 06:04 KST
**Project:** ~/Projects/gs_daily_trend_news_public_temp

## Result summary

| Step | Status | Notes |
|------|--------|-------|
| Git locks 정리 | ✅ | `index.lock` / `HEAD.lock` 없음. push 후 재생성된 `index.lock`은 rm 불가(Operation not permitted)이나 동작에 영향 없음 |
| 1단계 Chrome MCP 구글시트 fetch | ⏭️ SKIPPED | 브라우저 미연결 (`list_connected_browsers` = [], 2회 재시도 실패). 폴백 규칙대로 기존 `campaign_data.json` 유지. 샌드박스 직접 fetch도 docs.google.com 403 Forbidden 차단 |
| 2단계 generate_campaign_performance.py | ✅ | raw_db 27/28 txt 매칭 10,164행 → by_key 1, 누적 RN 4,723, 매출 909.4백만 |
| 3단계 build.py | ✅ | 전체 리빌드 완료 (index 1,598,084 bytes, otb 221,134 bytes, db_aggregated.json.gz 13.5MB, YoY 88개 사업장, 뉴스 265건/8카테고리). Auto-Built 2026-06-14 06:04 KST |
| 4단계 git commit | ✅ | `a90d6ba chore(auto): action calendar sync 2026-06-14 06:04 KST [skip ci]` (21 files changed, +4945 / -4268) |
| 4단계 git push | ❌ BLOCKED | 샌드박스 프록시가 GitHub egress 전면 차단 (ssh.github.com:443 Forbidden, https github.com 403 from proxy). pull --rebase도 동일 차단이라 불가 |

## Checkpoints
- campaign_main.csv: 65,378 bytes (>1000 ✓, 기존 캐시 유지)
- campaign_data.json: events 225 / package_codes 보유 key 4 (기존 유지)
- campaign_performance.json: by_key 1 (key '20') / 총 RN 4,723 / 매칭 10,164행 / 매출 909.4백만
- data_freshness.json: 빌드 로그상 5개 소스 생성

## Action needed
로컬 `main`이 origin보다 **16개 앞섬** (이전 무인 실행분 누적 포함). 샌드박스에서는 GitHub 접속이 SSH·HTTPS 모두 차단되어 push 불가 — push는 **호스트 측에서 처리**되어야 함. 호스트 daily 작업 또는 수동 `git push origin main` 실행 시 일괄 반영됨. 데이터/빌드 산출물은 모두 정상 생성·커밋 완료.

## Notes (자율 판단)
- Chrome MCP 무인 연결 불가 → 기획전 시트 동기화는 건너뛰고 기존 JSON 유지 (더미 데이터 미생성, 규칙 준수)
- 실적 재집계(raw_db 기반)는 네트워크 불필요하므로 정상 실행
- 빌드 산출물은 최신 실적 반영 완료, 커밋까지 무인으로 처리됨
