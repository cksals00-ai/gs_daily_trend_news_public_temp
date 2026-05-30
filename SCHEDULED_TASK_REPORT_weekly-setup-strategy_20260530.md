# 자동 실행 리포트 — weekly-setup-strategy

실행 시각: 2026-05-30 06:05 KST (자동 스케줄, 매주 토 06:00)
프로젝트: ~/Desktop/gs_daily_trend_news_public_temp

## 결과 요약

| 단계 | 상태 | 비고 |
|---|---|---|
| 1. 구축리포트 (gs-setup-report.html) | ✅ | 스케줄러·파이프라인·신선도·이슈 2026-05-30 기준 최신화 |
| 2. 전략대시보드 (gs-strategy.html) | ✅ | 전 구간 데이터 드리븐(런타임 fetch) — 최신 JSON 반영, 빌드 재생성 |
| 3. 전략리포트 (gs-strategy-report.html) | ✅ | 변경이력 v3.0 추가 · 점검기간 갱신 · CRITICAL→RESOLVED |
| 4. 마감자료 주간리포트 탭 | ✅ | 이미 존재 (periodType=weekly · switchPeriod 연동 확인) — 추가 작업 없음 |
| 5. 빌드 + 커밋 | ✅ | build.py 정상 (Auto-Built 06:11 KST) · 커밋 완료 |
| 6. 푸시 | ❌ | 샌드박스 GitHub 차단 (ssh 443 Forbidden). 호스트 맥에서 일괄 push 필요 |

## 구축리포트 주요 변경
- 활성 스케줄 8/15 → **9/17** (yanolja-dashboard-monitor 활성 추가, chrome-mcp-tool-approval 1회성 완료 편입)
- 스케줄러 last/next 실행시각 전 항목 2026-05-30 기준 갱신
- 데이터 신선도: db_aggregated 06:02 · otb_data 05:06 · rm_fcst 05:04(Revenue Meeting_2026.05.26.pdf) · 뉴스 317건
- 자동 커밋 체인: 정상→**커밋 정상·푸시 차단**으로 정정 (origin/main 대비 미푸시)
- 자동화 커버리지 ~78% → ~83%
- 알려진 이슈: exports CSV 정리 완료, 경쟁사 크롤링 정상 가동, git push 차단 지속(2026-05-22~30)

## 전략 리포트/대시보드
- gs-strategy.html, gs-strategy-report.html 모두 런타임에 docs/data/*.json 을 fetch → 빌드로 데이터 동기화 시 자동 반영
- 5월 온북 192,864실(달성 89.6%·OCC 53.6%), 6월 132,257실(달성 61.7%) — RM FCST 05.26 스냅샷
- 변경이력 v3.0(2026-05-26~30) 추가, "기획전 실적 지연" CRITICAL 알림을 RESOLVED 로 갱신

## 커밋 현황 (origin/main 대비 3 ahead · 미푸시)
- `a909354` rm-fcst + booking update 2026-05-30 05:09
- `8a9b526` action calendar sync 2026-05-30 06:10 (gs-setup-report.html 94줄 변경 포함)
- `4380030` weekly setup+strategy update 2026-05-30

## 조치 필요
- **푸시**: 호스트 맥에서 `_auto_commit_push.command` 실행하여 3개 커밋 origin/main 전송
- 관리자 전용 페이지(gs-strategy·gs-strategy-report·gs-setup-report) GSN 공개 네비 미노출 유지 — 변경 없음

## 주의: 더미 데이터 미사용, 실제 PDF/JSON 데이터만 반영.
