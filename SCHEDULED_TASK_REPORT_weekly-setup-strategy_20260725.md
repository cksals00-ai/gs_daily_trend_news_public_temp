# 자동 실행 리포트 — weekly-setup-strategy

실행 시각: 2026-07-25 06:12 KST (자동 스케줄, 매주 토 06:00)
프로젝트: ~/Projects/gs_daily_trend_news_public_temp

## 결과 요약

| 단계 | 상태 | 비고 |
|---|---|---|
| 1. 구축리포트 (gs-setup-report.html) | ✅ | 기준시각·KPI·자동커밋체인·신선도·최근커밋·타임라인·§6 콜아웃 2026-07-25 최신화 + `build_setup_timeline.py` 자동 갱신(05-21 이후 43일·259건) |
| 2. 전략대시보드 (gs-strategy.html) | ✅ | 전 구간 런타임 데이터 드리븐 — `docs/data/*.json` fetch로 자동 반영(별도 수정 불필요), fetch 로직 정상 확인 |
| 3. 전략리포트 (gs-strategy-report.html) | ✅ | 변경이력 **v3.8 (07-21~25)** 신규 추가 · 점검기간 07-21~25 갱신 · RESOLVED 신선도 7/25·RM 07.21 스냅샷 갱신 |
| 4. 마감자료 주간리포트 탭 | ✅ | 이미 존재 (`buildWeeklyTab`·`WEEKLY_REPORT_HTML` 주입 확인) — 추가 작업 없음 |
| 5. 빌드 (build.py) | ⏭️ | 의도적 생략 — 호스트 완전빌드(05:02)가 데이터 산출물 최신화 완료, 샌드박스 build.py는 45초 제한 + gz 압축 절단 리스크. 본 태스크 대상(admin 정적 2종)은 build.py 미재생성 파일이라 영향 없음 |
| 6. 커밋 | ✅ | `a934c704` 2 files (+32 −18) — gs-setup-report·gs-strategy-report만 커밋 |
| 7. 푸시 | ❌ | 샌드박스 GitHub SSH 차단(Host key verification failed) — origin 대비 1 ahead. 호스트 `host_daily_crawl.sh`(05:00, git 충돌 자동해소 내장)가 다음 실행 시 전담 push (영구 제약) |

## 구축리포트 주요 변경 (gs-setup-report.html)
- 기준 시각 2026-07-24 08:55 → **2026-07-25 06:08 KST**, 구축 타임라인 07-24 → **07-25**
- KPI **활성 13 / 24** (정기 활성 11 · 수기(간트) 2 · 비활성 6 · 1회성 완료 5) — 실측 스케줄러 기준 갱신(localift-push-watcher·gyosin-auto-poll·datalab-refresh 활성). 최근 자동 커밋 07-25 05:02
- 데이터 신선도: db_aggregated 07-25 05:02 · otb_data 07-25 05:01 · daily_booking report_date **07-24**·25개 사업장·**7/8/9/10월** (**7월 GT 온북 244,339실 · 달성 93.3%** · 8월 221,092실 77.9% · 9월 58,625실 29.3%) · rm_fcst **Revenue Meeting_2026.07.21.pdf 스냅샷**(6/7/8/9월·07-22 추출, 7월 FCST 250,614실/57,373백만) · 뉴스 **315건(신규 43** · 07-25 05:00)
- 자동 커밋 체인 카드·최근 커밋 테이블 07-23~25 갱신(host crawl 07-25 05:02, daily-booking 244,339, special-period OCC/픽업 증감·stale db 대조검증 수정)
- §6 타임라인: `build_setup_timeline.py`가 AUTO 블록 07-20~24 자동 재생성, 헤더 범위·⭐콜아웃(07-06~07-25) 및 dim 라인(07-24~25 데일리 갱신 중심) 갱신

## 전략 리포트/대시보드
- gs-strategy.html · gs-strategy-report.html 모두 런타임 `docs/data/*.json` fetch → 빌드 데이터 동기화 시 자동 반영(데이터는 호스트 05:02 완전빌드로 최신)
- 변경이력 **v3.8 (2026-07-21~25)** 추가:
  - RM FCST 07.21 스냅샷 — Grand Total 7월 250,614실/57,373백만, 8월 271,992실/66,798백만, 9월 188,856실/38,447백만 (6월 192,111실/35,149백만)
  - Daily Booking 07-24 GT 온북 7월 244,339실(93.3%·YoY +3.2%) · 8월 221,092실(77.9%·YoY -15.8%) · 9월 58,625실(29.3%·YoY -66.0%) · 10월 47,419실(19.5%·YoY -80.0%)
  - 연간 누적 온북 584,830실(Budget 859,709·달성 68.0%·YoY +21.3%) · 매출 127,919백만원(70.3%·YoY +32.0%) · ADR 218,728원(YoY +8.8%) · AI FCST 892,691실(103.8%) · 당일 순증 +3,055실
  - 신규 구축: 스페셜/연휴 전략상품 팔로업(데일리 증감·사업장 OCC), 방문 현황 실시간 뷰어(방문로그 1개월 트라이얼), 부킹/RM PDF host crawl 자동 반영 전환
- 자동화 로직 점검 섹션 점검기간 07-05~11 → **07-21~25**, RESOLVED 신선도 7/25·RM 07.21 스냅샷 갱신

## 검증 결과
- HTML 구조 정합: gs-setup-report 1,172 태그·gs-strategy-report 887 태그 파서 OK · 변경이력 v3.8 1건 정상 삽입 ✅
- 관리자 전용 페이지 GSN 네비 미노출: **gs-strategy.html(대시보드)·gs-map·strategy-keyin·admin 모두 공개 GSN 네비 부재 확인** ✅ (index.html은 미수정, 호스트 완전빌드 유지)
- 데이터 무결성: otb_data·rm_fcst·daily_booking JSON 파싱 OK, 실제 데이터만 사용(RM 07.21 PDF·온북 07-24 스냅샷·뉴스 315건), 더미 미생성 ✅
- 2026-06-09/10 소사업 자동화 구축내역 타임라인 보존 확인(콜아웃 dim 라인 + 타임라인 행) ✅

## 조치 필요 (호스트 맥)
- **푸시**: 샌드박스 SSH 차단으로 미푸시 1건(`a934c704`, origin/main 대비 1 ahead). 호스트 `host_daily_crawl.sh`(05:00 launchd, git 충돌 자동해소 내장) 다음 실행 시 자동 push, 또는 `_auto_commit_push.command` 수동 실행.
- **참고(확인 요망)**: 공개 GSN 네비에 "전략 리포트"(gs-strategy-report.html, `data-gsn="strategy"`) 항목이 노출돼 있음. 금지 대상인 gs-strategy.html(원본 대시보드)·gs-map·strategy-keyin·admin은 미노출이며, gs-strategy-report는 수주간 의도적으로 노출돼 온 큐레이션 리포트로 판단(GSN_HIDDEN_MENUS 미포함)해 변경하지 않음. 관리자 전용으로 숨겨야 한다면 `docs/js/menu-visibility.js`의 `GSN_HIDDEN_MENUS`에 추가 필요.

## ⚠ 환경 제약 메모 (반복 발생)
- 샌드박스 bash는 45초 후 종료 + FUSE 마운트가 unlink 차단(Operation not permitted)이라 git 락 정리는 rename(mv)으로 우회. build.py 완전 실행/gz 압축은 호스트 전담(일일빌드로 커버). 본 주간 태스크의 핵심은 build.py가 재생성하지 않는 admin 정적 리포트 2종 갱신이며 정상 반영됨. GitHub push도 호스트 전담(SSH 허용목록 제약).
