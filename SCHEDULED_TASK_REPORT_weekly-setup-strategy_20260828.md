# weekly-setup-strategy 실행 보고 — 2026-08-28 (금) 10:10 KST

## 요약
매주 토요일 구축리포트 + 전략대시보드/전략리포트 주간 업데이트. 콘텐츠 갱신·커밋 완료(`746ac59b`). **push는 샌드박스 SSH 키 부재로 로컬 커밋까지만 — 호스트 05:00 크롤 safe_push가 다음 실행 시 자동 반영.**

- 대상 저장소: `~/Projects/gs_daily_trend_news_public_temp` (Desktop 경로 없음 → Projects 폴백, 이미 마운트됨)
- 참고: 08-22(토) 회차는 트리거만 되고 산출물 미반영이어서(리포트 기준시각 08-15 유지), 이번 회차에서 **2주 치(08-16~08-28) 변동을 함께 반영**. 정규 토요일(08-29 06:03) 이전의 보정 실행.

## §1 구축리포트 (docs/gs-setup-report.html) — 완료
- `scripts/build_setup_timeline.py` 실행: 2026-05-21 이후 **65일 · 348건 커밋** 기준 §6 타임라인 자동 갱신 (06-10 소사업·PPT 기간라벨·RM FCST 엑셀 항목 포함 확인).
- 헤더 기준시각 2026-08-15 → **2026-08-28 10:05 KST**.
- KPI 스트립: 활성 스케줄 **17/30**(GS 정기 8 · 수기 2 · 타 프로젝트 7), 리포트 페이지 **64**(+decision-log, 08-20), 파이프라인 정상(host crawl 08-28 05:03 · exit 0 · **stale 없음**).
- §1 스케줄러 표 최신화: lia-kyoshin-check 등록 해제 반영, kis-balance-snapshot OFF 전환(08-18), 타 프로젝트 활성 7종(alfred 4종·lia 2종·lukalift) 반영, lia-s4 1회성 완료 반영.
- §1 "이번 주 관측(08-22~08-28)" 재작성 — host crawl 6건 정상 커밋, 주간 총 75커밋(daily update 50건). **지난 회차 주의신호 2건 모두 해소**: ① Daily Booking 원본 08-24 수신 재개(stale_sources 공백) ② daily update 커밋 부재 일시 현상 확인.
- §2 파이프라인 카드·최근 커밋 표를 08-28 기준으로 교체.

## §2 전략대시보드/전략리포트 — 완료
- **gs-strategy.html**: 런타임 fetch 완전 동적 페이지 — 데이터 파일이 호스트 크롤(08-28 05:02)로 최신이므로 수기 편집 불필요.
- **gs-strategy-report.html**:
  - §3 점검기간 08-22~08-28 갱신. ATTENTION 2건(Daily Booking stale·daily update 커밋 부재)을 **RESOLVED 로 전환**(카드 스타일 포함).
  - §4 변경이력 **v4.1(2026-08-16~08-28)** 신규 — 전 수치 실데이터 검증:
    - RM FCST(Revenue Meeting_2026.08.25 스냅샷·08-26 추출·06~10월 5개월): GT FCST 8월 274,820실/66,476백만, 9월 182,072실/37,274백만, 10월 209,342실/43,372백만 (`_validation.sum_property_grand` 기준)
    - Daily Booking(08-28): GT 온북 8월 273,311실(96.3%·YoY+4.1%) · 9월 124,542(62.2%) · 10월 97,174(40.0%) · 11월 30,541(15.7%)
    - 연간 누적(OTB 08-28): 온북 676,688실(달성 78.7%·YoY+19.5%) · 매출 149,328백만(82.0%·YoY+29.5%) · ADR 220,675원 · AI FCST 878,920실(102.2%) · 당일 순증 +1,752실
  - 점검 필요 유지: 기획전 패키지코드 4/224건 등록 정체(실측 재확인함).
- HTML 태그 밸런스 검증 완료(양 파일 div/li/tr 등 균형 0 오류, 구버전 날짜 잔존 0).

## §3 마감자료 주간리포트 탭 — 이미 존재(조치 불필요)
gs-closing-report.html에 `WEEKLY_REPORT_INJECT` 마커 기반 "주간 리포트" 탭 존재(weekly-report 스케줄러 관리 · 최신 주간 08.24~08.26 주입 확인).

## §4 빌드 + 푸시 — 부분 완료(환경 제약, 이전 회차와 동일)
- **build.py 스킵(의도적)**: 호스트가 오늘 05:02 동일 최신 데이터로 전체 빌드 완료 → 재빌드 무의미. 샌드박스에서 105MB 재빌드는 호출 시간 한도 초과로 반쪽 산출물 위험(08-01 회차 확인) → 콘텐츠 2파일만 갱신.
- **commit**: `746ac59b` (docs/gs-setup-report.html · docs/gs-strategy-report.html, +84/−54).
- **push 실패(예상됨)**: SSH 키·GITHUB_TOKEN 부재 → origin/main 대비 1 ahead. 호스트 host_daily_crawl.sh(05:00)의 safe_push가 자동 반영 예정. 별도 조치 불필요.
- FUSE unlink 불가로 git stale lock 은 `mv *.stale` 우회 처리(무해 · 호스트 git gc 정리 대상).

## 주의사항 준수
- gs-strategy/gs-strategy-report 관리자 전용 유지 — 외부 대시보드·GSN 네비 링크 노출 없음(네비 미수정).
- 더미 데이터 없음 — 전 수치 otb_data.json·rm_fcst.json·daily_booking.json·data_freshness.json·git log 실측.
