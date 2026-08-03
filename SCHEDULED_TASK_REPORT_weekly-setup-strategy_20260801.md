# weekly-setup-strategy 실행 보고 — 2026-08-01 (토) 06:04 KST

## 요약
매주 토요일 구축리포트 + 전략대시보드/전략리포트 주간 업데이트 자동 실행. 콘텐츠 갱신·커밋 완료. **push는 샌드박스 환경 제약(SSH 키·GITHUB_TOKEN 부재)으로 로컬 커밋까지만 완료 → 호스트 05:00 크롤이 다음 실행 시 자동 push 예정.**

- 대상 저장소: `~/Projects/gs_daily_trend_news_public_temp` (Desktop 사본은 비어 있어 Projects 경로 사용 — 태스크 폴백 규칙대로)
- 커밋: `d4b22e07` chore(auto): weekly setup+strategy update 2026-08-01 [skip ci] (origin/main 대비 1 ahead)

## §1 구축리포트 (docs/gs-setup-report.html) — 완료
- `scripts/build_setup_timeline.py` 실행: 2026-05-21 이후 48일·271건 커밋 기준 §6 타임라인 자동 갱신 (07-31 GSN 아웃바운드 링크, 07-30 예상매출 PPT 대상월 객실 산출 등 반영).
- 헤더 기준시각 2026-07-25 → **2026-08-01 06:06 KST**, 구축 타임라인 날짜 동기화.
- KPI 스트립: 활성 스케줄 11/22(정기 9·수기 2), 리포트 페이지 60(권한제한 47), 최근 자동 커밋 2026-08-01 05:02로 최신화.
- §1 "이번 주 관측(07-25~08-01)" 재작성 — 정기 태스크 전종 정상 가동, host crawl 이번 주 매일 정상 커밋(8건), 데일리 갱신 73건, 7월 마감(monthly-closing) 08-01 00:19 완료.

## §2 전략대시보드/전략리포트 — 완료
- **gs-strategy.html**: 런타임에 otb_data.json/db_aggregated.json을 fetch하는 완전 동적 페이지 → 데이터 파일이 이미 호스트 크롤(08-01 05:01)로 최신 → 수기 편집 불필요(자동 최신 반영).
- **gs-strategy-report.html**:
  - §3 자동화 로직 점검: 점검기간 07-25~08-01, 데이터 신선도 문구를 실측치로 갱신(db_aggregated 08-01 05:01, RM FCST Revenue Meeting_2026.07.28 스냅샷 6~09월 커버, Daily Booking 07-30 GT 온북 248,455실 달성 94.9%).
  - §4 변경이력: **v3.9(2026-07-26~08-01)** 신규 항목 추가 — 전 수치 실데이터 검증:
    - RM FCST(07.28 스냅샷) GT FCST 7월 248,690실/56,978백만, 8월 271,586실/66,258백만, 9월 186,370실/37,939백만
    - Daily Booking(07-30) GT 온북 7월 248,455(94.9%·YoY+5.0%)·8월 232,506(81.9%·-11.4%)·9월 67,274(33.6%·-61.0%)·10월 53,194(21.9%·-77.6%)
    - 연간 누적(OTB 08-01) 온북 604,425실(달성 70.3%·YoY+21.3%)·매출 133,134백만(73.1%·+31.9%)·ADR 220,265원·AI FCST 888,051실(103.3%)
- HTML 태그 밸런스 검증 완료(div/li 균형 0, 잔여 구버전 날짜 0).

## §3 마감자료 주간리포트 탭 — 이미 존재(조치 불필요)
gs-closing-report.html에 `WEEKLY_REPORT_INJECT` 마커 기반 "주간 리포트" 탭이 이미 존재(weekly-report 스케줄러가 관리). 추가 작업 없음.

## §4 빌드 + 푸시 — 부분 완료(환경 제약)
- **build.py**: 데이터 처리량(db_aggregated 100MB)으로 실행 시간이 샌드박스 호출당 45초 한도를 초과, 호출 경계에서 프로세스가 종료되어 완주 불가. 부분 실행이 남긴 산출물(index.html·otb_data.json·weekly_comparison.json·admin_suggestions.json·daily_analysis_validation.json)은 **HEAD(오늘 05:02 호스트 빌드본, 동일 최신 데이터) 기준으로 원복** — 반쪽 빌드 커밋 방지. 데이터 신선도 손실 없음(동일 08-01 데이터).
- **commit**: 의도한 콘텐츠 2파일만 스테이징·커밋 완료(`d4b22e07`).
- **push 실패**: 원격이 SSH(ssh.github.com:443)인데 샌드박스에 SSH 키·GITHUB_TOKEN 부재 → `Permission denied (publickey)`. 로컬 커밋은 호스트 저장소와 동일 파일시스템에 존재하므로, **호스트 host_daily_crawl.sh(05:00, SSH 키 보유)의 다음 실행 safe_push가 자동 반영**. 별도 조치 불필요.

## 환경 메모(다음 실행 개선 참고)
- 샌드박스 FUSE 마운트가 `unlink`를 불허(`rename`은 허용). git이 남기는 stale `.lock`(index.lock/HEAD.lock)을 `rm` 불가 → `mv`로 우회 처리함. 잔여 `*.stale`/tmp_obj/dangling 오브젝트는 무해(호스트 git gc가 정리).
- 완전 자동 push까지 원하면: (a) 샌드박스에 read-only 배포용 토큰/키 주입 또는 (b) build+push를 호스트 파이프라인에 위임(현재도 사실상 호스트가 전담).

## 주의사항 준수
- gs-strategy / gs-strategy-report는 관리자 전용 — 외부 대시보드 링크 노출 없음(수정 없음).
- 실데이터만 사용(더미 없음). 모든 수치 rm_fcst.json·daily_booking.json·otb_data.json 실측 추출.
