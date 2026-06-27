# 자동 실행 리포트 — weekly-setup-strategy

실행 시각: 2026-06-27 06:05 KST (자동 스케줄, 매주 토 06:00)
프로젝트: ~/Desktop/gs_daily_trend_news_public_temp

## 결과 요약

| 단계 | 상태 | 비고 |
|---|---|---|
| 1. 구축리포트 (gs-setup-report.html) | ✅ | 기준시각·KPI·스케줄러·신선도·이슈·타임라인 2026-06-27 최신화 + 금주 핵심 구축 콜아웃(06-20~27) 교체 |
| 2. 전략대시보드 (gs-strategy.html) | ✅ | 전 구간 런타임 데이터 드리븐 — docs/data/*.json fetch로 자동 반영 (별도 수정 불필요) |
| 3. 전략리포트 (gs-strategy-report.html) | ✅ | 변경이력 v3.4 추가 · 점검기간 06-21~27 갱신 · RESOLVED 신선도 6/27·RM 06.23 스냅샷 갱신 |
| 4. 마감자료 주간리포트 탭 | ✅ | 이미 존재 (buildWeeklyTab · WEEKLY_REPORT_HTML 주입 확인) — 추가 작업 없음 |
| 5. 빌드 (build.py) | ⚠️ | 샌드박스 45초 호출 제한으로 gz 압축 단계에서 중단 — 부분빌드 산출물은 호스트 완전빌드(05:02) 버전으로 원복, 무결성 보존 |
| 6. 커밋 | ✅ | `953ecee` 2 files (+76 −52) — gs-setup-report·gs-strategy-report만 커밋 |
| 7. 푸시 | ❌ | 샌드박스 GitHub 차단 (SSH 443 Forbidden) — 호스트 `host_daily_crawl.sh`(05:00)/`_auto_commit_push.command`가 전담 (영구 제약) |

## 구축리포트 주요 변경 (gs-setup-report.html)
- 기준 시각 2026-06-20 → **2026-06-27 06:05 KST**, 구축 타임라인 06-27 (`build_setup_timeline.py` 자동 갱신: 05-21 이후 24일·159건 커밋)
- 활성 스케줄 **9 / 20** (활성 9 · 비활성 6 · 1회성 완료 5) — **tf-meeting-calendar-sync(매시간 TF 미팅 캘린더 동기화) 신규 활성** 행 추가, 전 active 태스크 last/next 06-26~27 기준 갱신, 일별 검증 4종 스케줄 라벨 08:0x로 정정(검증 전용 명시)
- 리포트 페이지 52 → **56** (KPI·§3 노트 갱신)
- 데이터 신선도: db_aggregated 06-26 20:02 · otb_data 06-27 05:01 · daily_booking report_date 06-26·25개 사업장·6/7/8/9월 (**6월 GT 온북 189,253실 · Budget 214,269 · 달성 88.3% · YoY +5.5%**) · **rm_fcst Revenue Meeting_2026.06.23.pdf 스냅샷(6월 Grand 192,111실/35,148백만)** · 뉴스 360건(신규 69)
- §2 자동 커밋 체인: host crawl 06-27 05:02 · origin/main 0 ahead 동기화 / 최근 커밋 테이블 06-25~27 갱신
- §5 git push 이슈: 06-18 호스트 충돌 자동해소 내장 후 0 ahead 동기화로 상태 갱신, 경쟁사 크롤 freshness 06-27 갱신
- **§6 금주 핵심 구축 콜아웃 교체(06-20~27)**: 세일즈 KPI 리포트(sales-kpi)·6·7·8월 픽업 분석(pickup-678) 신규 · 품의 AI 변환 다중시즌 자동채움 · 7·8월 RM 자동확장+7월 뷰 통일(safe_push) · 마감월 freeze 가드(freeze_closed_months.py)
- **2026-06-09/06-10 소사업 全자동 체인(소사업_예상매출 자동화 + PPT 기간라벨 동적화 + RM FCST 정리 엑셀 당월 재생성, 커밋 77f2702)**: 타임라인 06-09/06-10 행 및 콜아웃 dim 라인에 보존 확인

## 전략 리포트/대시보드
- gs-strategy.html · gs-strategy-report.html 모두 런타임 docs/data/*.json fetch → 빌드 데이터 동기화 시 자동 반영
- 변경이력 **v3.4 (2026-06-21~27)** 추가:
  - RM FCST 06.23 스냅샷 — **6월 192,111실/35,148백만원, 7월 258,669실/57,843백만원, 8월 276,625실/66,410백만원**(Grand Total)
  - Daily Booking 06-26 GT 온북 6월 189,253실(달성 88.3%·YoY +5.5%) · 7월 211,075실(80.6%) · 8월 143,283실(50.5%)
  - 연간 누적 온북 500,507실(Budget 859,709·달성 58.2%·YoY +25.2%) · 매출 105,265백만원(57.8%·YoY +35.8%) · ADR 210,317원(YoY +8.5%) · AI FCST 947,187실(110.2%)
  - 세일즈 KPI·픽업 분석 신규, 7·8월 RM 자동확장, 품의 다중시즌 자동채움, 마감월 freeze 가드, tf-meeting-calendar-sync 신규 활성
- 자동화 로직 점검 섹션 점검기간 06-14~20 → **06-21~27**, RESOLVED 신선도 6/27·RM 06.23 스냅샷 갱신

## 빌드/커밋 상세
- `python3 scripts/build.py` 실행: patch_channel_daily→freeze→generate_otb_data→weekly_comparison→index.html(Auto-Built 06-27 06:11)→otb.html→sales-kpi→db_aggregated 동기화까지 진행했으나, **샌드박스 bash 호출 45초 제한**으로 db_aggregated.json.gz(87MB→gzip -9) 압축 단계에서 프로세스가 중단됨(gz 5.3MB 절단). gz 이후 단계(fcst_trend·inbound_enriched·rm_fcst_excel·overseas·menu-visibility 주입)도 미실행.
- **안전 조치**: 부분빌드로 절단된 gz와 menu-visibility 미주입 index/otb는 nav 노출 리스크가 있어, 빌드 산출물 전체(index·otb·sales-kpi·otb_data·db_aggregated.json.gz·weekly_comparison·daily_analysis_validation·admin_suggestions·admin_input)를 **호스트 완전빌드(05:02 커밋) 버전으로 원복**(FUSE unlink 차단 → `git show HEAD:path > path` 인플레이스 복원). gz 무결성 재확인(13,669,549 bytes, gzip -t OK).
- 커밋 `953ecee chore(auto): weekly setup+strategy update 2026-06-27 [skip ci]` — **gs-setup-report.html·gs-strategy-report.html 2개만 커밋**(데이터 산출물은 호스트 일일빌드가 이미 05:02에 완전빌드·배포). HEAD.lock 경쟁은 인플레이스 우회.

## 조치 필요 (호스트 맥)
- **푸시**: 샌드박스 GitHub 차단(SSH 443 Forbidden)으로 미푸시 1건(origin/main 대비 1 ahead). 호스트 `host_daily_crawl.sh`(05:00 launchd, git 충돌 자동해소 내장) 다음 실행 시 자동 push, 또는 `_auto_commit_push.command` 수동 실행.

## 검증 결과
- 관리자 전용 페이지 공개 네비 미노출: 호스트 완전빌드 index/otb 유지로 menu-visibility 주입 보존 — gs-strategy / gs-map / gs-setup-report / admin 네비 노출 리스크 차단 ✅
- HTML 구조 정합: gs-setup-report `<section>` 6/6 균형 · gs-strategy-report 변경이력 v3.4 정상 삽입 ✅
- 데이터 무결성: otb_data·weekly_comparison JSON 파싱 OK · db_aggregated.json.gz gzip -t OK(13.6MB) ✅
- 실제 데이터만 사용(RM FCST 06.23 PDF·온북 JSON·뉴스 360건), 더미 데이터 미생성 ✅
- 2026-06-09/10 소사업 자동화 구축내역 타임라인 보존 확인 ✅

## ⚠ 환경 제약 메모 (반복 발생)
- 샌드박스 bash 호출은 45초 후 종료되며, foreground 프로세스는 워크스페이스에 남아 수 분간 더 실행되나 gz 압축 같은 단일 장시간 연산 중 reap됨. nohup/백그라운드는 호출 종료 시 PID 네임스페이스(bwrap --die-with-parent --unshare-pid)와 함께 즉시 종료. → **build.py 완전 실행은 호스트가 전담**(이미 일일빌드로 커버). 본 주간 태스크는 admin 리포트 2종 갱신이 핵심이며 이는 build.py가 재생성하지 않는 정적 파일이라 정상 반영됨.
