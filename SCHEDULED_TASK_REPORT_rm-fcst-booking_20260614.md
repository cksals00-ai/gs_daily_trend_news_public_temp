# RM FCST + Daily Booking 자동 업데이트 리포트 — 2026-06-14

실행 시각: 2026-06-14 (KST, 자동 실행)
대상 프로젝트: gs_daily_trend_news_public_temp

## 결론: 신규 소스 PDF 없음 → 데이터 재생성/커밋 생략

오늘은 RM FCST·Daily Booking 모두 **신규 입력 PDF가 없어** 리포트 숫자를 갱신할 필요가 없었습니다.
기존 리포트가 이미 최신 PDF 데이터를 반영하고 있어, 재빌드 시 오히려 표시 날짜가 과거로
되돌아가는 churn만 발생하므로 4단계(재생성·빌드)와 6단계(커밋·푸시)를 생략했습니다.
(주의사항 "PDF 파일이 없으면 해당 단계 스킵하고 이유 리포트", "더미 데이터 절대 금지" 적용)

## 단계별 결과

### 0. 사전 정리
- `.git/index.lock`, `.git/HEAD.lock` 점검 → 없음(정상).

### 1. RM FCST 파싱
- 최신 RM PDF: `data/RM자료/Revenue Meeting_2026.06.08.pdf` (6/8자, 최신).
- 기존 `docs/data/rm_fcst.json` 이 이미 동일 PDF(`_source_pdf = Revenue Meeting_2026.06.08.pdf`,
  `_snapshot_date = 2026.06.08`)로 생성되어 있어 **이미 최신 상태**.
- 신규 RM PDF 없음 → 재파싱은 동일 결과만 생성하므로 생략. JSON 유효성 확인 완료.

### 2. Daily Booking 파싱 / 5. 숫자 검증
- 리포지토리 내 최신 Booking PDF: `Daily Booking Report_2026.06.10.pdf`,
  `Booking Status Report_2026.06.10.pdf` (둘 다 6/10에 커밋된 파일, 신규 업로드 아님).
- 현재 라이브 리포트(`docs/data/daily_booking.json`)와 위 6/10 PDF를 **전수 비교**:
  - 26개 사업장 × 3개월 × (actual_rns / budget_rns / ly_actual) = **312개 필드 검증 → 차이 0건**.
  - 예) Grand Total 온북(actual_rns): 6월 168,610 / 7월 152,108 / 8월 101,913 — PDF·JSON 완전 일치.
- 즉, 현재 리포트가 이미 6/10 PDF 데이터를 그대로 반영 중. 5% 초과 오차 없음(오차 0%).
- 신규 Booking PDF 없음 → 재파싱·빌드 생략.

### 3·4. OTB/인사이트/기획전 재생성 및 전체 빌드
- 입력 데이터(RM·Booking) 변동 없음 → 생략.

### 6. 커밋·푸시
- 데이터 변경 없음 → 커밋·푸시 생략.
- (참고) 작업 트리에 타 작업의 미커밋 변경이 존재하여 `git add -A` 일괄 커밋은
  의도치 않게 다른 작업물을 포함할 수 있어 수행하지 않음:
  `_host_crawl_status.json`(M), 미추적 리포트 .md 2건, `scripts/build_weekly_report_html.py`.

## 발견된 이슈 (확인 권장)

1. **report_date 라벨 ±1일 불일치 (데이터는 동일).**
   현재 `daily_booking.json`의 `report_date`는 `2026-06-11`로 표기되어 있으나, 실제 숫자는
   `Daily Booking Report_2026.06.10.pdf`와 100% 동일합니다. 다른 파이프라인('daily update')이
   같은 PDF를 파싱하며 보고일을 +1일(생성일 기준)로 기록한 것으로 보입니다.

2. **파이프라인 간 상호 덮어쓰기(flip-flop).**
   `rm-fcst+booking` 작업은 6/10 PDF 기준 `report_date=2026-06-10` + 4개월(6~9월)을 생성하고,
   `action-calendar-sync`/`daily update` 작업은 `report_date=2026-06-11` + 3개월(6~8월)을 생성하여
   커밋마다 두 값이 번갈아 나타나고 있습니다(6/12 커밋 이력에서 확인). 두 작업이 동일 파일을
   서로 다른 형식으로 갱신하므로, 보고일 산정·월 범위 로직을 한쪽으로 통일하는 것을 권장합니다.

3. **신규 PDF 공급 경로.**
   6/11 이후 신규 Booking/RM PDF가 리포지토리에 추가되지 않았습니다(주말 영향 추정).
   일일 최신화를 위해서는 매일 신규 PDF가 지정 폴더(`data/Daily Booking Report PDF/`,
   `data/RM자료/`)에 공급되는지 확인이 필요합니다.

## 다음 실행 시 권장
- 신규 PDF가 들어오면 정상적으로 1~6단계 전체 수행.
- RM 파싱(pdfplumber same-period 추출)이 대용량 PDF에서 수 분 소요됨 → 타임아웃 여유 확보.
