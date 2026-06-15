# 스케줄 작업 리포트 — rm-fcst-booking-update (2026-06-16 05:00 KST)

자동 실행 / 사용자 부재. 구현 세부사항은 자율 판단으로 진행.
대상 프로젝트: `~/Projects/gs_daily_trend_news_public_temp`

## 결론: 신규 소스 PDF 없음 → 재파싱·재생성·빌드·신규커밋 생략 (06-14, 06-15와 동일 상황)

RM FCST·Daily Booking 모두 **신규 입력 PDF가 없어** 숫자 갱신 불필요. 기존 리포트가 이미
최신 PDF 데이터를 반영 중이며, OTB는 오늘 05:01 KST host-crawl 작업이 이미 재생성 완료.
입력 변동 없이 재빌드 시 알려진 flip-flop churn(보고일 라벨 번복)만 재발하므로 4단계
(재생성·빌드)·6단계(신규 커밋)는 생략. (주의사항 "PDF 없으면 스킵", "더미 데이터 금지" 적용)

## 단계별 결과

| 단계 | 작업 | 결과 |
|------|------|------|
| 0 | git lock 점검 | ✅ 시작 시 lock 없음 (`.git/index.lock`, `.git/HEAD.lock` 모두 부재) |
| 1 | RM FCST 파싱 | ⏭️ 최신 RM PDF `Revenue Meeting_2026.06.08.pdf` 이미 파싱됨 (`rm_fcst.json._source_pdf` 일치, snapshot 2026.06.08). 신규 없음 → 스킵 |
| 2 | Daily Booking 파싱 | ⏭️ 최신 부킹 PDF `Daily Booking Report_2026.06.10`(루트)가 이미 반영됨. 신규 없음 → 스킵 |
| 3 | OTB/인사이트/기획전 재생성 | ⏭️ `otb_data.json`이 **오늘 05:01 KST**(host-crawl 작업) 이미 재생성됨 (baseDate 2026-06-16). 신규 입력 없음 → 중복 실행 생략 |
| 4 | 전체 빌드 | ⏭️ 입력 변동 없음 → 생략 (churn 방지) |
| 5 | 숫자 검증 | ✅ 두 JSON 유효성 통과. `daily_booking.json` Grand Total 일치 (아래) |
| 6 | 커밋·푸시 | ⚠️ 신규 데이터 없어 신규 커밋 안 함. 백로그 1커밋 푸시 시도 → **네트워크 차단 지속** |

## 데이터 신선도 (변동 없음, 실데이터)

- **RM FCST**: `Revenue Meeting_2026.06.08.pdf` (최신, 변동 없음) — 대상월 2026-06/07/08
- **Daily Booking**: `Daily Booking Report_2026.06.10.pdf` 반영
  - 6월 Grand Total 온북 **168,610 RN** / Budget 214,269 / 달성 78.7% / OCC 48.5% / 당일변동 ▲2,741
  - `months_detail[0].report_date = 2026-06-10` (정확) / `meta.report_date = 2026-06-15` (라벨만 상이 — 기존 flip-flop 이슈, **숫자는 동일**)
- **OTB**: `otb_data.json` refreshTime 2026-06-16 05:01 KST / baseDate 2026-06-16 (오늘 host-crawl이 온북 DB 기준 재생성)

## 신규 발견 사항

- **`data/Daily Booking Report PDF/Booking Status Report_2026.06.15.pdf`** (mtime 06-15 07:05) 신규 존재.
  단, **현재 파이프라인의 어떤 스크립트도 Booking Status Report PDF를 파싱하지 않음**
  (`grep "Booking Status Report" scripts/*.py` → 결과 없음; `update_daily_booking.sh`는 명시적으로 제외).
  투숙일별 OCC% 추출 로직은 task 5단계 주석대로 "세부 로직 확정 예정" 상태 → **숫자 영향 없음**.
  booking-status.html / otb 데이터는 Booking Status PDF가 아니라 온북 DB(`raw_db/27.*예약자료*.txt`) 기반.

## ⚠️ 조치 필요

1. **원격 푸시 차단 지속.** `git push origin main` 실패 — remote가
   `ssh://git@ssh.github.com:443/...` 이고 sandbox 프록시가 `ssh.github.com:443`을 Forbidden 처리.
   병합 충돌 아님, 네트워크 정책 문제. 로컬 `main`이 origin 대비 **1 커밋 앞섬**
   (오늘 05:02 host-crawl 커밋 `f2c5f36`). 사용자 PC에서 직접 푸시 권장:
   ```
   cd ~/Projects/gs_daily_trend_news_public_temp
   git push origin main
   ```
   (참고: 직전 17커밋 백로그는 그동안 정상 푸시되어 현재 1커밋만 잔여)

2. **신규 PDF 공급 경로.** 6/10(부킹)·6/8(RM) 이후 신규 RM/Daily Booking PDF 미공급.
   일일 최신화를 위해 `data/RM자료/`, `data/Daily Booking Report PDF/`(또는 루트)에
   매일 신규 PDF 공급 여부 확인 필요.

3. **parse_rm_fcst.py 성능.** `extract_same_period()`가 87페이지 PDF에 대해 pdfplumber
   `extract_text(layout=True)`를 전 페이지 호출 → 3분 이상 소요(본 sandbox 단일 실행 한도 초과).
   `[N월]` 요약 페이지에만 pdfplumber 적용하도록 최적화 권장(현재는 신규 PDF 없어 재파싱 불필요했음).

4. **parse 스크립트 glob 패턴.** `Daily_Booking_Report_*.pdf`(언더스코어)가 실제 공백 파일명과
   불일치 → 명시 경로로 우회 가능하나 파일명 통일 또는 패턴 보강 권장.

## 다음 실행 시 권장
- 신규 RM/Daily Booking PDF 유입 시 1~6단계 전체 정상 수행.
- 네트워크(SSH github) 복구 후 백로그(1커밋) 푸시 확인. HTTPS remote 전환도 검토 가치 있음.
- Booking Status Report PDF 파서 신설 시 5단계 투숙일별 OCC% 검증 로직 활성화.
