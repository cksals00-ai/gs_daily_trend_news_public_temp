# 스케줄 작업 리포트 — rm-fcst-booking-update (2026-06-15 05:00 KST)

자동 실행 / 사용자 부재. 구현 세부사항은 자율 판단으로 진행.
대상 프로젝트: `~/Projects/gs_daily_trend_news_public_temp`

## 결론: 신규 소스 PDF 없음 → 재생성·빌드·커밋 생략 (06-14과 동일 상황)

RM FCST·Daily Booking 모두 **신규 입력 PDF가 없어** 숫자 갱신 불필요. 기존 리포트가
이미 최신 PDF 데이터를 반영 중이며, 재빌드 시 알려진 flip-flop churn(보고일·월범위 번복)만
재발하므로 4단계(재생성·빌드)·6단계(커밋·푸시 신규)는 생략. (주의사항 "PDF 없으면 스킵",
"더미 데이터 금지" 적용)

## 단계별 결과

| 단계 | 작업 | 결과 |
|------|------|------|
| 0 | git lock 점검 | 시작 시 없음 → 진행 중 git이 빈 `.git/index.lock` 생성, 마운트 권한으로 sandbox에서 삭제 불가 (아래 조치 필요) |
| 1 | RM FCST 파싱 | ⏭️ 최신 RM PDF `Revenue Meeting_2026.06.08.pdf`가 이미 파싱됨 (`rm_fcst.json._source_pdf=Revenue Meeting_2026.06.08.pdf`, snapshot 2026.06.08). 신규 없음 → 스킵 |
| 2 | Daily Booking 파싱 | ⏭️ 최신 부킹 PDF `2026.06.10`(루트)가 이미 반영됨. 신규 없음 → 스킵 |
| 3·4 | OTB/인사이트/기획전 재생성 + 전체 빌드 | ⏭️ 입력 변동 없음 → 생략 (churn 방지) |
| 5 | 숫자 검증 | ✅ 두 JSON 유효성 통과. `daily_booking.json` Grand Total 일치 (아래) |
| 6 | 커밋·푸시 | ⚠️ 신규 데이터 없어 신규 커밋 안 함. 기존 백로그 푸시 시도 → **네트워크 차단 지속** |

## 데이터 신선도 (변동 없음, 실데이터)

- **RM FCST**: `Revenue Meeting_2026.06.08.pdf` (최신, 변동 없음)
- **Daily Booking**: 2026.06.10 PDF 반영
  - 6월 Grand Total 온북 **168,610 RN** / Budget 214,269 / 달성 78.7% / OCC 48.5% / 당일변동 ▲2,741
  - `months_detail[].report_date = 2026-06-10` (정확) / `meta.report_date = 2026-06-11` (라벨만 +1일 — 기존 flip-flop 이슈, 숫자는 동일)

## ⚠️ 조치 필요

1. **원격 푸시 차단 지속.** `git push origin main` 실패 — sandbox 프록시가
   `ssh.github.com:443`(Forbidden) 차단. 병합 충돌 아님, 네트워크 정책 문제.
   로컬 `main`이 origin 대비 **17 커밋 앞섬**(이전 미푸시 누적). 사용자 PC에서 직접 푸시 권장:
   ```
   cd ~/Projects/gs_daily_trend_news_public_temp
   rm -f .git/index.lock .git/HEAD.lock
   git push origin main
   ```

2. **빈 `.git/index.lock` 잔존.** git 읽기 작업 중 생성된 0바이트 lock을 sandbox에서
   삭제 불가("Operation not permitted", 마운트 제약). 위 명령의 `rm -f`로 함께 정리됨.
   현재 git 읽기는 정상 동작하나, 다음 커밋 전 제거 필요.

3. **신규 PDF 공급 경로.** 6/10(부킹)·6/8(RM) 이후 신규 PDF 미공급(주말 영향 추정).
   일일 최신화를 위해 `data/RM자료/`, `data/Daily Booking Report PDF/`(또는 루트)에
   매일 신규 PDF 공급 여부 확인 필요.

## 다음 실행 시 권장
- 신규 PDF 유입 시 1~6단계 전체 정상 수행.
- 네트워크 복구 후 백로그(17커밋) 일괄 푸시 확인.
- parse 스크립트 glob 패턴(`Daily_Booking_Report_*.pdf`, 언더스코어)이 공백 파일명과
  불일치 → 파일명 통일 또는 패턴 보강 권장(현재는 명시 경로로 우회 가능).
