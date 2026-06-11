# 스케줄 작업 리포트 — rm-fcst-booking-update (2026-06-12 05:00 KST)

자동 실행 / 사용자 부재. 구현 세부사항은 자율 판단으로 진행.

## 실행 요약

| 단계 | 스크립트/작업 | 결과 |
|------|---------------|------|
| 1 | git lock 정리 | ✅ index.lock / HEAD.lock 정리 |
| 2 | parse_rm_fcst.py (RM FCST) | ⏭️ 최신 RM PDF(Revenue Meeting_2026.06.08)가 이미 파싱됨(rm_fcst.json `_source_pdf` = 06.08). 신규 RM PDF 없음 → 재파싱 불필요 |
| 3 | parse_daily_booking.py (Daily Booking) | ✅ 최신 부킹 PDF(2026.06.10) **명시 경로로 재파싱**. (자동 glob 패턴이 `Daily_Booking_Report_*.pdf`(언더스코어)라 공백 파일명을 못 잡아, 직접 경로 지정으로 처리) → daily_booking.json `report_date` 2026-06-05 → **2026-06-10** 갱신 |
| 4a | generate_otb_data.py | ✅ rc=0. 목표 RNS 878,322 / 실적 449,458 / 달성률 51.2% |
| 4b | generate_insights.py | ✅ rc=0. DB 인사이트 3건 (6월 온북 71,320RN, 전월비 ▼9.0%, 전년동월비 ▼12.6%) |
| 4c | generate_campaign_performance.py | ✅ rc=0. 누적 RN 4,723 / 매출 909.4백만 |
| 5 | build.py (전체 빌드) | ✅ rc=0. index.html(1,745,846B)·otb.html(220,770B) 재빌드. **Auto-Built 2026-06-12 05:09 KST** |
| 6 | 숫자 검증 | ✅ 빌드 내부 검증 통과 (seg_match true, passed true) |
| 7 | git commit | ✅ 커밋 성공 (`8cc2ba8`, 20 files, +6,618 / −6,605) |
| 7 | git push | ⚠️ **푸시 실패 — 네트워크 차단** (SSH Forbidden / HTTPS 403 proxy) |

## 데이터 신선도

- **RM FCST**: Revenue Meeting_2026.06.08 (최신, 변동 없음)
- **Daily Booking**: 2026.06.10 PDF 신규 반영 (직전 데이터는 06.05 기준이었음 → 06.10으로 최신화)
  - 6월 온북 168,610 RN (Budget 214,269, 달성 78.7%, OCC 48.5%)
  - 7월 152,108 RN (달성 58.1%) / 8월 101,913 RN / 9월 19,130 RN
- 더미 데이터 없음 — 전부 실데이터 기반.

## 숫자 검증 메모 (5단계)

- 빌드 내부 교차검증(daily_analysis_validation.json): `prop_net=3176, seg_total=3176 (OTA=1713, G-OTA=1464, IB=-1), seg_match: true, passed: true` (2026-06-12 05:09 KST).
- Daily Booking Report(PDF) Grand Total 온북 vs 우리 온북 비교:
  - PDF 6월 Grand Total 온북: **168,610 RN** (전 채널 합계)
  - 우리 온북: 온라인영업팀(대매점·OTA·패키지) 한정 서브셋 → **집계 범위가 달라** 단순 5% 비교 부적절. (작업 정의의 "5단계 세부 로직 추후 확정" 항목과 일치, 오경보 미발생 처리)

## ⚠️ 조치 필요: 원격 푸시 차단 (지속)

- `git push origin main` 실패: 샌드박스 프록시가 `ssh.github.com:443`(Forbidden) 및 `https://github.com`(HTTP 403) 모두 차단. 이전 실행과 동일한 네트워크 정책 문제 (병합 충돌 아님).
- 현재 로컬 `main`이 마지막 확인된 origin 대비 **3 커밋 앞섬** (이번 분 포함, 이전 미푸시 누적분 일부 포함).
- `.git/objects` tmp 파일 unlink 시 "Operation not permitted" 경고 발생(마운트 권한 제약) — 단, 커밋 자체는 정상 완료됨.
- **사용자 직접 조치 권장** (본인 PC 터미널):
  ```
  cd ~/Desktop/gs_daily_trend_news_public_temp   # 또는 ~/Projects/gs_daily_trend_news_public_temp
  rm -f .git/index.lock .git/HEAD.lock
  git push origin main
  ```

## 비고

- 프로젝트 실제 경로: `~/Projects/gs_daily_trend_news_public_temp` (작업 정의의 `~/Desktop/...` 경로에는 폴더 없음 → Projects 경로에서 실행).
- 최신 부킹/Status PDF(06.10)는 프로젝트 루트에 위치. 자동 glob이 공백 파일명을 못 잡으므로, 향후 자동화를 위해 파일명을 언더스코어로 통일하거나 glob 패턴에 공백 변형 추가 권장.
