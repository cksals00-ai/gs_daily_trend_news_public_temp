# 스케줄 작업 리포트 — rm-fcst-booking-update

- 실행 일시: 2026-06-11 05:18 KST (자동 실행)
- 프로젝트: gs_daily_trend_news_public_temp
- 커밋: `f607a11` chore(auto): rm-fcst + booking update 2026-06-11 05:18 KST [skip ci]

## 단계별 결과

| 단계 | 스크립트/작업 | 결과 |
|------|---------------|------|
| 1 | git lock 정리 | ✅ 락 없음 |
| 2 | parse_rm_fcst.py (RM FCST) | ⏭️ 최신 PDF(Revenue Meeting_2026.06.08)가 이미 파싱됨. 신규 RM PDF 없음 → 데이터 최신 상태 유지 |
| 3 | parse_daily_booking.py (Daily Booking) | ⏭️ 최신 부킹 PDF(2026.06.10) 이미 파싱됨(daily_booking.json report_date 2026-06-10). 금일 신규 PDF 없음 |
| 4a | generate_otb_data.py | ✅ 6초, rc=0 (실적 RNS 445,219 / 달성률 50.5%) |
| 4b | generate_insights.py | ✅ 1초, rc=0 |
| 4c | generate_campaign_performance.py | ✅ 10초, rc=0 (누적 RN 4,625 / 매출 891.9백만) |
| 5 | build.py (전체 빌드) | ✅ 35초, rc=0. index.html·otb.html 재빌드, 데이터 동기화 완료 (Auto-Built 2026-06-11 05:16 KST) |
| 6 | 숫자 검증 | ✅ 빌드 내부 검증 통과 (seg_match true, passed true). 상세 비교는 아래 참고 |
| 7 | git commit + push | ⚠️ 커밋 성공 / **푸시 실패 (네트워크 차단)** |

## 숫자 검증 메모

- 빌드 자체 검증(daily_analysis_validation.json): `seg_match: true, passed: true` (2026-06-11 05:16 KST 기준) — 통과.
- Daily Booking Report(PDF) Grand Total 온북 vs 우리 온북 파싱 비교:
  - PDF 6월 Grand Total 온북: **157,878 RN** (전 채널 합계)
  - 우리 온북(db_aggregated net, 온라인영업팀=대매점·OTA·패키지 한정): **69,702 RN**
  - 두 수치는 **집계 범위가 다름**(전체 채널 vs 온라인 채널 서브셋)이라 단순 5% 비교는 부적절. 작업 정의의 "5단계 세부 로직 내일 확정" 항목과 일치하므로 오경보 미발생 처리.
- 더미 데이터 없음 — 모든 수치 실데이터 기반.

## ⚠️ 조치 필요: 원격 푸시 차단

- `git push origin main` 실패: 샌드박스 프록시가 `ssh.github.com:443`(Forbidden) 및 `https://github.com`(HTTP 403)을 모두 차단.
- `git pull --rebase`도 동일 사유로 차단됨 (병합 충돌이 아닌 네트워크 정책 문제).
- 현재 로컬 `main`이 origin 대비 **7 커밋 앞섬** (이전 스케줄 실행분도 미푸시 누적).
- 추가로 `.git/index.lock` 잔여 파일이 생성되어 후속 커밋이 막힘. 샌드박스에서 마운트 권한 제약으로 삭제 불가(Operation not permitted).
- 이 리포트 파일(`SCHEDULED_TASK_REPORT_..._20260611.md`)은 폴더에 저장됐으나 위 락 때문에 커밋되지 못함(스테이징 상태).
- **사용자 직접 조치 필요**: 본인 PC 터미널에서 아래 실행 권장.
  ```
  cd ~/Desktop/gs_daily_trend_news_public_temp
  rm -f .git/index.lock .git/HEAD.lock
  git add -A && git commit -m "docs(auto): scheduled task report 2026-06-11"
  git push origin main
  ```

## 비고

- 단일 PDF 파싱(parse_rm_fcst, parse_daily_booking)은 실행 환경의 45초 단일 호출 제한을 초과함. 다만 최신 PDF가 이미 직전 실행(6/10)에서 파싱되어 있어 재파싱 불필요(동일 결과). 신규 PDF 입수 시 별도 처리 필요.
