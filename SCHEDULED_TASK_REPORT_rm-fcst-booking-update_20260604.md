# 자동 실행 리포트 — rm-fcst-booking-update

실행 시각: 2026-06-04 05:07 KST (자동 스케줄)
프로젝트: ~/Desktop/gs_daily_trend_news_public_temp

## 결과 요약

| 단계 | 상태 | 비고 |
|---|---|---|
| 0. git lock 정리 | ✅ | index.lock / HEAD.lock 잔존 없음 (정상 시작) |
| 1. RM FCST 파싱 | ✅ | 최신 PDF: `Revenue Meeting_2026.06.01.pdf` (신규 RM PDF 없음, 전일과 동일) → rm_fcst.json (23사업장×4권역) |
| 2. Daily Booking 파싱 | ✅ | `Daily Booking Report_2026.06.02.pdf` (신규) → daily_booking.json (25사업장, 4개월). 명시 경로 인자 전달(기본 glob 미일치) |
| 3. OTB/인사이트/기획전 재생성 | ✅ | otb_data.json, enriched_notes.json, campaign_performance.json. 기획전 raw_db `_20260603` 신규 반영 |
| 4. 전체 빌드 | ✅ | index.html, otb.html 재빌드 (Auto-Built 2026-06-04 05:06 KST). 내부 교차검증 통과 |
| 5. 숫자 검증 | ⚠️ | 단순 월합 비교는 측정범위 불일치로 무의미(정상) — 아래 참고 |
| 6. 커밋 | ✅ | `68ce277` 로컬 커밋 (16 files changed, +6,037/-5,970) |
| 7. 푸시 | ❌ | **샌드박스 네트워크 차단**(SSH 443 Forbidden). origin/main 대비 **ahead 3, behind 1**(분기) → 호스트 자동푸시 시 **pull --rebase 필요**(fast-forward 불가) |

## RM FCST 검증 (월별) — 전일과 동일 (신규 RM PDF 없음)
- 2026-06: grand_rn=188,721 / grand_rev=34,906M / seg(O+G+I)_rn=70,885 / rev=14,070M
- 2026-07: grand_rn=253,987 / grand_rev=56,081M / seg(O+G+I)_rn=85,484 / rev=19,793M

## Daily Booking Report (Grand Total, 전채널 PMS 온북 RNs · 06.02자)
| 투숙월 | 온북 RNs | Budget | 달성 | OCC | 당일변동 | 전일(06.01) 대비 |
|---|---|---|---|---|---|---|
| 2026-06 | 149,625 | 214,269 | 69.8% | 43.1% | ▲4,554 | +4,554 |
| 2026-07 | 131,076 | 261,914 | 50.0% | 36.5% | ▲2,737 | +2,737 |
| 2026-08 | 84,906 | 283,784 | 29.9% | 23.7% | ▲1,788 | +1,788 |
| 2026-09 | 16,214 | 200,177 | 8.1% | 4.7% | ▲497 | +497 |

## OTB 재생성 결과
- 오늘 데이터 날짜: 20260602 / today_booking=199, today_cancel=65, **today_net=134**
- 총 목표 RNS=878,322 · 총 실적 RNS=360,712 · 달성률 41.1% (전일 41.0%)
- AI FCST RN=823,423 (CI 748,459~898,387) · RM FCST RN=156,369
- Daily Booking 보정 대상: 25.팔라티움(온북 DB 미포함 사업장)

## 빌드 내부 교차검증 (통과)
- 당일분석 NET=134 = 세그먼트합(OTA 80 + G-OTA 54 + IB 0) 일치 ✅
- OTB 합계 = 세그먼트합 일치 ✅
- 신호등 4개 권역 갱신(vivaldi 5/central 7/south 7/apac 3), 경쟁사 9개, 뉴스 306건 주입

## 숫자 검증 상세 (5단계 — 세부 로직 미확정)
온북 DB(stay-month net_rn) vs Daily Booking Report actual_rns:

| 투숙월 | 온북DB | DailyBkRpt | 차이 | 차이% |
|---|---|---|---|---|
| 2026-06 | 57,908 | 149,625 | −91,717 | −61.3% ⚠️ |
| 2026-07 | 27,277 | 131,076 | −103,799 | −79.2% ⚠️ |
| 2026-08 | 1,421 | 84,906 | −83,485 | −98.3% ⚠️ |
| 2026-09 | 480 | 16,214 | −15,734 | −97.0% ⚠️ |

➡ **차이는 오류가 아니라 측정 범위 불일치(정상)**: 온북 DB는 온라인영업팀 채널(대매점/OTA/패키지) 부분집합, Daily Booking Report는 전 채널 PMS 전체 온북. 원거리 투숙월(08·09월)일수록 온라인영업팀 미입력분이 커 격차 확대. 단순 월합 비교는 부적절 — 채널 매핑 기준 확정 후 5단계 재설계 필요. 의미 있는 검증(빌드 내부 NET=세그먼트합, OTB=세그합)은 모두 통과.

## 기획전 실적 재집계
- raw_db `_20260603` 신규 반영: 예약 매칭 3,964행 + 취소 매칭 4,240행 = 총 8,204행
- 실적 적재: 1 Key / 누적 RN 3,970 / 누적 매출 766.6백만

## 참고
- RM PDF는 신규 없음(최신 `Revenue Meeting_2026.06.01.pdf` 전일과 동일). Booking Status / Daily Booking PDF는 06.02자 신규 반영. 더미 데이터 미사용.
- Booking Status Report PDF(투숙일별 OCC%) 전용 파서는 미배선 — OCC%는 generate_otb_data.py 경로로 산출(기존과 동일).
- 샌드박스 git object 락(tmp_obj, HEAD.lock, index.lock)은 unlink 권한 제약으로 잔존하나 커밋 자체는 성공. 호스트 git이 자동 정리.

## 조치 필요
- **푸시**: 이번 커밋 `68ce277`은 origin 대비 **ahead 3 / behind 1**(분기 상태)이므로 호스트(launchd / `_auto_commit_push.command`)가 단순 fast-forward로 보낼 수 없음. **`git pull --rebase origin main` 후 push** 필요. 샌드박스에서는 GitHub SSH 차단으로 직접 push·pull 모두 불가.
- 5단계 검증 로직: 채널 범위 정규화 후 재적용.
