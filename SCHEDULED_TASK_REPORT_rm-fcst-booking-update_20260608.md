# 자동 실행 리포트 — rm-fcst-booking-update

실행 시각: 2026-06-08 05:08 KST (자동 스케줄)
프로젝트: ~/Desktop/gs_daily_trend_news_public_temp (→ ~/Projects/gs_daily_trend_news_public_temp 심볼릭 링크)

## 결과 요약

| 단계 | 상태 | 비고 |
|---|---|---|
| 0. git lock 정리 | ✅ | index.lock / HEAD.lock 정리 후 시작 (시작 시점 락 없음) |
| 1. RM FCST 파싱 | ✅ | 최신 PDF `Revenue Meeting_2026.06.01.pdf` (신규 RM PDF 없음, 전일과 동일) → rm_fcst.json (23사업장×4권역) |
| 2. Daily Booking 파싱 | ✅ | `Daily Booking Report_2026.06.05.pdf` → daily_booking.json (25사업장, 4개월). 명시 경로 인자 전달(기본 glob은 언더스코어 패턴이라 공백 파일명 미일치) |
| 3. OTB/인사이트/기획전 재생성 | ✅ | otb_data.json, enriched_notes.json, campaign_performance.json |
| 4. 전체 빌드 | ✅ | index.html, otb.html 재빌드 (Auto-Built 2026-06-08 05:07 KST). 내부 교차검증 통과 |
| 5. 숫자 검증 | ⚠️ | 단순 월합 비교는 측정범위 불일치로 무의미(정상) — 아래 참고. 의미 있는 검증(빌드 내부)은 통과 |
| 6. 커밋 | ✅ | `549ff07` 로컬 커밋 (16 files changed, +306/−237) |
| 7. 푸시 | ❌ | **샌드박스 네트워크 차단**(SSH 443 Forbidden). origin/main 대비 **ahead 15 / behind 0**(클린 fast-forward) → 호스트 자동푸시 시 **rebase 불필요** |

## RM FCST 검증 (월별) — 전일과 동일 (신규 RM PDF 없음)
- 2026-06: grand_rn=188,721 / grand_rev=34,906M / seg(O+G+I)_rn=70,885 / rev=14,070M
- 2026-07: grand_rn=253,987 / grand_rev=56,081M / seg(O+G+I)_rn=87,834 / rev=20,707M

## Daily Booking Report (Grand Total, 전채널 PMS 온북 RNs · 06.05자, 전일 동일)
| 투숙월 | 온북 RNs | Budget | 달성 | OCC | 당일변동 |
|---|---|---|---|---|---|
| 2026-06 | 157,878 | 214,269 | 73.7% | 45.5% | ▲2,427 |
| 2026-07 | 137,929 | 261,914 | 52.7% | 38.4% | ▲3,042 |
| 2026-08 | 90,909 | 283,784 | 32.0% | 25.3% | ▲3,243 |
| 2026-09 | 17,343 | 200,177 | 8.7% | 5.0% | ▲661 |

## OTB 재생성 결과
- 오늘 데이터 날짜: 20260604 / today_booking=4,854, today_cancel=1,984, **today_net=2,870**
- 총 목표 RNS=878,322 · 총 실적 RNS=367,809 · 달성률 41.9%
- AI FCST RN=823,337 (CI 748,368~898,306) · RM FCST RN=158,719
- Daily Booking 보정 대상: 25.팔라티움 해운대(온북 DB 미포함 사업장)

## 빌드 내부 교차검증 (통과)
- 당일분석 NET=2,870 = 세그먼트합(OTA 1,387 + G-OTA 1,276 + IB 207) 일치 ✅
- OTB 합계 = 세그먼트합 일치 ✅
- 신호등 4개 권역 갱신(vivaldi 5/central 7/south 7/apac 3), 경쟁사 9개, 뉴스 225건 주입
- YoY 사업장별 추이 88개, Daily Booking 섹션 25개 사업장, 패키지 트렌드 TOP 100 주입

## 숫자 검증 상세 (5단계 — 세부 로직 미확정)
SKILL 명시대로 5단계 세부 로직은 검증 프로세스 재검토 후 적용 예정. 단순 월합 비교(온북 DB stay-month vs Daily Booking Report 전채널 actual_rns)는 **측정 범위가 달라 의미가 없으며 오류가 아님**: 온북 DB는 온라인영업팀 채널(대매점/OTA/패키지) 부분집합, Daily Booking Report는 전 채널 PMS 전체 온북. 채널 매핑 기준 확정 후 5단계 재설계 필요. 의미 있는 검증(빌드 내부 NET=세그먼트합, OTB=세그합)은 모두 통과했으므로 빌드 산출물은 정상.

## 기획전 실적 재집계
- raw_db `_20260605` 반영: 예약 매칭 4,200행 + 취소 매칭 4,576행 = 총 8,776행
- 실적 적재: 1 Key / 누적 RN 4,206 / 누적 매출 812.4백만

## 참고
- RM PDF 신규 없음(최신 `Revenue Meeting_2026.06.01.pdf`). Daily Booking PDF는 06.05자(신규 미수신, 전일 동일) 반영. 더미 데이터 미사용.
- Booking Status Report PDF(투숙일별 OCC%) 전용 파서는 미배선 — OCC%는 generate_otb_data.py / daily_booking 파서 경로로 산출(기존과 동일).
- 샌드박스 git object 락(tmp_obj, HEAD.lock)은 unlink 권한 제약으로 잔존 경고가 뜨나 커밋 자체는 성공. 호스트 git이 자동 정리.

## 조치 필요
- **푸시**: 이번 커밋 `549ff07`은 origin 대비 **ahead 15 / behind 0**(클린 상태)이므로 호스트(launchd / `_auto_commit_push.command`)가 단순 fast-forward로 push 가능. rebase 불필요. 샌드박스에서는 GitHub SSH(443) 차단으로 직접 push 불가.
- 5단계 검증 로직: 채널 범위 정규화 후 재적용.
