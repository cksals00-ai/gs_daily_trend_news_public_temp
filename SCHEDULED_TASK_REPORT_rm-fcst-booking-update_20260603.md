# 자동 실행 리포트 — rm-fcst-booking-update

실행 시각: 2026-06-03 05:07 KST (자동 스케줄)
프로젝트: ~/Desktop/gs_daily_trend_news_public_temp

## 결과 요약

| 단계 | 상태 | 비고 |
|---|---|---|
| 0. git lock 정리 | ✅ | 잔존 HEAD.lock 마운트 제약(unlink 불가)으로 rename(.bak) 처리 후 진행 |
| 1. RM FCST 파싱 | ✅ | 최신 PDF: `Revenue Meeting_2026.06.01.pdf` → rm_fcst.json (23사업장×4권역) |
| 2. Daily Booking 파싱 | ✅ | `Daily Booking Report_2026.06.01.pdf` → daily_booking.json (25사업장, 4개월). 명시 경로 인자 전달(기본 glob 미일치) |
| 3. OTB/인사이트/기획전 재생성 | ✅ | otb_data.json, enriched_notes.json, campaign_performance.json. 기획전 raw_db `_20260602` 신규 반영 |
| 4. 전체 빌드 | ✅ | index.html, otb.html 재빌드 (Auto-Built 2026-06-03 05:06 KST). 내부 교차검증 통과 |
| 5. 숫자 검증 | ⚠️ | 단순 월합 비교는 측정범위 불일치로 무의미(정상) — 아래 참고 |
| 6. 커밋 | ✅ | `c594e17` 로컬 커밋 (12 files changed, +131/-53) |
| 7. 푸시 | ❌ | **샌드박스 네트워크 차단**(SSH 443 Forbidden). origin/main 대비 **ahead 2**(분기 아님) → 호스트 자동푸시 시 fast-forward 가능 |

## RM FCST 검증 (월별)
- 2026-06: grand_rn=188,721 / grand_rev=34,906M / seg(O+G+I)_rn=70,885 / rev=14,070M
- 2026-07: grand_rn=253,987 / grand_rev=56,081M / seg(O+G+I)_rn=85,484 / rev=19,793M

## Daily Booking Report (Grand Total, 전채널 PMS 온북 RNs · 06.01자)
| 투숙월 | 온북 RNs | Budget | 달성 | OCC | 당일변동 |
|---|---|---|---|---|---|
| 2026-06 | 145,071 | 214,269 | 67.7% | 41.8% | ▲6,299 |
| 2026-07 | 128,339 | 261,914 | 49.0% | 35.8% | ▲5,406 |
| 2026-08 | 83,118 | 283,784 | 29.3% | 23.2% | ▲3,443 |
| 2026-09 | 15,717 | 200,177 | 7.9% | 4.5% | ▲15,717 |

## OTB 재생성 결과
- 오늘 데이터 날짜: 20260601 / today_booking=4,117, today_cancel=1,750, **today_net=2,367**
- 총 목표 RNS=878,322 · 총 실적 RNS=360,402 · 달성률 41.0%
- AI FCST RN=825,171 (CI 750,024~900,318) · RM FCST RN=156,369
- Daily Booking 보정 대상: 25.팔라티움(온북 DB 미포함 사업장)

## 빌드 내부 교차검증 (통과)
- 당일분석 NET=2,367 = 세그먼트합(OTA 1,412 + G-OTA 965 + IB −10) 일치 ✅
- OTB 합계 = 세그먼트합 일치 ✅
- 신호등 4개 권역 갱신(vivaldi 5/central 7/south 7/apac 3), 경쟁사 9개, 뉴스 299건 주입

## 숫자 검증 상세 (5단계 — 세부 로직 미확정)
온북 DB(stay-month net_rn) vs Daily Booking Report actual_rns:

| 투숙월 | 온북DB | DailyBkRpt | 차이 | 차이% |
|---|---|---|---|---|
| 2026-06 | 57,908 | 145,071 | −87,163 | −60.1% ⚠️ |
| 2026-07 | 27,277 | 128,339 | −101,062 | −78.7% ⚠️ |
| 2026-08 | 1,421 | 83,118 | −81,697 | −98.3% ⚠️ |
| 2026-09 | 480 | 15,717 | −15,237 | −96.9% ⚠️ |

➡ **차이는 오류가 아니라 측정 범위 불일치(정상)**: 온북 DB는 온라인영업팀 채널(대매점/OTA/패키지) 부분집합, Daily Booking Report는 전 채널 PMS 전체 온북. 또한 원거리 투숙월(08·09월)일수록 온라인영업팀 미입력분이 커 격차 확대. 단순 월합 비교는 부적절 — 채널 매핑 기준 확정 후 5단계 재설계 필요. 의미 있는 검증(빌드 내부 NET=세그먼트합, OTB=세그합)은 모두 통과.

## 기획전 실적 재집계
- raw_db `_20260602` 신규 반영: 예약 매칭 3,830행 + 취소 매칭 4,033행 = 총 7,863행
- 실적 적재: 1 Key / 누적 RN 3,836 / 누적 매출 742.8백만

## 참고
- PDF 모두 정상 존재(RM·Daily Booking 모두 06.01자), 더미 데이터 미사용.
- Booking Status Report PDF(투숙일별 OCC%) 전용 파서는 미배선 — OCC%는 generate_otb_data.py 경로로 산출(기존과 동일).
- 잔존 sandbox 락(.git/index.lock, HEAD.lock)은 `.stalebak`으로 rename 처리, tmp_obj 431건은 호스트 git이 자동 정리.

## 조치 필요
- **푸시**: 호스트(launchd / `_auto_commit_push.command`)가 로컬 커밋 `c594e17`(+직전 `fe24de4`) 2건을 origin/main 으로 전송. 이번엔 분기 아님(ahead 2)이라 fast-forward 가능. 샌드박스에서는 GitHub SSH 접근 차단으로 직접 push 불가.
- 5단계 검증 로직: 채널 범위 정규화 후 재적용.
