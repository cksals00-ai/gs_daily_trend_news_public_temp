# 자동 실행 리포트 — rm-fcst-booking-update

실행 시각: 2026-05-30 05:09 KST (자동 스케줄)
프로젝트: ~/Desktop/gs_daily_trend_news_public_temp

## 결과 요약

| 단계 | 상태 | 비고 |
|---|---|---|
| 0. git lock 정리 | ✅ | 잔존 `index.lock`은 마운트 제약(rm 불가)으로 rename 처리 |
| 1. RM FCST 파싱 | ✅ | 최신 PDF: `Revenue Meeting_2026.05.26.pdf` → rm_fcst.json (23사업장×4권역) |
| 2. Daily Booking 파싱 | ✅ | `Daily Booking Report_2026.05.26.pdf` → daily_booking.json (25사업장, 3개월) |
| 3. OTB/인사이트/기획전 재생성 | ✅ | otb_data.json, enriched_notes.json, campaign_performance.json |
| 4. 전체 빌드 | ✅ | index.html, otb.html 외 재빌드 완료 (Auto-Built 2026-05-30 05:06 KST) |
| 5. 숫자 검증 | ⚠️ | 아래 참고 — 단순 월합 비교는 범위 불일치로 무의미 |
| 6. 커밋 | ✅ | `a909354` 로컬 커밋 (origin/main 대비 1 ahead) |
| 7. 푸시 | ❌ | **샌드박스 네트워크에서 GitHub 차단** (SSH 443 Forbidden / HTTPS 403). 호스트 푸시 필요 |

## RM FCST 검증 (월별)
- 2026-05: grand_rn=196,035 / grand_rev=38,441M / seg(O+G+I)_rn=65,041 / rev=14,321M
- 2026-06: grand_rn=189,853 / grand_rev=35,342M / seg(O+G+I)_rn=70,812 / rev=14,094M

## Daily Booking Report (Grand Total, 온북 RNs)
- 2026-05: 192,864 (Budget 215,280, 달성 89.6%, OCC 53.6%, 당일변동 +2,021)
- 2026-06: 132,257 (Budget 214,269, 달성 61.7%, OCC 38.0%, 당일변동 +6,187)
- 2026-07: 56,885 (Budget 261,914, 달성 21.7%, OCC 15.8%, 당일변동 +3,440)

## 숫자 검증 상세 (5단계 — 세부 로직 미확정)
온북 DB(raw 27/28 예약·취소자료) 월합 vs Daily Booking Report actual_rns 비교:

| 투숙월 | 온북DB | DailyBkRpt | 차이 | 차이% |
|---|---|---|---|---|
| 2026-05 | 78,393 | 192,864 | -114,471 | -59.4% |
| 2026-06 | 52,603 | 132,257 | -79,654 | -60.2% |
| 2026-07 | 25,144 | 56,885 | -31,741 | -55.8% |

➡ **차이 원인은 오류가 아니라 측정 범위 불일치**: 온북 DB는 온라인영업팀 채널(대매점/OTA/패키지) 부분집합이고, Daily Booking Report는 전 채널 PMS 전체 온북. 따라서 단순 월합 비교는 부적절 — SKILL에 명시된 대로 검증 세부 로직 확정 후 채널 매핑 기준으로 재설계 필요.
- 빌드 내부 교차검증은 통과: 당일분석 NET=2,925 = 세그먼트합(OTA 1,087 + G-OTA 1,724 + IB 114); OTB 합계=세그합 일치.

## 조치 필요
- **푸시**: 호스트의 자동 푸시(_auto_commit_push.command / launchd 등)가 로컬 커밋 `a909354`를 origin/main 으로 전송해야 함. 샌드박스에서는 GitHub 접근이 차단되어 불가.
- 5단계 검증 로직: 채널 범위 정규화 후 재적용.

## 주의: PDF는 모두 정상 존재, 더미 데이터 미사용.
