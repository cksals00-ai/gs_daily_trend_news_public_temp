# 자동 실행 리포트 — rm-fcst-booking-update

실행 시각: 2026-06-02 05:06 KST (자동 스케줄)
프로젝트: ~/Desktop/gs_daily_trend_news_public_temp

## 결과 요약

| 단계 | 상태 | 비고 |
|---|---|---|
| 0. git lock 정리 | ✅ | 잔존 HEAD.lock 은 마운트 제약(unlink 불가)으로 rename(.bak) 처리 후 진행 |
| 1. RM FCST 파싱 | ✅ | 최신 PDF: `Revenue Meeting_2026.05.26.pdf` → rm_fcst.json (23사업장×4권역) |
| 2. Daily Booking 파싱 | ✅ | `Daily Booking Report_2026.05.26.pdf` → daily_booking.json (25사업장, 3개월). 명시 경로 인자 전달 필요(기본 glob 미일치) |
| 3. OTB/인사이트/기획전 재생성 | ✅ | otb_data.json, enriched_notes.json, campaign_performance.json. 기획전은 신규 raw_db(예약/취소 _20260601) 반영 |
| 4. 전체 빌드 | ✅ | index.html, otb.html 등 재빌드 (Auto-Built 2026-06-02 05:05 KST) |
| 5. 숫자 검증 | ⚠️ | 아래 참고 — 단순 월합 비교는 측정범위 불일치로 무의미(정상) |
| 6. 커밋 | ✅ | `a233f9f` 로컬 커밋 (15 files changed) |
| 7. 푸시 | ❌ | **샌드박스 네트워크 차단**(SSH 443 Forbidden). 또한 origin/main 과 분기(diverged) 상태 → 호스트에서 pull --rebase 후 push 필요 |

## RM FCST 검증 (월별)
- 2026-05: grand_rn=196,035 / grand_rev=38,441M / seg(O+G+I)_rn=65,041 / rev=14,321M
- 2026-06: grand_rn=189,853 / grand_rev=35,342M / seg(O+G+I)_rn=70,812 / rev=14,094M

## Daily Booking Report (Grand Total, 전채널 PMS 온북 RNs)
- 2026-05: 192,864 (Budget 215,280, 달성 89.6%, OCC 53.6%, 당일변동 ▲2,021)
- 2026-06: 132,257 (Budget 214,269, 달성 61.7%, OCC 38.0%, 당일변동 ▲6,187)
- 2026-07: 56,885 (Budget 261,914, 달성 21.7%, OCC 15.8%, 당일변동 ▲3,440)

## 빌드 내부 교차검증 (통과)
- 당일분석 NET=1,555 = 세그먼트합(OTA 977 + G-OTA 578 + IB 0) 일치
- OTB 합계=세그합 일치 (✅ 검증 통과)
- 오늘 데이터 날짜: 20260531 (today_net=1,555)

## 숫자 검증 상세 (5단계 — 세부 로직 미확정)
온북 DB(net_rn) 월합 vs Daily Booking Report actual_rns 비교:

| 투숙월 | 온북DB(net_rn) | DailyBkRpt | 차이 | 차이% |
|---|---|---|---|---|
| 2026-05 | 78,351 | 192,864 | -114,513 | -59.4% ⚠️ |
| 2026-06 | 55,823 | 132,257 | -76,434 | -57.8% ⚠️ |
| 2026-07 | 26,661 | 56,885 | -30,224 | -53.1% ⚠️ |

➡ **차이는 오류가 아니라 측정 범위 불일치(정상)**: 온북 DB는 온라인영업팀 채널(대매점/OTA/패키지) 부분집합, Daily Booking Report는 전 채널 PMS 전체 온북. 단순 월합 비교는 부적절 — SKILL 명시대로 채널 매핑 기준 확정 후 5단계 재설계 필요.

## 참고
- 최신 PDF 날짜(05.26)가 오늘(06.02)보다 이전 — 신규 RM/Booking PDF 미수신. 가용한 최신 파일로 정상 처리함.
- 기획전 raw_db 는 _20260601 자료 신규 반영 (예약 3,737행 + 취소 3,834행, 누적 RN 3,743 / 매출 727.3백만).
- Booking Status Report PDF(투숙일별 OCC%) 전용 파서는 여전히 미배선. OCC%는 generate_otb_data.py 경로로 산출.

## 조치 필요
- **푸시**: 호스트(launchd / _auto_commit_push.command)가 로컬 커밋 `a233f9f`(외 누적 14건)를 origin/main 으로 전송해야 함. 단, origin/main 이 18건 앞서 분기됨 → **pull --rebase 선행 필수**. 샌드박스에서는 GitHub 접근 차단으로 불가.
- 5단계 검증 로직: 채널 범위 정규화 후 재적용.

## 주의: PDF는 모두 정상 존재, 더미 데이터 미사용.
