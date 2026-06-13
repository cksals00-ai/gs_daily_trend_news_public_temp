# 주간리포트 자동 작성 — 2026-06-13 (토) 실행 리포트

## 결과 요약
- 대상 주차: **2026-W24 · 2026.06.08(월)~06.10(수)** 3일 (06/11 목·06/12 금은 예약데이터 D-1 수집 지연으로 미반영 — build_weekly_comparison.py가 06/11을 partial로 감지: RN 338 vs 7일평균 5,957)
- 전년 동기간: 2025.06.09(월)~06.11(수), 요일 정렬(364일 시프트) 3일
- `docs/gs-closing-report.html` 주간리포트 서브탭(WEEKLY_REPORT_HTML)에 6개 섹션 HTML 재작성 완료
- `python3 scripts/build.py` 정상 완료(EXIT 0)

## 금주 핵심 수치 (OTA·G-OTA·Inbound 순예약, Net = Pickup − Cancel)
- 순예약 RN **9,199실** (목표 7,334 대비 125.4%, 전년 9,941 대비 -7.5%, 전주 6,040 WoW +52.3%)
- 순예약 매출 **2,218,270천원** (목표 1,426,300 대비 155.5%, 전년 1,977,080 대비 +12.2%)
- 세그먼트 구성: OTA 4,814 / G-OTA 4,211 / Inbound 174
- ※ 달성률·WoW가 높은 이유: 3일(월~수) WTD라 일할 환산 목표(×3/30)가 작고, 전주(6/01~6/03)는 취소 반영분이 커 순예약 베이스(6,040)가 낮았던 기저효과

## 섹션
1. 사업장별 실적(25개, **RM 사업계획 번호순 01~23 + 말미 2개**, 10개+더보기 15) — RM 2026-06 Budget(OTA+G-OTA+Inbound) ×3/30 일할 환산. 사업장합 RN = 세그먼트합 RN = **9,199 일치**
2. 세그먼트별 실적(OTA/G-OTA/Inbound 3개만)
3. 채널별 실적(45개, 10개+더보기 35) — 배지는 by_channel_segment 누적 net_rn 우위로 OTA/G-OTA 판정. OTA·G-OTA 실적 있는 거래처만(단체/회원·Inbound-only·기타 제외)
4. 주간 인사이트(전사/주간흐름/급등·급락/세그이동/채널/달성률 미달/Inbound/권역) 9건 — enriched_notes.json region_status 반영
5. 기획전: 금주 진행 8건(판매 6/08~6/14 중첩) · 차주(6/15~6/21) 예정 8건 — campaign_data.json summer_detail 실제 데이터
6. +60일(기준 2026.08.12 → 전년 2025.08): 사업장별 실적(25개)·전년비(24→25) + 카테고리(룸온리 22,538 / 패키지 27,628 / 연박 8,174). 합계 RN 109,984

## 데이터 기준 메모 / 이번 주 변경점
- **RM 스냅샷 갱신**: Revenue Meeting_2026.06.08.pdf (전주는 06.01) — 이번엔 **2026-08까지 커버**되어 섹션6에 8월 RM Budget(OTA+G-OTA+IB 96,390실) 참고치 추가. 단 전년도(2025.08) 목표 데이터는 여전히 부재 → 사업장별 표는 전년비(24→25) 기준 유지(더미 목표 미생성).
- 집계 윈도우는 build_weekly_comparison.py와 동일(월~마지막 완전일). 목표(Budget)는 rm_fcst.json 2026-06 세그먼트 Budget ×3/30. 전사 7,334실.
- 섹션6 사업장별·카테고리: db_aggregated 투숙월(stay-month) 202508/202408 net. 카테고리는 product_detail 9개를 3개 그룹으로 통합(기타 제외).
- 신규 생성 스크립트: **scripts/build_weekly_report_html.py** — WEEKLY_REPORT_HTML 블록을 데이터에서 결정적으로 재생성/주입(차주 이후 재현성 확보). wkToggle/buildWeeklyTab 함수는 보존하고 const 블록만 교체.

## 검증
- RN 정합성: 사업장 합(OTA+GOTA+IB) = 세그먼트 합 = **9,199 일치**
- 섹션6 합계 RN 109,984 / 매출 24,968,430천원 (전주 방법론 재현 일치: 소노벨 비발디파크 16,884·델피노 11,549)
- "기타" 카테고리 데이터 노출 없음 / 세그먼트(OTA·G-OTA·Inbound) vs 상품카테고리(룸온리·연박·패키지) 분리 / 더미 없음
- 6개 섹션 · 태그 밸런스(section 8/8, table 6/6, tr 126/126, thead 6/6, tbody 6/6, div 30/30) · 템플릿 리터럴 백틱/`${}` 0개(정상)
- 더보기 토글: s1-extra 15 · s3-extra 35 · s6-extra 15 (wkToggle 3개)

## ⚠️ 수동 조치 필요 — git 커밋/푸시 미완료
샌드박스 환경 제약으로 자동 커밋/푸시 불가(전주와 동일 상황):
1. `.git/index.lock`·`.git/HEAD.lock`(이전 세션 06-12 21:16 잔여 잠금) 제거 불가(Operation not permitted)
2. GitHub SSH(ssh.github.com:443) egress 차단

변경 파일은 디스크에 저장 완료(docs/gs-closing-report.html · docs/index.html · docs/data/* · scripts/build_weekly_report_html.py 등). 로컬 머신에서 아래 실행 필요:
```
cd ~/Desktop/gs_daily_trend_news_public_temp   # 또는 ~/Projects/gs_daily_trend_news_public_temp
rm -f .git/index.lock .git/HEAD.lock
git add -A
git commit -m "chore(auto): weekly report 2026-06-13 [skip ci]"
git push origin main
```
