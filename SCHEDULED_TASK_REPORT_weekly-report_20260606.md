# 주간리포트 자동 작성 — 2026-06-06 (토) 실행 리포트

## 결과 요약
- 대상 주차: **2026-W23 · 2026.06.01(월)~06.04(목)** (06/05 금요일은 예약데이터 D-1 수집지연으로 미반영 — 전주와 동일 처리)
- 전년 동기간: 2025.06.02(월)~06.05(목), 요일 정렬(364일 시프트)
- `docs/gs-closing-report.html` 주간리포트 서브탭(WEEKLY_REPORT_HTML)에 6개 섹션 HTML 주입 완료
- `python3 scripts/build.py` 정상 완료(EXIT 0) — index.html / otb.html 재빌드

## 금주 핵심 수치 (OTA·G-OTA·Inbound 순예약, Net = Pickup − Cancel)
- 순예약 RN **8,902실** (목표 9,778 대비 91.0%, 전년 9,466 대비 -6.0%, 전주 9,892 WoW -10.0%)
- 순예약 매출 **2,043,070천원** (목표 1,901,733 대비 107.4%, 전년 2,037,270 대비 +0.3%)
- 세그먼트 구성: OTA 4,788 / G-OTA 3,764 / Inbound 350
- 채널 33개(거래처, 금주 실적>0) 집계 · 상위 야놀자·아고다·여기어때·트립닷컴·익스피디아

## 섹션
1. 사업장별 실적(25개, 사업계획 번호순, 10개+더보기 15) — RM Budget(OTA+G-OTA+Inbound) ×4/30 일할 환산
2. 세그먼트별 실적(OTA/G-OTA/Inbound 3개만)
3. 채널별 실적(33개, 10개+더보기 23) — 배지는 by_channel_segment 누적 net_rn 우위로 OTA/G-OTA 판정(아고다·트립닷컴·익스피디아·트립비토즈=G-OTA)
4. 주간 인사이트(전사/주간흐름/급등·급락/기저효과/세그이동/채널/달성률 미달/Inbound/권역)
5. 기획전: 금주 진행 4건(판매 06/01~06/04 중첩) · 차주(06/08~06/14) 예정 8건
6. +60일(2026.08.05 → 전년 2025.08): 사업장별 실적(25개)·전년비(24→25) + 카테고리(룸온리 22,538 / 패키지 27,628 / 연박 8,174)

## 검증
- RN 정합성: 사업장 합(OTA+GOTA+IB) = 세그먼트 합 = **8,902** (일치), 전년 합도 9,466 일치
- "기타" 카테고리 데이터 노출 없음(설명 각주 1건만) / 세그먼트(OTA·G-OTA·Inbound) vs 상품카테고리(룸온리·연박·패키지) 분리 / 더미 없음
- 6개 섹션 · 태그 밸런스(section 8/8, table 6/6, tr 110/110, div 30/30) · 템플릿 리터럴 백틱/`${}` 0 (정상)
- 더보기 토글: s1-extra 15 · s3-extra 23 · s6-extra 15 (wkToggle 3개)

## 데이터 기준 메모
- 집계 윈도우는 build_weekly_comparison.py와 동일 — 최신 예약일(06/05)이 partial(7일평균의 30% 미만)이라 06/04를 마지막 완전일로 사용, 금주 = 월~목 4일.
- 목표(Budget)는 rm_fcst.json 2026-06 OTA+G-OTA+Inbound 세그먼트 Budget을 ×4/30 일할 환산. 전사 9,778실.
- 섹션6 RM 달성률: RM 스냅샷(2026.06.01)이 6·7월만 커버하여 8월 목표 부재 → 더미 목표 미생성, 전년 실적·전년비(24→25) 기준으로 준비 우선순위 제시(각주 명기).

## ⚠️ 수동 조치 필요 — git 커밋/푸시 미완료
샌드박스 환경 제약으로 자동 커밋/푸시 불가(전주와 동일 상황):
1. `.git/index.lock`·`.git/HEAD.lock`(이전 세션 21:13~21:15 잔여 잠금) 제거 불가(Operation not permitted)
2. GitHub SSH(ssh.github.com:443) egress 차단(Forbidden)

변경 파일은 디스크에 저장 완료(docs/gs-closing-report.html · docs/index.html · docs/otb.html 등). 로컬 머신에서 아래 실행 필요:
```
cd ~/Desktop/gs_daily_trend_news_public_temp
rm -f .git/index.lock .git/HEAD.lock
git add -A
git commit -m "chore(auto): weekly report 2026-06-06 [skip ci]"
git push origin main
```
