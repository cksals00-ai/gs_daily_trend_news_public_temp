# 주간리포트 자동 작성 — 2026-05-30 (토) 실행 리포트

## 결과 요약
- 대상 주차: **2026-W22 · 2026.05.25(월)~05.28(목)** (05/29 금요일은 예약데이터 D-1 수집지연으로 미반영 — 전주와 동일 처리)
- 전년 동기간: 2025.05.26(월)~05.29(목), 요일 정렬
- `docs/gs-closing-report.html` 주간리포트 서브탭(WEEKLY_REPORT_HTML)에 6개 섹션 HTML 주입 완료
- `python3 scripts/build.py` 정상 완료(EXIT 0) — index.html / otb.html 재빌드

## 금주 핵심 수치 (OTA·G-OTA·Inbound 순예약, Net=Pickup−Cancel)
- 순예약 RN **9,882실** (목표 7,897 대비 125.1%, 전년 8,058 대비 +22.6%, 전주 9,492 WoW +4.1%)
- 순예약 매출 **2,027,630천원** (목표 대비 120.4%, 전년比 +49.8%)
- 세그먼트 구성: OTA 5,380 / G-OTA 4,296 / Inbound 206
- 채널 37개(거래처) 집계 · 상위 웹투어·트립닷컴·야놀자·여기어때·아고다

## 섹션
1. 사업장별 실적(25개, 번호순, 10개+더보기 15)
2. 세그먼트별 실적(OTA/G-OTA/Inbound)
3. 채널별 실적(37개, 10개+더보기 27)
4. 주간 인사이트(급등/급락/세그이동/달성률 미달/권역)
5. 기획전: 금주 진행 7건 · 측정실적(86코드 매칭) · 차주(6/1~6/7) 예정 3건
6. +60일(2026.07.29→전년 2025.07): 사업장별 실적·RM달성률 + 카테고리(룸온리 18,590 / 연박 9,145 / 패키지 21,291)

## 검증
- RN 정합성: 사업장 합 = 세그먼트 합 = 9,882 (일치)
- "기타" 카테고리 데이터 노출 없음 / 세그먼트·상품카테고리 분리 / 더미 없음
- 6개 섹션, 태그 밸런스(section 8/8, table 6/6, tr 111/111), 템플릿 리터럴 정상

## ⚠️ 수동 조치 필요 — git 커밋/푸시 미완료
샌드박스 환경 제약으로 자동 커밋/푸시 불가:
1. `.git/index.lock`·`.git/HEAD.lock` 제거 불가(Operation not permitted)
2. GitHub SSH(ssh.github.com:443) egress 차단(Forbidden)

변경 파일은 디스크에 저장 완료. 로컬 머신에서 아래 실행 필요:
```
cd ~/Desktop/gs_daily_trend_news_public_temp
rm -f .git/index.lock .git/HEAD.lock
git add -A
git commit -m "chore(auto): weekly report 2026-05-30 [skip ci]"
git push origin main
```
