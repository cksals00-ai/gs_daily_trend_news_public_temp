# 주간리포트 자동 작성 — 2026-08-28 (금) 실행 리포트

## 결과 요약
- 대상 주차: **2026.08.24(월)~08.26(수)** 3일 — DB 원천이 08/26까지 수집되어 목·금은 미반영 (weekly_comparison.json 기준, 금일 05:02 파이프라인 산출)
- 전년 동기간: 2025.08.25(월)~08.27(수), 요일 정렬 3일
- `docs/gs-closing-report.html` 주간리포트 서브탭(`WEEKLY_REPORT_HTML`) **6개 섹션 재생성 완료** (`scripts/build_weekly_report_html.py`)
- `python3 scripts/build.py` 전체 빌드 완료 (10:03 KST)
- git 커밋 완료: `8e9364d7 chore(auto): weekly report 2026-08-28 [skip ci]`
- ⚠️ **push 미완** — 샌드박스에서 SSH 키 접근 불가(ssh.github.com 인증 실패). 아래 "푸시 처리" 참고.

## 금주 핵심 수치 (OTA·G-OTA·Inbound 순예약)
- 순예약 RN **9,030실** · 매출 **1,890,070천원**
- 일할 환산 RM Budget 대비 **93.7%** · 전년 동기간 **YoY +4.3%** · 전주 대비 **WoW −0.5%**
- 세그먼트 RN: OTA 4,190 / G-OTA 3,774 / Inbound 1,066
- **RN 정합성: 사업장합 9,030 = 세그먼트합 9,030 (OK)**

## 섹션 산출물
1. 사업장별 실적 25개 (사업계획 번호순, 10개+더보기)
2. 세그먼트별 실적 OTA/G-OTA/Inbound 3개만
3. 채널별 실적 48개 거래처 (10개+더보기)
4. 주간 인사이트 9건 (enriched_notes.json 기반)
5. 기획전: 금주 진행 1건 · 차주 예정 0건 (campaign_data.json)
6. +60일 (기준 2026.10.27 → 전년 2025.10): 사업장별 25개 · 합계 RN 75,116 / 매출 15,863,420천원 / 카테고리합 25,137

## 검증
- SECTION 01~06 각 1개 · 미치환 `${}` 0건 · 더보기 토글 3개
- "기타" 합성 버킷 없음 (잔존 '기타' 2건 = 거래처명 + 제외 규칙 주석)
- 상품카테고리(룸온리/연박/패키지) vs 세그먼트 혼용 없음
- +60일 라벨 2025.10 정상, 스테일 라벨 0건 · 더미 데이터 없음
- 소스 신선도: db_aggregated / enriched_notes / campaign_* / weekly_comparison 모두 금일 05:02 산출분

## ⚠️ 푸시 처리
샌드박스에서 SSH 인증 불가로 push 실패(커밋은 로컬 완료). 두 가지 경로:
1. **자동**: 호스트 launchd 데몬(`com.gs.daily-crawl`, 매일 05:00)이 rebase 후 push — 내일(08/29) 05:00 자동 반영 예정.
2. **수동(즉시 반영 원할 시)**:
   ```bash
   cd ~/Projects/gs_daily_trend_news_public_temp
   rm -f .git/index.lock   # 샌드박스 잔여 락 (아래 참고)
   git pull --no-rebase --autostash origin main && git push origin main
   ```

※ 커밋 직후 `.git/index.lock` 잔여 락이 생겼고 샌드박스 권한으로 삭제 불가 — 본 실행 리포트 파일은 디스크 저장만 되고 커밋 미포함. 호스트 데몬이 시작 시 락을 자동 정리하므로 다음 자동 사이클에서 해소됨.

## 판단 사항 (자율 실행 노트)
- 원래 토요일 10:00 작업이나 금요일 실행됨 → 가용 최신 주차(08/24~08/26)로 작성. 내일 데일리 파이프라인 후 재실행하면 목·금 포함 주차로 자동 갱신됨(멱등).
- `git add -A`에 데일리 파이프라인이 정리한 `data/palatium_db/*.xlsx` 삭제분 등 기존 워킹트리 변경이 함께 커밋됨(태스크 지침대로 add -A 수행).
