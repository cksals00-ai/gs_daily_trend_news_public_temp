# 주간리포트 자동 작성 — 2026-06-27 (토) 실행 리포트

## 결과 요약
- 대상 주차: **2026-W26 · 2026.06.22(월)~06.25(목)** 4일 (06/26 금은 예약데이터 D-1 수집 지연 → 미반영, 전주 동일 패턴)
- 전년 동기간: 2025.06.23(월)~06.26(목), 요일 정렬(364일 시프트) 4일
- `docs/gs-closing-report.html` 주간리포트 서브탭(WEEKLY_REPORT_INJECT)에 **6개 섹션 HTML 재작성 완료**
- `scripts/build_weekly_report_html.py` **정상(EXIT 0)** — 사업장 25 / 세그 3 / 채널 49 / 인사이트 8 / 금주캠페인 7 / 차주캠페인 5 / 섹션6 25
- `scripts/build.py` — 45초 샌드박스 콜 제한으로 한 번에 완주 불가하여 **모든 단계를 개별 실행해 동등하게 완료**(아래 참조). 모든 산출물 디스크 저장 완료.
- ⚠️ **git 커밋/푸시는 환경 제약으로 미완료** — 로컬 수동 실행 필요(아래). 변경 29파일은 모두 디스크 저장 + 인덱스 스테이징 완료.

## 금주 핵심 수치 (OTA·G-OTA·Inbound 순예약, Net = Pickup − Cancel)
- 순예약 RN **16,710실** (일할 환산 목표 대비 **달성률 170.9%**, 전년 대비 **YoY +39.7%**, 전주 대비 **WoW −8.0%**)
- 순예약 매출 **4,215,430천원**
- 세그먼트 구성(RN): **OTA 9,949 / G-OTA 5,058 / Inbound 1,703**
- **RN 정합성: 사업장 합 16,710 = 세그먼트 합 16,710 = 일치(OK)**

## 섹션
1. **사업장별 실적**(25개, 사업계획 번호순, 10개+더보기) — RM 2026-06 Budget(OTA+G-OTA+Inbound) ×4/30 일할 환산
2. **세그먼트별 실적**(OTA/G-OTA/Inbound 3개만)
3. **채널별 실적**(49개, 10개+더보기) — 거래처별 순예약 RN 순
4. **주간 인사이트** 8건 — enriched_notes.json 반영(급등/급락·세그이동·채널호조·달성률·Inbound·권역)
5. **기획전**: 금주 진행 7건 · 차주(6/29~) 판매개시 예정 5건
6. **+60일(기준 2026.08.21 → 전년 2025.08)**: 사업장별 실적(25개)·전년비 + 상품카테고리(룸온리/패키지/연박). 합계 RN 109,984 / 매출 24,968,430천원 / 카테고리합 60,077

## build.py 처리 (45초 콜 제한 우회)
build.py는 100MB db_aggregated.json을 다루는 하위 스크립트를 다수 체이닝하여 단일 콜(최대 45초) 내 완주 불가. 샌드박스는 백그라운드 프로세스도 콜 종료 시 종료됨. 따라서 build.py 내부 단계를 동일 순서로 개별 실행:
- freeze_closed_months(복원 0건) · generate_otb_data · build_weekly_comparison ✓
- index.html · otb.html · sales-kpi.html 재빌드 + 주간리포트 주입 ✓ (build.py 본 실행이 db 동기화 직전까지 완료)
- docs/data/db_aggregated.json 동기화(87MB, 39키, 유효성 검증 OK) ✓
- **db_aggregated.json.gz 재생성** (87,411,174 → 13,669,346 bytes) ✓
- package_series_trend.json · rm_fcst.json docs 동기화 ✓
- build_fcst_trend(69 snapshots) · build_inbound_enriched · build_rm_fcst_excel · parse_overseas · inject_menu_visibility ✓
- data_freshness.json 재생성(4 sources, 2026-06-27 10:16 KST) ✓

## 검증
- **RN 정합성**: 사업장 합 16,710 = 세그먼트 합 16,710 = **일치(OK)**
- **태그 밸런스**: section 8 · table 6 · thead 6 · tbody 6 · SECTION 01~06 전부 존재 · 템플릿 리터럴 `${}` **0개** · 더보기 토글(wkToggle) 4개
- **집계 기준일**: "금주 월~목(06/22~06/25) 4일" · "2026.06.22 기준" 정상 반영
- **세그먼트 vs 상품카테고리 분리**: OTA/G-OTA/Inbound(세그) ↔ 룸온리/연박/패키지(상품) 혼용 없음
- **"기타" 점검**: 블록 내 2건 모두 적법 — ① 섹션3 `기타OTA`는 원천 booking의 실제 거래처명(개별 노출, 합성 버킷 아님) ② 섹션6 "분류 미상(원천 '기타')은 제외" 안내문(금지 카테고리 제외 명시). 금지 위반 없음
- **더미 데이터 없음** (전 수치 db_aggregated.json 실데이터 산출)
- **docs/data/db_aggregated.json**: 재로드 검증 통과(39키, valid JSON)

## ⚠️ 수동 조치 필요 — git 커밋/푸시 미완료
샌드박스 환경 제약(전주들과 동일):
1. `.git/HEAD.lock`(이전 세션 2026-06-26 21:26 잔여 잠금) 제거 불가 — `Operation not permitted`. 이로 인해 `git commit`이 HEAD 갱신에 실패("Another git process seems to be running").
2. GitHub egress 차단 — `CONNECT ssh.github.com:443: Forbidden` → `git push` 불가.

변경 파일(29개)은 디스크 저장 + 인덱스 스테이징 완료. 로컬 머신에서 아래 실행 필요:

```bash
cd ~/Desktop/gs_daily_trend_news_public_temp   # 또는 ~/Projects/gs_daily_trend_news_public_temp
rm -f .git/index.lock .git/HEAD.lock
git add -A
git commit -m "chore(auto): weekly report 2026-06-27 [skip ci]"
git push origin main
```
