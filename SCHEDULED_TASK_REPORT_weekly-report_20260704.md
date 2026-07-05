# 주간리포트 자동 작성 — 2026-07-04 (토) 실행 리포트

## 결과 요약
- 대상 주차: **2026-W27 · 2026.06.29(월)~07.02(목)** 4일 (07/03 금·이후는 예약데이터 D-1 수집 지연 → 미반영, 전주와 동일 패턴)
- 전년 동기간: 2025.06.30(월)~07.03(목), 요일 정렬 4일
- `docs/gs-closing-report.html` 주간리포트 서브탭(`WEEKLY_REPORT_HTML`)에 **6개 섹션 HTML 재작성 완료** (`scripts/build_weekly_report_html.py`)
- ⚠️ `python3 scripts/build.py` **완주 불가** — 샌드박스 제약(콜당 45초 상한 + 백그라운드 프로세스 미유지). build.py는 gzip(82MB, level 9) 및 여러 서브프로세스로 수 분 소요 → 단일 콜 내 완료 불가. **단, build.py는 `gs-closing-report.html`을 건드리지 않으므로 주간리포트 산출물은 영향 없음.**
- ⚠️ **git 커밋/푸시 미완료** — `.git/HEAD.lock`(06:20 데일리런 잔여, 0바이트) 삭제 불가(`Operation not permitted`) → `cannot lock ref 'HEAD'`로 commit 실패. push는 GitHub egress 차단(`ssh.github.com:443 Forbidden`). **변경분은 모두 디스크에 저장됨.**

## 금주 핵심 수치 (OTA·G-OTA·Inbound 순예약, Net = Pickup − Cancel)
- 순예약 RN **16,477실** (일할 환산 RM Budget 대비 **138.3%**, 전년 동기간 대비 **YoY −1.5%**, 전주 대비 **WoW −2.8%**)
- 순예약 매출 **4,306,020천원**
- 세그먼트 구성(RN): **OTA 9,446 / G-OTA 6,427 / Inbound 604**
- **RN 정합성: 사업장합 16,477 = 세그먼트합 16,477 = 일치(OK)**

## 섹션
1. **사업장별 실적**(25개, RM 사업계획 번호순, 10개+더보기) — RM **2026-07** Budget(OTA+G-OTA+Inbound) ×4/30 일할 환산
2. **세그먼트별 실적**(OTA/G-OTA/Inbound 3개만)
3. **채널별 실적**(46개, 10개+더보기) — OTA/G-OTA 배지 판정, 실적 있는 거래처만 노출
4. **주간 인사이트** 9건 — enriched_notes.json 기반 자동 분석
5. **기획전**: 금주 진행 6건 · 차주 판매개시 예정 4건 — campaign_data.json
6. **+60일(기준 2026.09.02 → 전년 2025.09)**: 사업장별 실적(25개)·전년비(24→25) + 카테고리(룸온리/패키지/연박). 합계 RN 71,130 / 매출 12,900,460천원 / 카테고리합 29,589

## 이번 주 변경점 (스크립트) — 7월 진입에 따른 하드코딩 동적화
`scripts/build_weekly_report_html.py`의 **월 하드코딩을 날짜 자동추종으로 개선**(전월 값이 그대로 새어나오는 오류 제거):
- **RM Budget 기준월**: `2026-06` 하드코딩 → 금주 종료월 자동(`BUDGET_MONTH`, 이번주=`2026-07`). 섹션1·2 달성률이 7월 Budget 기준으로 정상 산출.
- **섹션6 +60일 대상월**: `202508`/`202408`(8월) 하드코딩 → `date.today()+60일`의 월 자동(`S6_LY`/`S6_LLY`, 이번=`202509`/`202409`). 헤더·표·인사이트·카테고리 라벨 전부 9월로 동적 교체.
- **RM +60 Budget 안내**: 9월은 현 RM 스냅샷 미커버 → 미커버 문구로 자동 분기(`rm_bullet`). 더미 목표 미생성 유지.

## 검증
- **RN 정합성**: 사업장 합 16,477 = 세그먼트 합 16,477 = **일치(OK)**
- **태그 밸런스**: section 8/8 · table 6/6 · thead 6/6 · tbody 6/6 · tr 121 · SECTION 01~06 전부 존재 · 템플릿 리터럴 `${}` 0개 · 더보기 토글(wkToggle) 3개
- **"기타" 카테고리 금지 준수**: 섹션2 세그는 OTA/G-OTA/Inbound 3개만, 섹션6 카테고리는 룸온리/연박/패키지만. 잔존 '기타' 문자열 2건은 모두 "분류 미상(원천 '기타')은 제외" **주석**(합성 버킷 아님).
- **세그먼트 vs 상품카테고리 분리**: OTA/G-OTA/Inbound(세그) ↔ 룸온리/연박/패키지(상품) 혼용 없음
- **월 라벨 최신화**: 섹션6 '2025년 8월'/'2025.08' 잔재 **0건**, '2025.09' 정상 노출
- **더미 데이터 없음**

## ⚠️ 수동 조치 필요 — 로컬 머신에서 실행
샌드박스 제약으로 build/commit/push 미완. 사용자 로컬(권한·네트워크 정상)에서:

```bash
cd ~/Desktop/gs_daily_trend_news_public_temp   # 또는 ~/Projects/gs_daily_trend_news_public_temp
rm -f .git/HEAD.lock .git/index.lock           # 잔여 잠금 제거
python3 scripts/build.py                        # 파생물(index/otb/.gz 등) 재생성
git add -A
git commit -m "chore(auto): weekly report 2026-07-04 [skip ci]"
git push origin main
```

- 주간리포트 산출물(`docs/gs-closing-report.html` 주간리포트 탭)과 스크립트 수정(`scripts/build_weekly_report_html.py`)은 이미 디스크에 반영 완료.
- `build.py`는 `gs-closing-report.html`을 수정하지 않으므로, 재실행해도 주간리포트 내용은 그대로 유지됨.
