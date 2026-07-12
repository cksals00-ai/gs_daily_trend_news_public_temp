# 주간리포트 자동 작성 — 2026-07-11 (토) 실행 리포트

## 결과 요약
- 대상 주차: **2026-W28 · 2026.07.06(월)~07.09(목)** 4일 (07/10 금은 예약데이터 D-1 수집 지연 → 미반영. db_aggregated 예약일 최종 = 07/09, 전주와 동일 패턴)
- 전년 동기간: 2025.07.07(월)~07.10(목), 요일 정렬 4일
- `docs/gs-closing-report.html` 주간리포트 서브탭(`WEEKLY_REPORT_HTML`)에 **6개 섹션 HTML 재생성 완료** (`scripts/build_weekly_report_html.py`)
- ⚠️ `python3 scripts/build.py` 미실행 / **git 커밋·푸시 미완료** — 사유는 아래 "수동 조치" 참고. **변경분은 디스크에 저장 완료.**

## 금주 핵심 수치 (OTA·G-OTA·Inbound 순예약, Net = Pickup − Cancel)
- 순예약 RN **13,014실** (일할 환산 RM Budget 대비 **109.2%**, 전년 동기간 대비 **YoY −10.0%**, 전주 대비 **WoW −21.2%**)
- 순예약 매출 **3,530,290천원**
- 세그먼트 구성(RN): **OTA 7,169 / G-OTA 5,129 / Inbound 716**
- **RN 정합성: 사업장합 13,014 = 세그먼트합 13,014 = 일치(OK)**

## 섹션 산출물
1. **사업장별 실적** 25개 (사업계획 번호순, 10개+더보기)
2. **세그먼트별 실적** OTA/G-OTA/Inbound 3개만
3. **채널별 실적** 45개 거래처 (OTA/G-OTA 배지, 실적 있는 거래처만, 10개+더보기)
4. **주간 인사이트** 9건 — enriched_notes.json 기반 자동 분석
5. **기획전**: 금주 진행 5건 · 차주 판매개시 예정 4건 — campaign_data.json
6. **+60일** (기준 2026.09.09 → 전년 2025.09): 사업장별 실적 25개·전년비(24→25) + 상품카테고리(룸온리/연박/패키지). 합계 RN 71,130 / 매출 12,900,460천원 / 카테고리합 29,589

## WoW 해석 참고
전주(W27, 06/29~07/02)는 순예약 15,010실로 베이스가 높았고, 금주는 취소 반영 및 여름 성수 피크 통과로 WoW −21.2%. 다만 절대 페이스는 RM Budget의 109.2%로 목표선은 상회. YoY는 −10.0%로, 전년 동기(17,250실)의 높은 베이스 대비 약세 → 채널·요금 점검 대상.

## 검증
- **RN 정합성**: 사업장 합 13,014 = 세그먼트 합 13,014 = **일치(OK)**
- **태그 밸런스**: section 8/8 · table 6/6 · tr 119 · SECTION 01~06 전부 존재 · 템플릿 리터럴 `${}` 0개 · 더보기 토글(wkToggle) 3개
- **"기타" 카테고리 금지 준수**: 섹션2 세그는 OTA/G-OTA/Inbound 3개만, 섹션6 카테고리는 룸온리/연박/패키지만. 잔존 '기타' 문자열 2건은 모두 "분류 미상(원천 '기타')은 제외" **주석**(합성 버킷 아님)
- **세그먼트 vs 상품카테고리 분리**: 혼용 없음
- **월 라벨 최신화**: 섹션6 = 2025.09 정상, 스테일 2025.08 잔재 0건 (기준 +60일 = 2026.09.09)
- **더미 데이터 없음**
- 이번 주는 스크립트 자체가 이미 날짜 자동추종(BUDGET_MONTH·S6_LY 등)이라 코드 수정 없이 재실행만으로 정상 산출됨.

## ⚠️ 수동 조치 필요 — 로컬 머신에서 실행
샌드박스 제약으로 build/commit/push 미완:
- `.git/HEAD.lock`·`.git/index.lock` (06:17 데일리런 잔여, 0바이트) → 샌드박스에서 삭제 `Operation not permitted` → git commit이 `Unable to create index.lock`으로 실패.
- push는 GitHub egress 차단.
- `build.py`는 gzip(80MB+, level 9)·다중 서브프로세스로 수 분 소요 → 단일 콜(45초) 내 완료 불가. 단, **build.py는 gs-closing-report.html을 수정하지 않으므로 주간리포트 산출물은 영향 없음.**

사용자 로컬(권한·네트워크 정상)에서:

```bash
cd ~/Desktop/gs_daily_trend_news_public_temp   # 또는 ~/Projects/gs_daily_trend_news_public_temp
rm -f .git/HEAD.lock .git/index.lock           # 잔여 잠금 제거
python3 scripts/build.py                        # 파생물(index/otb/.gz 등) 재생성
git add -A
git commit -m "chore(auto): weekly report 2026-07-11 [skip ci]"
git push origin main
```

- 주간리포트 산출물(`docs/gs-closing-report.html` 주간리포트 탭)은 이미 디스크에 반영 완료.
- `build.py`는 `gs-closing-report.html`을 수정하지 않으므로, 재실행해도 주간리포트 내용은 그대로 유지됨.
