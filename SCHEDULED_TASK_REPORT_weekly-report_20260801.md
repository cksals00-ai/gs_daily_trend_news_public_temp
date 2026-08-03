# 주간리포트 자동 작성 — 2026-08-01 (토) 실행 리포트

## 결과 요약
- 대상 주차: **2026-W31 · 2026.07.27(월)~07.30(목)** 4일 (07/31 금은 D-1 수집 지연으로 미반영, weekly_comparison 종료일 = 07/30)
- 전년 동기간: 2025.07.28(월)~07.31(목), 요일 정렬 4일
- `docs/gs-closing-report.html` 주간리포트 서브탭(`WEEKLY_REPORT_HTML`)에 **6개 섹션 HTML 재생성 완료** (`scripts/build_weekly_report_html.py`)
- ⚠️ `git 커밋·푸시 미완료` — 샌드박스 제약(아래 "수동 조치"). **주간리포트 산출물은 디스크에 저장 완료.**
- `python3 scripts/build.py` 는 백그라운드 실행(대용량 gzip 다단계로 수 분 소요) — 단, build.py는 gs-closing-report.html을 수정하지 않으므로 주간리포트 산출물에는 영향 없음.

## 금주 핵심 수치 (OTA·G-OTA·Inbound 순예약, Net = Pickup − Cancel)
- 순예약 RN **12,029실** (일할 환산 RM Budget 대비 **100.9%**, 전년 동기간 대비 **YoY +28.2%**, 전주 대비 **WoW −11.7%**)
- 순예약 매출 **3,120,180천원**
- 세그먼트 구성(RN): **OTA 6,325 / G-OTA 4,873 / Inbound 831**
- **RN 정합성: 사업장합 12,029 = 세그먼트합 12,029 = 일치(OK)**

## 섹션 산출물
1. **사업장별 실적** 25개 (사업계획 번호순, 10개+더보기)
2. **세그먼트별 실적** OTA/G-OTA/Inbound 3개만
3. **채널별 실적** 47개 거래처 (OTA/G-OTA 배지, 실적 있는 거래처만, 10개+더보기)
4. **주간 인사이트** 9건 — enriched_notes.json 기반 자동 분석
5. **기획전**: 금주 진행 2건 · 차주 판매개시 예정 1건 — campaign_data.json
6. **+60일** (기준 2026.09.30 → 전년 2025.09): 사업장별 실적 25개·전년비(24→25) + 상품카테고리(룸온리/연박/패키지). 합계 RN 71,130 / 매출 12,900,460천원 / 카테고리합 29,589

## 검증
- **RN 정합성**: 사업장 합 12,029 = 세그먼트 합 12,029 = **일치(OK)**
- **태그 밸런스**: section 8/8 · table 6/6 · tr 115 · SECTION 01~06 전부 존재 · 미치환 템플릿 리터럴 `${}` 0개 · 더보기 토글(wkToggle) 3개
- **"기타" 카테고리 금지 준수**: 섹션2 세그는 OTA/G-OTA/Inbound 3개만, 섹션6 카테고리는 룸온리/연박/패키지만. 잔존 '기타' 문자열 2건은 실제 거래처명("기타OTA") 1건 + "분류 미상(원천 '기타')은 제외" **주석** 1건 (합성 버킷 아님)
- **세그먼트 vs 상품카테고리 분리**: 혼용 없음
- **월 라벨 최신화**: 섹션6 = 2025.09 정상(+60일=2026.09.30), 스테일 2025.08 잔재 0건
- **더미 데이터 없음**
- 소스 데이터 신선도: db_aggregated.json / enriched_notes.json / campaign_data.json / weekly_comparison.json 모두 금일(08/01) 데일리 파이프라인 재생성분.

## ⚠️ 수동 조치 필요 — 로컬 머신에서 실행
샌드박스 제약으로 build/commit/push 미완:
- `.git/index.lock` (06:22 데일리런 잔여, 0바이트) → 샌드박스에서 삭제 `Operation not permitted` → git add/commit이 `Unable to create index.lock`으로 실패.

사용자 로컬(권한 정상)에서:

```bash
cd ~/Desktop/gs_daily_trend_news_public_temp   # 또는 ~/Projects/gs_daily_trend_news_public_temp
rm -f .git/HEAD.lock .git/index.lock           # 잔여 잠금 제거
python3 scripts/build.py                        # 파생물(index/otb/.gz 등) 재생성 (미완 시)
git add -A
git commit -m "chore(auto): weekly report 2026-08-01 [skip ci]"
git push origin main
```

- 주간리포트 산출물(`docs/gs-closing-report.html` 주간리포트 탭)은 이미 디스크에 반영 완료.
- `build.py`는 `gs-closing-report.html`을 수정하지 않으므로, 재실행해도 주간리포트 내용은 그대로 유지됨.
