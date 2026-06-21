# 주간리포트 자동 작성 — 2026-06-20 (토) 실행 리포트

## 결과 요약
- 대상 주차: **2026-W25 · 2026.06.15(월)~06.18(목)** 4일 (06/19 금은 예약데이터 D-1 수집 지연 → partial(RN 463)로 미반영, 전주와 동일 패턴)
- 전년 동기간: 2025.06.16(월)~06.19(목), 요일 정렬(364일 시프트) 4일
- `docs/gs-closing-report.html` 주간리포트 서브탭(WEEKLY_REPORT_HTML)에 **6개 섹션 HTML 재작성 완료**
- `python3 scripts/build.py` **정상 완료(EXIT 0 · "전체 빌드 완료 · Auto-Built 2026-06-20 10:23 KST")**
- ⚠️ **git 커밋/푸시는 환경 제약으로 미완료** — 로컬 수동 실행 필요(아래 참조). 변경 파일은 디스크에 모두 저장됨.

## 금주 핵심 수치 (OTA·G-OTA·Inbound 순예약, Net = Pickup − Cancel)
- 순예약 RN **18,054실** (일할 환산 목표 9,779 대비 **184.6%**, 전년 13,168 대비 **YoY +37.1%**, 전주 13,712 대비 **WoW +31.7%**)
- 순예약 매출 **4,128,920천원**
- 세그먼트 구성(RN): **OTA 9,871 / G-OTA 4,991 / Inbound 3,192**
- ※ 달성률·WoW가 높은 배경: 4일(월~목) WTD라 일할 환산 목표(×4/30)가 작고, 금주 Inbound 단체 예약이 다수 사업장에 유입(아래 검증 참조)

## 섹션
1. **사업장별 실적**(25개, RM 사업계획 번호순 01~23 + 말미 2개, 10개+더보기 15) — RM 2026-06 Budget(OTA+G-OTA+Inbound) ×4/30 일할 환산. **사업장합 RN = 세그먼트합 RN = 18,054 일치**
2. **세그먼트별 실적**(OTA/G-OTA/Inbound 3개만)
3. **채널별 실적**(49개, 10개+더보기 39) — by_channel_segment 누적 net_rn 우위로 OTA/G-OTA 배지 판정. OTA·G-OTA 실적 있는 거래처만 노출
4. **주간 인사이트** 8건 — enriched_notes.json region_status 반영(급등/급락·세그이동·채널호조·달성률미달·Inbound·권역)
5. **기획전**: 금주 진행 10건(판매기간이 6/15~6/21과 중첩) · 차주(6/22~6/28) 판매개시 예정 5건 — campaign_data.json summer_detail
6. **+60일(기준 2026.08.19 → 전년 2025.08)**: 사업장별 실적(25개)·전년비(24→25) + 카테고리(룸온리/패키지/연박). 합계 RN 109,984 / 매출 24,968,430천원 / 카테고리합 60,077

## 이번 주 변경점 (스크립트)
- `scripts/build_weekly_report_html.py`를 **날짜 동적화**로 개선(차주 이후 재현성 강화):
  - 캠페인 금주/차주 윈도우를 this_week 월요일 기준 **Mon~Sun 동적 산출**(기존 6/8~6/14·6/15~6/21 하드코딩 제거)
  - "집계 기준"·푸터의 미반영 평일 안내문을 **end+1~금요일 자동 산출**(`miss_note`)로 교체(기존 "06/11~06/12" 하드코딩 제거)
  - 섹션5 카드 부제(판매기간·판매개시)도 동적 날짜로 교체
- 그 외 수치 로직·세그먼트/카테고리 분리 규칙은 전주와 동일.

## 검증
- **RN 정합성**: 사업장 합(OTA+GOTA+IB) 18,054 = 세그먼트 합 18,054 = **일치(OK)**
- **Inbound 급증 검증**: 3,192실 — 단일 사업장 쏠림·오류 아님. 소노문 해운대 1,384 · 소노캄 고양 1,339 · 소노벨 제주 136 · 소노캄 여수 83 등 **복수 사업장 분산 유입**(도심·단체 강세 사업장 중심) → 단체 인바운드 예약 유입으로 판단, 정상 데이터
- **"기타" 카테고리 금지 준수**: 상품카테고리 '기타(분류 미상)'은 섹션6에서 제외(주석으로 명시). 섹션3의 `기타OTA`는 원천 booking 데이터의 실제 거래처명(개별 노출 항목)으로 합성 버킷 아님 → 유지
- **세그먼트 vs 상품카테고리 분리**: OTA/G-OTA/Inbound(세그) ↔ 룸온리/연박/패키지(상품) 혼용 없음
- **태그 밸런스**: section 8/8 · table 6/6 · thead 6/6 · tbody 6/6 · tr 129/129 · SECTION 01~06 전부 존재 · 템플릿 리터럴 `${}` 0개 · 더보기 토글(wkToggle) 3개
- **더미 데이터 없음**
- **build.py**: index.html·otb.html·db_aggregated.json(.gz)·otb_data.json·weekly_comparison.json·data_freshness.json·rm_fcst 동기화 전부 갱신, "전체 빌드 완료" 마커 도달(EXIT 0)

## ⚠️ 수동 조치 필요 — git 커밋/푸시 미완료
샌드박스 환경 제약(전주와 동일):
1. `.git/index.lock`·`.git/HEAD.lock`(이전 세션 06-19 21:20/21:22 잔여 잠금) 제거 불가 — `Operation not permitted`. 이로 인해 `git add`가 `index.lock: File exists`로 실패.
2. GitHub egress 차단 — `ssh.github.com`/`github.com` DNS 해석 불가(`Temporary failure in name resolution`).

변경 파일은 디스크에 저장 완료(`docs/gs-closing-report.html` · `docs/index.html` · `docs/otb.html` · `docs/data/*` · `scripts/build_weekly_report_html.py` 등). 로컬 머신에서 아래 실행 필요:

```bash
cd ~/Desktop/gs_daily_trend_news_public_temp   # 또는 ~/Projects/gs_daily_trend_news_public_temp
rm -f .git/index.lock .git/HEAD.lock
git add -A
git commit -m "chore(auto): weekly report 2026-06-20 [skip ci]"
git push origin main
```
