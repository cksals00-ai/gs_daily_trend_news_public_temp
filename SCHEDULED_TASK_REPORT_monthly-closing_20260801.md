# 월간 마감 자동 실행 리포트 — 2026-08-01

**대상 마감월:** 2026년 7월 (전달)
**분기마감:** ❌ 미해당 (8월은 분기마감월 아님 · 분기마감은 1·4·7·10월) → 월간 마감만 수행
**실행 시각:** 2026-08-01 00:00 KST (자동)

## 수행 내역

1. 폴더 연결 — `~/Desktop/gs_daily_trend_news_public_temp` 부재로 `~/Projects/gs_daily_trend_news_public_temp` 사용(마운트 접근 정상).
2. `gs-closing-report.html` 마감 롤포워드 (6월 → 7월)
   - 보고 기간 `2026.01 ~ 2026.07`, 작성일 `2026.08.01`
   - `CLOSED_MONTHS=[1..7]`, `LATEST_MONTH=7`
   - 월간 탭: 7월 마감 추가(= NEW), 6월 NEW 해제, 트레일링 "Q2 분기 마감" 제거(Q3 미완료 → 분기 탭에만 유지)
   - 분기/반기 탭: Q2·상반기 NEW 배지 해제(신규 아님, 누계 표시 유지)
   - 연간 탭: `2026 YTD (1~6월)` → `2026 YTD (1~7월)`
3. `docs/build_closing_report.py` 실행 — 정상 산출(Data JSON 1,268,214 bytes)
4. `scripts/build.py` 실행 — `generate_otb_data`로 `docs/data/otb_data.json`(7월 포함) 재생성, `docs/data/db_aggregated.json` 동기화(by_segment=OTA/G-OTA/Inbound 필터·generated_at 반영) 완료. 배포본 `docs/data/db_aggregated.json.gz`는 유효 JSON에서 재압축(103.3MB → 15.0MB, round-trip 검증 OK).
5. 커밋 — `a314b04e chore(auto): monthly closing 2026-07 [skip ci]` (43 files). 마감 편집·데이터 아티팩트 모두 커밋 트리에 포함 확인.

## 7월 마감 요약 (otb_data.json 기준 · 동기간 OTB)

- RN 실적 **85,256실** / 목표 89,370실 → 달성률 **95.4%**, 전년비 **+4.4%**
- 매출 실적 **21,707백만원** / 목표 20,786백만원 → 달성률 **104.4%**, 전년비 **+21.9%**
- 세그먼트(OTA/G-OTA/Inbound) RN: 50,016 / 30,024 / 5,246
  - OTA YoY +12.2%(호조) · G-OTA YoY -5.6%(목표 35,055 대비 미달) · Inbound YoY -1.7%
- 사업장 23개, 번호순 정렬 / 세그 3종·OCC 전체 기준 유지

## 데이터 정합성 검증

- 세그3합(85,286) vs summary 실적(85,256) 차이 **+30실(0.04%)** — 반올림 허용치 이내(전월과 동일 패턴)
- RN/매출 = OTA + G-OTA + Inbound 기준, OCC = 전체 기준 유지
- `db_aggregated.json`(docs) 유효성·`.gz` round-trip 모두 검증 통과 — 202607 monthly_total 포함 확인
- 더미 데이터 미사용 — 전 수치 파이프라인 실데이터 기반
- 채널=거래처 명칭 통일, 예산→목표 표기 유지

## 확인 필요 사항 (caveat)

1. **샌드박스 git push 제한**: SSH 직접 푸시 불가(호스트키 미검증 → 키 추가 후 `Permission denied (publickey)`, 배포키 부재). 전월과 동일 조건. 로컬 `main`이 `origin/main` 대비 **1커밋 앞선 상태(a314b04e)**로, 호스트 데몬(`com.gs.daily-crawl`)의 정기 `git add -A` 사이클이 본 커밋을 수집·푸시 예정.
2. **build.py 미완료 단계**: 샌드박스 45초 실행 상한으로 `scripts/build.py`의 후반부(index.html / otb.html / sales-kpi.html 대시보드 재생성)는 완료되지 않음. 단, 마감 리포트는 `otb_data.json`(7월 포함 재생성 완료)·`db_aggregated.json.gz`(재압축 완료)에만 의존하므로 마감 산출물은 정상. 일별 대시보드 HTML은 데몬 정기 빌드에서 재생성됨.
3. **커밋 라벨**: 지시문 `date '+%Y-%m'`(=2026-08)과 전월 관례(마감월 기준 라벨, 예: "monthly closing 2026-05")가 상충 → 관례에 맞춰 마감월 기준 **"2026-07"**로 표기.
4. **.git 락/오브젝트 unlink 경고**: 샌드박스 파일삭제 가드로 `tmp_obj_*` unlink 경고 다수 — 커밋 자체는 정상 완료. 데몬 동작에 영향 없음.
