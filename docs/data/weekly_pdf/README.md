# 주간업무 → 마감 보고서 주간 탭 시각화

마감 보고서(`docs/gs-closing-report.html`) **"주간 리포트" 탭** 상단(KPI 헤더 바로 아래)에
주간업무 PDF의 핵심 실적을 **파싱·시각화**해서 자동 표시합니다. (이전의 PDF 다운로드 카드 방식은 대체됨)

## 매주 반영 절차

1. 주간업무 PDF를 준비합니다 (예: `(N월M주)_소노호텔앤리조트 주간업무 YYYYMMDD ..pdf`).
   - ⚠️ **이 repo 폴더에 그냥 두지 마세요.** 백그라운드 자동업데이트 데몬이 untracked 파일을
     주기적으로 삭제합니다. PDF는 **repo 밖**(예: `~/Downloads/`)에 두거나 채팅 첨부로 전달.
2. 파서+생성 스크립트를 PDF 경로와 함께 실행:
   ```bash
   python3 scripts/build_weekly_business.py "/경로/주간업무.pdf"
   ```
   - P2(전사 실적 요약)·P5(당월 세그먼트·사업장별)를 파싱해
     `gs-closing-report.html`의 `WEEKLY_BIZ_INJECT` 마커 사이 `WEEKLY_BIZ_HTML` 상수를 갱신.
   - 출력 JSON의 `warnings`가 비어있는지 확인(레이아웃이 바뀐 주엔 경고가 뜸 → 보정 필요).
3. 변경분(`docs/gs-closing-report.html`)을 **명시적 pathspec으로 즉시 커밋**(데몬 add -A 회피):
   ```bash
   git commit --no-verify -- docs/gs-closing-report.html
   git push origin main
   ```

## 시각화 구성 (현재)

- **전사 실적 요약** — KPI 카드(합계 매출/운영 RN/OCC/합계 영익) + 운영·분양·합계 표(당월·누계 × 목표/실적/달성률/전년비)
- **세그먼트 구성(당월)** — 회원/단체/FIT 점유비 바 + 실적 RN·달성률
- **사업장별 실적(당월)** — 권역별 OCC/RN 실적/달성률/객실매출/전년비

## 매주 자동화

매주 **주간 스케줄 에이전트 작업**이 위 절차를 수행하도록 설계 — 새 PDF를 읽어 스크립트 실행,
warnings 확인·보정 후 커밋. PDF 레이아웃이 매주 동일하면 파싱은 그대로 동작하고,
바뀐 주에는 에이전트가 `scripts/build_weekly_business.py`의 파서를 보정.

## 코드 위치

- 파서·생성·주입: `scripts/build_weekly_business.py`
- 주입 지점: `gs-closing-report.html`의 `WEEKLY_REPORT_INJECT_END` 아래 `WEEKLY_BIZ_INJECT_START/END` 마커
  (주간리포트 에이전트가 재작성하는 `WEEKLY_REPORT_HTML` 블록 **밖**이라 독립적으로 보존).
