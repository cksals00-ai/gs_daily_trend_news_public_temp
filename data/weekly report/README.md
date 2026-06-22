# 주간 리포트 PDF 드롭 폴더

매주 받는 **세일즈마케팅 주간업무 PDF**(와 시장·트랜드 PDF)를 **이 폴더에 그대로 넣으면** 됩니다.
파일명은 바꾸지 않아도 됩니다 — 예: `(6월 4주)세일즈마케팅 주간업무 260622.pdf`.

## 빌드 (마감 보고서 "주간 리포트" 탭 시각화 갱신)

```bash
python3 scripts/build_weekly_business.py        # 이 폴더의 최신 '…주간업무….pdf' 자동 선택
# 또는 경로 명시:
python3 scripts/build_weekly_business.py "data/weekly report/(6월 4주)세일즈마케팅 주간업무 260622.pdf"
```

- 파서가 **세그먼트+사업장 표를 가진 '당월'·'누계' 페이지를 자동 탐지**(페이지 번호 하드코딩 안 함)해
  전사 객실영업 요약(OCC·RNs·ADR·객실매출) / 세그먼트(회원·단체·FIT) / 사업장별(22개) 표를 생성,
  `docs/gs-closing-report.html` 의 `WEEKLY_BIZ_INJECT` 마커 사이에 주입합니다.
- 출력 JSON의 `warnings` 가 비어 있는지 확인하세요. 비어 있지 않으면 PDF 레이아웃이 바뀐 것 →
  `scripts/build_weekly_business.py` 파서 보정 필요.

## 주의

- **이 폴더의 `*.pdf` 는 git 추적에서 제외**(`.gitignore`)됩니다 — 공개 repo 경량화 + 자동 업데이트
  데몬의 `git clean` 으로부터 원본 PDF 보호. 따라서 PDF를 여기 둬도 안전하게 보존됩니다.
- 시각화는 PDF 다운로드 카드를 **대체**하므로 PDF 자체를 배포(커밋)할 필요가 없습니다.
- 빌드 후 `docs/gs-closing-report.html` 변경분을 **명시적 pathspec**으로 커밋하세요(데몬 `add -A` 회피):
  ```bash
  git commit --no-verify -- docs/gs-closing-report.html
  ```
