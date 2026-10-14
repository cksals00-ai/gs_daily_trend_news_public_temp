# 인바운드 리포트 자동 생성 (업체별·사업장별 / 전략방안)

GS 인바운드 실적 엑셀을 raw_db(집계코드 58)에서 자동 생성한다. 매년 **연도 인자만 바꿔** 재실행.
수작업(피벗 돌려 붙이기, 4h+)을 대체한다.

## 공통 정의 (`_common.py`)
- 인바운드 = `변경예약집계코드`(없으면 `예약집계코드`) == `58`
- **RNS = 객실수**(room count — 원본 파일 정의와 동일, 레코드별 RNS==객실수 확인)
- REV(백만원) = `1박객실료 × 객실수 / 1.1`(VAT 제외), ADR(천원) = REV×1000/RNS
- 국가 매핑 = `partner_mapping.json`(기존 전략파일 피벗캐시에서 1회 추출, 1,707 거래처)
  + 회원명 `(국적)` 파싱 폴백 → 미매핑 ~8%만 '기타'
- 부패행(객실수>5000, 예: 110,000 오류행) 자동 컷

## 1) 업체별·사업장별 실적 파일  `build_actuals.py`
2024 양식을 복사해 **그 해 전체 업체로 행을 새로 생성**(누락 0, RNs 내림차순·국가 내림차순).
```bash
python3 scripts/inbound_reports/build_actuals.py --year 2025            # 전체연도
python3 scripts/inbound_reports/build_actuals.py --year 2026 --max-month 09   # 마감월까지
python3 scripts/inbound_reports/build_actuals.py --year 2025 2026       # 여러 해
```
출력: `~/Downloads/(GS)<연도>년 인바운드 실적_업체별,사업장별.xlsx`
- `--template` 양식 파일(기본 2024 파일), `--outdir` 출력 폴더

## 2) 전략방안 롤포워드  `rollforward_strategy.py`
총괄/사업장별 시트의 연도열을 **한 칸씩 당겨**(2025슬롯→BASE_YEAR, 2024→-1, 2023→-2, 2019→-3)
재집계 값으로 채우고 **피벗/캐시 제거(경량화)**. 양식 보존.
```bash
python3 scripts/inbound_reports/rollforward_strategy.py --base-year 2026 --max-month 09
# 내년: --base-year 2027
```
출력: `~/Downloads/(인바운드) 전략방안_실적반영.xlsx`

## 매핑 갱신(선택)
신규 거래처가 많아 '기타'가 늘면, 최신 전략파일(피벗캐시 보유)에서 매핑을 다시 추출해
`partner_mapping.json` 을 교체하면 된다. (추출 스니펫은 커밋 이력 `inb_probe.py` 참고)

> Python 3.12(openpyxl 설치본) 필요: `/Library/Frameworks/Python.framework/Versions/3.12/bin/python3`
> (homebrew `python3`=3.14엔 openpyxl 없음)
