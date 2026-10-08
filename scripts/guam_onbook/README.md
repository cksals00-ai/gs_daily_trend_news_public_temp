# 괌 On Book 업데이트

Golfmanager 예약 파일을 맥에서 가공하여 `docs/guam-onbook.html`에 표시합니다. 웹에는 월·골프장·채널별 집계만 생성합니다.

## 매일 사용할 방법
1. Golfmanager **Billing → Bookings → Breakdown**에서 Creation date 제한을 지우고, Start date로 조회 기간을 정합니다. 예: 2026-10-01~2026-12-31. Cancelled=No로 조회합니다.
2. `… → Export to excel`로 내려받습니다. 조회 기간 전체 예약이 필요합니다. 당일 생성분만 내려받으면 누적 현황을 재현할 수 없습니다.
3. 파일을 `local/guam-onbook/inbox/bookings_2026-10-08.xlsx`처럼 저장합니다. 날짜는 다운로드/조회 기준일입니다. 하루에 여러 번 갱신하면 같은 날짜 파일을 교체합니다. 기존 날짜 파일은 보관합니다.
4. 목표·예산·전년 실적은 같은 폴더의 `targets.xlsx`에서 수정합니다.
5. `local/guam-onbook/업데이트.command`를 더블클릭합니다. `폴더감시.command`는 실행된 동안 변경을 감지합니다. 감시는 터미널을 닫거나 Ctrl+C로 중지합니다.
6. JSON은 `docs/data/guam_onbook.json`에 생성됩니다. 이 단계는 **맥의 사본만 갱신**합니다. 웹 게시를 하려면 기존 대시보드 작업이 끝난 후 이 집계 파일을 배포 경로에 연결해야 합니다.

## 처음 설치할 방법
Python 3와 `requirements.txt`의 openpyxl이 필요합니다. 저장소 루트에서:

```sh
python3 -m pip install -r scripts/guam_onbook/requirements.txt
mkdir -p local/guam-onbook/inbox local/guam-onbook/history
cp scripts/guam_onbook/config.example.json local/guam-onbook/config.json
python3 scripts/guam_onbook/import_onbook.py --config local/guam-onbook/config.json
```

이 채팅의 맥 작업 사본에는 입력 폴더·목표 엑셀·실행 파일을 이미 준비했습니다.

## 목표 엑셀
첫 시트의 1행 헤더는 `year, month, venue, category, budget_pax, budget_rev, prev_pax, prev_rev`입니다. 추가 `label` 열은 읽기 편의를 위한 것으로 계산에 사용하지 않습니다.
- `venue`: `mangilao`, `talofofo`.
- `category`: 화면 분류 ID. `total`은 골프장 전체 목표입니다.
- budget은 목표/예산, prev는 동일 월 전년 실적. REV 단위 USD.
- 빈칸은 미입력, 0은 실제 0입니다. 비율의 분모가 0이면 화면에 —를 표시합니다.
- 상위 합계 목표와 세부 목표는 별도 입력입니다. 세부 목표를 바꿀 때 해당 상위 목표도 확인하세요. 기존 엑셀의 고정값 구조를 유지합니다.
- 입력 셀은 숫자로 저장하세요. 수식을 사용할 경우 Excel에서 재계산 후 저장해야 캐시된 결과를 읽을 수 있습니다.

## 재현 기준과 차이
예약 Id마다 1라운드, Start date의 **연도와 월**, 정확한 resourceTypeName, clientName/clientGroupName/Channel을 사용합니다. 원본 SUMIFS의 각각의 조건과 소노 회원에서 J&J 회원을 차감하는 규칙을 그대로 계산합니다. 조건이 중복되거나 차감 범위가 잘못되면 원본 규칙의 합계와 원천의 독립 합계를 대조해 경고합니다. 미분류는 별도 행으로 표시하여 누락을 드러냅니다.

중복 Id, 잘못된 원천, 빈 날짜, 잘못된 취소 값, 비정상 금액은 업데이트를 중지합니다. 오류 시 기존 현재 JSON은 유지합니다. 생성일·결제·체크인 여부는 집계 조건이 아닙니다. 식당·스파는 제외합니다. 소스의 필터가 올바른지는 Excel에 저장되지 않으므로 내려받을 때 확인해야 합니다.

기준일과 조회 범위가 같은 형식으로 이력에 저장됩니다. 동일 조회 범위의 가장 최근 **이전 날짜**와만 비교합니다. 같은 날 재업로드는 당일 이력을 교체합니다. 이전 이력에는 해당 날짜의 목표도 저장하지만 증감은 예약 실적만 비교합니다. 다른 기간이나 최신일보다 오래된 파일은 합치지 않습니다.

기존 보고서 Total은 9~11월이며 이 범위는 오른쪽 로직 패널의 참고 합계로 유지합니다. `historical_months`에 원본 9월 고정값이 있을 때만 계산합니다. 주 리포트의 전체는 사용자가 선택한 이용월입니다. 잘못된 원본 Z14 인원 참조는 실제 매출 경로에 적용하지 않습니다. 스냅샷을 새로 받으면 과거 실적과 미래 예약이 함께 갱신됩니다.

원본 Nationality는 변경하지 않습니다. UU에 대해 원본 고객명/그룹/채널 규칙으로 분석용 판매시장과 근거를 표시합니다. 국적을 이름·이메일로 추정하지 않습니다. 목표/전년 입력과 실적 보정은 별개이며 자동 실적을 임의 숫자로 보강하지 않습니다.

## 통합과 게시
기존 괌 대시보드에는 별도 USD 리포트 링크를 추가합니다. 기존 원화 실적 데이터·계산은 수정하지 않습니다. 원본 Excel, 고객별 개인정보, 입력 파일과 스냅샷 이력은 로컬에 보관합니다. 사용자가 웹 반영을 요청한 집계 JSON만 게시합니다. 공개 GitHub Pages의 집계 JSON은 공개 접근 가능하며 기존 브라우저 로그인 가드는 JSON 접근 자체를 보호하지 않습니다.

## 검증
```sh
python3 -m unittest discover -s scripts/guam_onbook/tests -v
node --check docs/js/guam-onbook.js
node scripts/guam_onbook/tests/test_report.cjs
```

초기 2026-10-07 보고서의 월별·골프장별·분류별 PAX/REV 612개 값을 대조했으며 모두 일치했습니다. 4,247 원천행 중 골프 3,787행, 기타 시설 460행, 미분류 0행입니다.
