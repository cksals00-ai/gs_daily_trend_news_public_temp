/**
 * 품의 작성용 — Google Apps Script 웹앱
 *
 * 설치 방법:
 * 1. 구글시트 열기 (액션플랜 시트 또는 새 시트)
 * 2. 확장 프로그램 > Apps Script
 * 3. 아래 코드를 Code.gs에 붙여넣기
 * 4. PREFERRED_SHEET_NAME을 원하는 기본 시트명으로 수정 (선택)
 * 5. 배포 > 새 배포 > 웹 앱
 *    - 실행 사용자: 나
 *    - 액세스 권한: 모든 사용자
 * 6. 배포 URL 복사 → pumui.html 하단 Apps Script URL에 붙여넣기
 *
 * 시트명이 변경되어도 헤더 시그니처(아래 SIGNATURE_HEADERS)로 자동 탐색합니다.
 */

// 신규 시트 생성 시 사용할 기본 시트명 (기존 시트가 헤더 시그니처에 매칭되면 그걸 사용)
const PREFERRED_SHEET_NAME = '품의입력';

// 전체 헤더 (신규 시트 생성 시 1행에 기록)
const SHEET_HEADERS = [
  '제출일시', '사업장', '채널', '판매일자', '투숙일자',
  '상품', '상품비고',
  '객실_회원_정상기_정상가', '객실_회원_비수기_정상가', '객실_FIT_정상기_정상가', '객실_FIT_비수기_정상가',
  '객실_회원_정상기_배분가', '객실_회원_비수기_배분가', '객실_FIT_정상기_배분가', '객실_FIT_비수기_배분가',
  '객실_할인_회원정상기', '객실_할인_회원비수기', '객실_할인_FIT정상기', '객실_할인_FIT비수기',
  '화원가比',
  '식음_조식_정상기', '식음_석식_정상기', '식음_오션_정상기', '식음_레전드_정상기',
  '식음_조식_배분기', '식음_석식_배분기', '식음_오션_배분기', '식음_레전드_배분기',
  '식음_할인_조식', '식음_할인_석식', '식음_할인_오션', '식음_할인_레전드',
  '식음_비고',
  '판매가_회원_정상기', '판매가_FIT_정상기',
  '판매가_회원_배분기', '판매가_FIT_배분기',
  '판매가_할인_회원', '판매가_할인_FIT',
  '수수료', 'KPI', '비고'
];

// 시트 자동탐색용 핵심 시그니처 헤더 (1행이 이 헤더들을 모두 포함하면 품의 시트로 간주)
// — 시트명이 바뀌어도 헤더 패턴이 같으면 매칭됨
const SIGNATURE_HEADERS = ['제출일시', '사업장', '채널', '판매일자', '투숙일자', '상품'];

/**
 * 1행 헤더가 SIGNATURE_HEADERS를 모두 포함하면 true
 */
function matchesPumuiSignature_(sheet) {
  const lastCol = sheet.getLastColumn();
  if (lastCol < SIGNATURE_HEADERS.length) return false;
  const header = sheet.getRange(1, 1, 1, lastCol).getValues()[0]
    .map(function(v) { return (v == null ? '' : String(v)).trim(); });
  // 모든 시그니처 헤더가 1행 어딘가에 존재해야 함 (순서 무관)
  return SIGNATURE_HEADERS.every(function(h) { return header.indexOf(h) >= 0; });
}

/**
 * 품의 시트를 찾는다.
 *  1) PREFERRED_SHEET_NAME으로 직접 조회 → 헤더 시그니처 일치하면 채택
 *  2) 그 외 모든 시트를 순회하며 헤더 시그니처 매칭
 *  3) 어떤 시트도 매칭되지 않으면 PREFERRED_SHEET_NAME으로 새로 생성하고 헤더 기록
 */
function resolvePumuiSheet_(ss) {
  // 1) 선호 이름 먼저 시도
  const named = ss.getSheetByName(PREFERRED_SHEET_NAME);
  if (named && named.getLastRow() >= 1 && matchesPumuiSignature_(named)) {
    return named;
  }

  // 2) 전체 시트 순회 (이름이 바뀐 경우 대비)
  const sheets = ss.getSheets();
  for (let i = 0; i < sheets.length; i++) {
    const s = sheets[i];
    if (s.getLastRow() < 1) continue;
    if (matchesPumuiSignature_(s)) {
      return s;
    }
  }

  // 3) 신규 생성: 선호 이름이 이미 점유되어 있으면 비어있는 동명 시트일 수 있음 → 헤더만 채워서 사용
  let sheet = named;
  if (!sheet) {
    sheet = ss.insertSheet(PREFERRED_SHEET_NAME);
  }
  sheet.getRange(1, 1, 1, SHEET_HEADERS.length).setValues([SHEET_HEADERS]);
  sheet.getRange(1, 1, 1, SHEET_HEADERS.length).setFontWeight('bold');
  sheet.setFrozenRows(1);
  return sheet;
}

function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = resolvePumuiSheet_(ss);

    const row = [
      data.submittedAt || new Date().toISOString(),
      data.property, data.channel, data.salePeriod, data.stayPeriod,
      data.product, data.productNote,
      data.roomMemPeak, data.roomMemOff, data.roomFitPeak, data.roomFitOff,
      data.roomMemPeakDist, data.roomMemOffDist, data.roomFitPeakDist, data.roomFitOffDist,
      data.roomDiscMemPeak, data.roomDiscMemOff, data.roomDiscFitPeak, data.roomDiscFitOff,
      data.memberRatio,
      data.fbBkPeak, data.fbDnPeak, data.fbOceanPeak, data.fbLegendPeak,
      data.fbBkOff, data.fbDnOff, data.fbOceanOff, data.fbLegendOff,
      data.fbDiscBk, data.fbDiscDn, data.fbDiscOcean, data.fbDiscLegend,
      data.fbNote,
      data.sellMemPeak, data.sellFitPeak,
      data.sellMemOff, data.sellFitOff,
      data.sellDiscMem, data.sellDiscFit,
      data.commission, data.kpi, data.remarks
    ];

    sheet.appendRow(row);

    return ContentService
      .createTextOutput(JSON.stringify({
        status: 'ok',
        sheet: sheet.getName(),
        gid: sheet.getSheetId(),
        row: sheet.getLastRow()
      }))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    return ContentService
      .createTextOutput(JSON.stringify({ status: 'error', message: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function doGet(e) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = resolvePumuiSheet_(ss);
    return ContentService
      .createTextOutput(JSON.stringify({
        status: 'ok',
        message: '품의 입력 웹앱이 정상 동작 중입니다.',
        sheet: sheet.getName(),
        gid: sheet.getSheetId()
      }))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService
      .createTextOutput(JSON.stringify({ status: 'error', message: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}
