/**
 * 품의 작성용 — Google Apps Script 웹앱
 *
 * 설치 방법:
 * 1. 구글시트 열기 (액션플랜 시트 또는 새 시트)
 * 2. 확장 프로그램 > Apps Script
 * 3. 아래 코드를 Code.gs에 붙여넣기
 * 4. SHEET_NAME을 원하는 시트명으로 수정
 * 5. 배포 > 새 배포 > 웹 앱
 *    - 실행 사용자: 나
 *    - 액세스 권한: 모든 사용자
 * 6. 배포 URL 복사 → pumui.html 하단 Apps Script URL에 붙여넣기
 */

const SHEET_NAME = '품의입력';  // ← 원하는 시트명으로 변경

function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(SHEET_NAME);

    // 시트 없으면 자동 생성 + 헤더
    if (!sheet) {
      sheet = ss.insertSheet(SHEET_NAME);
      const headers = [
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
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      sheet.getRange(1, 1, 1, headers.length).setFontWeight('bold');
      sheet.setFrozenRows(1);
    }

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
      .createTextOutput(JSON.stringify({ status: 'ok', row: sheet.getLastRow() }))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    return ContentService
      .createTextOutput(JSON.stringify({ status: 'error', message: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function doGet(e) {
  return ContentService
    .createTextOutput(JSON.stringify({ status: 'ok', message: '품의 입력 웹앱이 정상 동작 중입니다.' }))
    .setMimeType(ContentService.MimeType.JSON);
}
