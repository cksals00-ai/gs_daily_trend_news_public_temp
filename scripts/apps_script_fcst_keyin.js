/**
 * FCST 키인용 — Google Apps Script 웹앱
 *
 * 설치 방법:
 * 1. 구글시트 새로 만들기 (예: "FCST 키인 중앙저장소")
 * 2. 확장 프로그램 > Apps Script
 * 3. 아래 코드 전체를 Code.gs에 붙여넣기
 * 4. (선택) PREFERRED_SHEET_NAME 변경
 * 5. 배포 > 새 배포 > 웹 앱
 *    - 실행 사용자: 나
 *    - 액세스 권한: 모든 사용자
 * 6. 배포 URL 복사 → fcst-admin.html 상단 설정에 붙여넣기
 *
 * 시트 구조 (자동 생성):
 *  timestamp | week | month | property | segment | metric | value | updated_by
 *
 * 동작:
 *  - POST {entries:[{week, month, property, segment, metric, value, updated_by?}, ...]}
 *    → 같은 (week, month, property, segment, metric) 조합 있으면 update, 없으면 append.
 *    POST {week, month, property, ...} 단건도 지원.
 *  - GET ?week=YYYY-Www (선택) → 해당 주차만 (없으면 전체) JSON 배열로 반환.
 */

const PREFERRED_SHEET_NAME = 'FCST_KEYIN';

const SHEET_HEADERS = [
  'timestamp', 'week', 'month', 'property', 'segment', 'metric', 'value', 'updated_by'
];

const SIGNATURE_HEADERS = ['timestamp', 'week', 'month', 'property', 'segment', 'metric', 'value'];

function matchesSignature_(sheet) {
  const lastCol = sheet.getLastColumn();
  if (lastCol < SIGNATURE_HEADERS.length) return false;
  const header = sheet.getRange(1, 1, 1, lastCol).getValues()[0]
    .map(function (v) { return (v == null ? '' : String(v)).trim(); });
  return SIGNATURE_HEADERS.every(function (h) { return header.indexOf(h) >= 0; });
}

function resolveSheet_(ss) {
  const named = ss.getSheetByName(PREFERRED_SHEET_NAME);
  if (named && named.getLastRow() >= 1 && matchesSignature_(named)) return named;

  const sheets = ss.getSheets();
  for (let i = 0; i < sheets.length; i++) {
    const s = sheets[i];
    if (s.getLastRow() < 1) continue;
    if (matchesSignature_(s)) return s;
  }

  let sheet = named || ss.insertSheet(PREFERRED_SHEET_NAME);
  sheet.getRange(1, 1, 1, SHEET_HEADERS.length).setValues([SHEET_HEADERS]);
  sheet.getRange(1, 1, 1, SHEET_HEADERS.length).setFontWeight('bold');
  sheet.setFrozenRows(1);
  return sheet;
}

function colIndex_(headerRow, name) {
  const i = headerRow.indexOf(name);
  return i >= 0 ? i : -1;
}

function readAll_(sheet) {
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow < 2) return { header: SHEET_HEADERS.slice(), rows: [] };
  const all = sheet.getRange(1, 1, lastRow, lastCol).getValues();
  const header = all[0].map(function (v) { return (v == null ? '' : String(v)).trim(); });
  const rows = all.slice(1);
  return { header: header, rows: rows };
}

function rowKey_(row, idx) {
  return [
    String(row[idx.week] || ''),
    String(row[idx.month] || ''),
    String(row[idx.property] || ''),
    String(row[idx.segment] || ''),
    String(row[idx.metric] || '')
  ].join('||');
}

function entryKey_(entry) {
  return [
    String(entry.week || ''),
    String(entry.month || ''),
    String(entry.property || ''),
    String(entry.segment || ''),
    String(entry.metric || '')
  ].join('||');
}

function upsertEntries_(sheet, entries) {
  const { header, rows } = readAll_(sheet);
  const idx = {
    timestamp: colIndex_(header, 'timestamp'),
    week: colIndex_(header, 'week'),
    month: colIndex_(header, 'month'),
    property: colIndex_(header, 'property'),
    segment: colIndex_(header, 'segment'),
    metric: colIndex_(header, 'metric'),
    value: colIndex_(header, 'value'),
    updated_by: colIndex_(header, 'updated_by')
  };

  // index existing rows by composite key (sheet-row-number 2-based)
  const keyToRowNum = {};
  for (let i = 0; i < rows.length; i++) {
    keyToRowNum[rowKey_(rows[i], idx)] = i + 2;
  }

  const nowIso = new Date().toISOString();
  let updated = 0, inserted = 0;
  const toAppend = [];

  entries.forEach(function (entry) {
    if (!entry || entry.value === undefined || entry.value === null || entry.value === '') return;
    const key = entryKey_(entry);
    const ts = entry.timestamp || nowIso;
    const row = new Array(header.length).fill('');
    row[idx.timestamp] = ts;
    row[idx.week] = entry.week || '';
    row[idx.month] = entry.month || '';
    row[idx.property] = entry.property || '';
    row[idx.segment] = entry.segment || '';
    row[idx.metric] = entry.metric || '';
    row[idx.value] = entry.value;
    if (idx.updated_by >= 0) row[idx.updated_by] = entry.updated_by || '';

    if (keyToRowNum[key]) {
      const rowNum = keyToRowNum[key];
      sheet.getRange(rowNum, 1, 1, header.length).setValues([row]);
      updated++;
    } else {
      toAppend.push(row);
      keyToRowNum[key] = -1; // mark seen
      inserted++;
    }
  });

  if (toAppend.length) {
    sheet.getRange(sheet.getLastRow() + 1, 1, toAppend.length, header.length).setValues(toAppend);
  }

  return { updated: updated, inserted: inserted };
}

function doPost(e) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(20000);
  } catch (_) { /* best effort */ }
  try {
    const body = JSON.parse(e.postData.contents);
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = resolveSheet_(ss);

    let entries = [];
    if (Array.isArray(body.entries)) entries = body.entries;
    else if (body.week && body.property) entries = [body];
    else if (Array.isArray(body)) entries = body;

    const result = upsertEntries_(sheet, entries);

    return ContentService
      .createTextOutput(JSON.stringify({
        status: 'ok',
        sheet: sheet.getName(),
        gid: sheet.getSheetId(),
        inserted: result.inserted,
        updated: result.updated,
        total_rows: sheet.getLastRow() - 1
      }))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService
      .createTextOutput(JSON.stringify({ status: 'error', message: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  } finally {
    try { lock.releaseLock(); } catch (_) { /* noop */ }
  }
}

function doGet(e) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = resolveSheet_(ss);
    const { header, rows } = readAll_(sheet);
    const idx = {};
    SHEET_HEADERS.forEach(function (h) { idx[h] = colIndex_(header, h); });

    const wantWeek = e && e.parameter && e.parameter.week ? String(e.parameter.week) : '';
    const out = [];
    rows.forEach(function (r) {
      if (idx.week < 0 || idx.property < 0) return;
      const w = String(r[idx.week] || '');
      if (wantWeek && w !== wantWeek) return;
      out.push({
        timestamp: idx.timestamp >= 0 ? String(r[idx.timestamp] || '') : '',
        week: w,
        month: idx.month >= 0 ? String(r[idx.month] || '') : '',
        property: idx.property >= 0 ? String(r[idx.property] || '') : '',
        segment: idx.segment >= 0 ? String(r[idx.segment] || '') : '',
        metric: idx.metric >= 0 ? String(r[idx.metric] || '') : '',
        value: idx.value >= 0 ? r[idx.value] : null,
        updated_by: idx.updated_by >= 0 ? String(r[idx.updated_by] || '') : ''
      });
    });

    return ContentService
      .createTextOutput(JSON.stringify({
        status: 'ok',
        sheet: sheet.getName(),
        gid: sheet.getSheetId(),
        count: out.length,
        entries: out
      }))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService
      .createTextOutput(JSON.stringify({ status: 'error', message: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}
