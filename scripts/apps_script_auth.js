/**
 * 인증/회원관리용 Google Apps Script 웹앱
 * ============================================
 *
 * 설치 방법:
 *   1) 구글시트 새로 만들기 (예: "GSN 회원 DB")
 *   2) 확장 프로그램 > Apps Script
 *   3) 본 파일 전체를 Code.gs에 붙여넣기
 *   4) (필수) AUTH_SECRET 값을 강력한 임의 문자열로 교체  ← 노출되지 않게!
 *   5) (선택) GOOGLE_CLIENT_ID 설정 (구글 OAuth 사용 시)
 *   6) 배포 > 새 배포 > 웹 앱
 *      - 실행 사용자: 나 (Me)
 *      - 액세스 권한: 모든 사용자
 *   7) 배포 URL 복사 → docs/js/auth.js 의 AUTH_API_URL 에 붙여넣기
 *
 * 시트 구조 (USERS, 자동 생성):
 *   email | name | role | password_hash | status | created_at | last_login
 *
 *   - role:   admin / sales / executive / external
 *   - status: active / inactive
 *   - password_hash: SHA-256(password + email)
 *     · 구글 로그인 전용 사용자는 빈칸으로 두면 됨
 *
 * 라우팅(쿼리 파라미터 `action`):
 *   GET  ?action=me         & token=...                   → 내 정보 + 권한
 *   GET  ?action=users      & token=...                   → 회원 목록 (admin)
 *   GET  ?action=load_admin & token=...                   → 데일리 키인(admin_input) 최신 로드
 *   POST ?action=login      body: {email, password|google_id_token}
 *   POST ?action=users      & token=... body: {email, name, role, status, password?}
 *   POST ?action=delete     & token=... body: {email}     → soft delete (status=inactive)
 *   POST ?action=save_admin body: {token, data}           → 데일리 키인(admin_input) 저장 (admin)
 *
 *  ※ Apps Script 는 응답 헤더(CORS) 제어가 제한적이라 token 은 query 또는 body 로 받음.
 */

// ============================================================================
// 설정
// ============================================================================
const AUTH_SECRET       = 'CHANGE-ME-TO-A-LONG-RANDOM-STRING';     // ⚠️ 반드시 변경
const GOOGLE_CLIENT_ID  = '';                                       // 선택. 빈 값이면 검증 시 audience 확인 생략
const TOKEN_TTL_HOURS   = 24;                                       // 토큰 유효시간

const SHEET_NAME = 'USERS';
const HEADERS = ['email', 'name', 'role', 'password_hash', 'status', 'created_at', 'last_login'];
const VALID_ROLES = ['admin', 'sales', 'executive', 'external'];

// 초기 관리자 시드 (시트가 비어있을 때만 자동 등록)
const SEED_ADMINS = [
  { email: 'cksals00@gmail.com', name: '관리자', role: 'admin' }
];

// ============================================================================
// 시트 헬퍼
// ============================================================================
function getSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(SHEET_NAME);
  if (!sh) {
    sh = ss.insertSheet(SHEET_NAME);
    sh.getRange(1, 1, 1, HEADERS.length).setValues([HEADERS]);
    sh.getRange(1, 1, 1, HEADERS.length).setFontWeight('bold');
    sh.setFrozenRows(1);
  } else if (sh.getLastRow() < 1) {
    sh.getRange(1, 1, 1, HEADERS.length).setValues([HEADERS]);
    sh.getRange(1, 1, 1, HEADERS.length).setFontWeight('bold');
    sh.setFrozenRows(1);
  }
  seedAdmins_(sh);
  return sh;
}

function seedAdmins_(sh) {
  if (sh.getLastRow() > 1) return; // 이미 사용자 존재 → 시드 안함
  const now = new Date().toISOString();
  const rows = SEED_ADMINS.map(function (a) {
    return [a.email, a.name, a.role, '', 'active', now, ''];
  });
  if (rows.length) {
    sh.getRange(2, 1, rows.length, HEADERS.length).setValues(rows);
  }
}

function readAll_(sh) {
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return [];
  const data = sh.getRange(2, 1, lastRow - 1, HEADERS.length).getValues();
  return data.map(function (r, i) {
    return {
      _row: i + 2,
      email: String(r[0] || '').trim().toLowerCase(),
      name: String(r[1] || ''),
      role: String(r[2] || '').trim(),
      password_hash: String(r[3] || ''),
      status: String(r[4] || 'active').trim(),
      created_at: String(r[5] || ''),
      last_login: String(r[6] || '')
    };
  });
}

function findUser_(sh, email) {
  const target = String(email || '').trim().toLowerCase();
  if (!target) return null;
  const all = readAll_(sh);
  for (let i = 0; i < all.length; i++) {
    if (all[i].email === target) return all[i];
  }
  return null;
}

function updateLastLogin_(sh, rowNum) {
  sh.getRange(rowNum, 7).setValue(new Date().toISOString());
}

function upsertUser_(sh, user) {
  const existing = findUser_(sh, user.email);
  const now = new Date().toISOString();
  if (existing) {
    const row = existing._row;
    const newRow = [
      existing.email,
      user.name !== undefined ? user.name : existing.name,
      user.role !== undefined ? user.role : existing.role,
      user.password_hash !== undefined ? user.password_hash : existing.password_hash,
      user.status !== undefined ? user.status : existing.status,
      existing.created_at || now,
      existing.last_login || ''
    ];
    sh.getRange(row, 1, 1, HEADERS.length).setValues([newRow]);
    return { updated: true, email: existing.email };
  } else {
    const newRow = [
      String(user.email || '').trim().toLowerCase(),
      user.name || '',
      user.role || 'executive',
      user.password_hash || '',
      user.status || 'active',
      now,
      ''
    ];
    sh.appendRow(newRow);
    return { inserted: true, email: newRow[0] };
  }
}

// ============================================================================
// 토큰 / 해시
// ============================================================================
function sha256_(str) {
  const raw = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, str, Utilities.Charset.UTF_8);
  let hex = '';
  for (let i = 0; i < raw.length; i++) {
    const b = (raw[i] + 256) & 0xff;
    hex += (b < 16 ? '0' : '') + b.toString(16);
  }
  return hex;
}

function hashPassword_(password, email) {
  return sha256_(String(password) + '|' + String(email).toLowerCase());
}

function base64UrlEncode_(str) {
  return Utilities.base64EncodeWebSafe(str).replace(/=+$/, '');
}

function base64UrlDecode_(str) {
  let s = str.replace(/-/g, '+').replace(/_/g, '/');
  while (s.length % 4) s += '=';
  return Utilities.newBlob(Utilities.base64Decode(s)).getDataAsString('UTF-8');
}

function signToken_(payload) {
  const body = base64UrlEncode_(JSON.stringify(payload));
  const sig = sha256_(body + '|' + AUTH_SECRET).substring(0, 32);
  return body + '.' + sig;
}

function verifyToken_(token) {
  if (!token || typeof token !== 'string') return null;
  const parts = token.split('.');
  if (parts.length !== 2) return null;
  const expectSig = sha256_(parts[0] + '|' + AUTH_SECRET).substring(0, 32);
  if (expectSig !== parts[1]) return null;
  let payload;
  try {
    payload = JSON.parse(base64UrlDecode_(parts[0]));
  } catch (e) { return null; }
  if (!payload || !payload.exp || payload.exp < Date.now()) return null;
  return payload;
}

function authedUser_(sh, token) {
  const payload = verifyToken_(token);
  if (!payload) return null;
  const user = findUser_(sh, payload.email);
  if (!user || user.status !== 'active') return null;
  return user;
}

// ============================================================================
// 구글 OAuth 검증
// ============================================================================
function verifyGoogleIdToken_(idToken) {
  try {
    const res = UrlFetchApp.fetch(
      'https://oauth2.googleapis.com/tokeninfo?id_token=' + encodeURIComponent(idToken),
      { muteHttpExceptions: true }
    );
    if (res.getResponseCode() !== 200) return null;
    const info = JSON.parse(res.getContentText());
    if (info.error) return null;
    if (GOOGLE_CLIENT_ID && info.aud !== GOOGLE_CLIENT_ID) return null;
    const now = Math.floor(Date.now() / 1000);
    if (info.exp && Number(info.exp) < now) return null;
    return { email: String(info.email || '').toLowerCase(), name: info.name || '', email_verified: info.email_verified === 'true' || info.email_verified === true };
  } catch (e) {
    return null;
  }
}

// ============================================================================
// 응답 헬퍼
// ============================================================================
function jsonOut_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function ok_(extra) {
  const out = { status: 'ok' };
  if (extra) Object.keys(extra).forEach(function (k) { out[k] = extra[k]; });
  return jsonOut_(out);
}
function err_(message, code) {
  return jsonOut_({ status: 'error', message: message || 'error', code: code || 'error' });
}

function userPublic_(u) {
  if (!u) return null;
  return { email: u.email, name: u.name, role: u.role, status: u.status, created_at: u.created_at, last_login: u.last_login };
}

// ============================================================================
// 액션 핸들러
// ============================================================================
function handleLogin_(body) {
  const sh = getSheet_();
  const email = String(body.email || '').trim().toLowerCase();
  let user = null;

  if (body.google_id_token) {
    const info = verifyGoogleIdToken_(body.google_id_token);
    if (!info || !info.email) return err_('구글 토큰 검증 실패', 'invalid_google_token');
    user = findUser_(sh, info.email);
    if (!user) return err_('등록되지 않은 사용자입니다. 관리자에게 문의하세요.', 'not_registered');
    if (user.status !== 'active') return err_('비활성화된 계정입니다.', 'inactive');
  } else if (email && body.password) {
    user = findUser_(sh, email);
    if (!user) return err_('이메일 또는 비밀번호가 올바르지 않습니다.', 'invalid_credentials');
    if (user.status !== 'active') return err_('비활성화된 계정입니다.', 'inactive');
    if (!user.password_hash) return err_('비밀번호가 설정되어 있지 않습니다. 구글 로그인 또는 관리자 문의.', 'no_password');
    const want = hashPassword_(body.password, user.email);
    if (want !== user.password_hash) return err_('이메일 또는 비밀번호가 올바르지 않습니다.', 'invalid_credentials');
  } else {
    return err_('email/password 또는 google_id_token 이 필요합니다.', 'bad_request');
  }

  updateLastLogin_(sh, user._row);

  const payload = {
    email: user.email,
    role: user.role,
    name: user.name,
    iat: Date.now(),
    exp: Date.now() + TOKEN_TTL_HOURS * 3600 * 1000
  };
  const token = signToken_(payload);

  return ok_({ token: token, exp: payload.exp, user: userPublic_(user) });
}

function handleMe_(params) {
  const sh = getSheet_();
  const u = authedUser_(sh, params.token);
  if (!u) return err_('인증 실패 또는 만료된 토큰', 'unauthorized');
  return ok_({ user: userPublic_(u) });
}

function handleListUsers_(params) {
  const sh = getSheet_();
  const me = authedUser_(sh, params.token);
  if (!me) return err_('인증 실패', 'unauthorized');
  if (me.role !== 'admin') return err_('권한 없음', 'forbidden');
  const all = readAll_(sh).map(userPublic_);
  return ok_({ users: all, count: all.length });
}

function handleUpsertUser_(body, params) {
  const sh = getSheet_();
  const me = authedUser_(sh, params.token || body.token);
  if (!me) return err_('인증 실패', 'unauthorized');
  if (me.role !== 'admin') return err_('권한 없음', 'forbidden');

  const email = String(body.email || '').trim().toLowerCase();
  if (!email) return err_('email 필수', 'bad_request');
  if (body.role && VALID_ROLES.indexOf(body.role) < 0) return err_('잘못된 role', 'bad_request');
  if (body.status && ['active', 'inactive'].indexOf(body.status) < 0) return err_('잘못된 status', 'bad_request');

  const patch = { email: email };
  if (body.name !== undefined) patch.name = body.name;
  if (body.role !== undefined) patch.role = body.role;
  if (body.status !== undefined) patch.status = body.status;
  if (body.password) patch.password_hash = hashPassword_(body.password, email);
  else if (body.clear_password) patch.password_hash = '';

  const r = upsertUser_(sh, patch);
  return ok_(r);
}

function handleDeleteUser_(body, params) {
  const sh = getSheet_();
  const me = authedUser_(sh, params.token || body.token);
  if (!me) return err_('인증 실패', 'unauthorized');
  if (me.role !== 'admin') return err_('권한 없음', 'forbidden');
  const email = String(body.email || '').trim().toLowerCase();
  if (!email) return err_('email 필수', 'bad_request');
  if (email === me.email) return err_('본인 계정은 비활성화할 수 없습니다.', 'self_delete');
  const u = findUser_(sh, email);
  if (!u) return err_('대상 사용자를 찾을 수 없음', 'not_found');
  sh.getRange(u._row, 5).setValue('inactive');
  return ok_({ email: email, status: 'inactive' });
}

// ============================================================================
// 데일리 키인(admin_input) 저장소  — docs/admin.html 「⬆ 반영」 대상
// 단일 문서(최신 상태)를 ADMIN_INPUT 시트 2행(json 셀)에 덮어쓰기 저장.
// ============================================================================
const ADMIN_INPUT_SHEET   = 'ADMIN_INPUT';
const ADMIN_INPUT_HEADERS = ['updated_at', 'updated_by', 'json'];
const ADMIN_INPUT_MAX_LEN = 45000; // 시트 셀 한도(약 5만자) 여유

function getAdminInputSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(ADMIN_INPUT_SHEET);
  if (!sh) {
    sh = ss.insertSheet(ADMIN_INPUT_SHEET);
    sh.getRange(1, 1, 1, ADMIN_INPUT_HEADERS.length).setValues([ADMIN_INPUT_HEADERS]);
    sh.getRange(1, 1, 1, ADMIN_INPUT_HEADERS.length).setFontWeight('bold');
    sh.setFrozenRows(1);
  }
  return sh;
}

function handleSaveAdmin_(body) {
  const sh   = getSheet_();
  const user = authedUser_(sh, body.token);
  if (!user) return err_('인증이 만료되었습니다. 다시 로그인해주세요.', 'unauthorized');
  if (user.role !== 'admin') return err_('권한이 없습니다(admin 전용).', 'forbidden');

  const data = body.data;
  if (data === undefined || data === null) return err_('data 가 비어 있습니다.', 'bad_request');

  let json;
  try { json = JSON.stringify(data); } catch (e) { return err_('data 직렬화 실패', 'bad_request'); }
  if (json.length > ADMIN_INPUT_MAX_LEN) return err_('데이터가 너무 큽니다(> 45KB).', 'too_large');

  const now = new Date().toISOString();
  const ash = getAdminInputSheet_();
  ash.getRange(2, 1, 1, ADMIN_INPUT_HEADERS.length).setValues([[now, user.email, json]]);
  return ok_({ updated_at: now, updated_by: user.email });
}

function handleLoadAdmin_(params) {
  const sh   = getSheet_();
  const user = authedUser_(sh, params.token);
  if (!user) return err_('인증이 만료되었습니다. 다시 로그인해주세요.', 'unauthorized');

  const ash = getAdminInputSheet_();
  if (ash.getLastRow() < 2) return ok_({ data: null });
  const row = ash.getRange(2, 1, 1, ADMIN_INPUT_HEADERS.length).getValues()[0];
  let data = null;
  try { data = row[2] ? JSON.parse(row[2]) : null; } catch (e) { data = null; }
  return ok_({ data: data, updated_at: row[0], updated_by: row[1] });
}

// ============================================================================
// 엔트리포인트
// ============================================================================
function doGet(e) {
  try {
    const p = (e && e.parameter) || {};
    const action = String(p.action || 'me');
    if (action === 'me')         return handleMe_(p);
    if (action === 'users')      return handleListUsers_(p);
    if (action === 'load_admin') return handleLoadAdmin_(p);
    if (action === 'ping')       return ok_({ pong: true });
    return err_('알 수 없는 action: ' + action, 'bad_request');
  } catch (e) {
    return err_(e.toString(), 'internal');
  }
}

function doPost(e) {
  const lock = LockService.getScriptLock();
  try { lock.waitLock(15000); } catch (_) { /* best effort */ }
  try {
    const p = (e && e.parameter) || {};
    let body = {};
    try {
      if (e && e.postData && e.postData.contents) body = JSON.parse(e.postData.contents) || {};
    } catch (_) { body = {}; }
    const action = String(p.action || body.action || 'login');
    if (action === 'login')      return handleLogin_(body);
    if (action === 'users')      return handleUpsertUser_(body, p);
    if (action === 'delete')     return handleDeleteUser_(body, p);
    if (action === 'save_admin') return handleSaveAdmin_(body);
    return err_('알 수 없는 action: ' + action, 'bad_request');
  } catch (e) {
    return err_(e.toString(), 'internal');
  } finally {
    try { lock.releaseLock(); } catch (_) { /* noop */ }
  }
}
