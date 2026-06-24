/**
 * GS 요금표 → 품의 배분가(안) 변환 백엔드 (Cloudflare Worker)
 * ==========================================================
 *
 * 역할:
 *   - 프론트(docs/rate-converter.html)가 보낸 "요금표 시트 텍스트"를 받아
 *     Claude(Opus 4.8)에게 정규화된 숫자(시즌별 객실/부대 정상·배분가)를 추출시키고
 *     구조화 JSON 으로 돌려준다. 배분가 표의 수식(정상/배분/할인율/GAP/판매가)은
 *     프론트가 결정론적으로 계산한다.
 *
 * 보안/한도:
 *   - gsn_auth_token(JWT 유사 토큰) 을 AUTH_SECRET 으로 검증 → sales/admin 만 허용
 *   - 하루 변환 횟수 상한(DAILY_MAX) + 월 예산 상한(MONTHLY_BUDGET_USD) 을 KV 로 카운트
 *
 * 배포: workers/rate-converter/README.md 참고.
 */

const ANTHROPIC_URL = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION = "2023-06-01";

// 토큰 가격 (USD / 1M tokens) — 비용 추정용. claude-opus-4-8 기준.
const PRICE = { input: 5, output: 25, cacheWrite: 6.25, cacheRead: 0.5 };

// 추출 스키마: Claude 가 이 형태의 JSON 만 반환하도록 강제 (structured outputs)
const OUTPUT_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    meta: {
      type: "object",
      additionalProperties: false,
      properties: {
        biz: { type: "string", description: "사업장명 (예: 비발디파크)" },
        channel: { type: "string", description: "판매채널 (예: 여기어때)" },
        feePct: { type: "string", description: "수수료 %, 숫자만 (예: 13)" },
        pax: { type: "integer", description: "부대 인원 기준 (보통 2 또는 3)" },
        stayPeriod: { type: "string", description: "투숙기간 문구" },
        productComposition: { type: "string", description: "상품구성 문구" },
        paxBasis: { type: "string", description: "객실 배분가 인원기준을 성인/소인으로 구체화 (예: '성인2인 + 소인1인'). '3인'처럼 뭉뚱그리지 말 것. 모르면 사용자 입력값 사용." },
      },
      required: ["biz", "channel", "feePct", "pax", "stayPeriod", "productComposition", "paxBasis"],
    },
    // 4. 배분가 원천: 시즌 × 객실라인 단위의 raw 행. 금액은 KRW 원(전체값, 천원 아님).
    rows: {
      type: "array",
      description: "객실 요금 행. 시즌별로 여러 객실타입/기간 행이 있을 수 있음.",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          season: { type: "string", description: "시즌 라벨. 다음 중 하나로 정규화: 주중/금요일/토요일/7-16/하이/골드/스페셜. 모르면 가장 가까운 값." },
          roomMemberNormal: { type: "integer", description: "객실 무기명(회원) 정상가, 원" },
          roomFitNormal: { type: "integer", description: "객실 FIT 정상가, 원" },
          roomMemberDist: { type: "integer", description: "객실 무기명(회원) 배분가, 원" },
          roomFitDist: { type: "integer", description: "객실 FIT 배분가, 원" },
          facNormalSum: { type: "integer", description: "이 행의 부대 정상가 합계, 원 (없으면 0)" },
          facDistSum: { type: "integer", description: "이 행의 부대 배분가 합계, 원 (없으면 0)" },
        },
        required: ["season", "roomMemberNormal", "roomFitNormal", "roomMemberDist", "roomFitDist", "facNormalSum", "facDistSum"],
      },
    },
    // 5. 부대 상세 원천: 주중/주말 × 항목 단위. 회원=FIT 동일가.
    facilityRows: {
      type: "array",
      description: "부대 항목별 행. bucket 은 주중/주말, name 은 항목명(오션월드/조식/곤돌라/액티 등).",
      items: {
        type: "object",
        additionalProperties: false,
        properties: {
          bucket: { type: "string", description: "주중 또는 주말" },
          name: { type: "string", description: "부대 항목명" },
          normal: { type: "integer", description: "정상가, 원" },
          dist: { type: "integer", description: "배분가, 원" },
          paxLabel: { type: "string", description: "이 부대 항목의 기준인원을 성인/소인으로 구체화 (예: '성인2인+소인1인', '2인', '성인1인'). 요금표에 적힌 인원 기준 그대로. 정상가·배분·할인율 모두 이 인원기준이 적용됨." },
        },
        required: ["bucket", "name", "normal", "dist", "paxLabel"],
      },
    },
    notes: { type: "string", description: "추출 시 주의/가정/누락 메모 (한국어, 짧게)" },
  },
  required: ["meta", "rows", "facilityRows", "notes"],
};

const SYSTEM_PROMPT = [
  "당신은 SONO 리조트 영업기획팀의 요금표 해석 전문가입니다.",
  "입력은 '프로모션 요금표' 엑셀 시트를 텍스트로 옮긴 격자(행:열=값)입니다. 채널/사업장마다 양식이 매번 다릅니다.",
  "목표: 이 요금표에서 품의 '배분가(안)' 작성에 필요한 정규화된 숫자만 정확히 추출합니다.",
  "",
  "규칙:",
  "1) 금액은 모두 KRW '원' 전체값으로 반환합니다(예: 352000). 시트가 천원 단위면 ×1000 하세요.",
  "2) 객실: '무기명'(=회원)과 'FIT'(=비회원/일반) 두 가격대를, '정상가'와 '배분가(=배분/정산/넷)' 각각 추출합니다.",
  "   - 배분가 라벨이 명시 안 되면, 정상가 대비 낮은 정산가 컬럼을 배분가로 봅니다.",
  "3) 한 시즌에 객실타입/기간이 여러 행이면 rows 에 행을 모두 넣습니다(범위는 프론트가 계산).",
  "4) 시즌 라벨은 주중/금요일/토요일/7-16/하이/골드/스페셜 로 정규화. 연휴=스페셜. 애매하면 가장 가까운 값.",
  "5) 부대(조식/오션월드/곤돌라/음료/액티비티 등): rows 의 facNormalSum/facDistSum 은 '오션기준'(조식 별도)으로 합산하되, 조식 외 부대가 없으면 조식 포함. 부대는 회원=FIT 동일가.",
  "6) facilityRows: 부대 항목별로 주중/주말 각각 정상/배분을 1인 기준이 아니라 '표기 그대로' 넣되, 표가 1인기준이면 pax 를 곱한 값으로 넣습니다(meta.pax 도 같이 채움).",
  "7) 인원기준은 반드시 성인/소인으로 구체화합니다. '3인 기준' 같은 뭉뚱그린 표기 금지 → meta.paxBasis 는 '성인2인 + 소인1인' 처럼, 각 부대 항목의 paxLabel 도 요금표에 적힌 인원기준(성인/소인) 그대로. 정상가·배분가·할인율 모두 동일 인원기준이 적용됨을 전제로 표기합니다.",
  "8) 시트에서 실제로 읽히는 값만 사용합니다. 추정·창작 금지. 없으면 0 또는 빈 문자열, notes 에 사유 기재.",
  "9) 사업장/채널/수수료/투숙기간/상품구성은 시트 상단 제목·머리말에서 최대한 추출해 meta 에 채웁니다.",
].join("\n");

function corsHeaders(env) {
  return {
    "Access-Control-Allow-Origin": env.ALLOW_ORIGIN || "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "content-type",
    "Access-Control-Max-Age": "86400",
  };
}

function json(obj, status, env) {
  return new Response(JSON.stringify(obj), {
    status: status || 200,
    headers: { "content-type": "application/json;charset=utf-8", ...corsHeaders(env) },
  });
}

// ── 토큰 검증 (apps_script_auth.js signToken_ 과 동일 스킴) ──
function b64urlToString(s) {
  s = s.replace(/-/g, "+").replace(/_/g, "/");
  while (s.length % 4) s += "=";
  const bin = atob(s);
  const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
  return new TextDecoder("utf-8").decode(bytes);
}
async function sha256hex(str) {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(str));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
async function verifyToken(token, secret) {
  if (!token || typeof token !== "string" || !secret) return null;
  const parts = token.split(".");
  if (parts.length !== 2) return null;
  const expect = (await sha256hex(parts[0] + "|" + secret)).substring(0, 32);
  if (expect !== parts[1]) return null;
  let payload;
  try { payload = JSON.parse(b64urlToString(parts[0])); } catch (_) { return null; }
  if (!payload || !payload.exp || payload.exp < Date.now()) return null;
  return payload; // { email, role, exp }
}

// ── 한도(KV) ──
function todayKey() { return "count:" + new Date().toISOString().slice(0, 10); }
function monthKey() { return "spend:" + new Date().toISOString().slice(0, 7); }

async function checkLimits(env) {
  const dailyMax = parseInt(env.DAILY_MAX || "40", 10);
  const budget = parseFloat(env.MONTHLY_BUDGET_USD || "20");
  const count = parseInt((await env.LIMITS.get(todayKey())) || "0", 10);
  const spend = parseFloat((await env.LIMITS.get(monthKey())) || "0");
  if (count >= dailyMax) return { ok: false, why: `오늘 변환 한도(${dailyMax}건)를 초과했습니다. 내일 다시 시도해주세요.` };
  if (spend >= budget) return { ok: false, why: `이번 달 사용 예산($${budget})을 초과했습니다. 다음 달 또는 관리자에게 문의하세요.` };
  return { ok: true, count, spend, dailyMax, budget };
}

async function recordUsage(env, usage) {
  const u = usage || {};
  const cost =
    ((u.input_tokens || 0) * PRICE.input +
      (u.output_tokens || 0) * PRICE.output +
      (u.cache_creation_input_tokens || 0) * PRICE.cacheWrite +
      (u.cache_read_input_tokens || 0) * PRICE.cacheRead) /
    1e6;
  // 카운터 증가 (저빈도라 단순 read-modify-write; 소프트 캡이므로 경합 허용)
  const ck = todayKey(), mk = monthKey();
  const count = parseInt((await env.LIMITS.get(ck)) || "0", 10) + 1;
  const spend = parseFloat((await env.LIMITS.get(mk)) || "0") + cost;
  await env.LIMITS.put(ck, String(count), { expirationTtl: 60 * 60 * 48 });        // 2일
  await env.LIMITS.put(mk, spend.toFixed(6), { expirationTtl: 60 * 60 * 24 * 45 }); // 45일
  return { cost, count, spend };
}

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders(env) });
    }
    if (request.method !== "POST") {
      return json({ ok: false, error: "POST only" }, 405, env);
    }

    let body;
    try { body = await request.json(); } catch (_) {
      return json({ ok: false, error: "본문 JSON 파싱 실패" }, 400, env);
    }

    // 1) 인증
    const payload = await verifyToken(body.token, env.AUTH_SECRET);
    if (!payload) return json({ ok: false, error: "로그인이 필요합니다(토큰 무효/만료)." }, 401, env);
    if (payload.role !== "admin" && payload.role !== "sales") {
      return json({ ok: false, error: "권한이 없습니다(sales 이상 필요)." }, 403, env);
    }

    // 2) 입력
    const grid = (body.grid || "").toString();
    if (grid.length < 20) return json({ ok: false, error: "요금표 내용이 비어 있습니다." }, 400, env);
    const GRID_CAP = 60000; // 과도한 입력 방지
    const gridText = grid.length > GRID_CAP ? grid.slice(0, GRID_CAP) + "\n…(이하 생략)" : grid;
    const metaHint = body.meta && typeof body.meta === "object" ? body.meta : {};

    // 3) 한도
    const lim = await checkLimits(env);
    if (!lim.ok) return json({ ok: false, error: lim.why, limited: true }, 429, env);

    // 4) Claude 호출
    const userContent =
      "다음은 요금표 엑셀 시트의 텍스트 격자입니다.\n" +
      (Object.keys(metaHint).length ? "사용자가 확인한 메타(우선 적용): " + JSON.stringify(metaHint) + "\n" : "") +
      "------ 시트 시작 ------\n" + gridText + "\n------ 시트 끝 ------\n" +
      "위 규칙에 따라 정규화된 숫자만 추출해 스키마대로 반환하세요.";

    const reqBody = {
      model: env.MODEL || "claude-opus-4-8",
      max_tokens: 16000,
      thinking: { type: "adaptive" },
      output_config: {
        effort: "medium",
        format: { type: "json_schema", schema: OUTPUT_SCHEMA },
      },
      system: [{ type: "text", text: SYSTEM_PROMPT, cache_control: { type: "ephemeral" } }],
      messages: [{ role: "user", content: userContent }],
    };

    let resp, data;
    try {
      resp = await fetch(ANTHROPIC_URL, {
        method: "POST",
        headers: {
          "x-api-key": env.ANTHROPIC_API_KEY,
          "anthropic-version": ANTHROPIC_VERSION,
          "content-type": "application/json",
        },
        body: JSON.stringify(reqBody),
      });
      data = await resp.json();
    } catch (e) {
      return json({ ok: false, error: "AI 호출 실패: " + (e && e.message ? e.message : e) }, 502, env);
    }
    if (!resp.ok) {
      const msg = (data && data.error && data.error.message) || ("HTTP " + resp.status);
      return json({ ok: false, error: "AI 오류: " + msg }, 502, env);
    }
    if (data.stop_reason === "refusal") {
      return json({ ok: false, error: "AI가 요청을 거부했습니다(안전 정책)." }, 422, env);
    }

    // 구조화 출력: 첫 text 블록이 유효 JSON
    let extracted = null;
    try {
      const tb = (data.content || []).find((b) => b.type === "text");
      extracted = JSON.parse(tb.text);
    } catch (_) {
      return json({ ok: false, error: "AI 응답 파싱 실패." }, 502, env);
    }

    // 5) 사용량 기록
    const rec = await recordUsage(env, data.usage);

    return json(
      {
        ok: true,
        data: extracted,
        usage: data.usage,
        caps: {
          today: rec.count,
          dailyMax: lim.dailyMax,
          monthSpendUsd: Number(rec.spend.toFixed(4)),
          monthBudgetUsd: lim.budget,
          thisCallUsd: Number(rec.cost.toFixed(4)),
        },
      },
      200,
      env
    );
  },
};
