# GS 요금표 → 품의 배분가(안) AI 변환 백엔드

`docs/rate-converter.html`(AI 변환기 프론트)이 호출하는 **Cloudflare Worker** 입니다.
요금표 시트 텍스트를 받아 Claude(Opus 4.8)로 정규화 숫자를 추출하고, 사용 한도를 관리합니다.

```
브라우저(rate-converter.html, sales 로그인)
   → POST {token, grid, meta}
        → 이 Worker (API키 보관 + 토큰검증 + 한도)
             → Claude /v1/messages (structured outputs)
        ← 구조화 JSON
   ← 와이드 양식 xlsx 렌더(브라우저, 결정론적 수식)
```

## 한도 (코드/변수로 조절)
- `DAILY_MAX` : 하루 변환 횟수 상한 (기본 40)
- `MONTHLY_BUDGET_USD` : 월 예산 상한 USD (기본 20) — 초과 시 그 달 마감까지 429 차단
- 한도 카운터는 KV(`LIMITS`)에 저장 (`count:YYYY-MM-DD`, `spend:YYYY-MM`)

## 인증
- 프론트는 로그인 토큰(`gsn_auth_token`)을 함께 보냄
- Worker가 `AUTH_SECRET`(= `scripts/apps_script_auth.js` 의 `AUTH_SECRET` 와 **동일 값**)으로
  서명을 재계산해 검증 → `sales`/`admin` 역할만 허용. API 키는 서버에만 있음(브라우저 비노출).

---

## 배포 (최초 1회)

전제: Cloudflare 계정(무료 가입), Node.js 설치.

```bash
cd workers/rate-converter
npm i -g wrangler            # 또는 npx 사용
wrangler login              # 브라우저로 Cloudflare 로그인

# 1) KV 네임스페이스 생성 → 출력된 id 를 wrangler.toml 의 LIMITS id 에 붙여넣기
wrangler kv namespace create LIMITS

# 2) 시크릿 2개 등록
wrangler secret put ANTHROPIC_API_KEY    # Anthropic 콘솔(console.anthropic.com)에서 발급한 키
wrangler secret put AUTH_SECRET          # apps_script_auth.js 의 AUTH_SECRET 와 동일 문자열

# 3) (선택) wrangler.toml [vars] 에서 한도/모델/ALLOW_ORIGIN 조정
#    ALLOW_ORIGIN 을 GitHub Pages 주소로 두면 더 안전 (예: https://<계정>.github.io)

# 4) 배포
wrangler deploy
```

배포되면 `https://gs-rate-converter.<계정>.workers.dev` 같은 URL이 나옵니다.

## 프론트 연결
1. 배포된 Worker URL 복사
2. `rate-converter.html` 접속(sales 로그인) → **2. 확인 & 변환 → ⚙ 서버 설정** → URL 붙여넣고 **저장**
   (브라우저 localStorage `gsn_rate_worker_url` 에 저장 — 사이트 재배포 불필요)

## 한도 변경
`wrangler.toml [vars]` 의 `DAILY_MAX` / `MONTHLY_BUDGET_USD` 수정 후 `wrangler deploy`.

## 비용 (대략, Opus 4.8 $5/$25 per 1M)
요금표 1건 변환 ≈ 수십~수백 원. 월 $20 상한이면 약 100~200건/월.
사용량은 변환 결과 화면 하단(오늘 N건 / 이번 달 $X)에서 확인.

## 로컬 테스트
```bash
wrangler dev   # 로컬 8787 포트. 단, 실제 Claude 호출엔 ANTHROPIC_API_KEY 시크릿 필요
```
