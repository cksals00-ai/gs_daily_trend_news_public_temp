# GA4 웹 애널리틱스 대시보드 연동 — 설정 가이드 (OAuth 방식)

대시보드가 GA4 숫자를 **자동으로** 보여주게 하는 설정입니다. 코드(수집 스크립트·페이지·파이프라인 연결)는 이미 넣어 두었고, 아래 **OAuth 토큰 발급 + 시크릿 등록**만 하시면 다음 파이프라인 실행부터 데이터가 채워집니다.

> **왜 OAuth 방식인가:** cksals00@gmail.com 계정은 이 GA 속성의 리포트 **열람은 되지만 관리자 권한이 없어** 서비스계정을 뷰어로 추가할 수 없습니다. 그래서 서비스계정 대신, **이미 열람 권한이 있는 본인 계정**으로 인증(OAuth)합니다. 관리자 도움이 필요 없습니다.

대상 GA4 속성: **소노호텔앤리조트 통합 (속성 ID 433673272)**

---

## 1) OAuth 클라이언트 만들기 (Google Cloud · GSN-Auth 프로젝트, 약 5분)

1. https://console.cloud.google.com → 프로젝트 **GSN-Auth** 선택.
2. **API 및 서비스 → OAuth 동의 화면**
   - User Type: **External** → 만들기
   - 앱 이름/이메일 등 필수값만 입력
   - **범위 추가**: `.../auth/analytics.readonly` 검색해 추가
   - **테스트 사용자**에 본인(cksals00@gmail.com) 추가
   - ⚠ **가능하면 마지막에 "앱 게시(Production)"** 까지 눌러주세요. '테스트' 상태면 토큰이 7일 후 만료됩니다(미검증 경고는 본인 사용엔 무방).
3. **API 및 서비스 → 사용자 인증 정보 → + 사용자 인증 정보 만들기 → OAuth 클라이언트 ID**
   - 유형: **데스크톱 앱** → 만들기
   - 만들어진 클라이언트의 **JSON 다운로드** (`client_secret_XXXX.json`)

## 2) 리프레시 토큰 발급 (본인 Mac 터미널, 약 2분)

```bash
cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp
pip install google-auth-oauthlib
python scripts/ga4_oauth_setup.py ~/Downloads/client_secret_XXXX.json
```

- 브라우저가 열리면 **본인 구글 계정으로 로그인·동의**.
- (미검증 앱 경고가 나오면: 고급 → 안전하지 않음(이동) → 계속 — 본인 앱이라 정상)
- 터미널에 아래 3개 값이 출력됩니다:
  - `GA4_OAUTH_CLIENT_ID`
  - `GA4_OAUTH_CLIENT_SECRET`
  - `GA4_OAUTH_REFRESH_TOKEN`

## 3) GitHub 시크릿 3개 등록 (약 2분)

https://github.com/cksals00-ai/gs_daily_trend_news_public_temp/settings/secrets/actions → **New repository secret** 로 아래 3개 각각 등록:

| Name | Secret 값 |
|------|-----------|
| `GA4_OAUTH_CLIENT_ID` | 출력된 client id |
| `GA4_OAUTH_CLIENT_SECRET` | 출력된 client secret |
| `GA4_OAUTH_REFRESH_TOKEN` | 출력된 refresh token |

> 등록 후에는 로컬에 생성된 `ga4_oauth_token.json` 은 삭제하세요. (절대 커밋 금지 — `.gitignore` 에 넣어두면 안전)

---

## 4) 워크플로(deploy.yml) 수동 반영 — 2곳

`.github/workflows/deploy.yml` 은 보안상 원격으로 못 써서 직접 반영해야 합니다. (전달드린 수정본으로 덮어써도 됩니다.)

**① pip 설치 줄**에 `google-analytics-data` 추가:
```yaml
run: python -m pip install --upgrade pip && pip install requests openpyxl google-analytics-data
```

**② '[Step 3] 인사이트 자동 생성' 스텝 바로 앞**에 GA4 수집 스텝 추가:
```yaml
      - name: 📈 [Step 2.7] GA4 웹 애널리틱스 수집 (optional)
        if: steps.mode.outputs.full == 'true'
        continue-on-error: true
        timeout-minutes: 5
        env:
          GA4_OAUTH_CLIENT_ID: ${{ secrets.GA4_OAUTH_CLIENT_ID }}
          GA4_OAUTH_CLIENT_SECRET: ${{ secrets.GA4_OAUTH_CLIENT_SECRET }}
          GA4_OAUTH_REFRESH_TOKEN: ${{ secrets.GA4_OAUTH_REFRESH_TOKEN }}
          GA4_PROPERTY_ID: "433673272"
        run: |
          echo "::group::GA4 수집"
          python scripts/fetch_ga4.py || echo "⚠ GA4 수집 실패 (시크릿 없음/권한) - 스킵"
          echo "::endgroup::"
```
(`continue-on-error` 라 GA4가 실패해도 기존 배포는 절대 안 깨집니다.)

---

## 5) 실행 & 확인

- **자동**: 매일 08:00 KST(및 push 시) 파이프라인이 `docs/data/ga4_latest.json` 갱신 → 대시보드 자동 반영.
- **수동 즉시**: GitHub → **Actions** → `🚀 Auto Build & Deploy` → **Run workflow**.
- 확인: https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/ga-analytics.html

### 로컬에서 바로 테스트
```bash
pip install google-analytics-data
export GA4_OAUTH_CLIENT_ID=...  GA4_OAUTH_CLIENT_SECRET=...  GA4_OAUTH_REFRESH_TOKEN=...
python scripts/fetch_ga4.py       # → docs/data/ga4_latest.json 생성
```

---

## 넣은 코드 (참고)
| 파일 | 역할 |
|------|------|
| `scripts/fetch_ga4.py` | GA4 Data API로 최근 12개월 지표 수집 → `docs/data/ga4_latest.json` (OAuth·서비스계정 모두 지원) |
| `scripts/ga4_oauth_setup.py` | OAuth 리프레시 토큰 1회 발급(로컬) |
| `docs/ga-analytics.html` | 다크+골드 웹 애널리틱스 대시보드 페이지 |
| `scripts/add_ga_analytics_nav.py` | 상단 메뉴 전 페이지에 "웹 애널리틱스" 링크 일괄 삽입(선택, 멱등) |
| `.github/workflows/deploy.yml` | GA4 수집 스텝(위 4번 수동 반영) |

수집 지표: 사용자·신규사용자·세션·페이지뷰·참여율·평균 참여시간, 월별 추이, 유입 채널, 인기 페이지, 기기, 국가 (+ 전년 동기 델타).
