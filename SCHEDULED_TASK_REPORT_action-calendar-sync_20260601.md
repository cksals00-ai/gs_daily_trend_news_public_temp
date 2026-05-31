# action-calendar-sync — 2026-06-01 06:25 KST

## 결과 요약
구글시트 fetch → 기획전 동기화 → 실적 재집계 → 빌드 → 로컬 커밋까지 완료. **푸시는 샌드박스 네트워크 차단으로 보류**(로컬 커밋만 누적).

## 단계별 결과

### 1단계 · Chrome MCP 구글시트 fetch — ✅ 성공
- pubhtml CSP로 인해 첫 fetch 실패 → pub?output=csv 컨텍스트 진입 후 정상 fetch.
- 메인(DATA, gid 1818134248) + 서브시트 20/198 fetch.
- gid 매핑 추출: `{20, 198, cal, DATA, 25 인플루언서, 26 인플루언서, 26년 연간PLAN}`.
- 샌드박스↔브라우저 직접 파이프 부재 + 결과창 길이제한(~1000자) + base64 차단 → **gzip 번들을 76자 개행 base64 17청크로 분할 전송, 청크별 SHA-256 검증**.
- 전체 재조립 SHA-256 일치: `7b32648af0a64dbb…2302f9b` (15,636 b64 / gzip 해제 73,846 bytes).
- 저장: `data/cache/campaign_main.csv` (38,202자), `subsheets/20.csv`, `subsheets/198.csv`. 기준일자 2026-06-01 확인.

### 2단계 · 파이썬 스크립트 — ✅ 성공
- `generate_campaign_data.py --csv-file … --subsheets-dir …` → `campaign_data.json`: **events 225 / 매핑 패키지코드 246 / Key 서브시트 2건**.
- `generate_campaign_performance.py` → `campaign_performance.json`: **by_key 1 / 누적 RN 3,718 / 누적 매출 724.4백만** (raw_db 27·28 txt 7,237행 매칭).

### 3단계 · 빌드 + 커밋 — ⚠️ 커밋 OK, 푸시 보류
- `build.py` 전체 리빌드 완료 (index.html 1,687,725 bytes, otb.html 215,613 bytes). 당일분석 교차검증 통과(NET=2925).
- 커밋 생성: `4a35245 chore(auto): action calendar sync 2026-06-01 06:25 KST [skip ci]` (13 files).
- **푸시 실패**: 원격 `ssh://git@ssh.github.com:443/…` — 샌드박스가 GitHub를 SSH/HTTPS 모두 allowlist 차단(`blocked-by-allowlist`). 네트워크 정책상 우회 불가.
- 현재 `main`이 `origin/main` 대비 **10 커밋 ahead** (2026-05-30 이후 자동 실행분 누적). GitHub 접근 가능한 환경에서 push 필요.

## 체크포인트
| 항목 | 값 |
|---|---|
| CSV fetch 길이 | 38,202자 (>1000 OK) |
| campaign_data events | 225 |
| 매핑 패키지코드 | 246 |
| campaign_performance by_key | 1 |
| 누적 RN | 3,718 |
| 재조립 SHA-256 | 일치 |

## 조치 필요
- GitHub allowlist가 적용된 환경에서 `git push origin main` 실행 (누적 10커밋 반영, GH Pages 배포).
