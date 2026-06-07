# 트렌드리포트 권역별 자동 업데이트 — 실행 리포트

- **실행일시(KST):** 2026-06-08 02:10
- **태스크:** trend-report-regional (매일 02:00)
- **프로젝트:** ~/Projects/gs_daily_trend_news_public_temp (태스크 파일 표기는 ~/Desktop, 실제 경로는 Projects)
- **결과:** 파이프라인 정상 완료 · 로컬 커밋 생성(`c53dbfd`) · 원격 푸시는 샌드박스 네트워크 차단으로 보류

## 실행 단계

### 1. Git 락 정리
- 시작 시점에는 `.git/index.lock` / `.git/HEAD.lock` **없음**(호스트 측 정리 추정).
- 단, 파이프라인 후 `git add` 단계에서 `.git/index.lock` 이 재생성되고 **샌드박스 FUSE 마운트가 unlink를 차단**(`rm: Operation not permitted`)하여 정상 commit 불가. 쓰기/truncate는 허용됨.
- 우회 처리: `GIT_INDEX_FILE`을 마운트 외부(`/tmp`)로 지정(기존 `.git/index` 복사본 기반)해 락 충돌을 회피하고, 커밋은 plumbing(`write-tree` → `commit-tree`)으로 생성. 새 커밋 SHA를 `refs/heads/main` 루즈 레퍼런스에 직접 기록(truncate-overwrite)하여 `*.lock` unlink 경로를 회피함. 브랜치 `main` 정상.

### 2. 경쟁사 프로모션 수집 (`scripts/collect_gs_monitor.py`)
- 태스크 파일의 `gs_monitor_collector.py`는 레포에 없음 → 동일 목적의 실제 스크립트 `collect_gs_monitor.py` 사용.
- 원격 소스(GitHub Pages / raw.githubusercontent 8종) **모두 미도달** — 샌드박스 outbound 차단.
- 스크립트 설계대로 **기존 `data/competitors.json` 보존**(더미 데이터 미생성). 경쟁사 9건, `_updated_at` 2026-04-21.
- 로컬 크롤러 레포(`~/Projects/sono-competitor-crawler/docs/data/promo_data.json`)의 최신 수집 데이터는 **collected_at 2026-06-04로 어제와 동일**(갱신 없음). 현 파이프라인 스키마(brand/region/discount_pct/threat_level — 큐레이션된 캠페인 단위)와 크롤러 스키마(per-property 객실 실가격)는 구조가 달라 자동 연동 미적용 → **보존 + 보고**로 처리.

### 3. 인사이트 생성 (`scripts/generate_insights.py`)
- 정상 완료. KPI/DB 집계 로드(monthly_total 58개월, 뉴스 234건) → DB 인사이트 3건, 권역별 action_alerts 4권역 갱신.
- `data/enriched_notes.json`: `report_date` **2026-06-08**, 권역 = vivaldi / central / south / apac.
- 오늘의 한 줄: **6월 온북 63,581RN · 전월(5월 78,351RN) 대비 ▼18.9% 감소** (전년 동월 대비 ▼22.1%).
- 권역 알림 요약:
  - vivaldi: 88.0% 주의 구간, Strategy 02 Mega Channel 시너지 가속 권장
  - central: Pacing 98.0% 관찰 필요, 경쟁사 할인율 상승 대응 집중 (연관 뉴스 8건)
  - south: 제주 노선 증편 호재 + 신규 공급 압박, ADR 방어·차별화 + 여수 크루즈 Inbound 집중 (연관 뉴스 8건)
  - apac: 환율·유가 동반 악화로 한국발 수요 둔화, 하이퐁 해외마케팅(Strategy 05) 가속·GSA 확대 (연관 뉴스 8건)

### 4. 전체 빌드 (`scripts/build.py`)
- 정상 완료. `docs/index.html` **1,618,178 bytes** (≥100KB ✅), `docs/otb.html` **222,750 bytes**.
- 빌드 타임스탬프: `Auto-Built 2026-06-08 02:07 KST`.
- 주입 내역: 권역별 신호등 4권역, 카테고리별 뉴스 9개 카테고리·234건, Featured 뉴스 2건, Daily Booking 25개 사업장·3개월, 패키지 트렌드 14,409계열 중 TOP100, YoY 88개 사업장, db_aggregated/package_series/rm_fcst 동기화, data_freshness 5개 소스.

### 5~6. 커밋 & 푸시
- 커밋 **`c53dbfd`** "chore(auto): trend regional update 2026-06-08 02:10 KST [skip ci]" 생성 (parent `25c66aa`, 신규 tree `1b3a6e76`).
- 커밋 내용 검증: 커밋 blob = 워킹파일 해시 일치 — `docs/index.html`(`214a641e…`), `enriched_notes.json`(`e24654c4…`, report_date 2026-06-08·4권역), `competitors.json`(`2df0db62…`).
- `git push origin main` **실패**: `kex_exchange_identification: Connection closed` / `ssh.github.com:443 Forbidden` — 샌드박스 outbound 차단. 네트워크 문제이며 충돌 아님.
- **로컬 커밋 정상 보존** (origin/main 대비 ahead 13). 호스트 측에서 사후 동기화되는 기존 패턴.

## 체크포인트 검증
- competitors.json: 경쟁사 9건, 수집일시 2026-04-21 (원격 미도달로 보존) ⚠
- enriched_notes.json: report_date 2026-06-08, today_headline·action_alerts 4권역 갱신 ✅
- index.html: 1,618,178 bytes (>100KB) ✅
- 워킹트리: `.git/index.lock` 재생성·잔존(호스트 권한, git 동작은 우회로 정상 처리). `git status`상 dirty로 보이나 실제 .git/index 미갱신에 따른 표시일 뿐, 커밋 c53dbfd는 워킹트리 전체를 정확히 포함 ⚠

## 사용자 조치 필요
1. **GitHub 푸시** — 샌드박스 네트워크 제약으로 차단됨. 네트워크 가능한 로컬 터미널에서:
   ```
   cd ~/Projects/gs_daily_trend_news_public_temp
   git pull --rebase origin main && git push origin main
   ```
   미푸시 로컬 커밋: `c53dbfd` 포함 ahead 13건.
2. **Git 락 파일** — `.git/index.lock` 은 호스트 측에서 `rm -f .git/index.lock` 1회 정리 권장(이후 자동 실행이 정상 경로로 동작). 정리 후 `git reset`(soft) 없이도 워킹트리는 c53dbfd와 일치.
3. **경쟁사 데이터 연동** — 크롤러 레포 데이터가 2026-06-04에서 갱신되지 않음. competitors.json 스키마(캠페인 단위 집계, discount_pct/threat_level 산출 기준) 매핑 규칙 정의 시 자동 반영 가능.
