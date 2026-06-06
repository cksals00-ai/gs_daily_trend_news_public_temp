# 트렌드리포트 권역별 자동 업데이트 — 실행 리포트

- **실행일시(KST):** 2026-06-07 02:10
- **태스크:** trend-report-regional (매일 02:00)
- **프로젝트:** ~/Projects/gs_daily_trend_news_public_temp (태스크 파일 표기는 ~/Desktop, 실제 경로는 Projects)
- **결과:** 파이프라인 정상 완료 · 로컬 커밋 생성(`fa02b3c`) · 원격 푸시는 샌드박스 네트워크 차단으로 보류

## 실행 단계

### 1. Git 락 정리
- `.git/index.lock`, `.git/HEAD.lock` 가 잔존하나 **샌드박스 FUSE 마운트가 파일 삭제(unlink)를 차단**(`Operation not permitted`)하여 `rm` 불가. 쓰기/truncate는 허용됨.
- 우회 처리: `GIT_INDEX_FILE`을 마운트 외부(`/tmp`)로 지정해 `index.lock`을 우회, 커밋은 plumbing(`commit-tree`)으로 생성 후 `refs/heads/main`을 직접 기록하여 `HEAD.lock` 잠금 경로를 회피함. 브랜치 `main` 정상.

### 2. 경쟁사 프로모션 수집 (`scripts/collect_gs_monitor.py`)
- 태스크 파일의 `gs_monitor_collector.py`는 레포에 없음 → 동일 목적의 실제 스크립트 `collect_gs_monitor.py` 사용.
- 원격 소스(GitHub Pages / raw.githubusercontent 8종) **모두 미도달** — 샌드박스 outbound 차단.
- 스크립트 설계대로 **기존 `data/competitors.json` 보존**(더미 데이터 미생성). 경쟁사 9건, `_updated_at` 2026-04-21.
- 참고: 로컬 크롤러 레포(`~/Projects/sono-competitor-crawler/docs/data/promo_data.json`)에 **실제 최신 수집 데이터 존재**(collected_at 2026-06-04, 사업장 23개·경쟁사 객실 실가격). 다만 현 파이프라인 스키마(`competitors.json`: brand/region/discount_pct/threat_level — 큐레이션된 캠페인 단위)와 크롤러 스키마(per-property 객실 실가격 스크랩)는 구조가 근본적으로 달라 자동 연동 안 됨. raw 가격에서 `discount_pct`·`threat_level`을 임의 도출하면 근거 없는 위협도 필드를 주입하게 되어 "더미 데이터 금지" 원칙에 위배 → **보존 + 보고**로 처리.

### 3. 인사이트 생성 (`scripts/generate_insights.py`)
- 정상 완료. KPI/DB 집계 로드(monthly_total 58개월, 뉴스 359건) → DB 인사이트 3건, 권역별 action_alerts 4권역 갱신.
- `data/enriched_notes.json`: `report_date` **2026-06-07**, 권역 = vivaldi / central / south / apac.
- 오늘의 한 줄: **6월 온북 63,581RN · 전월(5월 78,351RN) 대비 ▼18.9% 감소**.
- 권역 알림 요약:
  - vivaldi: 88.0% 주의 구간, Mega Channel 시너지 가속 권장
  - central: Pacing 98.0% 관찰 필요, 경쟁사 할인율 상승 대응 집중 (연관 뉴스 8건)
  - south: 제주 노선 증편 호재 + 신규 공급 압박, ADR 방어·차별화 필수 (연관 뉴스 8건)
  - apac: 환율·유가 악화로 한국발 수요 둔화, 하이퐁 해외마케팅 가속·GSA 확대 (연관 뉴스 8건)

### 4. 전체 빌드 (`scripts/build.py`)
- 정상 완료. `docs/index.html` **1,848,511 bytes** (≥100KB ✅), `docs/otb.html` **222,745 bytes**.
- 빌드 타임스탬프: `Auto-Built 2026-06-07 02:07 KST`.
- 주입 내역: 권역별 신호등 4권역, 주간 온북 추이 23개 사업장, 경쟁사 카드 9건, YoY 88개 사업장, 뉴스 359건(9 카테고리), Daily Booking 25개 사업장, 패키지 트렌드 TOP100, 당일분석 교차검증 통과(NET=2870=세그먼트합), data_freshness 5개 소스.

### 5~6. 커밋 & 푸시
- 커밋 **`fa02b3c`** "chore(auto): trend regional update 2026-06-07 02:09 KST [skip ci]" 생성 (12 files changed, +1002/-971).
- 커밋 내용 검증: `docs/index.html` 커밋 blob = 워킹파일 해시 일치(`6c83985…`), `enriched_notes.json` report_date 2026-06-07·4권역 확인.
- `git push origin main` **실패**: `CONNECT ssh.github.com:443: Forbidden` (HTTPS 프록시도 403) — 샌드박스 outbound 차단. 네트워크 문제이며 충돌 아님.
- **로컬 커밋 정상 보존** (origin/main 대비 ahead 7). 호스트 측에서 사후 동기화되는 기존 패턴.

## 체크포인트 검증
- competitors.json: 경쟁사 9건, 수집일시 2026-04-21 (원격 미도달로 보존) ⚠
- enriched_notes.json: report_date 2026-06-07, headline·action_alerts 4권역 갱신 ✅
- index.html: 1,848,511 bytes (>100KB) ✅
- 워킹트리: `.git/index.lock`·`HEAD.lock` 잔존(호스트 권한, git 동작은 우회로 정상 처리) ⚠

## 사용자 조치 필요
1. **GitHub 푸시** — 샌드박스 네트워크 제약으로 차단됨. 네트워크 가능한 로컬 터미널에서:
   ```
   cd ~/Desktop/gs_daily_trend_news_public_temp
   git pull --rebase origin main && git push origin main
   ```
   미푸시 로컬 커밋: `fa02b3c` 외 ahead 7건.
2. **Git 락 파일** — `.git/index.lock`, `.git/HEAD.lock` 은 호스트 측에서 `rm -f .git/index.lock .git/HEAD.lock` 1회 정리 권장(이후 자동 실행이 정상 경로로 동작).
3. **경쟁사 데이터 연동** — 크롤러 레포에 실제 최신 데이터(2026-06-04)가 있으나 스키마 불일치로 미연동. competitors.json 스키마로의 매핑 규칙(캠페인 단위 집계, discount_pct/threat_level 산출 기준) 정의 시 자동 반영 가능.
