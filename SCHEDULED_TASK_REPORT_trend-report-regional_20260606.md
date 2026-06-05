# 트렌드리포트 권역별 자동 업데이트 — 실행 리포트

- **실행일시(KST):** 2026-06-06 02:08
- **태스크:** trend-report-regional (매일 02:00)
- **프로젝트:** ~/Desktop/gs_daily_trend_news_public_temp
- **결과:** 파이프라인 정상 완료 · 로컬 커밋 생성 · 원격 푸시는 샌드박스 네트워크 차단으로 보류

## 실행 단계

### 1. Git 락 정리
- `.git/index.lock`, `.git/HEAD.lock` 정리 완료. 브랜치 `main` 확인.

### 2. 경쟁사 프로모션 수집 (`scripts/collect_gs_monitor.py`)
- 태스크 파일의 `gs_monitor_collector.py`는 실제 레포에 없음 → 동일 목적의 실제 스크립트 `collect_gs_monitor.py` 사용.
- 원격 소스(GitHub Pages / raw.githubusercontent 8종) **모두 미도달** — 샌드박스 outbound 차단.
- 스크립트 설계대로 **기존 competitors.json 보존** (더미 데이터 미생성). 경쟁사 9개, `_updated_at` 2026-04-21 (수동 수집).
- 참고: 로컬 크롤러 레포(`~/Projects/sono-competitor-crawler/docs/data/promo_data.json`, 수집일 2026-06-04)에 최신 데이터가 존재하나, 현 파이프라인 스키마(competitors.json: brand/region/discount_pct/threat_level)와 구조가 달라 자동 연동되어 있지 않음. 연동 시 `discount_pct`·`threat_level` 등 파생 필드 매핑 규칙 정의 필요.

### 3. 인사이트 생성 (`scripts/generate_insights.py`)
- 정상 완료. KPI/DB 집계 로드 → DB 인사이트 3개, 권역별 action_alerts 4권역 갱신.
- `data/enriched_notes.json`: `report_date` 2026-06-06, 권역 = vivaldi / central / south / apac.
- 주요 인사이트: 6월 온북 63,581RN (전월 78,351RN 대비 ▼18.9%, 전년 동월 대비 ▼22.1%), 상위 소노벨 비발디파크 7,672RN.

### 4. 전체 빌드 (`scripts/build.py`)
- 정상 완료. `docs/index.html` **1,776,692 bytes** (≥100KB ✅), `docs/otb.html` 222,745 bytes.
- 뉴스 318건(9 카테고리), Daily Booking 25개 사업장, 패키지 트렌드 TOP100, YoY 88개 사업장, data_freshness 5개 소스 동기화.

### 5~6. 커밋 & 푸시
- `git add -A` → 커밋 **`abe49cf`** "chore(auto): trend regional update 2026-06-06 02:08 KST [skip ci]" 생성 (10 files changed, +429/-413).
- `git push origin main` **실패**: `CONNECT ssh.github.com:443: Forbidden` (HTTPS도 프록시 403) — 샌드박스 outbound 차단.
- 폴백 `git pull --rebase`도 동일 사유로 불가 (네트워크 문제, 충돌 아님).
- **로컬 커밋 정상 보존** (origin/main 대비 ahead 1). 전일 커밋 `e5682f5`는 이미 origin 반영 확인됨 → 호스트 측에서 사후 동기화되는 패턴.

## 체크포인트 검증
- competitors.json: 경쟁사 9개, 수집일시 2026-04-21 (원격 미도달로 보존) ⚠
- enriched_notes.json: report_date 2026-06-06, action_alerts 4권역 갱신 ✅
- index.html: 1,776,692 bytes (>100KB) ✅
- 워킹트리: clean (단, 호스트 권한으로 `.git/index.lock` 잔존 — git 동작에는 영향 없음) ⚠

## 사용자 조치 필요
샌드박스 네트워크 제약으로 GitHub 푸시가 차단됩니다. 네트워크가 되는 로컬 터미널에서 아래 실행 시 반영됩니다:

```
cd ~/Desktop/gs_daily_trend_news_public_temp
git pull --rebase origin main && git push origin main
```

미푸시 로컬 커밋: `abe49cf` (2026-06-06 자동 빌드).
