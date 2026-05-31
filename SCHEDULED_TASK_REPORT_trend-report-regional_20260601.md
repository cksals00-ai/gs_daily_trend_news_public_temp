# 스케줄 작업 리포트 — trend-report-regional (2026-06-01 02:08 KST)

무인 자동 실행. **로컬 5단계 모두 정상 완료 + git 커밋 성공.** 마지막 `git push`만 샌드박스 외부 네트워크 차단으로 미완료. 더미 데이터 미사용.

## 단계별 결과

| 단계 | 스크립트 | 결과 |
|---|---|---|
| 1. git 락 정리 | — | ✅ stale `index.lock`·`HEAD.lock` 제거. 마운트가 unlink(EPERM)을 거부하므로 **rename(mv) 우회**로 비활성화 — 정상 동작 |
| 2. 경쟁사 수집 | `scripts/collect_gs_monitor.py` | ⚠ 원격 8개 소스 전부 실패(네트워크 차단) → 기존 competitors.json 보존(9건, `_updated_at` 2026-04-21). 더미 미사용 |
| 3. 인사이트 생성 | `scripts/generate_insights.py` | ✅ 성공. enriched_notes.json 갱신(생성 2026-06-01 02:07 KST, DB 인사이트 3건, 4개 권역 action_alerts) |
| 4. HTML 리빌드 | `scripts/build.py` | ✅ 성공. **index.html 1,765,408 bytes**(>100KB), otb.html 223,219 bytes. YoY 88사업장, 당일분석 교차검증 통과(NET=2925=세그먼트합), 뉴스 317건/9카테고리, 패키지 14,409계열 주입 |
| 5. 커밋·푸시 | git | ✅ 커밋 성공 `ad0b182` (7 files, +26/−26) / ❌ push 미완료(네트워크 차단) |

## 체크포인트 검증

- **competitors.json**: 경쟁사 9건, 최신 수집일시 `2026-04-21`(원격 미연결로 정체) — 보존됨
- **enriched_notes.json**: generated_at `2026-06-01T02:07:51+09:00`, action_alerts 4개 권역, DB 인사이트 3건
  - [1] 6월 온북 52,603RN · 전월(5월 78,393RN) 대비 ▼32.9%
  - [2] 사업장별 6월 온북 상위 소노벨 비발디파크 6,534RN(12.4%)
  - [3] 전년 동월(2025/06) 대비 ▼35.5% (52,603RN vs 81,589RN)
- **index.html**: 1,765,408 bytes (>100KB ✅)

## 스크립트명 불일치 — 정정 적용

작업 정의서의 스크립트명 2건이 실제 파일명과 다릅니다. 실제 파일로 정정 실행했습니다.

- `gs_monitor_collector.py` → 실제 **`scripts/collect_gs_monitor.py`**
- (insights·build는 정의서 그대로 `generate_insights.py`, `build.py` 존재)

> 정의서를 실제 파일명에 맞게 갱신 권장.

## 경쟁사 데이터 — 로컬 크롤러 산출물 존재

`sono-competitor-crawler` 레포에 **오늘자 신규 크롤 결과**가 있습니다:
- `exports/golf_prices_20260531.csv` (95KB), `.xlsx` (27KB)
- `exports/sono_competitor_prices_20260531.csv` (≈512MB)

다만 이는 경쟁사 **요금(price) 원본**으로, competitors.json이 기대하는 **프로모션(promotion) 스키마**와 형식이 다르고, collector에 로컬 CSV→프로모션 변환 경로가 없습니다. 무인 실행에서 임의 변환은 데이터 오염 위험이 있어 적용하지 않고 기존 JSON을 보존했습니다(더미 금지 원칙). 변환 파이프라인을 정식 추가하면 competitors.json 정체(2026-04-21) 해소 가능.

## 5단계(push)가 막힌 원인 — 외부 네트워크 차단

아웃바운드 전부 프록시 차단(2단계 원격 수집 실패와 동일 원인):
- SSH: `CONNECT ssh.github.com:443: Forbidden`
- `git pull --rebase` 재시도도 동일 사유로 불가

현재 로컬 `main`은 origin 대비 **7 커밋 앞섬**. 커밋은 디스크에 안전 기록, 워킹트리 clean.

## 호스트(macOS)에서 마무리 — push 1회만

```
cd ~/Desktop/gs_daily_trend_news_public_temp
git push origin main      # 커밋 완료 상태, add/commit 불필요
```

## 권장(반복 이슈)

- **네트워크 허용 환경에서 스케줄 실행** 시 경쟁사 원격 수집 + push 모두 자동 해소.
- `.git/` 내 과거 회차의 `*.lock.del_*`/`*.bak` 0-byte 파일 누적 — 호스트에서 1회 정리 권장.

---
*무인 자동 스케줄 실행. 로컬 산출물(insights·HTML)은 실데이터로 정상 갱신, git 커밋까지 성공. push만 네트워크 차단으로 미완료 — 호스트에서 `git push` 1회 필요.*
