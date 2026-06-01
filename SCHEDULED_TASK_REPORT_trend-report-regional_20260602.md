# 스케줄 작업 리포트 — trend-report-regional (2026-06-02 02:07 KST)

무인 자동 실행. **로컬 5단계 모두 정상 완료 + git 커밋 성공.** 마지막 `git push`만 샌드박스 외부 네트워크 차단으로 미완료. 더미 데이터 미사용.

## 단계별 결과

| 단계 | 스크립트 | 결과 |
|---|---|---|
| 1. git 락 정리 | — | ✅ `index.lock`·`HEAD.lock` 없음(정상). 정리 명령 실행 |
| 2. 경쟁사 수집 | `scripts/collect_gs_monitor.py` | ⚠ 원격 8개 소스 전부 실패(네트워크 차단) → 기존 competitors.json 보존(9건, `_updated_at` 2026-04-21). 더미 미사용 |
| 3. 인사이트 생성 | `scripts/generate_insights.py` | ✅ 성공. enriched_notes.json 갱신(생성 2026-06-02 02:06 KST, DB 인사이트 3건, 4개 권역 action_alerts) |
| 4. HTML 리빌드 | `scripts/build.py` | ✅ 성공. **index.html 1,754,664 bytes**(>100KB), otb.html 218,252 bytes. 당일분석 교차검증 통과(NET=1555=세그먼트합), 뉴스 317건/9카테고리, YoY 86사업장, 패키지 14,409계열 주입 |
| 5. 커밋·푸시 | git | ✅ 커밋 성공 `1758334` (10 files, +4829/−4887) / ❌ push 미완료(네트워크 차단) |

## 체크포인트 검증

- **competitors.json**: 경쟁사 9건, 최신 수집일시 `2026-04-21`(원격 미연결로 정체) — 보존됨
- **enriched_notes.json**: `_generated_at` 2026-06-02T02:06:16+09:00, action_alerts 4개 권역(vivaldi·central·south·apac)
  - headline: 6월 온북 55,823RN · 전월(5월 78,351RN) 대비 ▼28.8% 감소
  - [2] 사업장별 6월 온북 상위 소노벨 비발디파크 6,926RN(12.4%)
  - [3] 전년 동월(2025/06) 대비 ▼31.6% (55,823RN vs 81,589RN)
- **index.html**: 1,754,664 bytes (>100KB ✅)

## 스크립트명 불일치 — 정정 적용(반복)

작업 정의서의 스크립트명이 실제 파일명과 다릅니다. 실제 파일로 정정 실행했습니다.

- 정의서 `scripts/gs_monitor_collector.py` → 실제 **`scripts/collect_gs_monitor.py`**
- 정의서 `docs/data/competitors.json` → 실제 경로 **`data/competitors.json`**
- (insights·build는 정의서 그대로 존재)

> 정의서를 실제 파일명·경로에 맞게 갱신 권장.

## 경쟁사 데이터 정체 원인

collector는 원격 GitHub Pages/raw URL 8개를 시도하나 샌드박스 아웃바운드 전면 차단으로 모두 실패 → 기존 JSON 보존(더미 금지 원칙). 로컬 `sono-competitor-crawler` 레포에 오늘자 크롤 산출물(`exports/sono_competitor_prices_20260601.csv`, 537MB)이 있으나 **요금(price) 원본**으로 competitors.json이 기대하는 **프로모션(promotion) 스키마**와 다르고 로컬 변환 경로가 없어 임의 변환은 적용하지 않음. 변환 파이프라인 정식 추가 시 정체 해소 가능.

## 5단계(push) 미완료 — 외부 네트워크 차단

- `git push origin main` → `CONNECT ssh.github.com:443: Forbidden`
- 현재 로컬 `main`은 origin 대비 **ahead 13 / behind 18**(diverged). pull 역시 동일 사유로 불가
- 커밋은 디스크에 안전 기록(`1758334`), 워킹트리 clean

## 호스트(macOS)에서 마무리

네트워크 차단으로 diverged 상태이므로 호스트에서 rebase 후 push 권장:

```
cd ~/Desktop/gs_daily_trend_news_public_temp
git pull --rebase origin main
git push origin main
```

---
*무인 자동 스케줄 실행. 로컬 산출물(insights·HTML)은 실데이터로 정상 갱신, git 커밋까지 성공. push만 네트워크 차단으로 미완료 — 호스트에서 rebase + push 필요.*
