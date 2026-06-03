# 스케줄 작업 리포트 — trend-report-regional

- 실행 일시: 2026-06-04 02:07 KST (자동 실행)
- 작업: 매일 02:00 — 트렌드리포트 권역별 정보 + 경쟁사 프로모션/인사이트 업데이트
- 프로젝트: ~/Desktop/gs_daily_trend_news_public_temp (실제 작업 경로: ~/Projects/gs_daily_trend_news_public_temp)

## 결과 요약

| 단계 | 상태 | 비고 |
|------|------|------|
| 1. git lock 정리 | ✅ | index.lock / HEAD.lock 삭제 완료 |
| 2. 경쟁사 데이터 수집 | ⚠️ | 원격 소스 도달 불가 → 기존 데이터 보존 (더미 미사용) |
| 3. 인사이트 생성 | ✅ | enriched_notes.json 갱신 (2026-06-04, 헤드라인 + 권역 알림 4건) |
| 4. HTML 리빌드 | ✅ | index.html 1.77MB, otb.html 222KB |
| 5. git commit | ✅ | 커밋 1e74d1b 생성 |
| 6. git push | ❌ | 샌드박스가 GitHub SSH(443) 차단 — 푸시 불가 |

## 단계별 상세

### 2. 경쟁사 프로모션 수집 (collect_gs_monitor.py)
- 스케줄 파일의 `gs_monitor_collector.py`는 존재하지 않아 실제 스크립트 `scripts/collect_gs_monitor.py`를 실행.
- 원격 소스 8개 후보(`cksals00-ai.github.io/sono-competitor-crawler/...`, `raw.githubusercontent.com/...`) 전부 도달 실패 — 샌드박스 outbound 차단.
- 로컬에 마운트된 sono-competitor-crawler 레포도 확인했으나, competitors.json 스키마에 맞는 경쟁사 프로모션 데이터는 없음(PowerBI/palatium 데이터만 존재).
- 주의사항("더미 데이터 절대 금지", "에러 시 기존 JSON 보존")에 따라 **기존 `data/competitors.json` 보존**.
  - 현재 데이터: 경쟁사 9개, `_updated_at` = 2026-04-21 (실데이터, 다소 오래됨)
- 참고: 스케줄 파일은 `docs/data/competitors.json` 점검을 지시하나, 실제 파이프라인은 `data/competitors.json`을 사용하고 build.py가 이를 HTML에 직접 주입함(빌드 산출물에 정상 포함).

### 3. 인사이트 생성 (generate_insights.py)
- `data/enriched_notes.json` 갱신, `_generated_at` = 2026-06-04T02:06:48 KST, `report_date` = 2026-06-04.
- today_headline: "6월 온북 57,908RN · 전월(5월 78,351RN) 대비 ▼26.1% 감소."
- DB 인사이트 3건: 전월 대비 ▼26.1%, 사업장 상위 소노벨 비발디파크 7,173RN(12.4%), 전년 동월(2025/06) 대비 ▼29.0%(57,908 vs 81,589RN).
- action_alerts 4개 권역(vivaldi/central/south/apac) 갱신.
- 주의: KPI K1/K2/K3가 None으로 로드됨(소스 KPI 미연동). DB집계 기반 인사이트는 정상 생성.

### 4. 전체 빌드 (build.py)
- index.html 빌드 완료: 1,771,853 bytes (체크포인트 100KB 기준 충족) — "Auto-Built 2026-06-04 02:07 KST". 헤드라인(57,908RN) HTML 주입 확인.
- otb.html: 222,661 bytes.
- 주입 항목: OTB allMonths, 주간리포트, YoY 사업장별(88개), 당일분석 교차검증 통과(NET=134), YoY OTB 비교(4탭/25사업장), 인사이트 패널, 카테고리 뉴스(9개·328건), 패키지 트렌드(14,409계열 중 TOP100), Daily Booking(25사업장·3개월), data_freshness.json(5개 소스).

### 5~6. 커밋 & 푸시
- `git add -A` → 커밋 `1e74d1b` "chore(auto): trend regional update 2026-06-04 02:07 KST [skip ci]" 생성 완료. 워킹트리 clean.
- `git push origin main` **실패**: `CONNECT ssh.github.com:443: Forbidden` — 샌드박스 outbound SSH 차단.
- 폴백 `git pull --rebase origin main`도 동일 사유로 불가.
- **로컬 커밋은 정상 보존됨.** origin과 ahead 1 / behind 1 상태(미푸시 + 원격 선행 커밋 존재).

## 후속 조치 (사용자 확인 필요)
샌드박스 네트워크 제약으로 GitHub 푸시가 차단됩니다. 네트워크가 되는 로컬 터미널에서 아래를 실행하면 반영됩니다:

```
cd ~/Desktop/gs_daily_trend_news_public_temp   # 또는 ~/Projects/gs_daily_trend_news_public_temp
git pull --rebase origin main && git push origin main
```

origin이 behind 1(원격 선행 커밋) 상태이므로 `--rebase`로 먼저 정렬 후 푸시 권장.
