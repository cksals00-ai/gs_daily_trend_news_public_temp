# 스케줄 작업 리포트 — trend-report-regional

- 실행 일시: 2026-06-03 02:09 KST (자동 실행)
- 작업: 매일 02:00 — 트렌드리포트 권역별 정보 + 경쟁사 프로모션/인사이트 업데이트
- 프로젝트: ~/Desktop/gs_daily_trend_news_public_temp

## 결과 요약

| 단계 | 상태 | 비고 |
|------|------|------|
| 1. git lock 정리 | ✅ | index.lock / HEAD.lock 없음 (정리 완료) |
| 2. 경쟁사 데이터 수집 | ⚠️ | 원격 소스 도달 불가 → 기존 데이터 보존 (더미 미사용) |
| 3. 인사이트 생성 | ✅ | enriched_notes.json 갱신 (오늘 날짜·헤드라인·권역 알림 4건) |
| 4. HTML 리빌드 | ✅ | index.html 1.74MB, otb.html 223KB |
| 5. git commit | ✅ | 커밋 fe24de4 생성 (13 files) |
| 5. git push | ❌ | 샌드박스 네트워크가 GitHub SSH 차단 — 푸시 불가 |

## 단계별 상세

### 2. 경쟁사 프로모션 수집 (collect_gs_monitor.py)
- 스케줄 파일에 적힌 `gs_monitor_collector.py`는 존재하지 않아 실제 스크립트 `collect_gs_monitor.py`를 실행함.
- 원격 소스(`cksals00-ai.github.io/sono-competitor-crawler/...`, raw.githubusercontent.com) 8개 후보 전부 도달 실패 — 샌드박스에서 해당 도메인 outbound 차단.
- 주의사항("더미 데이터 절대 금지", "에러 시 기존 JSON 보존")에 따라 **기존 `data/competitors.json` 보존**.
  - 현재 데이터: 경쟁사 9개, 권역 4개(vivaldi/central/south/apac), `_updated_at` = 2026-04-21 (실데이터, 다소 오래됨)
- 참고: 스케줄 파일은 `docs/data/competitors.json`을 점검하라고 하지만, 실제 파이프라인은 `data/competitors.json`을 사용하며 build.py가 이를 HTML에 직접 주입함(경쟁사 콘텐츠 빌드 산출물에 정상 포함 확인).

### 3. 인사이트 생성 (generate_insights.py)
- `data/enriched_notes.json` 갱신, `_generated_at` = 2026-06-03T02:06 KST, `report_date` = 2026-06-03 수요일.
- today_headline: "6월 온북 57,908RN · 전월(5월 78,351RN) 대비 ▼26.1% 감소."
- action_alerts 4개 권역 갱신:
  - vivaldi: 88.0% 주의 구간, Mega Channel 시너지 가속 권장 (뉴스 3건)
  - central: Pacing 98.0% 관찰 필요, 경쟁사 할인율 상승 대응 (뉴스 8건)
  - south: 제주 노선 증편 호재 + 신규 숙박 공급 압박, ADR 방어·차별화 (뉴스 8건)
  - apac: 환율·유가 악화로 한국발 수요 둔화, 하이퐁 Strategy 05 가속 (뉴스 8건)
- 주의: KPI K1/K2/K3가 None으로 로드됨(소스 KPI 미연동). DB집계 기반 인사이트는 정상 생성.

### 4. 전체 빌드 (build.py)
- index.html 빌드 완료: 1,741,941 bytes (체크포인트 100KB 기준 충족) — "Auto-Built 2026-06-03 02:07 KST"
- otb.html: 222,788 bytes
- 주입 항목: OTB allMonths, 주간리포트, YoY 사업장별(88개), 당일분석 교차검증 통과(NET=2367), YoY OTB 비교(4탭/25사업장), 인사이트 패널, 카테고리 뉴스(9개·299건), 패키지 트렌드(14,409계열 중 TOP100), Daily Booking(25사업장·4개월).

### 5. 커밋 & 푸시
- `git add -A` → 커밋 `fe24de4` "chore(auto): trend regional update ... [skip ci]" (13 files, +1411/-1221) 생성 완료.
- `git push origin main` **실패**: `CONNECT ssh.github.com:443: Forbidden` — 샌드박스 outbound SSH 차단.
- 폴백 `git pull --rebase origin main`도 동일 사유로 실패.
- **로컬 커밋은 정상 보존됨.** 워킹트리 clean. origin과 1커밋 차이(미푸시).

## 후속 조치 (사용자 확인 필요)
샌드박스 네트워크 제약으로 GitHub 푸시가 차단됩니다. 로컬 커밋(fe24de4)은 만들어져 있으니, 네트워크가 되는 환경(로컬 터미널)에서 아래만 실행하면 반영됩니다:

```
cd ~/Desktop/gs_daily_trend_news_public_temp
git push origin main
```

푸시 거부 시: `git pull --rebase origin main && git push origin main`
