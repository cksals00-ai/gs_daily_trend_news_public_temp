# 스케줄 작업 리포트 — trend-report-regional (2026-06-16 02:08 KST)

## 요약
데이터 파이프라인(수집 → 인사이트 → 빌드)은 **실제 데이터로 정상 완료**되었습니다.
다만 마지막 git commit/push 단계가 **실행 환경(FUSE 마운트) 제약**으로 실패했습니다.
모든 산출물은 워킹트리에 보존되어 있으며, 더미 데이터는 사용하지 않았습니다.

## 단계별 결과

1. **git lock 정리** — `.git/index.lock`, `.git/HEAD.lock` 제거 시도. (시작 시점 정상)
2. **경쟁사 데이터 수집** ✅
   - `scripts/collect_gs_monitor.py`는 published GitHub Pages/raw 소스를 시도했으나 **샌드박스 프록시가 외부 GitHub 접근을 차단(HTTP 403)** 하여 모든 원격 소스 실패 → 설계대로 기존 JSON 보존.
   - 대신 **로컬에 마운트된 `sono-competitor-crawler` 레포의 실제 크롤 산출물**(`data/competitors.json`, 2026-06-15 16:45 KST, 커밋 완료분)을 소스로 사용. published mirror는 이 레포의 배포본이므로 동일한 실데이터입니다.
   - 수집기의 자체 정규화 함수(`normalize_competitor`)와 권역 요약 로직을 그대로 적용.
   - 결과: **경쟁사 5개사**, 전 권역 커버.
     - vivaldi 1 (한화리조트 -76%)
     - central 1 (하이원리조트, 신규)
     - south 1 (켄싱턴리조트, 신규)
     - apac 2 (롯데 -50%, 신라)
   - 직전 대비 중부·남부 권역이 신규 채워짐(이전엔 0건).
3. **인사이트 생성** ✅
   - `scripts/generate_insights.py` 정상 실행.
   - headline: "6월 온북 76,468RN · 전월(5월 78,351RN) 대비 ▼2.4% 감소"
   - action_alerts: vivaldi / central / south / apac 4개 권역 갱신.
   - report_date: 2026-06-16 화요일.
4. **HTML 빌드** ✅
   - `scripts/build.py` 정상 실행.
   - 경쟁사 카드 5개 + 권역요약 4개 권역 주입 확인.
   - `docs/index.html` **1,793,525 bytes** (100KB 기준 통과).
   - `docs/otb.html` 228,786 bytes.
5. **git commit & push** ❌ 실패
   - 원인: `.git`이 FUSE(virtiofs) 마운트 위에 있어 `unlink`가 차단됨("Operation not permitted").
   - `.git/index.lock`(0바이트)을 제거할 수 없어 commit이 거부됨:
     `fatal: Unable to create '.../.git/index.lock': File exists.`
   - 이는 일시적 lock이 아니라 현재 실행 환경의 파일시스템 제약으로, 샌드박스에서 복구 불가.

## 조치 필요 (호스트에서)
워킹트리에 갱신된 파일 **22건**이 커밋 대기 중입니다. 호스트(실제 맥)에서 아래를 실행하면 반영됩니다:

```
cd ~/Desktop/gs_daily_trend_news_public_temp   # 또는 ~/Projects/gs_daily_trend_news_public_temp
rm -f .git/index.lock .git/HEAD.lock
git add -A
git commit -m "chore(auto): trend regional update 2026-06-16 02:08 KST [skip ci]"
git push origin main      # 실패 시: git pull --rebase origin main 후 재시도
```

## 참고
- 프로젝트 실제 경로는 `~/Projects/gs_daily_trend_news_public_temp` (작업 파일의 `~/Desktop/...` 경로와 다름).
- 수집기 파일명은 작업 파일의 `gs_monitor_collector.py`가 아니라 실제로는 `scripts/collect_gs_monitor.py`.
- 외부 GitHub 접근이 차단된 환경에서는 collect 단계가 항상 원격 실패 → 로컬 크롤 레포를 소스로 쓰는 현재 방식이 유효.
