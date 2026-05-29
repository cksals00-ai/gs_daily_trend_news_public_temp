# 스케줄 작업 리포트 — trend-report-regional (2026-05-30 02:08 KST)

무인 자동 실행. 5단계 중 **로컬 작업(인사이트 생성·HTML 리빌드)은 정상 완료**, 외부 네트워크가 필요한 단계(경쟁사 원격 수집·git push)와 git 커밋은 환경 제약으로 미완료.

## 단계별 결과

| 단계 | 스크립트 | 결과 |
|---|---|---|
| 1. git 락 정리 | — | 시작 시 락 없음, 워킹트리 clean |
| 2. 경쟁사 수집 | `scripts/collect_gs_monitor.py` | ⚠ 원격 8개 소스 전부 실패(403) → 기존 competitors.json 보존(9건, _updated_at 2026-04-21). 더미 데이터 미사용 |
| 3. 인사이트 생성 | `scripts/generate_insights.py` | ✅ 성공. enriched_notes.json 갱신(생성 2026-05-30 02:07 KST, report_date 토요일, DB 인사이트 3건, 4개 권역 action_alerts) |
| 4. HTML 리빌드 | `scripts/build.py` | ✅ 성공. index.html 1,771,363 bytes(>100KB), otb.html 225,782 bytes. 권역 신호등(비발디5·중부7·남부7·APAC3), 경쟁사 카드 9건, 뉴스 317건/9카테고리 주입 |
| 5. 커밋·푸시 | git | ❌ 미완료(아래 참조) |

## 5단계가 막힌 원인 (2건, 7일째 동일 증상)

1. **git 커밋 불가 — stale `.git/index.lock`**
   `git add -A`가 0-byte `index.lock`을 남겼고, 샌드박스 마운트 권한(EPERM)으로 제거 불가(`Operation not permitted`). 호스트(macOS)에서 1회 수동 제거 필요:
   ```
   cd ~/Desktop/gs_daily_trend_news_public_temp
   rm -f .git/index.lock .git/HEAD.lock
   ```

2. **git push 불가 — 외부 네트워크 전면 차단**
   github.com·githubusercontent.com 포함 모든 아웃바운드가 403(Tunnel connection failed). 경쟁사 원격 수집(2단계)이 실패한 원인도 동일. `git pull --rebase` 재시도도 동일 이유로 불가.

## 워킹트리 상태 — 변경분은 디스크에 안전하게 기록됨 (미커밋 11개 파일)

```
 M data/admin_input.json
 M data/enriched_notes.json
 M docs/admin_suggestions.json
 M docs/data/admin_input.json
 M docs/data/daily_analysis_validation.json
 M docs/data/data_freshness.json
 M docs/data/otb_data.json
 M docs/data/weekly_comparison.json
 M docs/index.html
 M docs/otb.html
```
git log/status 읽기는 정상이며 index 손상 없음. 락 제거 후 정상 커밋/푸시 가능.

## 호스트에서 마무리할 조치
```
cd ~/Desktop/gs_daily_trend_news_public_temp
rm -f .git/index.lock .git/HEAD.lock
git add -A
git commit -m "chore(auto): trend regional update 2026-05-30 02:08 KST [skip ci]"
git push origin main
```

## 참고 (미해결 누적 이슈)
- 경쟁사 데이터는 호스트의 별도 파이프라인(`sono-competitor-crawler`)에서 실수집됨. 본 스케줄 작업 샌드박스에서는 네트워크 차단으로 원격 fetch 불가 — 네트워크 허용 환경에서 실행하거나 호스트 파이프라인으로 일원화 검토 권장.
- competitors.json `_updated_at`이 2026-04-21로 정체. 원격 소스 미연결 상태 지속.

---
*무인 자동 스케줄 실행. 환경 제약(네트워크 차단·마운트 EPERM)으로 커밋/푸시 불가하여 발견 사항 리포트로 갈음. 로컬 산출물(insights·HTML)은 실데이터로 정상 갱신 완료.*
