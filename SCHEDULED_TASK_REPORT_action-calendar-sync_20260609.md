# 자동 실행 리포트 — action-calendar-sync (2026-06-09)

실행 시각: 2026-06-09 08:00 KST (자동 스케줄)

## 요약
빌드·데이터 재집계는 정상 완료. **GitHub push만 실패** (샌드박스에서 GitHub 접속 차단). 커밋은 로컬에 생성됨.

## 단계별 결과

### 1단계: Chrome MCP 구글시트 fetch — ❌ 건너뜀 (fallback)
- Chrome 브라우저 미연결 (`list_connected_browsers` → 빈 배열, `tabs_context_mcp` 2회 재시도 모두 "not connected").
- 주의사항 규칙에 따라 fallback: 기존 캐시 CSV 유지, 시트 fetch 생략.
- `data/cache/campaign_main.csv` (65,378자, 6/1자 캐시) 사용.

### 2단계: 파이썬 스크립트 — ✅ 성공
- `generate_campaign_data.py --csv-file ... --subsheets-dir ...` → campaign_data.json 생성, **events 225건**, 패키지코드 매핑 246개 (다중 Key 중복 123건은 첫 Key 귀속).
- `generate_campaign_performance.py` → raw_db 27/28 txt 매칭 9,695행, **by_key 1건 / 누적 RN 4,554 / 매출 879.5백만**. (raw_db 기반이라 네트워크 불필요 — 정상 실행)

### 3단계: 빌드 + 푸시
- `build.py` → ✅ 전체 리빌드 완료 (index.html 1.65MB, otb.html, db_aggregated 등 동기화, data_freshness 2026-06-09 07:59 KST).
- `git commit` → ✅ 로컬 커밋 생성: **949f5b9** `chore(auto): action calendar sync 2026-06-09 08:00 KST [skip ci]`
- `git push origin main` → ❌ **실패**.

## Push 실패 원인
샌드박스 네트워크에서 GitHub가 차단됨:
- SSH (`ssh://git@ssh.github.com:443`): `E CONNECT ssh.github.com:443: Forbidden`
- HTTPS (`https://github.com`): `HTTP code 403 from proxy after CONNECT`

`pull --rebase` 후 재시도도 동일하게 실패. 네트워크 정책 우회는 시도하지 않음.

⚠️ **미푸시 커밋 19건 누적** (2026-06-07 이후). 이전 자동 실행들도 동일하게 push에 실패해온 것으로 보임. GitHub 접속이 가능한 환경(사용자 PC 등)에서 `git push origin main` 1회 실행 필요.

## 체크포인트
| 항목 | 값 | 상태 |
|---|---|---|
| campaign_main.csv | 65,378자 (>1000) | ✅ (캐시) |
| campaign_data events | 225 | ✅ |
| campaign_data 패키지코드 매핑 | 246개 | ✅ |
| performance by_key | 1건 | ✅ |
| performance 총 RN | 4,554 | ✅ |
| build | 완료 | ✅ |
| push | GitHub 차단 | ❌ |

## 조치 필요
1. 매일 정상 push를 원하면 (a) Chrome 확장 연결 유지(시트 최신화) + (b) 샌드박스 git push 가능 여부 확인. 현재 환경에서는 GitHub push가 구조적으로 불가.
2. 누적 19개 커밋을 외부에서 push 권장.
