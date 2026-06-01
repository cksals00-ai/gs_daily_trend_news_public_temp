# action-calendar-sync 자동 실행 리포트 — 2026-06-02

실행 시각: 2026-06-02 06:20 KST (자동 스케줄)
커밋: `b0dc8b9` chore(auto): action calendar sync 2026-06-02 06:20 KST [skip ci]

## 결과 요약

| 단계 | 상태 |
|---|---|
| git 락 정리 | ✅ index.lock / HEAD.lock 제거 (mv 우회) |
| 구글시트 CSV fetch (Chrome MCP) | ✅ 메인 + 서브시트 |
| campaign_data.json 생성 | ✅ |
| campaign_performance.json 재집계 | ✅ |
| build.py 전체 리빌드 | ✅ |
| git commit | ✅ (로컬) |
| git push origin main | ❌ 차단 (아래 참조) |

## 1단계: 구글시트 fetch

Chrome MCP(Browser 1)로 pubhtml 페이지 접속 후 `pub?output=csv` fetch.

- 메인 기획전 시트(gid 1818134248, "DATA"): 기준일자 **2026-06-02**, 404행, 38,033자.
- 서브시트 gid 매핑 추출: cal / DATA / 25·26 인플루언서 / 26년 연간PLAN / 20(1066532729) / 198(730981956).
- Key 서브시트(20, 198) fetch 완료.

검증: 어제 캐시본과 라인 단위 해시 비교 → 메인 시트는 하단 91행(인덱스 313~403)만 변경, 상단 313행 동일. 변경분만 전송 후 **전체 파일 서명(길이·문자합·다항해시)을 브라우저 원본과 정확히 일치 확인**하여 무결성 보장. 서브시트 20/198은 실질 내용 변동 없음(이모지 UTF-16 카운팅 차이로 인한 오탐, 패키지코드 전부 동일).

- 캐시 저장: `data/cache/campaign_main.csv` (65,378 bytes), `data/cache/subsheets/20.csv`, `198.csv`.

## 2단계: 파이썬 재집계

`generate_campaign_data.py`
- events: **225건**, 여름 캠페인 49건, 채널 카테고리 23개
- 패키지코드 적재 Key: 2건(20, 198), 매핑 패키지코드 246개
- package_codes 보유 이벤트: 4건

`generate_campaign_performance.py`
- by_key: **1건**, 누적 RN **3,743**, 누적 매출 **727.3백만**
- 매칭 행: 7,571 (예약 3,737 / 취소 3,834) — raw_db 27/28 자료 기준
- raw_db 소스 일자: 20260601 (가용 최신본)

## 3단계: 빌드

`build.py` 전체 리빌드 성공 — index.html(1,677,087 bytes), otb.html(210,755 bytes), db_aggregated.json·package_series_trend.json·rm_fcst.json·data_freshness.json 동기화. "Auto-Built 2026-06-02 06:19 KST".

## ⚠ push 실패 — 조치 필요

`git push origin main` 이 **샌드박스 네트워크 정책으로 차단**되었습니다.
- 원격: `ssh://git@ssh.github.com:443/cksals00-ai/...` → 프록시 403 Forbidden
- HTTPS(github.com) 도 프록시 403 → GitHub 자체가 이 실행 환경에서 도달 불가

현재 로컬 `main` 이 `origin/main` 보다 **15 커밋 앞서** 있습니다(오늘 1건 + 5/30 이후 누적 14건). 즉 이전 자동 실행들의 푸시도 동일 사유로 누락된 상태입니다.

커밋·빌드 산출물은 로컬 저장소에 정상 보존되어 있으며 작업 트리는 clean 합니다. **GitHub 접근이 가능한 환경(사용자 Mac 터미널 등)에서 `git push origin main` 1회 실행이 필요**합니다. 보안 규칙상 네트워크 우회는 시도하지 않았습니다.

```
cd ~/Desktop/gs_daily_trend_news_public_temp
git push origin main
```
