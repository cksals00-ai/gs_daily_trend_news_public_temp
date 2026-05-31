# action-calendar-sync — 2026-05-31 06:2x KST

## 결과 요약
구글시트 fetch → 기획전 동기화 → 빌드 → 커밋까지 정상 완료. **푸시만 실패** (샌드박스 네트워크 차단).

## 단계별
1. **Git lock 정리**: 잔존 `.git/index.lock`(20:04 생성)을 rename으로 우회 제거(virtiofs는 unlink 불가, rename 가능).
2. **Chrome MCP fetch** (샌드박스→docs.google.com 직접 접속 불가 확인, HTTP 000):
   - 메인 시트 CSV: 기준일자 **2026-05-31**, 38,202자. gzip+base64로 전송, DJB2 체크섬 일치(392759409) — 바이트 단위 무손실 검증.
   - 서브시트 gid 매핑 7개 발견. Key 서브시트 **20**(gid 1066532729), **198**(gid 730981956) fetch — CRC32 일치 검증, 각 123 패키지코드.
   - `data/cache/campaign_main.csv`, `data/cache/subsheets/{20,198}.csv` 저장.
3. **generate_campaign_data.py**: 404행 수신 → events **225**건, total_campaigns 225 / summer 49, 채널 카테고리 23개, Key 서브시트 2건 적재(패키지코드 246개).
4. **generate_campaign_performance.py**: raw_db 27/28 매칭 7,237행 → by_key **1건**(Key 20), 누적 RN **3,718**, 매출 724.4백만.
5. **build.py**: 전체 리빌드 완료 (index.html 1,693,128 bytes / otb.html 218,113 bytes). `Auto-Built 2026-05-31 06:23 KST`. data_freshness.json(5개 소스) 생성.
6. **커밋**: `2807340 chore(auto): action calendar sync ... [skip ci]` — 13 files changed.

## 체크포인트
- CSV 길이 38,202자(>1000) ✓
- campaign_data.json: events 225, package_codes 보유 key = {20, 198} ✓
- campaign_performance.json: by_key 1, 총 RN 3,718 ✓

## ⚠ 미완료: git push
- 원격이 `ssh://git@ssh.github.com:443`인데 샌드박스 프록시가 SSH(ssh.github.com:443 "Forbidden") 및 HTTPS(github.com 403/000) egress를 차단.
- `/tmp/gh_credentials` 부재. pull/rebase로 해결되는 충돌이 아니라 네트워크 허용목록 문제.
- **로컬 커밋은 보존**되어 origin/main 대비 5 커밋 앞섬. 네트워크가 열린 다음 실행 또는 사용자가 직접 `git push origin main` 하면 반영됨.

*더미 데이터 미사용. 모든 데이터는 원본 시트/ raw_db에서 무손실 검증 후 생성.*
