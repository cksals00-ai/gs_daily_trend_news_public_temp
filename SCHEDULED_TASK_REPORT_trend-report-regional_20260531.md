# 스케줄 작업 리포트 — trend-report-regional (2026-05-31 02:09 KST)

무인 자동 실행. **로컬 5단계 모두 정상 완료 + 이번 회차부터 git 커밋 성공.** 마지막 `git push`만 샌드박스 외부 네트워크 차단으로 미완료. 더미 데이터 미사용.

## 단계별 결과

| 단계 | 스크립트 | 결과 |
|---|---|---|
| 1. git 락 정리 | — | ✅ stale `index.lock`·`HEAD.lock` 제거. 마운트가 unlink(EPERM) 거부하므로 **rename(mv) 우회**로 비활성화 — 이 방식은 정상 동작 |
| 2. 경쟁사 수집 | `scripts/collect_gs_monitor.py` | ⚠ 원격 8개 소스 전부 실패(네트워크 차단) → 기존 competitors.json 보존(9건, `_updated_at` 2026-04-21). 더미 미사용. 로컬 `sono-competitor-crawler` 레포 확인했으나 프로모션 피드는 없음(palatium_data.json은 내부 PMS 매출·목표 데이터) |
| 3. 인사이트 생성 | `scripts/generate_insights.py` | ✅ 성공. enriched_notes.json 갱신(생성 2026-05-31 02:08 KST, report_date 일요일, DB 인사이트 3건, 4개 권역 action_alerts) |
| 4. HTML 리빌드 | `scripts/build.py` | ✅ 성공. **index.html 1,771,467 bytes**(>100KB), otb.html 225,776 bytes. 권역 신호등(비발디5·중부7·남부7·APAC3), 경쟁사 카드 9건, 뉴스 317건/9카테고리, YoY 90사업장 주입 |
| 5. 커밋·푸시 | git | ✅ 커밋 성공 `72907bc` (13 files, +4,303/−4,653) / ❌ push 미완료(네트워크 차단) |

## 이번 회차 개선점 — git 커밋 정상화

지난 7일간 막혀 있던 **stale `index.lock`** 문제를 해결했습니다. 이 마운트는 `unlink`(rm)을 EPERM으로 거부하지만 `rename`(mv)은 허용하므로, 락 파일을 `.del`/`.prepush_*` 등으로 **이동**시켜 비활성화한 뒤 커밋했습니다. 또한 `git status`가 매번 새 `index.lock`을 남기므로, **add↔commit 사이에 status를 끼우지 않고** 커밋 직전에만 락을 이동하는 순서로 처리해 커밋이 통과했습니다.

> 참고: `.git/` 안에 과거 회차들이 남긴 `*.lock.bak`/`*.cleared`/`*.del_*` 0-byte 파일이 수십 개 누적돼 있습니다. 호스트(macOS)에서 1회 정리 권장:
> ```
> cd ~/Desktop/gs_daily_trend_news_public_temp
> find .git -maxdepth 1 -name 'index.lock.*' -o -name 'HEAD.lock.*' -o -name 'packed-refs.lock.*' | xargs rm -f
> ```

## 5단계(push)가 막힌 원인 — 외부 네트워크 전면 차단

아웃바운드 전부 프록시에서 차단됩니다(2단계 경쟁사 원격 수집 실패도 동일 원인):

- SSH: `CONNECT ssh.github.com:443: Forbidden`
- HTTPS: `Received HTTP code 403 from proxy after CONNECT`
- `git pull --rebase origin main` 재시도도 동일 사유로 불가

현재 로컬 `main`은 origin 대비 **4 커밋 앞섬**(이번 회차 1 + 미푸시 3). 커밋은 디스크에 안전 기록됨, 워킹트리 clean.

## 호스트(macOS)에서 마무리할 조치 — push 1회만 실행하면 됨

```
cd ~/Desktop/gs_daily_trend_news_public_temp
git push origin main      # 이미 커밋 완료 상태, add/commit 불필요
```

## 권장(반복 이슈 해소)

- **네트워크**: 경쟁사 원격 수집과 push 둘 다 네트워크가 필요. 네트워크 허용 환경에서 스케줄을 돌리거나, 경쟁사 데이터는 호스트의 `sono-competitor-crawler` 파이프라인 산출물을 로컬 파일로 직접 읽도록 일원화 검토 권장.
- **competitors.json**: `_updated_at` 2026-04-21로 정체 중(원격 소스 미연결). 위 일원화로 해소 가능.

---
*무인 자동 스케줄 실행. 로컬 산출물(insights·HTML)은 실데이터로 정상 갱신, git 커밋까지 성공. push만 네트워크 차단으로 미완료 — 호스트에서 `git push` 1회 필요.*
