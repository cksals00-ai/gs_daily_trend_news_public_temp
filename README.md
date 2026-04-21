# SONO & TRINITY · GS Daily Trend Report (V7)

> © 2026 GS팀 · Haein Kim Manager

🔗 **대시보드**: https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/

---

## 🎯 V7 핵심 특징

1. **KPI만 매일 수정** — 담당자 작업 2~3분
2. **뉴스 자동 수집** — Google News RSS에서 호텔/리조트/항공/OTA/관광/레저 (소노 언급 제외)
3. **오늘의 한 줄 자동 생성** — KPI 값에 따라 문구 자동 작성
4. **4개 권역 구조** — 비발디 > 중부 > 남부 > APAC
5. **완전 자동 푸시** — `push_to_github.py` 한 번이면 전체 배포

## 📂 구조

```
v7/
├── docs/
│   └── index.html                    ← GitHub Pages 대시보드
├── scripts/
│   ├── collect_news.py               ← 1️⃣ Google News 자동 수집 (소노 제외 필터)
│   ├── collect_powerbi.py            ← 2️⃣ Power BI 자동 수집 (선택)
│   ├── generate_insights.py          ← 3️⃣ KPI → 오늘의 한 줄 + 액션 자동 생성
│   ├── build.py                      ← 4️⃣ HTML 빌드 (모든 데이터 주입)
│   └── push_to_github.py             ← 🚀 전체 파이프라인 + git push
├── data/
│   ├── daily_notes.json              ← ⭐ 담당자가 매일 KPI만 수정
│   ├── enriched_notes.json           ← (자동 생성)
│   ├── news_latest.json              ← (자동 수집)
│   └── powerbi_latest.json           ← (자동 수집)
├── .github/workflows/
│   └── deploy.yml                    ← GitHub Actions 완전 자동화
├── GUIDE.md                          ← 2~3분 일일 업데이트 가이드
└── README.md                         ← 본 파일
```

## 🔄 자동화 흐름

```
┌─ 매일 08:00 KST ────────────────┐
│ 매 4시간마다 (뉴스 갱신)           │
│ data/ 파일 수정 시               │
│ 수동 트리거 (Actions 탭)          │
└────────────────────────────────┘
         ↓
┌─────────────────────────────────┐
│  GitHub Actions 자동 실행        │
├─────────────────────────────────┤
│ ① 뉴스 수집 (Google News RSS)   │
│ ② Power BI 수집 (옵션)           │
│ ③ 인사이트 자동 생성              │
│    (KPI → 오늘의 한 줄 + 액션)    │
│ ④ HTML 빌드                     │
│ ⑤ Git commit & push             │
│ ⑥ GitHub Pages 배포              │
└─────────────────────────────────┘
         ↓
대시보드 URL 자동 갱신 (2~3분)
```

## 👤 담당자 일일 작업

**매일 2~3분, KPI 3개 숫자만 수정**:

👉 https://github.com/cksals00-ai/gs_daily_trend_news_public_temp/edit/main/data/daily_notes.json

상세 가이드: [GUIDE.md](GUIDE.md)

## 🛠️ 로컬 실행

```bash
# 전체 파이프라인 (뉴스 수집 + 빌드 + 푸시)
python scripts/push_to_github.py

# 푸시 없이 테스트만
python scripts/push_to_github.py --dry-run

# 뉴스 건너뛰고 빌드만
python scripts/push_to_github.py --skip-news
```

## 🔒 보안 (7-day Pilot)

- **2026.04.21 ~ 04.27**: PUBLIC 레포 임시 운영
- **차주**: 보안 검토 후 PRIVATE 전환 예정
- 소노/대명 등 자사 언급 뉴스 자동 필터링
