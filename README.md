# SONO & TRINITY · GS Daily Trend Report (V7)

> © 2026 GS팀 · Haein Kim Manager

## 🎯 개요

SONO Hotels & Resorts + TRINITY GS팀 데일리 트렌드 리포트.

🔗 **[대시보드 접속](https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/)**

## 🆕 V7 핵심 변경

### 1. 4개 권역 구조 (재편)
| 순서 | 권역 | 포함 사업장 |
|------|------|----------|
| **Region 01** | 비발디파크 🍷 | 소노캄/소노벨/소노펠리체/빌리지 비발디 (4개) |
| **Region 02** | 한국중부 🏔️ | 델피노, 양평, 고양, 양양, 삼척, 단양 등 |
| **Region 03** | 한국남부 🌊 | 제주, 여수, 거제, 남해, 진도 등 |
| **Region 04** | 아시아퍼시픽 🌏 | 하이퐁, 괌, 하와이, GSA |

### 2. 트렌드 카드 3×2 = 6개 (권역마다)
- **시간축**: 8주 추이선 + 변곡점 + 4주 예측
- **맥락축**: 왜? 언제부터? 얼마나? (Context)
- **연결축**: 권역별 영향 분석

### 3. 🔥 오늘의 한 줄 - 매일 수기 입력
- 담당자가 아침마다 `data/daily_notes.json` 수정
- Git push → GitHub Actions 자동 빌드 → 즉시 대시보드 반영

### 4. 완전 자동화
- 매일 한국시간 08:00 자동 빌드
- `data/` 폴더 변경 시 즉시 배포
- Power BI 데이터 자동 수집

## 📦 구조

```
v7/
├── docs/
│   └── index.html              ← GitHub Pages 대시보드
├── scripts/
│   ├── build.py                ← daily_notes.json → HTML 주입
│   └── collect_powerbi.py      ← Power BI 데이터 수집
├── data/
│   ├── daily_notes.json        ← ⭐ 매일 담당자 수정
│   └── powerbi_latest.json     ← 자동 수집 (매일 08:00)
├── .github/workflows/
│   └── deploy.yml              ← 자동화 워크플로우
├── GUIDE.md                    ← 담당자 일일 업데이트 가이드
└── README.md                   ← 본 파일
```

## 🚀 매일 운영

담당자는 매일 아침 5~10분만 투자하면 됩니다:

1. `data/daily_notes.json` 열기
2. 오늘 날짜 + 🔥 오늘의 한 줄 + KPI 수정
3. 커밋 → 자동 반영

자세한 가이드: **[GUIDE.md](GUIDE.md)**

## 🔄 자동화 흐름

```
담당자 daily_notes.json 수정
         ↓
    Git Commit & Push
         ↓
 GitHub Actions 자동 실행
    ├─ Power BI 데이터 수집
    ├─ build.py 실행 (HTML 주입)
    └─ GitHub Pages 배포
         ↓
    대시보드 URL 자동 갱신 (2~3분)
```

## ⏰ 빌드 트리거

| 트리거 | 시점 | 용도 |
|--------|------|------|
| **자동 스케줄** | 매일 08:00 KST | Power BI 자동 수집 + 빌드 |
| **Push 이벤트** | data/ 변경 시 | 수기 입력 즉시 반영 |
| **수동 실행** | Actions 탭 | 긴급 재빌드 |

## 📊 데이터 소스

| 소스 | 상태 | 담당자 개입 |
|------|------|----------|
| Power BI (RNS/REV) | 🟢 자동 | 확인만 |
| 🔥 오늘의 한 줄 | 🟡 수기 | **매일 작성** |
| 임원 KPI | 🟡 수기 | 매일 확인/수정 |
| 액션 알림 | 🟡 수기 | 매일 확인/수정 |
| 매크로 (유가/환율) | 🔴 더미 | (향후 API 연동) |
| 경쟁사 | 🔴 더미 | (향후 GS Monitor 연동) |
| 뉴스 | 🔴 더미 | (향후 카카오톡 파서) |

## 🗓️ 로드맵

- **Week 1 (4/21~4/27)**: 7-day pilot (전체 노출)
- **Week 2**: 보안 검토 + 접근권한 설정
- **Week 3~4**: 매크로/경쟁사 API 연동
- **Month 2**: 뉴스 파서 통합, 사업장별 심화 뷰

## 📞 문의

GS팀 · Haein Kim Manager
