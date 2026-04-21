# GS팀 데일리 트렌드 리포트 (V6)

> © 2026 GS팀 · Haein Kim Manager · All Rights Reserved

## 🎯 개요

SONO Hotels & Resorts + TRINITY GS팀(General Sales, 채널 운영팀) 데일리 트렌드 리포트.

**R&R**: OTA · GOTA · Inbound

## 🆕 V6 주요 변경사항

### 1. 3개 권역 구조로 재편
- **Region 01 · 아시아퍼시픽** (APAC): 하이퐁, 괌, 하와이, GSA
- **Region 02 · 한국중부** (Central KR): 비발디권역, 델피노, 양평, 고양 등
- **Region 03 · 한국남부** (Southern KR): 제주, 여수, 거제, 남해, 진도 등

### 2. 트렌드 3축 강화
- **🕐 시간축**: 8주 추이선 + 변곡점 + 4주 예측
- **📍 맥락축**: 왜? 언제부터? 얼마나? (Context 박스)
- **🔗 연결축**: 인과 체인 다이어그램 (5단계 플로우)

### 3. 브랜딩
- `T'WAY` → `TRINITY`
- 흘림체 제목 (Gowun Batang + Nanum Pen Script)
- 본문 Noto Sans KR

### 4. 신규 UI 요소
- 🎯 **액션 알림** (권역별 "지금 뭘 해야" 박스)
- 🔮 **Forecast 박스** (4주 후 예측)
- 📊 **변곡점 표시** (추세 바뀐 시점)

## 📦 구조

```
v6_final/
├── docs/
│   └── index.html              # GitHub Pages
├── scripts/
│   └── collect_powerbi.py      # Power BI 수집
├── data/
│   └── powerbi_latest.json     # 자동 갱신
└── .github/workflows/
    └── deploy.yml              # 매일 08:00 KST
```

## 🚀 배포

```bash
# 기존 레포에 V6 덮어쓰기
cd <your-repo>
cp -r /path/to/v6_final/* .
cp -r /path/to/v6_final/.github .
git add .
git commit -m "feat: V6 - 3-region structure + trend 3-axis"
git push
```

## 📞 문의
GS팀 · Haein Kim Manager
