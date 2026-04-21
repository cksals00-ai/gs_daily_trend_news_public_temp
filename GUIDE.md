# 📝 매일 업데이트 가이드 (GS팀 담당자용)

> © 2026 GS팀 · Haein Kim Manager

## ⏰ 매일 작업 시간: 약 5~10분

매일 아침 출근 직후, **`data/daily_notes.json` 하나만 수정**하면 자동으로 대시보드에 반영됩니다.

---

## 🎯 작업 순서

### STEP 1 — GitHub에서 파일 열기 (1분)

👉 https://github.com/cksals00-ai/gs_daily_trend_news_public_temp/edit/main/data/daily_notes.json

✏️ 연필 아이콘 클릭해서 편집 모드로 진입

### STEP 2 — 수정 (5~8분)

**반드시 수정할 항목 3가지:**

#### 1️⃣ `report_date` (날짜)
```json
"report_date": "2026-04-22",  ← 오늘 날짜로
"report_day_kst": "수요일"      ← 요일
```

#### 2️⃣ `today_headline.text` (🔥 오늘의 한 줄)

임원 보고용 핵심 메시지. 2~3문장 이내.

**예시 템플릿:**
```
[권역/사업장] [상태/숫자]. [원인/맥락]. [필요 액션].

예:
"비발디파크 소노캄만 부진(47.6%). 한화 -35% 공세 직격. 
Smart 요금제 + 멤버십 강화 시급."

예:
"남부권 제주 노선 +12% 증편 호재 vs 히든클리프 -38% 오픈 공급 압박. 
ADR 방어 + 경험 차별화 대응 필요."
```

**작성 팁:**
- 최우선 이슈를 앞에
- 숫자로 설득력 강화 (달성률, YoY %, 경쟁사 할인율 등)
- 액션 포인트 포함

#### 3️⃣ `executive_kpi` (임원 KPI 3개)

아침 Power BI 확인 후 업데이트:
```json
"kpi_1": {
  "label": "🎯 비발디 권역 달성률",
  "value": "84.1",    ← 여기 숫자만 수정
  "unit": "%",
  "delta": "▼ 소노캄 부진 영향"   ← 간단한 코멘트
},
```

---

**추가 수정 권장 항목 (선택):**

#### 4️⃣ `region_status` (4개 권역 요약)

각 권역별 달성률과 한 줄 메모:
```json
"vivaldi": {
  "달성률": "84.1",
  "메모": "▼ 소노캄 부진"
},
"central": {
  "달성률": "108.4",
  "메모": "▲ 8.4% vs 목표"
},
```

#### 5️⃣ `action_alerts` (권역별 액션 알림)

권역마다 "지금 뭘 해야 하나" 한 문장:
```json
"vivaldi": "소노캄 47.6% 위기. 한화 -35% 공세 대응 시급.",
"central": "국내 수요 +18% vs 경쟁 31% 할인 쌍방향 압력.",
```

### STEP 3 — 커밋 (30초)

페이지 하단:
- **Commit message**: `daily: update 2026-04-22` (날짜 바꾸기만)
- **Commit directly to main branch** 선택
- **Commit changes** 버튼 클릭

### STEP 4 — 자동 반영 확인 (2~3분)

1. Actions 탭 이동:
   👉 https://github.com/cksals00-ai/gs_daily_trend_news_public_temp/actions

2. 워크플로우 실행 확인 (자동 시작됨)
3. 초록 체크마크 ✓ 뜨면 완료
4. 대시보드 URL 접속해서 반영 확인:
   👉 https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/

---

## 🚨 주의사항

### ✅ DO
- JSON 구조 유지 (따옴표, 쉼표, 중괄호)
- `"_"`로 시작하는 필드는 주석이므로 수정 불필요
- 숫자는 따옴표로 감싸기 (예: `"84.1"`)

### ❌ DON'T
- `_description`, `_instruction` 등 내부 필드 수정 금지
- JSON 파일 구조 변경 금지 (키 이름 유지)
- 마지막 중괄호 `}` 삭제 금지

---

## 🆘 실수했을 때

### JSON 문법 에러
- 커밋 후 Actions 탭에서 **빨간 X** 표시
- 이전 커밋으로 되돌리기:
  👉 Code 탭 → `data/daily_notes.json` 우측 상단 **History** → 이전 버전 **Revert**

### 대시보드가 업데이트 안 됨
1. Actions 탭 확인 (실패 여부)
2. 브라우저 강제 새로고침 (`Cmd+Shift+R`)
3. 수동 워크플로우 실행:
   - Actions 탭 → Daily Build & Deploy → **Run workflow** 버튼

---

## 📊 자동화 되는 부분

담당자가 **손댈 필요 없는** 자동 항목:

| 항목 | 자동화 방식 |
|------|----------|
| 8주 추이선 차트 | 매일 08:00 자동 갱신 |
| 채널 TOP 5 (RNS) | Power BI 자동 수집 |
| 매크로 지표 | API 연동 (향후) |
| 경쟁사 프로모션 | GS Monitor 연동 (향후) |
| 뉴스 | 카카오톡 파서 (향후) |

현재 **PILOT 기간**이라 일부는 더미 데이터이며, 정식 운영 시 점진적 자동화됩니다.

---

## 📞 문의

- **시스템 문제**: GS팀 기술 담당자
- **데이터 관련**: Haein Kim Manager

---

**마지막 업데이트**: 2026.04.21
