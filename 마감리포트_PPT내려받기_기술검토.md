# 마감리포트 PPT 내려받기 기능 — 기술 검토 보고서

**작성일:** 2026.05.13  
**검토 대상:** gs-closing-report.html (마감리포트 페이지)  
**요청 사항:** 마감리포트 상단에 "내려받기" 버튼 추가 → 리포트 내용을 PPTX로 생성/다운로드  

---

## 1. 현재 구조 분석

### 1-1. 파일 구성

| 파일 | 역할 |
|------|------|
| `docs/gs-closing-report.html` | 프론트엔드 SPA (2,266줄), 데이터 로딩·렌더링 전체를 클라이언트 JS로 처리 |
| `docs/build_closing_report.py` | Python 빌드 스크립트 (296줄), db_aggregated.json 기반 분석 데이터 사전 생성 |
| `docs/data/otb_data.json` | 메인 데이터 (월별 summary, 사업장별 실적, YoY 테이블) |
| `docs/data/db_aggregated.json` | 상세 데이터 (82MB, 채널/세그먼트/거래처별 집계) |
| `docs/data/package_series_trend.json` | 상품 카테고리별 추이 |
| `docs/data/inbound_kpi.json` | 인바운드 국가별 KPI |

### 1-2. 데이터 로딩 방식

클라이언트에서 4개 JSON을 `Promise.all` → `fetch`로 로딩:

```
otb_data.json → DATA (메인)
package_series_trend.json → PKG_DATA (상품)
db_aggregated.json → DB_DATA (채널 거래처)
inbound_kpi.json → IB_KPI_DATA (인바운드)
```

### 1-3. 리포트 섹션 구조 (PPT에 들어갈 콘텐츠)

| 섹션 | 내용 | PPT 슬라이드 대응 |
|------|------|-------------------|
| SECTION 01 | 전체 실적 리뷰 — KPI 카드(RN/Revenue/ADR), 핵심 요약, 누계, 월별 RN 추이 차트 | 슬라이드 1-2 |
| SECTION 02 | 시장 환경 분석 — 시장 동향·채널·상품·계절성 인사이트 | 슬라이드 3 |
| SECTION 03-1 | 사업장별 실적 — 목표/실적/전년비 테이블 (RN·ADR·Revenue) | 슬라이드 4-5 |
| SECTION 03-2 | 채널(거래처)별 실적 — 세그먼트별 + 거래처별 상세 | 슬라이드 6-7 |
| SECTION 03-3 | 상품(카테고리)별 구성 — 파이 차트 + 테이블 | 슬라이드 8 |
| SECTION 03-4 | 리드타임 분석 — 사업장별 평균 리드타임 차트 | 슬라이드 9 |
| SECTION 03-5 | ADR 분석 — 월별 ADR 추이 + 사업장별 ADR 테이블 | 슬라이드 10 |
| SECTION 03-6 | 취소율 분석 | 슬라이드 11 |
| SECTION 03-7 | 국가별 분석 (인바운드) | 슬라이드 12 |
| SECTION 04 | 전략 방향 — 전략적 시사점 | 슬라이드 13 |

### 1-4. 서식 규칙 (확인됨)

- **객실수(RN):** 정수, 천단위 콤마 (예: `12,345`)
- **Revenue:** 백만원 단위 (예: `1,234백만`)
- **ADR:** 천원 단위 (예: `234천`)
- **YoY:** 부호 포함 소수점 1자리 (예: `+5.3%`)
- **사업장:** 번호순 정렬 (`localeCompare('ko', {numeric: true})`)
- **예산 용어:** "예산" → "목표"로 표기
- **채널:** "채널" = "거래처"로 사용
- **세그먼트:** OTA, G-OTA, Inbound 3개 중심

---

## 2. PPT 생성 방식 비교

### 방안 A: 클라이언트 사이드 — PptxGenJS (추천)

| 항목 | 내용 |
|------|------|
| **라이브러리** | [PptxGenJS](https://gitbrent.github.io/PptxGenJS/) (CDN 로드 가능, ~170KB gzipped) |
| **장점** | 서버 불필요, 현재 아키텍처(GitHub Pages 정적 호스팅)와 완벽 호환, 이미 로딩된 DATA/DB_DATA 객체 직접 활용 가능 |
| **단점** | 차트는 이미지로 변환 필요(Canvas → toDataURL), 복잡한 레이아웃 제한적 |
| **템플릿 지원** | ⚠️ 제한적 — PptxGenJS는 기존 pptx 파일을 템플릿으로 여는 기능 없음. 마스터 슬라이드/레이아웃을 코드로 정의해야 함 |
| **구현 난이도** | 중 |

```javascript
// 구현 예시 (개념)
const pptx = new PptxGenJS();
pptx.defineSlideMaster({ title: 'SONO_MASTER', background: { color: '101217' } });
const slide = pptx.addSlide({ masterName: 'SONO_MASTER' });
slide.addTable(propertyRows, { x: 0.5, y: 1.2, w: 9, fontSize: 9 });
pptx.writeFile({ fileName: '2026년_마감보고서_4월.pptx' });
```

### 방안 B: 서버 사이드 — python-pptx

| 항목 | 내용 |
|------|------|
| **라이브러리** | [python-pptx](https://python-pptx.readthedocs.io/) |
| **장점** | 회사 양식 pptx를 템플릿으로 직접 사용 가능 (`Presentation('template.pptx')`), 마스터 슬라이드/레이아웃 완벽 지원, 차트 네이티브 지원 |
| **단점** | 별도 서버(API) 필요, 현재 GitHub Pages 정적 호스팅에서 직접 실행 불가 |
| **템플릿 지원** | ✅ 완벽 — `prs = Presentation('회사양식.pptx')`로 기존 템플릿의 마스터/레이아웃 유지 |
| **구현 난이도** | 중~상 (서버 인프라 추가 필요) |

```python
# 구현 예시 (개념)
from pptx import Presentation
prs = Presentation('회사양식_템플릿.pptx')  # 회사 양식 기반
layout = prs.slide_layouts[1]  # 회사 양식의 레이아웃 활용
slide = prs.slides.add_slide(layout)
slide.placeholders[0].text = "2026년 4월 마감 보고서"
# 테이블, 차트 추가...
prs.save('마감보고서_202604.pptx')
```

### 방안 C: 하이브리드 — build_closing_report.py 확장

| 항목 | 내용 |
|------|------|
| **방식** | 기존 `build_closing_report.py`에 pptx 생성 로직 추가, 파이프라인(run_pipeline.sh) 실행 시 사전 빌드 |
| **장점** | 서버 실시간 API 불필요, 기존 빌드 프로세스에 통합, python-pptx 템플릿 활용 가능 |
| **단점** | 실시간 기간 선택 반영 불가(사전 빌드된 고정 파일), 월별·분기별 모든 조합을 미리 생성해야 함 |
| **추천도** | 월간 1회 마감 시 고정 리포트로 충분하다면 적합 |

---

## 3. 회사 양식 템플릿 적용 검토

### 3-1. python-pptx 방식 (방안 B/C) — 완벽 지원

```
사용자가 업로드한 '회사양식.pptx'를 템플릿으로 사용:

1. Presentation('회사양식.pptx') → 마스터 슬라이드, 레이아웃, 테마 색상, 폰트 유지
2. prs.slide_layouts → 회사가 정의한 레이아웃(제목 슬라이드, 내용 슬라이드 등) 선택
3. layout.placeholders → 회사 양식의 제목/본문 placeholder에 데이터 삽입
4. 회사 로고, 배경, 색상 테마 자동 적용
```

**필요한 사용자 입력:**
- 회사 양식 pptx 파일 1개 (마스터 슬라이드 정의됨)
- 각 레이아웃의 용도 설명 (어떤 레이아웃을 어디에 쓸지)

### 3-2. PptxGenJS 방식 (방안 A) — 수동 재현 필요

PptxGenJS는 기존 pptx를 열 수 없으므로, 회사 양식의 디자인 요소를 코드로 재현해야 합니다:

```
1. 회사 양식에서 추출할 정보:
   - 배경색/이미지, 로고 위치·크기, 폰트·색상 팔레트
   - 제목/본문 위치·크기, 푸터 텍스트
2. defineSlideMaster()로 마스터 정의
3. 수작업으로 디자인 일치시킴 (완벽한 재현 어려움)
```

### 3-3. 비교 요약

| 기능 | PptxGenJS (클라이언트) | python-pptx (서버/빌드) |
|------|----------------------|----------------------|
| 기존 pptx 템플릿 열기 | ❌ 불가 | ✅ 가능 |
| 마스터 슬라이드 유지 | ❌ 코드로 재현 | ✅ 자동 유지 |
| 회사 테마 색상 | ❌ 수동 지정 | ✅ 자동 적용 |
| placeholder 활용 | ❌ 좌표 직접 지정 | ✅ placeholder 인덱스 |
| 차트 지원 | ⚠️ 이미지로 변환 | ✅ 네이티브 차트 |
| 서버 필요 여부 | 불필요 | 필요 |

---

## 4. 추천 접근 방식

### 1순위 추천: 방안 C (하이브리드 빌드)

**이유:**
- 현재 `build_closing_report.py`가 이미 존재하고, `run_pipeline.sh`에서 자동 실행되는 구조
- 마감 리포트는 월 1회 확정 데이터이므로 실시간 생성 불필요
- python-pptx로 회사 양식 템플릿 완벽 적용 가능
- 서버 API 추가 없이 기존 빌드 파이프라인에 통합

**구현 흐름:**
```
1. 회사 양식 pptx를 docs/templates/closing_template.pptx에 배치
2. build_closing_report.py에 pptx 생성 함수 추가
3. 월별·분기별 pptx 파일을 docs/data/exports/에 사전 생성
4. gs-closing-report.html 상단에 "내려받기" 버튼 추가
5. 버튼 클릭 시 해당 기간의 사전 생성 pptx 다운로드 (단순 <a href> 링크)
```

### 2순위 추천: 방안 A + C 병행

- 기본적으로 방안 C로 사전 빌드
- 사용자가 동적으로 기간 조합(예: 특정 2개월만 선택)을 원할 경우, PptxGenJS 클라이언트 생성을 보조 기능으로 추가

---

## 5. "내려받기" 버튼 UI 위치

현재 HTML 구조 기준으로 두 가지 위치가 적합합니다:

**위치 1 (추천): hero 섹션 내부 — hero-meta 아래**
```html
<section class="hero">
  <div class="hero-inner">
    ...
    <div class="hero-meta">...</div>
    <div class="confidential">...</div>
    <!-- 여기에 추가 -->
    <button class="download-pptx-btn" onclick="downloadPPTX()">
      📥 PPT 내려받기
    </button>
  </div>
</section>
```

**위치 2: 사이드바 기간 선택 아래**
```html
<aside class="cr-sidebar">
  ...
  <div class="cr-sb-section">
    <button class="download-pptx-btn" onclick="downloadPPTX()">
      📥 PPT 내려받기
    </button>
  </div>
</aside>
```

---

## 6. 예상 작업량

| 단계 | 작업 내용 | 예상 시간 |
|------|-----------|-----------|
| 1 | 회사 양식 pptx 분석 (레이아웃·placeholder 매핑) | 2~3시간 |
| 2 | build_closing_report.py에 pptx 생성 로직 추가 | 8~12시간 |
| 3 | 테이블 서식 구현 (서식 규칙 반영: 천원/백만원 단위, 번호순 정렬 등) | 4~6시간 |
| 4 | 차트 생성 (python-pptx 차트 또는 matplotlib → 이미지) | 4~6시간 |
| 5 | HTML에 내려받기 버튼 및 다운로드 로직 추가 | 1~2시간 |
| 6 | 파이프라인 통합 및 테스트 | 2~3시간 |
| **합계** | | **21~32시간 (3~4일)** |

※ 회사 양식 없이 기본 디자인으로 먼저 구현할 경우 1단계 생략 가능 → 약 19~29시간

---

## 7. 필요한 사용자 입력

| 항목 | 필수 여부 | 설명 |
|------|-----------|------|
| 회사 양식 pptx 파일 | 선택 (강력 권장) | 마스터 슬라이드·레이아웃 정의된 템플릿 |
| 슬라이드 구성 확인 | 필수 | 어떤 섹션을 몇 장으로 나눌지 (위 SECTION별 대응표 검토) |
| 차트 포함 여부 | 필수 | 차트를 이미지로 넣을지, 편집 가능한 pptx 차트로 넣을지 |
| 로고 이미지 파일 | 선택 | 회사 양식에 없는 경우 별도 제공 |
| 다운로드 범위 | 필수 | 현재 선택된 탭(월별/분기별)만? 전체 연간? |

---

## 8. 결론

기술적으로 **충분히 실현 가능**합니다. 현재 아키텍처(GitHub Pages 정적 호스팅 + Python 빌드 파이프라인)를 고려하면, **방안 C(빌드 시 사전 생성)**가 가장 현실적이며, 회사 양식 템플릿도 python-pptx를 통해 완벽하게 적용할 수 있습니다.

다음 단계로 회사 양식 pptx 파일을 제공해주시면, 레이아웃 분석 후 구체적인 구현 설계를 진행할 수 있습니다.
