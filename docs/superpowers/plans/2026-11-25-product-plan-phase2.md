# 2027 상품기획 2차 — GA4 상품 단위 + 2027 기획 매트릭스(SEC10) + 비용 입력칸 Implementation Plan

> **For agentic workers:** 1차 계획(`2026-11-25-product-cube-phase1.md`)의 Global Constraints를 그대로 적용. 인라인 실행.

**Goal:** 스펙 §4.2(GA4 items)·§4.4 SEC10·§4.5 산출 로직을 구현해 "2027년 언제·어느 채널·어느 상품(구성)·어느 사업장·목표 RN/ADR/REV·수수료·비용(입력)·예상 공헌·근거"를 행 단위로 내고 엑셀로 내려받게 한다. 매트릭스(SEC9)의 2027Y 열을 이 결과로 채운다.

**Architecture:** (1) `scripts/fetch_ga4_items.py` → `docs/data/ga4_items.json` (GA4 Data API, 로컬 OAuth 토큰). (2) `scripts/build_product_plan_2027.py` → `docs/data/product_plan_2027.json` — 큐브 + `generate_otb_data.load_budget_2027` + `db_aggregated.by_property_segment`(2026 실적) + ga4_items로 후보·배치·목표·근거 산출. (3) 페이지 SEC10(필터·편집 가능한 비용칸·localStorage·엑셀) + SEC9 2027Y 열 채움 + SEC8 레벨에 '웹 구매건' 열 추가.

## 산출 로직 (스펙 §4.5 구체화)
- 후보 = 큐브 campaigns 중 기획연도 2025·2026, RN ≥ 100(세그 GS). 사업장별로 전개(캠페인 코드의 prop 기준).
- 지표(2026 또는 최근 연도 실적): rn, rev, adr, weekday_share(41+11), comp/bk, comm_rate(수수료/(rev+comp)), sale_window, stay_window, 채널믹스(codes의 seg·chan RN 비중)
- **ADR 가드레일**: 캠페인 ADR ≥ 같은 사업장·같은 연도 베이스라인(큐브 baseline, 시즌 가중) ADR × 0.95 → pass. 미달이면 'ADR 미달' 플래그(후보 제외 아님, 표기)
- **주중 기여** = rn × weekday_share, 주중 증분매출 = 주중 RN × adr
- **카니벌 체크**: 캠페인 투숙기간 동안 해당 사업장 주말(42/43/12/14) RN이 전년 동기간 대비 −10% 이상 감소면 '주말 잠식 의심' (baseline 월단위 근사: stay_window 월의 주말 RN YoY)
- **2027 배치**: sale/stay window를 +1년, 요일 정렬(같은 ISO 주·요일). 월 = stay_window 시작월
- **목표**: growth(prop, month) = budget2027[prop][month].rn ÷ actual2026[prop][month].rn (db_aggregated by_property_segment OTA+G-OTA+Inbound net_rn 합), clamp 0.8~1.3, 없으면 1.0. 목표 RN = 최근 실적 RN × growth, 목표 ADR = max(최근 ADR, 베이스라인 ADR), 목표 REV = RN×ADR, 수수료 = REV × comm_rate
- 비용 = 입력(페이지, localStorage key `gsn_plan27_cost:{row_id}`), 공헌 = REV − 수수료 − 비용(페이지 계산)
- 근거 텍스트 = "2026 N실·ADR·주중비중·웹 구매 M건" + 플래그
- 순위 = 주중 증분매출 내림차순. 추정값 열(목표)은 헤더에 '(목표·추정)' 표기, 실적 열과 색 구분

## Tasks
- [ ] T1 `fetch_ga4_items.py` 실행(2024-01-01~어제) → ga4_items.json, 월 합계가 GA4 summary와 ±1% 확인
- [ ] T2 `build_product_plan_2027.py` + `scripts/tests/test_product_plan_2027.py`(합성 큐브로 growth·가드레일·배치·순위 검증) → product_plan_2027.json
- [ ] T3 페이지: SEC10 표(필터: 사업장·월·세그, 비용 입력, 공헌 계산, 📥 엑셀), SEC9 2027Y 열 채움(사업장별 상위 N), SEC8 '웹 구매건' 열
- [ ] T4 검증(콘솔 0, 수치 표본, 엑셀 메모리 검증) → 커밋 → 배포(사용자 확인) → 메모리 갱신
