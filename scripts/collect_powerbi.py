#!/usr/bin/env python3
"""
Power BI 사업장별 실적·목표·달성률 자동 수집기
============================================================
Power BI 공개 임베드 (Resource Key 인증)에서 사업장별로
4/5/6월 RNS·ADR·REV·목표·달성률을 자동 추출

기존 collect_powerbi.py(Ha Hyeoncheol)를 기반으로 확장:
  - 사업장 grouping 추가
  - 목표(Budget) 데이터 별도 쿼리
  - 달성률 자동 계산
  - 3개월 (4/5/6월) 일괄 수집
  - daily_notes.json의 property_performance에 직접 반영

© 2026 GS팀 · Haein Kim Manager
"""
import json
import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Power BI 공개 보고서 설정
# ─────────────────────────────────────────────
RESOURCE_KEY = "ec0be295-8880-42dd-b2ac-3217e9c42b24"
TENANT_ID    = "2f8cc8a8-a9b0-4f8f-8f9f-fb7a7fd13ff4"
MODEL_ID     = 902554
DATASET_ID   = "8ee000d9-5efb-403f-83ad-9a8e3d3b80eb"
REPORT_ID    = "846569"
CLUSTER      = "https://wabi-korea-central-a-primary-redirect.analysis.windows.net"

# 조회할 3개월
STAY_MONTHS = ["202604", "202605", "202606"]

# 권역 매핑 (사업장명 키워드 → 권역)
REGION_MAP = {
    "비발디": "vivaldi",
    "소노펫": "vivaldi",
    "펠리체 빌리지": "vivaldi",
    "델피노": "central",
    "양평": "central",
    "양양": "central",
    "삼척": "central",
    "단양": "central",
    "청송": "central",
    "천안": "central",
    "변산": "central",
    "여수": "south",
    "거제": "south",
    "남해": "south",
    "진도": "south",
    "경주": "south",
    "해운대": "south",
    "제주": "apac",
    "고양": "apac",
    "하이퐁": "apac",
    "괌": "apac",
    "하와이": "apac",
}

_BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://app.powerbi.com",
    "Referer": "https://app.powerbi.com/",
    "X-PowerBI-ResourceKey": RESOURCE_KEY,
}


def _headers() -> dict:
    return {
        **_BASE_HEADERS,
        "ActivityId": str(uuid.uuid4()),
        "RequestId": str(uuid.uuid4()),
    }


# ─────────────────────────────────────────────
# 쿼리 생성
# ─────────────────────────────────────────────
def _build_property_actual_query(stay_month: str) -> dict:
    """사업장별 실적 (RNS, REV) - 채널 무관 합산"""
    year = int(stay_month[:4])
    month = int(stay_month[4:6])
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {"Commands": [{
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [
                            {"Name": "d", "Entity": "data_raw", "Type": 0},
                        ],
                        "Select": [
                            {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "영업장변경"}, "Name": "data_raw.영업장변경"},
                            {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "RNS"}}, "Function": 0}, "Name": "Sum(data_raw.RNS)"},
                            {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "REV"}}, "Function": 0}, "Name": "Sum(data_raw.REV)"},
                        ],
                        "Where": [
                            {"Condition": {"Comparison": {"ComparisonKind": 0, "Left": {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "월"}}, "Right": {"Literal": {"Value": f"{month}L"}}}}},
                            {"Condition": {"Comparison": {"ComparisonKind": 0, "Left": {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "투숙년도"}}, "Right": {"Literal": {"Value": f"{year}L"}}}}},
                        ],
                        "OrderBy": [{"Direction": 2, "Expression": {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "RNS"}}, "Function": 0}}}],
                    },
                    "Binding": {
                        "Primary": {"Groupings": [{"Projections": [0, 1, 2]}]},
                        "DataReduction": {"DataVolume": 4, "Primary": {"Window": {"Count": 500}}},
                        "Version": 1,
                    },
                }
            }]},
            "QueryId": "",
            "ApplicationContext": {"DatasetId": DATASET_ID, "Sources": [{"ReportId": REPORT_ID, "VisualId": ""}]},
        }],
        "cancelQueries": [],
        "modelId": MODEL_ID,
    }


def _build_property_budget_query(stay_month: str) -> dict:
    """사업장별 목표 (Budget RNS, Budget REV) - data_budget 또는 budget 테이블 시도"""
    year = int(stay_month[:4])
    month = int(stay_month[4:6])
    
    # 일반적인 Budget 테이블 후보들 - 첫 번째 테이블명으로 시도
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {"Commands": [{
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [
                            {"Name": "b", "Entity": "data_budget", "Type": 0},
                        ],
                        "Select": [
                            {"Column": {"Expression": {"SourceRef": {"Source": "b"}}, "Property": "영업장변경"}, "Name": "data_budget.영업장변경"},
                            {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Source": "b"}}, "Property": "목표RNS"}}, "Function": 0}, "Name": "Sum(data_budget.목표RNS)"},
                            {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Source": "b"}}, "Property": "목표REV"}}, "Function": 0}, "Name": "Sum(data_budget.목표REV)"},
                        ],
                        "Where": [
                            {"Condition": {"Comparison": {"ComparisonKind": 0, "Left": {"Column": {"Expression": {"SourceRef": {"Source": "b"}}, "Property": "월"}}, "Right": {"Literal": {"Value": f"{month}L"}}}}},
                            {"Condition": {"Comparison": {"ComparisonKind": 0, "Left": {"Column": {"Expression": {"SourceRef": {"Source": "b"}}, "Property": "년도"}}, "Right": {"Literal": {"Value": f"{year}L"}}}}},
                        ],
                    },
                    "Binding": {
                        "Primary": {"Groupings": [{"Projections": [0, 1, 2]}]},
                        "DataReduction": {"DataVolume": 4, "Primary": {"Window": {"Count": 500}}},
                        "Version": 1,
                    },
                }
            }]},
            "QueryId": "",
            "ApplicationContext": {"DatasetId": DATASET_ID, "Sources": [{"ReportId": REPORT_ID, "VisualId": ""}]},
        }],
        "cancelQueries": [],
        "modelId": MODEL_ID,
    }


# ─────────────────────────────────────────────
# 쿼리 실행 + 파싱
# ─────────────────────────────────────────────
def execute_query(body: dict, query_label: str = "") -> dict | None:
    """Power BI 쿼리 실행"""
    url = f"{CLUSTER}/public/reports/querydata?synchronous=true"
    try:
        r = requests.post(url, headers=_headers(), json=body, timeout=30)
        r.raise_for_status()
        result = r.json()
        # 에러 응답 체크
        if "error" in result:
            logger.warning(f"  ⚠ {query_label} 쿼리 에러: {result.get('error', {}).get('message', 'unknown')}")
            return None
        return result
    except requests.RequestException as e:
        logger.warning(f"  ⚠ {query_label} 호출 실패: {e}")
        return None


def parse_dsr(result: dict, expected_cols: int = 3) -> list[list]:
    """DSR (Data Shape Result) 파싱 - 가변 컬럼 수 지원"""
    if not result:
        return []
    try:
        data = result["results"][0]["result"]["data"]
        dsr = data["dsr"]["DS"][0]
        rows_raw = dsr["PH"][0]["DM0"]
        value_dicts = dsr.get("ValueDicts", {})
        col_defs = dsr.get("S", [])
    except (KeyError, IndexError) as e:
        logger.debug(f"DSR 파싱 실패: {e}")
        return []

    col_dicts = [c.get("DN") for c in col_defs]
    rows = []
    last = [None] * expected_cols

    for entry in rows_raw:
        c_vals = entry.get("C", [])
        r_mask = entry.get("R", 0)
        values = [None] * expected_cols
        c_idx = 0
        for col_idx in range(expected_cols):
            if r_mask & (1 << col_idx):
                values[col_idx] = last[col_idx]
            else:
                if c_idx < len(c_vals):
                    raw = c_vals[c_idx]
                    dict_name = col_dicts[col_idx] if col_idx < len(col_dicts) else None
                    if dict_name and isinstance(raw, int) and dict_name in value_dicts:
                        try:
                            values[col_idx] = value_dicts[dict_name][raw]
                        except (IndexError, KeyError):
                            values[col_idx] = raw
                    else:
                        values[col_idx] = raw
                    c_idx += 1
                last[col_idx] = values[col_idx]
        rows.append(values)
    return rows


# ─────────────────────────────────────────────
# 데이터 정규화
# ─────────────────────────────────────────────
def detect_region(property_name: str) -> str:
    for keyword, region in REGION_MAP.items():
        if keyword in property_name:
            return region
    return "unknown"


def normalize_property_name(raw: str) -> str:
    """사업장명 표준화 (BI 결과를 properties.json 기준에 맞춤)"""
    raw = raw.strip()
    
    # 매핑 규칙 (BI 명칭 → 표준 명칭)
    mapping = {
        "비발디파크": "비발디파크 (통합)",
        "소노벨비발디": "소노벨 비발디파크",
        "소노캄비발디": "소노캄 비발디파크",
        "소노펠리체비발디": "소노펠리체 비발디파크",
        "펠리체빌리지": "펠리체 빌리지 비발디파크",
        "소노펫비발디": "소노펫 비발디파크",
    }
    return mapping.get(raw, raw)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("Power BI 사업장별 실적·목표·달성률 자동 수집")
    logger.info("=" * 60)
    
    # 결과 저장 구조: { region: [ { name, 2026-04: {...}, 2026-05: {...}, 2026-06: {...} } ] }
    properties_by_region = {"vivaldi": {}, "central": {}, "south": {}, "apac": {}}
    
    for stay_month in STAY_MONTHS:
        month_label = f"{stay_month[:4]}-{stay_month[4:6]}"
        logger.info(f"\n[{month_label}] 데이터 수집 중...")
        
        # 1. 사업장별 실적 (RNS, REV)
        actual_result = execute_query(_build_property_actual_query(stay_month), f"{month_label} 실적")
        actual_rows = parse_dsr(actual_result, expected_cols=3) if actual_result else []
        
        actual_dict = {}  # { property_name: { rns, rev } }
        for row in actual_rows:
            if len(row) < 3 or not row[0]:
                continue
            name = normalize_property_name(str(row[0]))
            actual_dict[name] = {
                "rns": int(row[1] or 0),
                "rev_원": int(row[2] or 0),
            }
        logger.info(f"  ✓ 실적 수집: {len(actual_dict)}개 사업장")
        
        # 2. 사업장별 목표 (Budget)
        budget_result = execute_query(_build_property_budget_query(stay_month), f"{month_label} 목표")
        budget_rows = parse_dsr(budget_result, expected_cols=3) if budget_result else []
        
        budget_dict = {}
        for row in budget_rows:
            if len(row) < 3 or not row[0]:
                continue
            name = normalize_property_name(str(row[0]))
            budget_dict[name] = {
                "target_rns": int(row[1] or 0),
                "target_rev_원": int(row[2] or 0),
            }
        logger.info(f"  ✓ 목표 수집: {len(budget_dict)}개 사업장")
        
        # 3. 사업장별로 합쳐서 region에 저장
        all_names = set(actual_dict.keys()) | set(budget_dict.keys())
        for name in all_names:
            region = detect_region(name)
            if region == "unknown":
                logger.debug(f"  ? 권역 미매핑: {name}")
                continue
            
            actual = actual_dict.get(name, {})
            budget = budget_dict.get(name, {})
            
            rns = actual.get("rns", 0)
            rev_won = actual.get("rev_원", 0)
            target_rns = budget.get("target_rns", 0)
            
            # 달성률
            achievement = round((rns / target_rns) * 100, 1) if target_rns > 0 else 0
            # ADR (천원 단위) = REV / RNS
            adr_thousand = round((rev_won / 1000) / rns, 0) if rns > 0 else 0
            # REV (백만원 단위)
            rev_million = round(rev_won / 1_000_000, 0)
            
            month_data = {
                "rns": rns,
                "adr": int(adr_thousand),
                "rev": int(rev_million),
                "target_rns": target_rns,
                "achievement": achievement,
            }
            
            if name not in properties_by_region[region]:
                properties_by_region[region][name] = {"name": name}
            properties_by_region[region][name][month_label] = month_data
    
    # 4. dict → list 변환 + daily_notes.json 업데이트
    notes_path = Path(__file__).parent.parent / "data" / "daily_notes.json"
    if notes_path.exists():
        notes = json.loads(notes_path.read_text(encoding="utf-8"))
    else:
        notes = {}
    
    if "property_performance" not in notes:
        notes["property_performance"] = {}
    
    # 모든 권역에 데이터 있을 때만 업데이트 (한 곳이라도 실패면 전체 보존)
    total_updated = 0
    for region in ("vivaldi", "central", "south", "apac"):
        props_dict = properties_by_region[region]
        if props_dict:
            # 달성률 기준 정렬 (낮은 곳이 우선 모니터링 대상)
            props_list = sorted(
                props_dict.values(),
                key=lambda p: p.get("2026-04", {}).get("achievement", 999)
            )
            notes["property_performance"][region] = props_list
            total_updated += len(props_list)
            logger.info(f"  ✓ {region}: {len(props_list)}개 사업장 갱신")
        else:
            logger.warning(f"  ⚠ {region}: 데이터 없음 (기존 데이터 유지)")
    
    if total_updated == 0:
        logger.error("=" * 60)
        logger.error("⚠ Power BI 수집 완전 실패 - daily_notes.json 변경 안 함")
        logger.error("=" * 60)
        # powerbi_latest.json에 메타 기록
        meta = {
            "_collected_at": datetime.now().isoformat(),
            "_status": "failed",
            "_reason": "Power BI 쿼리 모두 실패 (스키마 변경 또는 인증 만료 가능성)",
            "_action_required": "BI 관리자에게 테이블/컬럼명 확인 필요",
        }
        meta_path = Path(__file__).parent.parent / "data" / "powerbi_latest.json"
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        sys.exit(1)
    
    # 메타 정보
    notes["_powerbi_last_sync"] = datetime.now().isoformat()
    notes_path.write_text(json.dumps(notes, ensure_ascii=False, indent=2), encoding="utf-8")
    
    # 별도 메타 파일
    meta_path = Path(__file__).parent.parent / "data" / "powerbi_latest.json"
    meta_path.write_text(json.dumps({
        "_collected_at": datetime.now().isoformat(),
        "_status": "success",
        "_total_properties": total_updated,
        "_months_collected": STAY_MONTHS,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    
    logger.info("=" * 60)
    logger.info(f"✓ Power BI 자동 수집 완료")
    logger.info(f"  사업장: {total_updated}개 × 3개월 = {total_updated*3}개 데이터 포인트")
    logger.info(f"  저장: {notes_path}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
