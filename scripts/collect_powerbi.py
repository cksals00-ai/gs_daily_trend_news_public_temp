#!/usr/bin/env python3
"""
Power BI 사업장별 실적·목표·달성률·YoY 자동 수집기
============================================================
BI 관리자 확인 스키마 반영 (2026-04-21):
  - data_raw          : 당해년도 실적 (영업장변경 컬럼)
  - data_lastraw      : 전년도 실적 (YoY 계산용)
  - budget_RNS_2026   : 목표 객실수 (영업장 컬럼)
  - budget_ADR_2026   : 목표 단가
  - budget_REV_2026   : 목표 매출
  - 사업장_static      : 영업장 ↔ 영업장변경 매핑 테이블
  
월/년도 필터: 월, 투숙년도

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
RESOURCE_KEY    = "ec0be295-8880-42dd-b2ac-3217e9c42b24"
TENANT_ID       = "2f8cc8a8-a9b0-4f8f-8f9f-fb7a7fd13ff4"
MODEL_ID        = 902554
DATASET_ID      = "8ee000d9-5efb-403f-83ad-9a8e3d3b80eb"
REPORT_ID       = "846569"
_CLUSTER_FALLBACK = "https://wabi-korea-central-a-primary-redirect.analysis.windows.net"

# 조회할 3개월
STAY_MONTHS = ["202604", "202605", "202606"]

# 권역 매핑 (사업장명 키워드 → 권역)
REGION_MAP = {
    # Vivaldi
    "비발디": "vivaldi",
    "소노펫": "vivaldi",
    "펠리체 빌리지": "vivaldi",
    # Central
    "델피노": "central",
    "양평": "central",
    "양양": "central",
    "삼척": "central",
    "단양": "central",
    "청송": "central",
    "천안": "central",
    "변산": "central",
    "오크밸리": "central",
    # South
    "여수": "south",
    "거제": "south",
    "남해": "south",
    "진도": "south",
    "경주": "south",
    "해운대": "south",
    # APAC
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


def _headers(content_type: bool = False) -> dict:
    h = {
        **_BASE_HEADERS,
        "ActivityId": str(uuid.uuid4()),
        "RequestId": str(uuid.uuid4()),
    }
    if content_type:
        h["Content-Type"] = "application/json"
    return h


def _apim_url(cluster_uri: str) -> str:
    """
    cluster URI → APIM URL 변환 (Power BI JS getAPIMUrl 로직 재현)
    예: https://wabi-korea-central-a-primary-redirect.analysis.windows.net
      → https://wabi-korea-central-a-primary-api.analysis.windows.net
    """
    hostname = cluster_uri.rstrip("/").split("//")[-1]
    parts = hostname.split(".")
    parts[0] = parts[0].replace("-redirect", "").replace("global-", "") + "-api"
    return "https://" + ".".join(parts)


def get_cluster_uri() -> str:
    """
    라우팅 API로 테넌트의 클러스터 URI를 확인.
    실패하면 하드코딩된 fallback 사용.
    """
    apim = _apim_url(_CLUSTER_FALLBACK)
    url = f"{apim}/public/routing/cluster/{TENANT_ID}"
    try:
        r = requests.get(url, headers=_headers(), timeout=15)
        if r.status_code == 200:
            cluster = r.json().get("FixedClusterUri", "").rstrip("/")
            if cluster:
                logger.info(f"클러스터 URI: {cluster}")
                return cluster
    except Exception as e:
        logger.warning(f"라우팅 API 실패: {e}")
    logger.info(f"하드코딩된 클러스터 URI 사용: {_CLUSTER_FALLBACK}")
    return _CLUSTER_FALLBACK


# ─────────────────────────────────────────────
# 쿼리 빌더
# ─────────────────────────────────────────────
def _build_actual_query(stay_month: str, table_name: str = "data_raw") -> dict:
    """
    실적 쿼리 - data_raw (당해년도) 또는 data_lastraw (전년도)
    영업장변경 컬럼으로 집계
    """
    year = int(stay_month[:4])
    month = int(stay_month[4:6])
    
    # data_lastraw는 투숙년도에서 1 빼서 전년도 조회
    if table_name == "data_lastraw":
        year = year - 1
    
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {"Commands": [{
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [
                            {"Name": "d", "Entity": table_name, "Type": 0},
                        ],
                        "Select": [
                            {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "영업장변경"}, "Name": f"{table_name}.영업장변경"},
                            {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "RNS"}}, "Function": 0}, "Name": f"Sum({table_name}.RNS)"},
                            {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "REV"}}, "Function": 0}, "Name": f"Sum({table_name}.REV)"},
                        ],
                        "Where": [
                            {"Condition": {"Comparison": {"ComparisonKind": 0, "Left": {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "월"}}, "Right": {"Literal": {"Value": f"{month}L"}}}}},
                            {"Condition": {"Comparison": {"ComparisonKind": 0, "Left": {"Column": {"Expression": {"SourceRef": {"Source": "d"}}, "Property": "투숙년도"}}, "Right": {"Literal": {"Value": f"{year}L"}}}}},
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


def _build_budget_query(stay_month: str, budget_table: str, metric_column: str) -> dict:
    """
    목표 쿼리 - budget_RNS_2026, budget_ADR_2026, budget_REV_2026
    '영업장' 컬럼으로 집계
    """
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
                            {"Name": "b", "Entity": budget_table, "Type": 0},
                        ],
                        "Select": [
                            {"Column": {"Expression": {"SourceRef": {"Source": "b"}}, "Property": "영업장"}, "Name": f"{budget_table}.영업장"},
                            {"Aggregation": {"Expression": {"Column": {"Expression": {"SourceRef": {"Source": "b"}}, "Property": metric_column}}, "Function": 0}, "Name": f"Sum({budget_table}.{metric_column})"},
                        ],
                        "Where": [
                            {"Condition": {"Comparison": {"ComparisonKind": 0, "Left": {"Column": {"Expression": {"SourceRef": {"Source": "b"}}, "Property": "월"}}, "Right": {"Literal": {"Value": f"{month}L"}}}}},
                            {"Condition": {"Comparison": {"ComparisonKind": 0, "Left": {"Column": {"Expression": {"SourceRef": {"Source": "b"}}, "Property": "투숙년도"}}, "Right": {"Literal": {"Value": f"{year}L"}}}}},
                        ],
                    },
                    "Binding": {
                        "Primary": {"Groupings": [{"Projections": [0, 1]}]},
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


def _build_static_mapping_query() -> dict:
    """
    사업장_static 매핑 테이블 - 영업장 ↔ 영업장변경 매핑
    """
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {"Commands": [{
                "SemanticQueryDataShapeCommand": {
                    "Query": {
                        "Version": 2,
                        "From": [
                            {"Name": "s", "Entity": "사업장_static", "Type": 0},
                        ],
                        "Select": [
                            {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "영업장"}, "Name": "사업장_static.영업장"},
                            {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "영업장변경"}, "Name": "사업장_static.영업장변경"},
                        ],
                    },
                    "Binding": {
                        "Primary": {"Groupings": [{"Projections": [0, 1]}]},
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
    cluster_uri = get_cluster_uri()
    apim = _apim_url(cluster_uri)
    url = f"{apim}/public/reports/querydata?synchronous=true"
    try:
        r = requests.post(url, headers=_headers(content_type=True), json=body, timeout=30)
        r.raise_for_status()
        result = r.json()
        if "error" in result:
            logger.warning(f"  ⚠ {query_label}: {result.get('error', {}).get('message', 'unknown error')}")
            return None
        if result.get("results") and result["results"][0].get("result", {}).get("error"):
            err = result["results"][0]["result"]["error"]
            logger.warning(f"  ⚠ {query_label} 쿼리 오류: {err}")
            return None
        return result
    except requests.RequestException as e:
        logger.warning(f"  ⚠ {query_label} 호출 실패: {e}")
        return None


def parse_dsr(result: dict, expected_cols: int = 3) -> list[list]:
    """DSR (Data Shape Result) 파싱"""
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
# 데이터 헬퍼
# ─────────────────────────────────────────────
def detect_region(property_name: str) -> str:
    for keyword, region in REGION_MAP.items():
        if keyword in property_name:
            return region
    return "unknown"


def calculate_adr(rns: int, rev_won: int) -> int:
    """ADR = REV / RNS (천원 단위)"""
    if rns <= 0:
        return 0
    return round((rev_won / 1000) / rns)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("Power BI 사업장별 실적·목표·YoY·달성률 자동 수집")
    logger.info("=" * 60)
    
    # Step 0: 사업장 매핑 테이블 먼저 로드 (영업장 ↔ 영업장변경)
    logger.info("\n[Step 0] 사업장_static 매핑 로드")
    static_result = execute_query(_build_static_mapping_query(), "사업장_static")
    static_rows = parse_dsr(static_result, expected_cols=2) if static_result else []
    
    # 매핑: {영업장 → 영업장변경, 영업장변경 → 영업장}
    budget_to_actual = {}  # 목표 측 이름 → 실적 측 이름 (표시명)
    actual_to_budget = {}
    for row in static_rows:
        if len(row) < 2 or not row[0] or not row[1]:
            continue
        budget_name = str(row[0]).strip()   # 영업장 (목표 테이블)
        actual_name = str(row[1]).strip()   # 영업장변경 (실적 테이블, 표시명)
        budget_to_actual[budget_name] = actual_name
        actual_to_budget[actual_name] = budget_name
    logger.info(f"  ✓ 매핑 로드: {len(budget_to_actual)}개 사업장")
    
    if not budget_to_actual:
        logger.warning("  ⚠ 매핑 테이블 로드 실패. 이름이 같다고 가정하고 진행")
    
    # 결과 저장 구조
    # { property_name: { '2026-04': {...}, '2026-05': {...}, '2026-06': {...} } }
    all_properties = {}
    
    for stay_month in STAY_MONTHS:
        month_label = f"{stay_month[:4]}-{stay_month[4:6]}"
        logger.info(f"\n[{month_label}] 수집 시작")
        
        # 1. 실적 (RNS, REV) - data_raw
        logger.info(f"  [1/4] data_raw (실적)")
        actual_result = execute_query(_build_actual_query(stay_month, "data_raw"), f"{month_label} 실적")
        actual_rows = parse_dsr(actual_result, expected_cols=3) if actual_result else []
        
        actual_dict = {}  # 표시명 → {rns, rev_won}
        for row in actual_rows:
            if len(row) < 3 or not row[0]:
                continue
            name = str(row[0]).strip()
            actual_dict[name] = {
                "rns": round(float(row[1] or 0)),
                "rev_won": round(float(row[2] or 0)),
            }
        logger.info(f"      ✓ 실적: {len(actual_dict)}개 사업장")
        
        # 2. 전년도 실적 (YoY 계산용) - data_lastraw
        logger.info(f"  [2/4] data_lastraw (전년도 실적)")
        last_result = execute_query(_build_actual_query(stay_month, "data_lastraw"), f"{month_label} 전년실적")
        last_rows = parse_dsr(last_result, expected_cols=3) if last_result else []
        
        last_dict = {}
        for row in last_rows:
            if len(row) < 3 or not row[0]:
                continue
            name = str(row[0]).strip()
            last_dict[name] = {
                "rns": round(float(row[1] or 0)),
                "rev_won": round(float(row[2] or 0)),
            }
        logger.info(f"      ✓ 전년도: {len(last_dict)}개 사업장")
        
        # 3. 목표 RNS - budget_RNS_2026
        logger.info(f"  [3/4] budget_RNS_2026 (목표 객실수)")
        budget_rns_result = execute_query(
            _build_budget_query(stay_month, "budget_RNS_2026", "budget_RNS"),
            f"{month_label} 목표RNS"
        )
        budget_rns_rows = parse_dsr(budget_rns_result, expected_cols=2) if budget_rns_result else []
        
        budget_rns_dict = {}  # 영업장 → 목표RNS
        for row in budget_rns_rows:
            if len(row) < 2 or not row[0]:
                continue
            budget_name = str(row[0]).strip()
            # 매핑 적용: 목표측 '영업장' → 실적측 '영업장변경'
            display_name = budget_to_actual.get(budget_name, budget_name)
            budget_rns_dict[display_name] = round(float(row[1] or 0))
        logger.info(f"      ✓ 목표RNS: {len(budget_rns_dict)}개 사업장")
        
        # 4. 목표 REV - budget_REV_2026
        logger.info(f"  [4/4] budget_REV_2026 (목표 매출)")
        budget_rev_result = execute_query(
            _build_budget_query(stay_month, "budget_REV_2026", "budget_REV"),
            f"{month_label} 목표REV"
        )
        budget_rev_rows = parse_dsr(budget_rev_result, expected_cols=2) if budget_rev_result else []
        
        budget_rev_dict = {}
        for row in budget_rev_rows:
            if len(row) < 2 or not row[0]:
                continue
            budget_name = str(row[0]).strip()
            display_name = budget_to_actual.get(budget_name, budget_name)
            budget_rev_dict[display_name] = round(float(row[1] or 0))
        logger.info(f"      ✓ 목표REV: {len(budget_rev_dict)}개 사업장")
        
        # 5. 데이터 병합
        all_names = (
            set(actual_dict.keys()) | 
            set(last_dict.keys()) | 
            set(budget_rns_dict.keys()) | 
            set(budget_rev_dict.keys())
        )
        
        for name in all_names:
            actual = actual_dict.get(name, {})
            last = last_dict.get(name, {})
            
            rns = actual.get("rns", 0)
            rev_won = actual.get("rev_won", 0)
            last_rns = last.get("rns", 0)
            
            target_rns = budget_rns_dict.get(name, 0)
            target_rev_won = budget_rev_dict.get(name, 0)
            
            # 계산
            achievement = round((rns / target_rns) * 100, 1) if target_rns > 0 else 0
            yoy_pct = round(((rns - last_rns) / last_rns) * 100, 1) if last_rns > 0 else 0
            adr = calculate_adr(rns, rev_won)
            rev_million = round(rev_won / 1_000_000)
            
            if name not in all_properties:
                all_properties[name] = {"name": name}
            
            all_properties[name][month_label] = {
                "rns": rns,
                "adr": adr,
                "rev": rev_million,
                "target_rns": target_rns,
                "target_rev": round(target_rev_won / 1_000_000),
                "achievement": achievement,
                "yoy_pct": yoy_pct,
                "last_rns": last_rns,
            }
    
    # ─────────────────────────────────────────
    # daily_notes.json 업데이트
    # ─────────────────────────────────────────
    notes_path = Path(__file__).parent.parent / "data" / "daily_notes.json"
    notes = json.loads(notes_path.read_text(encoding="utf-8")) if notes_path.exists() else {}
    
    # 권역별 분류
    properties_by_region = {"vivaldi": [], "central": [], "south": [], "apac": [], "unknown": []}
    for name, data in all_properties.items():
        region = detect_region(name)
        properties_by_region.setdefault(region, []).append(data)
    
    # 권역별로 4월 달성률 오름차순 정렬 (하위 사업장이 위)
    for region in properties_by_region:
        properties_by_region[region].sort(
            key=lambda p: p.get("2026-04", {}).get("achievement", 999)
        )
    
    # 성공 여부 판정 (최소 하나의 권역에 데이터가 있어야)
    total_props = sum(len(v) for k, v in properties_by_region.items() if k != "unknown")
    
    if total_props == 0:
        logger.error("=" * 60)
        logger.error("⚠ Power BI 수집 완전 실패")
        logger.error("=" * 60)
        meta = {
            "_collected_at": datetime.now().isoformat(),
            "_status": "failed",
            "_reason": "모든 쿼리 실패 - BI 관리자 확인 필요",
        }
        (Path(__file__).parent.parent / "data" / "powerbi_latest.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        sys.exit(1)
    
    # daily_notes.json에 반영 (unknown 권역은 제외)
    notes["property_performance"] = {
        "_description": f"Power BI 자동 수집 ({datetime.now().strftime('%Y-%m-%d %H:%M')})",
        "_status": "auto_synced",
        "_last_sync": datetime.now().isoformat(),
        "vivaldi": properties_by_region["vivaldi"],
        "central": properties_by_region["central"],
        "south": properties_by_region["south"],
        "apac": properties_by_region["apac"],
    }
    
    # Hero OTA 채널 데이터도 자동 수집 가능 (추후)
    # 지금은 property_performance만
    
    notes_path.write_text(json.dumps(notes, ensure_ascii=False, indent=2), encoding="utf-8")
    
    # 별도 메타 저장
    meta_path = Path(__file__).parent.parent / "data" / "powerbi_latest.json"
    meta_path.write_text(json.dumps({
        "_collected_at": datetime.now().isoformat(),
        "_status": "success",
        "_total_properties": total_props,
        "_months_collected": STAY_MONTHS,
        "_by_region": {k: len(v) for k, v in properties_by_region.items() if k != "unknown"},
        "_unknown_properties": [p["name"] for p in properties_by_region.get("unknown", [])],
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    
    logger.info("=" * 60)
    logger.info(f"✓ Power BI 자동 수집 완료")
    logger.info(f"  총 사업장: {total_props}개")
    for region in ("vivaldi", "central", "south", "apac"):
        props = properties_by_region[region]
        logger.info(f"    {region}: {len(props)}개")
    if properties_by_region.get("unknown"):
        logger.warning(f"    ⚠ 권역 미매핑 {len(properties_by_region['unknown'])}개: {[p['name'] for p in properties_by_region['unknown']]}")
    logger.info(f"  저장: {notes_path}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
