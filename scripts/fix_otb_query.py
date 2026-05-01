#!/usr/bin/env python3
"""
OTB 데이터 수정 스크립트
- data_raw: 예약일자 = 2026-04-21, 월 = 4/5/6, 투숙년도 = 2026 → SUM(RNS)
- data_cxl_2mthraw: 취소일자 = 2026-04-21, 월 = 4/5/6 → SUM(RNS)
- weekly_report.json 업데이트

사용법: python scripts/fix_otb_query.py
"""
import json
import logging
import uuid
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Power BI 설정 (collect_powerbi.py와 동일)
# ─────────────────────────────────────────────
RESOURCE_KEY = "ec0be295-8880-42dd-b2ac-3217e9c42b24"
TENANT_ID    = "2f8cc8a8-a9b0-4f8f-8f9f-fb7a7fd13ff4"
MODEL_ID     = 902554
DATASET_ID   = "8ee000d9-5efb-403f-83ad-9a8e3d3b80eb"
REPORT_ID    = "846569"
_CLUSTER_FALLBACK = "https://wabi-korea-central-a-primary-redirect.analysis.windows.net"

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
    hostname = cluster_uri.rstrip("/").split("//")[-1]
    parts = hostname.split(".")
    parts[0] = parts[0].replace("-redirect", "").replace("global-", "") + "-api"
    return "https://" + ".".join(parts)


def get_cluster_uri() -> str:
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
    logger.info(f"하드코딩된 fallback 사용: {_CLUSTER_FALLBACK}")
    return _CLUSTER_FALLBACK


def execute_query(body: dict, label: str = "") -> dict | None:
    cluster_uri = get_cluster_uri()
    apim = _apim_url(cluster_uri)
    url = f"{apim}/public/reports/querydata?synchronous=true"
    try:
        r = requests.post(url, headers=_headers(content_type=True), json=body, timeout=30)
        r.raise_for_status()
        result = r.json()
        if "error" in result:
            logger.warning(f"  ⚠ {label}: {result.get('error', {}).get('message', 'unknown error')}")
            return None
        if result.get("results") and result["results"][0].get("result", {}).get("error"):
            err = result["results"][0]["result"]["error"]
            logger.warning(f"  ⚠ {label} 쿼리 오류: {err}")
            return None
        return result
    except requests.RequestException as e:
        logger.warning(f"  ⚠ {label} 호출 실패: {e}")
        return None


def parse_dsr_simple(result: dict, expected_cols: int) -> list[list]:
    """DSR 결과 파싱 (집계 결과용)"""
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
        # 디버그: 전체 구조 출력
        try:
            logger.debug(f"결과 구조: {json.dumps(result, ensure_ascii=False)[:2000]}")
        except Exception:
            pass
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
# 쿼리 빌더
# ─────────────────────────────────────────────

def _make_app_ctx():
    return {
        "DatasetId": DATASET_ID,
        "Sources": [{"ReportId": REPORT_ID, "VisualId": ""}],
    }


def _wrap_query(query_obj: dict) -> dict:
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {"Commands": [{"SemanticQueryDataShapeCommand": query_obj}]},
            "QueryId": "",
            "ApplicationContext": _make_app_ctx(),
        }],
        "cancelQueries": [],
        "modelId": MODEL_ID,
    }


def _src(alias: str):
    return {"SourceRef": {"Source": alias}}


def _col(alias: str, prop: str):
    return {"Column": {"Expression": _src(alias), "Property": prop}}


def _agg(alias: str, prop: str, func: int = 0):
    """Aggregation: func=0 → SUM"""
    return {"Aggregation": {"Expression": _col(alias, prop), "Function": func}}


def _eq_int(alias: str, prop: str, value: int):
    return {"Condition": {"Comparison": {
        "ComparisonKind": 0,
        "Left": _col(alias, prop),
        "Right": {"Literal": {"Value": f"{value}L"}},
    }}}


def _gte_datetime(alias: str, prop: str, dt_str: str):
    """>= datetime literal"""
    return {"Condition": {"Comparison": {
        "ComparisonKind": 2,
        "Left": _col(alias, prop),
        "Right": {"Literal": {"Value": f"datetime'{dt_str}'"}},
    }}}


def _lt_datetime(alias: str, prop: str, dt_str: str):
    """< datetime literal"""
    return {"Condition": {"Comparison": {
        "ComparisonKind": 3,
        "Left": _col(alias, prop),
        "Right": {"Literal": {"Value": f"datetime'{dt_str}'"}},
    }}}


def _in_ints(alias: str, prop: str, values: list[int]):
    """IN filter for integer values"""
    return {"Condition": {"In": {
        "Expressions": [_col(alias, prop)],
        "Values": [[{"Literal": {"Value": f"{v}L"}}] for v in values],
    }}}


def build_explore_cxl_table() -> dict:
    """data_cxl_2mthraw 테이블 컬럼 탐색 - 상위 10행 조회 (월 컬럼 존재 가정)"""
    q = {
        "Query": {
            "Version": 2,
            "From": [{"Name": "c", "Entity": "data_cxl_2mthraw", "Type": 0}],
            "Select": [
                {"Column": _col("c", "취소일자")["Column"], "Name": "취소일자"},
                {"Column": _col("c", "월")["Column"], "Name": "월"},
                {"Column": _col("c", "RNS")["Column"], "Name": "RNS"},
            ],
        },
        "Binding": {
            "Primary": {"Groupings": [{"Projections": [0, 1, 2]}]},
            "DataReduction": {"DataVolume": 4, "Primary": {"Window": {"Count": 10}}},
            "Version": 1,
        },
    }
    return _wrap_query(q)


def build_explore_cxl_table_v2() -> dict:
    """data_cxl_2mthraw - 투숙년도 컬럼도 시도"""
    q = {
        "Query": {
            "Version": 2,
            "From": [{"Name": "c", "Entity": "data_cxl_2mthraw", "Type": 0}],
            "Select": [
                {"Column": _col("c", "취소일자")["Column"], "Name": "취소일자"},
                {"Column": _col("c", "월")["Column"], "Name": "월"},
                {"Column": _col("c", "투숙년도")["Column"], "Name": "투숙년도"},
                {"Column": _col("c", "RNS")["Column"], "Name": "RNS"},
            ],
        },
        "Binding": {
            "Primary": {"Groupings": [{"Projections": [0, 1, 2, 3]}]},
            "DataReduction": {"DataVolume": 4, "Primary": {"Window": {"Count": 10}}},
            "Version": 1,
        },
    }
    return _wrap_query(q)


def build_booking_query_utc(date_start: str, date_end: str, months: list[int]) -> dict:
    """
    data_raw: 예약일자 범위 [date_start, date_end) UTC, 월 IN months, 투숙년도=2026
    → 월별 SUM(RNS)
    date_start/date_end: 'YYYY-MM-DDTHH:MM:SS'
    """
    q = {
        "Query": {
            "Version": 2,
            "From": [{"Name": "d", "Entity": "data_raw", "Type": 0}],
            "Select": [
                {"Column": _col("d", "월")["Column"], "Name": "월"},
                {"Aggregation": {
                    "Expression": _col("d", "RNS"),
                    "Function": 0,
                }, "Name": "SUM_RNS"},
            ],
            "Where": [
                _gte_datetime("d", "예약일자", date_start),
                _lt_datetime("d", "예약일자", date_end),
                _in_ints("d", "월", months),
                _eq_int("d", "투숙년도", 2026),
            ],
        },
        "Binding": {
            "Primary": {"Groupings": [{"Projections": [0, 1]}]},
            "DataReduction": {"DataVolume": 4, "Primary": {"Window": {"Count": 500}}},
            "Version": 1,
        },
    }
    return _wrap_query(q)


def build_booking_query_date_only(date_str: str, months: list[int]) -> dict:
    """
    data_raw: 예약일자 = date (date literal), 월 IN months, 투숙년도=2026
    → 월별 SUM(RNS)
    """
    q = {
        "Query": {
            "Version": 2,
            "From": [{"Name": "d", "Entity": "data_raw", "Type": 0}],
            "Select": [
                {"Column": _col("d", "월")["Column"], "Name": "월"},
                {"Aggregation": {
                    "Expression": _col("d", "RNS"),
                    "Function": 0,
                }, "Name": "SUM_RNS"},
            ],
            "Where": [
                {"Condition": {"Comparison": {
                    "ComparisonKind": 0,
                    "Left": _col("d", "예약일자"),
                    "Right": {"Literal": {"Value": f"date'{date_str}'"}},
                }}},
                _in_ints("d", "월", months),
                _eq_int("d", "투숙년도", 2026),
            ],
        },
        "Binding": {
            "Primary": {"Groupings": [{"Projections": [0, 1]}]},
            "DataReduction": {"DataVolume": 4, "Primary": {"Window": {"Count": 500}}},
            "Version": 1,
        },
    }
    return _wrap_query(q)


def build_cancel_query_date_range(date_start: str, date_end: str, months: list[int]) -> dict:
    """
    data_cxl_2mthraw: 취소일자 범위 [date_start, date_end), 월 IN months
    → 월별 SUM(RNS)
    """
    q = {
        "Query": {
            "Version": 2,
            "From": [{"Name": "c", "Entity": "data_cxl_2mthraw", "Type": 0}],
            "Select": [
                {"Column": _col("c", "월")["Column"], "Name": "월"},
                {"Aggregation": {
                    "Expression": _col("c", "RNS"),
                    "Function": 0,
                }, "Name": "SUM_RNS"},
            ],
            "Where": [
                _gte_datetime("c", "취소일자", date_start),
                _lt_datetime("c", "취소일자", date_end),
                _in_ints("c", "월", months),
            ],
        },
        "Binding": {
            "Primary": {"Groupings": [{"Projections": [0, 1]}]},
            "DataReduction": {"DataVolume": 4, "Primary": {"Window": {"Count": 500}}},
            "Version": 1,
        },
    }
    return _wrap_query(q)


def build_cancel_query_date_only(date_str: str, months: list[int]) -> dict:
    """
    data_cxl_2mthraw: 취소일자 = date literal, 월 IN months
    → 월별 SUM(RNS)
    """
    q = {
        "Query": {
            "Version": 2,
            "From": [{"Name": "c", "Entity": "data_cxl_2mthraw", "Type": 0}],
            "Select": [
                {"Column": _col("c", "월")["Column"], "Name": "월"},
                {"Aggregation": {
                    "Expression": _col("c", "RNS"),
                    "Function": 0,
                }, "Name": "SUM_RNS"},
            ],
            "Where": [
                {"Condition": {"Comparison": {
                    "ComparisonKind": 0,
                    "Left": _col("c", "취소일자"),
                    "Right": {"Literal": {"Value": f"date'{date_str}'"}},
                }}},
                _in_ints("c", "월", months),
            ],
        },
        "Binding": {
            "Primary": {"Groupings": [{"Projections": [0, 1]}]},
            "DataReduction": {"DataVolume": 4, "Primary": {"Window": {"Count": 500}}},
            "Version": 1,
        },
    }
    return _wrap_query(q)


def parse_month_rns(result: dict) -> dict[int, int]:
    """결과 파싱 → {월: RNS합계} dict"""
    rows = parse_dsr_simple(result, expected_cols=2)
    out = {}
    for row in rows:
        if len(row) < 2:
            continue
        try:
            month = int(row[0])
            rns = round(float(row[1] or 0))
            out[month] = rns
        except (TypeError, ValueError):
            continue
    return out


def log_raw_result(result: dict, label: str):
    """디버깅용: 결과 원본 출력"""
    if not result:
        logger.info(f"  [{label}] 결과 없음")
        return
    try:
        snippet = json.dumps(result, ensure_ascii=False)[:3000]
        logger.info(f"  [{label}] 결과 스니펫: {snippet}")
    except Exception:
        pass


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("OTB 데이터 수집 - 예약/취소 일자별 집계")
    logger.info("기준일: 2026-04-21 (어제)")
    logger.info("=" * 60)

    # 매월 2일부터 다음 3개월로 자동 롤링
    from datetime import datetime, timezone, timedelta
    _kst = timezone(timedelta(hours=9))
    _now = datetime.now(_kst)
    _base = _now.month if _now.day >= 2 else (_now.month - 1 if _now.month > 1 else 12)
    MONTHS = [(_base + i - 1) % 12 + 1 for i in range(3)]

    # ── Step 1: data_cxl_2mthraw 테이블 탐색 ──
    logger.info("\n[Step 1] data_cxl_2mthraw 컬럼 탐색")
    explore_result = execute_query(build_explore_cxl_table(), "cxl 탐색 v1")
    if explore_result:
        log_raw_result(explore_result, "cxl 탐색 v1")
    else:
        logger.info("  → v1 실패. v2 시도 (투숙년도 포함)")
        explore_result = execute_query(build_explore_cxl_table_v2(), "cxl 탐색 v2")
        if explore_result:
            log_raw_result(explore_result, "cxl 탐색 v2")
        else:
            logger.warning("  → cxl 테이블 탐색 실패. 쿼리 계속 진행")

    # ── Step 2: 예약(BOOKING) - data_raw ──
    logger.info("\n[Step 2] 예약 수집 - data_raw (예약일자 = 2026-04-21)")

    booking_by_month = {}

    # 전략 A: UTC 전체 날짜 범위 (00:00 ~ 24:00)
    logger.info("  전략 A: UTC 범위 2026-04-21T00:00:00 ~ 2026-04-22T00:00:00")
    res_a = execute_query(
        build_booking_query_utc("2026-04-21T00:00:00", "2026-04-22T00:00:00", MONTHS),
        "예약 UTC-A"
    )
    parsed_a = parse_month_rns(res_a) if res_a else {}
    total_a = sum(parsed_a.values())
    logger.info(f"    결과: {parsed_a}  합계={total_a}")

    # 전략 B: KST 기준 (UTC+9 → UTC로 변환: 04-20 15:00 ~ 04-21 15:00)
    logger.info("  전략 B: KST 기준 UTC 범위 2026-04-20T15:00:00 ~ 2026-04-21T15:00:00")
    res_b = execute_query(
        build_booking_query_utc("2026-04-20T15:00:00", "2026-04-21T15:00:00", MONTHS),
        "예약 UTC-B(KST)"
    )
    parsed_b = parse_month_rns(res_b) if res_b else {}
    total_b = sum(parsed_b.values())
    logger.info(f"    결과: {parsed_b}  합계={total_b}")

    # 전략 C: date literal (시간 무시)
    logger.info("  전략 C: date literal '2026-04-21'")
    res_c = execute_query(
        build_booking_query_date_only("2026-04-21", MONTHS),
        "예약 date-only"
    )
    parsed_c = parse_month_rns(res_c) if res_c else {}
    total_c = sum(parsed_c.values())
    logger.info(f"    결과: {parsed_c}  합계={total_c}")

    # 가장 많은 결과를 선택 (더 넓은 범위 = 더 정확)
    best_total = max(total_a, total_b, total_c)
    if total_a == best_total and best_total > 0:
        booking_by_month = parsed_a
        logger.info(f"  ✓ 전략 A 선택 (합계={total_a})")
    elif total_b == best_total and best_total > 0:
        booking_by_month = parsed_b
        logger.info(f"  ✓ 전략 B 선택 (합계={total_b})")
    elif total_c == best_total and best_total > 0:
        booking_by_month = parsed_c
        logger.info(f"  ✓ 전략 C 선택 (합계={total_c})")
    else:
        logger.warning("  ⚠ 모든 예약 전략이 0 결과. 원본 디버그 출력:")
        for res, label in [(res_a, "A"), (res_b, "B"), (res_c, "C")]:
            log_raw_result(res, f"예약 전략-{label}")

    logger.info(f"  최종 예약 by month: {booking_by_month}")

    # ── Step 3: 취소(CANCELLATION) - data_cxl_2mthraw ──
    logger.info("\n[Step 3] 취소 수집 - data_cxl_2mthraw (취소일자 = 2026-04-21)")

    cancel_by_month = {}

    # 전략 A: UTC 범위
    logger.info("  전략 A: UTC 범위 2026-04-21T00:00:00 ~ 2026-04-22T00:00:00")
    cxl_a = execute_query(
        build_cancel_query_date_range("2026-04-21T00:00:00", "2026-04-22T00:00:00", MONTHS),
        "취소 UTC-A"
    )
    cxl_parsed_a = parse_month_rns(cxl_a) if cxl_a else {}
    cxl_total_a = sum(cxl_parsed_a.values())
    logger.info(f"    결과: {cxl_parsed_a}  합계={cxl_total_a}")

    # 전략 B: KST 기준 UTC
    logger.info("  전략 B: KST 기준 UTC 범위 2026-04-20T15:00:00 ~ 2026-04-21T15:00:00")
    cxl_b = execute_query(
        build_cancel_query_date_range("2026-04-20T15:00:00", "2026-04-21T15:00:00", MONTHS),
        "취소 UTC-B(KST)"
    )
    cxl_parsed_b = parse_month_rns(cxl_b) if cxl_b else {}
    cxl_total_b = sum(cxl_parsed_b.values())
    logger.info(f"    결과: {cxl_parsed_b}  합계={cxl_total_b}")

    # 전략 C: date literal
    logger.info("  전략 C: date literal '2026-04-21'")
    cxl_c = execute_query(
        build_cancel_query_date_only("2026-04-21", MONTHS),
        "취소 date-only"
    )
    cxl_parsed_c = parse_month_rns(cxl_c) if cxl_c else {}
    cxl_total_c = sum(cxl_parsed_c.values())
    logger.info(f"    결과: {cxl_parsed_c}  합계={cxl_total_c}")

    best_cxl = max(cxl_total_a, cxl_total_b, cxl_total_c)
    if cxl_total_a == best_cxl and best_cxl >= 0:
        cancel_by_month = cxl_parsed_a
        logger.info(f"  ✓ 전략 A 선택 (합계={cxl_total_a})")
    elif cxl_total_b == best_cxl:
        cancel_by_month = cxl_parsed_b
        logger.info(f"  ✓ 전략 B 선택 (합계={cxl_total_b})")
    elif cxl_total_c == best_cxl:
        cancel_by_month = cxl_parsed_c
        logger.info(f"  ✓ 전략 C 선택 (합계={cxl_total_c})")
    else:
        logger.warning("  ⚠ 모든 취소 전략이 0 결과. 원본 디버그 출력:")
        for res, label in [(cxl_a, "A"), (cxl_b, "B"), (cxl_c, "C")]:
            log_raw_result(res, f"취소 전략-{label}")

    logger.info(f"  최종 취소 by month: {cancel_by_month}")

    # ── Step 4: 월별 net OTB 계산 ──
    logger.info("\n[Step 4] net OTB 계산")
    month_labels = {4: "APR", 5: "MAY", 6: "JUN"}
    stay_months  = {4: "2026-04", 5: "2026-05", 6: "2026-06"}

    results = {}
    for m in MONTHS:
        bk  = booking_by_month.get(m, 0)
        cxl = cancel_by_month.get(m, 0)
        net = bk - cxl
        results[m] = {"booking_rns": bk, "cancel_rns": cxl, "net_otb": net}
        logger.info(f"  {month_labels[m]} (월={m}): 예약={bk}, 취소={cxl}, net={net}")

    # ── Step 5: weekly_report.json 업데이트 ──
    logger.info("\n[Step 5] weekly_report.json 업데이트")
    json_path = Path(__file__).parent.parent / "data" / "weekly_report.json"
    report = json.loads(json_path.read_text(encoding="utf-8"))

    months_list = report.get("daily_otb", {}).get("months", [])
    updated = []
    for entry in months_list:
        label = entry.get("label", "")
        if label == "APR":
            m = 4
        elif label == "MAY":
            m = 5
        elif label == "JUN":
            m = 6
        else:
            updated.append(entry)
            continue

        r = results.get(m, {})
        bk  = r.get("booking_rns", 0)
        cxl = r.get("cancel_rns", 0)
        net = r.get("net_otb", 0)

        # 0이면 null 유지 (데이터 없음으로 간주)
        if bk == 0 and cxl == 0:
            logger.warning(f"  ⚠ {label}: 예약/취소 모두 0 → null 유지")
            updated.append(entry)
        else:
            entry["booking_rns"] = bk
            entry["cancel_rns"]  = cxl
            entry["net_otb"]     = net
            logger.info(f"  ✓ {label}: 예약={bk}, 취소={cxl}, net={net}")
            updated.append(entry)

    report["daily_otb"]["months"] = updated
    report["_updated_at"] = "2026-04-22"

    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"  저장 완료: {json_path}")

    logger.info("=" * 60)
    logger.info("✓ OTB 데이터 수집 완료")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
