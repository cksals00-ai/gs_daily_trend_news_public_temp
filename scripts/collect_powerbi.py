#!/usr/bin/env python3
"""
GS팀 데일리 트렌드 리포트 V5 - Power BI 데이터 수집기
======================================================
Based on powerbi_collector.py (Ha Hyeoncheol, GS팀)

© 2026 GS팀 · Haein Kim Manager

실행: python scripts/collect_powerbi.py
출력: data/powerbi_latest.json
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


def _build_ty_query(stay_month: str) -> dict:
    """당월 채널별 RNS/REV 쿼리 (data_raw)"""
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
                            {"Name": "d2", "Entity": "DimAgent", "Type": 0},
                        ],
                        "Select": [
                            {"Column": {"Expression": {"SourceRef": {"Source": "d2"}}, "Property": "AGENT명"}, "Name": "DimAgent.AGENT명"},
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
                        "Primary": {"Groupings": [{"Projections": [0, 1, 2, 3]}]},
                        "DataReduction": {"DataVolume": 4, "Primary": {"Window": {"Count": 1000}}},
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


def fetch_data(stay_month: str) -> dict:
    """Power BI에서 채널별 데이터 가져오기"""
    url = f"{CLUSTER}/public/reports/querydata?synchronous=true"
    body = _build_ty_query(stay_month)
    logger.info(f"Power BI 쿼리 호출: {url}")
    r = requests.post(url, headers=_headers(), json=body, timeout=30)
    r.raise_for_status()
    return r.json()


def parse_dsr(result: dict) -> list[dict]:
    """DSR 파싱 (채널, 사업장, RNS, REV)"""
    try:
        data = result["results"][0]["result"]["data"]
        dsr = data["dsr"]["DS"][0]
        rows_raw = dsr["PH"][0]["DM0"]
        value_dicts = dsr.get("ValueDicts", {})
        col_defs = dsr.get("S", [])
    except (KeyError, IndexError) as e:
        logger.error(f"DSR 파싱 실패: {e}")
        return []

    # 컬럼별 사전 이름
    col_dicts = [c.get("DN") for c in col_defs]

    rows = []
    last = [None, None, None, None]
    for entry in rows_raw:
        c_vals = entry.get("C", [])
        r_mask = entry.get("R", 0)
        
        values = [None, None, None, None]
        c_idx = 0
        for col_idx in range(4):
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
        
        rows.append({
            "channel": values[0],
            "property": values[1],
            "rns": values[2] or 0,
            "rev_만원": values[3] or 0,
        })
    return rows


def main():
    # 현재 월 (YYYYMM)
    stay_month = datetime.now().strftime("%Y%m")
    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(exist_ok=True)
    
    logger.info(f"당월 투숙기준: {stay_month}")
    
    try:
        result = fetch_data(stay_month)
        rows = parse_dsr(result)
        logger.info(f"✓ {len(rows)}행 수집")
        
        # TOP 5 채널 계산
        channel_totals = {}
        for row in rows:
            ch = row["channel"]
            if not ch:
                continue
            ch_entry = channel_totals.setdefault(ch, {"rns": 0, "rev_만원": 0, "properties": {}})
            ch_entry["rns"] += row["rns"]
            ch_entry["rev_만원"] += row["rev_만원"] or 0
            if row["property"]:
                prop_entry = ch_entry["properties"].setdefault(row["property"], 0)
                ch_entry["properties"][row["property"]] = prop_entry + row["rns"]
        
        # 매출 TOP 5
        top5 = sorted(
            channel_totals.items(),
            key=lambda x: x[1]["rev_만원"] or 0,
            reverse=True,
        )[:5]
        
        output = {
            "collected_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "stay_month": stay_month,
            "total_rows": len(rows),
            "top5_by_revenue": [
                {"rank": i+1, "channel": ch, **data} 
                for i, (ch, data) in enumerate(top5)
            ],
            "all_channels": channel_totals,
            "raw_rows": rows,
        }
        
        # 저장
        latest = output_dir / "powerbi_latest.json"
        dated = output_dir / f"powerbi_{stay_month}_{datetime.now().strftime('%Y%m%d')}.json"
        
        with open(latest, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        with open(dated, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✓ 저장: {latest.name}")
        logger.info(f"✓ TOP 5 채널 (매출순):")
        for item in output["top5_by_revenue"]:
            logger.info(f"   [{item['rank']}] {item['channel']}: RNS {item['rns']:,} / REV {item['rev_만원']:,}만원")
        
    except Exception as e:
        logger.error(f"수집 실패: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
