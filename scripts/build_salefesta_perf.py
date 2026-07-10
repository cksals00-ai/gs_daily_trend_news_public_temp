#!/usr/bin/env python3
"""
build_salefesta_perf.py — 숙박세일페스타 2026 여름편 실적 슬라이스 (gs-salefesta.html §5 연동)

소스: docs/data/campaign_history.json (build_campaign_history.py 산출, daily_update.sh 10b/12)
      → 4.4MB 큐브를 페이지가 직접 받지 않도록, 숙세페 스코프만 잘라 경량 JSON으로 재발행.

스코프 2종 (합계가 서로 다름 — 페이지에 명시):
  1) campaign  = 패키지분류코드명 '2026년 숙세페'  ← 대표 실적(요약·차원분해의 기준)
  2) products  = 회원명 정규화 계열 중 숙세페/숙박세일 계열(2026 판매분) ← 상품별 상세
     · by_product 에는 campaign 역참조가 없어 두 스코프는 커버리지가 완전히 일치하지 않음.

참고군: 직전 세일페스타 계열(판매 2025-03~04) — 상품계열 합계만.

RN=객실수, 금액 단위=백만원(VAT 제외), 온북(유효예약) 기준. 취소 축 없음.
일별 축은 원천에 없으므로, 실행 시점의 캠페인 합계를 history[]에 날짜별 1건씩 누적(멱등).

출력: docs/data/salefesta_perf.json
"""
from __future__ import annotations
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).resolve().parent.parent
SOURCE_JSON = PROJECT_DIR / "docs" / "data" / "campaign_history.json"
OUTPUT_JSON = PROJECT_DIR / "docs" / "data" / "salefesta_perf.json"

CAMPAIGN_KEY = "2026년 숙세페"
SUMMER_TOKENS = ("숙세페", "숙박세일")
SPRING_TOKEN = "세일페스타"
KST = timezone(timedelta(hours=9))


def _is_2026_sale(entry: dict) -> bool:
    return any(m.startswith("2026") for m in entry.get("by_sale_month", {}))


def _is_pre_2026_sale(entry: dict) -> bool:
    months = entry.get("by_sale_month", {})
    return bool(months) and all(m < "202601" for m in months)


def _rank(dim: dict) -> list[dict]:
    """{name: {rn, rev_m}} → rn 내림차순 리스트"""
    rows = [{"name": k, "rn": v["rn"], "rev_m": v["rev_m"]} for k, v in dim.items()]
    rows.sort(key=lambda r: -r["rn"])
    return rows


def _summary(entry: dict) -> dict:
    return {k: entry[k] for k in ("rn", "room_rev_m", "total_rev_m", "commission_m", "adr")}


def main() -> None:
    if not SOURCE_JSON.exists():
        logger.error("소스 없음: %s — build_campaign_history.py 먼저 실행", SOURCE_JSON)
        raise SystemExit(1)

    cube = json.loads(SOURCE_JSON.read_text(encoding="utf-8"))
    by_campaign = cube.get("by_campaign", {})
    by_product = cube.get("by_product", {})

    camp = by_campaign.get(CAMPAIGN_KEY)
    if not camp:
        logger.error("기획전 키 '%s' 없음 — 패키지분류코드명 변경 여부 확인", CAMPAIGN_KEY)
        raise SystemExit(1)

    # ── 상품계열 (여름 / 직전 봄편 참고군) ──────────────────────────
    summer_products, spring_products = [], []
    for name, e in by_product.items():
        if any(t in name for t in SUMMER_TOKENS) and _is_2026_sale(e):
            summer_products.append({
                "name": name, "rn": e["rn"], "room_rev_m": e["room_rev_m"],
                "total_rev_m": e["total_rev_m"], "adr": e["adr"],
                "sale_months": sorted(e["by_sale_month"]),
            })
        elif SPRING_TOKEN in name and _is_pre_2026_sale(e):
            spring_products.append({"name": name, "rn": e["rn"], "room_rev_m": e["room_rev_m"]})

    summer_products.sort(key=lambda r: -r["rn"])
    spring_products.sort(key=lambda r: -r["rn"])

    spring_months = sorted({m for n in spring_products
                            for m in by_product[n["name"]]["by_sale_month"]})

    today = datetime.now(KST).strftime("%Y-%m-%d")

    out = {
        "meta": {
            "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
            "base_date": today,
            "source": "docs/data/campaign_history.json",
            "campaign_key": CAMPAIGN_KEY,
            "basis": "온북(유효예약, 27/43) · RN=객실수 · 금액=백만원(VAT 제외)",
            "scope_note": (
                "요약·차원분해=패키지분류코드명 기준. 상품계열 표=회원명 정규화 기준으로 "
                "커버리지가 달라 합계가 일치하지 않음."
            ),
        },
        "summary": _summary(camp),
        "by_sale_month": camp["by_sale_month"],
        "by_stay_month": camp["by_stay_month"],
        "by_segment": _rank(camp["by_segment"]),
        "by_property": _rank(camp["by_property"]),
        "by_agent": _rank(camp["by_agent"]),
        "products": summer_products,
        "spring_ref": {
            "sale_months": spring_months,
            "products": len(spring_products),
            "rn": sum(p["rn"] for p in spring_products),
            "room_rev_m": round(sum(p["room_rev_m"] for p in spring_products), 2),
            "top": spring_products[:5],
        },
        "history": [],
    }

    # ── 일별 온북 스냅샷 누적 (같은 날짜 재실행 시 갱신) ──────────────
    prior = []
    if OUTPUT_JSON.exists():
        try:
            prior = json.loads(OUTPUT_JSON.read_text(encoding="utf-8")).get("history", [])
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("기존 history 읽기 실패(%s) — 새로 시작", exc)
    snap = {"date": today, "rn": camp["rn"], "room_rev_m": camp["room_rev_m"]}
    out["history"] = sorted(
        [h for h in prior if h.get("date") != today] + [snap],
        key=lambda h: h["date"],
    )

    OUTPUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    logger.info(
        "✅ %s — RN %s · 객실매출 %.1f백만 · 상품 %d계열 · 스냅샷 %d일",
        OUTPUT_JSON.name, f"{camp['rn']:,}", camp["room_rev_m"],
        len(summer_products), len(out["history"]),
    )


if __name__ == "__main__":
    main()
