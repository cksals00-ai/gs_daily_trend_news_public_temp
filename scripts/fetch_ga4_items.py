#!/usr/bin/env python3.12
"""GA4 상품(item) 단위 수집 — 2027 상품기획 2차.
fetch_ga4.py의 클라이언트/실행 헬퍼를 재사용하되 별도 산출물로 저장(일일 파이프라인 무영향).

산출 docs/data/ga4_items.json:
  meta            {built, property, date_start, date_end}
  items[]         {name, category, prop(display|None), series(정규화), by_month:{YYYYMM:[purchases, revenue]}, purchases, revenue}
  channel_month   {YYYYMM: {channelGroup: [sessions, purchases, revenue]}}
  dow_month       {YYYYMM: [일..토 purchases]}
  totals_month    {YYYYMM: [purchases, revenue]}
사용: python3.12 scripts/fetch_ga4_items.py [--start 2024-01-01] [--end yesterday] [--property 433673272]
자격증명: fetch_ga4._load_client (env 또는 ga4_oauth_token.json). 대외비·관리자용.
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_ga4 as fg  # noqa: E402
import product_naming as pn  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("fetch_ga4_items")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def prop_display_from_category(cat):
    km = fg._property_key(cat)
    if not km:
        return None
    return f"{km[0]}.{km[1]}"


def collect(client, pid, start, end):
    out = {"meta": {"built": datetime.now().strftime("%Y-%m-%d %H:%M"), "property": pid, "date_start": start, "date_end": end,
                    "note": "GA4 이커머스 구매(예약일 기준) · 상품명=GA itemName · 대외비·관리자용"}}
    # 1) itemName × itemCategory × yearMonth
    rows = fg._run(client, pid, ["itemName", "itemCategory", "yearMonth"], ["itemsPurchased", "itemRevenue"],
                   start, end, order_by_metric="itemsPurchased", limit=100000)
    items = {}
    for r in rows:
        name, cat, ym = r.get("itemName") or "", r.get("itemCategory") or "", r.get("yearMonth") or ""
        pur, rev = int(r.get("itemsPurchased") or 0), float(r.get("itemRevenue") or 0)
        if not name or not ym or (pur == 0 and rev == 0):
            continue
        key = (name, cat)
        it = items.get(key)
        if it is None:
            it = items[key] = {"name": name, "category": cat, "prop": prop_display_from_category(cat),
                               "series": pn.clean_product_name(name.split("_")[0] if "_" in name and name.split("_")[0] else name),
                               "by_month": {}, "purchases": 0, "revenue": 0.0}
        bm = it["by_month"].setdefault(ym, [0, 0.0])
        bm[0] += pur; bm[1] += rev
        it["purchases"] += pur; it["revenue"] += rev
    il = sorted(items.values(), key=lambda x: -x["purchases"])
    for it in il:
        it["revenue"] = round(it["revenue"])
        it["by_month"] = {k: [v[0], round(v[1])] for k, v in sorted(it["by_month"].items())}
    out["items"] = il
    log.info("items: %d (rows %d)", len(il), len(rows))
    # 2) channel group × yearMonth
    rows = fg._run(client, pid, ["sessionDefaultChannelGroup", "yearMonth"], ["sessions", "ecommercePurchases", "purchaseRevenue"],
                   start, end, limit=5000)
    cm = {}
    for r in rows:
        ym = r.get("yearMonth") or ""
        cm.setdefault(ym, {})[r.get("sessionDefaultChannelGroup") or "(none)"] = [
            int(r.get("sessions") or 0), int(r.get("ecommercePurchases") or 0), round(float(r.get("purchaseRevenue") or 0))]
    out["channel_month"] = dict(sorted(cm.items()))
    # 3) dayOfWeek × yearMonth purchases (0=일 … 6=토)
    rows = fg._run(client, pid, ["dayOfWeek", "yearMonth"], ["ecommercePurchases"], start, end, limit=5000)
    dm = {}
    for r in rows:
        ym = r.get("yearMonth") or ""
        try:
            d = int(r.get("dayOfWeek"))
        except (TypeError, ValueError):
            continue
        dm.setdefault(ym, [0] * 7)[d] += int(r.get("ecommercePurchases") or 0)
    out["dow_month"] = dict(sorted(dm.items()))
    # 4) totals per month
    rows = fg._run(client, pid, ["yearMonth"], ["ecommercePurchases", "purchaseRevenue", "sessions"], start, end, limit=1000)
    out["totals_month"] = {r.get("yearMonth"): [int(r.get("ecommercePurchases") or 0), round(float(r.get("purchaseRevenue") or 0)),
                                                int(r.get("sessions") or 0)] for r in rows if r.get("yearMonth")}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--property", default=fg.DEFAULT_PROPERTY_ID)
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="yesterday")
    ap.add_argument("--output", default=os.path.join(ROOT, "docs", "data", "ga4_items.json"))
    a = ap.parse_args()
    client = fg._load_client()
    if client is None:
        log.error("GA4 클라이언트 없음 — 자격증명 확인")
        return 1
    data = collect(client, a.property, a.start, a.end)
    with open(a.output, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    tm = data["totals_month"]
    log.info("저장 %s | months %s~%s | items %d | size %.1fMB", a.output, min(tm) if tm else "-", max(tm) if tm else "-",
             len(data["items"]), os.path.getsize(a.output) / 1e6)
    return 0


if __name__ == "__main__":
    sys.exit(main())
