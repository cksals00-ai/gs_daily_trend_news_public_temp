#!/usr/bin/env python3
"""
build_visit_stats.py — 방문 로그(VISIT_LOG)를 페이지별 사용도로 집계.

  입력(둘 중 하나):
    1) GAS export:  GSN_SYNC_KEY 환경변수 + (선택) GSN_API_URL 로 export_visits 호출
    2) CSV 파일:    시트 'VISIT_LOG' 를 CSV 로 내려받아 경로 인자로 전달
                    예)  python3 scripts/build_visit_stats.py ~/Downloads/VISIT_LOG.csv

  출력:
    - docs/data/visit_stats.json  (페이지별 집계 + 미방문 페이지 목록)
    - 콘솔에 사용/미사용 랭킹 요약

  집계: 페이지별 총 방문수, 순사용자(uid) 수, 역할별 분해, 최초/최근 방문일.
        docs/*.html 전체와 대조해 "0회 방문(미사용 후보)" 을 별도로 표기.
"""
import csv
import json
import os
import sys
import urllib.request
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
OUT = DOCS / "data" / "visit_stats.json"
DEFAULT_API = ("https://script.google.com/macros/s/"
               "AKfycbz9JwH0BJfcH-AVo9Vy1EYasER5jAz_5ZL9e2v22PtrGZ7Yb5ATbJLuUJ9UvGDjv07MJA/exec")


def load_rows_from_api(api_url, key):
    url = f"{api_url}?action=export_visits&key={key}"
    with urllib.request.urlopen(url, timeout=60) as r:
        payload = json.loads(r.read().decode("utf-8"))
    if payload.get("status") != "ok":
        raise SystemExit(f"export 실패: {payload}")
    return payload.get("rows", [])


def load_rows_from_csv(path):
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            rows.append({
                "ts": r.get("ts_kst") or r.get("ts", ""),
                "date": r.get("date", ""),
                "page": r.get("page", ""),
                "uid": r.get("uid", ""),
                "role": r.get("role", ""),
                "referrer": r.get("referrer", ""),
            })
    return rows


def aggregate(rows):
    per = {}
    for r in rows:
        page = (r.get("page") or "").strip()
        if not page:
            continue
        p = per.setdefault(page, {
            "page": page, "visits": 0, "uids": set(),
            "by_role": defaultdict(int), "first": None, "last": None,
        })
        p["visits"] += 1
        uid = (r.get("uid") or "").strip()
        if uid:
            p["uids"].add(uid)
        p["by_role"][r.get("role") or "anon"] += 1
        d = (r.get("date") or "").strip()
        if d:
            p["first"] = d if p["first"] is None else min(p["first"], d)
            p["last"] = d if p["last"] is None else max(p["last"], d)
    out = []
    for p in per.values():
        out.append({
            "page": p["page"],
            "visits": p["visits"],
            "unique_users": len(p["uids"]),
            "by_role": dict(p["by_role"]),
            "first_visit": p["first"],
            "last_visit": p["last"],
        })
    out.sort(key=lambda x: (-x["visits"], x["page"]))
    return out


def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else None
    if csv_path:
        rows = load_rows_from_csv(csv_path)
        src = f"csv:{csv_path}"
    else:
        key = os.environ.get("GSN_SYNC_KEY", "")
        if not key:
            raise SystemExit("CSV 경로 인자 없이 실행하려면 GSN_SYNC_KEY 환경변수 필요.\n"
                             "  또는:  python3 scripts/build_visit_stats.py VISIT_LOG.csv")
        api = os.environ.get("GSN_API_URL", DEFAULT_API)
        rows = load_rows_from_api(api, key)
        src = "api:export_visits"

    stats = aggregate(rows)
    logged_pages = {s["page"] for s in stats}
    all_pages = sorted(p.name for p in DOCS.glob("*.html"))
    never = [p for p in all_pages if p not in logged_pages]

    dates = [r.get("date") for r in rows if r.get("date")]
    result = {
        "source": src,
        "total_events": len(rows),
        "period": {"from": min(dates) if dates else None,
                   "to": max(dates) if dates else None},
        "pages": stats,
        "never_visited": never,
    }
    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[visit-stats] {len(rows)} events · {len(stats)} pages 방문 · "
          f"{len(never)} pages 미방문  → {OUT.relative_to(ROOT)}")
    print(f"기간: {result['period']['from']} ~ {result['period']['to']}\n")
    print("── 상위 방문 페이지 ──")
    for s in stats[:15]:
        roles = ",".join(f"{k}:{v}" for k, v in sorted(s["by_role"].items()))
        print(f"  {s['visits']:>5}  {s['unique_users']:>3}명  {s['page']:<34} [{roles}]")
    if never:
        print(f"\n── 0회 방문(미사용 후보, {len(never)}개) ──")
        for p in never:
            print(f"  ·  {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
