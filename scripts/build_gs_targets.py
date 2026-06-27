#!/usr/bin/env python3
"""독립 목표(Target) 테이블 생성기 — gs_targets.json

GS 세일즈 OTA+G-OTA **월별 목표(rm_budget_rn, 사업계획)** 를 연도별로 정제해 단일
산출물로 관리한다. Start 달성률(booking-trend.html '예약추이')·YoY 의 분모로 쓰인다.

소스 (연도별 권위 소스 분리):
  • 2026  : otb_data.json segmentData[OTA|G-OTA].rns_budget — 사업계획 12개월 완비(권위).
  • 2024/2025: Revenue Meeting PDF 원본(data/RM자료/*.pdf)을 parse_rm_fcst 로 직접 파싱.
             세그먼트 rm_budget_rn(OTA+G-OTA) 합. 예산은 회의마다 재표기되지만 연내 안정적이라,
             각 (연-월)에 대해 **사업장 커버리지가 가장 큰 스냅샷**의 값을 채택(부분 스냅샷 배제).

자체 PDF 캐시(_PDF_CACHE) 보유 → 재실행 시 변경 PDF만 재파싱(빠름). rm_fcst_trend.json
(일일 파이프라인 산출물)과 독립적으로 동작·관리한다.

사용:
  python3 scripts/build_gs_targets.py            # 증분(캐시 사용)
  python3 scripts/build_gs_targets.py --rebuild  # 캐시 무시 전체 재파싱
"""
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from parse_rm_fcst import parse as parse_pdf  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
PDF_DIR = REPO / "data" / "RM자료"
OTB_PATH = REPO / "docs" / "data" / "otb_data.json"
OUT = REPO / "docs" / "data" / "gs_targets.json"
OUT2 = REPO / "data" / "gs_targets.json"
PDF_CACHE = REPO / "data" / "_gs_targets_pdf_cache.json"

# RM PDF에서 목표를 끌어올 연도 (2026은 otb_data 권위 소스 사용)
RM_YEARS = {"2024", "2025"}
SKIP_PDFS = {"Revenue Meeting_2024.01.24.pdf"}  # hang/corrupt (build_fcst_trend와 동일)


def ota_gota_budget_by_ym(props):
    """parse 결과 properties → {ym: (budget_sum, prop_count)}  (OTA+G-OTA rm_budget_rn)."""
    agg = defaultdict(int)
    cnt = defaultdict(int)
    for _prop, months in props.items():
        for ym, node in months.items():
            seg = node.get("segments", {})
            ota = (seg.get("OTA", {}) or {}).get("rm_budget_rn", 0) or 0
            gota = (seg.get("G-OTA", {}) or {}).get("rm_budget_rn", 0) or 0
            b = ota + gota
            if b > 0:
                agg[ym] += b
                cnt[ym] += 1
    return {ym: (agg[ym], cnt[ym]) for ym in agg}


def load_cache():
    if PDF_CACHE.exists():
        try:
            return json.loads(PDF_CACHE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def parse_rm_targets(force_full=False):
    """RM PDF 전수 파싱 → ym별 후보값 수집(스냅샷별 budget,prop_count,date,pdf)."""
    cache = {} if force_full else load_cache()
    new_cache = {}
    pdfs = sorted(PDF_DIR.glob("Revenue Meeting_*.pdf"))
    # ym -> list of {budget, props, date, pdf}
    cands = defaultdict(list)
    n_parsed = n_cached = n_fail = 0
    for pdf in pdfs:
        m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", pdf.name)
        if not m:
            continue
        year = m.group(1)
        if year not in RM_YEARS:
            continue
        if pdf.name in SKIP_PDFS:
            continue
        snap_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        key = pdf.name
        # 캐시 키: 파일명 + mtime (변경 감지)
        mtime = int(pdf.stat().st_mtime)
        cached = cache.get(key)
        if cached and cached.get("_mtime") == mtime:
            by_ym = cached["by_ym"]
            n_cached += 1
        else:
            try:
                parsed = parse_pdf(pdf)
            except Exception as e:
                print(f"  ERR {pdf.name}: {e!r}", file=sys.stderr)
                n_fail += 1
                continue
            props = parsed.get("properties", {})
            if not props:
                print(f"  EMPTY {pdf.name}", file=sys.stderr)
                n_fail += 1
                continue
            by_ym = {ym: list(v) for ym, v in ota_gota_budget_by_ym(props).items()}
            n_parsed += 1
            print(f"  OK {pdf.name}: {len(by_ym)} months", file=sys.stderr)
        new_cache[key] = {"_mtime": mtime, "by_ym": by_ym}
        for ym, (budget, pcount) in by_ym.items():
            cands[ym].append({"budget": budget, "props": pcount,
                              "date": snap_date, "pdf": pdf.name})
    PDF_CACHE.write_text(json.dumps(new_cache, ensure_ascii=False), encoding="utf-8")
    print(f"RM 파싱: 신규 {n_parsed}, 캐시 {n_cached}, 실패 {n_fail}", file=sys.stderr)
    return cands


def select_target(cands_for_ym):
    """(연-월) 목표 채택 규칙:
    1) 사업장 커버리지(props) 최대인 스냅샷만 남김(부분 스냅샷 배제).
    2) 그 중 **최빈 budget** 채택 — 사업계획은 연내 안정적이므로 참값이 가장 자주 등장.
       1회성 오파싱(예: 옆월값 혼입)은 빈도 1로 배제됨. 동률이면 최신 날짜.
    """
    from collections import Counter
    maxp = max(c["props"] for c in cands_for_ym)
    full = [c for c in cands_for_ym if c["props"] == maxp]
    freq = Counter(c["budget"] for c in full)
    top = max(freq.values())
    winners = {b for b, f in freq.items() if f == top}
    best = max((c for c in full if c["budget"] in winners), key=lambda c: c["date"])
    return best


def load_otb_2026():
    """otb_data.json segmentData → 2026 월별 OTA+G-OTA 목표(권위 사업계획)."""
    o = json.loads(OTB_PATH.read_text(encoding="utf-8"))
    am = o.get("allMonths", {})
    out = {}
    prov = {}
    for mi in range(1, 13):
        sd = (am.get(str(mi)) or {}).get("segmentData") or {}
        ota = (sd.get("OTA", {}) or {}).get("rns_budget", 0) or 0
        gota = (sd.get("G-OTA", {}) or {}).get("rns_budget", 0) or 0
        if ota + gota > 0:
            mm = f"{mi:02d}"
            out[mm] = ota + gota
            prov[f"2026-{mm}"] = {"value": ota + gota, "source": "otb_data.json segmentData"}
    return out, prov


def main():
    force_full = ("--rebuild" in sys.argv) or ("--full" in sys.argv)
    targets = {}
    provenance = {}

    # 2026: otb_data 권위
    t26, p26 = load_otb_2026()
    targets["2026"] = t26
    provenance.update(p26)

    # 2024/2025: RM PDF
    cands = parse_rm_targets(force_full=force_full)
    by_year = defaultdict(dict)
    for ym, clist in cands.items():
        y, mm = ym.split("-")
        if y not in RM_YEARS:
            continue
        best = select_target(clist)
        by_year[y][mm] = best["budget"]
        provenance[ym] = {"value": best["budget"], "source": best["pdf"],
                          "prop_count": best["props"],
                          "candidates": len({c["budget"] for c in clist})}
    for y in RM_YEARS:
        if by_year.get(y):
            targets[y] = dict(sorted(by_year[y].items()))

    out = {
        "_description": "GS OTA+G-OTA 월별 목표(rm_budget_rn, 사업계획) — Start 달성률/YoY 분모",
        "_unit": "RN (rooms), OTA+G-OTA 합산",
        "_generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "_sources": {
            "2026": "otb_data.json segmentData (사업계획 12개월)",
            "2024/2025": "data/RM자료/Revenue Meeting_*.pdf (parse_rm_fcst, 커버리지 최대 스냅샷)",
        },
        "targets": {y: targets[y] for y in sorted(targets)},
        "_provenance": dict(sorted(provenance.items())),
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT2.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    # 요약 출력
    for y in sorted(targets):
        row = " ".join(f"{m}:{targets[y][m]:,}" for m in sorted(targets[y]))
        print(f"{y}: {row}")
    print(f"\n✅ 작성: {OUT}")


if __name__ == "__main__":
    main()
