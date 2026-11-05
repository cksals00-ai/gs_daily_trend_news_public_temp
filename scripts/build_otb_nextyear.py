#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""build_otb_nextyear.py — 트렌드 리포트 '익익월'이 차년도(2027-01~)로 넘어가는 연말 보정.

  generate_otb_data.py 는 2026년까지만 집계 → 연말(11·12월)엔 익익월(2027-01) 칼럼이 빔.
  핵심 파이프라인(parse_raw_db)을 건드리지 않고, 트렌드 리포트 OTB 칼럼에 쓸
  2027-01·02 온북/예산만 별도 산출해 docs/data/otb_nextyear.json 으로 내보낸다.

    · 온북(actual)  = 2026 스냅샷 예약자료(현 온북)에서 투숙월 2027-01·02, OTA/GOTA/IB 합(RN=박수×객실수)
    · 예산(budget)  = 2027 GS 예산 업로드본(OTA+GOTA+IB RNS) 월합
    · 달성률 ach    = actual / budget
    · fcst          = budget(계획), fcst_ach = 100%
  build.py 가 익익월 키(13↑)일 때 이 값을 주입.
"""
import os, re, glob, json
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW26 = ROOT / "data" / "raw_db" / "2026"
BUDGET_XLSX = ROOT / "data" / "raw_db" / "budget" / "2027" / "gs_budget_2027_upload.xlsx"
OUT = ROOT / "docs" / "data" / "otb_nextyear.json"

TARGET_YM = ["202701", "202702"]
SCOPE = {"53", "72", "A4", "A5", "58"}   # OTA(53,72)·GOTA(A4,A5)·Inbound(58)
ENC = ["cp949", "euc-kr", "utf-8"]

BUDGET_CACHE = ROOT / "docs" / "data" / "budget_2027_monthly.json"

def _budget():
    b = {ym: 0.0 for ym in TARGET_YM}
    # 1순위: stdlib JSON 캐시(데몬 python3에 openpyxl 없어도 동작)
    if BUDGET_CACHE.exists():
        try:
            j = json.loads(BUDGET_CACHE.read_text(encoding="utf-8"))
            for ym in TARGET_YM:
                b[ym] = float(j.get(ym, 0) or 0)
            if any(b.values()):
                return b
        except Exception:
            pass
    # 폴백: 업로드 xlsx (openpyxl 있을 때만)
    try:
        import openpyxl
    except ImportError:
        return b
    if not BUDGET_XLSX.exists():
        return b
    wb = openpyxl.load_workbook(BUDGET_XLSX, read_only=True)
    ws = wb["샘플"] if "샘플" in wb.sheetnames else wb.worksheets[0]
    for r in ws.iter_rows(min_row=2, values_only=True):
        if len(r) < 7:
            continue
        ym, metric, val = str(r[0]), r[2], r[6]
        if metric == "RNS" and ym in b:
            try: b[ym] += float(val or 0)
            except (TypeError, ValueError): pass
    wb.close()
    return b

def _open(fp):
    for e in ENC:
        try:
            fh = open(fp, encoding=e); fh.readline(); fh.seek(0); return fh
        except Exception:
            continue
    return None

def _onbook():
    ob = {ym: 0.0 for ym in TARGET_YM}
    # 스냅샷 예약자료(현 온북, 취소 제외분)만 — 27(온라인) + 43([57,87])
    files = [f for f in glob.glob(str(RAW26 / "*생성시간*.txt"))
             if os.path.basename(f).split(".")[0] in ("27", "43")]
    for fp in files:
        fh = _open(fp)
        if not fh:
            continue
        hdr = [h.strip() for h in fh.readline().split(";")]
        idx = {h: i for i, h in enumerate(hdr)}
        def gi(*names):
            for n in names:
                if n in idx: return idx[n]
            return -1
        i_code = gi("변경예약집계코드", "예약집계코드")
        i_ci = gi("입실일자"); i_n = gi("박수"); i_r = gi("객실수")
        if min(i_code, i_ci, i_n, i_r) < 0:
            fh.close(); continue
        for ln in fh:
            p = ln.rstrip("\n").split(";")
            if len(p) <= i_r:
                continue
            if (p[i_code] or "").strip() not in SCOPE:
                continue
            ci = (p[i_ci] or "").strip()
            ym = ci[:6]
            if ym not in ob or not re.fullmatch(r"\d{8}", ci):
                continue
            try: ob[ym] += float(p[i_n] or 0) * float(p[i_r] or 0)
            except (TypeError, ValueError): pass
        fh.close()
    return ob

def main():
    budget = _budget()
    onbook = _onbook()
    out = {}
    for ym in TARGET_YM:
        bud = round(budget[ym]); act = round(onbook[ym])
        ach = round(act / bud * 100, 1) if bud > 0 else None
        out[ym] = {"actual": act, "budget": bud, "ach": ach,
                   "fcst": bud, "fcst_ach": 100.0 if bud > 0 else None, "today_net": 0}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print("✓ otb_nextyear.json:", {k: (v["actual"], v["budget"], v["ach"]) for k, v in out.items()})

if __name__ == "__main__":
    main()
