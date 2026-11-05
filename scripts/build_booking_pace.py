#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""build_booking_pace.py — 사업장×투숙월×세그 주차별 예약추이(동기간 as-of 순온북).

  블록 시뮬 실적표에서 사업장 행을 펼치면 "예약이 주차별로 어떻게 쌓였나"를 본다.
    · D0 = 해당 투숙월 1일, 주차 컷오프 t_w = D0 + w*7일 (w = -8 … +5, 총 14개)
    · 각 주차 순온북(as-of) = 예약(최초입력≤t_w) + 취소복원(최초입력≤t_w & 취소일>t_w)
      → 취소 데이터 반영(동기간 로직)해야 그 시점 실제 예약수가 나옴.
  세그: OTA(53,72)·GOTA(A4,A5)·IB(58). 라인ID 중복제거(스냅샷 적층).
  출력: docs/data/booking_pace.json
    { "YYYY|M|site|seg": {"weeks": [-8..5], "rn": [netRN per week]}, ... }  seg∈OTA/GOTA/IB
"""
import os, re, glob, json, calendar
from collections import defaultdict
from datetime import date
from pathlib import Path

# 공통 로직 재사용(사이트매핑·세그·파일선택·인코딩)
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_leadtime_share import (SITE_ROOMS, _seg, _site, _open, _num, _pdate,
                                   select_files, YEARS, MONTHS_BY_YEAR, mly, RAW)

OUT = Path(__file__).resolve().parent.parent / "docs" / "data" / "booking_pace.json"
WEEKS = list(range(-8, 6))          # D-8주 … D+5주 (주차 샘플)
OFF_MIN, OFF_MAX = -56, 41          # 일별 윈도 오프셋(D0 기준): D-8주 ~ D+5주(7일단위)
NDAYS = OFF_MAX - OFF_MIN + 1       # 98일

# acc[(year, month, site, seg)] = [resv_diff(NDAYS+1), canc_diff(NDAYS+1)]  (차분배열)
acc = defaultdict(lambda: [[0.0] * (NDAYS + 1), [0.0] * (NDAYS + 1)])

def _process(fp, is_cancel):
    fh = _open(fp)
    if not fh:
        return 0
    hdr = [h.strip() for h in fh.readline().split(";")]
    idx = {h: i for i, h in enumerate(hdr)}
    g = lambda *ns: next((idx[n] for n in ns if n in idx), -1)
    i_code = g("변경예약집계코드", "예약집계코드"); i_biz = g("변경사업장명")
    i_first = g("최초입력일자"); i_ci = g("입실일자"); i_n = g("박수"); i_r = g("객실수")
    i_user = g("이용자명"); i_mem = g("회원번호"); i_rt = g("객실타입"); i_cancel = g("취소일자")
    if min(i_code, i_biz, i_first, i_ci, i_n, i_r) < 0:
        return 0
    hi = max(i_code, i_biz, i_first, i_ci, i_n, i_r, i_user, i_mem, i_rt, i_cancel)
    seen = set(); n = 0
    for ln in fh:
        p = ln.rstrip("\n").split(";")
        if len(p) <= hi: continue
        seg = _seg(p[i_code])
        if not seg: continue
        site = _site(p[i_biz])
        if not site: continue
        ci = _pdate(p[i_ci])
        if not ci or ci.year not in YEARS or ci.month not in mly(ci.year): continue
        key = (p[i_user] if i_user >= 0 else "", p[i_mem] if i_mem >= 0 else "",
               p[i_rt] if i_rt >= 0 else "", p[i_ci], p[i_n], p[i_r], p[i_code],
               p[i_first] if i_first >= 0 else "", p[i_cancel] if i_cancel >= 0 else "")
        if key in seen: continue
        seen.add(key)
        rn = _num(p[i_n]) * _num(p[i_r])
        if rn <= 0: continue
        fd = _pdate(p[i_first])
        if not fd: continue
        xd = _pdate(p[i_cancel]) if (is_cancel and i_cancel >= 0) else None
        d0 = date(ci.year, ci.month, 1)
        cs = (fd - d0).days                       # 최초입력 − D0 (일)
        xs = (xd - d0).days if xd else None        # 취소일 − D0
        darr = acc[(ci.year, ci.month, site, seg)][1 if is_cancel else 0]
        if is_cancel:
            if xs is None: continue
            s = max(OFF_MIN, cs); e = min(OFF_MAX, xs - 1)   # 복원: [cs, 취소일-1]
            if e < s: continue
            darr[s - OFF_MIN] += rn; darr[e - OFF_MIN + 1] -= rn
        else:
            s = max(OFF_MIN, cs)                              # 예약(생존): [cs, 윈도끝]
            if s > OFF_MAX: continue
            darr[s - OFF_MIN] += rn
        n += 1
    fh.close()
    return n

def main():
    for y in YEARS:
        resv, canc = select_files(str(RAW / str(y)))
        print(f"[{y}] 예약 {len(resv)} · 취소 {len(canc)} 파일")
        for fp in resv: print(f"   예약 {_process(fp, False):>9,}행")
        for fp in canc: print(f"   취소 {_process(fp, True):>9,}행")

    out = {}
    for (y, m, site, seg), (rv, cc) in acc.items():
        daily = []; run = 0.0
        for i in range(NDAYS):                       # 차분배열 누적 → 일별 as-of 순온북
            run += rv[i] + cc[i]
            daily.append(round(run))
        out[f"{y}|{m}|{site}|{seg}"] = daily          # 98일(오프셋 OFF_MIN..OFF_MAX)
    data = {"meta": {"weeks": WEEKS, "off_min": OFF_MIN, "off_max": OFF_MAX, "ndays": NDAYS,
                     "d0": "투숙월 1일", "segs": ["OTA", "GOTA", "IB"],
                     "note": "일별 동기간(as-of) 순온북 = 예약(최초입력≤컷)+취소복원(최초입력≤컷&취소일>컷). 주차=7일 샘플"},
            "series": out}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print(f"✓ booking_pace.json: {len(out)} 계열")
    # 검증: 천안 2026-01 OTA 주차 순온북
    k = "2026|1|소노벨 천안|OTA"
    if k in out:
        dl = out[k]
        wk = [dl[w * 7 - OFF_MIN] for w in WEEKS]
        print("천안 2026-01 OTA 주차(-8..+5):", wk)
        print("  D-1주 7일(일별):", dl[(-7) - OFF_MIN:(0) - OFF_MIN])

if __name__ == "__main__":
    main()
