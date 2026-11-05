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
                                   _daytype_int, _daytype_wd,
                                   select_files, YEARS, MONTHS_BY_YEAR, mly, RAW)

OUT = Path(__file__).resolve().parent.parent / "docs" / "data" / "booking_pace.json"
WEEKS = list(range(-8, 6))          # D-8주 … D+5주 (주차 샘플)
OFF_MIN, OFF_MAX = -56, 41          # 일별 윈도 오프셋(D0 기준): D-8주 ~ D+5주(7일단위)
NDAYS = OFF_MAX - OFF_MIN + 1       # 98일

# acc[key] = 6개 차분배열: [rn예약, rn취소, room예약, room취소, rev예약, rev취소]
#   rn=총 net, room=룸온니(회원번호 46/56) net, rev=매출(1박객실료×RN, 천원) → ADR=rev/rn
#   pkg(패키지, 회원번호 86)=rn−room 로 대시보드에서 산출
acc = defaultdict(lambda: [[0.0] * (NDAYS + 1) for _ in range(6)])
# ch_acc[(year,month,site,seg)][거래처] = net RN (거래처 비중용, dt 무관)
ch_acc = defaultdict(lambda: defaultdict(float))

_AGENT_ALIAS = {"놀유니버스": "야놀자"}
def _agent(a):
    a = re.sub(r"^G?OTA_", "", (a or "").strip())
    a = re.sub(r"\([^)]*\)", "", a)               # 괄호내용 제거(스마트인피니(객실)→스마트인피니)
    a = re.sub(r"컴퍼니$", "", a).strip()          # 여기어때컴퍼니 → 여기어때
    return _AGENT_ALIAS.get(a, a)                  # 놀유니버스 → 야놀자

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
    i_season = g("시즌명"); i_rate = g("1박객실료"); i_agent = g("AGENT명")
    if min(i_code, i_biz, i_first, i_ci, i_n, i_r) < 0:
        return 0
    hi = max(i_code, i_biz, i_first, i_ci, i_n, i_r, i_user, i_mem, i_rt, i_cancel, i_season, i_rate, i_agent)
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
        # 공통 윈도 구간(모든 가중치 동일)
        if is_cancel:
            if xs is None: continue
            s = max(OFF_MIN, cs); e = min(OFF_MAX, xs - 1)   # 복원: [cs, 취소일-1]
            if e < s: continue
        else:
            s = max(OFF_MIN, cs)                              # 예약(생존): [cs, 윈도끝]
            if s > OFF_MAX: continue
        mem = (p[i_mem] or "").strip() if i_mem >= 0 else ""
        is_room = mem[:2] in ("46", "56")                    # 룸온니(46/56), 패키지(86)
        rate = _num(p[i_rate]) if i_rate >= 0 else 0.0
        revk = rate * rn / 1000.0                             # 매출(천원)
        agent = _agent(p[i_agent]) if i_agent >= 0 else ""
        dt_int = _daytype_int(p[i_season] if i_season >= 0 else "", ci)  # 통합 구조
        dt_wd = _daytype_wd(ci)                                          # 요일 구조
        sgn = -1.0 if is_cancel else 1.0
        if agent:                                             # 거래처 net(최종, dt무관)
            ch_acc[(ci.year, ci.month, site, seg)][agent] += sgn * rn
        for bkt in (f"INT:{dt_int}", f"WD:{dt_wd}"):          # 두 구조 동시 적재
            cell = acc[(ci.year, ci.month, site, seg, bkt)]
            base = 1 if is_cancel else 0                      # 0/2/4=예약, 1/3/5=취소
            # (배열, 가중치) — rn / room(룸온니만) / rev
            for off, wt in ((0, rn), (2, rn if is_room else 0.0), (4, revk)):
                if not wt: continue
                arr = cell[off + base]
                arr[s - OFF_MIN] += wt
                if is_cancel: arr[e - OFF_MIN + 1] -= wt      # 취소복원 구간 닫기
        n += 1
    fh.close()
    return n

def _cum(resv, canc):
    """차분배열 2개(예약+취소복원) → 일별 as-of 누적 98개."""
    out = []; run = 0.0
    for i in range(NDAYS):
        run += resv[i] + canc[i]
        out.append(run)
    return out

def main():
    for y in YEARS:
        resv, canc = select_files(str(RAW / str(y)))
        print(f"[{y}] 예약 {len(resv)} · 취소 {len(canc)} 파일")
        for fp in resv: print(f"   예약 {_process(fp, False):>9,}행")
        for fp in canc: print(f"   취소 {_process(fp, True):>9,}행")

    # 거래처 비중(최종 net) — (y,m,site,seg)별 상위(≥5%)만 저장
    channels = {}
    for (y, m, site, seg), d in ch_acc.items():
        tot = sum(v for v in d.values() if v > 0)
        if tot <= 0: continue
        top = [[a, round(v)] for a, v in sorted(d.items(), key=lambda kv: -kv[1]) if v > 0 and v / tot >= 0.05]
        if top:
            channels[f"{y}|{m}|{site}|{seg}"] = top

    WI = [w * 7 - OFF_MIN for w in WEEKS]                     # 주차 샘플 인덱스(14)
    out = {}
    for (y, m, site, seg, dt), cell in acc.items():
        rn = _cum(cell[0], cell[1]); room = _cum(cell[2], cell[3]); rev = _cum(cell[4], cell[5])
        if not any(round(x) for x in rn): continue
        out[f"{y}|{m}|{site}|{seg}|{dt}"] = {
            "rn":  [round(x) for x in rn],                    # 총 net 일별(98) — 막대높이·%final·일별드릴
            "wkr": [round(room[i]) for i in WI],              # 주차별 룸온니 net(14) — pkg=rn-wkr
            "wkv": [round(rev[i]) for i in WI],               # 주차별 매출 천원(14) — ADR=wkv*1000/rn
        }
    data = {"meta": {"weeks": WEEKS, "off_min": OFF_MIN, "off_max": OFF_MAX, "ndays": NDAYS,
                     "d0": "투숙월 1일", "segs": ["OTA", "GOTA", "IB"],
                     "bases": ["INT", "WD"], "daytypes_int": ["주중", "금", "토", "연휴", "스페셜"],
                     "daytypes_wd": ["주중", "금", "토", "일"],
                     "note": "일별 동기간(as-of) 순온북. series[key]={rn:총net, room:룸온니(46/56), rev:매출천원}. "
                             "pkg(패키지,86)=rn-room, ADR=rev*1000/rn. channels[y|m|site|seg]=[[거래처,net]…≥5%]. "
                             "key=연|월|사업장|세그|구조:요일(INT=통합/일주중편입, WD=실제입실요일)"},
            "series": out, "channels": channels}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    sz = OUT.stat().st_size / 1e6
    print(f"✓ booking_pace.json: {len(out)} 계열 · 거래처 {len(channels)} 스코프 · {sz:.1f}MB")
    # 검증: 천안 2026-01 OTA — 통합 전체/룸온니/매출 + 거래처
    base = "2026|1|소노벨 천안|OTA"
    def wkvals(d, w):
        return d[w * 7 - OFF_MIN] if d else None
    intju = out.get(f"{base}|INT:주중")
    if intju:
        rn = intju["rn"]; wkr = intju["wkr"]; wkv = intju["wkv"]
        d0w = WEEKS.index(0)              # D0 주차 위치
        rn_d0 = rn[-OFF_MIN]; fin = rn[OFF_MAX - OFF_MIN]
        adr = round(wkv[d0w] * 1000 / rn_d0) if rn_d0 else 0
        print(f"천안 26-01 OTA 통합:주중 D0 rn={rn_d0} 룸온니={wkr[d0w]} pkg={rn_d0-wkr[d0w]} "
              f"ADR={adr:,} finalRN={fin} (D0는 final의 {round(rn_d0/fin*100) if fin else 0}%)")
    print("천안 26-01 OTA 거래처(≥5%):", channels.get("2026|1|소노벨 천안|OTA"))
    print("천안 26-01 GOTA 거래처(≥5%):", channels.get("2026|1|소노벨 천안|GOTA"))

if __name__ == "__main__":
    main()
