#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_pickup_678.py — 최근 7일 픽업(투숙 6/7/8월) 호조 분석 + 엑셀 출력

픽업 정의(하우스 컨벤션, parse_raw_db.pickup_daily_agg/cancel_daily 동일):
  신규(gross add, +) = 27/43 + 28/44 행 중 최초입력일자 ∈ 윈도우, 투숙월 ∈ 대상월
  취소(-)            = 28/44 행 중 취소일자  ∈ 윈도우, 투숙월 ∈ 대상월
  Net 픽업 = 신규 − 취소
정제: 행 중복제거(라인해시), 거래처(예약자명==회원명, 단 Inbound 58 예외) 제거, 매출조정 제거.
RN = 객실수(없으면 1), REV(원) = 1박객실료×객실수÷1.1 (VAT제외/공급가). 엑셀은 백만원.

윈도우:
  CUR : 2026-06-18~06-24 (투숙 202606/07/08)   ※06-24는 스냅샷 07:45 컷 → 부분일
  WOW : 2026-06-11~06-17
  MOM : 2026-05-18~05-24
  YOY : 2025-06-18~06-24 (투숙 202506/07/08)
"""
import os, sys, json, math
from pathlib import Path
from collections import defaultdict
from datetime import datetime

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
RAW = PROJECT_DIR / "data" / "raw_db"
sys.path.insert(0, str(SCRIPT_DIR))

from parse_raw_db import (classify_segment, extract_channel, normalize_property,
                          classify_product_category)
from parse_package_trend import normalize_series, classify_v4_ctx

# ─────────────────────────── 설정 ───────────────────────────
TODAY = "20260624"
SM_26 = {"202606", "202607", "202608"}
SM_25 = {"202506", "202507", "202508"}
WINDOWS = {  # name: (lo, hi, staymonths, year)  — 6완전일 (06-24 부분일 제외, 동일길이)
    "CUR": ("20260618", "20260623", SM_26, "2026"),
    "WOW": ("20260612", "20260617", SM_26, "2026"),
    "MOM": ("20260518", "20260523", SM_26, "2026"),
    "YOY": ("20250618", "20250623", SM_25, "2025"),
}
WIN_LABEL = {
    "CUR": "최근6일(06.18~06.23)",
    "WOW": "직전6일(06.12~06.17)",
    "MOM": "전월동기(05.18~05.23)",
    "YOY": "전년동기(25.06.18~06.23)",
}

FILES = {
    "2026": [
        "2026/27.온라인영업팀(대매점, OTA,패키지) 예약자료_20260624_생성시간(202606240745).txt",
        "2026/28.온라인영업팀(대매점,OTA,패키지) 취소자료_20260624_생성시간(202606240747).txt",
        "2026/43.[57,87]온라인영업팀(대매점,OTA,패키지) 예약자료_20260624_생성시간(202606240752).txt",
        "2026/44.[57,87]온라인영업팀(대매점,OTA,패키지) 취소자료_20260624_생성시간(202606240752).txt",
    ],
    "2025": [
        "2025/27.온라인영업팀(대매점, OTA,패키지) 예약자료_재전송(20250101-20251231).txt",
        "2025/28.온라인영업팀(대매점,OTA,패키지) 취소자료_재전송(20250401-20250630).txt",
        "2025/28.온라인영업팀(대매점,OTA,패키지) 취소자료_재전송(20250701-20250930).txt",
        "2025/43.[57,87]온라인영업팀(대매점,OTA,패키지) 예약자료_재전송(20250101-20251231).txt",
        "2025/44.[57,87]온라인영업팀(대매점,OTA,패키지) 취소자료_재전송(20250101-20251231).txt",
    ],
}

SEG3 = ("OTA", "G-OTA", "Inbound")

# ─────────────────── 기획전(비발디 5월전략) 코드 ───────────────────
def load_campaign_codes():
    codes = set()
    code_prop = {}
    cd = json.load(open(PROJECT_DIR / "docs/data/campaign_data.json", encoding="utf-8"))
    k2c = cd.get("key_to_codes", {})
    for k in ("196", "198", "20", "134"):
        for c in k2c.get(k, []):
            codes.add(c)
    for ev in cd.get("events", []):
        for c in (ev.get("package_codes") or []):
            codes.add(c)
    try:
        c86 = json.load(open(PROJECT_DIR / "docs/data/campaign86_data.json", encoding="utf-8"))
        for prop, lst in c86.get("pkg_codes_by_prop", {}).items():
            for c in lst:
                codes.add(c); code_prop[c] = prop
    except Exception:
        pass
    return codes, code_prop

CAMP_CODES, CAMP_CODE_PROP = load_campaign_codes()
CAMP_NAME = "비발디 5월전략 프로모션"

# ─────────────────── OTA 연박 체인 (스코프 한정) ───────────────────
def build_chain(files, staymonths):
    """주어진 파일에서 86xx OTA(53/72) 1박행의 연속숙박 체인 식별 (parse_package_trend.ota_chain_nights 축약)."""
    groups = defaultdict(list)
    for rel in files:
        fp = RAW / rel
        for enc in ("cp949", "euc-kr", "utf-8"):
            try:
                fh = open(fp, encoding=enc)
                hdr = fh.readline().strip().split(";")
                idx = {h.strip(): i for i, h in enumerate(hdr)}
                im, ic = idx.get("회원번호", 5), idx.get("변경예약집계코드", 12)
                iu, ip = idx.get("이용자명", 7), idx.get("영업장명", 3)
                irt, inm = idx.get("객실타입", 8), idx.get("회원명", 6)
                inight = idx.get("박수", 27)
                iin, iout = idx.get("입실일자", 33), idx.get("퇴실일자", 34)
                isd = idx.get("판매일자", 1)
                need = max(im, ic, iu, ip, irt, inm, inight, iin, iout, isd)
                for line in fh:
                    p = line.split(";")
                    if len(p) <= need:
                        continue
                    if not p[im].strip().startswith("86"):
                        continue
                    if p[ic].strip() not in ("53", "72"):
                        continue
                    if p[inight].strip() != "1":
                        continue
                    if p[isd].strip()[:6] not in staymonths:
                        continue
                    ci, co = p[iin].strip()[:8], p[iout].strip()[:8]
                    if len(ci) != 8 or len(co) != 8:
                        continue
                    gid = (p[iu].strip(), p[ip].strip(), p[irt].strip(), p[inm].strip())
                    groups[gid].append((ci, co))
                break
            except UnicodeDecodeError:
                continue
    chain = set()
    for gid, items in groups.items():
        if len(items) < 2:
            continue
        ins = {ci for ci, _ in items}; outs = {co for _, co in items}
        for ci, co in set(items):
            if co in ins or ci in outs:
                chain.add((gid[0], gid[1], gid[2], gid[3], ci))
    return chain

# ─────────────────────────── 파싱 ───────────────────────────
ROWS = []           # 픽업 이벤트(부호 포함)
DAILY_DIAG = defaultdict(lambda: defaultdict(int))   # (win, kind) -> day -> rn  (검증용)

def parse_file(rel, year, chain):
    fp = RAW / rel
    base = os.path.basename(rel)
    ft = base[:2]
    is_cancel = ft in ("28", "44")
    wins = [(w, *WINDOWS[w]) for w in WINDOWS if WINDOWS[w][3] == year]
    for enc in ("cp949", "euc-kr", "utf-8"):
        try:
            fh = open(fp, encoding=enc)
            hdr = fh.readline().strip().split(";")
            idx = {h.strip(): i for i, h in enumerate(hdr)}
            i_prop = idx.get("영업장명", -1); i_cprop = idx.get("변경사업장명", -1)
            i_sell = idx.get("판매일자", -1); i_check = idx.get("입실일자", -1)
            i_cnum = idx.get("변경예약집계코드", idx.get("예약집계코드", -1))
            i_cname = idx.get("변경예약집계코드명", idx.get("예약집계명", -1))
            i_agent = idx.get("AGENT명", -1)
            i_rooms = idx.get("객실수", -1); i_rate = idx.get("1박객실료", -1)
            i_entry = idx.get("최초입력일자", -1); i_cancel = idx.get("취소일자", -1)
            i_mnum = idx.get("회원번호", -1); i_mname = idx.get("회원명", -1)
            i_user = idx.get("이용자명", -1); i_nights = idx.get("박수", -1)
            i_pkgcls = idx.get("패키지분류코드명", -1); i_rt = idx.get("객실타입", -1)
            seen = set()
            for line in fh:
                ls = line.rstrip("\n\r")
                h = hash(ls)
                if h in seen:
                    continue
                seen.add(h)
                p = line.split(";"); plen = len(p)
                def g(i):
                    return p[i].strip() if 0 <= i < plen else ""
                sell = g(i_sell) or g(i_check)
                if len(sell) < 6:
                    continue
                sm = sell[:6]
                # 어떤 윈도우와도 무관한 투숙월이면 스킵 (대상월만)
                if not any(sm in w[3] for w in wins):
                    continue
                cnum = g(i_cnum); cname = g(i_cname); agent = g(i_agent)
                # 거래처 제거 (Inbound 58 예외)
                mname = g(i_mname); uname = g(i_user)
                if mname and uname and mname == uname and cnum != "58":
                    continue
                if "매출조정" in mname or "매출조정" in uname:
                    continue
                rooms = int(g(i_rooms)) if g(i_rooms).isdigit() else 0
                rn = rooms if rooms > 0 else 1
                rate = int(g(i_rate)) if g(i_rate).lstrip("-").isdigit() else 0
                rev = int(rate * rn / 1.1)
                prop = normalize_property(g(i_cprop)) if g(i_cprop) else normalize_property(g(i_prop))
                seg = classify_segment(cnum, cname, agent, ft)
                ch = extract_channel(agent, cnum)
                mnum = g(i_mnum)
                # 상품 카테고리
                is_chain = False
                if chain and i_user >= 0 and i_prop >= 0 and i_rt >= 0 and i_check >= 0:
                    gid = (g(i_user), g(i_prop), g(i_rt), mname, g(i_check)[:8])
                    is_chain = gid in chain
                cat = classify_product_category(mnum, mname, g(i_nights), g(i_pkgcls), is_chain)
                if cat is None:
                    cat = "비패키지(일반)"
                is_camp = mnum in CAMP_CODES
                entry = g(i_entry)[:8]
                cancel = g(i_cancel)[:8]
                base_rec = dict(prop=prop, seg=seg, ch=ch, cat=cat, mnum=mnum,
                                is_camp=is_camp, sm=sm, rn=rn, rev=rev)
                for wname, lo, hi, months, yr in wins:
                    if sm not in months:
                        continue
                    # 신규 (gross add, +): 최초입력일자 ∈ 윈도우  (27/43 + 28/44 모두)
                    if len(entry) == 8 and lo <= entry <= hi:
                        kind = "신규-유지" if not is_cancel else "신규-기취소"
                        ROWS.append({**base_rec, "win": wname, "kind": kind,
                                     "date": entry, "sign": 1,
                                     "rn_s": rn, "rev_s": rev})
                        DAILY_DIAG[(wname, "신규")][entry] += rn
                    # 취소 (−): 취소일자 ∈ 윈도우  (28/44만)
                    if is_cancel and len(cancel) == 8 and lo <= cancel <= hi:
                        ROWS.append({**base_rec, "win": wname, "kind": "취소",
                                     "date": cancel, "sign": -1,
                                     "rn_s": -rn, "rev_s": -rev})
                        DAILY_DIAG[(wname, "취소")][cancel] += rn
            fh.close()
            return
        except UnicodeDecodeError:
            continue

def main():
    t0 = datetime.now()
    print("기획전 코드 수:", len(CAMP_CODES))
    chains = {}
    for year in ("2026", "2025"):
        sm = SM_26 if year == "2026" else SM_25
        chains[year] = build_chain(FILES[year], sm)
        print(f"{year} OTA 연박체인:", len(chains[year]))
    for year in ("2026", "2025"):
        for rel in FILES[year]:
            parse_file(rel, year, chains[year])
            print("  parsed", os.path.basename(rel)[:45])
    print(f"이벤트행 수: {len(ROWS):,}  소요 {(datetime.now()-t0).seconds}s")
    # JSON 덤프(후속 엑셀 빌더가 읽음)
    out = PROJECT_DIR / "data" / "_pickup678_rows.json"
    json.dump({"rows": ROWS,
               "daily": {f"{w}|{k}": dict(d) for (w, k), d in DAILY_DIAG.items()},
               "camp_codes": sorted(CAMP_CODES),
               "camp_code_prop": CAMP_CODE_PROP},
              open(out, "w", encoding="utf-8"), ensure_ascii=False)
    print("dump →", out)

if __name__ == "__main__":
    main()
