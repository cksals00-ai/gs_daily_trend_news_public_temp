#!/usr/bin/env python3
"""
build_leadtime_share.py — OTA/GOTA 주말블록 사전배정 의사결정 분석 (2024~2026 · 1~10월)

  목적: 회사가 주말을 '회원 선점(선open) → 미소진분 GS 이관' 정책을 쓸 때,
        어떤 영업장·요일(day-type)에 블록을 더/덜 받아야 이득인지 판단.
        핵심 신호 = (GS 비중↑ = 수요 강함) × (리드타임↓ = 빨리/확실히 소진).

  분석축: 연도(24·25·26) × 월(1~10) × 사업장 × 세그(OTA·GOTA·합계) × day-type
  day-type: 금 / 토 / 일 / 주중 / 연휴 / 스페셜  (시즌명 기준, 공란은 입실요일 fallback)
  지표:
    · net RN  = 예약(27·43) − 취소(28·44),  RN = 박수 × 객실수,  입실일자 연·월 귀속
    · 리드타임 = (입실일자 − 최초입력일자) 일,  net 가중 평균
    · 비중     = net RN / (사업장 객실수 × 월 일수)    [월 가용 대비 점유기여]
    · 객실수   = Daily Booking Report(2026.11.03) 사업장별 객실수 [전 연도 고정]
  세그: 변경예약집계코드/예약집계코드  53·72=OTA, A4·A5=GOTA  (classify_segment 준거)
  파일: 재전송만 사용(스냅샷 제외) → 연도별 비중복 완전셋
  출력: docs/data/leadtime_share.json  +  docs/data/gs_leadtime.xlsx
"""
import os, re, glob, json, calendar
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw_db"
OUT_JSON = ROOT / "docs" / "data" / "leadtime_share.json"
OUT_XLSX = ROOT / "docs" / "data" / "gs_leadtime.xlsx"

YEARS = [2024, 2025, 2026]
MONTHS = list(range(1, 13))                       # 마감연도(2024·2025)는 11·12월 포함
MONTHS_BY_YEAR = {2024: list(range(1, 13)), 2025: list(range(1, 13)), 2026: list(range(1, 11))}
def mly(y): return MONTHS_BY_YEAR.get(y, MONTHS)
DAYTYPES = ["주중", "금", "토", "연휴", "스페셜"]          # 통합 구조 전체 버킷
DAYTYPES_INT = ["주중", "금", "토", "연휴", "스페셜"]       # 통합 구조(일→주중)
DAYTYPES_WD = ["주중", "금", "토", "일"]                    # 요일 구조(실제 입실요일)
SEGS = ["OTA", "GOTA", "IB"]          # 저장 단위 (IB=인바운드, 참고용)
ONLINE = ["OTA", "GOTA"]              # 메인 = 온라인 채널
SEG_OUT = ["ONLINE", "OTA", "GOTA", "IB", "ALL"]
SEG_GROUPS = {"OTA": ["OTA"], "GOTA": ["GOTA"], "IB": ["IB"],
              "ONLINE": ["OTA", "GOTA"], "ALL": ["OTA", "GOTA", "IB"]}
ENC = ["cp949", "euc-kr", "utf-8"]

# 변경사업장명(번호 제거) → (표시 사업장, 객실수[PDF 2026.11.03])
#  비발디 소노문(D호텔)은 소노벨 비발디파크로 합산(동일 단지·블록 운영)
SITE_ROOMS = {
    "소노캄 비발디파크": ("소노캄 비발디파크", 363),
    "소노벨 비발디파크": ("소노벨 비발디파크", 1399),
    "소노문 비발디파크": ("소노벨 비발디파크", 1399),
    "소노펫 비발디파크": ("소노펫 비발디파크", 157),
    "소노펠리체 비발디파크": ("소노펠리체 비발디파크", 500),
    "소노펠리체 빌리지 비발디파크": ("소노펠리체 빌리지 비발디파크", 219),
    "소노휴 양평": ("소노벨 양평", 193),
    "델피노": ("델피노", 1256),
    "쏠비치 양양": ("쏠비치 양양", 497),
    "쏠비치 삼척": ("쏠비치 삼척", 707),
    "소노문 단양": ("소노벨 단양", 843),
    "소노벨 경주": ("소노캄 경주", 417),
    "소노캄 경주": ("소노캄 경주", 417),
    "소노벨 청송": ("소노벨 청송", 320),
    "소노벨 천안": ("소노벨 천안", 450),
    "소노벨 변산": ("소노벨 변산", 502),
    "소노캄 여수": ("소노캄 여수", 309),
    "소노캄 거제": ("소노캄 거제", 508),
    "쏠비치 진도": ("쏠비치 진도", 570),
    "소노벨 제주": ("소노벨 제주", 396),
    "소노캄 제주": ("소노캄 제주", 405),
    "소노캄 고양": ("소노캄 고양", 824),
    "소노문 해운대": ("소노문 해운대", 235),
    "쏠비치 남해": ("쏠비치 남해", 449),
    "르네블루": ("르네블루", 87),
}
# 표시 사업장 → 객실수 (중복 제거)
ROOMS = {}
for _k, (_disp, _rm) in SITE_ROOMS.items():
    ROOMS[_disp] = _rm
COMPANY_ROOMS = sum(ROOMS.values())

def _open(fp):
    for e in ENC:
        try:
            fh = open(fp, encoding=e); fh.readline(); fh.seek(0); return fh
        except Exception:
            continue
    return None

def _seg(code):
    c = (code or "").strip()
    if c in ("A4", "A5"): return "GOTA"
    if c in ("53", "72"): return "OTA"
    return None

def _site(changed_biz):
    s = re.sub(r"^\s*\d+\.\s*", "", (changed_biz or "").strip())
    hit = SITE_ROOMS.get(s)
    return hit[0] if hit else None

def _daytype_int(season, ci):
    # 통합 구조 — 연휴·스페셜은 요금시즌, 금/토/주중은 실제 입실요일.
    #  (시즌명 '주말'은 토·일을 뭉뚱그려 토요일을 주중에 흘리는 오류가 있어 입실요일로 분해)
    #  버킷: 주중(월~목+일, 일요일 주중편입) / 금 / 토 / 연휴 / 스페셜
    s = season or ""
    if "연휴" in s: return "연휴"
    if "최성수기" in s: return "스페셜"
    if ci:
        return {4: "금", 5: "토"}.get(ci.weekday(), "주중")  # 금=금요일, 토=토요일, 월~목·일=주중
    # 입실일자 파싱 불가 시에만 시즌명 토큰 폴백
    if "금요일" in s: return "금"
    if "토요일" in s: return "토"
    return "주중"

def _daytype_wd(ci):
    # 요일 구조 — 실제 입실일자 요일: 월~목=주중, 금/토/일. 연휴/스페셜 구분 없음.
    if ci:
        return {4: "금", 5: "토", 6: "일"}.get(ci.weekday(), "주중")
    return "주중"

# 하위호환: 기존 임포트용 — 통합 구조를 기본으로 노출
def _daytype(season, ci):
    return _daytype_int(season, ci)

def _pdate(s):
    s = (s or "").strip()
    if not re.fullmatch(r"\d{8}", s): return None
    try: return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except ValueError: return None

def _num(s):
    s = (s or "").strip().replace(",", "")
    try: return float(s) if s else 0.0
    except ValueError: return 0.0

# acc[key] = [resv_rn, resv_lt_rn, resv_rate_rn, canc_rn, canc_lt_rn, canc_rate_rn]
acc = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
# 연휴/스페셜 실제 입실일자 집합 (사업장·월별) — av_dt(연휴/스페셜) = 객실수 × 실제 날짜수.
#   연휴·스페셜은 달력이 아니라 요금시즌 태깅이라 _daytype_int 분류 결과로 날짜를 수집한다.
#   HOLDATES[(year, month, site, '연휴'|'스페셜')] = set(입실일자 ordinal)
HOLDATES = defaultdict(set)

def process(fp, is_cancel):
    fh = _open(fp)
    if not fh:
        print("  ! 인코딩 실패:", fp.name); return 0
    hdr = [h.strip() for h in fh.readline().split(";")]
    idx = {h: i for i, h in enumerate(hdr)}
    def gi(*names):
        for n in names:
            if n in idx: return idx[n]
        return -1
    i_code = gi("변경예약집계코드", "예약집계코드")
    i_biz = gi("변경사업장명")
    i_season = gi("시즌명")
    i_first = gi("최초입력일자")
    i_ci = gi("입실일자")
    i_nights = gi("박수")
    i_rooms = gi("객실수")
    i_rate = gi("1박객실료")
    i_user = gi("이용자명")
    i_mem = gi("회원번호")
    i_rt = gi("객실타입")
    i_cancel = gi("취소일자")
    if min(i_code, i_biz, i_first, i_ci, i_nights, i_rooms) < 0:
        print("  ! 필수열 누락:", fp.name); return 0
    hi = max(i_code, i_biz, i_season, i_first, i_ci, i_nights, i_rooms, i_user, i_mem, i_rt, i_cancel)
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
        # 스냅샷 적층 중복 제거: 예약 라인 식별자(전체라인 해시 아님 — 상태필드만 다른 적층 제거)
        key = (p[i_user] if i_user >= 0 else "", p[i_mem] if i_mem >= 0 else "",
               p[i_rt] if i_rt >= 0 else "", p[i_ci], p[i_nights], p[i_rooms], p[i_code],
               p[i_first] if i_first >= 0 else "", p[i_cancel] if i_cancel >= 0 else "")
        if key in seen: continue
        seen.add(key)
        rn = _num(p[i_nights]) * _num(p[i_rooms])
        if rn <= 0: continue
        fd = _pdate(p[i_first])
        lt = max((ci - fd).days, 0) if fd else 0
        dt_int = _daytype_int(p[i_season] if i_season >= 0 else "", ci)  # 통합 구조
        dt_wd = _daytype_wd(ci)                                          # 요일 구조
        if dt_int in ("연휴", "스페셜"):   # 실제 연휴/스페셜 날짜 수집(가용 산출용)
            HOLDATES[(ci.year, ci.month, site, dt_int)].add(ci.toordinal())
        rate = _num(p[i_rate]) if i_rate >= 0 else 0.0
        # 통합 버킷 + 요일 버킷 + 전체(DB총계) 동시 적재 → 대시보드에서 구조 토글
        for bucket in (f"INT:{dt_int}", f"WD:{dt_wd}", "__ALL__"):
            cell = acc[(ci.year, ci.month, site, seg, bucket)]
            if is_cancel: cell[3] += rn; cell[4] += lt * rn; cell[5] += rate * rn
            else:         cell[0] += rn; cell[1] += lt * rn; cell[2] += rate * rn
        n += 1
    fh.close()
    return n

def select_files(year_dir):
    resv, canc = [], []
    for fp in sorted(glob.glob(os.path.join(year_dir, "*.txt"))):
        b = os.path.basename(fp)
        if not re.search(r"\(\d{8}-\d{8}\)", b):  # 재전송만, 스냅샷 제외
            continue
        t = b.split(".")[0]
        if t in ("27", "43"): resv.append(fp)
        elif t in ("28", "44"): canc.append(fp)
    return resv, canc

def net(y, m, site, seg, dt):
    # 예약(최종 온북)만 사용 — 취소 데이터 미반영(사용자 지시)
    c = acc.get((y, m, site, seg, dt))
    if not c: return 0.0, 0.0, 0.0
    return c[0], c[1], c[2]

def main():
    for y in YEARS:
        resv, _canc = select_files(str(RAW / str(y)))
        print(f"[{y}] 예약 {len(resv)} 파일 (취소 미반영)")
        for fp in resv: print(f"   예약 {process(fp, False):>9,}행  {os.path.basename(fp)[:46]}")

    sites = sorted(ROOMS.keys(), key=lambda s: -ROOMS[s])
    def dt_nights(y, m, dt):
        dim = calendar.monthrange(y, m)[1]
        wd = {"금": 4, "토": 5, "일": 6}
        if dt in wd: return sum(1 for d in range(1, dim+1) if date(y, m, d).weekday() == wd[dt])
        if dt == "주중": return sum(1 for d in range(1, dim+1) if date(y, m, d).weekday() < 4)
        return dim  # ALL fallback (연휴/스페셜은 avdt_for에서 실제 날짜수로 산출)

    _intc_cache = {}
    def _int_counts(y, m, site):
        """통합구조(INT) day-type별 '날짜수'를 사업장 단위로 분할.
          연휴/스페셜 = 실제 요금시즌 태깅 날짜(HOLDATES, 연휴 우선), 나머지 날짜는
          입실요일로 금/토/주중(월~목+일) 배분 → 5버킷 날짜수 합 = 월 전체 일수(정합)."""
        ck = (y, m, site)
        if ck in _intc_cache: return _intc_cache[ck]
        dim = calendar.monthrange(y, m)[1]
        hol = HOLDATES.get((y, m, site, "연휴"), set())
        spc = HOLDATES.get((y, m, site, "스페셜"), set()) - hol   # 연휴 우선
        cnt = {"연휴": len(hol), "스페셜": len(spc), "금": 0, "토": 0, "주중": 0}
        for d in range(1, dim + 1):
            o = date(y, m, d).toordinal()
            if o in hol or o in spc: continue
            wd = date(y, m, d).weekday()
            cnt["금" if wd == 4 else "토" if wd == 5 else "주중"] += 1   # 월~목·일 = 주중
        _intc_cache[ck] = cnt
        return cnt

    def avdt_for(y, m, site, basis, dt, rooms, dim):
        """day-type별 가용 객실·박 = 객실수 × 해당 day-type 날짜수.
          · ALL           → 월 전체 (rooms × dim)
          · WD 금/토/일/주중 → 순수 달력 날짜수 (주중=월~목)
          · INT 5버킷      → 연휴/스페셜=실제 태깅 날짜, 금/토/주중=나머지 달력(일요일 주중 포함).
          전사는 연휴 날짜가 사업장마다 달라 사업장별 av_dt 합산."""
        if dt == "ALL":
            return rooms * dim
        if site == "전사":   # 사업장별 av_dt 합산 (요일계도 연휴 carve-out이 사업장별로 달라 동일 처리)
            return sum(avdt_for(y, m, s, basis, dt, ROOMS[s], dim) for s in sites)
        if basis == "INT":
            return rooms * _int_counts(y, m, site).get(dt, 0)
        return rooms * dt_nights(y, m, dt)   # WD = 순수 달력

    data = {"meta": {"company_rooms": COMPANY_ROOMS, "years": YEARS, "months": MONTHS,
                     "months_by_year": {str(k): v for k, v in MONTHS_BY_YEAR.items()},
                     "daytypes": DAYTYPES, "daytypes_int": DAYTYPES_INT, "daytypes_wd": DAYTYPES_WD,
                     "bases": ["INT", "WD"], "basis_default": "INT",
                     "basis_label": {"INT": "통합 구조", "WD": "요일 구조"},
                     "segs": SEGS, "rooms": ROOMS, "weekend": ["금", "토"],
                     "note": "net=예약(취소 미반영), RN=박수×객실수, 입실월 귀속, 비중=RN/(객실수×월일수), 리드타임=입실−최초입력(net가중). "
                             "day-type 구조 2종: INT=통합(요금시즌, 일요일 주중편입, 주중/금/토/연휴/스페셜) · WD=요일(실제 입실요일, 주중/금/토/일)"},
            "rows": []}

    # 구조(basis)별 day-type 집합 — ALL(전체 DB총계)은 구조 무관(공용)
    BASIS_DTS = {"INT": DAYTYPES_INT, "WD": DAYTYPES_WD}

    scope_sites = sites + ["전사"]
    for y in YEARS:
        for m in mly(y):
            dim = calendar.monthrange(y, m)[1]
            for site in scope_sites:
                rooms = COMPANY_ROOMS if site == "전사" else ROOMS[site]
                av_mon = rooms * dim
                site_list = sites if site == "전사" else [site]
                for seg in SEG_OUT:
                    seg_list = SEG_GROUPS[seg]
                    # (basis, daytype, bucketkey) 목록 — ALL 1개 + WD 4개 + SZ 6개
                    combos = [("ALL", "ALL", "__ALL__")]
                    for basis, dts in BASIS_DTS.items():
                        for dt in dts:
                            combos.append((basis, dt, f"{basis}:{dt}"))
                    for basis, dt, bucket in combos:
                        rn = ltw = ratew = 0.0
                        for s in site_list:
                            for sg in seg_list:
                                a, b, rt = net(y, m, s, sg, bucket); rn += a; ltw += b; ratew += rt
                        lead = round(ltw / rn, 1) if rn > 0 else None
                        adr = round(ratew / rn) if rn > 0 else None
                        av_dt = avdt_for(y, m, site, basis, dt, rooms, dim)
                        data["rows"].append({"year": y, "month": m, "site": site, "seg": seg,
                                             "basis": basis, "daytype": dt,
                                             "rn": round(rn), "lead": lead, "adr": adr,
                                             "rooms": rooms, "av_dt": av_dt, "av_mon": av_mon,
                                             "share": round(rn/av_mon, 4) if av_mon else 0.0})
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print("✓ JSON:", OUT_JSON, f"({len(data['rows'])} rows)")

    # ── 검증 + 주말 블록 기회 인사이트 ──
    print(f"\n총 객실수(분모) = {COMPANY_ROOMS:,}실  ·  사업장 {len(sites)}개")
    print("\n── 연도별 OTA/GOTA net RN (전사·1~10월 합) ──")
    for y in YEARS:
        line = f"  {y}"
        for seg in ["OTA", "GOTA", "IB", "ONLINE"]:
            tot = sum(r["rn"] for r in data["rows"] if r["year"]==y and r["site"]=="전사" and r["seg"]==seg and r["daytype"]=="ALL")
            line += f"  {seg}={tot:>9,}"
        print(line)

    # 주말(토) 기회 랭킹: 2026 토요일(실제 입실요일) OTA+GOTA, 사업장별 비중·리드타임
    print("\n── [2026·토요일(요일기준)·OTA+GOTA] 블록 기회 랭킹 (비중↓리드 = 확대 유리) ──")
    agg = {}
    for site in sites:
        rn = ltw = av = 0.0
        for m in mly(2026):
            a, b = 0.0, 0.0
            for sg in ONLINE:
                x, y2, _ = net(2026, m, site, sg, "WD:토"); a += x; b += y2
            rn += a; ltw += b
            av += ROOMS[site] * sum(1 for d in range(1, calendar.monthrange(2026, m)[1]+1) if date(2026, m, d).weekday()==5)
        lead = (ltw/rn) if rn>0 else None
        occ = (rn/av) if av else 0.0
        agg[site] = (rn, lead, occ)
    for site, (rn, lead, occ) in sorted(agg.items(), key=lambda kv: -(kv[1][2])):
        ls = f"{lead:4.0f}일" if lead is not None else "  - "
        print(f"  {site:20} 토점유 {occ*100:5.1f}%  리드 {ls}  RN {int(rn):>7,}")

    build_xlsx(data, sites)

def build_xlsx(data, sites):
    """제공 템플릿(data/templates/leadtime_xlsx_template.xlsx) 양식 그대로,
       통합구조 day-type별 시트(전체·주중·금·토·연휴·스페셜)에 전사·ONLINE 데이터만 채운다."""
    try:
        import openpyxl
        from openpyxl.utils import get_column_letter as L
    except ImportError:
        print("! openpyxl 없음 → xlsx 생략"); return
    TMPL = ROOT / "data" / "templates" / "leadtime_xlsx_template.xlsx"
    if not TMPL.exists():
        print("! xlsx 템플릿 없음 → 생략:", TMPL); return
    wb = openpyxl.load_workbook(TMPL)
    src = wb["DB"]
    idx = {(r["year"], r["month"], r["site"], r["seg"], r.get("basis"), r["daytype"]): r for r in data["rows"]}
    SITE, SEG = "전사", "ONLINE"
    SHEETS = [("전체", "ALL", "ALL"), ("주중", "INT", "주중"), ("금", "INT", "금"),
              ("토", "INT", "토"), ("연휴", "INT", "연휴"), ("스페셜", "INT", "스페셜")]
    YEAR_COL = {2026: 3, 2025: 8, 2024: 13}      # RNS 시작열: C · H · M

    def fill(ws, basis, dtkey):
        # 데이터 영역 선-초기화(템플릿 1월 샘플값·ADR수식 제거 → 빈 월 #DIV/0! 방지)
        for rr in range(6, 18):
            for c in range(3, 18):                # C..Q
                ws.cell(rr, c).value = None
            ws.cell(rr, 20).value = None          # T(가용)
        for y in (2024, 2025, 2026):
            c0 = YEAR_COL[y]
            tot_av = 0.0
            for m in range(1, 13):
                rr = 5 + m                        # 1월=6 … 12월=17
                r = idx.get((y, m, SITE, SEG, basis, dtkey))
                if not r or not r.get("rn"):
                    continue
                rn = r["rn"]; adr = r.get("adr") or 0; av = r.get("av_dt") or r.get("av_mon") or 0
                ws.cell(rr, c0).value = round(rn)                                   # RNS
                ws.cell(rr, c0+1).value = f"={L(c0+2)}{rr}/{L(c0)}{rr}*1000"        # ADR=REV/RNS*1000
                ws.cell(rr, c0+2).value = round(rn * adr / 1e6)                     # REV(백만원)
                cp = ws.cell(rr, c0+3); cp.value = (rn/av if av else 0); cp.number_format = "0.0%"  # 점유율
                ws.cell(rr, c0+4).value = (f"{round(r['lead'])}일" if r.get("lead") is not None else "")  # 리드타임
                if y == 2026:
                    ws.cell(rr, 20).value = round(av); tot_av += av                 # 가용(T열, 2026 기준)
            # Total 행: ADR 수식 + 점유율(값)
            ws.cell(5, c0+1).value = f"={L(c0+2)}5/{L(c0)}5*1000"
            trn = sum((idx.get((y, m, SITE, SEG, basis, dtkey)) or {}).get("rn", 0) for m in range(1, 13))
            tav = sum((idx.get((y, m, SITE, SEG, basis, dtkey)) or {}).get("av_dt", 0) for m in range(1, 13))
            if tav:
                cp = ws.cell(5, c0+3); cp.value = trn/tav; cp.number_format = "0.0%"
            if y == 2026 and tot_av:
                ws.cell(5, 20).value = round(tot_av)

    made = []
    for name, basis, dtkey in SHEETS:
        ws = wb.copy_worksheet(src)
        ws.title = name
        ws["B2"].value = f"- {name}"
        fill(ws, basis, dtkey)
        made.append(name)
    wb.remove(src)
    OUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    wb.save(OUT_XLSX)
    # 검증
    chk = idx.get((2026, 1, "전사", "ONLINE", "INT", "토"))
    print("✓ XLSX(템플릿 양식):", OUT_XLSX.name, "· 시트", made,
          "· 검증 토 2026-01 RNS=", round(chk["rn"]) if chk else "N/A",
          "(템플릿값 13828)")

if __name__ == "__main__":
    main()
