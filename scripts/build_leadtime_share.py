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
MONTHS = list(range(1, 11))
DAYTYPES = ["주중", "금", "토", "일", "연휴", "스페셜"]
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

def _daytype(season, ci):
    s = season or ""
    if "연휴" in s: return "연휴"
    if "최성수기" in s: return "스페셜"
    if "금요일" in s: return "금"
    if "토요일" in s: return "토"
    if "주말" in s: return "일"
    if "주중" in s: return "주중"
    if ci:
        return {4: "금", 5: "토", 6: "일"}.get(ci.weekday(), "주중")
    return "주중"

def _pdate(s):
    s = (s or "").strip()
    if not re.fullmatch(r"\d{8}", s): return None
    try: return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except ValueError: return None

def _num(s):
    s = (s or "").strip().replace(",", "")
    try: return float(s) if s else 0.0
    except ValueError: return 0.0

# acc[key] = [resv_rn, resv_lt_rn, resv_rate_rn, canc_rn, canc_lt_rn]
acc = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, 0.0])

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
    if min(i_code, i_biz, i_first, i_ci, i_nights, i_rooms) < 0:
        print("  ! 필수열 누락:", fp.name); return 0
    seen = set(); n = 0
    for ln in fh:
        p = ln.rstrip("\n").split(";")
        if len(p) <= max(i_code, i_biz, i_season, i_first, i_ci, i_nights, i_rooms): continue
        seg = _seg(p[i_code])
        if not seg: continue
        site = _site(p[i_biz])
        if not site: continue
        ci = _pdate(p[i_ci])
        if not ci or ci.year not in YEARS or ci.month not in MONTHS: continue
        h = hash(ln)
        if h in seen: continue
        seen.add(h)
        rn = _num(p[i_nights]) * _num(p[i_rooms])
        if rn <= 0: continue
        fd = _pdate(p[i_first])
        lt = max((ci - fd).days, 0) if fd else 0
        dt = _daytype(p[i_season] if i_season >= 0 else "", ci)
        rate = _num(p[i_rate]) if i_rate >= 0 else 0.0
        cell = acc[(ci.year, ci.month, site, seg, dt)]
        if is_cancel: cell[3] += rn; cell[4] += lt * rn
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
    # 재전송 예약(27/43)은 이미 취소분 제외된 최종 온북 → 예약만 사용(취소 재차감 금지)
    c = acc.get((y, m, site, seg, dt))
    if not c: return 0.0, 0.0, 0.0
    return c[0], c[1], c[2]

def main():
    for y in YEARS:
        resv, _canc = select_files(str(RAW / str(y)))
        print(f"[{y}] 예약(최종 온북) {len(resv)} 파일")
        for fp in resv: print(f"   예약 {process(fp, False):>9,}행  {os.path.basename(fp)[:50]}")

    sites = sorted(ROOMS.keys(), key=lambda s: -ROOMS[s])
    def dt_nights(y, m, dt):
        dim = calendar.monthrange(y, m)[1]
        wd = {"금": 4, "토": 5, "일": 6}
        if dt in wd: return sum(1 for d in range(1, dim+1) if date(y, m, d).weekday() == wd[dt])
        if dt == "주중": return sum(1 for d in range(1, dim+1) if date(y, m, d).weekday() < 4)
        return dim  # 연휴/스페셜/ALL fallback

    data = {"meta": {"company_rooms": COMPANY_ROOMS, "years": YEARS, "months": MONTHS,
                     "daytypes": DAYTYPES, "segs": SEGS, "rooms": ROOMS,
                     "weekend": ["금", "토", "일"],
                     "note": "net=예약−취소, RN=박수×객실수, 입실월 귀속, 비중=RN/(객실수×월일수), 리드타임=입실−최초입력(net가중)"},
            "rows": []}

    scope_sites = sites + ["전사"]
    for y in YEARS:
        for m in MONTHS:
            dim = calendar.monthrange(y, m)[1]
            for site in scope_sites:
                rooms = COMPANY_ROOMS if site == "전사" else ROOMS[site]
                av_mon = rooms * dim
                site_list = sites if site == "전사" else [site]
                for seg in SEG_OUT:
                    seg_list = SEG_GROUPS[seg]
                    for dt in DAYTYPES + ["ALL"]:
                        dt_list = DAYTYPES if dt == "ALL" else [dt]
                        rn = ltw = ratew = 0.0
                        for s in site_list:
                            for sg in seg_list:
                                for d in dt_list:
                                    a, b, rt = net(y, m, s, sg, d); rn += a; ltw += b; ratew += rt
                        lead = round(ltw / rn, 1) if rn > 0 else None
                        adr = round(ratew / rn) if rn > 0 else None
                        av_dt = av_mon if dt == "ALL" else rooms * dt_nights(y, m, dt)
                        data["rows"].append({"year": y, "month": m, "site": site, "seg": seg,
                                             "daytype": dt, "rn": round(rn), "lead": lead, "adr": adr,
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

    # 주말(토) 기회 랭킹: 2026 토요일 OTA+GOTA, 사업장별 비중·리드타임
    print("\n── [2026·토요일·OTA+GOTA] 블록 기회 랭킹 (비중↓리드 = 확대 유리) ──")
    rowmap = {(r["site"], r["seg"], r["daytype"]): r for r in data["rows"]
              if r["year"]==2026 and r["month"]==0}  # placeholder
    agg = {}
    for site in sites:
        rn = ltw = av = 0.0
        for m in MONTHS:
            a, b = 0.0, 0.0
            for sg in ONLINE:
                x, y2, _ = net(2026, m, site, sg, "토"); a += x; b += y2
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
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    except ImportError:
        print("! openpyxl 없음 → xlsx 생략"); return
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    thin = Side(style="thin", color="D9D9D9"); bd = Border(thin, thin, thin, thin)
    hf = PatternFill("solid", fgColor="305496"); hfont = Font(bold=True, color="FFFFFF", name="맑은 고딕")
    idx = {(r["year"], r["month"], r["site"], r["seg"], r["daytype"]): r for r in data["rows"]}
    def mk(metric, title, fmt):
        ws = wb.create_sheet(title)
        ws.append(["연도", "월", "사업장", "세그먼트"] + DAYTYPES + ["전체"])
        for c in ws[1]:
            c.fill = hf; c.font = hfont; c.border = bd
            c.alignment = Alignment("center", "center")
        segname = {"ONLINE": "OTA+GOTA(온라인)", "ALL": "전체(+IB)", "OTA": "OTA", "GOTA": "GOTA", "IB": "IB(인바운드)"}
        for y in YEARS:
            for site in ["전사"] + sites:
                for seg in SEG_OUT:
                    sn = segname[seg]
                    for m in MONTHS:
                        row = [y, f"{m}월", site, sn]
                        for dt in DAYTYPES + ["ALL"]:
                            r = idx.get((y, m, site, seg, dt), {})
                            v = r.get(metric)
                            row.append(v if v is not None else "")
                        ws.append(row)
        for rr in ws.iter_rows(min_row=2):
            for c in rr:
                c.border = bd; c.alignment = Alignment("center")
                if c.column >= 5 and isinstance(c.value, (int, float)): c.number_format = fmt
        for i, w in enumerate([7, 6, 22, 11] + [10]*7, 1):
            ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = w
        ws.freeze_panes = "E2"
    mk("rn", "RN(실적)", "#,##0")
    mk("share", "가용비중", "0.0%")
    mk("lead", "리드타임(일)", "0.0")
    OUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    wb.save(OUT_XLSX)
    print("✓ XLSX:", OUT_XLSX)

if __name__ == "__main__":
    main()
