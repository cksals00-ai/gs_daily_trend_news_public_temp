#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_july_pickup_tracker.py — 7월 동기간 픽업 일자별 관리 엑셀 (송예건 CM 지시, 2026-06-30~)

대상: 7월 동기간 YoY 모니터링 6개 사업장 (세그 구분없이 전체 = 온라인영업팀 raw_db 전 세그)
  소노캄 비발디파크 / 소노문 단양 / 소노벨 청송 / 소노캄 여수 / 소노캄 거제 / 쏠비치 진도

데이터: data/raw_db (27/28/43/44) 라이브 스냅샷 직접 net 계산 — db_aggregated 이중계상(doubling) 회피.
픽업 정의(하우스 컨벤션, parse_raw_db.pickup_daily_agg/cancel_daily 동일):
  신규(+) = 27/43 + 28/44 행 중 최초입력일자 == 그 날 (gross add; 28/44는 기취소분 복원)
  취소(−) = 28/44 행 중 취소일자 == 그 날
  net = 신규 − 취소.  투숙월 = 판매일자[:6] == 202607(2026) / 202507(2025).
정제: 라인 중복제거, 거래처(회원명==이용자명, 단 Inbound 58 예외) 제거, 매출조정 제거.
RN = 객실수(없으면 1). 세그 구분 없음(전체 합).

동기간 온북(요약):  entry ≤ asof − cancel ≤ asof.
  asof_2026 = 스냅샷일 − 1 (마지막 완전일, 예 20260629).  asof_2025 = 동일 MMDD 전년 (20250629).
  → 전사 OTB 대시보드 todayDate(=어제) 기준과 동일. [[yoy-same-period-base-date]]

서식: GS 소노위크 실적 분석 보고 양식(대명체, 흑색 헤더+백색 텍스트, 헤어라인 테두리,
  회계 숫자서식 _-* #,##0_-, 0.0% 비중, 합계 이중테두리, 제목/번호목차/총평 박스).
날짜 하드코딩 없음: 27 라이브 스냅샷 파일명에서 기준일 자동 추출 → 매일 재실행하면 일자별 자동 연장.
산출물: ~/Desktop/7월동기간_픽업_일자별관리_YYYYMMDD.xlsx

사용: python3 scripts/build_july_pickup_tracker.py
"""
import os, re, sys, glob, json, shutil, unicodedata
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
RAW = PROJECT_DIR / "data" / "raw_db"

WINDOW_DAYS = 30  # 일별 픽업 시트: 마지막 완전일 기준 최근 N일

# 대상 6개 사업장: (raw_db 정규화명, 표시 라벨)
TARGETS = [
    ("소노캄 비발디파크", "소노캄 비발디파크"),
    ("소노문 단양",       "소노문 단양(舊 소노벨)"),
    ("소노벨 청송",       "소노벨 청송"),
    ("소노캄 여수",       "소노캄 여수"),
    ("소노캄 거제",       "소노캄 거제"),
    ("쏠비치 진도",       "쏠비치 진도"),
]
TARGET_SET = {t[0] for t in TARGETS}
LABEL = {t[0]: t[1] for t in TARGETS}

# ───────────────────────── 파일 해소 (macOS NFD/NFC) ─────────────────────────
def _nfc(s): return unicodedata.normalize("NFC", s)

def _list(year):
    d = RAW / str(year)
    return [(str(d / fn), _nfc(fn)) for fn in os.listdir(d) if fn.endswith(".txt")]

def live_files(year):
    fs = []
    for pfx in ("27", "28", "43", "44"):
        c = sorted(fp for fp, n in _list(year) if n.startswith(pfx) and "생성시간" in n)
        if c:
            fs.append(c[-1])
    return fs

def retrans_files(year):
    return sorted(fp for fp, n in _list(year) if "재전송" in n)

def snapshot_date():
    """최신 27 라이브 스냅샷 파일명에서 기준일(YYYYMMDD) 추출."""
    best = None
    for fp, n in _list(2026):
        if n.startswith("27") and "생성시간" in n:
            m = re.search(r"생성시간\((\d{8})", n)
            if m:
                best = max(best, m.group(1)) if best else m.group(1)
    if not best:
        raise SystemExit("27 라이브 스냅샷을 찾지 못했습니다.")
    return best

def _openf(fp):
    for enc in ("cp949", "euc-kr", "utf-8"):
        try:
            f = open(fp, encoding=enc); f.readline(); f.close()
            return open(fp, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise IOError(f"인코딩 실패: {fp}")

def _norm_prop(s):
    return re.sub(r"^\d+\.\s*", "", s).strip()

# ───────────────────────── 로딩 ─────────────────────────
# row = dict(prop, entry(YYYYMMDD|None), cancel(YYYYMMDD|None), rn, is_cancel)
def load_rows(files, staymon):
    rows = []
    for fp in files:
        ft = os.path.basename(fp)[:2]
        is_cancel = ft in ("28", "44")
        f = _openf(fp)
        hdr = f.readline().strip().split(";")
        idx = {h.strip(): i for i, h in enumerate(hdr)}
        ip = idx.get("영업장명", -1); icp = idx.get("변경사업장명", -1)
        isd = idx.get("판매일자", -1); ici = idx.get("입실일자", -1)
        irooms = idx.get("객실수", -1)
        ient = idx.get("최초입력일자", -1); ican = idx.get("취소일자", -1)
        imn = idx.get("회원명", -1); iun = idx.get("이용자명", -1)
        icn = idx.get("변경예약집계코드", idx.get("예약집계코드", -1))
        seen = set()
        for line in f:
            ls = line.rstrip("\n\r"); h = hash(ls)
            if h in seen:
                continue
            seen.add(h)
            p = line.split(";"); plen = len(p)
            def g(i): return p[i].strip() if 0 <= i < plen else ""
            sell = g(isd) or g(ici)
            if sell[:6] != staymon:
                continue
            prop = _norm_prop(g(icp)) if g(icp) else _norm_prop(g(ip))
            if prop not in TARGET_SET:
                continue
            mn, un, cn = g(imn), g(iun), g(icn)
            if mn and un and mn == un and cn != "58":  # 거래처 제거 (Inbound 58 예외)
                continue
            if "매출조정" in mn or "매출조정" in un:
                continue
            rooms = int(g(irooms)) if g(irooms).isdigit() else 0
            rn = rooms if rooms > 0 else 1
            ent = g(ient)[:8]; can = g(ican)[:8]
            rows.append(dict(prop=prop, rn=rn, is_cancel=is_cancel,
                             entry=ent if len(ent) == 8 else None,
                             cancel=can if (is_cancel and len(can) == 8) else None))
    return rows

# ───────────────────────── 집계 ─────────────────────────
def onbook_at(rows, cutoff):
    """동기간 net 온북: entry ≤ cutoff − cancel ≤ cutoff."""
    net = defaultdict(int)
    for r in rows:
        if r["entry"] and r["entry"] <= cutoff:
            net[r["prop"]] += r["rn"]
        if r["cancel"] and r["cancel"] <= cutoff:
            net[r["prop"]] -= r["rn"]
    return net

def daily_newcancel(rows):
    """(prop, YYYYMMDD) → 신규rn / 취소rn."""
    new = defaultdict(int); cxl = defaultdict(int)
    for r in rows:
        if r["entry"]:
            new[(r["prop"], r["entry"])] += r["rn"]
        if r["cancel"]:
            cxl[(r["prop"], r["cancel"])] += r["rn"]
    return new, cxl

# ───────────────────────── 엑셀 (소노위크 보고 양식) ─────────────────────────
def build_excel(out_path, data_date, asof26, asof25, rows26, rows25):
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    # ── 하우스 팔레트/서식 (템플릿 styles.xml에서 추출) ──
    FN_R = "대명체 보통"; FN_B = "대명체 굵게"
    WHITE = "FFFFFF"; BLACK = "000000"; RED = "C00000"; BLUE = "1F4E78"; GREEN = "375623"
    NF_NUM = r'_-* #,##0_-;\-* #,##0_-;_-* "-"_-;_-@_-'        # 회계(천단위, 0=대시)
    NF_DELTA = r'_-* +#,##0_-;_-* \-#,##0_-;_-* "-"_-;_-@_-'   # 부호표시 증감
    NF_PCT = '0.0%'

    def F(sz=10, bold=False, color=BLACK):
        return Font(name=(FN_B if bold else FN_R), size=sz, bold=bold, color=color)
    cen = Alignment(horizontal="center", vertical="center")
    cenw = Alignment(horizontal="center", vertical="center", wrap_text=True)
    rgt = Alignment(horizontal="right", vertical="center")
    lft = Alignment(horizontal="left", vertical="center")
    HAIR = Side(style="hair", color=BLACK)
    THIN = Side(style="thin", color=BLACK)
    DBL = Side(style="double", color=BLACK)
    box = Border(left=HAIR, right=HAIR, top=HAIR, bottom=HAIR)
    HFILL = PatternFill("solid", fgColor=BLACK)     # 헤더 흑색
    ZFILL = PatternFill("solid", fgColor="F2F2F2")  # 총평 박스 연회색

    def hcell(ws, r, c, v):
        cell = ws.cell(r, c, v); cell.font = F(10, color=WHITE); cell.fill = HFILL
        cell.alignment = cenw; cell.border = box
        return cell

    def d2k(s):  # 20260629 → 06/29
        return f"{s[4:6]}/{s[6:8]}"
    WD = "월화수목금토일"

    wb = openpyxl.Workbook()
    # 결정적 출력(같은 데이터 → 같은 바이트): 문서 타임스탬프를 기준일로 고정
    _fixed = datetime.strptime(data_date, "%Y%m%d")
    wb.properties.created = _fixed
    wb.properties.modified = _fixed
    wb.properties.creator = "build_july_pickup_tracker"
    wb.properties.lastModifiedBy = "build_july_pickup_tracker"

    # 윈도우 날짜축 (최근 N완전일, 과거→최근)
    hi = datetime.strptime(asof26, "%Y%m%d")
    days26 = [(hi - timedelta(days=k)).strftime("%Y%m%d") for k in range(WINDOW_DAYS)][::-1]
    def to25(d): return "2025" + d[4:]
    ordered = list(reversed(days26))  # 최근 → 과거

    new26, cxl26 = daily_newcancel(rows26)
    new25, cxl25 = daily_newcancel(rows25)
    nb26 = onbook_at(rows26, asof26); nb25 = onbook_at(rows25, asof25)
    def net_of(new, cxl, name, day): return new.get((name, day), 0) - cxl.get((name, day), 0)

    # ============================================================ 개요 ============
    ws = wb.active; ws.title = "개요"
    ws.sheet_view.showGridLines = False
    LASTC = 9  # B..I (구분+2026+2025+갭+%+상태+필요)  → 컨텐츠 폭
    ws.column_dimensions["A"].width = 1.2
    ws.column_dimensions["B"].width = 22
    for c in range(3, LASTC + 1):
        ws.column_dimensions[get_column_letter(c)].width = 11

    # 제목 (B2 병합, 14pt 중앙)
    ws.merge_cells(start_row=2, start_column=2, end_row=2, end_column=LASTC)
    t = ws.cell(2, 2, "7월 동기간 픽업 일자별 관리 보고의 건"); t.font = F(14); t.alignment = cen
    ws.row_dimensions[2].height = 24
    # 기준일 (우상단)
    dcell = ws.cell(3, LASTC, datetime.strptime(data_date, "%Y%m%d"))
    dcell.number_format = "yyyy-mm-dd"; dcell.font = F(9); dcell.alignment = rgt
    # 번호 목차
    metas = [
        f"1. 대 상 : 7월 동기간 전년대비 모니터링 6개 사업장",
        f"          (소노캄 비발디파크 · 소노문 단양 · 소노벨 청송 · 소노캄 여수 · 소노캄 거제 · 쏠비치 진도)",
        f"2. 투숙일자 : 2026-07 (7월 투숙)",
        f"3. 기 준 일 : {d2k(asof26)}(마지막 완전일) vs 전년 {d2k(asof25)}  ·  스냅샷 {d2k(data_date)}",
        f"4. 데이터 : raw_db(온라인영업팀) net 직접 산출 · 세그 구분없이 전체 RN · 이중계상 없음",
        f"5. 총 평",
    ]
    r = 4
    for m in metas:
        ws.cell(r, 2, m).font = F(10); r += 1
    # 총평 박스
    tot26 = sum(nb26.get(n, 0) for n, _ in TARGETS); tot25 = sum(nb25.get(n, 0) for n, _ in TARGETS)
    gtot = tot26 - tot25
    behind = [LABEL[n] for n, _ in TARGETS if nb26.get(n, 0) - nb25.get(n, 0) < 0]
    ahead = [LABEL[n] for n, _ in TARGETS if nb26.get(n, 0) - nb25.get(n, 0) >= 0]
    cmt = (f'  6개 합계 동기간 온북 {tot26:,}실 (전년 {tot25:,}실, {"+" if gtot>=0 else "−"}{abs(gtot):,}실 / '
           f'{gtot/tot25*100:+.1f}%).  전년미달 {len(behind)}개({", ".join(behind) if behind else "없음"}), '
           f'전년초과 {len(ahead)}개({", ".join(ahead) if ahead else "없음"}).\n'
           f'  목표 = 세그 구분없이 전체 RN을 전년 동기간 초과로 유지 · 일별 픽업(신규−취소) net 모니터링으로 갭 관리.')
    ws.merge_cells(start_row=r, start_column=2, end_row=r + 1, end_column=LASTC)
    cc = ws.cell(r, 2, cmt); cc.font = F(10, color=RED); cc.alignment = Alignment(
        horizontal="left", vertical="center", wrap_text=True); cc.fill = ZFILL
    for rr in (r, r + 1):
        for c in range(2, LASTC + 1):
            ws.cell(rr, c).border = box
    ws.row_dimensions[r].height = 22; ws.row_dimensions[r + 1].height = 22
    r += 2
    r += 1  # 한 줄 띄움

    # 표 제목
    ws.cell(r, 2, "6. 사업장별 7월 동기간 온북 (YoY)").font = F(11)
    unit = ws.cell(r, LASTC, "[단위 : 실]"); unit.font = F(9); unit.alignment = rgt
    r += 1
    # 2단 헤더
    h1 = r; h2 = r + 1
    hcell(ws, h1, 2, "구분"); ws.merge_cells(start_row=h1, start_column=2, end_row=h2, end_column=2)
    hcell(ws, h1, 3, "동기간 온북"); ws.merge_cells(start_row=h1, start_column=3, end_row=h1, end_column=4)
    hcell(ws, h1, 4, None)
    hcell(ws, h1, 5, "전년 比"); ws.merge_cells(start_row=h1, start_column=5, end_row=h1, end_column=6)
    hcell(ws, h1, 6, None)
    hcell(ws, h1, 7, "상태"); ws.merge_cells(start_row=h1, start_column=7, end_row=h2, end_column=7)
    hcell(ws, h1, 8, "목표(전년초과)"); ws.merge_cells(start_row=h1, start_column=8, end_row=h1, end_column=LASTC)
    hcell(ws, h1, 9, None)
    for c, lab in ((3, "2026"), (4, "2025"), (5, "갭(실)"), (6, "%")):
        hcell(ws, h2, c, lab)
    hcell(ws, h2, 8, "필요 픽업"); ws.merge_cells(start_row=h2, start_column=8, end_row=h2, end_column=LASTC)
    hcell(ws, h2, 9, None)
    rr = h2 + 1
    for name, lab in TARGETS:
        v26, v25 = nb26.get(name, 0), nb25.get(name, 0); gap = v26 - v25
        yoy = (gap / v25) if v25 else 0.0
        status = "전년초과 ▲" if gap > 0 else ("동률" if gap == 0 else "전년미달 ▼")
        need = "유지·확대" if gap >= 0 else f"+{-gap:,}실"
        clr = GREEN if gap > 0 else (RED if gap < 0 else BLACK)
        cells = [
            (2, lab, lft, None, F(10)),
            (3, v26, rgt, NF_NUM, F(10)),
            (4, v25, rgt, NF_NUM, F(10)),
            (5, gap, rgt, NF_NUM, F(10, bold=True, color=clr)),
            (6, yoy, rgt, NF_PCT, F(10, color=clr)),
            (7, status, cen, None, F(10, bold=True, color=clr)),
        ]
        for c, v, al, nf, ft in cells:
            cell = ws.cell(rr, c, v); cell.alignment = al; cell.font = ft; cell.border = box
            if nf: cell.number_format = nf
        nc = ws.cell(rr, 8, need); nc.font = F(10, bold=(gap < 0), color=clr); nc.alignment = cen; nc.border = box
        ws.merge_cells(start_row=rr, start_column=8, end_row=rr, end_column=LASTC)
        ws.cell(rr, 9).border = box
        rr += 1
    # 합계 (이중 상단 테두리)
    gy = (gtot / tot25) if tot25 else 0.0
    clr = GREEN if gtot > 0 else RED
    tvals = [(2, "합계 (6개)", lft, None), (3, tot26, rgt, NF_NUM), (4, tot25, rgt, NF_NUM),
             (5, gtot, rgt, NF_NUM), (6, gy, rgt, NF_PCT),
             (7, ("전년초과 ▲" if gtot > 0 else "전년미달 ▼"), cen, None)]
    for c, v, al, nf in tvals:
        cell = ws.cell(rr, c, v); cell.alignment = al
        cell.font = F(10, bold=True, color=(clr if c in (5, 6, 7) else BLACK))
        cell.border = Border(left=HAIR, right=HAIR, top=DBL, bottom=HAIR)
        if nf: cell.number_format = nf
    nc = ws.cell(rr, 8, ("유지·확대" if gtot >= 0 else f"+{-gtot:,}실"))
    nc.font = F(10, bold=True, color=clr); nc.alignment = cen
    nc.border = Border(left=HAIR, right=HAIR, top=DBL, bottom=HAIR)
    ws.merge_cells(start_row=rr, start_column=8, end_row=rr, end_column=LASTC)
    ws.cell(rr, 9).border = Border(left=HAIR, right=HAIR, top=DBL, bottom=HAIR)
    rr += 2
    ws.cell(rr, 2, "※ 7월 동기간 = 판매일자 202607(7월 투숙) 온북. 픽업 net = 신규(최초입력)−취소(취소일자), 이중계상 없음.").font = F(9, color="808080")
    ws.cell(rr + 1, 2, "※ 갭(+)=전년초과(목표달성), 갭(−)=전년미달. 상세 추이는 [일별픽업]·[전년대비] 시트 참조.").font = F(9, color="808080")

    # ====================================================== 일별 픽업(net·Δ) ======
    ws2 = wb.create_sheet("일별픽업(net·Δ)")
    ws2.sheet_view.showGridLines = False
    ws2.column_dimensions["A"].width = 1.2
    ws2.column_dimensions["B"].width = 14
    ws2.cell(2, 2, "일별 픽업 (7월 투숙 net = 신규−취소, 예약 유입일 기준) · 전일대비 Δ").font = F(13)
    ws2.merge_cells(start_row=2, start_column=2, end_row=2, end_column=2 + len(TARGETS) * 2 + 2)
    ws2.cell(3, 2, f"최근 {WINDOW_DAYS}일({d2k(days26[0])}~{d2k(days26[-1])}, 위=최근) · 세그 구분없이 전체 RN · "
                   "Δ=전일대비 net 증감 · 매일 재실행 시 완전일 1행씩 자동 연장").font = F(9, color="808080")
    top, sub = 5, 6
    hcell(ws2, top, 2, "예약 유입일"); ws2.merge_cells(start_row=top, start_column=2, end_row=sub, end_column=2)
    col = 3; prop_cols = {}
    for name, lab in TARGETS:
        hcell(ws2, top, col, lab); ws2.merge_cells(start_row=top, start_column=col, end_row=top, end_column=col + 1)
        hcell(ws2, top, col + 1, None)
        hcell(ws2, sub, col, "net"); hcell(ws2, sub, col + 1, "Δ")
        ws2.column_dimensions[get_column_letter(col)].width = 8
        ws2.column_dimensions[get_column_letter(col + 1)].width = 8
        prop_cols[name] = col; col += 2
    hcell(ws2, top, col, "6개 합계"); ws2.merge_cells(start_row=top, start_column=col, end_row=top, end_column=col + 1)
    hcell(ws2, top, col + 1, None); hcell(ws2, sub, col, "net"); hcell(ws2, sub, col + 1, "Δ")
    ws2.column_dimensions[get_column_letter(col)].width = 9
    ws2.column_dimensions[get_column_letter(col + 1)].width = 9
    sum_col = col
    rr = sub + 1
    for di, day in enumerate(ordered):
        wd = WD[datetime.strptime(day, "%Y%m%d").weekday()]
        dc = ws2.cell(rr, 2, f"{d2k(day)} ({wd})"); dc.font = F(9); dc.alignment = cen; dc.border = box
        nxt = ordered[di + 1] if di + 1 < len(ordered) else None
        daysum = 0; psum = 0
        for name, lab in TARGETS:
            net = net_of(new26, cxl26, name, day); daysum += net
            c = ws2.cell(rr, prop_cols[name], net); c.alignment = rgt; c.border = box
            c.number_format = NF_NUM; c.font = F(9)
            dcell = ws2.cell(rr, prop_cols[name] + 1); dcell.alignment = rgt; dcell.border = box
            if nxt is not None:
                delta = net - net_of(new26, cxl26, name, nxt); psum += net_of(new26, cxl26, name, nxt)
                dcell.value = delta; dcell.number_format = NF_DELTA
                dcell.font = F(9, color=(GREEN if delta > 0 else RED if delta < 0 else BLACK))
            else:
                dcell.value = "–"; dcell.font = F(9, color="808080"); dcell.alignment = cen
        sc = ws2.cell(rr, sum_col, daysum); sc.alignment = rgt; sc.border = box
        sc.number_format = NF_NUM; sc.font = F(9, bold=True)
        sdc = ws2.cell(rr, sum_col + 1); sdc.alignment = rgt; sdc.border = box
        if nxt is not None:
            dl = daysum - psum; sdc.value = dl; sdc.number_format = NF_DELTA
            sdc.font = F(9, bold=True, color=(GREEN if dl > 0 else RED if dl < 0 else BLACK))
        else:
            sdc.value = "–"; sdc.font = F(9, color="808080"); sdc.alignment = cen
        rr += 1
    ws2.freeze_panes = ws2.cell(sub + 1, 3).coordinate

    # ===================================================== 전년대비(누적추이) =====
    ws3 = wb.create_sheet("전년대비(누적추이)")
    ws3.sheet_view.showGridLines = False
    ws3.column_dimensions["A"].width = 1.2
    ws3.column_dimensions["B"].width = 13
    ws3.cell(2, 2, "전년 동기간 대비 누적 온북 추이 (일자별 cutoff) — 갭 좁혀지는지 추적").font = F(13)
    ws3.merge_cells(start_row=2, start_column=2, end_row=2, end_column=2 + len(TARGETS) * 3 + 3)
    ws3.cell(3, 2, f"각 행 = 해당일까지 누적 net 온북. '26(좌)/'25 동일MMDD(중)/갭(우). 최근 {WINDOW_DAYS}일.").font = F(9, color="808080")
    top, sub = 5, 6
    hcell(ws3, top, 2, "기준일"); ws3.merge_cells(start_row=top, start_column=2, end_row=sub, end_column=2)
    col = 3; pc3 = {}
    for name, lab in TARGETS:
        hcell(ws3, top, col, lab); ws3.merge_cells(start_row=top, start_column=col, end_row=top, end_column=col + 2)
        hcell(ws3, top, col + 1, None); hcell(ws3, top, col + 2, None)
        for j, t2 in enumerate(("'26", "'25", "갭")):
            hcell(ws3, sub, col + j, t2)
            ws3.column_dimensions[get_column_letter(col + j)].width = 7.5
        pc3[name] = col; col += 3
    hcell(ws3, top, col, "6개 합계"); ws3.merge_cells(start_row=top, start_column=col, end_row=top, end_column=col + 2)
    hcell(ws3, top, col + 1, None); hcell(ws3, top, col + 2, None)
    for j, t2 in enumerate(("'26", "'25", "갭")):
        hcell(ws3, sub, col + j, t2); ws3.column_dimensions[get_column_letter(col + j)].width = 8
    sc3 = col
    rr = sub + 1
    for day in ordered:
        d25 = to25(day); nb26d = onbook_at(rows26, day); nb25d = onbook_at(rows25, d25)
        wd = WD[datetime.strptime(day, "%Y%m%d").weekday()]
        dc = ws3.cell(rr, 2, f"{d2k(day)} ({wd})"); dc.font = F(9); dc.alignment = cen; dc.border = box
        st26 = st25 = 0
        for name, lab in TARGETS:
            v26 = nb26d.get(name, 0); v25 = nb25d.get(name, 0); gp = v26 - v25
            st26 += v26; st25 += v25
            for j, v in enumerate((v26, v25, gp)):
                c = ws3.cell(rr, pc3[name] + j, v); c.alignment = rgt; c.border = box; c.number_format = NF_NUM
                c.font = F(9, color=(GREEN if (j == 2 and gp > 0) else RED if (j == 2 and gp < 0) else BLACK),
                           bold=(j == 2))
        for j, v in enumerate((st26, st25, st26 - st25)):
            c = ws3.cell(rr, sc3 + j, v); c.alignment = rgt; c.border = box; c.number_format = NF_NUM
            c.font = F(9, bold=True, color=(GREEN if (j == 2 and st26 - st25 > 0) else RED if j == 2 else BLACK))
        rr += 1
    ws3.freeze_panes = ws3.cell(sub + 1, 3).coordinate

    # ======================================================= 상세(신규·취소) ======
    ws4 = wb.create_sheet("상세(신규·취소)")
    ws4.sheet_view.showGridLines = False
    ws4.column_dimensions["A"].width = 1.2
    ws4.cell(2, 2, "일자×사업장 상세 — 신규 / 취소 / net / 당일 누적온북 (2026 7월 투숙)").font = F(13)
    ws4.merge_cells(start_row=2, start_column=2, end_row=2, end_column=7)
    hdr4 = [("예약 유입일", 16), ("사업장", 22), ("신규(+)", 10), ("취소(−)", 10), ("net", 9), ("당일 누적온북", 13)]
    for i, (h, w) in enumerate(hdr4):
        hcell(ws4, 4, 2 + i, h); ws4.column_dimensions[get_column_letter(2 + i)].width = w
    rr = 5
    for day in ordered:
        nbd = onbook_at(rows26, day)
        wd = WD[datetime.strptime(day, "%Y%m%d").weekday()]
        for name, lab in TARGETS:
            nw = new26.get((name, day), 0); cx = cxl26.get((name, day), 0); net = nw - cx
            row = [(f"{d2k(day)} ({wd})", cen, None, F(9)),
                   (lab, lft, None, F(9)),
                   (nw, rgt, NF_NUM, F(9)),
                   (-cx, rgt, NF_NUM, F(9)),
                   (net, rgt, NF_NUM, F(9, bold=True, color=(GREEN if net > 0 else RED if net < 0 else BLACK))),
                   (nbd.get(name, 0), rgt, NF_NUM, F(9))]
            for i, (v, al, nf, ft) in enumerate(row):
                c = ws4.cell(rr, 2 + i, v); c.alignment = al; c.font = ft; c.border = box
                if nf: c.number_format = nf
            rr += 1
    ws4.freeze_panes = "A5"

    wb.save(out_path)

# ───────────────────────── JSON (대시보드 HTML용) ─────────────────────────
def build_payload(data_date, asof26, asof25, rows26, rows25):
    hi = datetime.strptime(asof26, "%Y%m%d")
    days26 = [(hi - timedelta(days=k)).strftime("%Y%m%d") for k in range(WINDOW_DAYS)][::-1]
    ordered = list(reversed(days26))  # 최근 → 과거
    def to25(d): return "2025" + d[4:]
    WD = "월화수목금토일"

    new26, cxl26 = daily_newcancel(rows26)
    nb26 = onbook_at(rows26, asof26); nb25 = onbook_at(rows25, asof25)

    summary = []
    t26 = t25 = 0
    for name, lab in TARGETS:
        v26, v25 = nb26.get(name, 0), nb25.get(name, 0); gap = v26 - v25
        t26 += v26; t25 += v25
        summary.append({"name": name, "label": lab, "v26": v26, "v25": v25,
                        "gap": gap, "yoy": (gap / v25 if v25 else 0.0)})
    total = {"v26": t26, "v25": t25, "gap": t26 - t25, "yoy": ((t26 - t25) / t25 if t25 else 0.0)}

    daily = []
    for day in ordered:
        wd = WD[datetime.strptime(day, "%Y%m%d").weekday()]
        per = {name: [new26.get((name, day), 0), cxl26.get((name, day), 0)] for name, _ in TARGETS}
        daily.append({"d": day, "wd": wd, "per": per})  # per[name]=[신규,취소], net=신규-취소

    cumulative = []
    for day in ordered:
        nb26d = onbook_at(rows26, day); nb25d = onbook_at(rows25, to25(day))
        wd = WD[datetime.strptime(day, "%Y%m%d").weekday()]
        cper = {name: [nb26d.get(name, 0), nb25d.get(name, 0)] for name, _ in TARGETS}
        cumulative.append({"d": day, "wd": wd, "per": cper})  # per[name]=[온북26,온북25]

    return {
        "meta": {"data_date": data_date, "asof26": asof26, "asof25": asof25,
                 "window_days": WINDOW_DAYS,
                 "targets": [{"name": n, "label": l} for n, l in TARGETS]},
        "summary": summary, "total": total,
        "daily": daily, "cumulative": cumulative,
    }


# ───────────────────────── main ─────────────────────────
def main():
    data_date = snapshot_date()                       # 예: 20260630
    asof26 = (datetime.strptime(data_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")  # 마지막 완전일
    asof25 = "2025" + asof26[4:]
    print(f"스냅샷일={data_date}  동기간 기준(완전일)={asof26} / 전년={asof25}")

    print("2026 7월 투숙 로딩...", flush=True)
    rows26 = load_rows(live_files(2026), "202607")
    print(f"  rows={len(rows26):,}")
    print("2025 7월 투숙 로딩...", flush=True)
    rows25 = load_rows(retrans_files(2025) + live_files(2025), "202507")
    print(f"  rows={len(rows25):,}")

    nb26 = onbook_at(rows26, asof26); nb25 = onbook_at(rows25, asof25)
    print("\n[검증] 동기간 온북 갭 (세그 구분없이 전체 RN):")
    print(f"  {'사업장':18}{'26온북':>8}{'25온북':>8}{'갭':>8}")
    t26 = t25 = 0
    for name, lab in TARGETS:
        v26, v25 = nb26.get(name, 0), nb25.get(name, 0); t26 += v26; t25 += v25
        print(f"  {lab:18}{v26:>8,}{v25:>8,}{v26 - v25:>8,}")
    print(f"  {'합계':18}{t26:>8,}{t25:>8,}{t26 - t25:>8,}")

    # 1) 대시보드 배포본 (docs/data): HTML이 fetch + 엑셀 다운로드
    docs_data = PROJECT_DIR / "docs" / "data"
    docs_data.mkdir(parents=True, exist_ok=True)
    xlsx_docs = docs_data / "july_pickup.xlsx"
    build_excel(str(xlsx_docs), data_date, asof26, asof25, rows26, rows25)
    payload = build_payload(data_date, asof26, asof25, rows26, rows25)
    json_docs = docs_data / "july_pickup.json"
    json.dump(payload, open(json_docs, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"\n대시보드 배포본 → {json_docs}\n               → {xlsx_docs}")

    # 2) 사용자 로컬 사본 (Desktop, 날짜 파일명)
    out_dir = Path(os.path.expanduser("~/Desktop"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"7월동기간_픽업_일자별관리_{data_date}.xlsx"
    shutil.copyfile(xlsx_docs, out_path)
    print(f"로컬 사본       → {out_path}")

if __name__ == "__main__":
    main()
