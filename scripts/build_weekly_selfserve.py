#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""주간회의 예상매출현황 — 담당자 자급식 워크북 (PPT 양식 / 당월+익월).

양식 = 세일즈마케팅 예상매출현황 PPT 테이블(세그먼트 축):
  운영매출 / 세일즈마케팅(마케팅·GS·MICE) / 레저마케팅,
  블록 = 당월 + 익월, 각 (목표/Forecast/증감/달성률/전년/전년증감/증감률).
GS 섹션(GS·OTA·G-OTA·Inbound)만 RM+소사업으로 자동계산(수식),
나머지(마케팅/MICE/레저/운영)는 담당자(마케팅팀) 입력칸.

로직: 목표=객실 목표, Forecast=객실 FCST + 소사업, 소사업=GS세그 FCST객실수×실당단가/1000.
소스 주입(이후 시트에서 편집 가능): data/rm_fcst.json, data/sosaup_ratio.json
사용:  python scripts/build_weekly_selfserve.py [당월YYYY-MM]
"""
import sys, json
from pathlib import Path
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter as L

REPO = Path(__file__).resolve().parent.parent
RM = json.loads((REPO / "data" / "rm_fcst.json").read_text(encoding="utf-8"))
# 소사업 실당 = 객실 ADR × factor((1-r)/r). factor = 2026 실적 소사업비중/(1-소사업비중),
# 사업장×채널, (GS)2027 세일즈마케팅 '산출근거(부대비율)'에서 추출(data/sosaup_r2026.json).
R2026 = json.loads((REPO / "data" / "sosaup_r2026.json").read_text(encoding="utf-8"))
OUT = Path.home() / "Downloads" / "주간회의_예상매출현황_자급식.xlsx"

# PROP(01.벨비발디…) → 부대비율 사업장명,  세그 → 부대비율 채널명
SITE_MAP = {
    "01.벨비발디": "비발디파크", "02.캄비발디": "비발디파크", "03.펫비발디": "비발디파크",
    "04.펠리체비발디": "비발디파크", "05.빌리지비발디": "비발디파크", "06.양평": "소노벨 양평",
    "07.델피노": "델피노", "08.쏠비치양양": "쏠비치 양양", "09.쏠비치삼척": "쏠비치 삼척",
    "10.소노벨단양": "소노벨 단양", "11.소노캄경주": "소노캄 경주", "12.소노벨청송": "소노벨 청송",
    "13.소노벨천안": "소노벨 천안", "14.소노벨변산": "소노벨 변산", "15.소노캄여수": "소노캄 여수",
    "16.소노캄거제": "소노캄 거제", "17.쏠비치진도": "쏠비치 진도", "18.소노벨제주": "소노벨 제주",
    "19.소노캄제주": "소노캄 제주", "20.소노캄고양": "소노캄 고양", "21.소노문해운대": "소노문 해운대",
    "22.쏠비치남해": "쏠비치 남해", "23.르네블루": "르네블루",
}
CHAN_MAP = {"OTA": "국내OTA", "G-OTA": "GOTA", "Inbound": "인바운드"}

CUR = sys.argv[1] if len(sys.argv) > 1 else "2026-10"
def nextm(ym):
    y, m = int(ym[:4]), int(ym[5:7]); m += 1
    if m == 13: y, m = y + 1, 1
    return f"{y}-{m:02d}"
NXT = nextm(CUR)
MONTHS = [CUR, NXT]
SEGS = ["OTA", "G-OTA", "Inbound"]
PROPS = list(RM["properties"].keys())

HDR = Font(bold=True, color="FFFFFF", size=10)
HFILL = PatternFill("solid", fgColor="2F5597")
SUBFILL = PatternFill("solid", fgColor="8EAADB")
INFILL = PatternFill("solid", fgColor="FFF2CC")      # 입력(노랑)
FIXFILL = PatternFill("solid", fgColor="E2EFDA")     # 고정(연녹)
GSFILL = PatternFill("solid", fgColor="FCE4D6")      # GS 자동(주황)
BOLD = Font(bold=True)
CEN = Alignment(horizontal="center", vertical="center")
TH = Side(style="thin", color="D0D0D0")
BD = Border(left=TH, right=TH, top=TH, bottom=TH)


def sv(prop, seg, mo, field):
    return RM["properties"][prop].get(mo, {}).get("segments", {}).get(seg, {}).get(field, 0) or 0

def ratio(prop, seg, mo):
    """소사업 실당단가(천원/실) = 객실 ADR × factor((1-r)/r, 2026 실적 부대비중)."""
    factor = R2026.get(SITE_MAP.get(prop, ""), {}).get(CHAN_MAP.get(seg, ""), 0)
    adr = sv(prop, seg, mo, "rm_fcst_adr")
    return adr * factor


wb = openpyxl.Workbook()

# ─────────────────────────────────────────────────────────────
# ② RM입력(주간) — 사업장×세그×월 long format (소사업 포함)
# ─────────────────────────────────────────────────────────────
w2 = wb.active; w2.title = "②RM입력(주간)"
w2["A1"] = f"② RM 입력 (주간 교체) — 당월 {CUR} · 익월 {NXT} / 노란칸=매주 입력, 연녹칸=고정(단가)"
w2["A1"].font = BOLD
h2 = ["사업장", "세그", "월", "목표 매출(백만)", "FCST 매출(백만)", "전년 매출(백만)",
      "FCST 객실수", "실당단가(천원/실)", "소사업(백만)"]
for i, h in enumerate(h2, 1):
    w2.cell(2, i, h)
for c in range(1, len(h2) + 1):
    cell = w2.cell(2, c); cell.fill = HFILL; cell.font = HDR; cell.alignment = CEN; cell.border = BD
r = 3
B_FIRST = r
rowkey = {}
for p in PROPS:
    for s in SEGS:
        for mo in MONTHS:
            rowkey[(p, s, mo)] = r
            w2.cell(r, 1, p); w2.cell(r, 2, s); w2.cell(r, 3, mo)
            w2.cell(r, 4, sv(p, s, mo, "rm_budget_rev_mil"))
            w2.cell(r, 5, sv(p, s, mo, "rm_fcst_rev_mil"))
            w2.cell(r, 6, 0)  # 전년: RM 전년 컬럼 입력(현재 0)
            w2.cell(r, 7, sv(p, s, mo, "rm_fcst_rn"))
            w2.cell(r, 8, round(ratio(p, s, mo), 2))
            w2.cell(r, 9, f"=G{r}*H{r}/1000")
            for c in (4, 5, 6, 7):
                w2.cell(r, c).fill = INFILL
            w2.cell(r, 8).fill = FIXFILL
            for c in range(1, 10):
                w2.cell(r, c).border = BD
            r += 1
B_LAST = r - 1

# SUMIFS 참조 범위
SH2 = "'②RM입력(주간)'"
RG = lambda col: f"{SH2}!${col}${B_FIRST}:${col}${B_LAST}"
SEGC, MOC = RG("B"), RG("C")
TGT, FC, LY, SOScol = RG("D"), RG("E"), RG("F"), RG("I")

# ─────────────────────────────────────────────────────────────
# ① 예상매출현황 — PPT 양식, 블록=당월·익월
# ─────────────────────────────────────────────────────────────
w1 = wb.create_sheet("①예상매출현황", 0)
w1["B1"] = "① 예상매출현황 (주간회의 양식)"; w1["B1"].font = Font(bold=True, size=12)
w1["B2"] = "단위: 매출 백만원(VAT제외) · 대외비·관리자용 / GS 섹션=자동계산, 그 외=입력"
# 월 라벨 (편집 가능)
w1["C3"] = f"당월 ({CUR})"; w1["J3"] = f"익월 ({NXT})"
w1["C3"].font = BOLD; w1["J3"].font = BOLD
w1["C3"].alignment = CEN; w1["J3"].alignment = CEN
w1.merge_cells("C3:I3"); w1.merge_cells("J3:P3")
sub = ["목표", "Forecast", "증감", "달성률", "전년", "전년증감", "증감률"]
w1["A4"] = "구분"; w1["B4"] = "세부"
for bi in range(2):
    base = 3 + bi * 7  # C=3, J=10
    for i, s in enumerate(sub):
        c = base + i
        w1.cell(4, c, s)
for c in range(1, 17):
    cell = w1.cell(4, c); cell.fill = HFILL; cell.font = HDR; cell.alignment = CEN; cell.border = BD
for c in (1, 2):
    w1.cell(4, c).alignment = CEN

# 행 정의: (id, 대분류, 세부, kind)
#   kind: input | sum:<자식id,...> | gs:<세그>
ROWS = [
    ("운영", "운영 매출", "", "input"),
    ("세마", "세일즈마케팅", "", "sum:마케팅,GS,MICE"),
    ("마케팅", "마케팅", "", "sum:자사몰M,제휴M"),
    ("자사몰M", "", "자사몰", "input"),
    ("제휴M", "", "제휴", "input"),
    ("GS", "GS", "", "sum:OTA,GOTA,Inbound"),
    ("OTA", "", "OTA", "gs:OTA"),
    ("GOTA", "", "G-OTA", "gs:G-OTA"),
    ("Inbound", "", "Inbound", "gs:Inbound"),
    ("MICE", "MICE", "", "sum:GMICE,EMICE,Other"),
    ("GMICE", "", "G-MICE", "input"),
    ("EMICE", "", "E-MICE", "input"),
    ("Other", "", "Other", "input"),
    ("레저", "레저마케팅", "", "sum:자사몰L,외부채널L"),
    ("자사몰L", "", "자사몰", "input"),
    ("외부채널L", "", "외부채널", "input"),
]
R0 = 5
tagrow = {rid: R0 + i for i, (rid, a, b, kind) in enumerate(ROWS)}  # id → 행번호

def put_block(rr, bi, seg=None, sumtags=None, inputrow=False):
    base = 3 + bi * 7
    cT, cF, cD, cAch, cLY, cLYd, cLYr = (base + k for k in range(7))
    mo = MONTHS[bi]
    if seg:   # GS 세그 자동
        crit = f'"{seg}"'
        w1.cell(rr, cT, f'=ROUND(SUMIFS({TGT},{SEGC},{crit},{MOC},"{mo}"),0)')
        w1.cell(rr, cF, f'=ROUND(SUMIFS({FC},{SEGC},{crit},{MOC},"{mo}")+SUMIFS({SOScol},{SEGC},{crit},{MOC},"{mo}"),0)')
        w1.cell(rr, cLY, f'=ROUND(SUMIFS({LY},{SEGC},{crit},{MOC},"{mo}"),0)')
    elif sumtags:  # 소계 = 자식 행 합
        for ci, col in enumerate([cT, cF, cLY]):
            parts = "+".join(f"{L(col)}{tagrow[t]}" for t in sumtags if t in tagrow)
            w1.cell(rr, col, f"={parts}" if parts else 0)
    elif inputrow:  # 입력(마케팅/MICE/레저 leaf) — 목표/FC/전년만 입력
        for col in (cT, cF, cLY):
            w1.cell(rr, col).fill = INFILL
    # 공통 계산식: 증감/달성/전년증감/증감률
    w1.cell(rr, cD, f"={L(cF)}{rr}-{L(cT)}{rr}")
    w1.cell(rr, cAch, f'=IF({L(cT)}{rr}=0,"",ROUND({L(cF)}{rr}/{L(cT)}{rr}*100,0))')
    w1.cell(rr, cLYd, f"={L(cF)}{rr}-{L(cLY)}{rr}")
    w1.cell(rr, cLYr, f'=IF({L(cLY)}{rr}=0,"",ROUND(({L(cF)}{rr}-{L(cLY)}{rr})/{L(cLY)}{rr}*100,0))')

GS_IDS = {"GS", "OTA", "GOTA", "Inbound"}
for i, (rid, a, b, kind) in enumerate(ROWS):
    rr = R0 + i
    w1.cell(rr, 1, a); w1.cell(rr, 2, b)
    for bi in range(2):
        if kind.startswith("gs:"):
            put_block(rr, bi, seg=kind.split(":")[1])
        elif kind.startswith("sum:"):
            put_block(rr, bi, sumtags=kind.split(":", 1)[1].split(","))
        else:
            put_block(rr, bi, inputrow=True)
    for c in range(1, 17):
        w1.cell(rr, c).border = BD
    if rid in GS_IDS:   # GS 섹션 자동계산 음영
        for c in range(3, 17):
            w1.cell(rr, c).fill = GSFILL
    if a in ("운영 매출", "세일즈마케팅", "GS", "마케팅", "MICE", "레저마케팅"):
        w1.cell(rr, 1).font = BOLD

# ─────────────────────────────────────────────────────────────
# ⑤ 로우(RM 전월별 원본)
# ─────────────────────────────────────────────────────────────
w5 = wb.create_sheet("⑤로우(RM원본)")
w5["A1"] = "⑤ 로우 — RM 월별×사업장×세그 (목표/FCST, 추적용)"; w5["A1"].font = BOLD
h5 = ["월", "사업장", "세그", "목표 객실수", "목표 매출", "FCST 객실수", "FCST ADR", "FCST 매출"]
for i, h in enumerate(h5, 1):
    w5.cell(2, i, h)
    cell = w5.cell(2, i); cell.fill = HFILL; cell.font = HDR; cell.alignment = CEN
rr = 3
for p in PROPS:
    for mo, node in RM["properties"][p].items():
        for s in SEGS:
            sg = node.get("segments", {}).get(s, {})
            vals = [mo, p, s, sg.get("rm_budget_rn", 0), sg.get("rm_budget_rev_mil", 0),
                    sg.get("rm_fcst_rn", 0), sg.get("rm_fcst_adr", 0), sg.get("rm_fcst_rev_mil", 0)]
            for i, v in enumerate(vals, 1):
                w5.cell(rr, i, v)
            rr += 1

# ─────────────────────────────────────────────────────────────
# ⑥ PowerQuery(윈도우) — RM 원본 엑셀 → ②자동붙이기 M코드
# ─────────────────────────────────────────────────────────────
wq = wb.create_sheet("⑥PowerQuery(윈도우)")
PQ = [
 "■ 윈도우 엑셀: RM 원본 엑셀을 ②RM입력 세그 데이터와 자동연결(새로고침) — Power Query M",
 "사용법: 데이터 → 데이터 가져오기 → 쿼리 결합/빈 쿼리 → 고급 편집기에 아래 붙여넣기.",
 "   (파일경로/시트명은 네 RM 원본에 맞게 1줄만 수정. 갑지 시트는 사업장 블록이 흩어져 있어",
 "    사업장별 셀좌표 매핑이 필요 → RM 원본 최신본 1개 주면 이 쿼리에 좌표표를 완성해줄게.)",
 "",
 "let",
 '    Src = Excel.Workbook(File.Contents("C:\\경로\\Revenue Meeting_주간.xlsx"), null, true),',
 '    갑지 = Src{[Item="갑지(10월)",Kind="Sheet"]}[Data],',
 "    // TODO: 사업장별 (목표/전망/전년) 셀좌표를 좌표표로 unpivot → 세그 매핑",
 "    //  (RM 원본 최신본 제공 시 이 구간 자동완성)",
 "    결과 = 갑지",
 "in",
 "    결과",
 "",
 "※ 좌표표 방식: 각 사업장 블록의 Revenue/RoomNights 목표·전망·전년 셀을 고정좌표로 읽어",
 "  [사업장, 세그, 월, 목표매출, FCST매출, 전년매출, FCST객실수] long 테이블로 만든 뒤 ②에 로드.",
 "※ 맥 엑셀은 PDF/일부 PQ 제약 → ②에 직접 붙여넣기(값) 방식 사용.",
]
for i, t in enumerate(PQ, 1):
    wq.cell(i, 1, t)
    if i == 1: wq.cell(i, 1).font = BOLD

# ⑦ 안내
wg = wb.create_sheet("⑦안내")
guide = [
 "■ 주간회의 예상매출현황 — 담당자 사용법",
 f"당월 {CUR} · 익월 {NXT}  (월 변경: ②와 ①의 월 라벨/수식 월을 바꾸거나 재생성 요청)",
 "",
 "1) 매주 Revenue Meeting이 오면 → [②RM입력(주간)] 노란칸(목표/FCST/전년 매출·FCST객실수)을 갈아끼운다.",
 "   · 세그=OTA/G-OTA/Inbound, 월=당월·익월. (윈도우면 ⑥PowerQuery로 자동연결)",
 "2) [①예상매출현황]의 GS 섹션(GS/OTA/G-OTA/Inbound)이 자동 재계산 → PPT 테이블에 붙여넣기.",
 "   · Forecast = 객실 FCST + 소사업,  소사업 = GS세그 FCST객실수 × 실당단가 ÷ 1000.",
 "3) 마케팅/MICE/레저/운영 행은 [①]의 노란칸에 마케팅팀 숫자 입력(소계는 자동 합).",
 "4) 실당단가는 [②] 연녹칸(고정)에서 관리. 목표는 [②] 목표열.",
 "",
 "※ 실당단가·전년 = 현재 placeholder(2025단가 / 전년0). 2027 세일즈마케팅 파일 매핑 확정 후 교체.",
 "※ 2027 목표 전환 시: ② 목표열을 2027 사업계획 값으로 교체(또는 재생성).",
 "※ 단위: 객실수=실, ADR=천원, 매출=백만(VAT제외). 대외비.",
]
for i, t in enumerate(guide, 1):
    wg.cell(i, 1, t)
    if i == 1: wg.cell(i, 1).font = BOLD

# 열너비
for i, wd in enumerate([10, 10] + [9] * 14, 1):
    w1.column_dimensions[L(i)].width = wd
for i, wd in enumerate([13, 8, 9, 13, 13, 13, 11, 15, 12], 1):
    w2.column_dimensions[L(i)].width = wd
for i, wd in enumerate([9, 13, 9, 11, 11, 11, 10, 11], 1):
    w5.column_dimensions[L(i)].width = wd
wq.column_dimensions["A"].width = 100
wg.column_dimensions["A"].width = 100

wb.save(OUT)

# 검증(파이썬 측 GS)
print(f"저장: {OUT}")
for mo in MONTHS:
    for s in SEGS:
        rn = sum(sv(p, s, mo, "rm_fcst_rn") for p in PROPS)
        tgt = sum(sv(p, s, mo, "rm_budget_rev_mil") for p in PROPS)
        fc = sum(sv(p, s, mo, "rm_fcst_rev_mil") for p in PROPS)
        sos = sum(sv(p, s, mo, "rm_fcst_rn") * ratio(p, s, mo) / 1000 for p in PROPS)
        print(f"  {mo} {s:8} 목표 {tgt:6,.0f} / FCST(객실+소사업) {fc+sos:7,.0f} (객실 {fc:,.0f}+소사업 {sos:,.1f})")
