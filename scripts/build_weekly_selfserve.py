#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""주간회의 예상매출현황 — 담당자 자급식 워크북 (사업장별 라이브 / 당월·익월·익익월).

담당자가 매주 Revenue Meeting 엑셀을 같은 폴더에 두고 새로고침하면:
  ⑤로우(사업장별)  = RM 2026.MM 의 사업장 블록 전부를 Power Query로 긁어옴
                      (사업장 × 세그(OTA/G-OTA/Inbound) × 월: 목표/FCST/전년/객실수 + 소사업)
  ①예상매출현황    = ⑤를 세그+월 SUMIFS로 집계(PPT 양식, 당월/익월/익익월 3블록)

소사업(사업장별) = 객실 FCST매출 × 소사업비중factor(사업장×세그, 2026실적 부대비중).
  factor 는 ⑥ Power Query M 안에 내장(아래 R2026 에서 생성). ③factor 시트는 참조용.
RM 좌표: 사업장 블록 78행 간격(헤더 r80·158·…), 블록당 Inbound=+17/OTA=+20/G-OTA=+21행,
         목표=I(9)/FCST=N(14)/전년(2025Actual)=AG(33)/FCST RN=K(11). PQ는 빈 A·B열 보정(c-2).
사용:  python scripts/build_weekly_selfserve.py [당월YYYY-MM] [RM엑셀경로]
"""
import sys, json
from pathlib import Path
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter as L

REPO = Path(__file__).resolve().parent.parent
R2026 = json.loads((REPO / "data" / "sosaup_r2026.json").read_text(encoding="utf-8"))
OUT = Path.home() / "Downloads" / "주간회의_예상매출현황_자급식.xlsx"

CUR = sys.argv[1] if len(sys.argv) > 1 else "2026-10"
def nextm(ym):
    y, m = int(ym[:4]), int(ym[5:7]); m += 1
    return f"{y+1}-01" if m == 13 else f"{y}-{m:02d}"
NXT = nextm(CUR); NXT2 = nextm(NXT)
MONTHS = [CUR, NXT, NXT2]
RM_SHEET = {m: m.replace("-", ".") for m in MONTHS}
SEGS = ["OTA", "G-OTA", "Inbound"]

HDR = Font(bold=True, color="FFFFFF", size=10)
HFILL = PatternFill("solid", fgColor="2F5597")
INFILL = PatternFill("solid", fgColor="FFF2CC")
FIXFILL = PatternFill("solid", fgColor="E2EFDA")
GSFILL = PatternFill("solid", fgColor="FCE4D6")
BOLD = Font(bold=True)
CEN = Alignment(horizontal="center", vertical="center")
TH = Side(style="thin", color="D0D0D0"); BD = Border(TH, TH, TH, TH)

wb = openpyxl.Workbook()

# ⑤ 로우(사업장별) — PQ 로드 전용(비워둠). 열: 사업장/세그/월/목표/FCST/전년/객실수/소사업
w5 = wb.active; w5.title = "⑤로우(사업장별)"
w5["J1"] = "⑥ PowerQuery가 A1부터 자동 채움(사업장×세그×월). A:H 비워두세요(미리 값 넣으면 로드시 열이 밀립니다)."
w5["J1"].font = Font(italic=True, color="888888")
Q5 = "'⑤로우(사업장별)'"
QPROP = f"{Q5}!$A:$A"; QSEG = f"{Q5}!$B:$B"; QMO = f"{Q5}!$C:$C"
QTGT = f"{Q5}!$D:$D"; QFC = f"{Q5}!$E:$E"; QLY = f"{Q5}!$F:$F"; QRN = f"{Q5}!$G:$G"; QSOS = f"{Q5}!$H:$H"

# ③ factor(참조) — 사업장 묶음(R2026 사이트)×세그 소사업비중 (PQ에 내장된 값과 동일)
w3 = wb.create_sheet("③factor(참조)")
w3["A1"] = "③ 소사업비중 factor = 식음·부대÷객실매출 (2026실적). 소사업=객실FCST매출×factor. ⑥PQ에 내장."
w3["A1"].font = BOLD
for i, h in enumerate(["사업장(묶음)", "OTA", "G-OTA", "Inbound"], 1):
    c = w3.cell(2, i, h); c.fill = HFILL; c.font = HDR; c.alignment = CEN; c.border = BD
r = 3
for site, d in R2026.items():
    w3.cell(r, 1, site); w3.cell(r, 2, round(d.get("국내OTA", 0), 4))
    w3.cell(r, 3, round(d.get("GOTA", 0), 4)); w3.cell(r, 4, round(d.get("인바운드", 0), 4))
    for c in range(1, 5): w3.cell(r, c).border = BD
    r += 1

# ① 예상매출현황 — SUMIFS(세그+월) over ⑤, PPT 양식
w1 = wb.create_sheet("①예상매출현황", 0)
w1["B1"] = "① 예상매출현황 (주간회의 양식)"; w1["B1"].font = Font(bold=True, size=12)
w1["B2"] = "단위: 매출 백만원(VAT제외) · 대외비·관리자용 / GS 섹션=⑤ 자동집계, 그 외=입력"
BLK = ["당월", "익월", "익익월"]; NB = len(MONTHS); NCOL = 2 + NB * 7
for bi, mo in enumerate(MONTHS):
    base = 3 + bi * 7
    c = w1.cell(3, base, f"{BLK[bi]} ({mo})"); c.font = BOLD; c.alignment = CEN
    w1.merge_cells(start_row=3, start_column=base, end_row=3, end_column=base + 6)
w1["A4"] = "구분"; w1["B4"] = "세부"
for bi in range(NB):
    for i, s in enumerate(["목표", "Forecast", "증감", "달성률", "전년", "전년증감", "증감률"]):
        w1.cell(4, 3 + bi * 7 + i, s)
for c in range(1, NCOL + 1):
    cell = w1.cell(4, c); cell.fill = HFILL; cell.font = HDR; cell.alignment = CEN; cell.border = BD

ROWS = [
    ("운영", "운영 매출", "", "input"), ("세마", "세일즈마케팅", "", "sum:마케팅,GS,MICE"),
    ("마케팅", "마케팅", "", "sum:자사몰M,제휴M"), ("자사몰M", "", "자사몰", "input"), ("제휴M", "", "제휴", "input"),
    ("GS", "GS", "", "sum:OTA,GOTA,Inbound"),
    ("OTA", "", "OTA", "seg:OTA"), ("GOTA", "", "G-OTA", "seg:G-OTA"), ("Inbound", "", "Inbound", "seg:Inbound"),
    ("MICE", "MICE", "", "sum:GMICE,EMICE,Other"), ("GMICE", "", "G-MICE", "input"), ("EMICE", "", "E-MICE", "input"), ("Other", "", "Other", "input"),
    ("레저", "레저마케팅", "", "sum:자사몰L,외부채널L"), ("자사몰L", "", "자사몰", "input"), ("외부채널L", "", "외부채널", "input"),
]
R0 = 5
tagrow = {rid: R0 + i for i, (rid, a, b, k) in enumerate(ROWS)}

def put(rr, bi, seg=None, sumtags=None):
    base = 3 + bi * 7; mo = MONTHS[bi]
    cT, cF, cD, cAch, cLY, cLYd, cLYr = (base + k for k in range(7))
    if seg:
        w1.cell(rr, cT, f'=SUMIFS({QTGT},{QSEG},"{seg}",{QMO},"{mo}")')
        w1.cell(rr, cF, f'=ROUND(SUMIFS({QFC},{QSEG},"{seg}",{QMO},"{mo}")+SUMIFS({QSOS},{QSEG},"{seg}",{QMO},"{mo}"),0)')
        w1.cell(rr, cLY, f'=SUMIFS({QLY},{QSEG},"{seg}",{QMO},"{mo}")')
    elif sumtags:
        for col in (cT, cF, cLY):
            w1.cell(rr, col, "=" + "+".join(f"{L(col)}{tagrow[t]}" for t in sumtags))
    else:
        for col in (cT, cF, cLY): w1.cell(rr, col).fill = INFILL
    w1.cell(rr, cD, f"={L(cF)}{rr}-{L(cT)}{rr}")
    w1.cell(rr, cAch, f'=IF({L(cT)}{rr}=0,"",{L(cF)}{rr}/{L(cT)}{rr})')
    w1.cell(rr, cLYd, f"={L(cF)}{rr}-{L(cLY)}{rr}")
    w1.cell(rr, cLYr, f'=IF({L(cLY)}{rr}=0,"",({L(cF)}{rr}-{L(cLY)}{rr})/{L(cLY)}{rr})')
    for col in (cT, cF, cD, cLY, cLYd): w1.cell(rr, col).number_format = "#,##0"
    for col in (cAch, cLYr): w1.cell(rr, col).number_format = "0%"

GS_IDS = {"GS", "OTA", "GOTA", "Inbound"}
for i, (rid, a, b, kind) in enumerate(ROWS):
    rr = R0 + i
    w1.cell(rr, 1, a); w1.cell(rr, 2, b)
    for bi in range(NB):
        if kind.startswith("seg:"): put(rr, bi, seg=kind.split(":")[1])
        elif kind.startswith("sum:"): put(rr, bi, sumtags=kind.split(":", 1)[1].split(","))
        else: put(rr, bi)
    for c in range(1, NCOL + 1): w1.cell(rr, c).border = BD
    if rid in GS_IDS:
        for c in range(3, NCOL + 1): w1.cell(rr, c).fill = GSFILL
    if a in ("운영 매출", "세일즈마케팅", "GS", "마케팅", "MICE", "레저마케팅"): w1.cell(rr, 1).font = BOLD

# ⑥ PowerQuery — 사업장 블록 전부 긁기 + 소사업(factor 내장)
wq = wb.create_sheet("⑥PowerQuery(윈도우)")
wq["H1"] = '=IFERROR(LEFT(CELL("filename",H1),FIND("[",CELL("filename",H1))-1),"")'
wq["H1"].font = Font(italic=True, color="888888")
try:
    from openpyxl.workbook.defined_name import DefinedName
    dn = DefinedName("폴더경로", attr_text="'⑥PowerQuery(윈도우)'!$H$1")
    try: wb.defined_names.add(dn)
    except Exception: wb.defined_names["폴더경로"] = dn
except Exception as e:
    print("  명명(폴더경로) 실패:", e)

frows = ", ".join(
    f'{{"{site}", {round(d.get("국내OTA",0),4)}, {round(d.get("GOTA",0),4)}, {round(d.get("인바운드",0),4)}}}'
    for site, d in R2026.items())
M = r'''let
    폴더 = Excel.CurrentWorkbook(){[Name="폴더경로"]}[Content]{0}[Column1],
    파일들 = Folder.Files(폴더),
    RM후보 = Table.SelectRows(파일들, each Text.Contains([Name], "Meeting") and Text.EndsWith(Text.Lower([Name]), ".xlsx") and not Text.StartsWith([Name], "~$")),
    최신 = Table.Sort(RM후보, {{"Date modified", Order.Descending}}),
    Src = Excel.Workbook(최신{0}[Content], null, true),
    준비 = (sh) => Table.Buffer(Table.SelectColumns(Src{[Item=sh, Kind="Sheet"]}[Data], {"Column1","Column7","Column9","Column12","Column31"}, MissingField.UseNull)),
    셀 = (t, r, c) => Record.Field(t{r-1}, "Column" & Text.From(c-2)),
    txt = (t, r, c) => let v = try Record.Field(t{r-1}, "Column" & Text.From(c-2)) otherwise null in if v = null then "" else Text.Trim(Text.Replace(Text.From(v), "▣", "")),
    사이트 = (nm) => if Text.Contains(nm, "비발디파크") then "비발디파크" else if Text.Contains(nm, "르네블루") then "르네블루" else Text.Replace(nm, "_", " "),
    factorT = #table({"S","OTA","GOTA","IN"}, {__FACTOR__}),
    factor = (nm, seg) => let s = 사이트(nm), rw = Table.SelectRows(factorT, each [S] = s) in if Table.IsEmpty(rw) then 0 else (if seg = "OTA" then rw{0}[OTA] else if seg = "G-OTA" then rw{0}[GOTA] else rw{0}[IN]),
    PH = List.Transform({0..27}, each 80 + 78 * _),
    오프셋 = {{"Inbound", 17}, {"OTA", 20}, {"G-OTA", 21}},
    한달 = (t, 월) => List.Combine(List.Transform(PH, (ph) =>
        let nm = txt(t, ph, 3) in
        if nm = "" or Text.Contains(nm, "Total") then {}
        else List.Transform(오프셋, (so) =>
            let rr = ph + so{1}, seg = so{0}, fc = 셀(t, rr, 14) in
            [사업장 = nm, 세그 = seg, 월 = 월,
             목표 = 셀(t, rr, 9), FCST = fc, 전년 = 셀(t, rr, 33),
             객실수 = 셀(t, rr, 11), 소사업 = fc * factor(nm, seg)]))),
    당월 = 준비("__M0__"),
    익월 = 준비("__M1__"),
    익익월 = 준비("__M2__"),
    결합 = 한달(당월, "__CUR__") & 한달(익월, "__NXT__") & 한달(익익월, "__NXT2__"),
    표 = Table.FromRecords(결합)
in
    표'''
M = (M.replace("__FACTOR__", frows)
       .replace("__M0__", RM_SHEET[CUR]).replace("__M1__", RM_SHEET[NXT]).replace("__M2__", RM_SHEET[NXT2])
       .replace("__CUR__", CUR).replace("__NXT__", NXT).replace("__NXT2__", NXT2))
notes = [
 "■ 윈도우 엑셀: 같은 폴더의 RM 엑셀 → [⑤로우(사업장별)] 자동로드(사업장 전부). 경로수정 불필요, 새로고침만.",
 "설정(1회): 데이터 → 새 쿼리 → 기타 원본에서 → 빈 쿼리 → 홈 → 고급 편집기 → 아래 M 붙여넣기 → 완료",
 "         → 닫기및로드(다음으로) → 표 → 기존시트 '⑤로우(사업장별)'!$A$1. (⑤는 반드시 비워둘 것)",
 "주의: 워크북 '저장' 필수(폴더경로). 개인정보수준 물으면 '무시(결합 허용)'.",
 "동작: 같은 폴더 최신 'Meeting*.xlsx' 자동사용. 사업장 블록(78행 간격) 전부 긁어 세그×월 long + 소사업(factor).",
 "────────────────────────────────────────────────────────",
]
for i, t in enumerate(notes, 1): wq.cell(i, 2, t)
wq.cell(1, 2).font = BOLD
for j, ln in enumerate(M.split("\n"), len(notes) + 1): wq.cell(j, 2, ln)

# ⑦ 안내
wg = wb.create_sheet("⑦안내")
guide = [
 "■ 주간회의 예상매출현황 — 담당자 사용법",
 f"당월 {CUR} · 익월 {NXT} · 익익월 {NXT2}",
 "",
 "1) (윈도우) 이 워크북과 RM 엑셀을 같은 폴더에 두고 → [데이터 → 모두 새로고침].",
 "   → [⑤로우(사업장별)]가 RM 사업장 블록 전부에서 자동 갱신(경로수정 불필요).",
 "   최초 1회는 ⑥의 M코드로 쿼리를 만들어 ⑤!A1에 로드(⑥ 안내 참고).",
 "2) [①예상매출현황] GS 섹션이 ⑤를 세그+월로 자동 집계 → PPT 테이블에 그대로 붙여넣기.",
 "   Forecast = 객실 FCST + 소사업(사업장별 객실매출×부대비중factor).",
 "3) 마케팅/MICE/레저/운영 행은 ① 노란칸에 마케팅팀 숫자 입력(소계 자동).",
 "4) factor(소사업비중)는 ③factor 참조(⑥PQ에 동일값 내장). 수정하려면 ⑥ M의 factorT도 같이.",
 "",
 "※ 로드 전엔 ⑤가 비어 ①이 0으로 보임(정상). 쿼리 로드/새로고침하면 채워짐.",
 "※ 단위: 객실수=실, ADR=천원, 매출=백만(VAT제외). 달성률/증감률=% 서식. 대외비.",
]
for i, t in enumerate(guide, 1): wg.cell(i, 1, t)
wg.cell(1, 1).font = BOLD

for i, wd in enumerate([10, 10] + [9] * (NB * 7), 1): w1.column_dimensions[L(i)].width = wd
for i, wd in enumerate([13, 9, 9, 10, 10, 10, 9, 10], 1): w5.column_dimensions[L(i)].width = wd
w3.column_dimensions["A"].width = 14
wq.column_dimensions["A"].width = 10; wq.column_dimensions["B"].width = 120
wg.column_dimensions["A"].width = 100

wb.save(OUT)
print(f"저장: {OUT}")
print(f"대상월 {MONTHS} | ⑤는 빈 PQ타겟 | factor 사이트 {len(R2026)}개 내장 | ①=SUMIFS(⑤)")
