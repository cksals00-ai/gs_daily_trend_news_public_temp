#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""주간회의 예상매출현황 — 담당자 자급식 워크북 (PPT 양식 / 당월+익월 / 실제 PQ).

담당자가 매주 Revenue Meeting 엑셀을 갈아끼우고 새로고침하면 ①예상매출현황이 자동 완성.
① = PPT 양식(세그축 운영/마케팅·GS·MICE/레저), 블록 = 당월 + 익월.
GS 섹션(GS·OTA·G-OTA·Inbound)만 RM+소사업으로 자동계산.

RM 소스 = 2026.MM 상세시트의 '전체 주요지표'(회사전체) = ① GS 세그총계와 동일.
  세그행: Inbound=19, OTA=22, G-OTA=23
  컬럼  : 목표Rev=I(9), FCST Rev=N(14), FCST RN=K(11), 전년(2025Actual)Rev=AG(33)
②[A](세그×월 목표/FCST/전년/객실수)는 ⑥ Power Query(윈도우)로 RM에서 자동 로드,
②[B] 소사업 = FCST객실수 × 소사업실당단가(회사평균, 2026실적 부대비중 기반) / 1000.
사용:  python scripts/build_weekly_selfserve.py [당월YYYY-MM] [RM엑셀경로]
"""
import sys, json
from pathlib import Path
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter as L

REPO = Path(__file__).resolve().parent.parent
RM = json.loads((REPO / "data" / "rm_fcst.json").read_text(encoding="utf-8"))
R2026 = json.loads((REPO / "data" / "sosaup_r2026.json").read_text(encoding="utf-8"))
OUT = Path.home() / "Downloads" / "주간회의_예상매출현황_자급식.xlsx"

CUR = sys.argv[1] if len(sys.argv) > 1 else "2026-10"
RM_XLSX = Path(sys.argv[2]) if len(sys.argv) > 2 else Path.home() / "Downloads" / "Revemue Meeting 261030.xlsx"
def nextm(ym):
    y, m = int(ym[:4]), int(ym[5:7]); m += 1
    return f"{y+1}-01" if m == 13 else f"{y}-{m:02d}"
NXT = nextm(CUR)
MONTHS = [CUR, NXT]
SEGS = ["OTA", "G-OTA", "Inbound"]
PROPS = list(RM["properties"].keys())
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
# RM 상세 전체 주요지표 좌표
SEG_ROW = {"Inbound": 19, "OTA": 22, "G-OTA": 23}
COL_TGT, COL_FCST, COL_RN, COL_LY = 9, 14, 11, 33   # I,N,K,AG
RM_SHEET = {CUR: "2026.10", NXT: "2026.11"}          # 당월/익월 → 상세시트명

HDR = Font(bold=True, color="FFFFFF", size=10)
HFILL = PatternFill("solid", fgColor="2F5597")
INFILL = PatternFill("solid", fgColor="FFF2CC")
FIXFILL = PatternFill("solid", fgColor="E2EFDA")
PQFILL = PatternFill("solid", fgColor="DDEBF7")
GSFILL = PatternFill("solid", fgColor="FCE4D6")
BOLD = Font(bold=True)
CEN = Alignment(horizontal="center", vertical="center")
TH = Side(style="thin", color="D0D0D0"); BD = Border(TH, TH, TH, TH)


def pv_sum(field, mo):  # 회사전체 = Σ사업장
    return sum(RM["properties"][p].get(mo, {}).get("segments", {}).get(s, {}).get(field, 0) or 0
               for p in PROPS for s in [None]) if False else None

def seg_company(seg, mo, field):
    return sum((RM["properties"][p].get(mo, {}).get("segments", {}).get(seg, {}).get(field, 0) or 0)
               for p in PROPS)

def sosaup_rate(seg, mo):  # 회사평균 소사업실당(천원/실) = Σ(RN×ADR×factor)/ΣRN
    num = den = 0.0
    for p in PROPS:
        sg = RM["properties"][p].get(mo, {}).get("segments", {}).get(seg, {})
        rn = sg.get("rm_fcst_rn", 0) or 0; adr = sg.get("rm_fcst_adr", 0) or 0
        fac = R2026.get(SITE_MAP.get(p, ""), {}).get(CHAN_MAP.get(seg, ""), 0)
        num += rn * adr * fac; den += rn
    return num / den if den else 0

# RM 엑셀에서 전체 주요지표 prefill (있으면)
def rm_prefill():
    out = {}
    if not RM_XLSX.exists():
        return out
    try:
        wb = openpyxl.load_workbook(RM_XLSX, read_only=True, data_only=True)
        for mo in MONTHS:
            ws = wb[RM_SHEET[mo]]
            grid = {}
            for ri, row in enumerate(ws.iter_rows(min_row=1, max_row=24, max_col=34, values_only=True), 1):
                grid[ri] = row
            for seg, rr in SEG_ROW.items():
                r = grid.get(rr, ())
                def g(c): return (r[c - 1] if len(r) >= c and isinstance(r[c - 1], (int, float)) else 0)
                out[(seg, mo)] = dict(tgt=round(g(COL_TGT), 1), fcst=round(g(COL_FCST), 1),
                                      rn=int(g(COL_RN)), ly=round(g(COL_LY), 1))
    except Exception as e:
        print("  RM prefill 실패(0으로):", e)
    return out
PRE = rm_prefill()
def pre(seg, mo, k, fb=0):
    return PRE.get((seg, mo), {}).get(k, fb)


wb = openpyxl.Workbook()

# ② RM입력(주간) — [A] 세그 회사전체(PQ) + [B] 소사업
w2 = wb.active; w2.title = "②RM입력(주간)"
w2["A1"] = f"② RM 입력 — 당월 {CUR}·익월 {NXT} / 파랑=PowerQuery 자동(⑥), 연녹=고정(단가)"
w2["A1"].font = BOLD
w2["A2"] = "[A] 세그 회사전체 (RM 2026.MM '전체 주요지표' → ⑥ PQ 자동로드)"; w2["A2"].font = BOLD
hA = ["세그", "월", "목표 매출(백만)", "FCST 매출(백만)", "전년 매출(백만)", "FCST 객실수"]
for i, h in enumerate(hA, 1):
    cell = w2.cell(3, i, h); cell.fill = HFILL; cell.font = HDR; cell.alignment = CEN; cell.border = BD
r = 4; Arow = {}
for s in SEGS:
    for mo in MONTHS:
        Arow[(s, mo)] = r
        w2.cell(r, 1, s); w2.cell(r, 2, mo)
        w2.cell(r, 3, pre(s, mo, "tgt", round(seg_company(s, mo, "rm_budget_rev_mil"), 1)))
        w2.cell(r, 4, pre(s, mo, "fcst", round(seg_company(s, mo, "rm_fcst_rev_mil"), 1)))
        w2.cell(r, 5, pre(s, mo, "ly", 0))
        w2.cell(r, 6, pre(s, mo, "rn", seg_company(s, mo, "rm_fcst_rn")))
        for c in range(3, 7):
            w2.cell(r, c).fill = PQFILL; w2.cell(r, c).border = BD
        for c in (1, 2): w2.cell(r, c).border = BD
        r += 1
A_LAST = r - 1

b0 = r + 1
w2.cell(b0, 1, "[B] 소사업 (단가=회사평균 소사업실당, 2026실적 부대비중 기반 / 고정)").font = BOLD
hB = ["세그", "월", "FCST 객실수", "소사업단가(천원/실)", "소사업(백만)"]
for i, h in enumerate(hB, 1):
    cell = w2.cell(b0 + 1, i, h); cell.fill = HFILL; cell.font = HDR; cell.alignment = CEN; cell.border = BD
r = b0 + 2; Brow = {}
for s in SEGS:
    for mo in MONTHS:
        Brow[(s, mo)] = r
        ar = Arow[(s, mo)]
        w2.cell(r, 1, s); w2.cell(r, 2, mo)
        w2.cell(r, 3, f"=F{ar}")                       # FCST 객실수 = [A]
        w2.cell(r, 4, round(sosaup_rate(s, mo), 2))    # 단가(고정)
        w2.cell(r, 5, f"=C{r}*D{r}/1000")              # 소사업
        w2.cell(r, 4).fill = FIXFILL
        for c in range(1, 6): w2.cell(r, c).border = BD
        r += 1
B_FIRST = b0 + 2; B_LAST = r - 1

# ① 예상매출현황 — PPT 양식 (당월+익월)
w1 = wb.create_sheet("①예상매출현황", 0)
w1["B1"] = "① 예상매출현황 (주간회의 양식)"; w1["B1"].font = Font(bold=True, size=12)
w1["B2"] = "단위: 매출 백만원(VAT제외) · 대외비·관리자용 / GS 섹션=자동(PQ), 그 외=입력"
w1["C3"] = f"당월 ({CUR})"; w1["J3"] = f"익월 ({NXT})"
for a in ("C3", "J3"): w1[a].font = BOLD; w1[a].alignment = CEN
w1.merge_cells("C3:I3"); w1.merge_cells("J3:P3")
sub = ["목표", "Forecast", "증감", "달성률", "전년", "전년증감", "증감률"]
w1["A4"] = "구분"; w1["B4"] = "세부"
for bi in range(2):
    for i, s in enumerate(sub):
        w1.cell(4, 3 + bi * 7 + i, s)
for c in range(1, 17):
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
SH2 = "'②RM입력(주간)'"

def put(rr, bi, seg=None, sumtags=None):
    base = 3 + bi * 7; mo = MONTHS[bi]
    cT, cF, cD, cAch, cLY, cLYd, cLYr = (base + k for k in range(7))
    if seg:
        a = Arow[(seg, mo)]; b = Brow[(seg, mo)]
        w1.cell(rr, cT, f"={SH2}!C{a}")
        w1.cell(rr, cF, f"=ROUND({SH2}!D{a}+{SH2}!E{b},0)")   # 객실FCST + 소사업
        w1.cell(rr, cLY, f"={SH2}!E{a}")
    elif sumtags:
        for col in (cT, cF, cLY):
            w1.cell(rr, col, "=" + "+".join(f"{L(col)}{tagrow[t]}" for t in sumtags))
    else:
        for col in (cT, cF, cLY): w1.cell(rr, col).fill = INFILL
    w1.cell(rr, cD, f"={L(cF)}{rr}-{L(cT)}{rr}")
    w1.cell(rr, cAch, f'=IF({L(cT)}{rr}=0,"",ROUND({L(cF)}{rr}/{L(cT)}{rr}*100,0))')
    w1.cell(rr, cLYd, f"={L(cF)}{rr}-{L(cLY)}{rr}")
    w1.cell(rr, cLYr, f'=IF({L(cLY)}{rr}=0,"",ROUND(({L(cF)}{rr}-{L(cLY)}{rr})/{L(cLY)}{rr}*100,0))')

GS_IDS = {"GS", "OTA", "GOTA", "Inbound"}
for i, (rid, a, b, kind) in enumerate(ROWS):
    rr = R0 + i
    w1.cell(rr, 1, a); w1.cell(rr, 2, b)
    for bi in range(2):
        if kind.startswith("seg:"): put(rr, bi, seg=kind.split(":")[1])
        elif kind.startswith("sum:"): put(rr, bi, sumtags=kind.split(":", 1)[1].split(","))
        else: put(rr, bi)
    for c in range(1, 17): w1.cell(rr, c).border = BD
    if rid in GS_IDS:
        for c in range(3, 17): w1.cell(rr, c).fill = GSFILL
    if a in ("운영 매출", "세일즈마케팅", "GS", "마케팅", "MICE", "레저마케팅"): w1.cell(rr, 1).font = BOLD

# ⑤ 로우 (사업장×세그×월, 참조)
w5 = wb.create_sheet("⑤로우(사업장별)")
w5["A1"] = "⑤ 로우 — 사업장×세그×월 (RM 전망, 참조)"; w5["A1"].font = BOLD
h5 = ["월", "사업장", "세그", "목표매출", "FCST객실수", "FCST ADR", "FCST매출", "소사업단가(천원/실)"]
for i, h in enumerate(h5, 1):
    cell = w5.cell(2, i, h); cell.fill = HFILL; cell.font = HDR; cell.alignment = CEN
rr = 3
for p in PROPS:
    for mo in MONTHS:
        for s in SEGS:
            sg = RM["properties"][p].get(mo, {}).get("segments", {}).get(s, {})
            adr = sg.get("rm_fcst_adr", 0) or 0
            fac = R2026.get(SITE_MAP.get(p, ""), {}).get(CHAN_MAP.get(s, ""), 0)
            vals = [mo, p, s, sg.get("rm_budget_rev_mil", 0), sg.get("rm_fcst_rn", 0), adr,
                    sg.get("rm_fcst_rev_mil", 0), round(adr * fac, 2)]
            for i, v in enumerate(vals, 1): w5.cell(rr, i, v)
            rr += 1

# ⑥ PowerQuery (실제 M코드) — 상대경로: 이 워크북과 같은 폴더의 최신 'Meeting' 엑셀 자동 탐지
wq = wb.create_sheet("⑥PowerQuery(윈도우)")
# 폴더경로 명명 셀(이 워크북이 저장된 폴더). 상대경로용. (파일 저장 후에만 값 생김)
wq["H1"] = '=IFERROR(LEFT(CELL("filename",H1),FIND("[",CELL("filename",H1))-1),"")'
try:
    from openpyxl.workbook.defined_name import DefinedName
    dn = DefinedName("폴더경로", attr_text="'⑥PowerQuery(윈도우)'!$H$1")
    try: wb.defined_names.add(dn)
    except Exception: wb.defined_names["폴더경로"] = dn
except Exception as e:
    print("  명명(폴더경로) 실패:", e)
M = f'''let
    폴더 = Excel.CurrentWorkbook(){{[Name="폴더경로"]}}[Content]{{0}}[Column1],   // 이 워크북이 있는 폴더
    파일들 = Folder.Files(폴더),
    RM후보 = Table.SelectRows(파일들, each Text.Contains([Name], "Meeting") and Text.EndsWith(Text.Lower([Name]), ".xlsx") and not Text.StartsWith([Name], "~$")),
    최신 = Table.Sort(RM후보, {{{{"Date modified", Order.Descending}}}}),   // 같은 폴더의 가장 최신 RM 자동
    Src = Excel.Workbook(최신{{0}}[Content], null, true),
    당월 = Src{{[Item="{RM_SHEET[CUR]}", Kind="Sheet"]}}[Data],
    익월 = Src{{[Item="{RM_SHEET[NXT]}", Kind="Sheet"]}}[Data],
    셀 = (t, r, c) => Record.Field(t{{r-1}}, "Column" & Text.From(c)),
    만들기 = (t, 월) => {{
        [세그="Inbound", 월=월, 목표=셀(t,{SEG_ROW["Inbound"]},{COL_TGT}), FCST=셀(t,{SEG_ROW["Inbound"]},{COL_FCST}), 전년=셀(t,{SEG_ROW["Inbound"]},{COL_LY}), 객실수=셀(t,{SEG_ROW["Inbound"]},{COL_RN})],
        [세그="OTA",     월=월, 목표=셀(t,{SEG_ROW["OTA"]},{COL_TGT}), FCST=셀(t,{SEG_ROW["OTA"]},{COL_FCST}), 전년=셀(t,{SEG_ROW["OTA"]},{COL_LY}), 객실수=셀(t,{SEG_ROW["OTA"]},{COL_RN})],
        [세그="G-OTA",   월=월, 목표=셀(t,{SEG_ROW["G-OTA"]},{COL_TGT}), FCST=셀(t,{SEG_ROW["G-OTA"]},{COL_FCST}), 전년=셀(t,{SEG_ROW["G-OTA"]},{COL_LY}), 객실수=셀(t,{SEG_ROW["G-OTA"]},{COL_RN})]
    }},
    결합 = 만들기(당월, "{CUR}") & 만들기(익월, "{NXT}"),
    표 = Table.FromRecords(결합)
in
    표'''
lines = [
 "■ 윈도우 엑셀: 같은 폴더의 RM 엑셀 → ②[A] 자동로드 (경로수정 불필요, 새로고침만)",
 "배포법: 이 워크북 + RM 엑셀을 '같은 폴더'에 두고 받는 사람은 새로고침만.",
 "설정(1회): 데이터 → 새 쿼리 → 기타 원본에서 → 빈 쿼리 → 홈 → 고급 편집기 →",
 "           아래 M 전체 붙여넣기 → 완료 → 닫기및로드(다음으로) → 표 → 기존시트 '②RM입력(주간)'!$A$3.",
 "주의1: 이 워크북을 반드시 '저장'해야 폴더경로가 잡힘(새 통합문서 상태면 안 됨).",
 "주의2: 완료 시 개인정보/자격증명 물으면 '공용/조직' 허용, 개인정보수준은 '무시(결합 허용)'.",
 "동작: 같은 폴더에서 이름에 'Meeting' 들어간 .xlsx 중 가장 최신본을 자동 사용(파일명 안 바꿔도 됨).",
 "좌표 근거: 2026.MM '전체 주요지표' — Inbound=19행, OTA=22행, G-OTA=23행 /",
 "           목표=I열, FCST=N열, 전년(2025Actual)=AG열, FCST객실수=K열.",
 "────────────────────────────────────────────────────────",
]
for i, t in enumerate(lines, 1):
    wq.cell(i, 2, t)   # B열(H1 명명셀과 분리)
wq.cell(1, 2).font = BOLD
for j, ln in enumerate(M.split("\n"), len(lines) + 1):
    wq.cell(j, 2, ln)

# ⑦ 안내
wg = wb.create_sheet("⑦안내")
guide = [
 "■ 주간회의 예상매출현황 — 담당자 사용법",
 f"당월 {CUR} · 익월 {NXT}",
 "",
 "1) (윈도우) 이 워크북과 'RM 엑셀'을 같은 폴더에 두고 → [데이터 → 모두 새로고침].",
 "   → ②[A](세그 회사전체 목표/FCST/전년/객실수)가 같은 폴더 최신 RM에서 자동 갱신(경로수정 불필요).",
 "   (수동) PQ 없이 쓰려면 ②[A] 파랑칸에 RM 전체 주요지표 값 직접 입력.",
 "2) ①예상매출현황 GS 섹션(GS/OTA/G-OTA/Inbound)이 자동 계산 → PPT 테이블에 붙여넣기.",
 "   Forecast = 객실 FCST + 소사업(=FCST객실수×소사업실당 ÷1000).",
 "3) 마케팅/MICE/레저/운영 행은 ① 노란칸에 마케팅팀 숫자 입력(소계 자동).",
 "4) 소사업 실당단가(②[B] 연녹칸·고정) = 회사평균, 2026 실적 부대비중 기반. 필요시 수정.",
 "",
 "※ 월 변경/2027 목표 전환 시 재생성 요청(python scripts/build_weekly_selfserve.py YYYY-MM [RM경로]).",
 "※ 단위: 객실수=실, ADR=천원, 매출=백만(VAT제외). 대외비.",
]
for i, t in enumerate(guide, 1):
    wg.cell(i, 1, t)
wg.cell(1, 1).font = BOLD

for i, wd in enumerate([10, 10] + [9] * 14, 1): w1.column_dimensions[L(i)].width = wd
for i, wd in enumerate([9, 9, 14, 15, 14, 12], 1): w2.column_dimensions[L(i)].width = wd
for i, wd in enumerate([9, 13, 9, 11, 11, 10, 11, 16], 1): w5.column_dimensions[L(i)].width = wd
wq.column_dimensions["A"].width = 10; wq.column_dimensions["B"].width = 120
wg.column_dimensions["A"].width = 100

wb.save(OUT)
print(f"저장: {OUT}  (RM prefill: {'O' if PRE else 'X(0)'})")
for mo in MONTHS:
    for s in SEGS:
        tgt = pre(s, mo, "tgt", round(seg_company(s, mo, "rm_budget_rev_mil"), 1))
        fc = pre(s, mo, "fcst", round(seg_company(s, mo, "rm_fcst_rev_mil"), 1))
        rn = pre(s, mo, "rn", seg_company(s, mo, "rm_fcst_rn"))
        sos = rn * sosaup_rate(s, mo) / 1000
        print(f"  {mo} {s:8} 목표{tgt:8,.0f} FCST{fc:8,.0f} +소사업{sos:7,.0f} = {fc+sos:8,.0f}")
