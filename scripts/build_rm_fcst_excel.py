#!/usr/bin/env python3
"""
build_rm_fcst_excel.py
======================

data/rm_fcst.json → data/RM_FCST_정리.xlsx (+ docs/data 동기화) 재생성.

주간업무보고 'RM FCST 정리' 버튼이 내려주는 정적 엑셀을 rm_fcst.json 기준으로
다시 만든다. 대상월 = 당월(현재 달). rm_fcst.json에 당월이 없으면 _months_covered
첫 월로 폴백.

시트 3개:
  - 사업장별 총괄: 23사업장 + 권역 4행([비발디]/[한국중부]/[아시아퍼시픽]/[한국남부])
  - 세그먼트별 상세: 사업장×(OTA/G-OTA/Inbound)
  - 메타정보

단위: RN=실, ADR=천원, 매출=백만원(VAT제외). FCST/예산 = FCST RN / 예산 RN × 100.
"""

import json
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "data" / "rm_fcst.json"
OUT = REPO / "data" / "RM_FCST_정리.xlsx"
DOCS_OUT = REPO / "docs" / "data" / "RM_FCST_정리.xlsx"

SEGS = ["OTA", "G-OTA", "Inbound"]
REGION_LABELS = {
    "비발디": "[비발디]",
    "한국중부": "[한국중부]",
    "아시아퍼시픽": "[아시아퍼시픽]",
    "한국남부": "[한국남부]",
}

HDR_FILL = PatternFill("solid", fgColor="2F2F2F")
HDR_FONT = Font(bold=True, color="FFFFFF", size=10)
TITLE_FONT = Font(bold=True, size=12)
BOLD = Font(bold=True, size=10)
THIN = Side(style="thin", color="D0D0D0")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
CENTER = Alignment(horizontal="center", vertical="center")
RIGHT = Alignment(horizontal="right", vertical="center")


def _ratio(fcst_rn, bud_rn):
    return round(fcst_rn / bud_rn * 100, 1) if bud_rn else 0


def _style_header(ws, row, ncol):
    for c in range(1, ncol + 1):
        cell = ws.cell(row=row, column=c)
        cell.fill = HDR_FILL
        cell.font = HDR_FONT
        cell.alignment = CENTER
        cell.border = BORDER


def main():
    d = json.loads(SRC.read_text(encoding="utf-8"))
    props = d["properties"]
    regions = d.get("regions", {})
    covered = d.get("_months_covered", [])

    cur = datetime.now().strftime("%Y-%m")
    target = cur if cur in covered else (covered[0] if covered else cur)

    wb = openpyxl.Workbook()

    # ── 시트1: 사업장별 총괄 ──
    ws = wb.active
    ws.title = "사업장별 총괄"
    ws["A1"] = f"RM FCST - {target}"
    ws["A1"].font = TITLE_FONT
    hdr = ["사업장", "예산 RN", "예산 ADR", "예산 매출(백만)",
           "FCST RN", "FCST ADR", "FCST 매출(백만)", "FCST/예산"]
    ws.append([])  # row2 placeholder via direct write
    for i, h in enumerate(hdr, 1):
        ws.cell(row=2, column=i, value=h)
    _style_header(ws, 2, len(hdr))

    def total_row(label, node):
        return [label, node["rm_budget_rn"], node["rm_budget_adr"], node["rm_budget_rev_mil"],
                node["rm_fcst_rn"], node["rm_fcst_adr"], node["rm_fcst_rev_mil"],
                _ratio(node["rm_fcst_rn"], node["rm_budget_rn"])]

    r = 3
    for name in props:
        node = props[name].get(target)
        if not node:
            continue
        for i, v in enumerate(total_row(name, node), 1):
            ws.cell(row=r, column=i, value=v)
        r += 1
    r += 1  # blank separator
    for rkey, label in REGION_LABELS.items():
        node = regions.get(rkey, {}).get(target)
        if not node:
            continue
        for i, v in enumerate(total_row(label, node), 1):
            cell = ws.cell(row=r, column=i, value=v)
            cell.font = BOLD
        r += 1

    # ── 시트2: 세그먼트별 상세 ──
    ws2 = wb.create_sheet("세그먼트별 상세")
    ws2["A1"] = f"RM FCST 세그먼트별 - {target}"
    ws2["A1"].font = TITLE_FONT
    hdr2 = ["사업장", "세그먼트", "예산 RN", "예산 ADR", "예산 매출(백만)",
            "FCST RN", "FCST ADR", "FCST 매출(백만)", "FCST/예산", "차이 RN"]
    for i, h in enumerate(hdr2, 1):
        ws2.cell(row=2, column=i, value=h)
    _style_header(ws2, 2, len(hdr2))

    r = 3
    for name in props:
        node = props[name].get(target)
        if not node:
            continue
        segd = node.get("segments", {})
        for seg in SEGS:
            s = segd.get(seg, {})
            brn = s.get("rm_budget_rn", 0)
            frn = s.get("rm_fcst_rn", 0)
            row = [name, seg, brn, s.get("rm_budget_adr", 0), s.get("rm_budget_rev_mil", 0),
                   frn, s.get("rm_fcst_adr", 0), s.get("rm_fcst_rev_mil", 0),
                   _ratio(frn, brn), frn - brn]
            for i, v in enumerate(row, 1):
                ws2.cell(row=r, column=i, value=v)
            r += 1

    # ── 시트3: 메타정보 ──
    ws3 = wb.create_sheet("메타정보")
    meta = [
        ("소스 PDF", d.get("_source_pdf", "")),
        ("추출일시", d.get("_extracted_at", "")),
        ("스냅샷일자", d.get("_snapshot_date", "")),
        ("대상월", target),
        ("비고", "OTA/G-OTA/Inbound 세그먼트 Forecast / 단위: RN=실, ADR=천원, 매출=백만원, VAT제외"),
    ]
    for i, (k, v) in enumerate(meta, 1):
        ws3.cell(row=i, column=1, value=k).font = BOLD
        ws3.cell(row=i, column=2, value=v)

    # ── 서식: 숫자 우측정렬 + 열너비 ──
    # FCST/예산 컬럼만 소수1자리: 총괄 8열, 세그 9열.
    ratio_col = {id(ws): 8, id(ws2): 9}
    for sheet, ncol in ((ws, 8), (ws2, 10)):
        rcol = ratio_col[id(sheet)]
        for col in range(2, ncol + 1):
            for row_cells in sheet.iter_rows(min_row=3, min_col=col, max_col=col):
                for cell in row_cells:
                    if isinstance(cell.value, (int, float)):
                        cell.alignment = RIGHT
                        cell.number_format = "#,##0.0" if cell.column == rcol else "#,##0"
        sheet.column_dimensions["A"].width = 16
    ws.column_dimensions["H"].width = 10
    ws2.column_dimensions["B"].width = 10
    ws3.column_dimensions["A"].width = 12
    ws3.column_dimensions["B"].width = 50

    wb.save(OUT)
    wb.save(DOCS_OUT)
    print(f"✓ RM_FCST_정리.xlsx 재생성 (대상월={target}, 사업장={sum(1 for n in props if props[n].get(target))})")
    print(f"  → {OUT}")
    print(f"  → {DOCS_OUT}")


if __name__ == "__main__":
    main()
