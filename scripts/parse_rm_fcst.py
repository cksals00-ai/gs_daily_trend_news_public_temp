#!/usr/bin/env python3
"""
parse_rm_fcst.py
================

Parses a Revenue Meeting PDF and writes per-property forecast values to
data/rm_fcst.json.

Backend: pypdfium2 (~50x faster than pdfplumber). Pure-Python, no system deps.

Each property page has a 주요 지표 table with Budget / Forecast columns.
We extract:
  - Grand Total (last data row in the 주요 지표 section)
  - Per-segment cells: OTA, G-OTA (FIT), Inbound (Group)
    Each: Budget RN, Budget REV(M), Forecast RN, Forecast REV(M) — PDF원본, 분배 X.

When run on the latest PDF, writes data/rm_fcst.json + docs/data/rm_fcst.json.

The `parse(pdf_path)` function returns the dict and is reused by
build_fcst_trend.py for historical PDFs.
"""

import json
import fs_utils  # macOS NFD→NFC 유니코드 정규화
import re
import sys
import warnings
from datetime import datetime
from pathlib import Path

import pypdfium2 as pdfium

warnings.filterwarnings("ignore")

REPO_ROOT = Path(__file__).resolve().parent.parent
PDF_DIR = REPO_ROOT / "data" / "RM자료"
OUT_PATH = REPO_ROOT / "data" / "rm_fcst.json"
DOCS_OUT_PATH = REPO_ROOT / "docs" / "data" / "rm_fcst.json"

NAME_MAP = {
    "소노벨_비발디파크":      "01.벨비발디",
    "소노캄_비발디파크":      "02.캄비발디",
    "소노펫_비발디파크":      "03.펫비발디",
    "소노펠리체_비발디파크":   "04.펠리체비발디",
    "소노빌리지_비발디파크":   "05.빌리지비발디",
    "소노벨_양평":            "06.양평",
    "델피노":                "07.델피노",
    "쏠비치_양양":            "08.쏠비치양양",
    "쏠비치_삼척":            "09.쏠비치삼척",
    "소노벨_단양":            "10.소노벨단양",
    "소노캄_경주":            "11.소노캄경주",
    "소노벨_청송":            "12.소노벨청송",
    "소노벨_천안":            "13.소노벨천안",
    "소노벨_변산":            "14.소노벨변산",
    "소노캄_여수":            "15.소노캄여수",
    "소노캄_거제":            "16.소노캄거제",
    "쏠비치_진도":            "17.쏠비치진도",
    "소노벨_제주":            "18.소노벨제주",
    "소노캄_제주":            "19.소노캄제주",
    "소노캄_고양":            "20.소노캄고양",
    "소노문_해운대":           "21.소노문해운대",
    "쏠비치_남해":            "22.쏠비치남해",
    "르네블루":               "23.르네블루",
    "르네블루 바이 쏠비치":     "23.르네블루",
}

# Regions in older PDFs may use slightly different naming
REGION_NAMES = {"비발디", "한국중부", "아시아퍼시픽", "한국남부", "한중국부"}

# ── 동기간(전년/25년) OTB 추출용 ───────────────────────────────────────────
# 월별 요약 페이지 하단의 "● Segment별 전년대비 OTB 현황 (R/N)" → 두 번째 표
# (홈페이지 / OTA / G-OTA / 제휴사 / (일반)기타 / Comp) 의 사업장별 25년 OTB 컬럼.
# pypdfium2는 이 표를 컬럼-스크램블해서 읽으므로 pdfplumber layout 모드를 사용.
SEG_TABLE_NAME_MAP = {
    "벨 비발디파크":     "01.벨비발디",
    "캄 비발디파크":     "02.캄비발디",
    "펫 비발디파크":     "03.펫비발디",
    "소노펠리체 비발디":  "04.펠리체비발디",
    "소노빌리지 비발디":  "05.빌리지비발디",
    "소노벨 양평":       "06.양평",
    "델피노":           "07.델피노",
    "쏠비치 양양":       "08.쏠비치양양",
    "쏠비치 삼척":       "09.쏠비치삼척",
    "소노벨 단양":       "10.소노벨단양",
    "소노캄 경주":       "11.소노캄경주",
    "소노벨 청송":       "12.소노벨청송",
    "소노벨 천안":       "13.소노벨천안",
    "소노벨 변산":       "14.소노벨변산",
    "소노캄 여수":       "15.소노캄여수",
    "소노캄 거제":       "16.소노캄거제",
    "쏠비치 진도":       "17.쏠비치진도",
    "소노벨 제주":       "18.소노벨제주",
    "소노캄 제주":       "19.소노캄제주",
    "소노캄 고양":       "20.소노캄고양",
    "소노문 해운대":      "21.소노문해운대",
    "쏠비치 남해":       "22.쏠비치남해",
    "르네블루":         "23.르네블루",
}
_SEG_NAMES_BY_LEN = sorted(SEG_TABLE_NAME_MAP, key=len, reverse=True)
# 두 번째 표 컬럼 순서: [홈페이지, OTA, G-OTA, 제휴사, (일반)기타, Comp]
_SEG_TABLE_COL_IDX = {"OTA": 1, "G-OTA": 2}


def _nfc(s: str) -> str:
    import unicodedata
    return unicodedata.normalize("NFC", s)


def _parse_25_token(tok: str):
    """25년(동기간) 셀 토큰을 int로. '-' (신규 사업장 등)는 None."""
    tok = tok.strip()
    if tok in ("-", ""):
        return None
    try:
        return int(tok.replace(",", ""))
    except ValueError:
        return None


def _parse_seg_table_row(line: str):
    """두 번째 Segment OTB 표의 한 행을 파싱.
    각 컬럼 셀은 '<사업장명> <26년> <25년> <증감율>' 형태로, 사업장명이 컬럼마다 반복됨.
    Returns: (canonical_name, {seg: ly25_rn or None}) 또는 None.
    """
    line = _nfc(line).strip()
    name = next((d for d in _SEG_NAMES_BY_LEN if line.startswith(d)), None)
    if not name:
        return None
    parts = [p.strip() for p in line.split(name) if p.strip()]
    cols = [p.split() for p in parts]
    out = {}
    for seg, idx in _SEG_TABLE_COL_IDX.items():
        if len(cols) > idx and len(cols[idx]) >= 2:
            out[seg] = _parse_25_token(cols[idx][1])  # [0]=26년, [1]=25년
        else:
            out[seg] = None
    return SEG_TABLE_NAME_MAP[name], out


def extract_same_period(pdf_path: Path) -> dict:
    """월별 요약 페이지에서 사업장별 OTA/G-OTA 동기간(25년) OTB(R/N)를 추출.
    Returns: {month_int: {canonical_name: {"OTA": rn, "G-OTA": rn}}}.
    pdfplumber 미설치/구형 PDF 등으로 실패하면 빈 dict 반환(파이프라인 비차단).
    """
    try:
        import pdfplumber
    except Exception:
        return {}

    result: dict[int, dict] = {}
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                txt = page.extract_text(layout=True) or ""
                txt = _nfc(txt)
                mm = re.search(r"\[(\d+)월\]", txt)
                if not mm:
                    continue
                month = int(mm.group(1))
                lines = txt.split("\n")
                # 두 번째 표(홈페이지/OTA/G-OTA/...) 헤더 위치 탐색
                hidx = next(
                    (i for i, l in enumerate(lines)
                     if "홈페이지" in l and "OTA" in l and "G-OTA" in l),
                    None,
                )
                if hidx is None:
                    continue
                month_map = result.setdefault(month, {})
                for l in lines[hidx + 1:]:
                    parsed = _parse_seg_table_row(l)
                    if parsed:
                        canon, segs = parsed
                        month_map[canon] = segs
    except Exception as e:
        warnings.warn(f"extract_same_period failed: {e}")
        return {}
    return result


def find_latest_pdf() -> Path:
    pdfs = sorted(PDF_DIR.glob("Revenue Meeting_*.pdf"))
    if not pdfs:
        sys.exit(f"No Revenue Meeting PDFs found in {PDF_DIR}")
    return pdfs[-1]


def extract_pages(pdf_path: Path) -> list[str]:
    doc = pdfium.PdfDocument(str(pdf_path))
    out = []
    for i in range(len(doc)):
        try:
            txt = doc[i].get_textpage().get_text_range() or ""
        except Exception:
            txt = ""
        out.append(txt)
    return out


# Match segment row at line start (no FIT/Group prefix in pypdfium2 output)
SEG_PATTERNS = {
    "OTA":     re.compile(r"^OTA\s+\d"),
    "G-OTA":   re.compile(r"^G-OTA\s+\d"),
    "Inbound": re.compile(r"^Inbound\s+\d"),
}

NUM_TOKEN = re.compile(r"\d+(?:,\d{3})*(?:\.\d+)?%?")


def _parse_10_tokens(s: str):
    """Extract first 10 numeric tokens. Layout:
      [0-4]=Budget {RN, RN%, ADR, REV, REV%}
      [5-9]=Forecast {RN, RN%, ADR, REV, REV%}
    """
    tokens = NUM_TOKEN.findall(s)
    if len(tokens) < 10:
        return None
    nums = []
    for t in tokens[:10]:
        t = t.replace(",", "").rstrip("%")
        try:
            v = float(t)
            nums.append(int(v) if v == int(v) else v)
        except ValueError:
            return None
    return nums


def _to_rec(nums) -> dict:
    return {
        "rm_budget_rn":      int(nums[0]),
        "rm_budget_adr":     int(nums[2]),
        "rm_budget_rev_mil": int(nums[3]),
        "rm_fcst_rn":        int(nums[5]),
        "rm_fcst_adr":       int(nums[7]),
        "rm_fcst_rev_mil":   int(nums[8]),
    }


def detect_property_name(page_text: str) -> str | None:
    """In pypdfium2 the '▣ <name> #N' header may appear anywhere on the page.
    Find the marker that maps to a known property or region (_Total).
    """
    for m in re.finditer(r"▣\s+(.+?)(?:\s+#\d+|[\r\n])", page_text):
        cand = m.group(1).strip()
        if not cand:
            continue
        # Skip the section/box headers that don't represent a property.
        if any(skip in cand for skip in (
            "주요 지표", "Pick up", "추가", "일별", "주단위", "추이",
            "Segment", "특이사항", "Forecast",
        )):
            continue
        if cand in NAME_MAP or cand.endswith("_Total"):
            return cand

    # Fallback: 2026.05+ PDFs use "#N\r\n<name>\r\nSegment" format
    for m in re.finditer(r"#\d+\r?\n(.+?)\r?\n", page_text):
        cand = m.group(1).strip()
        if not cand or cand == "Segment":
            continue
        if cand in NAME_MAP or cand in REGION_NAMES:
            return cand
    return None


def detect_month(page_text: str) -> int | None:
    """Property pages contain row prefix like 'N월 Homepage' (RN-style chart)
    or `[N월]` markers in summary pages. With pypdfium2 the 'N월' prefix on
    Homepage may be lost — fall back to scanning the full text for 'N월' tokens
    that appear immediately before well-known segment/labels.
    """
    # 1) "[N월]" marker
    m = re.search(r"\[(\d+)월\]", page_text)
    if m:
        return int(m.group(1))
    # 2) "N월" prefix on Homepage line (older PDFs)
    m = re.search(r"(\d+)월\s+Homepage", page_text)
    if m:
        return int(m.group(1))
    return None


def parse_property_page(page_text: str) -> dict | None:
    name = detect_property_name(page_text)
    if not name:
        return None

    # Restrict to the main 주요 지표 section
    main = page_text.split("▣ Pick up Trend")[0]
    lines = [l.strip() for l in main.split("\n") if l.strip()]

    # Detect segment rows
    segments: dict[str, dict] = {}
    for line in lines:
        for seg_name, pat in SEG_PATTERNS.items():
            if pat.match(line):
                nums = _parse_10_tokens(line)
                if nums:
                    segments[seg_name] = _to_rec(nums)
                break

    # Grand Total = the LAST data row in the main section that has 10 numeric
    # tokens and looks like a totals row (Budget RN >= 100 typically). In
    # pypdfium2 output the "Grand Total" label is usually missing from this row
    # (it appears on its own line elsewhere). We pick the last data row whose
    # second token is "100.0%" (RN composition = 100% since it's totals).
    grand = None
    for line in reversed(lines):
        if "100.0%" not in line:
            continue
        # also must contain at least 10 numeric tokens
        nums = _parse_10_tokens(line)
        if nums and len(nums) >= 10:
            grand = _to_rec(nums)
            break

    if grand is None:
        return None

    return {"name": name, "grand": grand, "segments": segments}


def parse(pdf_path: Path) -> dict:
    pages = extract_pages(pdf_path)

    # Detect months from summary pages (have [N월] marker)
    detected_months = []
    for p in pages:
        for m in re.finditer(r"\[(\d+)월\]", p[:300]):
            mo = int(m.group(1))
            if mo not in detected_months:
                detected_months.append(mo)

    # Property/region pages: those with ▣ <name> #N marker.
    # PDF layout: per-month section = [region totals × R + property pages × P].
    # To assign month: when we see a property name we've seen before, advance
    # to the next detected month (same approach as the legacy parser).
    rows = []
    for i, page in enumerate(pages):
        rec = parse_property_page(page)
        if not rec:
            continue
        rows.append({"page": i + 1, **rec})

    # Fallback: 2026.05+ PDFs drop the [N월] cover pages and instead embed
    # "YYYY년 N월" inline on every property page. Scan pages in order and
    # collect unique months by first appearance.
    if not detected_months:
        for p in pages:
            m = re.search(r"\d{4}년\s*(\d{1,2})월", p)
            if m:
                mo = int(m.group(1))
                if mo not in detected_months:
                    detected_months.append(mo)

    if not detected_months:
        return {
            "_source_pdf":     pdf_path.name,
            "_extracted_at":   datetime.now().isoformat() + "Z",
            "_snapshot_date":  pdf_path.stem.split("_")[-1],
            "_months_covered": [],
            "_validation":     {"unmapped_pdf_names": []},
            "regions":         {},
            "properties":      {},
        }

    # Assign months by section (advance when same name appears again)
    section_idx = 0
    seen_in_section: set[str] = set()
    for r in rows:
        if r["name"] in seen_in_section:
            section_idx += 1
            seen_in_section = set()
        seen_in_section.add(r["name"])
        if section_idx < len(detected_months):
            r["month"] = detected_months[section_idx]
        else:
            r["month"] = None

    # Determine the base year from the pdf filename for ym keys.
    # detected_months may wrap (e.g. 12, 1, 2) → bump year on wrap.
    fy = re.search(r"(\d{4})\.\d{2}\.\d{2}", pdf_path.name)
    base_year = int(fy.group(1)) if fy else datetime.now().year
    months_with_year = []
    cy = base_year
    prev_m = None
    for mo in detected_months:
        if prev_m is not None and mo < prev_m:
            cy += 1
        months_with_year.append((cy, mo))
        prev_m = mo

    properties: dict = {}
    regions: dict = {}
    unmapped = []

    for r in rows:
        if r["month"] is None:
            continue
        # Locate (year, month) for this row's section
        sec = next((i for i, (_y, mo) in enumerate(months_with_year) if mo == r["month"]), None)
        if sec is None:
            continue
        cy, mo = months_with_year[sec]
        ym = f"{cy}-{mo:02d}"
        record = {**r["grand"], "segments": r["segments"]}
        nm = r["name"]
        if nm.endswith("_Total"):
            regions.setdefault(nm.replace("_Total", ""), {})[ym] = record
            continue
        canonical = NAME_MAP.get(nm)
        if not canonical:
            unmapped.append(nm)
            continue
        properties.setdefault(canonical, {})[ym] = record

    # 동기간(전년/25년) OTB 병합: 월별 요약 페이지의 OTA/G-OTA 25년 컬럼 →
    # properties[canonical][ym]["segments"][seg]["ly_same_period_rn"]
    same_period = extract_same_period(pdf_path)  # {month_int: {canonical: {seg: rn}}}
    sp_merged = 0
    for mo, prop_map in same_period.items():
        sec = next((i for i, (_y, m) in enumerate(months_with_year) if m == mo), None)
        if sec is None:
            continue
        cy, _ = months_with_year[sec]
        ym = f"{cy}-{mo:02d}"
        for canon, segs in prop_map.items():
            prop_ym = properties.setdefault(canon, {}).setdefault(ym, {})
            seg_block = prop_ym.setdefault("segments", {})
            for seg, rn in segs.items():
                if rn is None:
                    continue
                seg_block.setdefault(seg, {})["ly_same_period_rn"] = rn
                sp_merged += 1

    # Validation
    val = {}
    for ym in sorted({y for p in properties.values() for y in p}):
        sum_grand_rn = sum(p[ym]["rm_fcst_rn"] for p in properties.values() if ym in p)
        sum_grand_rev = sum(p[ym]["rm_fcst_rev_mil"] for p in properties.values() if ym in p)
        sum_seg_rn = sum(
            sum(p[ym]["segments"].get(s, {}).get("rm_fcst_rn", 0) for s in ("OTA", "G-OTA", "Inbound"))
            for p in properties.values() if ym in p
        )
        sum_seg_rev = sum(
            sum(p[ym]["segments"].get(s, {}).get("rm_fcst_rev_mil", 0) for s in ("OTA", "G-OTA", "Inbound"))
            for p in properties.values() if ym in p
        )
        val[ym] = {
            "sum_property_grand_rn":  sum_grand_rn,
            "sum_property_grand_rev": sum_grand_rev,
            "sum_property_seg_rn":    sum_seg_rn,
            "sum_property_seg_rev":   sum_seg_rev,
        }

    return {
        "_source_pdf":      pdf_path.name,
        "_extracted_at":    datetime.now().isoformat() + "Z",
        "_snapshot_date":   pdf_path.stem.split("_")[-1],
        "_months_covered":  [f"{cy}-{mo:02d}" for cy, mo in months_with_year],
        "_units":           {"rn": "실 (rooms)", "rev_mil": "백만원 (million KRW)"},
        "_field_meaning":   ("rm_fcst_rn / rm_fcst_rev_mil = Revenue Meeting Forecast (Grand Total). "
                             "segments[OTA|G-OTA|Inbound] = same-PDF per-segment Budget+Forecast cells (no distribution)."),
        "_validation":      {**val, "unmapped_pdf_names": sorted(set(unmapped))},
        "regions":          regions,
        "properties":       properties,
    }


def main() -> int:
    pdf = find_latest_pdf()
    print(f"Latest PDF: {pdf.name}")
    out = parse(pdf)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_PATH.relative_to(REPO_ROOT)} ({len(out['properties'])} properties × {len(out['regions'])} regions)")

    DOCS_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    DOCS_OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {DOCS_OUT_PATH.relative_to(REPO_ROOT)}")

    print("\nValidation by month:")
    for ym, v in out["_validation"].items():
        if ym == "unmapped_pdf_names":
            continue
        print(f"  {ym}: grand_rn={v['sum_property_grand_rn']:,}  grand_rev={v['sum_property_grand_rev']:,}M  "
              f"seg(O+G+I)_rn={v['sum_property_seg_rn']:,}  seg(O+G+I)_rev={v['sum_property_seg_rev']:,}M")

    if out["_validation"]["unmapped_pdf_names"]:
        print(f"  WARNING: unmapped: {out['_validation']['unmapped_pdf_names']}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
