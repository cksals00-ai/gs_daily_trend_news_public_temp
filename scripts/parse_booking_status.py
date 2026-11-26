#!/usr/bin/env python3
"""Booking Status Report(사업장별 페이지) → docs/data/booking_status_detail.json

사업장 × 투숙월: 당일 온북(RN·OCC·ADR·매출) + 목표(RN·OCC·ADR·매출).
단위는 PDF 그대로 — ADR=원, 매출=천원(VAT 제외).

요약표만 읽는 parse_daily_booking.py 산출물(daily_booking.json)에는 온북 ADR·매출이 없어서
품의 페이지(pumui.html)가 '전체' 온북 ADR을 목표 ADR로 대신 쓰고 있었다.
이 파일로 사업장 페이지의 실제 온북 ADR·매출을 채운다.

사용:
  python3 scripts/parse_booking_status.py                       # 최신 Booking Status Report
  python3 scripts/parse_booking_status.py "<PDF 경로>"
parse_daily_booking.py 가 같은 날짜의 Booking Status Report 를 찾아 자동 호출한다.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PDF_DIR = ROOT / "data" / "Daily Booking Report PDF"
OUT = ROOT / "docs" / "data" / "booking_status_detail.json"

ROW = re.compile(r"^(\d{4})년\s*(\d{1,2})월\s+([\d,]+)\s+(-?[\d.]+)%\s+(-?[\d,]+)\s+(-?[\d,]+)")
HDR = re.compile(r"\[(.+?)\]\s*#(\d+)")
ISSUE = re.compile(r"Issue Date\s*:\s*(\d{4})\.(\d{2})\.(\d{2})")
DATE_IN_NAME = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})")


def norm(name: str) -> str:
    """사업장명 정규화: ' 바이 …'/' by …' 접미사와 공백 제거 (PDF·daily_booking 표기 차이 흡수)."""
    name = re.sub(r"\s*(바이|by)\s.*$", "", name.strip(), flags=re.I)
    return re.sub(r"\s+", "", name)


def _int(s: str) -> int:
    return int(s.replace(",", ""))


def parse(pdf_path) -> dict:
    import pdfplumber

    out = {"report_date": None, "source_pdf": Path(pdf_path).name,
           "units": {"adr": "원", "rev_k": "천원(VAT 제외)", "occ": "%"},
           "properties": {}}
    with pdfplumber.open(str(pdf_path)) as pdf:
        for pg in pdf.pages:
            lines = (pg.extract_text() or "").splitlines()
            if not lines:
                continue
            h = HDR.search(lines[0])
            if not h:
                continue
            name = h.group(1).strip()
            if out["report_date"] is None:
                for ln in lines[:3]:
                    mi = ISSUE.search(ln)
                    if mi:
                        out["report_date"] = f"{mi.group(1)}-{mi.group(2)}-{mi.group(3)}"
                        break
            sec, months = None, {}
            for ln in lines[1:]:
                s = ln.strip()
                if sec is None and s.startswith("Monthly"):
                    sec = "otb"          # 1번째 표: 당일 온북 (좌측 4열)
                    continue
                if re.match(r"^\d{4}\s*Budget", s):
                    sec = "budget"       # 2번째 표: 목표 (좌측 4열)
                    continue
                if s.startswith("Membership"):
                    break                # 이후는 세그·전년 표 — 사용 안 함
                m = ROW.match(s)
                if m and sec in ("otb", "budget"):
                    y, mo, rn, occ, adr, rev = m.groups()
                    ym = f"{y}-{int(mo):02d}"
                    months.setdefault(ym, {})
                    months[ym].setdefault(sec, {"rn": _int(rn), "occ": float(occ),
                                                "adr": _int(adr), "rev_k": _int(rev)})
            if months:
                out["properties"][norm(name)] = {"name": name, "months": months}
    return out


def latest_pdf():
    cands = []
    for p in PDF_DIR.glob("Booking Status Report_*.pdf"):
        m = DATE_IN_NAME.search(p.name)
        if m:
            cands.append(("".join(m.groups()), p))
    return max(cands)[1] if cands else None


def main():
    pdf = Path(sys.argv[1]) if len(sys.argv) > 1 else latest_pdf()
    if not pdf or not pdf.exists():
        print("Booking Status Report PDF 없음 — 스킵", file=sys.stderr)
        sys.exit(1)
    res = parse(pdf)
    n = len(res["properties"])
    if n < 20:   # 사업장 페이지가 대부분 안 읽히면 기존 파일을 덮지 않는다(회귀 방지)
        print(f"사업장 {n}개만 파싱됨 — 저장 안 함", file=sys.stderr)
        sys.exit(2)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"✓ {OUT.relative_to(ROOT)} — {res['report_date']} · 사업장 {n}개")


if __name__ == "__main__":
    main()
