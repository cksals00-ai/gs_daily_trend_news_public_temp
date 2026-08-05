#!/usr/bin/env python3
"""
parse_homepage_rm.py
====================
Revenue Meeting PDF(data/RM자료/*.pdf 최신본)에서 '홈페이지' 세그의
① 주요지표(Budget/Forecast/달성률: RN·ADR·Revenue)
② Pick up Trend(주차별 OTB, 26년 vs 25년)
를 파싱해 docs/data/homepage_rm.json 으로 저장.

주 1회 Revenue Meeting 반영 파이프라인에서 호출.
의존: pip install pdfplumber
"""
import pdfplumber, re, unicodedata, json, sys, glob, os
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PDF_DIR = REPO / "data" / "RM자료"
OUT = REPO / "docs" / "data" / "homepage_rm.json"

nfc = lambda s: unicodedata.normalize("NFC", s)


def _toks(s):
    return s.replace("▼", "-").replace("▲", "").split()


def _to_num(t):
    t = t.replace(",", "").replace("%", "").replace("p", "")
    try:
        return float(t) if "." in t else int(t)
    except ValueError:
        return None


def parse(pdf_path):
    out = {"source": os.path.basename(pdf_path), "main": {}, "pickup": {}, "pickup_weeks": {}}
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages[:6]:  # 홈페이지 표는 앞쪽 요약 페이지
            t = nfc(page.extract_text(layout=True) or "")
            lines = t.split("\n")
            wk = [m for l in lines if "W-4" in l and "[" in l
                  for m in re.findall(r"\[(\d+월\d+일)\]", l)]
            for l in lines:
                if "Homepage" not in l and "홈페이지" not in l:
                    continue
                mo = re.match(r"\s*(\d+)월", l)
                month = int(mo.group(1)) if mo else None
                body = l.split("Homepage")[-1] if "Homepage" in l else l.split("홈페이지")[-1]
                nums = [x for x in (_to_num(t) for t in _toks(body)) if x is not None]
                if "%" in body:  # 주요지표
                    if month and len(nums) >= 15 and month not in out["main"]:
                        out["main"][month] = {
                            "budget_rn": nums[0], "budget_adr": nums[2], "budget_rev": nums[3],
                            "fcst_rn": nums[5], "fcst_adr": nums[7], "fcst_rev": nums[8],
                            "rn_ach": nums[11], "rev_ach": nums[14]}
                else:  # pickup (RN,ADR 페어 × 10)
                    if month and len(nums) >= 20 and month not in out["pickup"]:
                        out["pickup"][month] = {"rn_2026": [nums[j] for j in range(0, 10, 2)],
                                                "rn_2025": [nums[j] for j in range(10, 20, 2)]}
                        if wk:
                            out["pickup_weeks"][month] = wk[:5]
    return out


def find_latest():
    cands = []
    for p in PDF_DIR.glob("*.pdf"):
        m = re.search(r"(\d{4}\.\d{2}\.\d{2})", p.name)
        if m:
            cands.append((m.group(1), p))
    if not cands:
        sys.exit(f"No Revenue Meeting PDFs in {PDF_DIR}")
    cands.sort()
    return cands[-1][1]


def main():
    pdf = Path(sys.argv[1]) if len(sys.argv) > 1 else find_latest()
    data = parse(str(pdf))
    if not data["main"] and not data["pickup"]:
        print(f"⚠ 홈페이지 표를 찾지 못함: {pdf.name} — 기존 {OUT.name} 유지")
        return 0
    OUT.parent.mkdir(parents=True, exist_ok=True)
    json.dump(data, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"저장 완료 → {OUT}  (main {list(data['main'])}, pickup {list(data['pickup'])})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
