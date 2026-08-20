#!/usr/bin/env python3
"""
action_plan_perf.py — 인플루언서 현황 시트(패키지번호 O열) → 실적(객실수/매출/ADR)
  python3 scripts/action_plan_perf.py ~/Downloads/시트.csv
"""
from __future__ import annotations
import csv, json, os, re, sys
from pathlib import Path
from collections import defaultdict

SCRIPT_DIR  = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
CODE_RE = re.compile(r"\b86\d{6}\b")

# 시트를 매번 라이브로 읽는다 (generate_campaign_data.py 와 동일한 publish-to-web 방식)
PUBLISH_ID = ("2PACX-1vTqe7nY8vHYVVnnGR5qrl-uubCABXtmbToAuKWziuaoms14hZ3qlJuQTBWUXDmjCOU-4hd0hp6cpO_O")
GID = "2074970026"   # [GS] 2026 인플루언서 현황 보고의 건 (패키지번호 기재)
SHEET_CSV_URL = (f"https://docs.google.com/spreadsheets/d/e/{PUBLISH_ID}"
                 f"/pub?gid={GID}&single=true&output=csv")

def fetch_csv_rows(url: str, tries: int = 3):
    import urllib.request, io, time
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                text = r.read().decode("utf-8-sig", errors="replace")
            if "<html" in text[:400].lower():
                raise RuntimeError("CSV가 아닌 HTML 응답 — 시트 발행 상태 확인 필요")
            return list(csv.reader(io.StringIO(text)))
        except Exception as e:
            last = e; time.sleep(2 * (i + 1))
    raise RuntimeError(f"시트 다운로드 실패: {last}")

def find_raw_db() -> Path:
    for c in (PROJECT_DIR / "data" / "raw_db",
              PROJECT_DIR.parents[2] / "data" / "raw_db" if len(PROJECT_DIR.parents) > 2 else None):
        if c and c.exists():
            return c
    raise FileNotFoundError("raw_db 없음")

def load_sheet(src):
    if isinstance(src, Path):
        with open(src, newline="", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
    else:
        rows = fetch_csv_rows(src)
    print(f"  총 {len(rows)}행 수신")
    # 제목줄에도 "패키지번호"가 들어있으므로 '정확히 일치'하는 셀만 헤더로 인정
    code_col = hdr_row = None
    for r, row in enumerate(rows[:25]):
        for c, v in enumerate(row):
            if str(v).strip() == "패키지번호":
                code_col, hdr_row = c, r
                break
        if code_col is not None:
            break
    if code_col is None:
        cnt = defaultdict(int)
        for row in rows:
            for c, v in enumerate(row):
                cnt[c] += len(CODE_RE.findall(str(v)))
        if not cnt: sys.exit("86XXXXXX 코드를 찾지 못했습니다")
        code_col = max(cnt, key=cnt.get); hdr_row = 0
        print(f"  헤더 못 찾음 — 코드가 가장 많은 {code_col}열 사용")

    def col_of(*labels, default=None):
        rng = list(range(max(0, hdr_row - 1), min(len(rows), hdr_row + 3)))
        for r in rng:                      # 1차: 완전 일치
            for c, v in enumerate(rows[r]):
                if str(v).strip() in labels:
                    return c
        for r in rng:                      # 2차: 부분 일치 (짧은 셀만)
            for c, v in enumerate(rows[r]):
                t = str(v).strip()
                if t and len(t) <= 20 and any(l in t for l in labels):
                    return c
        return default

    idx = {
        "시작일": col_of("시작일", default=1),
        "종료일": col_of("종료일", default=2),
        "채널":   col_of("채널", default=4),
        "사업장": col_of("사업장", default=5),
        "인플루언서": col_of("인플루언서", default=6),
        "상품":   col_of("상품", default=7),
    }
    print(f"  패키지번호={code_col}열, 헤더행={hdr_row}, 기타열={idx}")
    print(f"  헤더행 내용: {[str(x)[:12] for x in rows[hdr_row][:18]]}")

    items = []
    for r in range(hdr_row + 1, len(rows)):
        row = rows[r]
        raw = row[code_col] if code_col < len(row) else ""
        codes = CODE_RE.findall(str(raw))
        if not codes:
            continue
        get = lambda k: (row[idx[k]].strip() if idx[k] is not None and idx[k] < len(row) else "")
        items.append({
            "row": r + 1, "codes": sorted(set(codes)), "codes_raw": str(raw).strip(),
            "시작일": get("시작일"), "종료일": get("종료일"), "채널": get("채널"),
            "사업장": get("사업장"), "인플루언서": get("인플루언서"), "상품": get("상품")[:60],
        })
    return items

def parse_txt(fpath: Path, is_cancel: bool, code_to_rows: dict, agg: dict):
    try:
        lines = open(fpath, encoding="cp949", errors="replace").readlines()
    except Exception as e:
        print(f"  읽기 실패 {fpath.name}: {e}"); return 0
    if not lines: return 0
    col = {h.strip(): i for i, h in enumerate(lines[0].rstrip("\n").split(";"))}
    i_mem  = col.get("회원번호", 5)
    i_rn   = col.get("객실수", 28)
    i_rate = col.get("1박객실료", 26)
    n = 0
    for line in lines[1:]:
        p = line.rstrip("\n").split(";")
        if len(p) <= max(i_mem, i_rn, i_rate): continue
        mem = p[i_mem].strip()
        if not mem.startswith("86"): continue
        if mem not in code_to_rows: continue
        try:
            rn = int(p[i_rn].strip() or 0); rate = int(p[i_rate].strip() or 0)
        except ValueError: continue
        if rn <= 0: rn = 1
        b = agg[mem]                      # 코드 단위로 1회만 누적
        if is_cancel:
            b["cancel_rn"] += rn; b["cancel_rev"] += rate
        else:
            b["rn"] += rn; b["rev_rate_sum"] += rate; b["rev_rn_mult"] += rate * rn
            b["rows"] += 1
        n += 1
    return n

def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    sheet = Path(os.path.expanduser(args[0])) if args else SHEET_CSV_URL
    years = ["2024", "2025", "2026"]
    if "--years" in sys.argv:
        years = sys.argv[sys.argv.index("--years") + 1:]

    print(f"시트 읽는 중: {sheet}")
    items = load_sheet(sheet)
    print(f"  패키지번호가 있는 행 {len(items)}건")

    code_to_rows = defaultdict(list)
    for it in items:
        for c in it["codes"]:
            code_to_rows[c].append(it["row"])
    dup = {c: v for c, v in code_to_rows.items() if len(v) > 1}
    print(f"  고유 코드 {len(code_to_rows)}개" + (f" / 여러 행 중복 {len(dup)}개" if dup else ""))

    agg = {c: dict(rn=0, rev_rate_sum=0, rev_rn_mult=0, rows=0,
                   cancel_rn=0, cancel_rev=0) for c in code_to_rows}

    raw = find_raw_db(); print(f"raw_db: {raw}")
    files = 0
    for y in years:
        d = raw / y
        if not d.exists(): continue
        for fn in sorted(os.listdir(d)):
            if not fn.endswith(".txt"): continue
            pre = fn.split(".")[0]
            if pre in ("27", "43"):
                parse_txt(d / fn, False, code_to_rows, agg); files += 1
            elif pre in ("28", "44"):
                parse_txt(d / fn, True, code_to_rows, agg); files += 1
    print(f"  파싱 파일 {files}개")

    out, paste = [], []
    for it in items:
        cs   = it["codes"]
        rn   = sum(agg[c]["rn"] for c in cs)
        rev  = sum(agg[c]["rev_rn_mult"] for c in cs)
        can  = sum(agg[c]["cancel_rn"] for c in cs)
        rws  = sum(agg[c]["rows"] for c in cs)
        rsum = sum(agg[c]["rev_rate_sum"] for c in cs)
        adr  = round(rev / rn) if rn else 0
        shared = [c for c in cs if len(code_to_rows[c]) > 1]
        out.append({**it, "실적RN": rn, "실적매출": rev, "ADR": adr,
                    "취소RN": can, "매칭행수": rws, "매출_단순합": rsum,
                    "공유코드": shared})
        paste.append([rn, rev, adr, "중복" if shared else ""])

    matched = [r for r in out if r["실적RN"] > 0]
    print(f"\n실적이 잡힌 행 {len(matched)}/{len(out)}")
    print(f"{'행':>4} {'사업장':<8} {'인플루언서':<16} {'RN':>7} {'매출(백만)':>10} {'ADR':>9}  코드")
    print("─" * 92)
    for r in sorted(out, key=lambda x: -x["실적RN"])[:30]:
        print(f'{r["row"]:>4} {r["사업장"][:8]:<8} {r["인플루언서"][:16]:<16} '
              f'{r["실적RN"]:>7,} {r["실적매출"]/1e6:>10,.1f} {r["ADR"]:>9,}  {",".join(r["codes"][:3])}')

    tot_rn  = sum(v["rn"] for v in agg.values())
    tot_rev = sum(v["rev_rn_mult"] for v in agg.values())
    naive_rn  = sum(r["실적RN"] for r in out)
    naive_rev = sum(r["실적매출"] for r in out)
    dup_rows  = sum(1 for r in out if r["공유코드"])
    print("─" * 92)
    print(f"합계(중복 제거)  RN {tot_rn:,}  매출 {tot_rev/1e6:,.1f}백만  "
          f"ADR {round(tot_rev/tot_rn) if tot_rn else 0:,}")
    print(f"행 단순합        RN {naive_rn:,}  매출 {naive_rev/1e6:,.1f}백만  (공유코드 {dup_rows}행 포함 — 보고 금지)")

    (PROJECT_DIR / "docs" / "data").mkdir(parents=True, exist_ok=True)
    (PROJECT_DIR / "docs" / "data" / "action_plan_performance.json").write_text(
        json.dumps({"rows": out, "meta": {
            "source": getattr(sheet, "name", "google-sheet-live"), "codes": len(code_to_rows),
            "rows_with_codes": len(items), "rows_with_perf": len(matched),
            "duplicate_codes": {k: v for k, v in list(dup.items())[:50]},
            "total_rn": tot_rn, "total_rev_won": tot_rev,
            "naive_row_sum_rn": naive_rn, "naive_row_sum_rev_won": naive_rev,
            "rows_with_shared_codes": dup_rows,
            "rule": "행별=자기 코드 합 / 총합계=코드당 1회(중복 제거) / 기간 필터 없음",
        }}, ensure_ascii=False, indent=2), encoding="utf-8")

    (PROJECT_DIR / "data").mkdir(exist_ok=True)
    with open(PROJECT_DIR / "data" / "action_plan_paste.csv", "w",
              newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f); w.writerow(["실적RN", "실적매출(원)", "ADR", "비고"]); w.writerows(paste)

    print(f"\n저장:\n  docs/data/action_plan_performance.json\n  data/action_plan_paste.csv")

main()
