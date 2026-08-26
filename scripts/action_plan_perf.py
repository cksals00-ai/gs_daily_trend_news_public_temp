#!/usr/bin/env python3
"""
action_plan_perf.py — 인플루언서 현황 시트(패키지번호 O열) → 실적(객실수/매출/ADR)
  python3 scripts/action_plan_perf.py ~/Downloads/시트.csv
"""
from __future__ import annotations
import csv, json, os, re, sys, unicodedata
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

# ── 재전송/스냅샷 이중 카운트 방지 ────────────────────────────────────────
# raw_db 에는 누적 스냅샷(…_YYYYMMDD_생성시간(…))과 월별 재전송(…(YYYYMMDD-YYYYMMDD))이
# 공존하며 구간이 겹친다. 겹친 구간을 둘 다 더하면 동일 예약이 2회 계상된다.
# parse_raw_db.py 와 같은 규칙(판매일자 월 경계로 분리)을 그대로 쓴다.
RETRANS_RE   = re.compile(r"\(\d{8}-\d{8}\)")
SNAP_DATE_RE = re.compile(r"_(\d{8})_생성시간")

def _nfc(s: str) -> str:            # macOS 파일명은 NFD → 한글 패턴 매칭 전 정규화 필수
    return unicodedata.normalize("NFC", s)

def _prev_month(ym: str | None) -> str | None:
    if not ym or len(ym) < 6: return None
    y, m = int(ym[:4]), int(ym[4:6]) - 1
    if m == 0: y, m = y - 1, 12
    return f"{y}{m:02d}"

def _snap_min_month(paths, year: str, min_rows: int = 500) -> str | None:
    """스냅샷이 실제 보유한 최소 판매일자 월. 마감월은 live 스냅샷에서 빠지므로
    재전송이 '그 직전월까지' 커버해야 누락이 없다(잡음월 무시: min_rows 이상)."""
    counts = defaultdict(int)
    for fp in paths:
        try:
            with open(fp, encoding="cp949", errors="replace") as f:
                hdr = {h.strip(): i for i, h in enumerate(f.readline().split(";"))}
                i = hdr.get("판매일자", hdr.get("입실일자", -1))
                if i < 0: continue
                for line in f:
                    p = line.split(";")
                    if i >= len(p): continue
                    d = p[i].strip()
                    if len(d) >= 6 and d[:6].isdigit() and d[:4] == year:
                        counts[d[:6]] += 1
        except OSError:
            continue
    valid = [ym for ym, c in counts.items() if c >= min_rows]
    return min(valid) if valid else (min(counts) if counts else None)

def plan_files(raw: Path, years):
    """[(경로, is_cancel, min_month, max_month)] — 겹치는 구간을 월 경계로 분리."""
    plan = []
    for y in years:
        d = raw / y
        if not d.exists(): continue
        groups = defaultdict(list)
        for fn in sorted(os.listdir(d)):
            if not fn.endswith(".txt"): continue
            pre = fn.split(".")[0]
            if pre in ("27", "43", "28", "44"):
                groups[pre].append(d / fn)
        for pre, fps in sorted(groups.items()):
            is_cancel = pre in ("28", "44")
            retrans = [fp for fp in fps if RETRANS_RE.search(_nfc(fp.name))]
            snaps   = [fp for fp in fps if fp not in retrans]
            if len(snaps) > 1:                       # 구 스냅샷은 스킵(최신 1개만)
                def _snap_date(fp):
                    m = SNAP_DATE_RE.search(_nfc(fp.name))
                    return m.group(1) if m else "00000000"
                snaps.sort(key=_snap_date, reverse=True)
                for old in snaps[1:]:
                    print(f"  구 스냅샷 스킵: {old.name}")
                snaps = snaps[:1]
            if retrans and snaps:
                snap_min = _snap_min_month(snaps, y)
                rmax = _prev_month(snap_min)
                for fp in retrans:
                    plan.append((fp, is_cancel, None, rmax))
                    print(f"  {y}/{pre} 재전송 ≤{rmax}: {_nfc(fp.name)[:46]}")
                for fp in snaps:
                    plan.append((fp, is_cancel, snap_min, None))
                    print(f"  {y}/{pre} 스냅샷 ≥{snap_min}: {_nfc(fp.name)[:46]}")
            else:
                for fp in retrans + snaps:
                    plan.append((fp, is_cancel, None, None))
    return plan

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

def parse_txt(fpath: Path, is_cancel: bool, code_to_rows: dict, agg: dict,
              min_month=None, max_month=None):
    """예약(27/43)은 '취소를 제외한 생존 예약'만 담긴 이미 net 자료 → 그대로 실적으로 쓴다.
    취소(28/44)는 예약상태가 전부 '취소'인 별개 모집단 → 참고치로만 집계하고 차감하지 않는다
    (차감하면 이중차감. generate_campaign_performance.py 와 동일 원칙)."""
    try:
        lines = open(fpath, encoding="cp949", errors="replace").readlines()
    except Exception as e:
        print(f"  읽기 실패 {fpath.name}: {e}"); return 0
    if not lines: return 0
    col = {h.strip(): i for i, h in enumerate(lines[0].rstrip("\n").split(";"))}
    i_mem  = col.get("회원번호", 5)
    i_rn   = col.get("객실수", 28)
    i_rate = col.get("1박객실료", 26)
    i_sell = col.get("판매일자", 1)
    n = 0
    seen = set()                          # 파일 내 동일 행 중복 제거(해시)
    for line in lines[1:]:
        p = line.rstrip("\n").split(";")
        if len(p) <= max(i_mem, i_rn, i_rate): continue
        mem = p[i_mem].strip()
        if not mem.startswith("86"): continue
        if mem not in code_to_rows: continue
        if min_month or max_month:        # 판매일자 월 경계(재전송/스냅샷 분리)
            sm = p[i_sell].strip()[:6] if i_sell < len(p) else ""
            if len(sm) == 6 and sm.isdigit():
                if min_month and sm < min_month: continue
                if max_month and sm > max_month: continue
        h = hash(line)
        if h in seen: continue
        seen.add(h)
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
    plan = plan_files(raw, years)
    for fp, is_cancel, lo, hi in plan:
        parse_txt(fp, is_cancel, code_to_rows, agg, lo, hi)
    print(f"  파싱 파일 {len(plan)}개")

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
        json.dumps({"rows": out,
                    # 코드별 집계 — 화면에서 필터를 걸어도 '코드당 1회' 총합을 다시 낼 수 있게
                    "by_code": {c: {"rn": v["rn"], "rev": v["rev_rn_mult"],
                                    "cancel_rn": v["cancel_rn"]}
                                for c, v in agg.items() if v["rn"] or v["cancel_rn"]},
                    "meta": {
            "source": getattr(sheet, "name", "google-sheet-live"), "codes": len(code_to_rows),
            "rows_with_codes": len(items), "rows_with_perf": len(matched),
            "duplicate_codes": {k: v for k, v in list(dup.items())[:50]},
            "total_rn": tot_rn, "total_rev_won": tot_rev,
            "naive_row_sum_rn": naive_rn, "naive_row_sum_rev_won": naive_rev,
            "rows_with_shared_codes": dup_rows,
            "rule": "행별=자기 코드 합 / 총합계=코드당 1회(중복 제거) / 기간 필터 없음",
            "cancel_note": ("취소(28/44)는 예약상태가 전부 '취소'인 별개 모집단이고 "
                            "예약(27/43)에는 애초에 취소분이 들어있지 않다(이미 net). "
                            "따라서 차감하지 않으며 화면에는 참고치로만 표시한다. "
                            "기간 필터가 없어 캠페인 종료 후 취소까지 합산되므로 "
                            "취소RN이 실적RN보다 큰 행이 정상적으로 존재한다."),
            "dedupe_note": ("raw_db 의 누적 스냅샷과 월별 재전송은 구간이 겹친다. "
                            "parse_raw_db.py 와 같은 규칙(판매일자 월 경계 분리)으로 "
                            "겹친 구간을 한쪽에서만 읽어 이중 계상을 막는다."),
        }}, ensure_ascii=False, indent=2), encoding="utf-8")

    (PROJECT_DIR / "data").mkdir(exist_ok=True)
    with open(PROJECT_DIR / "data" / "action_plan_paste.csv", "w",
              newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f); w.writerow(["실적RN", "실적매출(원)", "ADR", "비고"]); w.writerows(paste)

    print(f"\n저장:\n  docs/data/action_plan_performance.json\n  data/action_plan_paste.csv")

main()
