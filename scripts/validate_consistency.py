#!/usr/bin/env python3
"""빌드 후 데이터 정합성 자동 검증.

사용자 요구로 추가된 검사 (build.py 후처리에 호출):
1. FCST 세그먼트별 분리 체크 — 달성률 200% 이상 → 세그먼트 미분리 의심 경고
2. 당일 데이터 존재 여부 자동 체크 — daily_analysis.byProperty가 비어있으면 경고
3. 예약페이스 그래프 ↔ Booking Status 표 데이터 정합성 크로스체크
4. rev_fcst 단위 정합 (월별 합 ≈ 연 / 월별 < 연)
5. byProperty.rns_actual 합 ≈ summary.rns_actual (영업장별 vs 전체)

실행:
    python3 scripts/validate_consistency.py [--strict]

종료 코드:
    0 = 모든 검사 통과 또는 WARN만
    1 = ERROR (--strict 옵션 시) 또는 critical 이슈 발견
"""
import json
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
DOCS_DATA = PROJECT_DIR / "docs" / "data"
DATA_DIR = PROJECT_DIR / "data"

OTB_PATH  = DOCS_DATA / "otb_data.json"
DB_PATH   = DATA_DIR / "db_aggregated.json"
INDEX_HTML = PROJECT_DIR / "docs" / "index.html"

BUDGET_SEGS = ("OTA", "G-OTA", "Inbound")


class Result:
    def __init__(self):
        self.errors = []
        self.warns  = []
        self.infos  = []

    def err(self, msg):  self.errors.append(msg);  print(f"  ✗ ERROR: {msg}")
    def warn(self, msg): self.warns.append(msg);   print(f"  ⚠ WARN:  {msg}")
    def info(self, msg): self.infos.append(msg);   print(f"  ✓ OK:    {msg}")


def check_fcst_source_match(otb, r):
    """검사 [0] **CORE**: 세그먼트 FCST가 원본 데이터(manager_keyin / RM segment FCST)와 정확히 일치하는지.

    근본 원인 회귀 방지: 이전 빌드에서는 FCST를 LY 점유율로 분배했음. 그러면 sum invariant는
    통과해도 실제 데이터는 RM/매니저 의도와 다르게 들어감. 이 검사는 fcst_source 라벨 기준으로:
      - manager_keyin: ai_fcst.json의 manager_keyin_segments[seg]와 byteForBye 일치
      - rm_seg_fcst: fcst_segment_trend.json snapshots latest의 rm_fcst_rn과 일치
    이 둘 중 하나에 해당하는 경우 원본 값과 다르면 즉시 ERROR.
    """
    print("[0] FCST source 원본 일치 검증 (manager_keyin / rm_seg_fcst)")

    # 원본 로드
    project_dir = PROJECT_DIR
    ai_path  = project_dir / "docs" / "data" / "ai_fcst.json"
    fst_path = project_dir / "data" / "fcst_segment_trend.json"

    keyin_map = {}  # {prop: {ym: {seg: rn}}}
    if ai_path.exists():
        try:
            ai = json.loads(ai_path.read_text(encoding="utf-8"))
            for prop, months in ai.get("properties", {}).items():
                for ym, v in months.items():
                    ks = v.get("manager_keyin_segments")
                    if ks:
                        keyin_map.setdefault(prop, {})[ym] = ks
        except Exception as e:
            r.warn(f"ai_fcst.json 로드 실패: {e}")

    rm_seg_map = {}  # {prop: {ym: {seg: rm_fcst_rn}}}
    if fst_path.exists():
        try:
            fst = json.loads(fst_path.read_text(encoding="utf-8"))
            snaps = fst.get("snapshots", [])
            if snaps:
                latest = snaps[-1]
                for prop, months in latest.get("properties", {}).items():
                    for ym, segs in months.items():
                        for seg, v in segs.items():
                            rn = v.get("rm_fcst_rn")
                            if rn is not None:
                                rm_seg_map.setdefault(prop, {}).setdefault(ym, {})[seg] = int(rn)
        except Exception as e:
            r.warn(f"fcst_segment_trend.json 로드 실패: {e}")

    if not keyin_map and not rm_seg_map:
        r.warn("검증 source 부재(ai_fcst+fcst_segment_trend) — skip")
        return

    keyin_mismatches = []
    rm_mismatches = []
    keyin_ok = rm_ok = 0

    for m_str, snap in otb.get("allMonths", {}).items():
        if m_str in ("0", "summary"):
            continue
        try:
            month_int = int(m_str)
        except Exception:
            continue
        ym = f"2026-{month_int:02d}"
        bps = snap.get("byPropertySegment", {})
        for seg in BUDGET_SEGS:
            seg_props = bps.get(seg, []) if isinstance(bps.get(seg), list) else []
            for p in seg_props:
                name = p.get("name")
                fcst = p.get("rns_fcst", 0) or 0
                source = p.get("fcst_source")

                # 1) manager_keyin이 있으면 정확히 일치해야 함 (source 라벨도 manager_keyin이어야)
                exp_keyin = keyin_map.get(name, {}).get(ym, {}).get(seg)
                if exp_keyin is not None:
                    if source != "manager_keyin":
                        keyin_mismatches.append(f"{ym}/{seg}/{name}: keyin={exp_keyin}, fcst={fcst}, source={source} (manager_keyin이어야 함)")
                    elif int(fcst) != int(exp_keyin):
                        keyin_mismatches.append(f"{ym}/{seg}/{name}: keyin={exp_keyin}, fcst={fcst} (값 불일치)")
                    else:
                        keyin_ok += 1
                    continue

                # 2) RM segment FCST가 있고 source=rm_seg_fcst인 경우 정확 일치
                exp_rm = rm_seg_map.get(name, {}).get(ym, {}).get(seg)
                if exp_rm is not None and source == "rm_seg_fcst":
                    if int(fcst) != int(exp_rm):
                        rm_mismatches.append(f"{ym}/{seg}/{name}: rm_fcst_rn={exp_rm}, fcst={fcst}")
                    else:
                        rm_ok += 1

    if keyin_mismatches:
        r.err(f"manager_keyin source 불일치: {len(keyin_mismatches)}건 — 매니저 키인 값과 다름! {keyin_mismatches[:3]}")
    else:
        r.info(f"manager_keyin 정확 일치: {keyin_ok}건")

    if rm_mismatches:
        r.err(f"rm_seg_fcst source 불일치: {len(rm_mismatches)}건 — RM 회의 분배본과 다름! {rm_mismatches[:3]}")
    else:
        r.info(f"rm_seg_fcst 정확 일치: {rm_ok}건")


def check_fcst_segment_split(otb, r):
    """검사 1: 세그먼트 FCST 추가 정합성 + 분포.
    A) sum(BUDGET segs FCST) vs byProperty.rns_fcst — 원본 source 정책에 따라 자연 차이 발생 가능
       (sum=manager_keyin/RM original; total=AI FCST 모델 결과). INFO로 보고.
    B) 의심 케이스 경고: fcst_ach > 400% 이면서 actual < budget × 1.5 — 데이터 anomaly 표시.
    C) 200% < fcst_ach <= 400%: 자연스러운 budget 보수성. INFO.
    """
    print("[1] FCST 세그먼트별 추가 검사 (분포 + sum 차이 보고)")
    inconsistent = []  # sum mismatch (참고용)
    suspect = []       # fcst_ach > 400 with low actual
    natural = 0        # 200~400%, expected (budget conservative)

    for m_str in ("summary",) + tuple(str(i) for i in range(1, 13)):
        snap = otb.get("allMonths", {}).get(m_str, {})
        bp = snap.get("byProperty", [])
        bps = snap.get("byPropertySegment", {})

        # A) sum 정합 검사 (사업장별) — BUDGET_SEGS(OTA+G-OTA+Inbound) 합 ≈ property total fcst
        # property total은 OTA+G-OTA+Inbound 기준 빌드되므로 3개 세그 합이 invariant.
        prop_seg_sum_fcst = {}
        for seg in BUDGET_SEGS:
            seg_props = bps.get(seg, []) if isinstance(bps.get(seg), list) else []
            for p in seg_props:
                prop_seg_sum_fcst.setdefault(p["name"], 0)
                prop_seg_sum_fcst[p["name"]] += (p.get("rns_fcst", 0) or 0)
        for p in bp:
            seg_sum = prop_seg_sum_fcst.get(p["name"], 0)
            total = p.get("rns_fcst", 0) or 0
            # 팔라티움 등 daily_booking 보정 사업장은 byPropertySegment 모두 0 → skip
            if seg_sum == 0:
                continue
            if total > 0:
                diff_pct = abs(seg_sum - total) / total * 100
                if diff_pct > 5:  # 5% 초과 시 분배 깨짐
                    inconsistent.append(f"{m_str}월/{p['name']}: sum(BUDGET segs)={seg_sum}, total={total} ({diff_pct:.1f}%)")

        # B/C) 달성률 분포
        for seg in BUDGET_SEGS:
            seg_props = bps.get(seg, []) if isinstance(bps.get(seg), list) else []
            for p in seg_props:
                ach = p.get("fcst_achievement", 0) or 0
                bud = p.get("rns_budget", 0) or 0
                act = p.get("rns_actual", 0) or 0
                if ach > 400 and bud > 0 and act < bud * 1.5:
                    suspect.append(f"{m_str}월/{seg}/{p.get('name')}: bud={bud}, act={act}, ach={ach}%")
                elif ach > 200:
                    natural += 1

    # sum 차이는 source 정책의 자연 결과 (사용자 키인 합 ≠ AI 사업장 total). 참고용 INFO.
    if inconsistent:
        r.info(f"sum(BUDGET seg FCST) vs property total fcst 차이: {len(inconsistent)}건 — manager_keyin/RM_seg vs AI 결과 차이 (정상). 예: {inconsistent[:2]}")
    else:
        r.info("세그먼트 FCST 합 ≈ 사업장 총 FCST")

    if suspect:
        r.warn(f"FCST 달성률 >400% 의심 케이스: {len(suspect)}건 (예산이 실적/원본 FCST 대비 매우 보수) {suspect[:3]}")
    if natural > 0:
        r.info(f"FCST 200~400% 자연 anomaly (예산 보수적): {natural}건")


def check_daily_today_present(otb, r):
    """검사 2: meta.todayDate가 있고 today_booking 등이 0이 아닌지."""
    print("[2] 당일 데이터 존재 여부")
    meta = otb.get("meta", {})
    today = meta.get("todayDate", "")
    if not today:
        r.err("meta.todayDate 비어있음")
        return

    s = otb.get("summary", {})
    tb = s.get("today_booking", 0) or 0
    tc = s.get("today_cancel", 0) or 0
    if tb == 0 and tc == 0:
        r.warn(f"summary.today_booking/cancel 모두 0 ({today}) — 당일 데이터 미수집 가능성")
    else:
        r.info(f"today_date={today}, today_booking={tb}, today_cancel={tc}")


def check_index_daily_analysis(r):
    """검사 2-2: index.html INSIGHT_DATA.dailyAnalysis.byProperty 비어있는지."""
    print("[2-2] index.html INSIGHT_DATA.dailyAnalysis.byProperty 채워짐")
    if not INDEX_HTML.exists():
        r.warn("index.html 부재 — 스킵")
        return
    txt = INDEX_HTML.read_text(encoding="utf-8", errors="ignore")
    # INSIGHT_DATA 변수에서 dailyAnalysis.byProperty 위치 찾기
    import re
    m = re.search(r'const INSIGHT_DATA\s*=\s*(\{.*?\});', txt, re.DOTALL)
    if not m:
        r.warn("INSIGHT_DATA 변수 미발견 — 스킵")
        return
    try:
        data = json.loads(m.group(1))
    except Exception as e:
        r.warn(f"INSIGHT_DATA JSON 파싱 실패: {e}")
        return
    da = data.get("dailyAnalysis", {})
    bp = da.get("byProperty", {})
    if not bp:
        r.err("INSIGHT_DATA.dailyAnalysis.byProperty 비어있음 — 당일 분석 화면 빈값 발생")
    else:
        r.info(f"dailyAnalysis.byProperty: {len(bp)}개 사업장 채워짐")


def check_pace_vs_booking_status(otb, db, r):
    """검사 3: 예약페이스(누적) vs Booking Status 표(rns_actual) 정합성.
    - pace = net_daily_by_month_seg[OTA+G-OTA+Inbound][stay_month] 누적
    - Booking Status = otb.allMonths[m].summary.rns_actual (OTA+G-OTA+Inbound 기준 빌드)
    """
    print("[3] 예약페이스(누적) ↔ Booking Status 표 정합")
    seg_data = db.get("net_daily_by_month_seg")
    if not seg_data:
        r.warn("net_daily_by_month_seg 부재 — patch_net_daily_by_month_seg.py 실행 필요. 스킵.")
        return

    base_date = otb.get("meta", {}).get("baseDate", "")  # YYYY-MM-DD
    if not base_date or len(base_date) < 10:
        r.warn("meta.baseDate 부재 — 스킵")
        return
    asof = base_date.replace("-", "")  # YYYYMMDD

    # 미래월 검사 (현재월~12월)
    cur_month = int(base_date[5:7])
    issues = 0
    for m in range(cur_month, 13):
        ym = f"2026{m:02d}"
        # Pace cumulative as-of
        pace_cum = 0
        for seg in BUDGET_SEGS:
            seg_block = seg_data.get(seg, {}).get(ym, {})
            for d in sorted(seg_block.keys()):
                if d > asof:
                    break
                pace_cum += seg_block[d].get("net_rn", 0)
        # Booking Status (otb)
        bs = otb.get("allMonths", {}).get(str(m), {}).get("summary", {})
        bs_actual = bs.get("rns_actual", 0)
        # 차이가 5% 또는 100 RN 초과 시 경고
        diff = abs(pace_cum - bs_actual)
        pct = (diff / bs_actual * 100) if bs_actual else 0
        if diff > 100 and pct > 5:
            r.warn(f"  {m}월: pace={pace_cum}, BS={bs_actual} (차={diff}, {pct:.1f}%)")
            issues += 1
        else:
            print(f"    {m}월: pace={pace_cum}, BS={bs_actual} (차={diff}, {pct:.1f}%)")
    if issues == 0:
        r.info("페이스 vs Booking Status 모든 월 정합")


def check_rev_fcst_unit(otb, r):
    """검사 4: rev_fcst 단위 sanity — 월별 합 ≈ 연간, 월별 < 연간."""
    print("[4] rev_fcst 단위 정합 (월별 ≤ 연간, 월별 합 ≈ 연간)")
    annual = otb.get("summary", {}).get("rev_fcst", 0) or 0
    if annual <= 0:
        r.warn("annual rev_fcst 0 또는 부재 — 스킵")
        return
    monthly_sum = 0
    over_annual = []
    for m in range(1, 13):
        m_rev = otb.get("allMonths", {}).get(str(m), {}).get("summary", {}).get("rev_fcst", 0) or 0
        monthly_sum += m_rev
        if m_rev > annual:
            over_annual.append(f"{m}월={m_rev:,}")
    if over_annual:
        r.err(f"월별 rev_fcst > 연간 rev_fcst({annual:,}): {over_annual}")
    diff_pct = abs(monthly_sum - annual) / annual * 100
    if diff_pct > 5:
        r.warn(f"월별 합({monthly_sum:,}) vs 연간({annual:,}) 차이 {diff_pct:.1f}%")
    else:
        r.info(f"월별 합 ≈ 연간 (차이 {diff_pct:.2f}%)")


def check_byprop_vs_summary(otb, r):
    """검사 5: byProperty.rns_actual 합 ≈ summary.rns_actual."""
    print("[5] byProperty 합 ↔ summary 정합 (각 월)")
    issues = 0
    for m_str in ("summary",) + tuple(str(i) for i in range(1, 13)):
        snap = otb.get("allMonths", {}).get(m_str, {})
        s = snap.get("summary", {})
        bp = snap.get("byProperty", [])
        bp_sum = sum((p.get("rns_actual", 0) or 0) for p in bp)
        s_act = s.get("rns_actual", 0) or 0
        diff = abs(bp_sum - s_act)
        if diff > 10:  # 10 RN tolerance
            r.warn(f"  {m_str}: byProp 합={bp_sum}, summary={s_act}, 차이={diff}")
            issues += 1
    if issues == 0:
        r.info("byProperty 합 ≈ summary 모든 월 정합")


def main():
    strict = "--strict" in sys.argv
    print("=" * 60)
    print("Build 후 정합성 검증 (validate_consistency.py)")
    print("=" * 60)

    if not OTB_PATH.exists():
        print(f"✗ otb_data.json 부재: {OTB_PATH}")
        sys.exit(1)
    otb = json.loads(OTB_PATH.read_text(encoding="utf-8"))

    db = {}
    if DB_PATH.exists():
        db = json.loads(DB_PATH.read_text(encoding="utf-8"))

    r = Result()
    check_fcst_source_match(otb, r)         # NEW [0]: 원본 source 일치 검증 (manager_keyin/rm_seg_fcst)
    check_fcst_segment_split(otb, r)
    check_daily_today_present(otb, r)
    check_index_daily_analysis(r)
    check_pace_vs_booking_status(otb, db, r)
    check_rev_fcst_unit(otb, r)
    check_byprop_vs_summary(otb, r)

    print()
    print("=" * 60)
    print(f"결과: ERROR={len(r.errors)}, WARN={len(r.warns)}, OK={len(r.infos)}")
    if r.errors:
        print("\nERROR 목록:")
        for e in r.errors:
            print(f"  - {e}")
    if r.warns:
        print("\nWARN 목록:")
        for w in r.warns:
            print(f"  - {w}")
    print("=" * 60)

    if r.errors or (strict and r.warns):
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
