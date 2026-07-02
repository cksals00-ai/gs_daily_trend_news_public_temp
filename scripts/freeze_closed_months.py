#!/usr/bin/env python3
"""마감월 보존(freeze) — db_aggregated.json 2026 마감월 데이터 복원/동결

배경: 2026 마감월(1~5월)의 권위 소스였던 **재전송 raw 파일이 일일 크롤에 삭제**되면,
parse_raw_db 는 매 실행 raw 전체를 재계산하므로(마감월 무보존) 라이브 스냅샷에 없는
마감월(특히 4월)이 통째로 사라지거나 잔여 노이즈(307 등)만 남는다. 마감월은 **불변**이므로
직전 정상 집계에서 동결해 둔다.

동작(자기치유):
  • 보호 대상 = 2026년 '당월 이전' 월(마감월).
  • 각 마감월이 현재 db_aggregated 에서 **건강**(booking_rn ≥ 임계, 베이스라인의 50%↑)하면
    → 그 달의 모든 차원 슬라이스를 baseline 에 갱신(정상 데이터가 흐르면 자동 최신화).
  • **손상/누락**이면 → baseline 에서 그 달 전 차원 슬라이스를 복원.
  • meta.months 재계산.

baseline 파일: data/db_closed_baseline.json (커밋 추적, 월별 pruned 슬라이스).
build.py 가 generate_otb_data 직전에 호출 → otb_data·.gz·동기화 모두 정상값 사용.

최초 시드: python3 scripts/freeze_closed_months.py --seed-from <good_db_aggregated.json>
정기 실행: python3 scripts/freeze_closed_months.py
"""
import copy
import json
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DATA_DB = REPO / "data" / "db_aggregated.json"
DOCS_DB = REPO / "docs" / "data" / "db_aggregated.json"
BASELINE = REPO / "data" / "db_closed_baseline.json"
MIN_HEALTHY = 5000        # baseline 없을 때 정상 판정 최소 booking_rn


def protected_months(cur_ym):
    """2026년 '당월 이전' 마감월 목록(YYYYMM)."""
    cy, cm = int(cur_ym[:4]), int(cur_ym[4:6])
    out = []
    for mm in range(1, 13):
        sm = f"2026{mm:02d}"
        if 2026 < cy or (2026 == cy and mm < cm):
            out.append(sm)
    return out


def _is_digits(k, n):
    s = str(k)
    return s.isdigit() and len(s) == n


def _period_level(d):
    """이 dict 의 키가 월(YYYYMM=6)·일(YYYYMMDD=8) 키링인지 판별."""
    if not isinstance(d, dict) or not d:
        return None
    ks = list(d.keys())
    if any(_is_digits(k, 6) for k in ks):
        return 6
    if any(_is_digits(k, 8) for k in ks):
        return 8
    return None


def extract_month(node, month):
    """node(전체 db 또는 하위) 에서 특정 월(month=YYYYMM) 의 period-key 만 추려 같은 형태로 반환."""
    pl = _period_level(node)
    if pl:
        out = {}
        for k, v in node.items():
            ks = str(k)
            if (pl == 6 and ks == month) or (pl == 8 and ks[:6] == month):
                out[k] = copy.deepcopy(v)
        return out or None
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            if isinstance(v, dict):
                sub = extract_month(v, month)
                if sub:
                    out[k] = sub
        return out or None
    return None


def restore_month(new, pruned):
    """pruned(특정 월 슬라이스) 를 new 에 덮어쓴다(해당 월 period-key 만, 타 월 보존)."""
    pl = _period_level(pruned)
    if pl:
        if isinstance(new, dict):
            for k, v in pruned.items():
                new[k] = copy.deepcopy(v)
        return
    if isinstance(pruned, dict):
        for k, v in pruned.items():
            if not isinstance(new.get(k), dict):
                new[k] = {}
            restore_month(new[k], v)


def month_booking_rn(db, sm):
    return ((db.get("monthly_total") or {}).get(sm) or {}).get("booking_rn", 0) or 0


def recompute_meta_months(db):
    mt = db.get("monthly_total") or {}
    months = sorted(mt.keys())
    meta = db.setdefault("meta", {})
    meta["months"] = months
    yrs = sorted({m[:4] for m in months})
    if "years" in meta:
        meta["years"] = yrs


def load_json(p):
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def seed(good_path):
    """최초 시드: 정상 db_aggregated 에서 보호월 슬라이스를 baseline 으로 추출."""
    good = load_json(Path(good_path))
    if not good:
        sys.exit(f"seed 실패: {good_path} 없음/빈값")
    cur_ym = datetime.now().strftime("%Y%m")
    base = {}
    for sm in protected_months(cur_ym):
        if month_booking_rn(good, sm) >= MIN_HEALTHY:
            base[sm] = extract_month(good, sm)
            print(f"  seed {sm}: booking_rn={month_booking_rn(good, sm):,}", file=sys.stderr)
    BASELINE.write_text(json.dumps(base, ensure_ascii=False), encoding="utf-8")
    print(f"✅ baseline 시드: {BASELINE} ({len(base)}개월)", file=sys.stderr)


def _strip_daily(slice_):
    """일자축(취소일자/입력일자 = YYYYMMDD) daily 필드를 슬라이스에서 제거.
    freeze 의 extract/restore 는 period-key(YYYYMMDD)를 투숙월 슬라이스로 오인하므로,
    cancel_daily/pickup_daily/net_daily/stay_date_daily 등 '일자축' 필드가 마감월 복원에
    끌려들어가면 base_date(전일)의 당일픽업이 stale baseline 으로 덮여 과소·차원불일치가 된다.
    이 필드들은 parse_raw_db 가 매 실행 raw 전체로 정확히 재계산하므로 freeze 보존 불필요.
    (_month 접미사=투숙월축 필드는 마감월 보존이 유효하므로 유지)"""
    return {k: v for k, v in (slice_ or {}).items()
            if not ('daily' in k and not k.endswith('_month'))}


def freeze_file(db_path, baseline, cur_ym, updated_baseline):
    """db_aggregated 파일 하나에 대해 마감월 동결/복원. updated_baseline 에 자가치유분 누적."""
    db = load_json(db_path)
    if not db:
        return None, 0, 0
    restored = healed = 0
    for sm in protected_months(cur_ym):
        rn = month_booking_rn(db, sm)
        base_rn = month_booking_rn(baseline.get(sm) or {}, sm) if baseline.get(sm) else 0
        # 마감월 booking_rn 은 상향 개정만 정상(late data). 라이브가 baseline 이상으로 완전할
        # 때만 갱신(자가치유), 더 낮으면(체크아웃 드롭·재전송 삭제 등 손실) baseline 복원.
        if rn >= MIN_HEALTHY and rn >= base_rn:
            updated_baseline[sm] = _strip_daily(extract_month(db, sm))
            healed += 1
        elif baseline.get(sm):
            restore_month(db, _strip_daily(baseline[sm]))
            restored += 1
            print(f"  복원 {sm}: live booking_rn={rn:,} → baseline {base_rn:,}", file=sys.stderr)
    recompute_meta_months(db)
    db_path.write_text(json.dumps(db, ensure_ascii=False), encoding="utf-8")
    return db, restored, healed


def main():
    if "--seed-from" in sys.argv:
        seed(sys.argv[sys.argv.index("--seed-from") + 1])
        return
    cur_ym = datetime.now().strftime("%Y%m")
    baseline = load_json(BASELINE) or {}
    if not baseline:
        print("⚠ baseline 없음 — freeze 스킵(먼저 --seed-from 으로 시드 필요)", file=sys.stderr)
        return
    updated = dict(baseline)  # 자가치유 누적(기존 유지 + 건강월 갱신)
    total_restored = 0
    for p in (DATA_DB, DOCS_DB):
        db, restored, healed = freeze_file(p, baseline, cur_ym, updated)
        if db is not None:
            total_restored += restored
            print(f"✓ {p.name}: 복원 {restored}, 갱신 {healed}", file=sys.stderr)
    # 자가치유로 갱신된 baseline 저장
    BASELINE.write_text(json.dumps(updated, ensure_ascii=False), encoding="utf-8")
    print(f"✅ freeze 완료 (복원 {total_restored}건)", file=sys.stderr)


if __name__ == "__main__":
    main()
