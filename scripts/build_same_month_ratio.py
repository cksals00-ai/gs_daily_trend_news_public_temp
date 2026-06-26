#!/usr/bin/env python3
"""
build_same_month_ratio.py — Start 달성률 집계 (OTA + G-OTA만)
투숙월 M의 전체 예약 중 M월 시작 전(사전)에 예약된 RN(advance_rn)을
목표(budget) 대비로 표시. GS 전체 + 사업장별.

  advance_rn      = total_rn - same_month_rn  (예약월 < 투숙월)
  start_ratio     = advance_rn / budget_rn * 100   (목표대비 — 모든 연도, gs_targets.json)
                  = advance_rn / total_rn  * 100   (목표 없는 월만 폴백)

목표(budget)는 **gs_targets.json**(독립 목표 테이블, OTA+G-OTA 월별, 2024~2026)에서 로드.
→ 마감월·전년 모두 목표대비로 표시되어 YoY(올해 목표대비 vs 작년 목표대비)가 성립.

마감월 보존(ledger): 2026 재전송 파일이 삭제되면 라이브 스냅샷엔 마감월이 없어
1~4월이 통째로 사라진다. 마감월(sm < 당월)이 이번 raw 에서 재전송 권위로 재계산되지
않으면 직전 산출물(불변 마감월 값)을 보존한다.

출력: data/same_month_booking.json, docs/data/same_month_booking.json
세그먼트: OTA + G-OTA만 (27번 예약파일)
"""
import os, sys, json, re, unicodedata
import fs_utils  # macOS NFD→NFC 유니코드 정규화
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw_db"
OUTPUT_PATH = PROJECT_DIR / "data" / "same_month_booking.json"
DOCS_PATH = PROJECT_DIR / "docs" / "data" / "same_month_booking.json"
DAILY_BOOKING_PATH = PROJECT_DIR / "data" / "daily_booking.json"
RM_FCST_PATH = PROJECT_DIR / "data" / "rm_fcst.json"
DB_AGG_PATH = PROJECT_DIR / "docs" / "data" / "db_aggregated.json"
TARGETS_PATH = PROJECT_DIR / "docs" / "data" / "gs_targets.json"  # 독립 목표 테이블

# rm_fcst.json 사업장명 → same_month by_property canon명(daily_booking 기준)
RM_FCST_TO_CANON = {
    "01.벨비발디": "소노벨 비발디파크",
    "02.캄비발디": "소노캄 비발디파크",
    "03.펫비발디": "소노펫 비발디파크",
    "04.펠리체비발디": "소노펠리체 비발디파크",
    "05.빌리지비발디": "소노펠리체빌리지 비발디파크",
    "06.양평": "소노벨 양평",
    "07.델피노": "델피노",
    "08.쏠비치양양": "쏠비치 양양",
    "09.쏠비치삼척": "쏠비치 삼척",
    "10.소노벨단양": "소노벨 단양",
    "11.소노캄경주": "소노캄 경주",
    "12.소노벨청송": "소노벨 청송",
    "13.소노벨천안": "소노벨 천안",
    "14.소노벨변산": "소노벨 변산",
    "15.소노캄여수": "소노캄 여수",
    "16.소노캄거제": "소노캄 거제",
    "17.쏠비치진도": "쏠비치 진도",
    "18.소노벨제주": "소노벨 제주",
    "19.소노캄제주": "소노캄 제주",
    "20.소노캄고양": "소노캄 고양",
    "21.소노문해운대": "소노문 해운대",
    "22.쏠비치남해": "쏠비치 남해",
    "23.르네블루": "르네블루",
}

# ── 권역 매핑 (parse_raw_db.PROPERTY_REGION / 페이지 region 칩과 동일 기준) ──
PROPERTY_REGION = {
    "비발디": "vivaldi", "양평": "central", "델피노": "central",
    "양양": "central", "삼척": "central", "단양": "central",
    "천안": "central", "변산": "central",
    "여수": "south", "거제": "south", "남해": "south",
    "진도": "south", "경주": "south", "해운대": "south",
    "청송": "south", "영덕": "south", "르네블루": "central",
    "제주": "apac", "고양": "apac",
    "하이퐁": "apac", "괌": "apac", "하와이": "apac",
}


def get_region(prop_name):
    if not prop_name:
        return "unknown"
    for key, region in PROPERTY_REGION.items():
        if key in prop_name:
            return region
    return "unknown"


def normalize_property(prop_name):
    """'02. 소노벨 비발디파크' → '소노벨 비발디파크'"""
    if not prop_name:
        return ""
    return re.sub(r'^\d+\.\s*', '', prop_name).strip()


def _parse_ym(s):
    s = s.strip().replace('-', '').replace('/', '').replace('.', '')
    return s[:6] if len(s) >= 6 else None


def _next_month(ym):
    """'202605' → '202606' (재전송 범위 다음 달 = 스냅샷 시작 경계)."""
    if not ym or len(ym) < 6:
        return None
    y, m = int(ym[:4]), int(ym[4:6])
    if m >= 12:
        y, m = y + 1, 1
    else:
        m += 1
    return f"{y}{m:02d}"


def classify_segment(code_num):
    num = (code_num or "").strip()
    if num in ("A4", "A5"): return "G-OTA"
    if num in ("53", "72"): return "OTA"
    if num == "58": return "Inbound"
    return "기타"


def detect_month_filter(filename):
    basename = unicodedata.normalize('NFC', os.path.basename(filename))
    m = re.search(r'재전송\((\d{8})-(\d{8})\)', basename)
    if m:
        return ('retransmit', m.group(1)[:6], m.group(2)[:6])
    m2 = re.search(r'_(\d{8})_생성시간', basename)
    if m2:
        return ('snapshot', None, None)
    return ('unknown', None, None)


def find_types_with_retransmit(year_dir):
    result = set()
    for f in os.listdir(year_dir):
        fn = unicodedata.normalize('NFC', f)
        if '재전송' in fn and fn.startswith("27"):
            result.add("27")
    return result


# ── canon 정답지: daily_booking.json 사업장명 (raw 매핑용) ──
def load_canon_names():
    canon = set()
    if not DAILY_BOOKING_PATH.exists():
        return canon
    data = json.load(open(DAILY_BOOKING_PATH, encoding='utf-8'))
    for md in data.get("months_detail", []):
        for p in md.get("properties", []):
            name = p.get("name", "")
            if name and name != "Grand Total":
                canon.add(name)
    return canon


# ── budget 로드: rm_fcst.json 의 OTA + G-OTA 세그먼트 budget만 ──
def load_budget():
    """OTA+G-OTA 세그먼트 budget을 rm_fcst.json에서 로드.
    daily_booking의 budget_rns(전 채널 합산)는 세그먼트 카드에 부적합.
    returns (budget_by_prop[canon][MM] = ota+gota rn,
             budget_gs[MM] = grand_total ota+gota rn)"""
    budget_by_prop = defaultdict(dict)
    budget_gs = defaultdict(int)
    if not RM_FCST_PATH.exists():
        print("⚠ rm_fcst.json 없음 — budget 없이 진행", file=sys.stderr)
        return budget_by_prop, budget_gs
    data = json.load(open(RM_FCST_PATH, encoding='utf-8'))
    for rm_name, months in data.get("properties", {}).items():
        canon = RM_FCST_TO_CANON.get(rm_name)
        if not canon:
            continue
        for ym, node in months.items():  # ym = "2026-05"
            if not ym.startswith("2026-"):
                continue
            mm = ym.split("-")[1]
            seg = node.get("segments", {})
            ota = seg.get("OTA", {}).get("rm_budget_rn", 0) or 0
            gota = seg.get("G-OTA", {}).get("rm_budget_rn", 0) or 0
            rn = ota + gota
            if rn:
                budget_by_prop[canon][mm] = rn
                budget_gs[mm] += rn
    return budget_by_prop, budget_gs


def load_targets():
    """gs_targets.json → targets_gs[year][month] = OTA+G-OTA 목표 RN (2024~2026).
    GS by_year 카드의 목표대비/YoY 분모. 독립 목표 테이블(build_gs_targets.py 산출)."""
    targets_gs = {}
    if not TARGETS_PATH.exists():
        print("⚠ gs_targets.json 없음 — 목표대비 폴백(advance/total)", file=sys.stderr)
        return targets_gs
    data = json.load(open(TARGETS_PATH, encoding='utf-8'))
    for year, months in (data.get("targets") or {}).items():
        targets_gs[year] = {mm: (v or 0) for mm, v in months.items() if v}
    return targets_gs


def load_ledger():
    """직전 산출물(same_month_booking.json) 전체 → 마감월 보존용.
    raw 에서 재계산 안 되는 마감월의 booking 값(불변)을 by_year·by_property 모두 가져온다."""
    for p in (DOCS_PATH, OUTPUT_PATH):
        if p.exists():
            try:
                d = json.load(open(p, encoding='utf-8')) or {}
                return d.get("by_year", {}) or {}, d.get("by_property", {}) or {}
            except Exception:
                continue
    return {}, {}


def load_closing(to_canon):
    """db_aggregated.json → 월별 마감(또는 OTB) RN."""
    closing_gs = {}
    closing_prop = defaultdict(lambda: defaultdict(int))
    if not DB_AGG_PATH.exists():
        print("⚠ db_aggregated.json 없음 — closing_rn 생략", file=sys.stderr)
        return closing_gs, closing_prop
    db = json.load(open(DB_AGG_PATH, encoding='utf-8'))
    for sm, node in (db.get("monthly_total") or {}).items():
        rn = (node or {}).get("booking_rn", 0) or 0
        if rn:
            closing_gs[sm] = rn
    for raw_name, months in (db.get("by_property") or {}).items():
        canon = to_canon(raw_name)
        for sm, node in (months or {}).items():
            rn = (node or {}).get("booking_rn", 0) or 0
            if rn:
                closing_prop[canon][sm] += rn
    return closing_gs, dict(closing_prop)


def build_alias_map(canon):
    """raw 정규화 사업장명 → budget 정답지(canon) 매핑.
    이름이 다른 케이스는 명시적으로, 나머지는 공백 무시 매칭."""
    explicit = {
        "소노문 비발디파크": "소노벨 비발디파크",
        "오션월드빌리지": "소노펠리체빌리지 비발디파크",
        "소노문 단양": "소노벨 단양",
        "소노벨 경주": "소노캄 경주",
        "소노휴 양평": "소노벨 양평",
    }
    nospace = {c.replace(" ", ""): c for c in canon}

    def to_canon(raw):
        if raw in explicit:
            return explicit[raw]
        if raw in canon:
            return raw
        hit = nospace.get(raw.replace(" ", ""))
        return hit if hit else raw  # 매칭 실패 시 raw 그대로(budget 없음)

    return to_canon


def process_file(filepath, to_canon, min_month=None, max_month=None):
    """27번 파일 → (canon_prop, stay_month, booking_month) → rn 집계 (OTA/G-OTA만)"""
    result = defaultdict(int)
    encodings = ['cp949', 'euc-kr', 'utf-8']

    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                header_line = f.readline().strip()
                headers = header_line.split(';')
                col_map = {h.strip(): i for i, h in enumerate(headers)}

                idx_prop = col_map.get('영업장명', -1)
                idx_cprop = col_map.get('변경사업장명', -1)
                idx_selldate = col_map.get('판매일자', -1)
                idx_checkin = col_map.get('입실일자', -1)
                idx_pickup_date = col_map.get('최초입력일자', -1)
                idx_rooms = col_map.get('객실수', -1)
                code_col = '변경예약집계코드' if '변경예약집계코드' in col_map else '예약집계코드'
                idx_code_num = col_map.get(code_col, -1)

                seen = set()
                count = 0

                for line in f:
                    h = hash(line.rstrip('\n\r'))
                    if h in seen: continue
                    seen.add(h)
                    parts = line.split(';')
                    plen = len(parts)

                    try:
                        code_num = parts[idx_code_num].strip() if 0 <= idx_code_num < plen else ''
                        seg = classify_segment(code_num)
                        if seg not in ("OTA", "G-OTA"):
                            continue

                        sell_date = parts[idx_selldate].strip() if 0 <= idx_selldate < plen else ''
                        if len(sell_date) < 6:
                            sell_date = parts[idx_checkin].strip() if 0 <= idx_checkin < plen else ''
                            if len(sell_date) < 6: continue
                        stay_month = _parse_ym(sell_date)
                        if not stay_month: continue
                        if min_month and stay_month < min_month: continue
                        if max_month and stay_month > max_month: continue

                        pickup_str = parts[idx_pickup_date].strip() if 0 <= idx_pickup_date < plen else ''
                        booking_month = _parse_ym(pickup_str)
                        if not booking_month: continue

                        cprop = parts[idx_cprop].strip() if 0 <= idx_cprop < plen else ''
                        praw = parts[idx_prop].strip() if 0 <= idx_prop < plen else ''
                        prop = normalize_property(cprop) if cprop else normalize_property(praw)
                        if not prop: continue
                        canon = to_canon(prop)

                        rn = int(parts[idx_rooms].strip()) if 0 <= idx_rooms < plen else 1
                        result[(canon, stay_month, booking_month)] += rn
                        count += 1
                    except (IndexError, ValueError):
                        continue

                print(f"  {os.path.basename(filepath)}: {count:,}행 (OTA+G-OTA)", file=sys.stderr)
                return result
        except UnicodeDecodeError:
            continue
    return result


def _ratios(total, same, budget_rn, is_unstarted=False):
    advance = total - same
    if advance < 0:
        advance = 0
    if budget_rn:
        start_ratio = round(advance / budget_rn * 100, 1)
    elif is_unstarted:
        start_ratio = None  # 미래월 + 목표 미입력 → 비교 불가
    elif total > 0:
        start_ratio = round(advance / total * 100, 1)
    else:
        start_ratio = 0
    rec = {
        "total_rn": total,
        "same_month_rn": same,
        "advance_rn": advance,
        "start_ratio": start_ratio,
        "ratio": round(same / total * 100, 1) if total > 0 else 0,
    }
    if budget_rn:
        rec["budget_rn"] = budget_rn
    return rec


def main():
    canon_names = load_canon_names()
    budget_by_prop, budget_gs = load_budget()
    targets_gs = load_targets()          # 독립 목표 테이블(전 연도)
    ledger, ledger_prop = load_ledger()  # 직전 산출물(마감월 보존용)
    to_canon = build_alias_map(canon_names)
    closing_gs, closing_prop = load_closing(to_canon)
    cur_ym = datetime.now().strftime("%Y%m")
    ret_end_by_year = {}                 # year -> 재전송 커버 마지막 투숙월(없으면 None)

    agg = defaultdict(int)  # (canon, stay_month, booking_month) -> rn
    year_dirs = sorted([d for d in RAW_DIR.iterdir() if d.is_dir() and d.name.isdigit()])

    for year_dir in year_dirs:
        year = year_dir.name
        print(f"Processing {year}...", file=sys.stderr)
        has_retransmit = find_types_with_retransmit(year_dir)
        txt_files = sorted(year_dir.glob("27*.txt"))

        # ── 스냅샷 중복 제거: 일일 크롤이 과거 스냅샷을 안 지우면 27*.txt 에
        #    스냅샷이 여러 개 남아 전부 합산 → 이중계상(예: 6월 109.7%, 5월 2배).
        #    parse_raw_db.py 와 동일하게 **최신 스냅샷 1개만** 사용한다(재전송은 모두 유지).
        def _snap_date(fp):
            m = re.search(r'_(\d{8})_생성시간', unicodedata.normalize('NFC', fp.name))
            return m.group(1) if m else '00000000'
        snaps = [f for f in txt_files if detect_month_filter(f.name)[0] == 'snapshot']
        if len(snaps) > 1:
            latest = max(snaps, key=_snap_date)
            drop = {f for f in snaps if f is not latest}
            for f in drop:
                print(f"  구 스냅샷 스킵 (최신={latest.name}): {f.name}", file=sys.stderr)
            txt_files = [f for f in txt_files if f not in drop]

        # ── 동적 경계: 재전송 파일이 커버하는 마지막 투숙월(ret_end) ──
        # 하드코딩(f"{year}03") 금지: 재전송은 선언 범위 전체가 권위 데이터이므로
        # 그 범위까지는 재전송이, 그 다음 달부터는 스냅샷이 채운다.
        # 스냅샷은 마감(체크아웃 완료)월이 빠지므로, 03 고정 시 04월이
        # 재전송(≤03)·스냅샷(≥05 실데이터) 사이 공백으로 통째로 누락됨.
        ret_end_max = None
        for fpath in txt_files:
            ft, _rs, re_end = detect_month_filter(fpath.name)
            if ft == 'retransmit' and re_end:
                ret_end_max = max(ret_end_max, re_end) if ret_end_max else re_end
        ret_end_by_year[year] = ret_end_max

        for fpath in txt_files:
            ftype, ret_start, ret_end = detect_month_filter(fpath.name)
            min_month = max_month = None
            if ftype == 'retransmit':
                max_month = ret_end
            elif ftype == 'snapshot' and has_retransmit:
                min_month = _next_month(ret_end_max) if int(year) >= 2026 else None

            result = process_file(fpath, to_canon, min_month, max_month)
            for k, v in result.items():
                agg[k] += v

    # ── GS 전체 집계 (by_year) ──
    gs_total = defaultdict(int)
    gs_same = defaultdict(int)
    # ── 사업장별 집계 (by_property) ──
    prop_total = defaultdict(int)  # (canon, stay_month) -> rn
    prop_same = defaultdict(int)

    for (canon, stay_m, booking_m), rn in agg.items():
        gs_total[stay_m] += rn
        prop_total[(canon, stay_m)] += rn
        if stay_m == booking_m:
            gs_same[stay_m] += rn
            prop_same[(canon, stay_m)] += rn

    def _target(year, month):
        return (targets_gs.get(year) or {}).get(month, 0) or 0

    def _fresh_authoritative(sm):
        """이번 raw 가 이 투숙월(sm)을 권위있게 산출하는가?
        - 당월 이상(미마감): 라이브 스냅샷이 정답.
        - 마감월: 재전송이 커버할 때만 권위(라이브 스냅샷은 체크아웃분 누락→과소)."""
        if sm >= cur_ym:
            return True
        re_max = ret_end_by_year.get(sm[:4])
        return bool(re_max and sm <= re_max)

    # by_year: 마감월은 raw 가 권위일 때만 신규값, 아니면 직전 산출물(불변 booking) 보존.
    by_year = {}
    fresh_sms = set(gs_total.keys())
    all_sms = set(fresh_sms) | {y + m for y, mm in ledger.items() for m in mm}
    for sm in sorted(all_sms):
        year, month = sm[:4], sm[4:6]
        use_fresh = (sm in fresh_sms) and _fresh_authoritative(sm)
        if use_fresh:
            total, same = gs_total[sm], gs_same.get(sm, 0)
        else:
            lrec = (ledger.get(year) or {}).get(month)
            if lrec is None:
                continue  # 마감월인데 raw 도 ledger 도 없음 → 스킵
            total, same = lrec.get("total_rn", 0), lrec.get("same_month_rn", 0)
        rec = _ratios(total, same, _target(year, month), is_unstarted=(sm > cur_ym))
        # closing 도 booking 과 같은 소스 결정: fresh→db_aggregated, 보존→ledger
        # (마감월 db_aggregated 가 재전송 삭제로 손상될 수 있어 ledger 우선이 안전)
        crn = closing_gs.get(sm, 0) if use_fresh else ((ledger.get(year) or {}).get(month, {}) or {}).get("closing_rn", 0)
        if crn:
            rec["closing_rn"] = crn
        by_year.setdefault(year, {})[month] = rec

    # by_property (per-property 목표는 rm_fcst budget_by_prop 유지 — 2026 한정)
    # 마감월 보존: raw 가 권위가 아니면 직전 산출물 사업장값 보존(드릴에서 1~4월 소실 방지).
    by_property = {}
    fresh_prop_sms = set(prop_total.keys())
    canons = set(c for c, _ in fresh_prop_sms) | set(ledger_prop.keys())
    for canon in canons:
        led_node = ledger_prop.get(canon, {})
        # 이 사업장이 가진 모든 (year, month): fresh + ledger
        ym_set = {(sm[:4], sm[4:6]) for (c, sm) in fresh_prop_sms if c == canon}
        ym_set |= {(y, m) for y, mm in led_node.items() if isinstance(mm, dict) and y.isdigit() for m in mm}
        for (year, month) in ym_set:
            sm = year + month
            use_fresh = ((canon, sm) in fresh_prop_sms) and _fresh_authoritative(sm)
            if use_fresh:
                total, same = prop_total[(canon, sm)], prop_same.get((canon, sm), 0)
            else:
                lrec = (led_node.get(year) or {}).get(month)
                if lrec is None:
                    continue
                total, same = lrec.get("total_rn", 0), lrec.get("same_month_rn", 0)
            budget_rn = budget_by_prop.get(canon, {}).get(month, 0) if year == "2026" else 0
            rec = _ratios(total, same, budget_rn, is_unstarted=(sm > cur_ym))
            crn = closing_prop.get(canon, {}).get(sm, 0) if use_fresh else ((led_node.get(year) or {}).get(month, {}) or {}).get("closing_rn", 0)
            if crn:
                rec["closing_rn"] = crn
            node = by_property.setdefault(canon, {"region": get_region(canon)})
            node.setdefault(year, {})[month] = rec

    result_json = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "description": "Start 달성률 (OTA+G-OTA): 투숙월 시작 전 사전예약(advance_rn) 목표대비. GS 목표=gs_targets.json(전 연도), 마감월 booking 보존(ledger).",
        "budget_source": "gs_targets.json (GS by_year, 2024~2026) / rm_fcst (by_property, 2026)",
        "by_year": by_year,
        "by_property": by_property,
    }

    for p in (OUTPUT_PATH, DOCS_PATH):
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, 'w', encoding='utf-8') as f:
            json.dump(result_json, f, ensure_ascii=False, indent=2 if p == OUTPUT_PATH else None)

    print(f"\n✅ {OUTPUT_PATH}", file=sys.stderr)
    print(f"✅ {DOCS_PATH}", file=sys.stderr)

    for year in sorted(by_year.keys()):
        print(f"\n{year}년 (GS):", file=sys.stderr)
        for month in sorted(by_year[year].keys()):
            d = by_year[year][month]
            b = d.get("budget_rn", 0)
            sr = d['start_ratio']
            sr_str = f"{sr}%" if sr is not None else "(목표 미입력)"
            crn = d.get("closing_rn", 0)
            crn_str = f"  closing={crn:,}" if crn else ""
            print(f"  {month}월: advance {d['advance_rn']:,} / "
                  f"{'budget '+format(b,',') if b else 'total '+format(d['total_rn'],',')} "
                  f"= Start {sr_str}{crn_str}", file=sys.stderr)


if __name__ == "__main__":
    main()
