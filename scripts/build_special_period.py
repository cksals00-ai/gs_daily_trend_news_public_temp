#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_special_period.py — 스페셜(최성수기)/연휴 기간 전략상품 실적 팔로업 산출

무엇을 하나:
  트렌드 리포트(docs/index.html)의 "스페셜 기간 전략상품 실적 팔로업" 섹션에 쓸
  데이터(data/special_period.json + docs 사본)를 만든다.

  · 기간(스페셜 최성수기 / 연휴·추석)은 아래 PERIODS 상수로 고정(설정값).
  · "투숙일자별"(=판매일자, 연박은 매일 반복) RN·매출·ADR을
    당해(2026, 현재 온북)와 전년(2025, 대응 캘린더일 실적)으로 각각 뽑는다.
  · 분해 축 3종: ① 사업장별 ② 세그먼트별(OTA/G-OTA/Inbound) ③ 채널별(거래처, OTA+G-OTA)
  · YoY = 2026 온북 ÷ 2025 동일 투숙일 실적.

⚠️ 독립 산출물 원칙 (exec_segment 패턴):
  · db_aggregated / otb_data / *.gz 를 절대 건드리지 않는다(읽지도 쓰지도 않음 — 대조검증 시에만 read-only).
  · 원천 data/raw_db 만 read-only 로 스트리밍 파싱한다.
  · 파싱 로직(컬럼/채널/세그/사업장 정규화)은 parse_raw_db.py 의 canonical helper 를 그대로 재사용
    → db_aggregated.stay_date_daily 와 세그먼트 net_rn 이 정확히 일치(빌드시 자동 대조).

세그먼트 기준: RN/매출은 OTA+G-OTA+Inbound (당일현황 actual 표준, by_segment 와 동일 스코프).
채널(거래처) 축은 OTA+G-OTA 만(Inbound·미분류 제외) — 리포트 거래처별 관례와 동일.
"""
import os, sys, json, hashlib
from collections import defaultdict
from datetime import datetime, date

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

# canonical 파싱 helper 재사용 (수정 금지 · 그대로 import)
from parse_raw_db import (
    normalize_property, get_region, extract_channel, classify_segment,
)

PROJECT_DIR = os.path.dirname(SCRIPTS_DIR)
RAW_DB_DIR  = os.path.join(PROJECT_DIR, "data", "raw_db")
OUT_DATA    = os.path.join(PROJECT_DIR, "data", "special_period.json")
OUT_DOCS    = os.path.join(PROJECT_DIR, "docs", "data", "special_period.json")
DB_AGG      = os.path.join(PROJECT_DIR, "docs", "data", "db_aggregated.json")

# ─── 기간 정의(첨부 md 확정값 · 설정 상수) ───────────────────────────────
#   dates_2026 = 대상 투숙일(YYYYMMDD). 전년(2025)은 동일 MMDD 로 자동 매핑.
def _daterange(y, m1, d1, m2, d2):
    """[y-m1-d1 .. y-m2-d2] 폐구간의 YYYYMMDD 리스트(월경계 포함)."""
    a, b = date(y, m1, d1), date(y, m2, d2)
    out, cur = [], a
    while cur <= b:
        out.append(cur.strftime("%Y%m%d"))
        cur = date.fromordinal(cur.toordinal() + 1)
    return out

PERIODS = [
    {
        "key": "special",
        "label": "스페셜(최성수기)",
        "range_label": "2026.07.24(금)~08.01(토) · 9일",
        "dates_2026": _daterange(2026, 7, 24, 8, 1),   # 9일
        "scope_note": "전 사업장 공통",
        "designated": None,   # 전 사업장
    },
    {
        "key": "holiday",
        "label": "연휴/추석",
        "range_label": "2026.09.23(수)~09.26(토) · 4일",
        "dates_2026": _daterange(2026, 9, 23, 9, 26),  # 4일
        "scope_note": "일부 사업장(지정 5곳) 대상 기간",
        # md 명시 5곳(정규화된 사업장명) — 표시 강조용. 데이터는 전 사업장 온북을 그대로 노출.
        "designated": ["소노벨 비발디파크", "소노캄 여수", "쏠비치 진도", "소노캄 고양", "소노캄 제주"],
    },
]

# 세그먼트 스코프 (헤드라인/사업장/세그 축)
CORE_SEGMENTS = {"OTA", "G-OTA", "Inbound"}
# 채널(거래처) 축 스코프: OTA+G-OTA 만
CHANNEL_EXCLUDE = {"Inbound", "미분류"}


def _year_of(ymd):
    return ymd[:4]


def _to_prev_year(ymd):
    """YYYYMMDD → 전년 동일 MMDD (YYYY-1)MMDD."""
    return str(int(ymd[:4]) - 1) + ymd[4:]


def _iter_booking_files(year_dir):
    """해당 연도 폴더에서 예약(booking) 파일(27/43)만. 취소(28/44)는 제외."""
    if not os.path.isdir(year_dir):
        return
    for fn in sorted(os.listdir(year_dir)):
        if not fn.lower().endswith(".txt"):
            continue
        if fn.startswith("27"):
            yield os.path.join(year_dir, fn), "27"
        elif fn.startswith("43"):
            yield os.path.join(year_dir, fn), "43"


def _parse_file(filepath, file_type, wanted_dates, acc):
    """예약 파일 1개를 스트리밍 파싱해 acc 에 집계.

    acc[(stay_date, prop, segment, channel)] = {'rn':int, 'rev':int}
    wanted_dates: 대상 YYYYMMDD set (그 외 행은 즉시 스킵 → 빠름).
    로직은 parse_raw_db.parse_and_aggregate 의 booking 경로와 동일(행 dedup·거래처/매출조정 제거).
    """
    encodings = ["cp949", "euc-kr", "utf-8"]
    for enc in encodings:
        try:
            with open(filepath, "r", encoding=enc) as f:
                headers = f.readline().strip().split(";")
                col_map = {h.strip(): i for i, h in enumerate(headers)}
                has_change_prop = "변경사업장명" in col_map
                code_col     = "변경예약집계코드명" if "변경예약집계코드명" in col_map else "예약집계명"
                code_num_col = "변경예약집계코드"   if "변경예약집계코드"   in col_map else "예약집계코드"

                idx_prop     = col_map.get("영업장명", -1)
                idx_cprop    = col_map.get("변경사업장명", -1) if has_change_prop else -1
                idx_selldate = col_map.get("판매일자", -1)
                idx_checkin  = col_map.get("입실일자", -1)
                idx_code     = col_map.get(code_col, -1)
                idx_code_num = col_map.get(code_num_col, -1)
                idx_agent    = col_map.get("AGENT명", -1)
                idx_rooms    = col_map.get("객실수", -1)
                idx_1night   = col_map.get("1박객실료", -1)
                idx_member   = col_map.get("회원명", -1)
                idx_user     = col_map.get("이용자명", -1)

                seen = set()
                for line in f:
                    line_stripped = line.rstrip("\n\r")
                    h = hash(line_stripped)
                    if h in seen:
                        continue
                    seen.add(h)
                    parts = line.split(";")
                    plen = len(parts)
                    try:
                        # 투숙일자(판매일자) — 대상 밖이면 즉시 스킵
                        sell_date = parts[idx_selldate].strip() if 0 <= idx_selldate < plen else ""
                        if len(sell_date) < 8:
                            sell_date = parts[idx_checkin].strip() if 0 <= idx_checkin < plen else ""
                        sd = sell_date[:8]
                        if sd not in wanted_dates:
                            continue

                        # 거래처 제거: 예약자명 == 회원명 (Inbound 코드58 예외)
                        code_num = parts[idx_code_num].strip() if 0 <= idx_code_num < plen else ""
                        if 0 <= idx_member < plen and 0 <= idx_user < plen:
                            mn = parts[idx_member].strip(); un = parts[idx_user].strip()
                            if mn and un and mn == un and code_num != "58":
                                continue
                            if "매출조정" in (parts[idx_member] if idx_member < plen else "") \
                               or "매출조정" in (parts[idx_user] if idx_user < plen else ""):
                                continue

                        code_name  = parts[idx_code].strip()  if 0 <= idx_code  < plen else ""
                        agent_name = parts[idx_agent].strip() if 0 <= idx_agent < plen else ""

                        def _int(idx):
                            if 0 <= idx < plen:
                                v = parts[idx].strip()
                                return int(v) if v else 0
                            return 0
                        rooms = _int(idx_rooms); night_rate = _int(idx_1night)
                        rn = rooms if rooms > 0 else 1
                        rev = int(night_rate * rn / 1.1)

                        segment = classify_segment(code_num, code_name, agent_name, file_type)
                        if segment not in CORE_SEGMENTS:
                            continue  # OTA+G-OTA+Inbound 만

                        prop_raw = parts[idx_prop] if 0 <= idx_prop < plen else ""
                        cprop = parts[idx_cprop].strip() if 0 <= idx_cprop < plen else ""
                        prop_name = normalize_property(cprop) if cprop else normalize_property(prop_raw)
                        channel = extract_channel(agent_name, code_num)

                        e = acc[(sd, prop_name, segment, channel)]
                        e["rn"] += rn
                        e["rev"] += rev
                    except (IndexError, ValueError):
                        continue
            return  # 인코딩 성공
        except UnicodeDecodeError:
            continue


def _collect_year(year, wanted_dates):
    """연도별 raw 폴더 스캔 → acc 반환."""
    year_dir = os.path.join(RAW_DB_DIR, str(year))
    acc = defaultdict(lambda: {"rn": 0, "rev": 0})
    files = list(_iter_booking_files(year_dir))
    for fp, ft in files:
        _parse_file(fp, ft, wanted_dates, acc)
    return acc, [os.path.basename(fp) for fp, _ in files]


def _adr(rev_won, rn):
    return int(round(rev_won / rn)) if rn else 0


def _yoy(cur, prev):
    if prev in (None, 0):
        return None
    return round((cur - prev) / prev * 100, 1)


def build_period(period):
    dates26 = period["dates_2026"]
    dates25 = [_to_prev_year(d) for d in dates26]
    want26 = set(dates26)
    want25 = set(dates25)

    acc26, files26 = _collect_year(2026, want26)
    acc25, files25 = _collect_year(2025, want25)

    # 전년 동일 투숙일 매핑(2026일 → 2025일)
    d26_to_d25 = {d: _to_prev_year(d) for d in dates26}

    def _blank():
        return {"rn": 0, "rev": 0}

    # ── 축별 집계 컨테이너 ──
    #   각 축: { key: { 'cur': {date: {rn,rev}}, 'prev': {date: {rn,rev}} } }
    def _axis():
        return defaultdict(lambda: {"cur": defaultdict(_blank), "prev": defaultdict(_blank)})

    ax_prop = _axis()   # 사업장
    ax_seg  = _axis()   # 세그먼트
    ax_chan = _axis()   # 채널(거래처)

    # 일자별 총계(전 축 공통 = OTA+G-OTA+Inbound)
    daily_cur = defaultdict(_blank)   # by 2026 date
    daily_prev = defaultdict(_blank)  # by 2026 date (전년 대응일 값)

    for (sd, prop, seg, chan), v in acc26.items():
        ax_prop[prop]["cur"][sd]["rn"] += v["rn"]; ax_prop[prop]["cur"][sd]["rev"] += v["rev"]
        ax_seg[seg]["cur"][sd]["rn"]   += v["rn"]; ax_seg[seg]["cur"][sd]["rev"]   += v["rev"]
        daily_cur[sd]["rn"] += v["rn"]; daily_cur[sd]["rev"] += v["rev"]
        if seg not in CHANNEL_EXCLUDE and chan not in CHANNEL_EXCLUDE:
            ax_chan[chan]["cur"][sd]["rn"] += v["rn"]; ax_chan[chan]["cur"][sd]["rev"] += v["rev"]

    for (sd25, prop, seg, chan), v in acc25.items():
        # 2025 일자를 대응하는 2026 일자로 되매핑(같은 MMDD)
        sd = str(int(sd25[:4]) + 1) + sd25[4:]
        if sd not in want26:
            continue
        ax_prop[prop]["prev"][sd]["rn"] += v["rn"]; ax_prop[prop]["prev"][sd]["rev"] += v["rev"]
        ax_seg[seg]["prev"][sd]["rn"]   += v["rn"]; ax_seg[seg]["prev"][sd]["rev"]   += v["rev"]
        daily_prev[sd]["rn"] += v["rn"]; daily_prev[sd]["rev"] += v["rev"]
        if seg not in CHANNEL_EXCLUDE and chan not in CHANNEL_EXCLUDE:
            ax_chan[chan]["prev"][sd]["rn"] += v["rn"]; ax_chan[chan]["prev"][sd]["rev"] += v["rev"]

    def _emit_axis(axis, top=None, exclude_channel=False):
        rows = []
        for key, io in axis.items():
            cur_rn = sum(io["cur"][d]["rn"] for d in dates26)
            cur_rev = sum(io["cur"][d]["rev"] for d in dates26)
            prev_rn = sum(io["prev"][d]["rn"] for d in dates26)
            prev_rev = sum(io["prev"][d]["rev"] for d in dates26)
            # 일자별 배열(2026 투숙일 순서)
            per_day = []
            for d in dates26:
                c = io["cur"][d]; p = io["prev"][d]
                per_day.append({
                    "date": d,
                    "rn": c["rn"], "rev_m": round(c["rev"]/1_000_000, 2), "adr": _adr(c["rev"], c["rn"]),
                    "py_rn": p["rn"], "py_rev_m": round(p["rev"]/1_000_000, 2), "py_adr": _adr(p["rev"], p["rn"]),
                    "yoy_rn": _yoy(c["rn"], p["rn"]),
                    "yoy_rev": _yoy(c["rev"], p["rev"]),
                    "yoy_adr": _yoy(_adr(c["rev"], c["rn"]), _adr(p["rev"], p["rn"])),
                })
            rows.append({
                "key": key,
                "rn": cur_rn, "rev_m": round(cur_rev/1_000_000, 2), "adr": _adr(cur_rev, cur_rn),
                "py_rn": prev_rn, "py_rev_m": round(prev_rev/1_000_000, 2), "py_adr": _adr(prev_rev, prev_rn),
                "yoy_rn": _yoy(cur_rn, prev_rn),
                "yoy_rev": _yoy(cur_rev, prev_rev),
                "yoy_adr": _yoy(_adr(cur_rev, cur_rn), _adr(prev_rev, prev_rn)),
                "daily": per_day,
            })
        rows.sort(key=lambda r: r["rn"], reverse=True)
        if top:
            rows = rows[:top]
        return rows

    # 일자별 총계(라인/표 상단)
    daily_total = []
    tot_cur_rn = tot_cur_rev = tot_prev_rn = tot_prev_rev = 0
    for d in dates26:
        c = daily_cur[d]; p = daily_prev[d]
        tot_cur_rn += c["rn"]; tot_cur_rev += c["rev"]; tot_prev_rn += p["rn"]; tot_prev_rev += p["rev"]
        daily_total.append({
            "date": d,
            "rn": c["rn"], "rev_m": round(c["rev"]/1_000_000, 2), "adr": _adr(c["rev"], c["rn"]),
            "py_rn": p["rn"], "py_rev_m": round(p["rev"]/1_000_000, 2), "py_adr": _adr(p["rev"], p["rn"]),
            "yoy_rn": _yoy(c["rn"], p["rn"]),
            "yoy_rev": _yoy(c["rev"], p["rev"]),
            "yoy_adr": _yoy(_adr(c["rev"], c["rn"]), _adr(p["rev"], p["rn"])),
        })

    return {
        "key": period["key"],
        "label": period["label"],
        "range_label": period["range_label"],
        "scope_note": period["scope_note"],
        "designated": period["designated"],
        "dates": dates26,
        "dow": [ "일월화수목금토"[date(int(d[:4]), int(d[4:6]), int(d[6:8])).isoweekday() % 7] for d in dates26 ],
        "summary": {
            "rn": tot_cur_rn, "rev_m": round(tot_cur_rev/1_000_000, 2), "adr": _adr(tot_cur_rev, tot_cur_rn),
            "py_rn": tot_prev_rn, "py_rev_m": round(tot_prev_rev/1_000_000, 2), "py_adr": _adr(tot_prev_rev, tot_prev_rn),
            "yoy_rn": _yoy(tot_cur_rn, tot_prev_rn),
            "yoy_rev": _yoy(tot_cur_rev, tot_prev_rev),
            "yoy_adr": _yoy(_adr(tot_cur_rev, tot_cur_rn), _adr(tot_prev_rev, tot_prev_rn)),
        },
        "daily_total": daily_total,
        "by_property": _emit_axis(ax_prop),
        "by_segment":  _emit_axis(ax_seg),
        "by_channel":  _emit_axis(ax_chan, top=20),
        "_files": {"2026": files26, "2025": files25},
    }


def validate_against_db(result_periods):
    """세그먼트 net_rn 을 db_aggregated.stay_date_daily 와 대조(read-only).
    기간 내 각 세그·일자 RN 이 일치해야 함(같은 원천·같은 로직).
    반환: (ok:bool, msgs:list)"""
    msgs = []
    try:
        db = json.load(open(DB_AGG, encoding="utf-8"))
        sdd = db.get("stay_date_daily", {})
    except Exception as e:
        return True, [f"(대조 스킵: db_aggregated 로드 실패 {e})"]

    ok = True
    for per in result_periods:
        for d in per["dates"]:
            if not d.startswith("2026"):
                continue
            month = d[:6]; day = int(d[6:8])
            m = sdd.get(month)
            if not m:
                continue
            days = m.get("days", [])
            if day not in days:
                continue
            di = days.index(day)
            for seg_row in per["by_segment"]:
                seg = seg_row["key"]
                db_rn = m.get("segments", {}).get(seg, {}).get("net_rn", [])
                db_val = db_rn[di] if di < len(db_rn) else 0
                my_val = next((x["rn"] for x in seg_row["daily"] if x["date"] == d), 0)
                if db_val != my_val:
                    ok = False
                    msgs.append(f"  ✗ {d} {seg}: 빌더 {my_val} ≠ db {db_val}")
    if ok:
        msgs.append("  ✓ 세그먼트 net_rn 이 db_aggregated.stay_date_daily 와 일치")
    return ok, msgs


def main():
    print("=== 스페셜/연휴 전략상품 실적 팔로업 산출 ===")
    if not os.path.isdir(RAW_DB_DIR):
        print(f"❌ raw_db 없음: {RAW_DB_DIR}"); sys.exit(1)

    periods = []
    for p in PERIODS:
        print(f"▶ {p['label']} ({p['range_label']}) 파싱…")
        periods.append(build_period(p))
        s = periods[-1]["summary"]
        print(f"   현재 온북 RN {s['rn']:,} · 매출 {s['rev_m']:.1f}백만 · ADR {s['adr']:,} "
              f"· YoY(RN) {s['yoy_rn']}%")

    ok, msgs = validate_against_db(periods)
    for line in msgs:
        print(line)
    if not ok:
        print("❌ 대조검증 실패 — 산출 중단(파일 미기록)")
        sys.exit(2)

    out = {
        "meta": {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "segment_scope": "OTA+G-OTA+Inbound (당일현황 actual 표준)",
            "channel_scope": "OTA+G-OTA 거래처만 (Inbound 제외)",
            "yoy_basis": "2026 현재 온북(진행 중) ÷ 2025 동일 투숙일(캘린더 대응) 마감 확정실적 — 동기간(리드타임 대칭) 아님",
            "resolution": "투숙일자별(=판매일자, 연박 매일반복) × 사업장/세그먼트/채널",
        },
        "periods": periods,
    }

    # 멱등: generated_at 제외 해시 비교 → 동일하면 기록 스킵
    def _hash(obj):
        c = json.loads(json.dumps(obj, ensure_ascii=False))
        c.get("meta", {}).pop("generated_at", None)
        for per in c.get("periods", []):
            per.pop("_files", None)
        return hashlib.md5(json.dumps(c, ensure_ascii=False, sort_keys=True).encode()).hexdigest()

    new_hash = _hash(out)
    old_hash = None
    if os.path.exists(OUT_DOCS):
        try:
            old_hash = _hash(json.load(open(OUT_DOCS, encoding="utf-8")))
        except Exception:
            old_hash = None
    if old_hash == new_hash:
        print("✅ 변경 없음(동일 수치) — 기록 스킵(멱등)")
        return

    payload = json.dumps(out, ensure_ascii=False, indent=1)
    for path in (OUT_DATA, OUT_DOCS):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(payload)
        print(f"✅ 기록: {path}")


if __name__ == "__main__":
    main()
