#!/usr/bin/env python3
"""
parse_campaign86.py — 기획전 196번 (비발디파크 x 5월 전략 프로모션) 실적 집계

- campaign_data.json에서 196번 패키지코드(123개) 로드
- 온북 27+43에서 해당 코드만 필터링 (28/44 사용 금지)
- 투숙일 = 판매일자 기준
- 세그먼트: 회원/무기명/D멤버스/OTA/G-OTA/Inbound/기타
- KPI: RN, ADR, 객실매출, 총매출, 수수료
- 투숙일 기준 일별 객실수 집계
"""
import os, json, re, sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
RAW_DB_DIR = PROJECT_DIR / "data" / "raw_db"
OUTPUT_DIR = PROJECT_DIR / "docs" / "data"
CAMPAIGN_JSON = OUTPUT_DIR / "campaign_data.json"

# ─── 패키지코드 로드 ───
def load_package_codes(campaign_key="196"):
    """campaign_data.json에서 특정 Key의 패키지코드 + 메타 로드"""
    with open(CAMPAIGN_JSON, 'r', encoding='utf-8') as f:
        data = json.load(f)
    codes = set(data.get('key_to_codes', {}).get(campaign_key, []))
    events = [e for e in data.get('events', []) if e.get('key') == campaign_key]
    return codes, events


# ─── 세그먼트 분류 ───
# OTA = 여기어때 / 놀(야놀자/놀유니버스)
# G-OTA = 아고다 / 트립닷컴 / 익스피디아
# Inbound = 여행사
# 회원 = 회원PKG (code MP)
# 무기명 = 자사패키지 (code 73) + AGENT 없는 건
# D멤버스 = code 34
# 기타 = 나머지 (쿠팡, 네이버, 스마트인피니 등)

OTA_KW = ["여기어때", "놀유니버스", "야놀자"]
GOTA_KW = ["아고다", "트립닷컴", "익스피디아"]
INBOUND_KW = ["여행사", "하나투어"]


def classify_segment(code_num, code_name, agent_name):
    """세그먼트 분류 (기획전 196번 전용)"""
    code = (code_num or "").strip()
    name = (code_name or "").strip()
    agent = (agent_name or "").strip()

    # D멤버스
    if code == "34" or "D멤버스" in name:
        return "D멤버스"

    # 회원PKG
    if code == "MP" or "회원" in name:
        return "회원"

    # G-OTA (코드 기반)
    if code in ("A4", "A5"):
        # A4/A5 중에서도 AGENT로 세분화
        for kw in GOTA_KW:
            if kw in agent:
                return "G-OTA"
        return "G-OTA"  # A4/A5는 모두 G-OTA

    # Inbound
    if code == "58":
        return "Inbound"

    # OTA 계열 코드 (53, 72) → AGENT로 세분화
    if code in ("53", "72"):
        for kw in OTA_KW:
            if kw in agent:
                return "OTA"
        for kw in GOTA_KW:
            if kw in agent:
                return "G-OTA"
        for kw in INBOUND_KW:
            if kw in agent:
                return "Inbound"
        # AGENT 없거나 매핑 안 됨
        if not agent:
            return "기타"
        return "기타"

    # 자사패키지 → 무기명
    if code == "73" or "자사" in name:
        return "무기명"

    # 나머지
    return "기타"


def extract_channel(agent_name):
    """AGENT명 → 거래처명"""
    if not agent_name:
        return "자사"
    agent = agent_name.strip()
    agent = re.sub(r"^(OTA_|GOTA_|마케팅_)", "", agent)
    agent = re.sub(r"\(.*?\)", "", agent).strip()
    return agent if agent else "자사"


def normalize_property(prop_name):
    if not prop_name:
        return "미분류"
    return re.sub(r'^\d+\.\s*', '', prop_name).strip() or "미분류"


def parse_file(filepath, file_type, target_codes):
    """단일 txt 파일에서 target_codes에 해당하는 레코드만 추출"""
    encodings = ['cp949', 'euc-kr', 'utf-8']
    records = []

    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                header_line = f.readline().strip()
                headers = header_line.split(';')
                col_map = {h.strip(): i for i, h in enumerate(headers)}

                idx = {
                    'sell_date': col_map.get('판매일자', -1),
                    'prop': col_map.get('영업장명', -1),
                    'cprop': col_map.get('변경사업장명', -1),
                    'member_code': col_map.get('회원번호', -1),
                    'member_name': col_map.get('회원명', -1),
                    'user_name': col_map.get('이용자명', -1),
                    'code_num': col_map.get('변경예약집계코드', -1),
                    'code_name': col_map.get('변경예약집계코드명', -1),
                    'agent': col_map.get('AGENT명', -1),
                    'night_rate': col_map.get('1박객실료', -1),
                    'rooms': col_map.get('객실수', -1),
                    'pkg_total': col_map.get('PKG패키지총금액', -1),
                    'sell_price': col_map.get('판매가', -1),
                    'deposit': col_map.get('입금가', -1),
                    'commission': col_map.get('수수료', -1),
                    'checkin': col_map.get('입실일자', -1),
                }

                seen_hashes = set()
                for line in f:
                    line_stripped = line.rstrip('\n\r')
                    h = hash(line_stripped)
                    if h in seen_hashes:
                        continue
                    seen_hashes.add(h)

                    parts = line.split(';')
                    plen = len(parts)

                    def _get(key):
                        i = idx.get(key, -1)
                        return parts[i].strip() if 0 <= i < plen else ''

                    def _int(key):
                        v = _get(key)
                        try:
                            return int(v) if v else 0
                        except ValueError:
                            return 0

                    # 패키지코드 필터
                    member_code = _get('member_code')
                    if member_code not in target_codes:
                        continue

                    # 판매일자 (투숙일)
                    sell_date = _get('sell_date')
                    if len(sell_date) < 6:
                        sell_date = _get('checkin')
                        if len(sell_date) < 6:
                            continue

                    # 거래처 제거 (회원명==이용자명, Inbound 예외)
                    code_num = _get('code_num')
                    member_name = _get('member_name')
                    user_name = _get('user_name')
                    if member_name and user_name and member_name == user_name:
                        if code_num != '58':
                            continue

                    # 매출조정 제거
                    if '매출조정' in member_name or '매출조정' in user_name:
                        continue

                    # 사업장
                    cprop = _get('cprop')
                    prop_raw = _get('prop')
                    prop_name = normalize_property(cprop) if cprop else normalize_property(prop_raw)

                    code_name = _get('code_name')
                    agent_name = _get('agent')

                    rooms = _int('rooms')
                    rn = rooms if rooms > 0 else 1
                    night_rate = _int('night_rate')

                    # 객실매출 = 1박객실료 × 객실수 / 1.1 (VAT 제외)
                    room_rev = int(night_rate * rn / 1.1)

                    # 총매출 = PKG패키지총금액 (27번) or 판매가 (43번)
                    pkg_total = _int('pkg_total')
                    sell_price = _int('sell_price')
                    total_amount = pkg_total if pkg_total > 0 else (sell_price if sell_price > 0 else night_rate)
                    # 총매출: 패키지총금액은 이미 1실 기준이므로 × rn (연박 시 각 행이 1박)
                    total_rev = int(total_amount / 1.1)

                    commission = _int('commission')

                    segment = classify_segment(code_num, code_name, agent_name)
                    channel = extract_channel(agent_name)

                    records.append({
                        'sell_date': sell_date[:8] if len(sell_date) >= 8 else sell_date,
                        'stay_month': sell_date[:6],
                        'prop': prop_name,
                        'member_code': member_code,
                        'segment': segment,
                        'channel': channel,
                        'rn': rn,
                        'room_rev': room_rev,
                        'total_rev': total_rev,
                        'commission': commission,
                    })

                print(f"  [{file_type}] {os.path.basename(filepath)} → {len(records):,}건")
                return records

        except UnicodeDecodeError:
            continue

    print(f"  인코딩 실패: {filepath}")
    return []


def aggregate(records):
    """레코드 → 다차원 집계"""
    def new_bucket():
        return {'rn': 0, 'room_rev': 0, 'total_rev': 0, 'commission': 0}

    grand_total = new_bucket()
    by_month = defaultdict(new_bucket)
    by_prop = defaultdict(lambda: defaultdict(new_bucket))
    by_seg = defaultdict(lambda: defaultdict(new_bucket))
    by_prop_seg = defaultdict(lambda: defaultdict(lambda: defaultdict(new_bucket)))
    by_channel = defaultdict(lambda: defaultdict(new_bucket))
    by_prop_channel = defaultdict(lambda: defaultdict(lambda: defaultdict(new_bucket)))
    by_date = defaultdict(lambda: defaultdict(new_bucket))  # 투숙일별 세그먼트별
    by_date_prop = defaultdict(lambda: defaultdict(lambda: defaultdict(new_bucket)))  # 투숙일별 사업장별 세그먼트별
    pkg_codes_by_prop = defaultdict(set)

    for r in records:
        m = r['stay_month']
        p = r['prop']
        s = r['segment']
        ch = r['channel']
        d = r['sell_date']

        for target in [grand_total, by_month[m], by_prop[p][m], by_seg[s][m],
                        by_prop_seg[p][s][m], by_channel[ch][m],
                        by_prop_channel[p][ch][m],
                        by_date[d][s], by_date_prop[d][p][s]]:
            target['rn'] += r['rn']
            target['room_rev'] += r['room_rev']
            target['total_rev'] += r['total_rev']
            target['commission'] += r['commission']

        pkg_codes_by_prop[p].add(r['member_code'])

    return {
        'grand_total': grand_total,
        'by_month': dict(by_month),
        'by_prop': {p: dict(v) for p, v in by_prop.items()},
        'by_seg': {s: dict(v) for s, v in by_seg.items()},
        'by_prop_seg': {p: {s: dict(v) for s, v in sv.items()} for p, sv in by_prop_seg.items()},
        'by_channel': {c: dict(v) for c, v in by_channel.items()},
        'by_prop_channel': {p: {c: dict(v) for c, v in cv.items()} for p, cv in by_prop_channel.items()},
        'by_date': {d: dict(v) for d, v in by_date.items()},
        'by_date_prop': {d: {p: dict(v) for p, v in pv.items()} for d, pv in by_date_prop.items()},
        'pkg_codes_by_prop': {p: sorted(v) for p, v in pkg_codes_by_prop.items()},
    }


def m(d):
    """원시 집계 → KPI 메트릭"""
    rn = d.get('rn', 0)
    room_rev = d.get('room_rev', 0)
    total_rev = d.get('total_rev', 0)
    commission = d.get('commission', 0)
    return {
        'rn': rn,
        'adr': round(room_rev / rn / 1000) if rn > 0 else 0,  # 천원
        'room_rev_m': round(room_rev / 1e6, 2),
        'total_rev_m': round(total_rev / 1e6, 2),
        'commission_m': round(commission / 1e6, 2),
    }


def distribute_kpi(agg, total_kpi_rn=2000, total_kpi_rev_m=426):
    """KPI 배분: 회원1:무기명1:D멤버스1:OTA4:G-OTA3"""
    SEG_RATIOS = {'회원': 1, '무기명': 1, 'D멤버스': 1, 'OTA': 4, 'G-OTA': 3}
    ratio_sum = sum(SEG_RATIOS.values())  # 10
    seg_kpi_rn = {s: int(total_kpi_rn * r / ratio_sum) for s, r in SEG_RATIOS.items()}

    by_seg = agg['by_seg']
    all_months = sorted(set(mo for sd in by_seg.values() for mo in sd))

    # 세그먼트×월별 KPI (실적 비율 기반)
    kpi_seg_month = {}
    for seg, kpi_rn in seg_kpi_rn.items():
        seg_data = by_seg.get(seg, {})
        month_rns = {mo: seg_data.get(mo, {}).get('rn', 0) for mo in all_months}
        total_rn = sum(month_rns.values())
        kpi_seg_month[seg] = {}
        if total_rn > 0:
            for mo in all_months:
                kpi_seg_month[seg][mo] = round(kpi_rn * month_rns[mo] / total_rn)
        else:
            n = len(all_months) or 1
            for i, mo in enumerate(all_months):
                kpi_seg_month[seg][mo] = kpi_rn // n + (1 if i < kpi_rn % n else 0)

    # 사업장×세그먼트별 KPI (실적 비율)
    kpi_prop_seg = {}
    for seg, kpi_rn in seg_kpi_rn.items():
        prop_totals = {}
        for p in agg['by_prop_seg']:
            prop_totals[p] = sum(
                agg['by_prop_seg'][p].get(seg, {}).get(mo, {}).get('rn', 0)
                for mo in all_months
            )
        total_rn = sum(prop_totals.values())
        for p, p_rn in prop_totals.items():
            if p not in kpi_prop_seg:
                kpi_prop_seg[p] = {}
            kpi_prop_seg[p][seg] = round(kpi_rn * p_rn / total_rn) if total_rn > 0 else 0

    return {
        'total_kpi_rn': total_kpi_rn,
        'total_kpi_rev_m': total_kpi_rev_m,
        'seg_kpi_rn': seg_kpi_rn,
        'kpi_seg_month': kpi_seg_month,
        'kpi_prop_seg': kpi_prop_seg,
    }


def build_output(agg, kpi, events):
    """JSON 출력 구조"""
    by_seg = agg['by_seg']
    by_prop = agg['by_prop']
    by_prop_seg = agg['by_prop_seg']
    by_channel = agg['by_channel']
    by_date = agg['by_date']

    all_months = sorted(set(mo for d in [agg['by_month']] + list(by_seg.values()) for mo in d))
    all_props = sorted(by_prop.keys())
    all_segs = sorted(by_seg.keys())
    all_channels = sorted(by_channel.keys())

    # ─ 전체 합계 ─
    total = m(agg['grand_total'])

    # ─ 월별 ─
    monthly = {mo: m(agg['by_month'].get(mo, {})) for mo in all_months}

    # ─ 세그먼트별 ─
    seg_monthly = {}
    seg_totals = {}
    for s in all_segs:
        raw_total = {'rn': 0, 'room_rev': 0, 'total_rev': 0, 'commission': 0}
        seg_monthly[s] = {}
        for mo in all_months:
            d = by_seg[s].get(mo, {})
            seg_monthly[s][mo] = m(d)
            for k in raw_total:
                raw_total[k] += d.get(k, 0)
        seg_totals[s] = m(raw_total)

    # ─ 사업장별 ─
    prop_monthly = {}
    prop_totals = {}
    for p in all_props:
        raw_total = {'rn': 0, 'room_rev': 0, 'total_rev': 0, 'commission': 0}
        prop_monthly[p] = {}
        for mo in all_months:
            d = by_prop[p].get(mo, {})
            prop_monthly[p][mo] = m(d)
            for k in raw_total:
                raw_total[k] += d.get(k, 0)
        prop_totals[p] = m(raw_total)

    # ─ 사업장×세그먼트 ─
    prop_seg = {}
    for p in all_props:
        prop_seg[p] = {}
        for s in all_segs:
            if s not in by_prop_seg.get(p, {}):
                continue
            raw_total = {'rn': 0, 'room_rev': 0, 'total_rev': 0, 'commission': 0}
            prop_seg[p][s] = {'monthly': {}}
            for mo in all_months:
                d = by_prop_seg[p].get(s, {}).get(mo, {})
                prop_seg[p][s]['monthly'][mo] = m(d)
                for k in raw_total:
                    raw_total[k] += d.get(k, 0)
            prop_seg[p][s]['total'] = m(raw_total)

    # ─ 거래처별 ─
    ch_totals = {}
    for ch in all_channels:
        raw_total = {'rn': 0, 'room_rev': 0, 'total_rev': 0, 'commission': 0}
        for mo in all_months:
            d = by_channel[ch].get(mo, {})
            for k in raw_total:
                raw_total[k] += d.get(k, 0)
        ch_totals[ch] = m(raw_total)

    # ─ 투숙일별 RN (일별 세그먼트별) ─
    all_dates = sorted(by_date.keys())
    stay_daily = {}
    for d in all_dates:
        segs = by_date[d]
        day_total_rn = sum(v.get('rn', 0) for v in segs.values())
        stay_daily[d] = {
            'total_rn': day_total_rn,
            'by_segment': {s: v.get('rn', 0) for s, v in segs.items()},
        }

    # ─ KPI 달성률 ─
    kpi_achievement = {}
    for s in kpi['seg_kpi_rn']:
        kpi_rn = kpi['seg_kpi_rn'][s]
        actual_rn = seg_totals.get(s, {}).get('rn', 0)
        kpi_achievement[s] = {
            'kpi_rn': kpi_rn,
            'actual_rn': actual_rn,
            'pct': round(actual_rn / kpi_rn * 100, 1) if kpi_rn > 0 else 0,
            'by_month': {},
        }
        for mo in all_months:
            m_kpi = kpi['kpi_seg_month'].get(s, {}).get(mo, 0)
            m_actual = seg_monthly.get(s, {}).get(mo, {}).get('rn', 0)
            kpi_achievement[s]['by_month'][mo] = {
                'kpi_rn': m_kpi,
                'actual_rn': m_actual,
                'pct': round(m_actual / m_kpi * 100, 1) if m_kpi > 0 else 0,
            }

    # 사업장별 KPI 달성
    prop_kpi = {}
    for p in all_props:
        prop_kpi[p] = {}
        for s in kpi['seg_kpi_rn']:
            k_rn = kpi['kpi_prop_seg'].get(p, {}).get(s, 0)
            a_rn = prop_seg.get(p, {}).get(s, {}).get('total', {}).get('rn', 0)
            prop_kpi[p][s] = {
                'kpi_rn': k_rn,
                'actual_rn': a_rn,
                'pct': round(a_rn / k_rn * 100, 1) if k_rn > 0 else 0,
            }

    # 메타 from events
    meta_events = []
    for ev in events:
        meta_events.append({
            '구분': ev.get('구분', ''),
            '사업장': ev.get('사업장', ''),
            '채널': ev.get('채널', ''),
            '판매시작': ev.get('판매시작', ''),
            '판매종료': ev.get('판매종료', ''),
            '투숙시작': ev.get('투숙시작', ''),
            '투숙종료': ev.get('투숙종료', ''),
            '상품': ev.get('상품', ''),
            '상품명': ev.get('상품명', ''),
        })

    return {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'campaign_key': '196',
        'campaign_name': '비발디파크 x 5월 전략 프로모션',
        'events': meta_events,
        'meta': {
            'months': all_months,
            'properties': all_props,
            'segments': all_segs,
            'channels': all_channels,
            'total_pkg_codes': sum(len(v) for v in agg['pkg_codes_by_prop'].values()),
        },
        'total': total,
        'monthly': monthly,
        'by_segment': {'monthly': seg_monthly, 'totals': seg_totals},
        'by_property': {'monthly': prop_monthly, 'totals': prop_totals},
        'by_property_segment': prop_seg,
        'by_channel': ch_totals,
        'stay_daily': stay_daily,
        'kpi': {
            'total_kpi_rn': kpi['total_kpi_rn'],
            'total_kpi_rev_m': kpi['total_kpi_rev_m'],
            'seg_kpi_rn': kpi['seg_kpi_rn'],
            'achievement': kpi_achievement,
            'by_property': prop_kpi,
            'kpi_seg_month': kpi['kpi_seg_month'],
        },
        'pkg_codes_by_prop': agg['pkg_codes_by_prop'],
    }


def main():
    print("=" * 60)
    print("기획전 196번 (비발디파크 x 5월 전략 프로모션) 실적 집계")
    print("=" * 60)

    # 패키지코드 로드
    target_codes, events = load_package_codes("196")
    print(f"패키지코드: {len(target_codes)}개")

    if not target_codes:
        print("패키지코드가 없습니다. campaign_data.json을 먼저 생성하세요.")
        sys.exit(1)

    # KPI
    kpi_rn = 2000
    kpi_rev_m = 426
    for ev in events:
        if ev.get('KPI_RN'):
            try:
                kpi_rn = int(ev['KPI_RN'].replace(',', ''))
            except ValueError:
                pass
        if ev.get('KPI_REV_M'):
            try:
                kpi_rev_m = float(ev['KPI_REV_M'].replace(',', ''))
            except ValueError:
                pass
    print(f"KPI: {kpi_rn:,}실 / {kpi_rev_m:,.0f}백만원")

    # 파싱 (2026년만 — 이 기획전은 2026년 기획)
    all_records = []
    year = "2026"
    year_dir = RAW_DB_DIR / year
    if year_dir.exists():
        for fpath in sorted(year_dir.iterdir()):
            if not fpath.name.lower().endswith('.txt'):
                continue
            if fpath.name.startswith('27'):
                file_type = '27'
            elif fpath.name.startswith('43'):
                file_type = '43'
            else:
                continue
            records = parse_file(str(fpath), file_type, target_codes)
            all_records.extend(records)

    print(f"\n총 레코드: {len(all_records):,}")

    if not all_records:
        print("해당 패키지코드의 실적이 없습니다.")
        # 빈 결과 저장
        output = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'campaign_key': '196',
            'campaign_name': '비발디파크 x 5월 전략 프로모션',
            'total': {'rn': 0, 'adr': 0, 'room_rev_m': 0, 'total_rev_m': 0, 'commission_m': 0},
            'meta': {'months': [], 'properties': [], 'segments': [], 'channels': []},
        }
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = OUTPUT_DIR / "campaign86_data.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        print(f"빈 결과 저장: {out_path}")
        return

    # 집계
    agg = aggregate(all_records)

    # KPI 배분
    kpi_data = distribute_kpi(agg, kpi_rn, kpi_rev_m)

    # 출력
    output = build_output(agg, kpi_data, events)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "campaign86_data.json"
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n출력: {out_path}")
    t = output['total']
    print(f"  총 RN: {t['rn']:,}")
    print(f"  ADR: {t['adr']:,} 천원")
    print(f"  객실매출: {t['room_rev_m']:,.1f} 백만원")
    print(f"  총매출: {t['total_rev_m']:,.1f} 백만원")
    print(f"  수수료: {t['commission_m']:,.1f} 백만원")
    print(f"\n세그먼트별:")
    for s in sorted(output['by_segment']['totals'].keys()):
        v = output['by_segment']['totals'][s]
        kpi_info = output['kpi']['achievement'].get(s, {})
        kpi_str = f" → KPI {kpi_info.get('kpi_rn', '-')}실, 달성 {kpi_info.get('pct', '-')}%" if kpi_info else ""
        print(f"  {s}: RN {v['rn']:,} | ADR {v['adr']}천원 | 객실 {v['room_rev_m']:,.1f}M | 총 {v['total_rev_m']:,.1f}M | 수수료 {v['commission_m']:,.1f}M{kpi_str}")
    print(f"\n사업장별:")
    for p in sorted(output['by_property']['totals'].keys(), key=lambda x: -output['by_property']['totals'][x]['rn']):
        v = output['by_property']['totals'][p]
        print(f"  {p}: RN {v['rn']:,} | ADR {v['adr']}천원 | 객실 {v['room_rev_m']:,.1f}M | 총 {v['total_rev_m']:,.1f}M")
    print(f"\n투숙일별 집계: {len(output.get('stay_daily', {}))}일")


if __name__ == "__main__":
    main()
