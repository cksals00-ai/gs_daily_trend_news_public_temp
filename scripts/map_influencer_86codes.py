#!/usr/bin/env python3
"""
map_influencer_86codes.py — 인플루언서 기획전 86XX 패키지코드 매핑 및 실적 집계

1. 27번 txt 4개년(2023~2026)에서 86XX 회원번호 중 인플루언서 키워드 포함 레코드 추출
2. 회원명의 [사업장], 인플루언서명 패턴으로 campaign_match_detail.json의 49개 기획전에 매핑
3. 매핑 결과 + 정확한 RN/매출/ADR 계산
4. campaign_individual_analysis.json 업데이트용 JSON 생성

중요:
- 27번 데이터는 넷(취소 반영됨). cancel 추가 차감 절대 금지
- 더미/추정 데이터 절대 금지
"""
import os, json, re, sys
import fs_utils  # macOS NFD→NFC 유니코드 정규화
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DB_DIR = PROJECT_DIR / "data" / "raw_db"
DOCS_DATA = PROJECT_DIR / "docs" / "data"

# ─── 사업장 정규화 맵 ───
PROP_NORMALIZE = {
    '벨비발디': '비발디', '캄비발디': '비발디', '문비발디': '비발디', '비발디파크': '비발디',
    '빌리지': '비발디', '펠리체': '비발디', '소노펫': '비발디',
    '벨제주': '제주', '캄제주': '제주',
    '쏠양': '양양', '쏠양양': '양양', '소노벨 양양': '양양',
    '삼척': '삼척', '소노벨 삼척': '삼척',
    '델피노': '델피노', '델': '델피노',
    '르네': '르네블루', '르네블루': '르네블루',
    '단양': '단양', '소노문 단양': '단양',
    '청송': '청송', '소노벨 청송': '청송',
    '진도': '진도', '소노캄 진도': '진도',
    '변산': '변산', '소노벨 변산': '변산',
    '천안': '천안', '소노벨 천안': '천안',
    '고양': '고양', '소노캄 고양': '고양',
    '여수': '여수', '소노캄 여수': '여수',
    '경주': '경주', '소노벨 경주': '경주',
    '거제': '거제', '소노캄 거제': '거제',
    '남해': '남해',
}

def normalize_prop_from_code_name(code_name):
    """86XX 회원명에서 사업장 추출"""
    # [벨비발디], (단양) 등 패턴
    m = re.match(r'[\[【\(]([^\]】\)]+)[\]】\)]', code_name)
    if m:
        raw = m.group(1).strip()
        for k, v in PROP_NORMALIZE.items():
            if k in raw:
                return v
    # 코드명 전체에서 검색
    for k, v in PROP_NORMALIZE.items():
        if k in code_name:
            return v
    return None

# ─── 인플루언서명 추출 ───
INFLUENCER_PATTERNS = {
    '한브로': ['한브로'],
    '제이드아이': ['제이드아이', '제이드'],
    '다온맘': ['다온맘'],
    '로제맘': ['로제맘', '로제'],
    '트윈윤혜나': ['트윈윤혜나', '윤혜나'],
    '룰루미뇽': ['룰루미뇽'],
    '이동아': ['이동아'],
    '희아': ['희아'],
    '스마트인피니': ['스마트인피니', '인피니'],
    '코이스토리': ['코이스토리', '코이'],
    '별이네자매': ['별이네자매', '별이네'],
    '은찐맘': ['은찐맘'],
    '마이아': ['마이아'],
    '미소데이즈': ['미소데이즈'],
    '바이쏨': ['바이쏨'],
    '리리맘': ['리리맘'],
    '요미맘': ['요미맘'],
    '민럽': ['민럽'],
    '지운맘': ['지운맘'],
    '캠퍼준영': ['캠퍼준영'],
    '꽃맘': ['꽃맘'],
    '율무': ['율무'],
    '로니맘': ['로니맘'],
    '카쇼라': ['카쇼라'],
    '비글리맘': ['비글리맘'],
    '제니져니': ['제니져니'],
    '맘맘': ['맘맘'],
    '캠미': ['캠미'],
    '쪼기': ['쪼기'],
    '프리즘': ['프리즘'],
}

def extract_influencer_from_code_name(code_name):
    """86XX 회원명에서 인플루언서명 추출"""
    for inf_name, patterns in INFLUENCER_PATTERNS.items():
        for pat in patterns:
            if pat in code_name:
                return inf_name
    return None

def normalize_prop_from_campaign(prop):
    """campaign_match_detail의 property를 정규화"""
    for k, v in PROP_NORMALIZE.items():
        if k in prop:
            return v
    return prop

def extract_influencer_from_channel(channel):
    """채널명에서 인플루언서명 추출"""
    for inf_name, patterns in INFLUENCER_PATTERNS.items():
        for pat in patterns:
            if pat in channel:
                return inf_name
    return None


# ─── 27번 파싱 ───
def parse_27_files():
    """모든 27번 txt에서 86XX 인플루언서 레코드 추출"""
    all_records = []
    
    for yr in ['2024', '2025', '2026']:
        ydir = RAW_DB_DIR / yr
        if not ydir.exists():
            continue
        for fname in sorted(os.listdir(ydir)):
            if not fname.startswith('27.') or not fname.endswith('.txt'):
                continue
            fpath = ydir / fname
            try:
                with open(fpath, encoding='cp949', errors='replace') as f:
                    lines = f.readlines()
            except:
                continue
            if not lines:
                continue
            
            headers = [h.strip() for h in lines[0].split(';')]
            col = {h: i for i, h in enumerate(headers)}
            
            for line in lines[1:]:
                parts = line.rstrip('\n').split(';')
                if len(parts) < 30:
                    continue
                
                def g(name, default=''):
                    i = col.get(name, -1)
                    return parts[i].strip() if 0 <= i < len(parts) else default
                
                def gi(name):
                    v = g(name, '0')
                    try: return int(v)
                    except: return 0
                
                mem_code = g('회원번호')
                if not mem_code.startswith('86'):
                    continue
                
                mem_name = g('회원명')
                # 인플루언서 키워드 필터
                if not any(kw in mem_name for kw in ['인플루언서', '인플', '유튜버', '블로거']):
                    continue
                
                # 매출조정 제거
                if '매출조정' in mem_name:
                    continue
                
                rn = gi('객실수')
                if rn <= 0:
                    rn = 1
                rate = gi('1박객실료')
                pkg_total = gi('PKG패키지총금액')
                sell_price = gi('판매가')
                commission = gi('수수료')
                
                prop_raw = g('변경사업장명') or g('영업장명')
                prop_clean = re.sub(r'^\d+\.\s*', '', prop_raw).strip()
                
                sell_date = g('판매일자')
                checkin = g('입실일자')
                checkout = g('퇴실일자')
                agent = g('AGENT명')
                code_num = g('변경예약집계코드')
                code_name = g('변경예약집계코드명')
                
                # 사업장 정규화
                prop_norm = normalize_prop_from_code_name(mem_name) or normalize_prop_from_code_name(prop_clean)
                if not prop_norm:
                    for k, v in PROP_NORMALIZE.items():
                        if k in prop_clean:
                            prop_norm = v
                            break
                if not prop_norm:
                    prop_norm = prop_clean
                
                # 인플루언서명 추출
                inf_name = extract_influencer_from_code_name(mem_name)
                
                # 객실매출 (VAT 제외)
                room_rev = int(rate * rn / 1.1)
                total_rev = int((pkg_total if pkg_total > 0 else (sell_price if sell_price > 0 else rate)) / 1.1)
                
                all_records.append({
                    'year': yr,
                    'pkg_code': mem_code,
                    'pkg_name': mem_name,
                    'prop': prop_norm,
                    'prop_raw': prop_clean,
                    'influencer': inf_name,
                    'sell_date': sell_date,
                    'checkin': checkin,
                    'checkout': checkout,
                    'agent': agent,
                    'code_num': code_num,
                    'rn': rn,
                    'room_rev': room_rev,
                    'total_rev': total_rev,
                    'rate': rate,
                    'pkg_total': pkg_total,
                    'commission': commission,
                })
    
    return all_records


def match_to_campaigns(records, campaigns):
    """레코드를 campaign_match_detail의 기획전에 매핑"""
    # campaign별 매핑 정보
    camp_map = []
    for c in campaigns:
        if c.get('channel_category') != '인플루언서':
            continue
        prop = normalize_prop_from_campaign(c.get('property', ''))
        inf = extract_influencer_from_channel(c.get('channel', ''))
        sale_start = c.get('sale_start', '')
        sale_end = c.get('sale_end', '')
        stay_start = c.get('stay_start', '')
        stay_end = c.get('stay_end', '')
        camp_map.append({
            'key': c['key'],
            'channel': c['channel'],
            'prop': prop,
            'influencer': inf,
            'sale_start': sale_start.replace('-', ''),
            'sale_end': sale_end.replace('-', ''),
            'stay_start': stay_start.replace('-', ''),
            'stay_end': stay_end.replace('-', ''),
            'raw': c,
        })
    
    # 매핑 결과
    matched = defaultdict(list)  # key -> [records]
    unmatched = []
    
    # 86코드별 그룹핑
    by_code = defaultdict(list)
    for r in records:
        by_code[r['pkg_code']].append(r)
    
    # 코드별로 캠페인 매칭 시도
    code_to_key = {}  # code -> campaign_key
    
    for code, code_records in by_code.items():
        r0 = code_records[0]
        prop = r0['prop']
        inf = r0['influencer']
        
        # 투숙기간 범위 파악
        checkins = [r['checkin'] for r in code_records if r['checkin']]
        min_checkin = min(checkins) if checkins else ''
        max_checkin = max(checkins) if checkins else ''
        
        best_match = None
        best_score = -1
        
        for cm in camp_map:
            score = 0
            
            # 사업장 매칭 (필수)
            if cm['prop'] != prop:
                continue
            score += 10
            
            # 인플루언서명 매칭
            if inf and cm['influencer'] and inf == cm['influencer']:
                score += 20
            elif inf and cm['influencer'] and inf != cm['influencer']:
                continue  # 다른 인플루언서면 매칭 안함
            
            # 투숙기간 겹침 체크
            if min_checkin and cm['stay_start'] and cm['stay_end']:
                if min_checkin >= cm['stay_start'] and min_checkin <= cm['stay_end']:
                    score += 5
                elif max_checkin and max_checkin >= cm['stay_start']:
                    score += 3
            
            # 판매기간 근접성
            sell_dates = [r['sell_date'] for r in code_records if r['sell_date']]
            if sell_dates and cm['sale_start']:
                min_sell = min(sell_dates)
                # 판매일이 판매시작 ±2개월 이내
                if abs(int(min_sell[:6]) - int(cm['sale_start'][:6])) <= 2:
                    score += 3
            
            if score > best_score:
                best_score = score
                best_match = cm
        
        if best_match and best_score >= 10:
            code_to_key[code] = best_match['key']
            matched[best_match['key']].extend(code_records)
        else:
            unmatched.extend(code_records)
    
    return matched, unmatched, code_to_key, camp_map


def aggregate_by_campaign(matched, camp_map):
    """캠페인별 KPI 집계"""
    camp_lookup = {cm['key']: cm for cm in camp_map}
    results = {}
    
    for key, records in matched.items():
        cm = camp_lookup.get(key, {})
        
        total_rn = sum(r['rn'] for r in records)
        total_room_rev = sum(r['room_rev'] for r in records)
        total_rev = sum(r['total_rev'] for r in records)
        total_commission = sum(r['commission'] for r in records)
        
        # 고유 예약건수 (KEY_RSV_NO 기반이 아니므로 86코드 수로 근사)
        pkg_codes = list(set(r['pkg_code'] for r in records))
        
        adr = round(total_room_rev / total_rn) if total_rn > 0 else 0
        
        # 월별 집계
        by_month = defaultdict(lambda: {'rn': 0, 'room_rev': 0, 'total_rev': 0})
        for r in records:
            ym = r['checkin'][:6] if r['checkin'] else r['sell_date'][:6]
            if ym:
                by_month[ym]['rn'] += r['rn']
                by_month[ym]['room_rev'] += r['room_rev']
                by_month[ym]['total_rev'] += r['total_rev']
        
        results[key] = {
            'campaign_key': key,
            'channel': cm.get('channel', ''),
            'property': cm.get('prop', ''),
            'influencer': cm.get('influencer', ''),
            'sale_start': cm.get('raw', {}).get('sale_start', ''),
            'sale_end': cm.get('raw', {}).get('sale_end', ''),
            'stay_start': cm.get('raw', {}).get('stay_start', ''),
            'stay_end': cm.get('raw', {}).get('stay_end', ''),
            'product': cm.get('raw', {}).get('product', ''),
            'rn': total_rn,
            'room_rev_m': round(total_room_rev / 1e6, 2),
            'total_rev_m': round(total_rev / 1e6, 2),
            'adr': adr,
            'adr_k': round(adr / 1000),
            'commission_m': round(total_commission / 1e6, 2),
            'pkg_codes': sorted(pkg_codes),
            'pkg_code_count': len(pkg_codes),
            'record_count': len(records),
            'by_month': {k: dict(v) for k, v in sorted(by_month.items())},
        }
    
    return results


def build_output(campaign_results, unmatched_records, code_to_key, all_records):
    """최종 JSON 빌드"""
    
    # 미매칭 86코드 요약
    unmatched_codes = defaultdict(lambda: {'name': '', 'rn': 0, 'rev': 0, 'count': 0})
    for r in unmatched_records:
        b = unmatched_codes[r['pkg_code']]
        b['name'] = r['pkg_name']
        b['rn'] += r['rn']
        b['rev'] += r['room_rev']
        b['count'] += 1
    
    # 전체 합계
    total_rn = sum(v['rn'] for v in campaign_results.values())
    total_room_rev = sum(v['room_rev_m'] for v in campaign_results.values())
    total_rev = sum(v['total_rev_m'] for v in campaign_results.values())
    
    # 사업장별 합계
    by_prop = defaultdict(lambda: {'rn': 0, 'room_rev_m': 0, 'total_rev_m': 0, 'campaigns': 0})
    for v in campaign_results.values():
        p = v['property']
        by_prop[p]['rn'] += v['rn']
        by_prop[p]['room_rev_m'] += v['room_rev_m']
        by_prop[p]['total_rev_m'] += v['total_rev_m']
        by_prop[p]['campaigns'] += 1
    
    # 인플루언서별 합계
    by_influencer = defaultdict(lambda: {'rn': 0, 'room_rev_m': 0, 'total_rev_m': 0, 'campaigns': 0, 'properties': set()})
    for v in campaign_results.values():
        inf = v['influencer'] or '미식별'
        by_influencer[inf]['rn'] += v['rn']
        by_influencer[inf]['room_rev_m'] += v['room_rev_m']
        by_influencer[inf]['total_rev_m'] += v['total_rev_m']
        by_influencer[inf]['campaigns'] += 1
        by_influencer[inf]['properties'].add(v['property'])
    
    # set -> list 변환
    for v in by_influencer.values():
        v['properties'] = sorted(v['properties'])
        v['adr_k'] = round(v['room_rev_m'] * 1e6 / v['rn'] / 1000) if v['rn'] > 0 else 0
    
    unmatched_rn = sum(r['rn'] for r in unmatched_records)
    
    output = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'data_source': '27번 txt (넷 데이터, 취소 반영됨)',
        'years_parsed': ['2024', '2025', '2026'],
        'method': '86XX 패키지코드 → 회원명 키워드 매칭 → 사업장+인플루언서명+투숙기간 기반 캠페인 매핑',
        'summary': {
            'total_86_influencer_records': len(all_records),
            'matched_to_campaigns': len(all_records) - len(unmatched_records),
            'unmatched_records': len(unmatched_records),
            'campaigns_with_data': len(campaign_results),
            'total_rn': total_rn,
            'total_room_rev_m': round(total_room_rev, 2),
            'total_rev_m': round(total_rev, 2),
            'avg_adr_k': round(total_room_rev * 1e6 / total_rn / 1000) if total_rn > 0 else 0,
            'unmatched_rn': unmatched_rn,
        },
        'by_campaign': {k: v for k, v in sorted(campaign_results.items(), key=lambda x: -x[1]['rn'])},
        'by_property': {k: dict(v) for k, v in sorted(by_prop.items(), key=lambda x: -x[1]['rn'])},
        'by_influencer': {k: dict(v) for k, v in sorted(by_influencer.items(), key=lambda x: -x[1]['rn'])},
        'code_to_campaign': code_to_key,
        'unmatched_top_codes': sorted(
            [{'code': k, **v} for k, v in unmatched_codes.items()],
            key=lambda x: -x['rn']
        )[:30],
    }
    
    return output


def main():
    print("=" * 60)
    print("인플루언서 86XX 패키지코드 매핑 및 실적 집계")
    print("=" * 60)
    
    # 1. 27번 파싱
    print("\n[1] 27번 txt 파싱...")
    all_records = parse_27_files()
    print(f"  → 인플루언서 86XX 레코드: {len(all_records):,}건")
    
    if not all_records:
        print("인플루언서 86XX 레코드 없음. 종료.")
        return
    
    # 2. campaign_match_detail 로드
    print("\n[2] campaign_match_detail.json 로드...")
    with open(PROJECT_DIR / 'data' / 'campaign_match_detail.json') as f:
        campaigns = json.load(f)
    inf_camps = [c for c in campaigns if c.get('channel_category') == '인플루언서']
    print(f"  → 인플루언서 기획전: {len(inf_camps)}건")
    
    # 3. 매핑
    print("\n[3] 86코드 → 기획전 매핑...")
    matched, unmatched, code_to_key, camp_map = match_to_campaigns(all_records, campaigns)
    print(f"  → 매칭된 기획전: {len(matched)}건")
    print(f"  → 매칭 레코드: {sum(len(v) for v in matched.values()):,}건")
    print(f"  → 미매칭 레코드: {len(unmatched):,}건")
    
    # 4. 집계
    print("\n[4] 캠페인별 집계...")
    campaign_results = aggregate_by_campaign(matched, camp_map)
    
    # 5. 출력
    output = build_output(campaign_results, unmatched, code_to_key, all_records)
    
    # 저장
    DOCS_DATA.mkdir(parents=True, exist_ok=True)
    out_path = DOCS_DATA / 'influencer_86code_performance.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n✓ 저장: {out_path}")
    
    # 결과 요약
    s = output['summary']
    print(f"\n{'─'*40}")
    print(f"총 RN: {s['total_rn']:,}")
    print(f"총 객실매출: {s['total_room_rev_m']:,.1f}백만원")
    print(f"총 매출: {s['total_rev_m']:,.1f}백만원")
    print(f"평균 ADR: {s['avg_adr_k']}천원")
    
    print(f"\n사업장별:")
    for p, v in output['by_property'].items():
        print(f"  {p}: RN {v['rn']:,} | 객실 {v['room_rev_m']:,.1f}M | {v['campaigns']}건")
    
    print(f"\n인플루언서별 TOP10:")
    for i, (inf, v) in enumerate(output['by_influencer'].items()):
        if i >= 10: break
        print(f"  {inf}: RN {v['rn']:,} | 객실 {v['room_rev_m']:,.1f}M | ADR {v['adr_k']}천 | {v['campaigns']}건 | {','.join(v['properties'])}")
    
    # 미매칭 TOP5
    print(f"\n미매칭 TOP5:")
    for item in output['unmatched_top_codes'][:5]:
        print(f"  {item['code']} | {item['name'][:50]} | RN={item['rn']} rev={item['rev']:,}")
    
    # campaign_individual_analysis.json 업데이트
    print(f"\n[5] campaign_individual_analysis.json 업데이트...")
    update_individual_analysis(output, campaign_results)


def update_individual_analysis(output, campaign_results):
    """campaign_individual_analysis.json의 인플루언서 데이터 업데이트"""
    analysis_path = DOCS_DATA / 'campaign_individual_analysis.json'
    with open(analysis_path) as f:
        analysis = json.load(f)
    
    # campaign_match_detail의 key -> individual_analysis의 campaign_id 매핑
    # campaign_match_detail.json key와 individual_analysis campaign_id가 다를 수 있음
    # individual_analysis에서 인플루언서 항목의 channel 명으로 매칭
    
    ia = analysis.get('individual_analysis', [])
    updated_count = 0
    
    for item in ia:
        ch = item.get('channel', '')
        prop = item.get('property', '')
        
        if '인플' not in ch and '유튜버' not in ch:
            continue
        
        # campaign_results에서 매칭
        best_key = None
        best_rn = 0
        
        for key, cr in campaign_results.items():
            # 같은 사업장
            cr_prop = cr.get('property', '')
            if cr_prop != prop and prop not in cr_prop and cr_prop not in prop:
                continue
            
            # 채널명 유사도
            cr_ch = cr.get('channel', '')
            # 인플루언서명 매칭
            cr_inf = cr.get('influencer', '')
            item_inf = extract_influencer_from_channel(ch)
            
            if cr_inf and item_inf and cr_inf == item_inf:
                # 기간도 체크
                if cr['rn'] > best_rn:
                    best_rn = cr['rn']
                    best_key = key
            elif cr_ch and ch and (cr_ch in ch or ch in cr_ch):
                if cr['rn'] > best_rn:
                    best_rn = cr['rn']
                    best_key = key
        
        if best_key and best_key in campaign_results:
            cr = campaign_results[best_key]
            # 기존 influencer_data 업데이트 또는 생성
            item['influencer_86code_data'] = {
                'source': '86XX 패키지코드 매핑',
                'campaign_key': best_key,
                'rn': cr['rn'],
                'room_rev_m': cr['room_rev_m'],
                'total_rev_m': cr['total_rev_m'],
                'adr_k': cr['adr_k'],
                'pkg_codes': cr['pkg_codes'],
                'pkg_code_count': cr['pkg_code_count'],
                'by_month': cr['by_month'],
            }
            updated_count += 1
    
    # 메타 업데이트
    analysis['meta']['86code_mapping'] = {
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'method': '86XX 패키지코드 직접 매핑 (27번 넷 데이터)',
        'campaigns_updated': updated_count,
        'total_mapped_rn': output['summary']['total_rn'],
    }
    
    # summary 업데이트
    analysis['summary']['influencer_86code_rns'] = output['summary']['total_rn']
    analysis['summary']['influencer_86code_rev_m'] = output['summary']['total_rev_m']
    
    with open(analysis_path, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, ensure_ascii=False, indent=2)
    print(f"  → {updated_count}건 업데이트 완료")


if __name__ == '__main__':
    main()
