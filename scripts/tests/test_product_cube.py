import sys, os, json, unittest, tempfile, shutil, gzip
from pathlib import Path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import build_product_cube as bpc
import product_naming as pn

HEADER = ['KEY_RSV_NO', '판매일자', '영업장', '영업장명', '변경사업장명', '회원번호', '회원명', '이용자명', '객실타입',
          '평형', '동', '타입', '변경예약집계코드', '변경예약집계코드명', '원예약집계코드', '원예약집계코드명', '고객유형',
          '단체구분', '예약구분', '예약구분명', '사용구분', '사용구분명', '시즌코드', '시즌코드명', '요금시즌코드', '요금시즌코드명',
          '1박객실료', '박수', '객실수', '최초입력일시', '최초입력일자', '투숙여부', '예약상태', '입실일자', '퇴실일자', '구입명',
          '대표번호별구좌수', '직원원소속', '사업장회사명', 'AGENT코드', 'AGENT명', 'PKG객실', 'PKG골프', 'PKG기타', 'PKG영업장',
          'PKG영업장식음', 'PKG영업장부대', 'PKG영업장스포츠', 'PKG영업장아쿠아', 'PKG영업장기타', 'PKG패키지총금액', '판매가',
          '입금가', '수수료', '선결제여부', '패키지분류코드', '패키지분류코드명']


def row(**kw):
    base = {h: '' for h in HEADER}
    base.update({'영업장명': '소노벨 비발디파크 B · C', '변경사업장명': '02. 소노벨 비발디파크', '회원번호': '86150059',
                 '회원명': '[벨비발디] 온(溫)가을 PKG_OTA', '객실타입': '00HOK101', '변경예약집계코드': '72',
                 '요금시즌코드': '41', '1박객실료': '110000', '박수': '1', '객실수': '1', 'AGENT명': 'OTA_놀유니버스(야놀자)',
                 'PKG객실': '110000', 'PKG영업장부대': '55000', 'PKG패키지총금액': '165000', '판매가': '110000',
                 '입금가': '98000', '수수료': '12000', '패키지분류코드명': '시즌'})
    base.update(kw)
    return ';'.join(base[h] for h in HEADER)


def write_raw(dirpath, name, lines):
    p = Path(dirpath) / name
    with open(p, 'w', encoding='cp949', newline='') as f:
        f.write(';'.join(HEADER) + '\n')
        for ln in lines:
            f.write(ln + '\n')
    return p


class SelectFiles(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_retrans_only_year_has_no_month_filter(self):
        y = Path(self.tmp) / '2025'; y.mkdir()
        write_raw(y, '27.온라인영업팀 예약자료_재전송(20250101-20251231).txt', [])
        write_raw(y, '28.온라인영업팀 취소자료_재전송(20250101-20250331).txt', [])
        sel = bpc.select_files(Path(self.tmp), [2025])
        self.assertEqual(len(sel), 1)
        self.assertTrue(sel[0][0].name.startswith('27.'))
        self.assertEqual((sel[0][1], sel[0][2]), (None, None))

    def test_retrans_plus_snapshot_split_by_snapshot_min_month(self):
        y = Path(self.tmp) / '2026'; y.mkdir()
        write_raw(y, '27.온라인영업팀 예약자료_재전송(20260101-20261030).txt', [])
        snap_rows = [row(KEY_RSV_NO=str(i), 판매일자='20261105', 최초입력일자='20261001') for i in range(600)]
        snap_rows += [row(KEY_RSV_NO='x%d' % i, 판매일자='20261012', 최초입력일자='20261001') for i in range(10)]  # 잡음월(<500행)
        write_raw(y, '27.온라인영업팀 예약자료_20261125_생성시간(202611250750).txt', snap_rows)
        write_raw(y, '27.온라인영업팀 예약자료_20261120_생성시간(202611200750).txt', snap_rows)  # 구 스냅샷 → 제외
        sel = {p.name: (a, b) for p, a, b in bpc.select_files(Path(self.tmp), [2026])}
        self.assertEqual(len(sel), 2)
        self.assertEqual(sel['27.온라인영업팀 예약자료_재전송(20260101-20261030).txt'], (None, '202610'))
        self.assertEqual(sel['27.온라인영업팀 예약자료_20261125_생성시간(202611250750).txt'], ('202611', None))


class ParseAndAggregate(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_rows_measures_and_booking_dedup(self):
        lines = [
            row(KEY_RSV_NO='R1', 판매일자='20251010', 최초입력일자='20251001', 박수='2', PKG객실='220000', PKG패키지총금액='330000', PKG영업장부대='110000'),
            row(KEY_RSV_NO='R1', 판매일자='20251011', 최초입력일자='20251001', 박수='2', PKG객실='220000', PKG패키지총금액='330000', PKG영업장부대='110000', 요금시즌코드='43'),
            row(KEY_RSV_NO='R2', 판매일자='20251012', 최초입력일자='20251011', 객실수='2', 변경예약집계코드='A5', AGENT명='GOTA_아고다'),
            row(KEY_RSV_NO='R3', 판매일자='20251012', 최초입력일자='20251011', 회원번호='12345678'),       # 86 아님 → 베이스라인에만
            row(KEY_RSV_NO='R4', 판매일자='20251013', 최초입력일자='20251011', 변경사업장명='25. 팔라티움 해운대'),  # 미매핑 사업장 → 제외
        ]
        p = write_raw(self.tmp, '27.x_재전송(20250101-20251231).txt', lines)
        rows = list(bpc.iter_rows(p, None, None))
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[0]['stay'], '20251010'); self.assertEqual(rows[0]['rate'], 110000.0); self.assertEqual(rows[0]['nights'], 2)
        self.assertEqual(rows[0]['pkg_parts'], {'부대': 110000.0})

        b = bpc.CubeBuilder()
        for r in rows:
            b.add(r)
        c = b.codes['86150059']
        tot_rn = sum(v[0] for ym in c['by_ym'].values() for v in ym['s'].values())
        tot_rev = sum(v[1] for ym in c['by_ym'].values() for v in ym['s'].values())
        self.assertEqual(tot_rn, 4)
        self.assertAlmostEqual(tot_rev, 440000 / 1.1, places=2)
        tot_comp = sum(v[2] for ym in c['by_ym'].values() for v in ym['s'].values())
        self.assertAlmostEqual(tot_comp, (110000 + 55000) / 1.1, places=2)
        s = c['by_ym']['202510']['s']
        self.assertEqual(s['41'][0], 3); self.assertEqual(s['43'][0], 1)
        self.assertEqual(sum(v[4] for v in s.values()), 2)
        self.assertEqual(sorted(c['by_ym']['202510']['lead_list']), [1, 9])
        self.assertEqual(c['by_ym']['202510']['dow'], [2, 0, 0, 0, 0, 1, 1])
        self.assertEqual(dict(c['seg_counts']), {'OTA': 2, 'G-OTA': 1})
        self.assertEqual(c['ag']['놀유니버스(야놀자)'][0], 2); self.assertEqual(c['ag']['아고다'][0], 2)
        # 41: R1 1행(1실) + R2(2실) + R3(비86, 1실) = 4 / 43: R1 2행째 1실. 팔라티움(R4) 제외
        self.assertEqual(b.baseline['01.벨비발디']['202510']['41'][0], 4)
        self.assertEqual(b.baseline['01.벨비발디']['202510']['43'][0], 1)
        self.assertNotIn('25.팔라티움', b.baseline)

    def test_channel_month_aggregation(self):
        lines = [row(KEY_RSV_NO='A', 판매일자='20260110', 최초입력일자='20251220', 회원번호='86100001', AGENT명='GOTA_아고다', 변경예약집계코드='A5'),
                 row(KEY_RSV_NO='B', 판매일자='20260111', 최초입력일자='20260105', 회원번호='86100001', AGENT명='GOTA_아고다', 변경예약집계코드='A5', 객실수='2'),
                 row(KEY_RSV_NO='C', 판매일자='20260112', 최초입력일자='20260105', 회원번호='86100002', AGENT명='GOTA_아고다', 변경예약집계코드='A5'),
                 row(KEY_RSV_NO='D', 판매일자='20260112', 최초입력일자='20260105', 회원번호='86100001', AGENT명='OTA_쿠팡', 변경예약집계코드='72')]
        p = write_raw(self.tmp, '27.x_재전송(20260101-20261231).txt', lines)
        b = bpc.CubeBuilder()
        for r in bpc.iter_rows(p, None, None):
            b.add(r)
        cm = bpc.build_channel_month(b)
        stay = cm['stay']['아고다']['202601']
        self.assertEqual([x[0] for x in stay], ['86100001', '86100002'])      # RN 내림차순 (3 vs 1)
        self.assertEqual(stay[0][1], 3); self.assertEqual(stay[0][2], round(330000 / 1.1))
        self.assertEqual(cm['book']['아고다']['202512'], [['86100001', 1, round(110000 / 1.1)]])   # 예약월 12월 1건
        self.assertEqual(cm['book']['아고다']['202601'][0][1] + cm['book']['아고다']['202601'][1][1], 3)
        self.assertEqual(cm['stay']['쿠팡']['202601'], [['86100001', 1, round(110000 / 1.1)]])

    def test_month_filter_applied(self):
        lines = [row(KEY_RSV_NO='A', 판매일자='20261001'), row(KEY_RSV_NO='B', 판매일자='20261101')]
        p = write_raw(self.tmp, '27.x_20261125_생성시간(1).txt', lines)
        self.assertEqual([r['stay'] for r in bpc.iter_rows(p, '202611', None)], ['20261101'])
        self.assertEqual([r['stay'] for r in bpc.iter_rows(p, None, '202610')], ['20261001'])


class Finalize(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_finalize_schema_campaign_and_windows(self):
        lines = [row(KEY_RSV_NO='R1', 판매일자='20251010', 최초입력일자='20250925'),
                 row(KEY_RSV_NO='R2', 판매일자='20251110', 최초입력일자='20251030', 요금시즌코드='43'),
                 row(KEY_RSV_NO='R3', 판매일자='20241010', 최초입력일자='20240925', 회원번호='86000001',
                     회원명='(델) 온가을패키지_G15', 변경사업장명='08. 델피노', 변경예약집계코드='A5', AGENT명='GOTA_아고다')]
        p = write_raw(self.tmp, '27.x_재전송(20240101-20251231).txt', lines)
        b = bpc.CubeBuilder()
        for r in bpc.iter_rows(p, None, None):
            b.add(r)
        cmap_path = Path(self.tmp) / 'map.json'
        cmap_path.write_text(json.dumps({"campaigns": [{"name": "2025 가을프로모션 온(溫)PKG", "year": 2025,
            "match": {"codes": ["86150059"], "regex": "온가을"}, "composition": "객실+사우나2인", "sale": "25.09.24~11.30"}]}), encoding='utf-8')
        cube = bpc.finalize(b, pn.load_campaign_map(str(cmap_path)), [2024, 2025], '2026-11-25')
        self.assertEqual(cube['meta']['years'], [2024, 2025])
        c = cube['codes']['86150059']
        self.assertEqual(c['campaign'], '2025 가을프로모션 온(溫)PKG')
        self.assertEqual(c['prop'], '01.벨비발디'); self.assertEqual(c['seg'], 'OTA')
        self.assertEqual(c['category'], '시즌패키지')
        self.assertEqual(c['by_ym']['202510']['s']['41'][0], 1)
        self.assertEqual(c['by_ym']['202510']['lead']['p50'], 15)
        self.assertNotIn('lead_list', c['by_ym']['202510'])
        self.assertEqual(cube['codes']['86000001']['campaign'], '온가을PKG (2024)')
        camp = cube['campaigns']['2025 가을프로모션 온(溫)PKG']
        self.assertEqual(camp['codes'], ['86150059']); self.assertEqual(camp['props'], ['01.벨비발디'])
        self.assertEqual(camp['by_year']['2025']['rn'], 2)
        self.assertEqual(camp['sale_window'], {'from': '20250925', 'to': '20251030'})
        self.assertEqual(camp['stay_window'], {'from': '20251010', 'to': '20251110'})
        self.assertEqual(camp['composition']['text'], '객실+사우나2인')
        self.assertIn('부대', camp['composition']['auto'])
        self.assertEqual(cube['baseline']['01.벨비발디']['202510']['41']['rn'], 1)
        self.assertEqual(cube['dims']['properties'][0]['display'], '01.벨비발디')
        self.assertIn('소노벨 비발디파크', cube['dims']['prop_alias'])

    def test_action_plan_map_and_precedence(self):
        ap_path = Path(self.tmp) / 'ap.json'
        ap_path.write_text(json.dumps({"rows": [
            {"row": 29, "codes": ["86000010", "86000011"], "시작일": "2026-02-23", "종료일": "2026-02-25", "채널": "OTA",
             "사업장": "거제", "인플루언서": "", "상품": "① 2+1 모닝 인클루시브 : 객실 + 조식 + 오션\n② 2박"},
            {"row": 30, "codes": ["86000011", "86000012"], "시작일": "2026-03-01", "종료일": "2026-03-03", "채널": "GOTA",
             "사업장": "진도", "인플루언서": "@요미맘", "상품": "객실+오션"},
            {"row": 31, "codes": [], "시작일": "2026-03-05", "종료일": "", "채널": "OTA", "사업장": "천안", "상품": "협의중"},
        ]}), encoding='utf-8')
        ap = bpc.load_action_plan_map(str(ap_path))
        self.assertEqual(len(ap['meta']), 2)                      # 코드 없는 행 제외
        n29 = ap['by_code']['86000010']
        self.assertTrue(n29.startswith('AP29 거제 ① 2+1 모닝 인클루시브'), n29)
        self.assertEqual(ap['by_code']['86000011'], n29)          # 공유 코드 → 시작일 빠른 행
        self.assertTrue(ap['by_code']['86000012'].startswith('AP30 진도'))
        self.assertEqual(ap['meta'][n29]['sale'], '26.02.23~26.02.25')
        self.assertEqual(ap['meta'][n29]['source'], 'action_plan'); self.assertEqual(ap['meta'][n29]['year'], 2026)
        self.assertIn('인플루언서 @요미맘', ap['meta'][ap['by_code']['86000012']]['note'])
        manual_path = Path(self.tmp) / 'map.json'
        manual_path.write_text(json.dumps({"campaigns": [{"name": "수기 기획전", "year": 2026, "match": {"codes": ["86000012"]}}]}), encoding='utf-8')
        merged = bpc.merge_campaign_maps(pn.load_campaign_map(str(manual_path)), ap)
        self.assertEqual(merged['by_code']['86000012'], '수기 기획전')   # 수기 맵 우선
        self.assertEqual(merged['by_code']['86000010'], n29)
        self.assertEqual(merged['meta']['수기 기획전']['source'], 'map')
        self.assertEqual(bpc.load_action_plan_map(str(Path(self.tmp) / 'none.json')), {'by_code': {}, 'meta': {}})

    def test_match_sheet_events_scoring(self):
        lines = [row(KEY_RSV_NO='A1', 판매일자='20260310', 최초입력일자='20260305', 회원번호='86100001', 회원명='[벨비발디] 스프링 올인_쿠팡', 변경예약집계코드='72'),
                 row(KEY_RSV_NO='A2', 판매일자='20260312', 최초입력일자='20260306', 회원번호='86100002', 회원명='[벨비발디] 스프링 올인_G15', 변경예약집계코드='A5', AGENT명='GOTA_아고다'),
                 row(KEY_RSV_NO='A3', 판매일자='20260320', 최초입력일자='20260304', 회원번호='86100003', 회원명='[벨비발디] 조식패키지_OTA', 변경예약집계코드='72'),
                 row(KEY_RSV_NO='A4', 판매일자='20260501', 최초입력일자='20260420', 회원번호='86100004', 회원명='[벨비발디] 썸머 쿠팡딜', 변경예약집계코드='72')]
        p = write_raw(self.tmp, '27.x_재전송(20260101-20261231).txt', lines)
        b = bpc.CubeBuilder()
        for r in bpc.iter_rows(p, None, None):
            b.add(r)
        events = [{'key': '4', '구분': '비발디파크', '사업장': '비발디', '영업장': '벨', '채널': '쿠팡', '채널카테고리': '쿠팡', '판매시작': '2026-03-04', '판매종료': '2026-03-10',
                   '투숙시작': '2026-03-04', '투숙종료': '2026-05-31', '상품': '객실+올인클루시브(오션+액티비티)', 'KPI_RN': '300'},
                  {'key': '9', '사업장': '진도', '영업장': '진도', '채널': '티딜', '채널카테고리': '티딜/톡딜', '판매시작': '2026-03-04', '판매종료': '2026-03-10', '상품': '협의중'}]
        import sheet_event_matcher as sem
        by_code, meta, recs = sem.match_events(b.codes, events, taken={'86100002'})
        self.assertIn('86100001', by_code)                       # 쿠팡 채널명+3·AGENT(쿠팡 없음)·구성(아쿠아=오션 ✓ 부대 기대? '액티비티'→스포츠 ✗)·OTA 세그+1·판매일±1 +2
        self.assertNotIn('86100002', by_code)                    # taken(AP 코드) 제외
        self.assertNotIn('86100004', by_code)                    # 판매기간 밖(4/20)
        self.assertNotIn('86100003', by_code)                    # 조식: 채널·구성 불일치 → 점수 부족
        name = by_code['86100001']
        self.assertTrue(name.startswith('SE4 비발디 쿠팡'), name)
        self.assertEqual(meta[name]['source'], 'sheet_event'); self.assertIn(meta[name]['conf'], ('high', 'medium'))
        self.assertEqual(recs[0]['codes'], ['86100001']); self.assertIn('채널명', recs[0]['why']); self.assertEqual(recs[0]['pass'], 1)
        self.assertEqual(recs[1]['conf'], 'none'); self.assertEqual(recs[1]['props'], ['17.쏠비치진도'])
        merged = bpc.merge_campaign_maps(pn.load_campaign_map('/nonexistent.json'), {'by_code': {'86100001': 'AP행'}, 'meta': {'AP행': {}}}, {'by_code': by_code, 'meta': meta})
        self.assertEqual(merged['by_code']['86100001'], 'AP행')   # AP > 시트 추정
        # AP 행 연결(확정): 같은 사업장·시작일·인플루언서명 → 코드 확정, 추정 안 함
        ap_rows = [{'row': 40, 'codes': ['86100002'], '시작일': '2026-03-04', '종료일': '2026-03-08', '채널': 'GOTA', '사업장': '비발디', '인플루언서': '@제이드아이', '상품': '객실+올인'}]
        ev2 = [{'key': '7', '사업장': '비발디', '영업장': '벨', '채널': '인플루언서x트립비토즈(제이드아이)', '채널카테고리': '인플루언서', '판매시작': '2026-03-04', '판매종료': '2026-03-08', '상품': '올인클루시브'}]
        bc2, m2, r2 = sem.match_events(b.codes, ev2, taken={'86100002'}, ap_rows=ap_rows, ap_by_code={'86100002': 'AP40 비발디 객실+올인 (GOTA · @제이드아이, 26.03)'})
        self.assertEqual(r2[0]['conf'], 'ap'); self.assertEqual(r2[0]['codes'], ['86100002']); self.assertEqual(r2[0]['ap_row'], 40)
        self.assertTrue(r2[0]['campaign'].startswith('AP40')); self.assertEqual(bc2, {})   # 확정 연결은 새 캠페인 생성 없음
        # 타채널 페널티: 11번가 이벤트에 '_쿠팡' 코드는 −3
        ev3 = [{'key': '8', '사업장': '비발디', '영업장': '벨', '채널': '11번가(스마트인피니)', '채널카테고리': '11번가', '판매시작': '2026-03-04', '판매종료': '2026-03-05', '상품': '객실+올인클루시브(오션+액티비티)'}]
        bc3, m3, r3 = sem.match_events(b.codes, ev3, taken=set())
        self.assertNotIn('86100001', r3[0]['codes'])   # 쿠팡 코드가 11번가 이벤트로 가지 않음
        # '협의중' 행은 채널 증거 없이는 매칭 안 함
        ev4 = [{'key': '9', '사업장': '비발디', '영업장': '벨', '채널': '맘맘', '채널카테고리': '맘맘', '판매시작': '2026-03-04', '판매종료': '2026-03-05', '상품': '협의중'}]
        bc4, m4, r4 = sem.match_events(b.codes, ev4, taken=set())
        self.assertEqual(r4[0]['conf'], 'none')

    def test_event_props(self):
        import sheet_event_matcher as sem
        self.assertEqual(sem.event_props('비발디', '벨/캄'), {'01.벨비발디', '02.캄비발디'})
        self.assertEqual(sem.event_props('비발디', ''), {'01.벨비발디', '02.캄비발디', '03.펫비발디', '04.펠리체비발디', '05.빌리지비발디'})
        self.assertEqual(sem.event_props('양양/삼척/르네', ''), {'08.쏠비치양양', '09.쏠비치삼척', '23.르네블루'})
        self.assertEqual(sem.event_props('캄제주', ''), {'19.소노캄제주'})
        self.assertEqual(sem.event_props('벨,캄 제주', ''), {'18.소노벨제주', '19.소노캄제주'})
        self.assertEqual(sem.event_props('비발디', '펫'), {'03.펫비발디'})
        self.assertEqual(len(sem.event_props('전체', '')), 23)

    def test_score_pair_signals(self):
        import sheet_event_matcher as sem
        from datetime import datetime
        class FakeCI:
            code = '86'; prop = '01.벨비발디'; name = '[벨비발디] 스프링 올인(오션+조식)_쿠팡'; name_l = name.lower(); seg = 'OTA'; chan = '쿠팡'
            fs = datetime(2026, 3, 4); stay_lo = datetime(2026, 3, 10); stay_hi = datetime(2026, 5, 20); rn = 100; pkg_price = 150000
            parts = {'아쿠아': 0.6, '식음': 0.4}; parts_set = {'아쿠아', '식음'}
            sale_days = {datetime(2026, 3, 4): 60, datetime(2026, 3, 6): 30, datetime(2026, 4, 20): 10}; sale_span = 47
            def sale_concentration(self, s0, s1): return sum(n for d, n in self.sale_days.items() if s0 <= d <= s1) / 100
        ev = {'채널카테고리': '쿠팡', '채널': '쿠팡', '상품': '객실+조식+오션 149,000원', '판매시작': '2026-03-04', '판매종료': '2026-03-10', '투숙시작': '2026-03-04', '투숙종료': '2026-05-31'}
        sc, why = sem.score_pair(ev, FakeCI(), {}, {})
        self.assertGreaterEqual(sc, 13)   # 채널명3+AGENT2+구성4+판매일2+판매집중2+투숙1+가격2+세그1
        for w in ('채널명', 'AGENT', '구성2/2', '판매일±1', '판매집중70%', '투숙겹침', '가격±15%', '세그'):
            self.assertIn(w, why)
        ev11 = dict(ev, **{'채널카테고리': '11번가', '채널': '11번가(스마트인피니)'})
        sc2, why2 = sem.score_pair(ev11, FakeCI(), {}, {})
        self.assertIn('타채널(쿠팡)', why2); self.assertLess(sc2, sc - 5)

    def test_write_outputs_json_and_gz(self):
        out = Path(self.tmp) / 'product_cube.json'
        n1, n2 = bpc.write_outputs({'meta': {'x': 1}, 'codes': {}}, out)
        self.assertTrue(out.exists()); self.assertTrue(out.with_suffix('.json.gz').exists())
        self.assertGreater(n1, 0); self.assertGreater(n2, 0)
        self.assertEqual(json.loads(gzip.open(out.with_suffix('.json.gz')).read())['meta']['x'], 1)


if __name__ == '__main__':
    unittest.main()
