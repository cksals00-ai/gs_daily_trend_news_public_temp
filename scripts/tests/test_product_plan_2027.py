import sys, os, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import build_product_plan_2027 as bp


def mini_cube():
    """캠페인 1개(2026, 델피노, 코드 2개) + 베이스라인."""
    def ym_node(rn41, rn42, adr):
        return {'s': {'41': [rn41, rn41 * adr, rn41 * 20000, rn41 * adr * 0.11, rn41],
                      '42': [rn42, rn42 * adr * 1.3, rn42 * 20000, rn42 * adr * 1.3 * 0.11, rn42]},
                'dow': [0] * 7, 'lead': {'p50': 5, 'p90': 20, 'mean': 7}}
    return {
        'meta': {'built': 'x', 'base_date': '2026-11-25'},
        'dims': {'properties': [{'display': '07.델피노', 'region': 'central'}], 'prop_alias': {'델피노': '07.델피노'}},
        'codes': {
            '86000001': {'name': '(델) 온가을PKG_OTA', 'series': '온가을PKG', 'campaign': '2026 온가을PKG', 'category': '시즌패키지',
                         'prop': '07.델피노', 'seg': 'OTA', 'chan': '야놀자', 'by_ym': {'202610': ym_node(80, 40, 200000), '202611': ym_node(60, 20, 200000)}},
            '86000002': {'name': '(델) 온가을PKG_G15', 'series': '온가을PKG', 'campaign': '2026 온가을PKG', 'category': '시즌패키지',
                         'prop': '07.델피노', 'seg': 'G-OTA', 'chan': '아고다', 'by_ym': {'202610': ym_node(30, 10, 220000)}},
            '86000003': {'name': '(델) 온가을PKG_회원', 'series': '온가을PKG', 'campaign': '2026 온가을PKG', 'category': '시즌패키지',
                         'prop': '07.델피노', 'seg': '회원', 'chan': '회원', 'by_ym': {'202610': ym_node(500, 0, 100000)}},  # GS 세그 아님 → 제외
        },
        'campaigns': {'2026 온가을PKG': {'codes': ['86000001', '86000002', '86000003'], 'props': ['07.델피노'], 'category': '시즌패키지',
                                      'year': 2026, 'composition': {'text': '객실+사우나2인', 'auto': {}},
                                      'sale_window': {'from': '20260924', 'to': '20261130'}, 'stay_window': {'from': '20261001', 'to': '20261130'},
                                      'sale_text': '26.09.24~11.30', 'note': '', 'source': 'map', 'by_year': {'2026': {'rn': 740}}}},
        'baseline': {'07.델피노': {
            '202510': {'41': {'rn': 1000, 'rev': 190000000, 'adr': 190000}, '42': {'rn': 400, 'rev': 120000000, 'adr': 300000}},
            '202610': {'41': {'rn': 1100, 'rev': 220000000, 'adr': 200000}, '42': {'rn': 300, 'rev': 90000000, 'adr': 300000}},   # 주말 −25%
            '202511': {'41': {'rn': 900, 'rev': 171000000, 'adr': 190000}, '42': {'rn': 380, 'rev': 114000000, 'adr': 300000}},
            '202611': {'41': {'rn': 950, 'rev': 190000000, 'adr': 200000}, '42': {'rn': 390, 'rev': 117000000, 'adr': 300000}},
        }},
    }


class Plan(unittest.TestCase):
    def test_build_row_metrics_guard_growth_placement(self):
        cube = mini_cube()
        actual26 = {'07.델피노': {'202610': 2000, '202611': 1800}}
        budget27 = {'07.델피노': {'10': 2400, '11': 1620}}   # 10월 ×1.2, 11월 ×0.9
        ga4 = [{'name': '델피노 온가을PKG_D_PKG', 'category': '소노벨 WEST 델피노', 'prop': '07.델피노', 'series': '델피노 온가을PKG', 'purchases': 40, 'revenue': 5400000,
                'by_month': {'202610': [37, 5000000], '202510': [3, 400000]}},   # 판매기간(26.09~11) 밖 2025-10 구매는 제외

               {'name': '소노벨 WEST 델피노_패밀리스탠다드', 'category': '소노벨 WEST 델피노', 'prop': '07.델피노', 'series': '소노벨 WEST 델피노', 'purchases': 999, 'revenue': 1, 'by_month': {}}]
        rows = bp.build(cube, actual26, budget27, ga4, '202610')
        self.assertEqual(len(rows), 1)
        r = rows[0]
        a, g, p = r['actual'], r['guard'], r['plan']
        self.assertEqual(a['rn'], 80 + 40 + 60 + 20 + 30 + 10)          # 회원 코드 제외 = 240
        self.assertAlmostEqual(a['weekday_share'], (80 + 60 + 30) / 240, places=3)
        self.assertEqual(a['seg_mix'], {'OTA': 0.833, 'G-OTA': 0.167})
        # 베이스라인 ADR: 시즌 가중(41: 170/240, 42: 70/240), 투숙월 202610+202611 합산 ADR(41)=410e6/2050=200000, (42)=207e6/690=300000
        self.assertEqual(g['baseline_adr'], round((170 / 240) * 200000 + (70 / 240) * 300000))
        self.assertTrue(g['adr_ok'] in (True, False))
        # 주말 YoY: (300+390)/(400+380) − 1 = −11.5% → 잠식 의심
        self.assertAlmostEqual(g['weekend_yoy'], 690 / 780 - 1, places=3)
        self.assertTrue(g['cannibal']); self.assertIn('주말 잠식 의심', r['flags'])
        # growth: 10월(1.2, w=160) · 11월(base_ym=202610 이후라 1.0, w=80) → (1.2*160+1.0*80)/240
        self.assertAlmostEqual(p['growth'], (1.2 * 160 + 1.0 * 80) / 240, places=3)
        self.assertEqual(p['target_rn'], round(240 * p['growth']))
        self.assertEqual(p['target_adr'], max(a['adr'], g['baseline_adr']))
        # 배치: +364일 요일 정렬 (2026-09-24 목 → 2027-09-23 목)
        self.assertEqual(p['sale_from'], '20270923'); self.assertEqual(p['stay_from'], '20270930'); self.assertEqual(p['month'], '202709')
        # 웹 매칭: 룸온리(사업장명 시리즈) 제외, 패키지 1건 매칭
        self.assertEqual(r['web']['purchases'], 37)
        self.assertEqual(r['rank'], 1); self.assertIsNone(p['cost'])

    def test_min_rn_filter_and_dedupe_latest_year(self):
        cube = mini_cube()
        # 2025 동일 시리즈 캠페인 추가(더 큰 RN) → 최신 연도(2026) 행만 남고 basis에 이전 연도 표기
        cube['codes']['86000009'] = {'name': '(델) 온가을PKG_OTA', 'series': '온가을PKG', 'campaign': '2025 온가을PKG', 'category': '시즌패키지',
                                     'prop': '07.델피노', 'seg': 'OTA', 'chan': '야놀자',
                                     'by_ym': {'202510': {'s': {'41': [400, 80000000, 0, 0, 400]}, 'dow': [0]*7, 'lead': {'p50': 3, 'p90': 9, 'mean': 4}}}}
        cube['campaigns']['2025 온가을PKG'] = {'codes': ['86000009'], 'props': ['07.델피노'], 'category': '시즌패키지', 'year': 2025,
                                             'composition': {'text': '', 'auto': {}}, 'sale_window': {'from': '20250924', 'to': '20251130'},
                                             'stay_window': {'from': '20251001', 'to': '20251130'}, 'sale_text': '', 'note': '', 'source': 'auto', 'by_year': {'2025': {'rn': 400}}}
        cube['codes']['86000010'] = {'name': '(델) 소형테스트_OTA', 'series': '소형테스트', 'campaign': '소형테스트 (2026)', 'category': '기타',
                                     'prop': '07.델피노', 'seg': 'OTA', 'chan': '야놀자',
                                     'by_ym': {'202610': {'s': {'41': [50, 10000000, 0, 0, 50]}, 'dow': [0]*7, 'lead': {'p50': 3, 'p90': 9, 'mean': 4}}}}
        cube['campaigns']['소형테스트 (2026)'] = {'codes': ['86000010'], 'props': ['07.델피노'], 'category': '기타', 'year': 2026,
                                              'composition': {'text': '', 'auto': {}}, 'sale_window': None, 'stay_window': None, 'sale_text': '', 'note': '', 'source': 'auto', 'by_year': {'2026': {'rn': 50}}}
        rows = bp.build(cube, {}, {}, [], '202610')
        self.assertEqual([r['campaign'] for r in rows], ['2026 온가을PKG'])     # RN<100 제외, 동일 시리즈는 최신 연도
        self.assertIn('이전 2025 400실', rows[0]['basis'])
        self.assertEqual(rows[0]['plan']['growth'], 1.0)                    # 예산/실적 없음 → 1.0

    def test_comp_type_from_actual_components(self):
        # 구성가치 없음/미미 → 객실만 (이름 추정 없이 실제 구성 매출로만 판정)
        self.assertEqual(bp.comp_type(0, {}), '객실만')
        self.assertEqual(bp.comp_type(3000, {'식음': 1.0}), '객실만')
        # 20% 이상 구성만 표기, 고정 순서(식음→아쿠아→부대시설→스포츠→기타구성)
        self.assertEqual(bp.comp_type(40000, {'식음': 1.0}), '식음')
        self.assertEqual(bp.comp_type(60000, {'아쿠아': 0.4, '식음': 0.533, '부대': 0.067}), '식음+아쿠아')
        self.assertEqual(bp.comp_type(50000, {'부대': 0.7, '식음': 0.3}), '식음+부대시설')
        self.assertEqual(bp.comp_type(90000, {'식음': 0.4, '아쿠아': 0.3, '스포츠': 0.3}), '복합(3종+)')
        self.assertEqual(bp.comp_type(20000, {'기타': 1.0}), '기타구성')

    def test_composition_analysis_and_recommendation(self):
        def row(prop, ctype, rn, wk, adr, base, comp_bk, bk, name):
            return {'prop': prop, 'comp_type': ctype, 'campaign': name,
                    'actual': {'rn': rn, 'rev': rn * adr, 'adr': adr, 'weekday_share': wk, 'comp_bk': comp_bk, 'bk': bk},
                    'guard': {'baseline_adr': base}}
        rows = [row('07.델피노', '객실만', 1000, 0.6, 200000, 210000, 0, 1000, '세일룸'),
                row('07.델피노', '식음+아쿠아', 400, 0.7, 240000, 220000, 60000, 300, '조식오션A'),
                row('07.델피노', '식음+아쿠아', 300, 0.5, 230000, 220000, 50000, 200, '조식오션B'),
                row('07.델피노', '식음', 500, 0.8, 190000, 220000, 30000, 400, '조식C'),   # 베이스라인 −13.6% → 권장 제외
                row('07.델피노', '식음', 200, 0.8, 195000, 220000, 30000, 150, '조식D')]
        an = bp.composition_analysis(rows)
        d = an['07.델피노']
        fo = d['식음+아쿠아']
        self.assertEqual(fo['n'], 2); self.assertEqual(fo['rn'], 700)
        self.assertEqual(fo['wk_rn'], round(400 * 0.7 + 300 * 0.5))
        self.assertEqual(fo['adr'], round((400 * 240000 + 300 * 230000) / 700))
        self.assertAlmostEqual(fo['prem'], (400 * 240000 + 300 * 230000) / (700 * 220000) - 1, places=3)
        self.assertAlmostEqual(fo['vs_room'], fo['adr'] / 200000 - 1, places=3)
        self.assertEqual(fo['comp_bk'], round((60000 * 300 + 50000 * 200) / 500))
        self.assertEqual(fo['top'][0], '조식오션A')
        self.assertIsNone(d['객실만']['vs_room'])
        self.assertIn('07.델피노', an['_reco'])
        self.assertEqual(an['_reco']['07.델피노'], ['식음+아쿠아'])       # 객실만 제외, 식음은 ADR 가드 미달
        self.assertEqual(an['전체']['식음+아쿠아']['rn'], 700)                # 전체 합계 포함

    def test_web_match_limited_to_sale_window(self):
        idx = {'07.델피노': [{'name': '델피노 온가을PKG', '_key': bp.pn.product_key('델피노 온가을PKG'), 'purchases': 50, 'revenue': 9,
                              'by_month': {'202510': [30, 6], '202610': [20, 3]}}]}
        w = bp.match_web(idx, '07.델피노', '델피노 온가을PKG (2025)', [], months={'202509', '202510', '202511'})
        self.assertEqual(w['purchases'], 30); self.assertEqual(w['revenue'], 6)
        w_all = bp.match_web(idx, '07.델피노', '델피노 온가을PKG (2025)', [])
        self.assertEqual(w_all['purchases'], 50)
        self.assertEqual(bp.window_months({'from': '20250920', 'to': '20251105'}), {'202509', '202510', '202511'})

    def test_shift_and_month_helpers(self):
        self.assertEqual(bp.shift_weekday_aligned('20260701', 1), '20270630')   # 수 → 수
        self.assertEqual(bp.shift_weekday_aligned('20251202', 2), '20271130')   # 화 → 화
        self.assertEqual(bp.month_add('202601', -1), '202512'); self.assertEqual(bp.month_add('202612', 1), '202701')


if __name__ == '__main__':
    unittest.main()
