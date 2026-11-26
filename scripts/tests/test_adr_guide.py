import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import build_adr_guide as g  # noqa: E402


def node():
    """사업장 1개·OTA: 주중 10일(빈 객실 100), 주말 4일(빈 객실 20 → 제한)."""
    return {'ly_ym': '202512', 'props': {'07.델피노': {
        'seg': {'OTA': {'cur': {'w': [1000, 200_000_000], 'e': [600, 180_000_000]},
                        'lys': {'w': [800, 150_000_000], 'e': [500, 140_000_000]},
                        'lyf': {'w': [1400, 250_000_000], 'e': [700, 196_000_000]}}},
        'ly_days': {'w': 10, 'e': 4},
        'dates': [[d, 'w', 100, 50.0, 100] for d in range(1, 11)] + [[d, 'e', 20, 95.0, 380] for d in range(11, 15)]}}}


class Guide(unittest.TestCase):
    def test_day_type_and_inventory(self):
        self.assertEqual(g.day_type('비수기 주중'), 'w')
        self.assertEqual(g.day_type('겨울 성수기주중'), 'w')
        self.assertEqual(g.day_type('비발디 2월주중'), 'w')
        for s in ('비수기 금요일', '비수기 주말', '연휴', '겨울 최성수기', '겨울 성수기토요일'):
            self.assertEqual(g.day_type(s), 'e', s)
        av, un = g.avail_unsold(1071, 76.6, 1399)           # OCC 역산 가능객실(판매중지 반영)
        self.assertAlmostEqual(av, 1071 * 100 / 76.6, places=3)
        self.assertAlmostEqual(un, av - 1071, places=3)
        self.assertEqual(g.avail_unsold(0, 0, 309), (309.0, 309.0))
        self.assertEqual(g.prop_disp('19. 소노벨 제주'), '18.소노벨제주')
        self.assertEqual(g.prop_disp('소노펠리체빌리지 비발디파크'), '05.빌리지비발디')
        self.assertEqual(g.prop_disp('르네블루 바이 쏠비치'), '23.르네블루')

    def test_guide_calc_mix_cap_and_price_level(self):
        r = g.guide_calc(node(), ['07.델피노'], ['OTA'])
        pace = 1600 / 1300
        f_w = (200_000_000 / 1000) / (150_000_000 / 800)
        f_e = (180_000_000 / 600) / (140_000_000 / 500)
        adr_w = (100_000_000 / 600) * f_w                     # 작년 이후 주중 판매 ADR × 올해 요금수준
        adr_e = (56_000_000 / 200) * f_e
        ex_w = min(600 / 10 * pace, 100) * 10                 # 주중: 제한 없음
        ex_e = min(200 / 4 * pace, 20) * 4                    # 주말: 빈 객실 20으로 제한
        self.assertAlmostEqual(r['t']['w']['exp_rn'], ex_w, places=6)
        self.assertAlmostEqual(r['t']['e']['exp_rn'], ex_e, places=6)
        self.assertEqual(r['t']['e']['capped'], 4)
        self.assertEqual(r['t']['w']['capped'], 0)
        exp_rev = ex_w * adr_w + ex_e * adr_e
        self.assertAlmostEqual(r['pick_adr'], exp_rev / (ex_w + ex_e), places=3)
        self.assertAlmostEqual(r['close_adr'], (380_000_000 + exp_rev) / (1600 + ex_w + ex_e), places=3)
        self.assertAlmostEqual(r['cur_adr'], 380_000_000 / 1600, places=3)
        self.assertAlmostEqual(r['ly_drift'], (446_000_000 / 2100) / (290_000_000 / 1300) - 1, places=6)
        self.assertLess(r['close_chg'], 0)                    # 주말이 막혀 주중 위주 → 마감 ADR 하락

    def test_no_same_period_or_inventory_fallback(self):
        n = node()
        n['props']['07.델피노']['seg']['OTA']['lys'] = {}
        r = g.guide_calc(n, ['07.델피노'], ['OTA'])
        self.assertEqual(r['exp_rn'], 0)                      # 전년동기 없으면 가이드 없음
        self.assertIsNone(r['pick_adr'])
        n = node(); n['props']['07.델피노']['dates'] = None   # 재고 정보 없으면 작년 남은 날짜 수 그대로
        r = g.guide_calc(n, ['07.델피노'], ['OTA'])
        self.assertAlmostEqual(r['t']['e']['exp_rn'], 200 / 4 * (1600 / 1300) * 4, places=6)


if __name__ == '__main__':
    unittest.main()
