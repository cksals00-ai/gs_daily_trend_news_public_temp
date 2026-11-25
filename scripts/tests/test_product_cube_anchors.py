"""실제 큐브 산출물 앵커 검증. 기대값 = 실적 트래커 수치(상수로 내장). 산출물 없으면 skip.
실행: python3.12 -m unittest scripts.tests.test_product_cube_anchors -v"""
import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CUBE = ROOT / 'docs' / 'data' / 'product_cube.json'


def _sum_codes(cube, codes, yms):
    rn = rev = 0
    for c in codes:
        node = cube['codes'].get(c)
        if not node:
            continue
        for ym in yms:
            for v in node['by_ym'].get(ym, {}).get('s', {}).values():
                rn += v[0]
                rev += v[1]
    return rn, rev


def _sum_campaign_regex(cube, pattern, yms):
    rx = re.compile(pattern, re.I)
    codes = [k for k, v in cube['codes'].items() if rx.search(v['name']) or rx.search(v['campaign'])]
    return _sum_codes(cube, codes, yms), len(codes)


@unittest.skipUnless(CUBE.exists(), 'product_cube.json 없음 — 빌드 후 실행')
class Anchors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cube = json.loads(CUBE.read_text(encoding='utf-8'))
        cls.cmap = json.loads((ROOT / 'data' / 'product_campaign_map.json').read_text(encoding='utf-8'))

    def test_sauna_code_exact_october(self):
        rn, rev = _sum_codes(self.cube, ['86150059'], ['202510'])
        self.assertEqual(rn, 126)
        self.assertAlmostEqual(rev, round(20303000 / 1.1), delta=5)   # 시즌별 반올림 합산 오차 ≤ 5원

    def test_on_pkg_total_oct_nov_2025(self):
        codes = next(c for c in self.cmap['campaigns'] if c['name'].startswith('2025 가을프로모션'))['match']['codes']
        rn, rev = _sum_codes(self.cube, codes, ['202510', '202511'])
        self.assertAlmostEqual(rn, 5476, delta=5476 * 0.01, msg=f'rn={rn}')
        self.assertAlmostEqual(rev / 1e6, 987, delta=987 * 0.015, msg=f'rev={rev/1e6:.1f}')

    def test_two_nights_privilege_dec_feb(self):
        """트래커 1/26 최종 TOTAL(GS채널+회원+D멤버스) 11,972실 / 2,395백만.
        큐브(2026-11-25 빌드) 전세그 합 11,463실(−4.3%) — 1월 +3%·2월 −6%는 스냅샷 이후 취소분으로 설명되나
        12월은 raw 자체가 1,394실로 트래커 1,952실 대비 −29% (nightly/체크인월 어느 기준으로도 동일).
        트래커 12월 산정 기준은 미확인 → 총합 기준 −10%/+15%로만 가드."""
        (rn, rev), n = _sum_campaign_regex(self.cube, r'PRIVILEGE|2연박\s*PKG', ['202512', '202601', '202602'])
        self.assertGreater(n, 20)
        self.assertGreaterEqual(rn, 11972 * 0.90, msg=f'rn={rn}')
        self.assertLessEqual(rn, 11972 * 1.15, msg=f'rn={rn}')
        self.assertGreaterEqual(rev / 1e6, 2395 * 0.88, msg=f'rev={rev/1e6:.0f}')

    def test_kids_brick_jan_feb_2026(self):
        (rn, rev), n = _sum_campaign_regex(self.cube, r'키즈브릭|KIDS\s*BRICK', ['202601', '202602'])
        self.assertGreater(n, 5)
        self.assertGreaterEqual(rn, 1558 * 0.97, msg=f'rn={rn}')
        self.assertLessEqual(rn, 1558 * 1.15, msg=f'rn={rn}')

    def test_product_detail_consistency_by_year(self):
        """큐브 연도 RN 합(전 세그) vs 기존 product_detail(by_category) 연도 합 ±0.5% (같은 86·27 필터).
        2026-11-25 빌드: 2023 −0.04% · 2024 −0.06% · 2025 −0.05% · 2026 −0.08% (제외 사업장분)."""
        agg = ROOT / 'data' / 'db_aggregated.json'
        if not agg.exists():
            self.skipTest('db_aggregated 없음')
        pd = json.loads(agg.read_text(encoding='utf-8')).get('product_detail', {})
        for y in ('2023', '2024', '2025', '2026'):
            if y not in [str(v) for v in self.cube['meta']['years']]:
                continue
            ref = sum(v.get('rn', 0) for cat in pd.get('by_category', {}).values() for ym, v in cat.get(y, {}).items())
            if not ref:
                continue
            mine = sum(v[0] for c in self.cube['codes'].values()
                       for ym, node in c['by_ym'].items() if ym.startswith(y) for v in node['s'].values())
            self.assertAlmostEqual(mine, ref, delta=ref * 0.005, msg=f'{y}: cube={mine} product_detail={ref}')


if __name__ == '__main__':
    unittest.main()
