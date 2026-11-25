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
        self.assertEqual(b.baseline['01.벨비발디']['2025']['41'][0], 4)
        self.assertEqual(b.baseline['01.벨비발디']['2025']['43'][0], 1)
        self.assertNotIn('25.팔라티움', b.baseline)

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
        self.assertEqual(cube['baseline']['01.벨비발디']['2025']['41']['rn'], 1)
        self.assertEqual(cube['dims']['properties'][0]['display'], '01.벨비발디')
        self.assertIn('소노벨 비발디파크', cube['dims']['prop_alias'])

    def test_write_outputs_json_and_gz(self):
        out = Path(self.tmp) / 'product_cube.json'
        n1, n2 = bpc.write_outputs({'meta': {'x': 1}, 'codes': {}}, out)
        self.assertTrue(out.exists()); self.assertTrue(out.with_suffix('.json.gz').exists())
        self.assertGreater(n1, 0); self.assertGreater(n2, 0)
        self.assertEqual(json.loads(gzip.open(out.with_suffix('.json.gz')).read())['meta']['x'], 1)


if __name__ == '__main__':
    unittest.main()
