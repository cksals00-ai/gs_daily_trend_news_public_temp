import sys, os, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import product_naming as pn


class CleanName(unittest.TestCase):
    def test_strips_property_prefix_year_and_channel(self):
        cases = {
            '[벨비발디] 온(溫)가을 PKG_OTA': '온(溫)가을 PKG',
            '(단양) 25Y 온가을 PKG(OTA_11%)': '온가을 PKG',
            '(델) 온가을패키지_G15': '온가을PKG',
            '[캄제주] 가을프로모션 사우나 PKG (G15)': '가을프로모션 사우나 PKG',
            '(델피노) 2Nights Privilege_D_PKG': '2Nights Privilege',
            '25-26 비발디 윈터키즈브릭PKG_G15': '비발디 윈터키즈브릭PKG',
            '비발디파크 테이스티모닝(신규) 노블리안무기명PKG_소노펠리체': '비발디파크 테이스티모닝 노블리안PKG 소노펠리체',
        }
        for raw, want in cases.items():
            self.assertEqual(pn.clean_product_name(raw), want, raw)

    def test_product_key_groups_variants(self):
        k1 = pn.product_key('[벨비발디] 온(溫)가을 PKG_OTA')
        k2 = pn.product_key('(캄비발디) 온(溫)가을 PKG_G15')
        self.assertEqual(k1, k2)
        self.assertNotEqual(k1, pn.product_key('(단양) 25Y 온가을 PKG(OTA_11%)'))  # 溫 유무는 다른 키 (수기 맵으로 묶음)


class Channel(unittest.TestCase):
    def test_agent_prefix_and_seg_code(self):
        self.assertEqual(pn.channel_class('OTA_놀유니버스(야놀자)', '72', ''), ('OTA', '놀유니버스(야놀자)'))
        self.assertEqual(pn.channel_class('GOTA_아고다', 'A5', ''), ('G-OTA', '아고다'))
        self.assertEqual(pn.channel_class('', '58', ''), ('Inbound', 'Inbound'))
        self.assertEqual(pn.channel_class('', 'MP', '회원PKG'), ('회원', '회원'))
        self.assertEqual(pn.channel_class('', '34', ''), ('D멤버스', 'D멤버스'))
        self.assertEqual(pn.channel_class('OTA_여기어때컴퍼니', '53', ''), ('OTA', '여기어때컴퍼니'))


class Props(unittest.TestCase):
    def test_prop_display(self):
        self.assertEqual(pn.prop_display('02. 소노벨 비발디파크'), '01.벨비발디')
        self.assertEqual(pn.prop_display('03. 소노문 비발디파크'), '01.벨비발디')
        self.assertEqual(pn.prop_display('08. 델피노'), '07.델피노')
        self.assertEqual(pn.prop_display('11. 소노문 단양'), '10.소노벨단양')
        self.assertEqual(pn.prop_display('20. 소노캄 제주'), '19.소노캄제주')
        self.assertIsNone(pn.prop_display('25. 팔라티움 해운대'))
        self.assertEqual(len(pn.PROP_ORDER), 23)

    def test_season_label(self):
        self.assertEqual(pn.season_label('41'), '비수기 주중')
        self.assertEqual(pn.season_label('30'), '연휴')
        self.assertEqual(pn.season_label('99'), '기타')


class CampaignMap(unittest.TestCase):
    def test_assign_campaign_precedence(self):
        import json, tempfile
        with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump({"campaigns": [{"name": "2025 가을 온PKG", "year": 2025,
                                       "match": {"codes": ["86150059"], "regex": "온가을|온\\(溫\\)가을"}}]}, f)
            path = f.name
        cmap = pn.load_campaign_map(path)
        self.assertEqual(pn.assign_campaign(cmap, '86150059', '아무이름', 2024), '2025 가을 온PKG')      # 코드 우선
        self.assertEqual(pn.assign_campaign(cmap, '86000000', '(단양) 온가을 PKG', 2025), '2025 가을 온PKG')  # 정규식+연도
        self.assertEqual(pn.assign_campaign(cmap, '86000000', '(단양) 온가을 PKG', 2024), '온가을 PKG (2024)')  # 연도 불일치 → 자동
        self.assertEqual(pn.load_campaign_map('/nonexistent.json'), {'by_code': {}, 'rules': [], 'meta': {}})
        os.unlink(path)

    def test_assign_campaign_sale_window(self):
        import json, tempfile
        with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump({"campaigns": [{"name": "2025-26 2연박PKG", "year": None,
                                       "match": {"regex": "2연박", "sale_from": "20251115", "sale_to": "20260215"}}]}, f)
            path = f.name
        cmap = pn.load_campaign_map(path)
        self.assertEqual(pn.assign_campaign(cmap, '86000001', '(델피노) 2연박 PKG', 2025, '20251202'), '2025-26 2연박PKG')
        self.assertEqual(pn.assign_campaign(cmap, '86000002', '(델피노) 2연박 PKG', 2025, '20250310'), '2연박 PKG (2025)')  # 봄 2연박 → 자동
        self.assertEqual(pn.assign_campaign(cmap, '86000003', '(델피노) 2연박 PKG', 2025, None), '2연박 PKG (2025)')        # 판매일 미상 → 자동
        os.unlink(path)

    def test_clean_leftover_brackets(self):
        self.assertEqual(pn.clean_product_name('(New)SONO Water-ful PKG (GOTA15)'), 'SONO Water-ful PKG')
        self.assertEqual(pn.clean_product_name(')SONOWaterfulPKG'), 'SONOWaterfulPKG')


if __name__ == '__main__':
    unittest.main()
