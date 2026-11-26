import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import parse_booking_status as bs  # noqa: E402

# Booking Status Report 사업장 페이지 텍스트(pdfplumber 추출 순서 그대로, 2026-11-25 운영3 Total 축약)
LINES = [
    "Booking Status Report _ Y2026 [3. 운영3 Total] #4",
    "Issue Date : 2026.11.25 [단위 : RN_실, ADR_원, Revenue_천원 / VAT 제외]",
    "2026년 11월 25일 (수) 2026년 11월 24일 (화) Pick Up",
    "Monthly",
    "RN Occ ADR Revenue RN Occ ADR Revenue RN Occ ADR Revenue",
    "2026년 11월 51,371 62.4% 172,440 8,858,408 51,184 62.2% 172,446 8,826,480 ▲ 187 ▲ 0.2%p ▼6 ▲ 31,929",
    "2026년 12월 43,221 50.8% 159,128 6,877,654 42,429 49.9% 158,561 6,727,581 ▲ 792 ▲ 0.9%p ▲ 567 ▲ 150,072",
    "2026 Budget Variance [2026 Today vs. Budget] RevPAR",
    "Monthly",
    "RN Occ ADR Revenue RN Occ ADR Revenue Budget OTB 달성도 전년대비",
    "2026년 11월 52,560 63.2% 165,212 8,683,561 ▼1,189 ▼0.8%p ▲ 7,228 ▲ 174,847 104,382 107,609 103.1% 105.0%",
    "Daily",
    "2026년 12월 58,305 67.8% 161,073 9,391,381 ▼15,084 ▼17.0%p ▼1,946 ▼2,513,728 109,249 80,853 74.0% 78.1%",
    "Membership Group FIT",
    "Monthly Grand Total Comp",
    "기 명 무기명 Staff Other Total G-Mice E-Mice Inbound Total Homepage OTA G-OTA Affiliate Other Total",
    "2026년 11월 ▲ 187 ▲ 22 ▲ 36 ▲ 19 ▲ 7 ▲ 84 ▲ 1 0 ▼5 ▼4 ▲ 4 ▲ 52 ▲ 38 ▲ 2 ▲ 10 ▲ 106 ▲ 1",
    "2025년 11월 26일 (수) 2025 Actual Pick Up",
    "Monthly",
    "RN Occ ADR Revenue RN Occ ADR Revenue RN Occ ADR Revenue",
    "2025년 11월 53,258 64.0% 159,020 8,469,104 53,556 64.4% 159,255 8,529,072 ▲ 127 ▲ 0.2%p ▼0 ▲ 20,175",
    "vs. 2025",
    "2025년 12월 42,630 49.6% 152,575 6,504,279 56,992 66.3% 156,248 8,904,913 ▲ 783 ▲ 0.9%p ▲ 295 ▲ 131,823",
    "Variance [Today, 2026 vs. 2025] Variance [vs. 2025 Actual] RevPAR",
    "2025년 11월 ▼1,887 ▼1.6%p ▲ 13,420 ▲ 389,305 ▲ 298 ▲ 0.4%p ▲ 235 ▲ 59,968 102,525 101,804 99.3%",
    "Segment 2025년 11월 22,184 12,336 18,435 303 53,258 64.0% 159,020 8,469,104 ▲ 47 ▼13 ▲ 92 ▲ 1 ▲ 127",
    "2025년 12월 19,888 14,774 7,849 119 42,630 49.6% 152,575 6,504,279 ▲ 316 ▲ 80 ▲ 378 ▲ 9 ▲ 783",
]


class BookingStatus(unittest.TestCase):
    def test_page_blocks(self):
        months, ly_date = bs.parse_page_lines(LINES)
        self.assertEqual(ly_date, '2025-11-26')
        self.assertEqual(months['2026-11']['otb'], {'rn': 51371, 'occ': 62.4, 'adr': 172440, 'rev_k': 8858408})
        self.assertEqual(months['2026-12']['budget'], {'rn': 58305, 'occ': 67.8, 'adr': 161073, 'rev_k': 9391381})
        # 작년 같은 날·작년 마감은 올해 같은 달 키(+1년)에 붙는다 — 'vs. 2025' 라벨이 행 사이에 끼어도 계속 읽음
        self.assertEqual(months['2026-11']['ly'], {'rn': 53258, 'occ': 64.0, 'adr': 159020, 'rev_k': 8469104})
        self.assertEqual(months['2026-11']['ly_act'], {'rn': 53556, 'occ': 64.4, 'adr': 159255, 'rev_k': 8529072})
        self.assertEqual(months['2026-12']['ly'], {'rn': 42630, 'occ': 49.6, 'adr': 152575, 'rev_k': 6504279})
        self.assertEqual(months['2026-12']['ly_act']['rn'], 56992)
        # 세그·증감 표 행은 어떤 블록에도 섞이지 않는다
        self.assertEqual(set(months), {'2026-11', '2026-12'})
        self.assertEqual(set(months['2026-11']), {'otb', 'budget', 'ly', 'ly_act'})

    def test_occ_with_arrow_quirk(self):
        # 소노펠리체 비발디파크 페이지: 작년 마감 OCC가 '▲ 21.7%p'로 찍힘 — 값은 OCC
        lines = [LINES[0], "2025년 11월 26일 (수) 2025 Actual Pick Up",
                 "2025년 11월 3,204 21.4% 228,168 731,051 3,254 ▲ 21.7%p 263,897 858,720 ▲ 7 ▲ 0.0% ▼668 ▼540"]
        months, _ = bs.parse_page_lines(lines)
        self.assertEqual(months['2026-11']['ly'], {'rn': 3204, 'occ': 21.4, 'adr': 228168, 'rev_k': 731051})
        self.assertEqual(months['2026-11']['ly_act'], {'rn': 3254, 'occ': 21.7, 'adr': 263897, 'rev_k': 858720})


if __name__ == '__main__':
    unittest.main()
