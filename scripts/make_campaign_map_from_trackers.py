#!/usr/bin/env python3.12
"""트래커 xlsx(사우나PKG '패키지코드' 시트)에서 86코드를 읽어 data/product_campaign_map.json을 만든다.
1회 실행 후 JSON을 커밋한다(Downloads 파일은 휘발). 실행: python3.12 -I scripts/make_campaign_map_from_trackers.py
대외비·관리자용."""
import json
import re
import sys
from pathlib import Path

from openpyxl import load_workbook

D = Path('/Users/chanminpark/Downloads/6_(GS) 가을프로모션_온(溫)PKG 최종실적')
OUT = Path(__file__).resolve().parent.parent / 'data' / 'product_campaign_map.json'

sauna_codes = set()
src = D / '(GS) 가을프로모션_(사우나PKG) 실적.xlsx'
if src.exists():
    ws = load_workbook(src, data_only=True)['패키지코드']
    for r in ws.iter_rows(min_row=7, max_row=25, values_only=True):
        for v in r:
            if isinstance(v, (int, float)) and 86000000 <= int(v) <= 86999999:
                sauna_codes.add(str(int(v)))
            elif isinstance(v, str) and re.fullmatch(r'86\d{6}', v.strip()):
                sauna_codes.add(v.strip())
else:
    print('경고: 트래커 파일 없음 → 코드 목록 비움(정규식 규칙만)', file=sys.stderr)

doc = {
    "_note": "수기 기획전 매핑. codes(정확) > regex(연도 일치) 순. composition/sale/note는 매트릭스 표기용. 대외비·관리자용",
    "campaigns": [
        {"name": "2025 가을프로모션 온(溫)PKG", "year": 2025, "scope": "전사업장(양평·남해·해운대 제외)",
         "match": {"codes": sorted(sauna_codes), "regex": r"온\(?溫\)?\s*가을|온가을|가을프로모션\s*사우나|사우나\s*PKG"},
         "composition": "객실+사우나2인+웰컴음료2잔 (비발디: 오션&찜질방+식혜 / 벨제주: 사우나+감귤와인)",
         "sale": "25.09.24~11.30", "stay": "25.10.01~11.30", "note": "음료 GS 예산 활용"},
        {"name": "2025-26 2연박PKG (2Nights Privilege)", "year": None, "scope": "전사업장",
         "match": {"codes": [], "regex": r"PRIVILEGE|2연박\s*PKG",
                   "sale_from": "20251115", "sale_to": "20260215"},
         "composition": "객실 2연박+사업장별 부대시설+레드와인 1병+굿즈 2종(커피2+10% 할인권)",
         "sale": "25.12.02~26.01.14", "stay": "25.12~26.02", "note": "와인 GS 예산 활용"},
        {"name": "2026 신년 윈터키즈브릭PKG", "year": None, "scope": "비발디·양양·삼척·천안·르네블루·거제·진도",
         "match": {"codes": [], "regex": r"키즈브릭|KIDS\s*BRICK", "sale_from": "20251215", "sale_to": "20260215"},
         "composition": "객실+키즈박스(레고 or 플레이모빌) 1개", "sale": "26.01.05~01.29", "stay": "26.01~02",
         "note": "키즈박스 GS 예산 활용"},
        {"name": "2025 Kids Free 조식PKG (놀유니버스×소노위크)", "year": None, "scope": "전사업장(양평·고양·남해·해운대 제외)",
         "match": {"codes": [], "regex": r"KIDS\s*FREE|키즈\s*프리|소노위크", "sale_from": "20251020", "sale_to": "20251130"},
         "composition": "객실+조식(성인2+미취학아동1)", "sale": "25.11.04~11.08", "stay": "", "note": ""},
    ],
}
OUT.write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding='utf-8')
print('wrote', OUT, '| sauna codes:', len(sauna_codes))
