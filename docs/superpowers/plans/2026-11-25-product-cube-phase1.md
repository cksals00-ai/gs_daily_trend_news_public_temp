# 2027 상품기획 1차 — 상품 큐브 + 상품상세 뎁스6 + 사업장별 상품 분석 매트릭스 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** raw_db(27 예약 파일)에서 상품코드(회원번호 86…) 단위의 상품 큐브 JSON을 만들고, `docs/product-detail.html`에 뎁스 6 드릴(SEC8)과 사업장별 상품 분석 매트릭스(SEC9, 엑셀 다운로드)를 추가한다.

**Architecture:** 새 빌더 `scripts/build_product_cube.py`가 raw 27 파일을 한 번 스트리밍하며 코드별 `by_ym × 요금시즌`, 요일, 리드타임, 객실타입, 거래처 집계와 사업장×연도×요금시즌 베이스라인을 만들고 `docs/data/product_cube.json(.gz)`로 쓴다. 상품명 정규화·기획전 묶기·채널 분류는 `scripts/product_naming.py`에 분리(순수 함수, 단위 테스트). 페이지는 큐브만 읽어 클라이언트에서 드릴·매트릭스·xlsx를 만든다. 기존 `parse_raw_db.py`·`build.py`·`db_aggregated`는 건드리지 않는다.

**Tech Stack:** Python 3.12(`/usr/local/bin/python3.12`, stdlib csv/json/gzip/unittest만), 기존 `scripts/parse_package_trend.classify_v4`, `scripts/generate_otb_data.PROPERTY_DEFS`, 브라우저 JS(ES5 스타일, 기존 페이지 관례), SheetJS `xlsx@0.18.5`(jsdelivr, 기존 페이지와 동일), Chart.js 4.4.0(이미 로드됨).

## Global Constraints

- 스펙: `docs/superpowers/specs/2026-11-25-product-plan-2027-design.md` (아래 값은 거기서 복사)
- 객실매출 = `1박객실료 × 객실수 ÷ 1.1` (VAT 제외). 구성가치 = `(PKG패키지총금액 − PKG객실) ÷ 1.1`을 **KEY_RSV_NO당 1회**만 가산
- 큐브 입력은 **27 파일만**(재전송 = 유효 예약). 28 취소 파일은 1차에서 읽지 않는다
- 대상 행: `회원번호`가 `86`으로 시작. 세그 코드(`변경예약집계코드`) 전부 보존하되 기본 뷰는 GS 세그(`72`,`53`→OTA / `A4`,`A5`→G-OTA / `58`→Inbound). `MP`→회원, `34`→D멤버스, `73`→무기명, `CP`→제휴, 그 외→기타
- 요금시즌코드 라벨: 41 비수기 주중 · 43 비수기 주말(금) · 42 비수기 주말(토) · 11 성수기 주중 · 14 성수기 주말(금) · 12 성수기 주말(토) · 13/30 연휴. 주중 = 41(보조: 11)
- 사업장 display명·순서 = `generate_otb_data.PROPERTY_DEFS` 23개(팔라티움 제외). 매핑은 `변경사업장명`에서 `^\d+\.\s*` 접두사를 뗀 이름을 각 def의 db_props 목록과 대조
- 2026 폴더: 재전송 파일(파일명에 `(YYYYMMDD-YYYYMMDD)`)과 최신 스냅샷(`_YYYYMMDD_생성시간`) 공존 시 **스냅샷이 실제 보유한 최소 투숙월(행 500 이상)**을 경계로, 재전송은 그 직전월까지·스냅샷은 그 달부터 파싱(`parse_raw_db._snapshot_min_stay_month`와 동일 규칙)
- 산출물·화면에 소스 출처 표현(출처·주간회의·PDF 등) 금지, 「대외비·관리자용」 표기. 추정치는 '가정' 라벨 없이는 표기 금지
- 커밋은 **명시적 pathspec**(`git add <파일>…` + `git commit -- <파일>…`). `_host_crawl_status.json` 등 자동 변경 파일은 절대 포함하지 않는다. 커밋 메시지 끝: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`
- 테스트 러너: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && /usr/local/bin/python3.12 -m unittest discover -s scripts/tests -p 'test_*.py' -v` (pytest 미설치)
- 페이지 인증 executive 유지, 데이터 fetch는 캐시버스터 `?t=`+Date.now(), 외부 스크립트는 고정 버전
- 배포(push)는 사용자가 화면 확인 후 승인했을 때만

---

## File Structure

| 파일 | 역할 |
|---|---|
| `scripts/product_naming.py` (신규) | 순수 함수: `clean_product_name`, `product_key`, `channel_class`, `season_label`, `prop_display`(PROPERTY_DEFS 기반), `load_campaign_map`, `assign_campaign` |
| `scripts/build_product_cube.py` (신규) | CLI. 파일 선택(`select_files`), 행 파서(`iter_rows`), 집계(`CubeBuilder`), 마무리(`finalize`), 저장(`write_outputs`) |
| `data/product_campaign_map.json` (신규) | 수기 기획전 매핑(트래커 4종 코드·구성·판매기간·비고). `scripts/make_campaign_map_from_trackers.py`로 생성 후 커밋 |
| `scripts/make_campaign_map_from_trackers.py` (신규, 1회용) | Downloads의 트래커 xlsx에서 코드 목록을 읽어 위 JSON 생성 |
| `scripts/tests/test_product_naming.py` (신규) | 이름 정규화·채널·기획전 배정 단위 테스트 |
| `scripts/tests/test_product_cube.py` (신규) | 합성 raw 파일(cp949, `;`)로 파서·집계·구성가치 dedup·파일 선택 규칙 테스트 |
| `scripts/tests/test_product_cube_anchors.py` (신규) | 실제 큐브 산출물 대상 앵커 검증(트래커 수치 내장, 산출물 없으면 skip) |
| `docs/data/product_cube.json`, `.json.gz` (산출물) | 큐브 |
| `docs/product-detail.html` (수정) | SheetJS 로드, SEC8 드릴, SEC9 매트릭스+엑셀, 큐브 로더 |

---

### Task 1: 상품명 정규화·채널·사업장·시즌 순수 함수 (`product_naming.py`)

**Files:**
- Create: `scripts/product_naming.py`
- Test: `scripts/tests/test_product_naming.py`

**Interfaces:**
- Produces:
  - `clean_product_name(name: str) -> str` — 사업장 접두사·연도 토큰·채널/수수료 토큰 제거, `패키지`→`PKG`, 공백 정리
  - `product_key(name: str) -> str` — `clean_product_name` 결과를 대문자·공백/기호 제거한 그룹 키
  - `channel_class(agent: str, seg_code: str, name: str) -> tuple[str, str]` — `(seg, chan)`; seg ∈ {OTA, G-OTA, Inbound, 회원, D멤버스, 무기명, 제휴, 기타}, chan = 거래처명 또는 세그명
  - `season_label(code: str) -> str`
  - `PROP_ORDER: list[str]` (display 23개), `prop_display(raw_prop: str) -> str | None`
  - `SEASONS: dict[str,str]`, `GS_SEGS = ("OTA","G-OTA","Inbound")`

- [ ] **Step 1: 실패하는 테스트 작성**

```python
# scripts/tests/test_product_naming.py
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


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: 실패 확인**

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && /usr/local/bin/python3.12 -m unittest scripts.tests.test_product_naming -v 2>&1 | tail -5`
Expected: `ModuleNotFoundError: No module named 'product_naming'` (또는 ImportError)

- [ ] **Step 3: 구현**

```python
# scripts/product_naming.py
"""상품 큐브용 순수 함수: 상품명 정규화 · 채널 분류 · 사업장 display · 요금시즌 라벨 · 기획전 맵."""
import json, os, re, sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from generate_otb_data import PROPERTY_DEFS  # (bi_name, display, region, db_props)

SEASONS = {'41': '비수기 주중', '43': '비수기 주말(금)', '42': '비수기 주말(토)',
           '11': '성수기 주중', '14': '성수기 주말(금)', '12': '성수기 주말(토)',
           '13': '연휴', '30': '연휴'}
WEEKDAY_SEASONS = ('41', '11')
GS_SEGS = ('OTA', 'G-OTA', 'Inbound')
SEG_BY_CODE = {'72': 'OTA', '53': 'OTA', 'A4': 'G-OTA', 'A5': 'G-OTA', '58': 'Inbound',
               'MP': '회원', '34': 'D멤버스', '73': '무기명', 'CP': '제휴'}

PROP_ORDER = [d[1] for d in PROPERTY_DEFS]
_PROP_LOOKUP = {}
for _bi, _disp, _region, _dbs in PROPERTY_DEFS:
    for _n in _dbs:
        _PROP_LOOKUP[_n.replace(' ', '')] = _disp
PROP_REGION = {d[1]: d[2] for d in PROPERTY_DEFS}


def prop_display(raw_prop):
    """'02. 소노벨 비발디파크' → '01.벨비발디'. 미매핑(팔라티움 등) → None."""
    if not raw_prop:
        return None
    s = re.sub(r'^\s*\d+\.\s*', '', raw_prop).replace(' ', '')
    if s in _PROP_LOOKUP:
        return _PROP_LOOKUP[s]
    for k, v in _PROP_LOOKUP.items():          # 부분 일치(예: '소노벨 비발디파크 B·C')
        if s.startswith(k):
            return v
    return None


def season_label(code):
    return SEASONS.get((code or '').strip(), '기타')


_PREFIX_RE = re.compile(r'^\s*[\[(（【][^\])）】]{1,16}[\])）】]\s*')
_YEAR_RE = re.compile(r'(?<![\d가-힣A-Za-z])(?:20)?2[2-7](?:\s*[-~/]\s*(?:20)?2[2-8])?\s*(?:Y|년)?(?![\d%인박명개])')
_CHAN_WORDS = ['GOTA', 'G-OTA', 'OTA', 'D멤버스', '디멤버스', 'D_PKG', 'D PKG', 'DPKG', '무기명', '기명',
               '회원', '직불', '네이버', '쿠팡', '제휴', '대매점', '홈페이지', '자사', '야놀자', '여기어때',
               '아고다', '트립닷컴', '익스피디아', '부킹닷컴', '트립비토즈', '놀유니버스', '맥스모바일',
               '타이드스퀘어', '스마트인피니', '인터파크', '마이리얼트립', '수수료', '신규', 'N']
_CHAN_RE = re.compile(r'(?<![가-힣A-Za-z])(?:' + '|'.join(re.escape(w) for w in sorted(_CHAN_WORDS, key=len, reverse=True))
                      + r'|G\d{1,2}|\d{1,2}(?:\.\d)?%|0\.1)(?![가-힣A-Za-z])', re.I)
_SEP_RE = re.compile(r'[\s_/,+·]+')
_EMPTY_PAREN_RE = re.compile(r'[\[(（【]\s*[\])）】]')


def clean_product_name(name):
    s = (name or '').strip()
    for _ in range(2):
        s = _PREFIX_RE.sub('', s)
    s = _YEAR_RE.sub(' ', s)
    s = _CHAN_RE.sub(' ', s)
    s = s.replace('패키지', 'PKG')
    s = _EMPTY_PAREN_RE.sub(' ', s)
    s = _SEP_RE.sub(' ', s).strip(' -_·')
    s = re.sub(r'\s*\(\s*\)', '', s).strip()
    return s


def product_key(name):
    return re.sub(r'[\s\-_()（）\[\]【】·.]', '', clean_product_name(name)).upper()


def channel_class(agent, seg_code, name):
    a = (agent or '').strip()
    code = (seg_code or '').strip()
    seg = SEG_BY_CODE.get(code, '기타')
    chan = None
    for pre, s in (('OTA_', 'OTA'), ('GOTA_', 'G-OTA')):
        if a.startswith(pre):
            seg = s if seg in ('OTA', 'G-OTA', '기타') else seg
            chan = a[len(pre):].strip()
            break
    if chan is None:
        chan = a if a and seg == '기타' else seg
    if seg == 'Inbound':
        chan = 'Inbound'
    return (seg, chan or seg)


def load_campaign_map(path):
    """data/product_campaign_map.json → {'by_code': {code: name}, 'rules': [(compiled_regex, name, year|None)], 'meta': {name: {...}}}"""
    if not os.path.exists(path):
        return {'by_code': {}, 'rules': [], 'meta': {}}
    with open(path, encoding='utf-8') as f:
        doc = json.load(f)
    out = {'by_code': {}, 'rules': [], 'meta': {}}
    for c in doc.get('campaigns', []):
        out['meta'][c['name']] = {k: c.get(k) for k in ('year', 'composition', 'sale', 'stay', 'note', 'scope')}
        for code in c.get('match', {}).get('codes', []):
            out['by_code'][str(code)] = c['name']
        rx = c.get('match', {}).get('regex')
        if rx:
            out['rules'].append((re.compile(rx, re.I), c['name'], c.get('year')))
    return out


def assign_campaign(cmap, code, name, sale_year):
    """수기 코드 매핑 → 정규식 규칙(연도 일치 또는 규칙에 연도 없음) → 자동 '{정규화명} ({판매연도})'."""
    if code in cmap['by_code']:
        return cmap['by_code'][code]
    for rx, cname, yr in cmap['rules']:
        if rx.search(name or '') and (yr is None or yr == sale_year):
            return cname
    base = clean_product_name(name) or '미상'
    return f'{base} ({sale_year})'
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && /usr/local/bin/python3.12 -m unittest scripts.tests.test_product_naming -v 2>&1 | tail -12`
Expected: `OK` (7 tests). 기대 문자열이 어긋나면 **테스트 기대값이 아니라 정규식을 고친다**(기대값은 스펙의 그룹핑 의도).

- [ ] **Step 5: 커밋**

```bash
cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp
git add scripts/product_naming.py scripts/tests/test_product_naming.py
git commit -m "feat(product-cube): 상품명 정규화·채널·사업장·시즌 순수 함수

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -- scripts/product_naming.py scripts/tests/test_product_naming.py
```

---

### Task 2: 파일 선택 + 행 파서 + 집계기 (`build_product_cube.py` 코어)

**Files:**
- Create: `scripts/build_product_cube.py`
- Test: `scripts/tests/test_product_cube.py`

**Interfaces:**
- Consumes: Task 1의 `product_naming` 전부
- Produces:
  - `select_files(raw_dir: Path, years: list[int]) -> list[tuple[Path, str|None, str|None]]` — `(파일, min_ym, max_ym)`; 27 파일만
  - `iter_rows(path: Path, min_ym, max_ym) -> Iterator[dict]` — 키: `key, stay(YYYYMMDD), prop_raw, code, name, roomtype, season, rate(float), nights(int), rooms(int), booked(YYYYMMDD), agent, seg_code, pkg_room, pkg_total, pkg_parts(dict), sale_price, deposit, comm, pkg_class`
  - `class CubeBuilder` with `add(row: dict)`, `finalize(cmap) -> dict` (Task 3에서 finalize 완성)

- [ ] **Step 1: 실패하는 테스트 작성** (합성 raw 파일을 tmp에 만든다)

```python
# scripts/tests/test_product_cube.py
import sys, os, unittest, tempfile, shutil
from pathlib import Path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import build_product_cube as bpc

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
        # RN: R1 2행(1실씩) + R2 2실 = 4 ; 매출 = (110000+110000+110000*2)/1.1
        tot_rn = sum(v[0] for ym in c['by_ym'].values() for v in ym['s'].values())
        tot_rev = sum(v[1] for ym in c['by_ym'].values() for v in ym['s'].values())
        self.assertEqual(tot_rn, 4)
        self.assertAlmostEqual(tot_rev, 440000 / 1.1, places=2)
        # 구성가치: R1 (330000-220000)/1.1 1회 + R2 (165000-110000)/1.1 1회 (2행째 R1은 가산 안 함)
        tot_comp = sum(v[2] for ym in c['by_ym'].values() for v in ym['s'].values())
        self.assertAlmostEqual(tot_comp, (110000 + 55000) / 1.1, places=2)
        # 예약건수 2, 시즌 분리(41: R1 1박 + R2 2실 = 3 / 43: 1)
        s = c['by_ym']['202510']['s']
        self.assertEqual(s['41'][0], 3); self.assertEqual(s['43'][0], 1)
        self.assertEqual(sum(v[4] for v in s.values()), 2)
        # 리드타임: R1 = 9일, R2 = 1일
        self.assertEqual(sorted(c['by_ym']['202510']['lead_list']), [1, 9])
        # 요일(2025-10-10 금=5, 10-11 토=6, 10-12 일=0 ×2실)
        self.assertEqual(c['by_ym']['202510']['dow'], [2, 0, 0, 0, 0, 1, 1])
        # 세그/채널 집계
        self.assertEqual(c['seg_counts'], {'OTA': 2, 'G-OTA': 1})
        self.assertEqual(c['ag']['놀유니버스(야놀자)'][0], 2); self.assertEqual(c['ag']['아고다'][0], 2)
        # 베이스라인(GS 세그 전체): 01.벨비발디 2025 41 → RN 1+1+2+1(R3) = 5, 43 → 1 ; 팔라티움 행 제외
        self.assertEqual(b.baseline['01.벨비발디']['2025']['41'][0], 5)
        self.assertEqual(b.baseline['01.벨비발디']['2025']['43'][0], 1)
        self.assertNotIn('25.팔라티움', b.baseline)

    def test_month_filter_applied(self):
        lines = [row(KEY_RSV_NO='A', 판매일자='20261001'), row(KEY_RSV_NO='B', 판매일자='20261101')]
        p = write_raw(self.tmp, '27.x_20261125_생성시간(1).txt', lines)
        self.assertEqual([r['stay'] for r in bpc.iter_rows(p, '202611', None)], ['20261101'])
        self.assertEqual([r['stay'] for r in bpc.iter_rows(p, None, '202610')], ['20261001'])


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: 실패 확인**

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && /usr/local/bin/python3.12 -m unittest scripts.tests.test_product_cube -v 2>&1 | tail -5`
Expected: `ModuleNotFoundError: No module named 'build_product_cube'`

- [ ] **Step 3: 구현 (코어)**

```python
#!/usr/bin/env python3.12
# scripts/build_product_cube.py
"""상품 큐브 빌더 — raw_db 27 파일 → docs/data/product_cube.json(.gz)
사용: python3.12 scripts/build_product_cube.py [--years 2023 2024 2025 2026] [--raw-dir data/raw_db]
        [--out docs/data/product_cube.json] [--campaign-map data/product_campaign_map.json] [--max-rows N]
대외비·관리자용 산출물. 기존 parse_raw_db/build.py와 독립."""
import argparse, csv, gzip, json, os, re, sys, time, statistics
from collections import defaultdict, Counter
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import product_naming as pn
from parse_package_trend import classify_v4

ROOT = Path(__file__).resolve().parent.parent
GS_CODES = {'72', '53', 'A4', 'A5', '58'}
PKG_PART_COLS = {'식음': ('PKG영업장식음',), '부대': ('PKG영업장부대', '부대'), '스포츠': ('PKG영업장스포츠', '스포츠'),
                 '아쿠아': ('PKG영업장아쿠아', '아쿠아'), '골프': ('PKG골프',), '기타': ('PKG영업장기타', 'PKG기타', '기타', 'PKG영업장')}
_RETRANS_RE = re.compile(r'\((\d{8})-(\d{8})\)')
_SNAP_RE = re.compile(r'_(\d{8})_생성시간')


def _prev_month(ym):
    y, m = int(ym[:4]), int(ym[4:])
    return f'{y-1}12' if m == 1 else f'{y}{m-1:02d}'


def _snapshot_min_stay_month(path, min_rows=500):
    counts = Counter()
    with open(path, encoding='cp949', errors='replace', newline='') as f:
        rd = csv.reader(f, delimiter=';')
        header = [h.strip().lstrip('﻿') for h in next(rd)]
        i = header.index('판매일자')
        for parts in rd:
            if i < len(parts):
                d = parts[i].strip()
                if len(d) >= 6 and d[:6].isdigit():
                    counts[d[:6]] += 1
    valid = sorted(ym for ym, c in counts.items() if c >= min_rows)
    return valid[0] if valid else None


def select_files(raw_dir, years):
    """연도별 27 파일 선택. 재전송+스냅샷 공존 시 스냅샷 최소 투숙월 경계로 분리."""
    out = []
    for y in years:
        d = Path(raw_dir) / str(y)
        if not d.is_dir():
            continue
        files = [p for p in d.iterdir() if p.is_file() and p.name.startswith('27') and p.suffix == '.txt']
        retrans = [p for p in files if _RETRANS_RE.search(p.name)]
        snaps = [p for p in files if _SNAP_RE.search(p.name)]
        snaps.sort(key=lambda p: _SNAP_RE.search(p.name).group(1), reverse=True)
        snaps = snaps[:1]
        if retrans and snaps:
            smin = _snapshot_min_stay_month(snaps[0])
            rmax = _prev_month(smin) if smin else None
            for p in retrans:
                out.append((p, None, rmax))
            out.append((snaps[0], smin, None))
        else:
            for p in retrans or snaps:
                out.append((p, None, None))
    return out


def _num(s):
    s = (s or '').replace(',', '').strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def iter_rows(path, min_ym, max_ym, max_rows=None):
    with open(path, encoding='cp949', errors='replace', newline='') as f:
        rd = csv.reader(f, delimiter=';')
        header = [h.strip().lstrip('﻿') for h in next(rd)]
        idx = {h: i for i, h in enumerate(header)}
        def col(name, alt=None):
            return idx.get(name, idx.get(alt, -1) if alt else -1)
        i_key, i_stay, i_prop, i_prop2 = col('KEY_RSV_NO'), col('판매일자'), col('변경사업장명'), col('영업장명')
        i_code, i_name, i_rt, i_seg = col('회원번호'), col('회원명'), col('객실타입'), col('변경예약집계코드', '예약집계코드')
        i_season, i_rate, i_nights, i_rooms = col('요금시즌코드'), col('1박객실료'), col('박수'), col('객실수')
        i_booked, i_agent, i_pr, i_pt = col('최초입력일자'), col('AGENT명'), col('PKG객실'), col('PKG패키지총금액')
        i_sale, i_dep, i_comm, i_pc = col('판매가'), col('입금가'), col('수수료'), col('패키지분류코드명')
        parts_idx = {k: [idx[c] for c in cols if c in idx] for k, cols in PKG_PART_COLS.items()}
        n = 0
        for p in rd:
            if len(p) <= i_stay:
                continue
            stay = p[i_stay].strip().replace('-', '')[:8]
            if len(stay) != 8 or not stay.isdigit():
                continue
            ym = stay[:6]
            if min_ym and ym < min_ym:
                continue
            if max_ym and ym > max_ym:
                continue
            g = lambda i: p[i].strip() if 0 <= i < len(p) else ''
            pkg_parts = {}
            for k, ii in parts_idx.items():
                v = sum(_num(p[i]) for i in ii if i < len(p))
                if v:
                    pkg_parts[k] = v
            yield {
                'key': g(i_key), 'stay': stay, 'prop_raw': g(i_prop) or g(i_prop2), 'code': g(i_code), 'name': g(i_name),
                'roomtype': g(i_rt), 'season': g(i_season), 'rate': _num(g(i_rate)),
                'nights': int(_num(g(i_nights)) or 1), 'rooms': int(_num(g(i_rooms)) or 1),
                'booked': g(i_booked).replace('-', '')[:8], 'agent': g(i_agent), 'seg_code': g(i_seg),
                'pkg_room': _num(g(i_pr)), 'pkg_total': _num(g(i_pt)), 'pkg_parts': pkg_parts,
                'sale_price': _num(g(i_sale)), 'deposit': _num(g(i_dep)), 'comm': _num(g(i_comm)), 'pkg_class': g(i_pc),
            }
            n += 1
            if max_rows and n >= max_rows:
                return


def _ymd(s):
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except Exception:
        return None


class CubeBuilder:
    def __init__(self):
        self.codes = {}
        self.baseline = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: [0, 0.0])))  # prop→year→season→[rn,rev]
        self.rows_parsed = 0
        self._seen = set()  # (code, key) 구성가치·예약건수·리드타임 1회 가산용

    def _code_node(self, code, r, prop):
        c = self.codes.get(code)
        if c is None:
            c = self.codes[code] = {'name': r['name'], 'prop': prop, 'by_ym': {}, 'rt': defaultdict(lambda: [0, 0.0]),
                                    'ag': defaultdict(lambda: [0, 0.0]), 'seg_counts': Counter(), 'chan_counts': Counter(),
                                    'comm_ratios': [], 'parts': Counter(), 'pkg_class': Counter(),
                                    'sale_days': Counter(), 'stay_days': Counter(), 'names': Counter()}
        return c

    def add(self, r):
        self.rows_parsed += 1
        prop = pn.prop_display(r['prop_raw'])
        if prop is None:
            return
        seg, chan = pn.channel_class(r['agent'], r['seg_code'], r['name'])
        rn = r['rooms']
        rev = r['rate'] * rn / 1.1
        year, ym = r['stay'][:4], r['stay'][:6]
        if r['seg_code'] in GS_CODES:
            b = self.baseline[prop][year][r['season'] or '00']
            b[0] += rn; b[1] += rev
        if not r['code'].startswith('86'):
            return
        c = self._code_node(r['code'], r, prop)
        c['names'][r['name']] += 1
        node = c['by_ym'].get(ym)
        if node is None:
            node = c['by_ym'][ym] = {'s': {}, 'dow': [0] * 7, 'lead_list': []}
        s = node['s'].setdefault(r['season'] or '00', [0, 0.0, 0.0, 0.0, 0])
        s[0] += rn; s[1] += rev; s[3] += r['comm'] / 1.1
        d = _ymd(r['stay'])
        if d:
            node['dow'][(d.weekday() + 1) % 7] += rn   # 0=일 … 6=토
        k = (r['code'], r['key'] or (r['stay'] + r['booked'] + r['roomtype']))
        if k not in self._seen:
            self._seen.add(k)
            s[4] += 1
            if r['pkg_total'] > r['pkg_room'] > 0 or (r['pkg_total'] > 0 and r['pkg_room'] == 0 and r['pkg_total'] > r['rate'] * r['nights']):
                room_part = r['pkg_room'] if r['pkg_room'] > 0 else r['rate'] * r['nights']
                s[2] += max(r['pkg_total'] - room_part, 0) / 1.1
            for pk, pv in r['pkg_parts'].items():
                c['parts'][pk] += pv
            bd = _ymd(r['booked'])
            if bd and d:
                node['lead_list'].append(max((d - bd).days, 0))
            if r['pkg_total'] > 0 and r['comm'] > 0:
                c['comm_ratios'].append(r['comm'] / r['pkg_total'])
            c['sale_days'][r['booked'][:8]] += 1
        c['stay_days'][r['stay']] += rn
        c['rt'][r['roomtype']][0] += rn; c['rt'][r['roomtype']][1] += rev
        c['ag'][chan][0] += rn; c['ag'][chan][1] += rev
        c['seg_counts'][seg] += 1
        c['chan_counts'][chan] += 1
        if r['pkg_class']:
            c['pkg_class'][r['pkg_class']] += 1
```

(`finalize`·`write_outputs`·`main`은 Task 3에서 추가. 이 단계에서는 파일 끝에 아래 임시 main만 둔다.)

```python
if __name__ == '__main__':
    print('finalize는 Task 3에서 구현')
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && /usr/local/bin/python3.12 -m unittest scripts.tests.test_product_cube -v 2>&1 | tail -12`
Expected: `OK` (4 tests). `dow` 기대값 `[2,0,0,0,0,1,1]`: 10/12(일) 2실, 10/10(금) 1, 10/11(토) 1.

- [ ] **Step 5: 커밋**

```bash
cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp
git add scripts/build_product_cube.py scripts/tests/test_product_cube.py
git commit -m "feat(product-cube): 27 파일 선택·행 파서·코드별 집계기

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -- scripts/build_product_cube.py scripts/tests/test_product_cube.py
```

---

### Task 3: 기획전 맵 생성 + finalize(기획전·구성·윈도우·베이스라인) + JSON/GZ 저장 + CLI

**Files:**
- Create: `scripts/make_campaign_map_from_trackers.py`, `data/product_campaign_map.json`
- Modify: `scripts/build_product_cube.py` (finalize/write_outputs/main 추가)
- Test: `scripts/tests/test_product_cube.py` (finalize 테스트 추가)

**Interfaces:**
- Consumes: Task 2 `CubeBuilder`, Task 1 `load_campaign_map`/`assign_campaign`/`classify_v4`
- Produces:
  - `finalize(builder: CubeBuilder, cmap: dict, years: list[int], base_date: str) -> dict` — 스펙 §4.1 스키마
  - `write_outputs(cube: dict, out_path: Path) -> tuple[int,int]` — (json bytes, gz bytes)
  - CLI `main()`

- [ ] **Step 1: 트래커 코드로 기획전 맵 생성 스크립트 작성 (1회용)**

```python
#!/usr/bin/env python3.12
# scripts/make_campaign_map_from_trackers.py
"""트래커 xlsx(사우나PKG '패키지코드' 시트)에서 86코드를 읽어 data/product_campaign_map.json을 만든다. 1회 실행 후 JSON을 커밋."""
import json, sys, re
from pathlib import Path
from openpyxl import load_workbook

D = Path('/Users/chanminpark/Downloads/6_(GS) 가을프로모션_온(溫)PKG 최종실적')
out = Path(__file__).resolve().parent.parent / 'data' / 'product_campaign_map.json'

sauna_codes = []
wb = load_workbook(D / '(GS) 가을프로모션_(사우나PKG) 실적.xlsx', data_only=True)
ws = wb['패키지코드']
for r in ws.iter_rows(min_row=7, max_row=25, values_only=True):
    for v in r:
        if isinstance(v, (int, float)) and 86000000 <= int(v) <= 86999999:
            sauna_codes.append(str(int(v)))
        elif isinstance(v, str) and re.fullmatch(r'86\d{6}', v.strip()):
            sauna_codes.append(v.strip())
sauna_codes = sorted(set(sauna_codes))

doc = {
  "_note": "수기 기획전 매핑. codes(정확) > regex(연도 일치) 순. composition/sale/note는 매트릭스 표기용. 대외비·관리자용",
  "campaigns": [
    {"name": "2025 가을프로모션 온(溫)PKG", "year": 2025, "scope": "전사업장(양평·남해·해운대 제외)",
     "match": {"codes": sauna_codes, "regex": r"온\(?溫\)?\s*가을|온가을|가을프로모션\s*사우나|사우나\s*PKG"},
     "composition": "객실+사우나2인+웰컴음료2잔 (비발디: 오션&찜질방+식혜 / 벨제주: 사우나+감귤와인)",
     "sale": "25.09.24~11.30", "stay": "25.10.01~11.30", "note": "음료 GS 예산 활용"},
    {"name": "2025-26 2연박PKG (2Nights Privilege)", "year": 2025, "scope": "전사업장",
     "match": {"codes": [], "regex": r"2\s*NIGHTS?\s*PRIVILEGE|2연박|2NIGHTS"},
     "composition": "객실 2연박+사업장별 부대시설+레드와인 1병+굿즈 2종(커피2+10% 할인권)",
     "sale": "25.12.02~26.01.14", "stay": "25.12~26.02", "note": "와인 GS 예산 활용"},
    {"name": "2026 신년 윈터키즈브릭PKG", "year": 2026, "scope": "비발디·양양·삼척·천안·르네블루·거제·진도",
     "match": {"codes": [], "regex": r"키즈브릭|KIDS\s*BRICK"},
     "composition": "객실+키즈박스(레고 or 플레이모빌) 1개", "sale": "26.01.05~01.29", "stay": "26.01~02",
     "note": "키즈박스 GS 예산 활용"},
    {"name": "2025 Kids Free 조식PKG (놀유니버스×소노위크)", "year": 2025, "scope": "전사업장(양평·고양·남해·해운대 제외)",
     "match": {"codes": [], "regex": r"KIDS\s*FREE|키즈\s*프리|소노위크"},
     "composition": "객실+조식(성인2+미취학아동1)", "sale": "25.11.04~11.08", "stay": "", "note": ""}
  ]
}
out.write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding='utf-8')
print('wrote', out, 'sauna codes', len(sauna_codes))
```

- [ ] **Step 2: 실행해 JSON 생성**

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && /usr/local/bin/python3.12 scripts/make_campaign_map_from_trackers.py && jq '.campaigns[0].match.codes|length' data/product_campaign_map.json`
Expected: `wrote … sauna codes 1xx` 와 jq 출력 ≥ 100 (19사업장 × 최대 7코드 중 공란 제외)

- [ ] **Step 3: finalize 테스트 추가** (`scripts/tests/test_product_cube.py` 끝의 `if __name__` 위에 추가)

```python
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
        # 2024 델피노 코드는 regex 규칙이 year=2025라 자동 기획전으로
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
        import gzip
        self.assertEqual(json.loads(gzip.open(out.with_suffix('.json.gz')).read())['meta']['x'], 1)
```

(테스트 파일 상단 import에 `import json` 과 `import product_naming as pn` 추가)

- [ ] **Step 4: 실패 확인**

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && /usr/local/bin/python3.12 -m unittest scripts.tests.test_product_cube.Finalize -v 2>&1 | tail -5`
Expected: `AttributeError: module 'build_product_cube' has no attribute 'finalize'`

- [ ] **Step 5: finalize / write_outputs / main 구현** (`build_product_cube.py`의 임시 main을 지우고 아래로 교체)

```python
def _pct(sorted_list, q):
    if not sorted_list:
        return None
    i = min(int(round((len(sorted_list) - 1) * q)), len(sorted_list) - 1)
    return sorted_list[i]


def _window(counter, lo=0.05, hi=0.95):
    """{'YYYYMMDD': n} → p05~p95 가중 분위 구간. 빈 경우 None."""
    items = sorted((d, n) for d, n in counter.items() if len(d) == 8 and d.isdigit())
    tot = sum(n for _, n in items)
    if not tot:
        return None
    acc, lo_d, hi_d = 0, items[0][0], items[-1][0]
    for d, n in items:
        acc += n
        if lo_d == items[0][0] and acc >= tot * lo:
            lo_d = d
        if acc >= tot * hi:
            hi_d = d; break
    return {'from': lo_d, 'to': hi_d}


def finalize(builder, cmap, years, base_date):
    codes_out, campaigns = {}, {}
    for code, c in builder.codes.items():
        name = c['names'].most_common(1)[0][0]
        seg = c['seg_counts'].most_common(1)[0][0] if c['seg_counts'] else '기타'
        chan = c['chan_counts'].most_common(1)[0][0] if c['chan_counts'] else seg
        sale_year = int(min(c['sale_days'])[:4]) if c['sale_days'] and min(c['sale_days']) else int(min(c['by_ym'])[:4])
        campaign = pn.assign_campaign(cmap, code, name, sale_year)
        clean = pn.clean_product_name(name)
        by_ym = {}
        for ym, node in sorted(c['by_ym'].items()):
            ll = sorted(node['lead_list'])
            by_ym[ym] = {'s': {k: [v[0], round(v[1]), round(v[2]), round(v[3]), v[4]] for k, v in sorted(node['s'].items())},
                         'dow': node['dow'],
                         'lead': {'p50': _pct(ll, 0.5), 'p90': _pct(ll, 0.9), 'mean': round(statistics.fmean(ll), 1) if ll else None}}
        rt = dict(sorted(((k, [v[0], round(v[1])]) for k, v in c['rt'].items()), key=lambda kv: -kv[1][0])[:8])
        ag = dict(sorted(((k, [v[0], round(v[1])]) for k, v in c['ag'].items()), key=lambda kv: -kv[1][0])[:8])
        codes_out[code] = {
            'name': name, 'series': clean, 'campaign': campaign, 'category': classify_v4(clean), 'prop': c['prop'],
            'seg': seg, 'chan': chan, 'comm_rate': round(statistics.median(c['comm_ratios']), 4) if c['comm_ratios'] else None,
            'pkg_class': c['pkg_class'].most_common(1)[0][0] if c['pkg_class'] else '',
            'by_ym': by_ym, 'rt': rt, 'ag': ag,
        }
        camp = campaigns.setdefault(campaign, {'codes': [], 'props': set(), 'cats': Counter(), 'by_year': defaultdict(lambda: [0, 0.0, 0.0, 0.0, 0, 0]),
                                               'sale_days': Counter(), 'stay_days': Counter(), 'parts': Counter()})
        camp['codes'].append(code); camp['props'].add(c['prop']); camp['cats'][codes_out[code]['category']] += 1
        camp['sale_days'].update(c['sale_days']); camp['stay_days'].update(c['stay_days']); camp['parts'].update(c['parts'])
        for ym, node in c['by_ym'].items():
            y = ym[:4]
            for sk, v in node['s'].items():
                b = camp['by_year'][y]
                b[0] += v[0]; b[1] += v[1]; b[2] += v[2]; b[3] += v[3]; b[4] += v[4]
                if sk in pn.WEEKDAY_SEASONS:
                    b[5] += v[0]
    camps_out = {}
    for name, camp in campaigns.items():
        meta = cmap['meta'].get(name, {})
        parts_tot = sum(camp['parts'].values())
        auto = {k: round(v / parts_tot, 3) for k, v in camp['parts'].most_common()} if parts_tot else {}
        by_year = {}
        for y, b in sorted(camp['by_year'].items()):
            by_year[y] = {'rn': b[0], 'rev': round(b[1]), 'comp': round(b[2]), 'comm': round(b[3]), 'bk': b[4],
                          'adr': round(b[1] / b[0]) if b[0] else 0, 'weekday_share': round(b[5] / b[0], 3) if b[0] else 0}
        camps_out[name] = {
            'codes': sorted(camp['codes']), 'props': sorted(camp['props'], key=lambda p: pn.PROP_ORDER.index(p) if p in pn.PROP_ORDER else 99),
            'category': camp['cats'].most_common(1)[0][0], 'year': meta.get('year'), 'scope': meta.get('scope'),
            'composition': {'auto': auto, 'text': meta.get('composition') or ''},
            'sale_window': _window(camp['sale_days']), 'stay_window': _window(camp['stay_days']),
            'sale_text': meta.get('sale') or '', 'note': meta.get('note') or '', 'by_year': by_year,
        }
    baseline = {p: {y: {s: {'rn': v[0], 'rev': round(v[1]), 'adr': round(v[1] / v[0]) if v[0] else 0} for s, v in ss.items()}
                    for y, ss in ys.items()} for p, ys in builder.baseline.items()}
    prop_alias = {}
    for _bi, disp, _reg, dbs in pn.PROPERTY_DEFS:
        for n in dbs:
            prop_alias[n] = disp
        prop_alias[disp] = disp
    return {
        'meta': {'built': datetime.now().strftime('%Y-%m-%d %H:%M'), 'years': list(years), 'base_date': base_date,
                 'rows_parsed': builder.rows_parsed, 'codes': len(codes_out), 'campaigns': len(camps_out),
                 'rules': {'rev': '1박객실료×객실수÷1.1', 'comp': '(PKG총−PKG객실)÷1.1 예약당 1회', 'src': '27 파일(유효 예약)', 'weekday': '요금시즌 41(보조 11)'},
                 'label': '대외비·관리자용'},
        'dims': {'seasons': pn.SEASONS, 'gs_segs': list(pn.GS_SEGS),
                 'properties': [{'display': d[1], 'region': d[2]} for d in pn.PROPERTY_DEFS], 'prop_alias': prop_alias,
                 'categories': sorted({v['category'] for v in codes_out.values()})},
        'codes': codes_out, 'campaigns': camps_out, 'baseline': baseline,
    }


def write_outputs(cube, out_path):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    txt = json.dumps(cube, ensure_ascii=False, separators=(',', ':'))
    out_path.write_text(txt, encoding='utf-8')
    gz_path = out_path.with_suffix('.json.gz')
    with gzip.open(gz_path, 'wb', compresslevel=9) as f:
        f.write(txt.encode('utf-8'))
    return len(txt.encode('utf-8')), gz_path.stat().st_size


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--years', nargs='+', type=int, default=[2023, 2024, 2025, 2026])
    ap.add_argument('--raw-dir', default=str(ROOT / 'data' / 'raw_db'))
    ap.add_argument('--out', default=str(ROOT / 'docs' / 'data' / 'product_cube.json'))
    ap.add_argument('--campaign-map', default=str(ROOT / 'data' / 'product_campaign_map.json'))
    ap.add_argument('--max-rows', type=int, default=None)
    a = ap.parse_args()
    t0 = time.time()
    files = select_files(a.raw_dir, a.years)
    if not files:
        sys.exit('27 파일 없음')
    b = CubeBuilder()
    base_date = None
    for p, lo, hi in files:
        m = _SNAP_RE.search(p.name)
        if m:
            base_date = f'{m.group(1)[:4]}-{m.group(1)[4:6]}-{m.group(1)[6:]}'
        n0 = b.rows_parsed
        for r in iter_rows(p, lo, hi, a.max_rows):
            b.add(r)
        print(f'  {p.name[:60]:60s} [{lo or "-"}..{hi or "-"}] rows={b.rows_parsed - n0:,} ({time.time()-t0:.0f}s)')
    cube = finalize(b, pn.load_campaign_map(a.campaign_map), a.years, base_date or datetime.now().strftime('%Y-%m-%d'))
    n1, n2 = write_outputs(cube, a.out)
    print(f'codes={cube["meta"]["codes"]:,} campaigns={cube["meta"]["campaigns"]:,} json={n1/1e6:.1f}MB gz={n2/1e6:.1f}MB total {time.time()-t0:.0f}s')


if __name__ == '__main__':
    main()
```

- [ ] **Step 6: 테스트 통과 확인**

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && /usr/local/bin/python3.12 -m unittest discover -s scripts/tests -p 'test_product_*.py' -v 2>&1 | tail -15`
Expected: `OK` (naming 7 + cube 6)

- [ ] **Step 7: 실제 빌드 (2025·2026 먼저, 소요시간 확인)**

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && time /usr/local/bin/python3.12 scripts/build_product_cube.py --years 2025 2026 && ls -la docs/data/product_cube.json*`
Expected: 두 파일 로그(2026은 재전송 `[-..202610]` + 스냅샷 `[202611..-]`), `codes=수천`, gz 수 MB 이하. 1분 넘게 걸리면 보고만 하고 진행.

- [ ] **Step 8: 앵커 즉석 확인**

Run:
```bash
cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && jq -c '.codes["86150059"] | {name, campaign, prop, seg, chan, comm_rate, oct: .by_ym["202510"].s}' docs/data/product_cube.json && jq -c '.campaigns["2025 가을프로모션 온(溫)PKG"] | {n:(.codes|length), props:(.props|length), by_year, sale_window, stay_window}' docs/data/product_cube.json
```
Expected: 86150059 10월 시즌 합 RN = **126**, rev 합 = **18,457,273**(=20,303,000÷1.1). 기획전 by_year 2025 rn ≈ **5,476**(±1%), rev ≈ 987,000,000(±1.5%). 어긋나면 코드 매핑(맵 JSON의 codes)과 월 범위를 먼저 의심한다.

- [ ] **Step 9: 전체 연도 빌드**

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && time /usr/local/bin/python3.12 scripts/build_product_cube.py && ls -la docs/data/product_cube.json*`
Expected: 4개년 완료, gz 크기 기록. json이 20MB를 넘으면 `rt`/`ag` 상위 N을 5로 줄여 재빌드.

- [ ] **Step 10: 커밋** (큐브 json은 .gz만 추적할지 결정: gz ≤ 10MB면 둘 다, 아니면 `.gitignore`에 `docs/data/product_cube.json` 추가하고 gz만)

```bash
cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp
git add scripts/build_product_cube.py scripts/make_campaign_map_from_trackers.py data/product_campaign_map.json scripts/tests/test_product_cube.py docs/data/product_cube.json.gz docs/data/product_cube.json
git commit -m "feat(product-cube): finalize(기획전·구성·윈도우·베이스라인)+json/gz 산출, 기획전 맵, 4개년 빌드

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -- scripts/build_product_cube.py scripts/make_campaign_map_from_trackers.py data/product_campaign_map.json scripts/tests/test_product_cube.py docs/data/product_cube.json.gz docs/data/product_cube.json
```

---

### Task 4: 앵커 검증 테스트 (실제 산출물 대상)

**Files:**
- Create: `scripts/tests/test_product_cube_anchors.py`

**Interfaces:**
- Consumes: `docs/data/product_cube.json` (없으면 skip), `data/product_campaign_map.json`

- [ ] **Step 1: 테스트 작성** (기대값은 트래커에서 옮겨 적은 상수 — Downloads 파일을 읽지 않는다)

```python
# scripts/tests/test_product_cube_anchors.py
"""실제 큐브 산출물 앵커 검증. 기대값 = 실적 트래커 수치(상수). 산출물 없으면 skip."""
import json, os, unittest
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
                rn += v[0]; rev += v[1]
    return rn, rev


def _sum_campaign_regex(cube, pattern, yms):
    import re
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
        self.assertEqual(rev, round(20303000 / 1.1))

    def test_on_pkg_total_oct_nov_2025(self):
        codes = next(c for c in self.cmap['campaigns'] if c['name'].startswith('2025 가을프로모션'))['match']['codes']
        rn, rev = _sum_codes(self.cube, codes, ['202510', '202511'])
        self.assertAlmostEqual(rn, 5476, delta=5476 * 0.01)
        self.assertAlmostEqual(rev / 1e6, 987, delta=987 * 0.015)

    def test_two_nights_privilege_dec_feb(self):
        (rn, rev), n = _sum_campaign_regex(self.cube, r'2\s*NIGHTS?\s*PRIVILEGE|2연박', ['202512', '202601', '202602'])
        self.assertGreater(n, 20)
        self.assertGreaterEqual(rn, 11972 * 0.97)      # 1/26 스냅샷 이후 2월 추가 예약 가능 → 하한만
        self.assertLessEqual(rn, 11972 * 1.15)
        self.assertGreaterEqual(rev / 1e6, 2395 * 0.95)

    def test_kids_brick_jan_feb_2026(self):
        (rn, rev), n = _sum_campaign_regex(self.cube, r'키즈브릭|KIDS\s*BRICK', ['202601', '202602'])
        self.assertGreater(n, 5)
        self.assertGreaterEqual(rn, 1558 * 0.97)
        self.assertLessEqual(rn, 1558 * 1.15)

    def test_product_detail_consistency_by_year(self):
        """큐브 연도 RN 합 vs 기존 product_detail(by_category) 연도 합 ±2% (같은 86·27 필터)."""
        agg = ROOT / 'data' / 'db_aggregated.json'
        if not agg.exists():
            self.skipTest('db_aggregated 없음')
        pd = json.loads(agg.read_text(encoding='utf-8')).get('product_detail', {})
        for y in ('2024', '2025'):
            ref = sum(v.get('rn', 0) for cat in pd.get('by_category', {}).values() for ym, v in cat.get(y, {}).items())
            if not ref:
                continue
            mine = sum(v[0] for c in self.cube['codes'].values() if c['seg'] in ('OTA', 'G-OTA', 'Inbound')
                       for ym, node in c['by_ym'].items() if ym.startswith(y) for v in node['s'].values())
            self.assertAlmostEqual(mine, ref, delta=ref * 0.02, msg=f'{y}: cube={mine} product_detail={ref}')


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: 실행**

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && /usr/local/bin/python3.12 -m unittest scripts.tests.test_product_cube_anchors -v 2>&1 | tail -15`
Expected: 5 tests OK. 실패 시 **원인을 숫자로 기록**(어느 코드/월에서 차이)하고 빌더 또는 맵을 고친다. `test_product_detail_consistency_by_year`가 2% 밖이면 product_detail이 제외한 사업장(팔라티움 등)·세그 차이를 확인해 허용치를 근거와 함께 조정한다(기대값을 맞추려고 큐브를 왜곡하지 않는다).

- [ ] **Step 3: 커밋**

```bash
cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp
git add scripts/tests/test_product_cube_anchors.py
git commit -m "test(product-cube): 트래커 앵커·product_detail 정합 검증

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -- scripts/tests/test_product_cube_anchors.py
```

---

### Task 5: 페이지 — 큐브 로더 + SEC8 상품 드릴(뎁스 6)

**Files:**
- Modify: `docs/product-detail.html` — `<head>`에 SheetJS 추가(Task 6에서 사용), SEC7 뒤에 SEC8 마크업, IIFE 안에 로더·드릴 JS, CSS 몇 줄

**Interfaces:**
- Consumes: `docs/data/product_cube.json(.gz)` 스키마(Task 3), 페이지 상태 `selYear`(string), `activePropsSet`(db명 Set), `fmtN`, `el`
- Produces: 전역 `window.__cube`(로드 후), 함수 `cubeFilterCodes()`, `drillRows(level, path)`, `renderDrill()`; Task 6이 `cubeFilterCodes`와 `__cube.campaigns`를 사용

- [ ] **Step 1: head에 SheetJS 로드 추가** (Chart.js 로드 줄 바로 아래)

```html
<script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>
```

- [ ] **Step 2: CSS 추가** (`table.pivot tr.total-row td{…}` 줄 아래)

```css
.drill-bar{display:flex;flex-wrap:wrap;gap:6px;align-items:center;padding:10px 20px;border-bottom:1px solid var(--rule);font-family:var(--mono);font-size:11px;}
.drill-crumb{padding:3px 8px;border-radius:3px;border:1px solid var(--rule-strong);color:var(--ink-muted);cursor:pointer;background:transparent;}
.drill-crumb.cur{border-color:var(--gold,#c9a063);color:var(--ink);}
.drill-sep{color:var(--ink-faint);}
table.pivot td.drill-name{cursor:pointer;text-decoration:underline dotted;text-underline-offset:3px;}
table.pivot td.drill-name:hover{color:var(--gold,#c9a063);}
.dowbar{display:inline-grid;grid-template-columns:repeat(7,10px);gap:1px;vertical-align:middle;margin-left:6px;}
.dowbar i{display:block;height:10px;background:var(--rule-strong);border-radius:1px;}
.dowbar i.on{background:var(--gold,#c9a063);}
.yoy-up{color:#7fd8a0;} .yoy-dn{color:#ef9a9a;} .yoy-na{color:var(--ink-faint);}
.seg-toggle{display:flex;gap:4px;}
```

- [ ] **Step 3: SEC8 마크업 추가** (SEC7 `</section>` 바로 뒤)

```html
    <!-- SEC 8: 상품 드릴 (뎁스 6) -->
    <section class="prod-section" id="sec8">
      <div class="section-header">
        <span class="section-num">SEC 8</span>
        <h2 class="section-title">상품 드릴 — 사업장 → 카테고리 → 기획전 → 상품코드 → 투숙월·요금시즌 → 객실타입</h2>
        <span class="section-sub" id="sec8-sub">큐브 로딩 중…</span>
      </div>
      <div class="table-card">
        <div class="drill-bar" id="drill-bar"></div>
        <div class="drill-bar" style="border-bottom:0;">
          <span class="drill-sep">세그</span>
          <div class="seg-toggle" id="drill-seg"></div>
          <span class="drill-sep" style="margin-left:14px;">주중 = 요금시즌 41(비수기 주중) · 구성가치 = 패키지 총액 − 객실분 · 대외비·관리자용</span>
        </div>
        <div class="table-wrap">
          <table class="pivot" id="drill-table">
            <thead><tr>
              <th>항목</th><th>RN</th><th>ADR(천)</th><th>객실매출(백만)</th><th>구성가치/예약(천)</th><th>수수료율</th>
              <th>주중비중</th><th>전년비 RN</th><th>리드타임 p50</th><th>요일</th>
            </tr></thead>
            <tbody id="drill-tbody"><tr><td colspan="10" style="text-align:center;padding:24px;color:var(--ink-faint);">—</td></tr></tbody>
          </table>
        </div>
      </div>
    </section>
```

- [ ] **Step 4: JS — 로더·필터·드릴** (IIFE 안, `function buildFilters()` 위에 추가)

```javascript
  // ===== 상품 큐브 (product_cube.json.gz → DecompressionStream, 실패 시 .json) =====
  var cube = null;
  var drillPath = [];                 // [{level, key, label}]
  var drillSegs = ['OTA', 'G-OTA', 'Inbound'];
  var SEASON_ORDER = ['41', '43', '42', '11', '14', '12', '13', '30', '00'];
  var DOW_LABEL = ['일', '월', '화', '수', '목', '금', '토'];

  function fetchCube() {
    var base = 'data/product_cube.json';
    var plain = function() { return fetch(base + '?t=' + Date.now()).then(function(r) { if (!r.ok) throw new Error('cube ' + r.status); return r.json(); }); };
    if (!window.DecompressionStream) return plain();
    return fetch(base + '.gz?t=' + Date.now()).then(function(r) {
      if (!r.ok) throw new Error('gz ' + r.status);
      return new Response(r.body.pipeThrough(new DecompressionStream('gzip'))).json();
    }).catch(plain);
  }

  function cubePropOf(dbName) { return cube && cube.dims.prop_alias[dbName] || null; }
  function activeDisplayProps() {
    var out = new Set();
    activePropsSet.forEach(function(p) { var d = cubePropOf(p); if (d) out.add(d); });
    return out;
  }
  // 현재 필터(연도=selYear, 사업장, 세그)에 맞는 [code, node, ymNodeMap] 목록. year 인자로 전년 계산 재사용.
  function cubeFilterCodes(year) {
    var props = activeDisplayProps(), out = [];
    if (!cube) return out;
    Object.keys(cube.codes).forEach(function(code) {
      var c = cube.codes[code];
      if (props.size && !props.has(c.prop)) return;
      if (drillSegs.indexOf(c.seg) === -1) return;
      var yms = {};
      Object.keys(c.by_ym).forEach(function(ym) { if (ym.slice(0, 4) === String(year)) yms[ym] = c.by_ym[ym]; });
      if (Object.keys(yms).length) out.push({code: code, c: c, yms: yms});
    });
    return out;
  }
  function emptyAgg() { return {rn: 0, rev: 0, comp: 0, comm: 0, bk: 0, wk: 0, dow: [0,0,0,0,0,0,0], leads: []}; }
  function addNode(a, node, seasonKey) {
    Object.keys(node.s).forEach(function(sk) {
      if (seasonKey && sk !== seasonKey) return;
      var v = node.s[sk]; a.rn += v[0]; a.rev += v[1]; a.comp += v[2]; a.comm += v[3]; a.bk += v[4];
      if (sk === '41' || sk === '11') a.wk += v[0];
    });
    if (!seasonKey) { for (var i = 0; i < 7; i++) a.dow[i] += node.dow[i]; if (node.lead && node.lead.p50 != null) a.leads.push(node.lead.p50); }
  }
  // 레벨별 그룹 키. path = [prop, category, campaign, code, ym|season] 순으로 쌓임
  function groupKeyFor(level, item, ym, sk) {
    switch (level) {
      case 0: return [item.c.prop, item.c.prop];
      case 1: return [item.c.category, item.c.category];
      case 2: return [item.c.campaign, item.c.campaign];
      case 3: return [item.code, item.c.series + ' · ' + item.c.chan + ' (' + item.code + ')'];
      case 4: return [ym + '|' + sk, ym.slice(0, 4) + '-' + ym.slice(4) + ' · ' + (cube.dims.seasons[sk] || '기타')];
    }
  }
  function matchesPath(item) {
    for (var i = 0; i < drillPath.length && i < 4; i++) {
      var p = drillPath[i];
      if (i === 0 && item.c.prop !== p.key) return false;
      if (i === 1 && item.c.category !== p.key) return false;
      if (i === 2 && item.c.campaign !== p.key) return false;
      if (i === 3 && item.code !== p.key) return false;
    }
    return true;
  }
  function drillRows(year) {
    var level = drillPath.length, rows = {}, items = cubeFilterCodes(year).filter(matchesPath);
    if (level === 5) {                       // 객실타입: 코드의 rt (연도 무관 누적) — 라벨에 명시
      var it = items[0]; if (!it) return [];
      return Object.keys(it.c.rt).map(function(rt) { var v = it.c.rt[rt]; return {key: rt, label: rt + ' (코드 누적)', rn: v[0], rev: v[1], comp: 0, comm: 0, bk: 0, wk: 0, dow: null, leads: [], leaf: true}; })
        .sort(function(a, b) { return b.rn - a.rn; });
    }
    items.forEach(function(item) {
      Object.keys(item.yms).forEach(function(ym) {
        var node = item.yms[ym];
        if (level === 4) {
          var ymSel = drillPath[3] ? null : null;
          Object.keys(node.s).forEach(function(sk) {
            var kl = groupKeyFor(4, item, ym, sk); var r = rows[kl[0]] || (rows[kl[0]] = Object.assign(emptyAgg(), {key: kl[0], label: kl[1], ord: SEASON_ORDER.indexOf(sk)}));
            addNode(r, node, sk);
          });
        } else {
          var kl2 = groupKeyFor(level, item); var r2 = rows[kl2[0]] || (rows[kl2[0]] = Object.assign(emptyAgg(), {key: kl2[0], label: kl2[1]}));
          addNode(r2, node, null);
        }
      });
    });
    var arr = Object.values(rows);
    if (level === 4) arr.sort(function(a, b) { return a.key.localeCompare(b.key) || a.ord - b.ord; });
    else if (level === 0) arr.sort(function(a, b) { return cube.dims.properties.findIndex(function(p){return p.display===a.key;}) - cube.dims.properties.findIndex(function(p){return p.display===b.key;}); });
    else arr.sort(function(a, b) { return b.rn - a.rn; });
    return arr;
  }
  function median(list) { if (!list.length) return null; var s = list.slice().sort(function(a,b){return a-b;}); return s[Math.floor(s.length/2)]; }
  function dowBar(d) {
    if (!d) return '';
    var mx = Math.max.apply(null, d) || 1;
    return '<span class="dowbar" title="' + d.map(function(v,i){return DOW_LABEL[i]+' '+v;}).join(' · ') + '">' + d.map(function(v){ return '<i style="opacity:' + (0.25 + 0.75*v/mx).toFixed(2) + '" class="' + (v===mx?'on':'') + '"></i>'; }).join('') + '</span>';
  }
  function yoyCell(cur, prev) {
    if (!prev) return '<span class="yoy-na">–</span>';
    var p = (cur / prev - 1) * 100;
    return '<span class="' + (p >= 0 ? 'yoy-up' : 'yoy-dn') + '">' + (p >= 0 ? '+' : '') + p.toFixed(1) + '%</span>';
  }
  function renderDrill() {
    if (!cube) return;
    var y = parseInt(selYear, 10), rows = drillRows(y), prevRows = {};
    drillRows(y - 1).forEach(function(r) { prevRows[r.key] = r; });
    var level = drillPath.length;
    var bar = el('drill-bar');
    bar.innerHTML = '<button class="drill-crumb' + (level === 0 ? ' cur' : '') + '" data-i="-1">전체</button>' +
      drillPath.map(function(p, i) { return '<span class="drill-sep">›</span><button class="drill-crumb' + (i === level - 1 ? ' cur' : '') + '" data-i="' + i + '">' + p.label + '</button>'; }).join('');
    bar.querySelectorAll('.drill-crumb').forEach(function(b) { b.onclick = function() { drillPath = drillPath.slice(0, parseInt(b.dataset.i, 10) + 1); renderDrill(); }; });
    var levelNames = ['사업장', '카테고리', '기획전', '상품코드·채널', '투숙월·요금시즌', '객실타입'];
    el('sec8-sub').textContent = selYear + '년 · ' + levelNames[level] + ' ' + rows.length + '행 · 큐브 ' + cube.meta.built;
    var tot = emptyAgg();
    rows.forEach(function(r) { tot.rn += r.rn; tot.rev += r.rev; tot.comp += r.comp; tot.comm += r.comm; tot.bk += r.bk; tot.wk += r.wk; });
    var html = rows.map(function(r) {
      var pv = prevRows[r.key];
      return '<tr><td class="' + (level < 5 ? 'drill-name' : '') + '" data-k="' + encodeURIComponent(r.key) + '" data-l="' + encodeURIComponent(r.label) + '">' + r.label + '</td>' +
        '<td>' + fmtN(r.rn) + '</td><td>' + (r.rn ? fmtN(r.rev / r.rn / 1000) : '–') + '</td><td>' + fmtR(r.rev / 1e6) + '</td>' +
        '<td>' + (r.bk ? fmtN(r.comp / r.bk / 1000) : '–') + '</td><td>' + (r.rev ? (r.comm / (r.rev + r.comp) * 100).toFixed(1) + '%' : '–') + '</td>' +
        '<td>' + (r.rn ? (r.wk / r.rn * 100).toFixed(0) + '%' : '–') + '</td><td>' + yoyCell(r.rn, pv ? pv.rn : 0) + '</td>' +
        '<td>' + (median(r.leads) != null ? median(r.leads) + '일' : '–') + '</td><td>' + dowBar(r.dow) + '</td></tr>';
    }).join('');
    html += '<tr class="total-row"><td>합계</td><td>' + fmtN(tot.rn) + '</td><td>' + (tot.rn ? fmtN(tot.rev / tot.rn / 1000) : '–') + '</td><td>' + fmtR(tot.rev / 1e6) + '</td>' +
      '<td>' + (tot.bk ? fmtN(tot.comp / tot.bk / 1000) : '–') + '</td><td>' + (tot.rev ? (tot.comm / (tot.rev + tot.comp) * 100).toFixed(1) + '%' : '–') + '</td>' +
      '<td>' + (tot.rn ? (tot.wk / tot.rn * 100).toFixed(0) + '%' : '–') + '</td><td></td><td></td><td></td></tr>';
    el('drill-tbody').innerHTML = html || '<tr><td colspan="10" style="text-align:center;padding:24px;color:var(--ink-faint);">데이터 없음</td></tr>';
    el('drill-tbody').querySelectorAll('td.drill-name').forEach(function(td) {
      td.onclick = function() { drillPath.push({key: decodeURIComponent(td.dataset.k), label: decodeURIComponent(td.dataset.l)}); renderDrill(); };
    });
    if (window.renderMatrix) window.renderMatrix();
  }
  function buildSegToggle() {
    var box = el('drill-seg'); box.innerHTML = '';
    [['OTA','OTA'],['G-OTA','G-OTA'],['Inbound','Inbound'],['회원','회원'],['D멤버스','D멤버스']].forEach(function(s) {
      var b = document.createElement('button'); b.className = 'chip' + (drillSegs.indexOf(s[0]) >= 0 ? ' active' : ''); b.textContent = s[1];
      b.onclick = function() { var i = drillSegs.indexOf(s[0]); if (i >= 0) drillSegs.splice(i, 1); else drillSegs.push(s[0]); b.classList.toggle('active'); drillPath = []; renderDrill(); };
      box.appendChild(b);
    });
  }
```

- [ ] **Step 5: 기존 `render()` 끝과 로더에 연결**

`function render() {` 본문 마지막 줄(기존 섹션 렌더 호출들 뒤)에 `if (cube) renderDrill();` 추가. 데이터 로딩 `Promise.all([...]).then(function(arr){ ... buildFilters(); render(); })` 안의 `render();` 뒤에:

```javascript
    fetchCube().then(function(c) { cube = c; window.__cube = c; buildSegToggle(); drillPath = []; renderDrill(); })
      .catch(function(e) { console.error('cube', e); el('sec8-sub').textContent = '큐브 로드 실패'; });
```

사업장 칩 클릭 핸들러(`activePropsSet.add/delete` 뒤에 `render()`를 부르는 곳 2군데)는 `render()`가 `renderDrill()`을 부르므로 추가 수정 없음. 단 사업장 변경 시 `drillPath = [];`가 필요하므로 `render()` 안의 호출을 `if (cube) { drillPath = drillPath.filter(function(p,i){return i>0 || activeDisplayProps().size===0 || activeDisplayProps().has(p.key);}); renderDrill(); }`로 둔다.

- [ ] **Step 6: 로컬 확인** (정적 서버 + executive 토큰 주입은 기존 방식: `python3 -m http.server 8799 -d docs` 후 pane에서 로컬 토큰 주입)

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && (lsof -i :8799 >/dev/null 2>&1 || (nohup /usr/local/bin/python3.12 -m http.server 8799 -d docs >/dev/null 2>&1 &)); sleep 1; curl -s -o /dev/null -w '%{http_code} %{size_download}\n' http://localhost:8799/data/product_cube.json.gz`
Expected: `200 <bytes>`. 브라우저 pane에서 `http://localhost:8799/product-detail.html` 열어 SEC8이 사업장 23행으로 뜨고, 클릭마다 레벨이 내려가며(6단계) 콘솔 에러 0. 2025 선택 후 `01.벨비발디 → 시즌패키지 → 2025 가을프로모션 온(溫)PKG → 86150059 → 2025-10 · 비수기 주중` 경로에서 RN 합이 10월 126과 맞는지 확인(시즌 행 합).

- [ ] **Step 7: 커밋**

```bash
cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp
git add docs/product-detail.html
git commit -m "feat(product-detail): SEC8 상품 드릴(뎁스6) + 상품 큐브 로더

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -- docs/product-detail.html
```

---

### Task 6: 페이지 — SEC9 사업장별 상품 분석 매트릭스 + 엑셀 다운로드

**Files:**
- Modify: `docs/product-detail.html` — SEC9 마크업, `renderMatrix()`, `exportMatrixXlsx()`

**Interfaces:**
- Consumes: `cube.campaigns`, `cube.codes`, `cubeFilterCodes`(세그 필터 공유), `activeDisplayProps()`, `selYear`
- Produces: `window.renderMatrix`, 파일 `GS_사업장별_상품분석_YYYYMMDD.xlsx`

- [ ] **Step 1: SEC9 마크업** (SEC8 `</section>` 뒤)

```html
    <!-- SEC 9: 사업장별 상품 분석 매트릭스 -->
    <section class="prod-section" id="sec9">
      <div class="section-header">
        <span class="section-num">SEC 9</span>
        <h2 class="section-title">사업장별 상품 분석 — 전략 상품 · 구성 · 판매일자 · 실적 (연도 비교)</h2>
        <span class="section-sub" id="sec9-sub">—</span>
      </div>
      <div class="table-card">
        <div class="table-card-header">
          <span class="table-card-title">기준연도 <span id="mx-y0">—</span> · 비교 <span id="mx-y1">—</span> · 다음해 <span id="mx-y2">—</span> 열은 기획(2차) · 행당 상위 <select id="mx-topn" class="chip"><option>3</option><option selected>5</option><option>10</option></select>개</span>
          <button class="chip" id="mx-xlsx">📥 엑셀 다운로드</button>
        </div>
        <div class="table-wrap">
          <table class="pivot" id="mx-table" style="font-size:11.5px;">
            <thead>
              <tr><th rowspan="2">구분</th><th colspan="3">전략 상품</th><th colspan="3">상품 구성</th><th colspan="3">판매일자</th><th colspan="6">판매 실적</th><th rowspan="2">비고</th></tr>
              <tr id="mx-h2"></tr>
            </thead>
            <tbody id="mx-tbody"></tbody>
          </table>
        </div>
      </div>
    </section>
```

- [ ] **Step 2: JS — 매트릭스 데이터·렌더·엑셀** (`buildSegToggle` 아래)

```javascript
  // ===== SEC9 매트릭스 =====
  function fmtWin(w) { if (!w) return ''; var f = w.from, t = w.to; return f.slice(2,4) + '.' + f.slice(4,6) + '.' + f.slice(6) + '~' + (t.slice(0,4) === f.slice(0,4) ? '' : t.slice(2,4) + '.') + t.slice(4,6) + '.' + t.slice(6); }
  function compText(camp) {
    if (camp.composition.text) return camp.composition.text;
    var a = camp.composition.auto, ks = Object.keys(a);
    return ks.length ? '객실+' + ks.map(function(k){ return k + ' ' + Math.round(a[k]*100) + '%'; }).join('·') : '객실';
  }
  // 연도 y의 기획전을 행(전사업장|사업장 display)별로 모아 RN 내림차순 상위 N
  function matrixRows(y, topN) {
    var props = activeDisplayProps(), segOK = function(camp) { return camp.codes.some(function(code){ var c = cube.codes[code]; return c && drillSegs.indexOf(c.seg) >= 0; }); };
    var rows = {'전사업장': []};
    cube.dims.properties.forEach(function(p) { if (!props.size || props.has(p.display)) rows[p.display] = []; });
    Object.keys(cube.campaigns).forEach(function(name) {
      var camp = cube.campaigns[name], by = camp.by_year[String(y)];
      if (!by || !by.rn || !segOK(camp)) return;
      var item = {name: name, comp: compText(camp), sale: camp.sale_text || fmtWin(camp.sale_window), rn: by.rn, rev: by.rev, adr: by.adr, note: camp.note || '', props: camp.props};
      if (camp.props.length >= 10) { rows['전사업장'].push(item); return; }
      // 사업장별: 해당 사업장 코드만 합산
      camp.props.forEach(function(p) {
        if (!rows[p]) return;
        var rn = 0, rev = 0;
        camp.codes.forEach(function(code) { var c = cube.codes[code]; if (!c || c.prop !== p || drillSegs.indexOf(c.seg) < 0) return;
          Object.keys(c.by_ym).forEach(function(ym) { if (ym.slice(0,4) !== String(y)) return; Object.values(c.by_ym[ym].s).forEach(function(v){ rn += v[0]; rev += v[1]; }); }); });
        if (rn) rows[p].push(Object.assign({}, item, {rn: rn, rev: rev}));
      });
    });
    Object.keys(rows).forEach(function(k) { rows[k].sort(function(a,b){return b.rn-a.rn;}); rows[k] = rows[k].slice(0, topN); });
    return rows;
  }
  function renderMatrix() {
    if (!cube) return;
    var y0 = parseInt(selYear, 10), y1 = y0 - 1, y2 = y0 + 1, topN = parseInt(el('mx-topn').value, 10);
    el('mx-y0').textContent = y0; el('mx-y1').textContent = y1; el('mx-y2').textContent = y2;
    el('mx-h2').innerHTML = [y2, y0, y1].map(function(y){return '<th>' + y + 'Y</th>';}).join('') + [y2, y0, y1].map(function(y){return '<th>' + y + 'Y</th>';}).join('') +
      [y2, y0, y1].map(function(y){return '<th>' + y + 'Y</th>';}).join('') + [y2, y0, y1].map(function(y){return '<th>' + y + 'Y RNs</th><th>' + y + 'Y REV(백만)</th>';}).join('');
    var r0 = matrixRows(y0, topN), r1 = matrixRows(y1, topN), html = '', n = 0;
    Object.keys(r0).forEach(function(group) {
      var a = r0[group], b = r1[group], len = Math.max(a.length, b.length);
      if (!len) return;
      for (var i = 0; i < len; i++) {
        var x = a[i], z = b[i]; n++;
        html += '<tr>' + (i === 0 ? '<td rowspan="' + len + '" style="vertical-align:top;">' + group + '</td>' : '') +
          '<td style="color:var(--ink-faint);">(기획)</td><td style="white-space:normal;min-width:160px;">' + (x ? x.name : '') + '</td><td style="white-space:normal;min-width:160px;">' + (z ? z.name : '') + '</td>' +
          '<td></td><td style="white-space:normal;min-width:180px;">' + (x ? x.comp : '') + '</td><td style="white-space:normal;min-width:180px;">' + (z ? z.comp : '') + '</td>' +
          '<td></td><td>' + (x ? x.sale : '') + '</td><td>' + (z ? z.sale : '') + '</td>' +
          '<td></td><td></td><td>' + (x ? fmtN(x.rn) : '') + '</td><td>' + (x ? fmtR(x.rev/1e6) : '') + '</td><td>' + (z ? fmtN(z.rn) : '') + '</td><td>' + (z ? fmtR(z.rev/1e6) : '') + '</td>' +
          '<td style="white-space:normal;">' + ((x && x.note) || (z && z.note) || '') + '</td></tr>';
      }
    });
    el('mx-tbody').innerHTML = html || '<tr><td colspan="17" style="text-align:center;padding:24px;color:var(--ink-faint);">데이터 없음</td></tr>';
    el('sec9-sub').textContent = n + '행 · 세그 ' + drillSegs.join('/') + ' · 2025Y·2026Y 자동, ' + y2 + 'Y 기획 열(2차) · 대외비·관리자용';
  }
  window.renderMatrix = renderMatrix;
  function exportMatrixXlsx() {
    if (!cube || typeof XLSX === 'undefined') return;
    var y0 = parseInt(selYear, 10), y1 = y0 - 1, y2 = y0 + 1, topN = parseInt(el('mx-topn').value, 10);
    var r0 = matrixRows(y0, topN), r1 = matrixRows(y1, topN);
    var head1 = ['구분', '전략 상품', '', '', '상품 구성', '', '', '판매일자', '', '', '판매 실적', '', '', '', '', '', '비고'];
    var head2 = ['', y2+'Y', y0+'Y', y1+'Y', y2+'Y', y0+'Y', y1+'Y', y2+'Y', y0+'Y', y1+'Y', y2+'Y RNs', y2+'Y REV', y0+'Y RNs', y0+'Y REV', y1+'Y RNs', y1+'Y REV', ''];
    var aoa = [['(GS) ' + y2 + '년 영업활성화 전략 — 사업장별 상품 분석 (대외비·관리자용)'], ['기준 ' + cube.meta.base_date + ' · REV 백만원 VAT제외'], [], head1, head2];
    var merges = [{s:{r:3,c:0},e:{r:4,c:0}}, {s:{r:3,c:1},e:{r:3,c:3}}, {s:{r:3,c:4},e:{r:3,c:6}}, {s:{r:3,c:7},e:{r:3,c:9}}, {s:{r:3,c:10},e:{r:3,c:15}}, {s:{r:3,c:16},e:{r:4,c:16}}];
    Object.keys(r0).forEach(function(group) {
      var a = r0[group], b = r1[group], len = Math.max(a.length, b.length), start = aoa.length;
      for (var i = 0; i < len; i++) { var x = a[i], z = b[i];
        aoa.push([i === 0 ? group : '', '', x ? x.name : '', z ? z.name : '', '', x ? x.comp : '', z ? z.comp : '', '', x ? x.sale : '', z ? z.sale : '',
                  '', '', x ? x.rn : '', x ? Math.round(x.rev/1e6*10)/10 : '', z ? z.rn : '', z ? Math.round(z.rev/1e6*10)/10 : '', (x && x.note) || (z && z.note) || '']); }
      if (len > 1) merges.push({s:{r:start,c:0},e:{r:start+len-1,c:0}});
    });
    var ws = XLSX.utils.aoa_to_sheet(aoa); ws['!merges'] = merges;
    ws['!cols'] = [{wch:12},{wch:18},{wch:30},{wch:30},{wch:18},{wch:40},{wch:40},{wch:14},{wch:16},{wch:16},{wch:9},{wch:10},{wch:9},{wch:10},{wch:9},{wch:10},{wch:22}];
    // 상세 시트: 기획전 × 사업장 × 연도
    var det = [['기획전', '사업장', '연도', 'RNs', 'REV(백만)', 'ADR(천)', '구성가치/예약(천)', '수수료(백만)', '주중비중', '판매기간', '투숙기간', '구성', '비고']];
    Object.keys(cube.campaigns).forEach(function(name) { var camp = cube.campaigns[name];
      Object.keys(camp.by_year).forEach(function(y) { var b = camp.by_year[y]; if (!b.rn) return;
        det.push([name, camp.props.join(', '), y, b.rn, Math.round(b.rev/1e6*10)/10, Math.round(b.adr/1000), b.bk ? Math.round(b.comp/b.bk/1000) : '', Math.round(b.comm/1e6*10)/10, Math.round(b.weekday_share*100) + '%', fmtWin(camp.sale_window), fmtWin(camp.stay_window), compText(camp), camp.note || '']); }); });
    var wb = XLSX.utils.book_new(); XLSX.utils.book_append_sheet(wb, ws, '사업장별 상품 분석'); XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(det), '기획전 상세');
    var d = new Date(), tag = d.getFullYear() + ('0' + (d.getMonth()+1)).slice(-2) + ('0' + d.getDate()).slice(-2);
    XLSX.writeFile(wb, 'GS_사업장별_상품분석_' + tag + '.xlsx');
  }
```

로더 `.then(function(c){ … renderDrill(); })` 안에 `el('mx-xlsx').onclick = exportMatrixXlsx; el('mx-topn').onchange = renderMatrix;` 추가. (`renderDrill` 끝에서 `window.renderMatrix()`를 이미 부른다.)

- [ ] **Step 3: 로컬 확인**

브라우저 pane `http://localhost:8799/product-detail.html`(토큰 유지): 2026 선택 → SEC9에 `2026Y`(온PKG·2연박·키즈브릭 등)와 `2025Y` 열이 채워지고 `2027Y` 열은 비어 있음. 비발디 사업장 선택 시 전사업장 행 + 비발디 행만. `📥 엑셀 다운로드` 클릭 → 파일 저장되고 열어서 헤더 2단 병합·행 수 일치 확인(다운로드 폴더의 파일을 openpyxl로 `ws.merged_cells.ranges` 6+개 확인). 콘솔 에러 0.

Run (다운로드 파일 검사):
```bash
cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && /usr/local/bin/python3.12 -I - <<'EOF'
import glob, os
from openpyxl import load_workbook
f = sorted(glob.glob(os.path.expanduser('~/Downloads/GS_사업장별_상품분석_*.xlsx')), key=os.path.getmtime)[-1]
wb = load_workbook(f); ws = wb.worksheets[0]
print(f, ws.max_row, ws.max_column, len(ws.merged_cells.ranges), wb.sheetnames)
print([c.value for c in ws[4]][:8]); print([c.value for c in ws[6]][:8])
EOF
```
Expected: 열 17, 병합 ≥ 6, 시트 2개, 4행 헤더가 `['구분','전략 상품',None,None,'상품 구성',…]`

- [ ] **Step 4: 모바일 폭 확인**

pane `resize_window` mobile → SEC8/SEC9 표가 `table-wrap` 가로 스크롤로 보이고 레이아웃 깨짐 없음 → desktop 복귀.

- [ ] **Step 5: 커밋**

```bash
cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp
git add docs/product-detail.html
git commit -m "feat(product-detail): SEC9 사업장별 상품 분석 매트릭스 + 엑셀 다운로드

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" -- docs/product-detail.html
```

---

### Task 7: 마무리 검증 · 금지어 스윕 · 스크린샷 보고 · (승인 후) 배포

**Files:**
- 변경 없음(검증) / 배포 시 push

- [ ] **Step 1: 전체 테스트**

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && /usr/local/bin/python3.12 -m unittest discover -s scripts/tests -p 'test_product_*.py' 2>&1 | tail -4`
Expected: `OK`

- [ ] **Step 2: 금지어 스윕** (페이지·큐브 메타·맵 JSON)

Run: `cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp && grep -nE '출처|주간회의|주간보고|발췌|PDF|원본|이미지화' docs/product-detail.html data/product_campaign_map.json scripts/build_product_cube.py | grep -v '^\s*#' | head`
Expected: 출력 없음(기존 문구에 있던 것은 보고 후 판단)

- [ ] **Step 3: 스크린샷 3장** — SEC8 레벨 0(사업장), 레벨 4(투숙월·시즌), SEC9 매트릭스 — pane `screenshot`으로 사용자에게 보고. 사용자 "배포" 전까지 push 금지.

- [ ] **Step 4: (승인 후) 배포**

```bash
cd /Users/chanminpark/Projects/gs_daily_trend_news_public_temp
git stash -u -q 2>/dev/null; git fetch -q origin && git rebase -q origin/main; git stash pop -q 2>/dev/null; git push origin main && git log --oneline -1
```
Expected: push 성공 → GitHub Pages 자동 배포. 라이브 `https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/product-detail.html`에서 SEC8/SEC9 확인(로그인 필요).

- [ ] **Step 5: 메모리 갱신** — `memory/` 에 `product-cube-phase1.md`(빌더 경로·재빌드 명령·앵커 수치·기획전 맵 위치·비정본/정본 여부·2차 TODO) 작성 + `MEMORY.md` 한 줄.
