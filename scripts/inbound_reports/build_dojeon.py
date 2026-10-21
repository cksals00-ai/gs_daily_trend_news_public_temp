#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""★★도전목표_2026_전략방안 — 매일 실적(인바운드 온북, raw_db 코드58) 반영.

산출물 3종:
  1) docs/inbound-dojeon.html  의 inline `const D=...,PROPS=[...]`  (달성률 대시보드)
  2) docs/dojeon_strategy_actual.xlsx                              (대시보드 다운로드 버튼용, 호스팅)
  3) ~/Downloads/★★도전목표_2026_전략방안_실적반영.xlsx            (사용자 보고용 사본)

원본 양식(총괄+18사업장)에 「2026 Actual」열을 채우며, 템플릿은 피벗을 값으로 치환한 경량본.
build.py/run.sh 에서 호출하면 매일 자동 갱신된다.
"""
import os, re, sys, json, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _common as C
import openpyxl, warnings; warnings.filterwarnings('ignore')

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
TEMPLATE = os.path.join(HERE, 'templates', 'dojeon_2026.xlsx')
TARGETS  = json.load(open(os.path.join(HERE, 'dojeon_targets.json'), encoding='utf-8'))
DOCS_HTML = os.path.join(REPO, 'docs', 'inbound-dojeon.html')
DOCS_XLSX = os.path.join(REPO, 'docs', 'dojeon_strategy_actual.xlsx')
DL_XLSX   = os.path.expanduser('~/Downloads/★★도전목표_2026_전략방안_실적반영.xlsx')
YEAR = 2026

# ── 사업장 시트/열 레이아웃 (고양 구조와 동일) ──────────────────────────
# actual 롤: 2026→23, 2025→29, 2024→35, 2023→41 (각 그룹 6열: RN/비중 ADR/지수 Rev/비중)
YEAR_COL = {2026: 23, 2025: 29, 2024: 35, 2023: 41}
PREFIX2SHEET = {'01':'비발디','02':'비발디','03':'비발디','04':'비발디','05':'비발디','06':'비발디',
 '07':'양평','08':'델피노','09':'양양','10':'삼척','11':'단양','12':'경주','13':'청송','14':'천안',
 '15':'변산','16':'여수','17':'거제','18':'진도','19':'벨제주','20':'캄제주','21':'고양','22':'해운대','23':'남해'}
SHEETS = ['총괄(사업장별 ▶)','고양','해운대','비발디','단양','여수','델피노','삼척','벨제주','양평',
          '캄제주','천안','변산','거제','청송','양양','경주','진도','남해']

def _short(cp):
    s = re.sub(r'^\s*\d+\.\s*', '', cp or '')
    if '비발디' in s: return '비발디'
    if '소노캄 제주' in s or '캄제주' in s: return '캄제주'
    if '제주' in s: return '벨제주'
    for key in ['고양','해운대','단양','여수','델피노','삼척','천안','변산','거제',
                '청송','양양','경주','진도','남해','양평']:
        if key in s: return key
    return None


def main():
    # ── 실적 집계 ─────────────────────────────────────────────
    lines = list(C.scan_inbound(YEAR, '12'))
    # (1) 대시보드용: 사업장 short × 월
    act = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0.0]))
    # (2) 엑셀용: year × (scope, 월, dim, name)  — scope=총괄 또는 사업장시트명
    xl = {y: collections.defaultdict(lambda: [0, 0.0]) for y in YEAR_COL}
    for ln in lines:
        sh = _short(ln['cprop'] or ln['prop'])
        if sh:
            for mk in (ln['month'], '계'):
                e = act[sh][mk]; e[0] += ln['rns']; e[1] += ln['rev']
    for y in YEAR_COL:
        for ln in C.scan_inbound(y, '12'):
            pref = (ln['cprop'] or ln['prop'])[:2]
            sheet = PREFIX2SHEET.get(pref if pref.isdigit() else '')
            for scope in (['총괄'] + ([sheet] if sheet else [])):
                for mmk in (ln['month'], '년간'):
                    for dim, name in (('계', '계'), ('국가', ln['nat']), ('Agent', ln['agent'])):
                        e = xl[y][(scope, mmk, dim, name)]; e[0] += ln['rns']; e[1] += ln['rev']

    # ── 대시보드 데이터 ────────────────────────────────────────
    D = {}
    for sh in set(TARGETS) | set(act):
        D[sh] = {'target': TARGETS.get(sh, {}),
                 'actual': {mk: act[sh][mk][0] for mk in act[sh]},
                 'actual_rev': {mk: round(act[sh][mk][1]) for mk in act[sh]}}
    tt = {'계': sum(TARGETS[s].get('계', 0) for s in TARGETS)}
    for m in range(1, 13):
        tt[str(m)] = sum(TARGETS[s].get(str(m), 0) for s in TARGETS)
    ta = collections.defaultdict(int)
    for sh in act:
        for mk, v in act[sh].items(): ta[mk] += v[0]
    D['전체'] = {'target': tt, 'actual': dict(ta), 'actual_rev': {}}
    PROPS = [s for s in ['고양','비발디','해운대','단양','여수','델피노','삼척','벨제주','양평',
             '청송','캄제주','경주','거제','양양','천안','변산','진도','남해'] if s in D]
    block = (f"const D={json.dumps(D, ensure_ascii=False)},"
             f"PROPS={json.dumps(PROPS, ensure_ascii=False)};")
    html = open(DOCS_HTML, encoding='utf-8').read()
    html2, n = re.subn(r'const D=.*?PROPS=\[[^\]]*\];', block, html, count=1, flags=re.S)
    if n != 1:
        raise SystemExit('ERR: dashboard inline data 패턴 미발견')
    open(DOCS_HTML, 'w', encoding='utf-8').write(html2)
    print(f'대시보드 갱신: 전체 도전 {tt["계"]:,} / 실적 {ta["계"]:,} '
          f'({ta["계"]/tt["계"]*100:.1f}%)')

    # ── 엑셀 채움 ─────────────────────────────────────────────
    def g(y, scope, mm, dim, name): return xl[y].get((scope, mm, dim, name), [0, 0.0])
    wb = openpyxl.load_workbook(TEMPLATE)
    for sh in SHEETS:
        if sh not in wb.sheetnames:
            print('  시트없음', sh); continue
        ws = wb[sh]; scope = '총괄' if sh.startswith('총괄') else sh
        for year, base in YEAR_COL.items():
            for col in range(base, base + 6):
                ws.cell(3, col).value = None            # 상단 연도숫자 제거(라벨 중복 방지)
            ws.cell(4, base).value = f'{year} Actual'   # 그룹라벨 단일 'YYYY Actual'
        cur_mm = cur_dim = None; blk = {}
        for r in range(7, ws.max_row + 1):
            b = ws.cell(r, 2).value; c = ws.cell(r, 3).value; d = ws.cell(r, 4).value
            if b not in (None, ''):
                s = str(b).strip()
                cur_mm = '년간' if s == '년간' else (s.zfill(2) if s.isdigit() else cur_mm)
            if c not in (None, ''):
                cc = str(c).strip(); cur_dim = cc if cc in ('계', '국가', 'Agent') else cur_dim
            if cur_mm is None: continue
            is_total = c is not None and str(c).strip() == '계'
            if is_total: name, dim = '계', '계'
            elif cur_dim in ('국가', 'Agent') and d not in (None, ''):
                name, dim = str(d).strip(), cur_dim
            else: continue
            for year, base in YEAR_COL.items():
                rns, rev = g(year, scope, cur_mm, dim, name)
                adr = (rev * 1e6 / rns / 1000) if rns else 0
                ws.cell(r, base).value = rns if rns else 0
                ws.cell(r, base + 2).value = round(adr, 1) if rns else 0
                ws.cell(r, base + 4).value = round(rev, 1) if rev else 0
                if is_total: blk[year] = (rns, rev)
                else:
                    trns, trev = blk.get(year, (0, 0))
                    tadr = (trev * 1e6 / trns / 1000) if trns else 0
                    ws.cell(r, base + 1).value = round(rns / trns, 3) if trns else 0
                    ws.cell(r, base + 3).value = round(adr / tadr, 2) if tadr else 0
                    ws.cell(r, base + 5).value = round(rev / trev, 3) if trev else 0
    # 총괄 요약 시트: 2026 Actual + 2027 도전목표
    try: fill_strategy(wb, act)
    except Exception as e: print('  총괄(전략) 실패:', e)
    try: fill_siljeok(wb, act)
    except Exception as e: print('  총괄(실적) 실패:', e)

    wb.save(DOCS_XLSX)
    try:
        import shutil; shutil.copyfile(DOCS_XLSX, DL_XLSX)
    except Exception as e:
        print('  Downloads 사본 실패:', e)
    print(f'엑셀 저장: {DOCS_XLSX}')
    print(f'         : {DL_XLSX}')


# ── 2027 도전목표 모델 (대시보드 시뮬레이션과 동일 로직) ────────────────
# 장수빈 매니저 가이드: 2027 도전목표 = 2026 실적 대비 5~10% 업, 양식 동일·숫자만.
# 증가율은 달성률로 차등(초과달성 10% → 미달 5%); 대시보드에서 사업장별 수기 조정.
def uplift_pct(ach):
    if ach >= 1.0: return 10
    if ach >= 0.9: return 8
    return 5
def target_2027(actual_rns, stretch_rns):
    ach = (actual_rns / stretch_rns) if stretch_rns else 1.0
    return round(actual_rns * (1 + uplift_pct(ach) / 100))


def fill_strategy(wb, act):
    """총괄(사업장별 전략): 2026 Actual + 2027 도전목표 열 삽입(값)."""
    import openpyxl
    ws = wb['총괄(사업장별 전략)']
    ROWS = list(range(15, 33))  # 사업장 15~32, 계=14
    def num(r, c):
        v = ws.cell(r, c).value
        return v if isinstance(v, (int, float)) else 0
    lit = {}
    for r in ROWS:
        lit[r] = dict(name=(ws.cell(r, 4).value or '').strip(),
                      rn19=num(r, 5), rev19=num(r, 7),
                      rn25=num(r, 8), rev25=num(r, 10),
                      bud_rn=num(r, 14), bud_adr=num(r, 15),
                      str_rn=num(r, 17), str_adr=num(r, 18),
                      key=ws.cell(r, 22).value)
    # insert_cols는 병합셀과 충돌(KeyError) → 삽입 대신 밴드(r12~32,col5~28)를
    # lit(원본값 선read)로 직접 재작성. 병합 해제 후 전 영역 클리어.
    for mr in list(ws.merged_cells.ranges):
        c1, r1, c2, r2 = mr.bounds
        if r2 >= 12 and r1 <= 32:
            ws.unmerge_cells(str(mr))
    for r in range(12, 33):
        for c in range(5, 29):
            ws.cell(r, c).value = None
    def grp(col, label):  # 라벨은 그룹 첫 셀에(병합 생략), 하위 RNs/ADR/REV
        ws.cell(12, col).value = label
        ws.cell(13, col).value = 'RNs'; ws.cell(13, col + 1).value = 'ADR'; ws.cell(13, col + 2).value = 'REV'
    grp(5, '2019 Actual'); grp(8, '2025 Actual'); grp(11, '2026 Actual'); grp(14, '2027 도전목표')
    grp(17, '25Y vs 19Y'); grp(20, '2026 Budget'); grp(23, '26Y Stretch Goal')
    ws.cell(12, 26).value = '25년 比'; ws.cell(13, 26).value = 'Δ'; ws.cell(13, 27).value = '달성률'
    ws.cell(12, 28).value = '달성 KEY 전략'
    def adr(rev, rn): return round(rev * 1000 / rn) if rn else 0
    for r in ROWS:
        L = lit[r]; a = act.get(L['name'], {}).get('계', [0, 0.0]); arn, arev = a[0], a[1]
        t27 = target_2027(arn, L['str_rn'])
        t27adr = adr(arev, arn) or L['str_adr']  # 2026 실적 ADR 유지
        v = [(5, L['rn19']), (6, adr(L['rev19'], L['rn19'])), (7, L['rev19']),
             (8, L['rn25']), (9, adr(L['rev25'], L['rn25'])), (10, L['rev25']),
             (11, arn), (12, adr(arev, arn)), (13, round(arev, 1)),
             (14, t27), (15, t27adr), (16, round(t27 * t27adr / 1000, 1)),
             (17, L['rn25'] - L['rn19']), (18, adr(L['rev25'], L['rn25']) - adr(L['rev19'], L['rn19'])), (19, round(L['rev25'] - L['rev19'], 1)),
             (20, L['bud_rn']), (21, L['bud_adr']), (22, round(L['bud_rn'] * L['bud_adr'] / 1000, 1)),
             (23, L['str_rn']), (24, L['str_adr']), (25, round(L['str_rn'] * L['str_adr'] / 1000, 1)),
             (26, L['str_rn'] - L['rn25']), (27, round(L['str_rn'] / L['rn25'], 3) if L['rn25'] else ''),
             (28, L['key'])]
        for c, val in v: ws.cell(r, c).value = val
    # 계 행(14) = 합/산출
    def csum(c): return sum(num(r, c) for r in ROWS)
    for c in [5, 7, 8, 10, 11, 13, 14, 16, 17, 19, 20, 22, 23, 25, 26]:
        ws.cell(14, c).value = round(csum(c), 1)
    for rnc, revc in [(5, 7), (8, 10), (11, 13), (14, 16), (17, 19), (20, 22), (23, 25)]:
        ws.cell(14, rnc + 1).value = adr(ws.cell(14, revc).value or 0, ws.cell(14, rnc).value or 0)
    ws.cell(14, 27).value = round((ws.cell(14, 23).value or 0) / (ws.cell(14, 8).value or 1), 3)
    print('  총괄(전략): 2026 Actual + 2027 도전목표 열 추가 (2027 계 %s)' % ws.cell(14, 14).value)


def fill_siljeok(wb, act):
    """총괄(사업장별 실적): 우측에 2026 Actual(온북) 열 추가."""
    ws = wb['총괄(사업장별 실적)']
    col = ws.max_column + 2
    ws.cell(4, col).value = '2026 Actual'
    ws.cell(5, col).value = 'RNs(온북)'
    tot = 0
    for r in range(1, ws.max_row + 1):
        nm = ws.cell(r, 3).value
        nm = str(nm).strip() if nm else ''
        if nm in act:
            rns = act[nm].get('계', [0, 0])[0]; ws.cell(r, col).value = rns; tot += rns
        elif nm == '계':
            ws.cell(r, col).value = sum(act[s].get('계', [0, 0])[0] for s in act)
    print('  총괄(실적): 2026 Actual(온북) 열 추가 (합 %s)' % tot)


if __name__ == '__main__':
    main()
