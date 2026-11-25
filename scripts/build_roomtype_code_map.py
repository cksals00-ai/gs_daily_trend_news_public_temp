#!/usr/bin/env python3.12
"""객실타입 코드 → 명칭 매핑 생성 (data/roomtype_code_map.json)

raw에는 객실타입 '코드'(예 00HOK101 = 평형 H + 동 00 + 타입 OK101)만 있고 명칭이 없다.
기존 객실타입별 실적 페이지(docs/data/roomtype_detail.json, 2026 실적·GS 세그)는 같은 raw에서 명칭 단위로 집계됐으므로,
(사업장, 투숙월) RN 벡터가 같은 (사업장, 코드)와 (사업장, 명칭)을 맞춰 코드↔명칭을 역산한다.
  exact : 월별 RN 벡터 완전 일치
  soft  : 코사인 ≥ 0.995 & 합계 차 ≤ 5% (스냅샷 시점 차이 허용)
  grade : 매칭 실패 → 코드 평형 문자(H/I/C/…)의 등급을 매칭된 코드들의 다수결로 추정(명칭은 '등급 (코드)')
결과는 신뢰도 라벨과 함께 기록. 대외비·관리자용. 실행: python3.12 scripts/build_roomtype_code_map.py
"""
import csv
import json
import math
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_product_cube as bpc  # noqa: E402  (select_files 재사용)

ROOT = Path(__file__).resolve().parent.parent
RT_DETAIL = ROOT / 'docs' / 'data' / 'roomtype_detail.json'
OUT = ROOT / 'data' / 'roomtype_code_map.json'
GS = {'72', '53', 'A4', 'A5', '58'}


PF_OF = {}   # 코드 → raw '평형' 값(H/I/K/J/C/A·AH/SH…). 객실타입 = 동(2) + 평형(1~2) + 타입(5)


def grade_letter(code):
    if code in PF_OF:
        return PF_OF[code]
    return code[2:-5] if code and len(code) >= 8 else ''


def main():
    rd = json.loads(RT_DETAIL.read_text(encoding='utf-8'))
    vec_rt = defaultdict(lambda: defaultdict(int))
    grade_of_rt = {}
    for si, mo, seg, rt, rn, rev in rd['cells']:
        vec_rt[(rd['sites'][si], rd['rts'][rt])][mo] += rn
        grade_of_rt[rd['rts'][rt]] = rd['grades'][rd['rtLead'][rt]]
    vec_code = defaultdict(lambda: defaultdict(int))
    for p, lo, hi in bpc.select_files(ROOT / 'data' / 'raw_db', [2026]):
        with open(p, encoding='cp949', errors='replace', newline='') as f:
            r = csv.reader(f, delimiter=';'); h = [x.strip().lstrip('﻿') for x in next(r)]; ix = {k: i for i, k in enumerate(h)}
            for row in r:
                if len(row) < 30:
                    continue
                ym = row[ix['판매일자']][:6]
                if (lo and ym < lo) or (hi and ym > hi) or not ym.isdigit():
                    continue
                if row[ix['변경예약집계코드']].strip() not in GS:
                    continue
                site = re.sub(r'^\d+\.\s*', '', row[ix['변경사업장명']].strip())
                code = row[ix['객실타입']].strip()
                vec_code[(site, code)][int(ym[4:6])] += int(float(row[ix['객실수']] or 1))
                if code not in PF_OF and '평형' in ix and row[ix['평형']].strip():
                    PF_OF[code] = row[ix['평형']].strip()

    def cos(a, b):
        ms = sorted(set(a) | set(b)); x = [a.get(m, 0) for m in ms]; y = [b.get(m, 0) for m in ms]
        nx, ny = math.sqrt(sum(i * i for i in x)), math.sqrt(sum(i * i for i in y))
        return sum(i * j for i, j in zip(x, y)) / (nx * ny) if nx and ny else 0.0

    mapping = {}   # 'site|code' → {name, grade, conf, score}
    used_rt = set()
    # 1) exact
    for (s, rt), v in vec_rt.items():
        if not sum(v.values()):
            continue
        cands = [c for (s2, c), v2 in vec_code.items() if s2 == s and all(v.get(m, 0) == v2.get(m, 0) for m in set(v) | set(v2))]
        if len(cands) == 1 and f'{s}|{cands[0]}' not in mapping:
            mapping[f'{s}|{cands[0]}'] = {'name': rt, 'grade': grade_of_rt[rt], 'conf': 'exact', 'score': 1.0}
            used_rt.add((s, rt))
    # 평형 문자 → 등급 (exact 매칭 다수결). soft 매칭은 이 등급과 일치할 때만 허용(오매칭 방지)
    letter_grade = defaultdict(Counter)
    for k, v in mapping.items():
        letter_grade[grade_letter(k.split('|')[1])][v['grade']] += 1
    letter_map = {l: c.most_common(1)[0][0] for l, c in letter_grade.items()}
    # 2) soft (best cosine per rt, one-to-one, 등급 일치)
    pairs = []
    for (s, rt), v in vec_rt.items():
        if (s, rt) in used_rt or not sum(v.values()):
            continue
        for (s2, c), v2 in vec_code.items():
            if s2 != s or f'{s}|{c}' in mapping:
                continue
            lg = letter_map.get(grade_letter(c))
            if lg and lg != grade_of_rt[rt]:
                continue
            t1, t2 = sum(v.values()), sum(v2.values())
            sc = cos(v, v2)
            if sc >= 0.995 and t1 and abs(t1 - t2) / t1 <= 0.05:
                pairs.append((sc, s, rt, c))
    for sc, s, rt, c in sorted(pairs, reverse=True):
        if (s, rt) in used_rt or f'{s}|{c}' in mapping:
            continue
        mapping[f'{s}|{c}'] = {'name': rt, 'grade': grade_of_rt[rt], 'conf': 'soft', 'score': round(sc, 4)}
        used_rt.add((s, rt))
    # 3) grade by letter (exact+soft 다수결) — 미매칭 코드는 '등급 (코드)'
    letter_grade = defaultdict(Counter)
    for k, v in mapping.items():
        letter_grade[grade_letter(k.split('|')[1])][v['grade']] += 1
    letter_map = {l: c.most_common(1)[0][0] for l, c in letter_grade.items()}
    n_grade = 0
    for (s, c), v in vec_code.items():
        key = f'{s}|{c}'
        if key in mapping:
            continue
        g = letter_map.get(grade_letter(c))
        if g:
            mapping[key] = {'name': f'{g} ({c})', 'grade': g, 'conf': 'grade', 'score': None}
            n_grade += 1
    # 코드만의 폴백(사업장 무관): 같은 코드가 여러 사업장에서 같은 명칭이면 code-level alias
    by_code = defaultdict(Counter)
    for k, v in mapping.items():
        if v['conf'] in ('exact', 'soft'):
            by_code[k.split('|')[1]][v['name']] += 1
    code_alias = {c: cnt.most_common(1)[0][0] for c, cnt in by_code.items()}
    out = {'meta': {'method': 'roomtype_detail(2026 GS) × raw 2026 월별 RN 벡터 매칭', 'exact': sum(1 for v in mapping.values() if v['conf'] == 'exact'),
                    'soft': sum(1 for v in mapping.values() if v['conf'] == 'soft'), 'grade_only': n_grade,
                    'rt_pairs': len(vec_rt), 'code_pairs': len(vec_code), 'letter_grade': letter_map, 'label': '대외비·관리자용'},
           'map': dict(sorted(mapping.items())), 'code_alias': dict(sorted(code_alias.items()))}
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=0), encoding='utf-8')
    print(json.dumps(out['meta'], ensure_ascii=False))
    print('→', OUT)


if __name__ == '__main__':
    main()
