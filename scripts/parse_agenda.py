#!/usr/bin/env python3
"""
사업장/부서 아젠다 추출기 (parse_agenda.py)
─────────────────────────────────────────────────────────────────
주간 리포트의 아젠다 페이지(구분 | 전주 주요사항 | 금주 계획)에서
사업장·부서별 아젠다 항목을 추출·분류한다.

  · 구분 컬럼(x<100)의 라벨 = 사업장/부서/해외 블록명
  · 블록 경계 = 라벨 y ~ 다음 라벨 y
  · 항목 = 내용영역(x>100)의 번호 항목("1. …") 헤드라인
  · 테마 = 키워드 규칙 분류(미디어/마케팅/식음/행사·MICE/시설·안전/멤버십·영업/디지털/상품/개발)
  · 금액 = 매출/예상매출 언급 추출

추정·창작 금지: 실제 텍스트 헤드라인을 그대로 보존하고, 테마는 규칙 태깅만.

build_exec_segment.py 가 월별 최신 주간 리포트에 대해 호출한다.
© 2026 GS팀
"""
import re
import unicodedata
from collections import defaultdict

nfc = lambda s: unicodedata.normalize("NFC", s or "")

# 사업장(운영 자산) — 부서/기능과 구분하기 위한 화이트리스트(부분일치)
PROPERTY_HINTS = [
    "소노캄", "소노벨", "소노문", "쏠비치", "델피노", "르네블루", "파나크",
    "팔라티움", "해운대", "고양", "제주", "양양", "삼척", "천안", "단양",
    "경주", "청송", "거제", "여수", "남해", "진도", "변산", "양평",
    "비발디", "소노펠리체", "소노펫", "소노빌리지", "스키&오션", "삼악산",
]
OVERSEAS_HINTS = ["하와이", "하이퐁", "괌", "베트남", "태국", "인도네시아", "망길라오", "탈로포포"]
DEPT_HINTS = ["마케팅", "식음기획", "레저", "PRM", "GS", "MICE", "펫", "예약센터",
              "멤버십", "CS기획", "프랜차이즈", "운영지원", "컨벤션"]

THEMES = [
    ("미디어·촬영", ["드라마", "촬영", "sbs", "kbs", "mbc", "예능", "방송", "유튜브", "인플루언서", "화보", "대관"]),
    ("마케팅·프로모션", ["프로모션", "이벤트", "홍보", "마케팅", "리뷰", "sns", "네이버", "제휴", "쿠폰", "할인"]),
    ("식음", ["식음", "셰프", "메뉴", "뷔페", "생맥주", "와인", "카페", "레스토랑", "다이닝", "망고", "F&B", "음료"]),
    ("행사·MICE", ["mice", "행사", "연회", "컨벤션", "단체", "세미나", "워크숍", "보안합숙", "임직원", "연합회"]),
    ("시설·안전", ["시설", "안전", "보수", "대수선", "소음", "공사", "점검", "위생", "a/r", "수상안전", "주차", "실외기", "폴딩"]),
    ("멤버십·영업", ["멤버십", "회원", "분양", "업셀링", "시그니처", "계약", "prm", "추천혜택", "구좌", "선납"]),
    ("디지털·시스템", ["시스템", "디지털", "html", "대시보드", "bi ", "tableau", "전산", "앱", "체크아웃", "이지패스"]),
    ("상품·패키지", ["패키지", "상품", "pkg", "예약", "요금", "객실", "관광", "dmz", "액티비티"]),
    ("개발·운영검토", ["프랜차이즈", "위탁운영", "mou", "개발", "매입", "부지", "오너", "협약", "감정"]),
]
AMT_RE = re.compile(r"(\d[\d,\.]*\s*(?:억|백만원|백만|만원|천원))")


def classify_theme(text):
    t = text.lower()
    for name, kws in THEMES:
        for kw in kws:
            if kw in t:
                return name
    return "기타"


def block_type(name):
    n = name.replace(" ", "")
    if any(h in n for h in OVERSEAS_HINTS):
        return "overseas"
    if any(h in n for h in PROPERTY_HINTS):
        return "property"
    if any(h in n for h in DEPT_HINTS):
        return "dept"
    return "etc"


def _agenda_pages(pdf):
    out = []
    for i, p in enumerate(pdf.pages):
        ts = nfc(p.extract_text() or "").replace(" ", "")
        if "구분" in ts and "주요사항" in ts and "금주" in ts:
            out.append(i)
    return out


def _labels_on_page(ws):
    """구분 컬럼(x<98) 라벨을 y-군집으로 묶어 (y_start, name) 리스트 반환."""
    left = sorted([w for w in ws if w["x0"] < 98 and w["top"] > 100], key=lambda w: w["top"])
    clus, cur = [], []
    for w in left:
        if cur and w["top"] - cur[-1]["top"] > 16:
            clus.append(cur); cur = []
        cur.append(w)
    if cur:
        clus.append(cur)
    labels = []
    for c in clus:
        name = " ".join(w["text"] for w in sorted(c, key=lambda w: (round(w["top"]), w["x0"])))
        name = re.sub(r"\s+", " ", name).strip()
        if name and not name[0].isdigit() and name != "구분" and "·" not in name and "[" not in name and len(name) <= 20:
            labels.append((min(w["top"] for w in c), name))
    return labels


COL_SPLIT = 430  # 주요사항(번호 x~120) | 금주 계획(번호 x~436) 경계 x


def _lines(ws, ylo, yhi, xmin, xmax):
    """[xmin,xmax) 열의 단어를 줄로 재구성."""
    rows = defaultdict(list)
    for w in ws:
        cx = (w["x0"] + w["x1"]) / 2
        if ylo <= w["top"] < yhi and xmin <= cx < xmax:
            rows[round(w["top"] / 3)].append((w["x0"], w["text"]))
    out = []
    for k in sorted(rows):
        line = re.sub(r"\s+", " ", " ".join(t for _, t in sorted(rows[k]))).strip()
        if line:
            out.append(line)
    return out


def _items_from_lines(lines, phase):
    items = []
    for ln in lines:
        m = re.match(r"^\s*(\d+)\.\s*(.+)", ln)
        if m:
            title = re.sub(r"\s*\([\d.~\s]*\)\s*$", "", m.group(2).strip())
            if len(title) >= 4:
                items.append((title[:80], phase))
    return items


# 라벨 병합(2줄 라벨: 소노펠리체+컨벤션, 태국+인도네시아)
MERGE = {"컨벤션": "소노펠리체 컨벤션", "인도네시아": "태국/인도네시아"}


def extract_agenda(pdf):
    """→ list[{name, type, items:[{title, theme, amounts:[...]}], raw_lines:int}]"""
    blocks = []
    for pi in _agenda_pages(pdf):
        ws = pdf.pages[pi].extract_words()
        for w in ws:
            w["text"] = nfc(w["text"])
        labels = _labels_on_page(ws)
        page_top = min((w["top"] for w in ws if w["top"] > 100), default=100) - 5
        page_bottom = max((w["top"] for w in ws), default=800) + 20
        for idx, (y, name) in enumerate(labels):
            # 구분 라벨은 블록 중앙 정렬 → 경계 = 인접 라벨과의 중점
            ylo = (labels[idx - 1][0] + y) / 2 if idx > 0 else page_top
            yhi = (y + labels[idx + 1][0]) / 2 if idx + 1 < len(labels) else page_bottom
            done_lines = _lines(ws, ylo, yhi, 105, COL_SPLIT)   # 전주 주요사항
            plan_lines = _lines(ws, ylo, yhi, COL_SPLIT, 900)   # 금주 계획
            raw = _items_from_lines(done_lines, "done") + _items_from_lines(plan_lines, "plan")
            amounts = AMT_RE.findall(" ".join(done_lines + plan_lines))
            if not raw and not amounts:
                continue
            blocks.append({
                "name": name, "type": block_type(name),
                "items": [{"title": t, "theme": classify_theme(t), "phase": ph} for t, ph in raw[:12]],
                "amounts": list(dict.fromkeys(a.strip() for a in amounts))[:6],
                "page": pi + 1,
            })
    # 동일 name 병합(멀티 페이지/2줄 라벨)
    merged = {}
    for b in blocks:
        key = MERGE.get(b["name"], b["name"])
        if key not in merged:
            merged[key] = {"name": key, "type": b["type"], "items": [], "amounts": []}
        merged[key]["items"].extend(b["items"])
        merged[key]["amounts"].extend(b["amounts"])
    out = []
    for b in merged.values():
        # 항목 중복 제거
        seen = set(); uitems = []
        for it in b["items"]:
            if it["title"] not in seen:
                seen.add(it["title"]); uitems.append(it)
        b["items"] = uitems
        b["amounts"] = list(dict.fromkeys(b["amounts"]))[:6]
        b["theme_counts"] = _theme_counts(uitems)
        out.append(b)
    return out


def _theme_counts(items):
    c = defaultdict(int)
    for it in items:
        c[it["theme"]] += 1
    return dict(sorted(c.items(), key=lambda x: -x[1]))


if __name__ == "__main__":
    import sys, json, pdfplumber
    pdf = pdfplumber.open(sys.argv[1])
    ag = extract_agenda(pdf)
    print(json.dumps(ag, ensure_ascii=False, indent=2))
