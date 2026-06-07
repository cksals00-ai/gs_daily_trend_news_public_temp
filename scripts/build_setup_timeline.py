#!/usr/bin/env python3
"""
build_setup_timeline.py — 구축 현황 리포트 '최근 구축 로그' 자동 생성
============================================================
docs/gs-setup-report.html 의 <!-- AUTO-TIMELINE:START --> ~ END 마커 구간을
git 커밋 로그(feat/fix/infra/data, 자동 데이터 커밋 제외)에서 날짜별로
집계해 재생성한다. 더미·추정 없이 실제 커밋만 사용.

사용:
  python3 scripts/build_setup_timeline.py            # 기본 SINCE 부터
  python3 scripts/build_setup_timeline.py 2026-05-21 # 시작일 지정

weekly-setup-strategy 스케줄 태스크 또는 build.py 후처리에서 호출하면
초기 큐레이션(04-21~05-20) 이후 구간이 매주 자동 최신화된다.
"""
import html as html_module
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
REPORT = ROOT / "docs" / "gs-setup-report.html"
SINCE_DEFAULT = "2026-05-21"          # 큐레이션 구간(04-21~05-20) 다음날
MAX_ITEMS_PER_DAY = 5                  # 날짜별 최대 표기 건수

START = "<!-- AUTO-TIMELINE:START -->"
END = "<!-- AUTO-TIMELINE:END -->"

# 제외: 자동 데이터 커밋·머지·임시 커밋·업로드
SKIP = re.compile(
    r"^(chore\(auto\)|docs\(auto\)|merge\b|temp:|rebuild\b)"
    r"|Merge |resolve|save local changes|Add files via upload|Update campaign_performance",
    re.IGNORECASE,
)
# conventional-commit 접두사 제거용
PREFIX = re.compile(r"^(feat|fix|refactor|chore|data|style|nav|docs|perf|infra)(\([^)]*\))?:?\s*")


def classify(subject: str):
    """커밋 제목 → (라벨, pill 클래스). 더미 분류 없음, 접두사/키워드 기반."""
    s = subject.lower()
    if s.startswith("infra") or s.startswith("chore(infra") or "launchd" in s or "pipeline" in s and s.startswith("feat(pipeline"):
        return "인프라", "done"
    if s.startswith("feat"):
        return "기능", "on"
    if s.startswith("data") or s.startswith("chore: 온북") or "온북" in s and s.startswith("data"):
        return "데이터", "off"
    if s.startswith("fix") or s.startswith("refactor"):
        return "수정", "warn"
    return "변경", "off"


def clean(subject: str) -> str:
    return PREFIX.sub("", subject).strip()


def collect(since: str):
    out = subprocess.run(
        ["git", "log", f"--since={since}", "--pretty=format:%ad\x1f%s", "--date=format:%Y-%m-%d"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout
    days = {}  # date -> list[(label, pill, text)]
    for line in out.splitlines():
        if "\x1f" not in line:
            continue
        date, subject = line.split("\x1f", 1)
        if SKIP.search(subject):
            continue
        label, pill = classify(subject)
        text = clean(subject)
        if not text:
            continue
        days.setdefault(date, []).append((label, pill, text))
    return days


def build_rows(days: dict) -> str:
    rows = []
    for date in sorted(days, reverse=True):
        items = days[date]
        # 날짜별 대표 pill: 인프라 > 기능 > 수정 > 데이터/변경
        order = {"인프라": 0, "기능": 1, "수정": 2, "데이터": 3, "변경": 4}
        items_sorted = sorted(items, key=lambda x: order.get(x[0], 9))
        label, pill = items_sorted[0][0], items_sorted[0][1]
        # 본문: 중복 제거 후 상위 N건 ' · ' 결합
        seen, texts = set(), []
        for _, _, t in items_sorted:
            key = t[:24]
            if key in seen:
                continue
            seen.add(key)
            texts.append(html_module.escape(t))
            if len(texts) >= MAX_ITEMS_PER_DAY:
                break
        more = len(items) - len(texts)
        body = " · ".join(texts)
        if more > 0:
            body += f' <span class="dim">외 {more}건</span>'
        md = date[5:]  # MM-DD
        rows.append(
            f'          <tr><td class="mono">{md}</td>'
            f'<td class="c"><span class="pill {pill}">{label}</span></td>'
            f'<td>{body}</td></tr>'
        )
    if not rows:
        rows.append('          <tr><td colspan="3" class="dim" style="text-align:center;padding:14px">해당 기간 구축 커밋 없음</td></tr>')
    return "\n".join(rows)


def main():
    since = sys.argv[1] if len(sys.argv) > 1 else SINCE_DEFAULT
    html = REPORT.read_text(encoding="utf-8")
    if START not in html or END not in html:
        print(f"✗ 마커 미발견: {START} / {END}", file=sys.stderr)
        sys.exit(1)
    days = collect(since)
    rows = build_rows(days)
    new_block = f"{START}\n{rows}\n          {END}"
    html = re.sub(
        re.escape(START) + r".*?" + re.escape(END),
        lambda _: new_block,
        html, count=1, flags=re.DOTALL,
    )
    REPORT.write_text(html, encoding="utf-8")
    n_days = len(days)
    n_items = sum(len(v) for v in days.values())
    print(f"✓ 구축 타임라인 갱신: {since} 이후 {n_days}일 · {n_items}건 커밋 → {REPORT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
