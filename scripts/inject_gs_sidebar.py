#!/usr/bin/env python3.12
"""GS 실적 리포트 공통 사이드바 블록을 사이드바가 없던 페이지에 주입 (멱등).
   - 자체 <aside class="sidebar"> / <aside class="cr-sidebar">가 있는 페이지: 그 첫 자식으로 블록 삽입
   - 사이드바가 전혀 없는 페이지(action_plan_dashboard): <main class="wrap">를 flex 셸로 감싸고 좌측 사이드바 생성
   순서/하이라이트는 unify_nav_v2 규칙과 동일(당일현황 › 예약추이✨ › 상품상세✨ › …). 현재 페이지 = active.
사용: python3.12 scripts/inject_gs_sidebar.py [--apply] [페이지명 …]"""
import os, re, sys

APPLY = '--apply' in sys.argv
ARGS = [a for a in sys.argv[1:] if not a.startswith('--')]
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CANON = [('otb.html', '당일현황'), ('booking-trend.html', '예약추이'), ('product-detail.html', '상품상세'), ('weekly-compare.html', '금주비교'),
         ('channel.html', '채널별'), ('stay-date.html', '투숙일기준'), ('booking-status.html', 'Booking Status'), ('manager-analysis.html', '담당자 분석'),
         ('property-report.html', '사업장별 리포트'), ('fcst-trend.html', 'FCST 추이'), ('action_plan_dashboard.html', 'Action Plan'),
         ('gs-weekly.html', '국내 영업 실적'), ('gs-sales-report.html', '영업활동 보고'), ('gs-strategy-report.html', '전략 리포트'), ('gs-closing-report.html', '마감 보고')]
SHINE = {'booking-trend.html', 'product-detail.html'}
TARGETS = {'fcst-trend.html': 'aside', 'gs-sales-report.html': 'aside', 'gs-strategy-report.html': 'aside', 'gs-closing-report.html': 'cr', 'action_plan_dashboard.html': 'shell'}
CSS = """<style id="gs-side-nav-css">
.gs-side-nav{display:flex;flex-direction:column;gap:2px;margin:0 0 14px;padding:0 0 12px;border-bottom:1px solid var(--rule,#2a3140);}
.gs-side-label{font-family:var(--mono,'JetBrains Mono',ui-monospace,monospace);font-size:10px;letter-spacing:.18em;color:var(--ink-faint,#6b7280);text-transform:uppercase;margin-bottom:6px;}
.gs-side-nav .otb-nav-link{display:block;padding:6px 10px;border-radius:4px;font-family:var(--mono,'JetBrains Mono',ui-monospace,monospace);font-size:11px;color:var(--ink-muted,#9aa3b2);text-decoration:none;transition:all .15s;letter-spacing:.03em;white-space:nowrap;}
.gs-side-nav .otb-nav-link:hover{background:var(--bg-soft,#161b24);color:var(--ink,#e6e9ef);}
.gs-side-nav .otb-nav-link.active{background:rgba(201,160,99,.18);color:var(--gold-bright,#e6b970);}
.gs-side-nav .otb-nav-link.shine{color:var(--gold-bright,#e6b970)!important;background:linear-gradient(90deg,rgba(201,160,99,.22),rgba(201,160,99,.05));box-shadow:0 0 0 1px rgba(201,160,99,.35),0 0 12px rgba(201,160,99,.40);animation:gsNavShine 2.1s ease-in-out infinite;}
@keyframes gsNavShine{0%,100%{box-shadow:0 0 0 1px rgba(201,160,99,.26),0 0 6px rgba(201,160,99,.20);}50%{box-shadow:0 0 0 1px rgba(201,160,99,.60),0 0 18px rgba(201,160,99,.60);}}
.gs-app-shell{display:flex;align-items:flex-start;}
.gs-sidebar{width:200px;flex-shrink:0;background:var(--bg-deep,#0b0f14);border-right:1px solid var(--rule,#2a3140);padding:18px 14px;position:sticky;top:42px;height:calc(100vh - 42px);overflow-y:auto;}
.gs-app-shell>main{flex:1;min-width:0;}
@media(max-width:860px){.gs-side-nav{flex-direction:row;flex-wrap:wrap;gap:4px;border-bottom:0;padding-bottom:6px;}.gs-side-label{width:100%;}
.gs-app-shell{flex-direction:column;}.gs-sidebar{width:100%;position:static;height:auto;border-right:0;border-bottom:1px solid var(--rule,#2a3140);padding:10px 12px;}}
</style>
"""


def block(page, indent='    '):
    lines = [f'{indent}<!-- GS 실적 리포트 공통 사이드바 (scripts/inject_gs_sidebar.py) -->', f'{indent}<div class="gs-side-nav" id="gs-side-nav">',
             f'{indent}  <div class="gs-side-label">GS 실적 리포트</div>']
    for href, label in CANON:
        cls = 'otb-nav-link' + (' shine' if href in SHINE else '') + (' active' if href == page else '')
        lines.append(f'{indent}  <a href="{href}" class="{cls}">{label}</a>')
    lines.append(f'{indent}</div>')
    return '\n'.join(lines) + '\n'


def process(page):
    path = os.path.join(ROOT, 'docs', page)
    s = open(path, encoding='utf-8').read(); orig = s
    if 'id="gs-side-nav"' in s:
        return 'skip(이미 주입)'
    mode = TARGETS[page]
    if mode == 'aside':
        m = re.search(r'<aside class="sidebar"[^>]*>\s*\n?', s)
        if not m: return '! aside.sidebar 없음'
        s = s[:m.end()] + block(page) + s[m.end():]
    elif mode == 'cr':
        m = re.search(r'<aside class="cr-sidebar"[^>]*>\s*\n?', s)
        if not m: return '! aside.cr-sidebar 없음'
        s = s[:m.end()] + block(page) + s[m.end():]
    elif mode == 'shell':
        m = re.search(r'<main class="wrap">', s)
        if not m: return '! main.wrap 없음'
        close = s.rfind('</main>')
        if close < 0: return '! </main> 없음'
        s = s[:close] + '</main>\n</div><!-- /gs-app-shell -->' + s[close + len('</main>'):]
        s = s[:m.start()] + '<div class="gs-app-shell">\n  <aside class="gs-sidebar">\n' + block(page) + '  </aside>\n' + s[m.start():]
    if 'id="gs-side-nav-css"' not in s:
        s = s.replace('</head>', CSS + '</head>', 1)
    if s != orig and APPLY:
        open(path, 'w', encoding='utf-8').write(s)
    return f'{"APPLY" if APPLY else "DRY"} mode={mode} +{len(s) - len(orig)}B'


for page in (ARGS or list(TARGETS)):
    print(f'{page:28s} {process(page)}')
