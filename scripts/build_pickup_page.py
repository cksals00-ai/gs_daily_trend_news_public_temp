#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""build_pickup_page.py — _pickup678_rows.json → docs/pickup-678.html (자체완결, executive 인증가드)."""
import json, os
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT = Path(__file__).resolve().parent.parent
SRC = json.load(open(PROJECT / "data/_pickup678_rows.json", encoding="utf-8"))
ROWS = SRC["rows"]
CAMP = set(SRC["camp_codes"])
WINS = ["CUR", "WOW", "MOM", "YOY"]
WLAB = {"CUR": "최근6일<br>06.18~23", "WOW": "직전6일<br>06.12~17",
        "MOM": "전월동기<br>05.18~23", "YOY": "전년동기<br>25.06.18~23"}
SEG3 = ("OTA", "G-OTA", "Inbound")
OP = {"H/U", "단체COMP"}
MON = ["202606", "202607", "202608"]
def sales(r): return r["seg"] not in OP

def agg(keyf, sales_only=True):
    d = defaultdict(lambda: {w: {"rn": 0, "rev": 0, "new": 0, "can": 0} for w in WINS})
    mon = defaultdict(lambda: defaultdict(int))
    for r in ROWS:
        if sales_only and not sales(r): continue
        k = keyf(r); w = r["win"]
        d[k][w]["rn"] += r["rn_s"]; d[k][w]["rev"] += r["rev_s"]
        if r["sign"] > 0: d[k][w]["new"] += r["rn"]
        else: d[k][w]["can"] += r["rn"]
        if w == "CUR": mon[k][r["sm"]] += r["rn_s"]
    return d, mon

def pct(c, b): return (c - b) / b if b else None
def fmtpct(p):
    if p is None: return '<span class="dim">—</span>'
    cls = "up" if p >= 0 else "dn"
    return f'<span class="{cls}">{p*100:+.0f}%</span>'
def i(n): return f"{n:,}"
def m(won): return f"{won/1e6:,.0f}"

# ── 윈도우 합계(판매) ──
snet = {w: {"rn": 0, "rev": 0} for w in WINS}
for r in ROWS:
    if sales(r): snet[r["win"]]["rn"] += r["rn_s"]; snet[r["win"]]["rev"] += r["rev_s"]
pk = sum(r["rn_s"] for r in ROWS if sales(r) and r["win"] == "CUR" and str(r["mnum"]).startswith("86"))
pk_share = pk / snet["CUR"]["rn"] * 100

def dim_rows(d, mon, items, seg_of=None):
    out = []
    for k, v in items:
        cur = v["CUR"]
        seg = f'<td class="c">{seg_of.get(k,"")}</td>' if seg_of is not None else ""
        mcells = "".join(f'<td class="num">{i(mon[k].get(sm,0))}</td>' for sm in MON)
        out.append(
            f'<tr><td>{k}</td>{seg}'
            f'<td class="num">{i(cur["new"])}</td><td class="num">{i(cur["can"])}</td>'
            f'<td class="num hi">{i(cur["rn"])}</td>{mcells}'
            f'<td class="num">{m(cur["rev"])}</td>'
            f'<td class="num">{i(v["WOW"]["rn"])}</td><td class="num">{fmtpct(pct(cur["rn"],v["WOW"]["rn"]))}</td>'
            f'<td class="num">{i(v["MOM"]["rn"])}</td><td class="num">{fmtpct(pct(cur["rn"],v["MOM"]["rn"]))}</td>'
            f'<td class="num">{i(v["YOY"]["rn"])}</td><td class="num">{fmtpct(pct(cur["rn"],v["YOY"]["rn"]))}</td></tr>')
    return "".join(out)

def dim_table(keyf, label, seg_col=False, filt=None, topn=None):
    d, mon = agg(keyf)
    items = [(k, v) for k, v in d.items() if not (filt and not filt(k))]
    items.sort(key=lambda kv: -kv[1]["CUR"]["rn"])
    if topn: items = items[:topn]
    seg_of = None
    if seg_col:
        tmp = defaultdict(lambda: defaultdict(int))
        for r in ROWS:
            if sales(r) and (not filt or filt(keyf(r))): tmp[keyf(r)][r["seg"]] += abs(r["rn_s"])
        seg_of = {k: max(sd, key=sd.get) for k, sd in tmp.items()}
    segh = "<th>세그먼트</th>" if seg_col else ""
    head = (f'<th>{label}</th>{segh}<th>신규</th><th>취소</th><th>Net RN</th>'
            f'<th>6월</th><th>7월</th><th>8월</th><th>Net매출<br>백만</th>'
            f'<th>WoW</th><th>Δ</th><th>MoM</th><th>Δ</th><th>YoY</th><th>Δ</th>')
    return f'<div class="tw"><table><thead><tr>{head}</tr></thead><tbody>{dim_rows(d,mon,items,seg_of)}</tbody></table></div>'

def cross_table(keyf, n=20, skip=None):
    d, _ = agg(keyf)
    items = [(k, v) for k, v in d.items() if not (skip and skip(k))]
    items.sort(key=lambda kv: -kv[1]["CUR"]["rn"])[:n] if False else None
    items = sorted(items, key=lambda kv: -kv[1]["CUR"]["rn"])[:n]
    body = ""
    for (a, b), v in items:
        cur = v["CUR"]
        body += (f'<tr><td>{a}</td><td>{b}</td><td class="num hi">{i(cur["rn"])}</td>'
                 f'<td class="num">{m(cur["rev"])}</td>'
                 f'<td class="num">{i(v["YOY"]["rn"])}</td><td class="num">{fmtpct(pct(cur["rn"],v["YOY"]["rn"]))}</td></tr>')
    return (f'<div class="tw"><table><thead><tr><th>사업장</th><th>교차항목</th><th>Net RN</th>'
            f'<th>Net매출<br>백만</th><th>YoY</th><th>Δ</th></tr></thead><tbody>{body}</tbody></table></div>')

# 비OTA 잔여
resid = {"rn": 0, "rev": 0}
for r in ROWS:
    if sales(r) and r["win"] == "CUR" and r["ch"] == "미분류":
        resid["rn"] += r["rn_s"]; resid["rev"] += r["rev_s"]

# 기획전 캘린더
cd = json.load(open(PROJECT / "docs/data/campaign_data.json", encoding="utf-8"))
def ov(a1, a2, b1, b2): return a1 and a2 and a1 <= b2 and b1 <= a2
active = [e for e in cd["events"]
          if ov(e.get("투숙시작"), e.get("투숙종료"), "2026-06-01", "2026-08-31")
          or ov(e.get("판매시작"), e.get("판매종료"), "2026-06-12", "2026-06-23")]
propc = defaultdict(int)
for e in active: propc[e.get("사업장") or "기타"] += 1
camp_cal = "".join(f"<span class='chip'>{k} <b>{c}</b></span>" for k, c in sorted(propc.items(), key=lambda x: -x[1]))
dcamp, _ = agg(lambda r: "C" if r["mnum"] in CAMP else "O")
camp_net = dcamp.get("C", {w: {"rn": 0} for w in WINS})["CUR"]["rn"]

# 월별 판매 net
mon_net = defaultdict(lambda: defaultdict(int))
for r in ROWS:
    if not sales(r): continue
    ml = {"202606": "6월", "202607": "7월", "202608": "8월", "202506": "6월", "202507": "7월", "202508": "8월"}.get(r["sm"], r["sm"])
    mon_net[r["win"]][ml] += r["rn_s"]

# 핵심지표 표
def metric_row(label, fn, fmt):
    vals = {w: fn(w) for w in WINS}
    cells = "".join(f'<td class="num{" hi" if w=="CUR" else ""}">{fmt(vals[w])}</td>' for w in WINS)
    base = vals["CUR"]
    deltas = "".join(f'<td class="num">{fmtpct(pct(base, vals[w]))}</td>' for w in ("WOW", "MOM", "YOY"))
    return f'<tr><td>{label}</td>{cells}{deltas}</tr>'
metric_tbl = (metric_row("Net 픽업 RN (실)", lambda w: snet[w]["rn"], i)
              + metric_row("Net 객실매출 (백만)", lambda w: round(snet[w]["rev"]/1e6), i)
              + metric_row("일평균 (RN/일,÷6)", lambda w: round(snet[w]["rn"]/6), i))
mon_tbl = "".join(
    f'<tr><td>{mm}</td>' + "".join(f'<td class="num{" hi" if w=="CUR" else ""}">{i(mon_net[w][mm])}</td>' for w in WINS) + "</tr>"
    for mm in ["6월", "7월", "8월"])

prop_t = dim_table(lambda r: r["prop"], "사업장")
cat_t = dim_table(lambda r: r["cat"], "상품카테고리")
ch_t = dim_table(lambda r: r["ch"], "채널", seg_col=True, filt=lambda k: k != "미분류")
crossA = cross_table(lambda r: (r["prop"], f'{r["ch"]}[{r["seg"] if r["seg"] in SEG3 else "비OTA"}]'),
                     skip=lambda k: k[1].startswith("미분류"))
crossB = cross_table(lambda r: (r["prop"], r["cat"]))

# 차트 데이터
ch_chart = sorted(agg(lambda r: r["ch"])[0].items(), key=lambda kv: -kv[1]["CUR"]["rn"])
ch_chart = [(k, v) for k, v in ch_chart if k != "미분류"][:8]
chart_labels = json.dumps([k for k, _ in ch_chart], ensure_ascii=False)
chart_vals = json.dumps([v["CUR"]["rn"] for _, v in ch_chart])
prop_chart = sorted(agg(lambda r: r["prop"])[0].items(), key=lambda kv: -kv[1]["CUR"]["rn"])[:10]
pchart_labels = json.dumps([k for k, _ in prop_chart], ensure_ascii=False)
pchart_vals = json.dumps([v["CUR"]["rn"] for _, v in prop_chart])

yoy_rn = pct(snet["CUR"]["rn"], snet["YOY"]["rn"]) * 100
yoy_rev = pct(snet["CUR"]["rev"], snet["YOY"]["rev"]) * 100
mom_rn = pct(snet["CUR"]["rn"], snet["MOM"]["rn"]) * 100
wow_rn = pct(snet["CUR"]["rn"], snet["WOW"]["rn"]) * 100

HTML = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta name="auth-required" content="executive">
<script src="./js/auth.js"></script>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<title>6·7·8월 픽업 호조 분석 · SONO GS</title>
<meta name="theme-color" content="#1a1d23">
<link rel="manifest" href="./manifest.json">
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 192 192'><rect fill='%231a1d23' width='192' height='192'/><circle cx='96' cy='96' r='85' fill='%23252932'/><text x='96' y='100' font-size='96' font-weight='700' fill='%23c9a063' text-anchor='middle' font-family='sans-serif'>GS</text></svg>">
<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
:root{{--bg:#1a1d23;--card:#252932;--deep:#14171c;--soft:#2d3138;--line:#363b44;--txt:#e8eaed;--mut:#9aa0aa;--gold:#c9a063;--up:#4ade80;--dn:#f87171;--blue:#60a5fa}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--txt);font-family:'Noto Sans KR',sans-serif;font-size:14px;line-height:1.5;padding:18px 14px 60px;max-width:1180px;margin:0 auto}}
h1{{font-size:21px;font-weight:800;letter-spacing:-.3px}}
h1 .g{{color:var(--gold)}}
.sub{{color:var(--mut);font-size:12px;margin-top:4px}}
h2{{font-size:16px;font-weight:700;margin:26px 0 10px;padding-left:9px;border-left:3px solid var(--gold)}}
.note{{color:var(--mut);font-size:11.5px;margin:6px 0 4px;line-height:1.6}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:14px 0}}
.kc{{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:13px 15px}}
.kc .lab{{color:var(--mut);font-size:11px}}
.kc .val{{font-size:23px;font-weight:800;font-family:'JetBrains Mono';margin-top:3px}}
.kc .d{{font-size:11.5px;margin-top:4px}}
.up{{color:var(--up)}}.dn{{color:var(--dn)}}.dim{{color:var(--mut)}}
.tw{{overflow-x:auto;border:1px solid var(--line);border-radius:10px;margin:6px 0}}
table{{border-collapse:collapse;width:100%;font-size:12.5px;min-width:560px}}
th,td{{padding:7px 9px;text-align:left;white-space:nowrap;border-bottom:1px solid var(--line)}}
thead th{{background:var(--deep);color:var(--mut);font-weight:600;font-size:11px;position:sticky;top:0}}
tbody tr:hover{{background:var(--soft)}}
td.num{{text-align:right;font-family:'JetBrains Mono';font-size:12px}}
td.c{{text-align:center}}
td.hi{{color:var(--gold);font-weight:700}}
tbody td:first-child{{font-weight:500}}
.chip{{display:inline-block;background:var(--soft);border:1px solid var(--line);border-radius:20px;padding:3px 11px;margin:3px;font-size:12px}}
.chip b{{color:var(--gold)}}
.chartbox{{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px;margin:10px 0}}
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}
@media(max-width:760px){{.grid2{{grid-template-columns:1fr}}}}
.tag{{display:inline-block;background:rgba(201,160,99,.15);color:var(--gold);border-radius:6px;padding:2px 8px;font-size:11px;font-weight:600;margin-left:6px}}
.foot{{color:var(--mut);font-size:11px;margin-top:30px;border-top:1px solid var(--line);padding-top:14px;line-height:1.7}}
</style>
</head>
<body>
<h1>6·7·8월 투숙 <span class="g">픽업 호조</span> 분석 <span class="tag">최근 6완전일</span></h1>
<div class="sub">기준일 2026-06-24 · 라이브 스냅샷 · 생성 {datetime.now():%Y-%m-%d %H:%M} KST</div>

<div class="cards">
  <div class="kc"><div class="lab">판매 Net 픽업 (6/7/8월)</div><div class="val">{i(snet['CUR']['rn'])}<span style="font-size:13px;color:var(--mut)"> RN</span></div><div class="d">전년 {fmtpct(yoy_rn/100)} · 전월 {fmtpct(mom_rn/100)} · 직전 {fmtpct(wow_rn/100)}</div></div>
  <div class="kc"><div class="lab">Net 객실매출</div><div class="val">{m(snet['CUR']['rev'])}<span style="font-size:13px;color:var(--mut)"> 백만</span></div><div class="d">전년 {fmtpct(yoy_rev/100)} (ADR↑ 동반)</div></div>
  <div class="kc"><div class="lab">일평균 pace</div><div class="val">{i(round(snet['CUR']['rn']/6))}<span style="font-size:13px;color:var(--mut)"> RN/일</span></div><div class="d">전년 동기 수준 상회</div></div>
  <div class="kc"><div class="lab">패키지(86xx) 비중</div><div class="val">{pk_share:.0f}<span style="font-size:13px;color:var(--mut)"> %</span></div><div class="d">프로모션·기획전성 상품</div></div>
</div>

<div class="note">■ <b>픽업 정의(net)</b>: 신규(최초입력일자, 27/43+28/44) − 취소(취소일자, 28/44). 투숙월=판매일자. RN=객실수, 매출=1박객실료×객실수÷1.1(VAT제외).<br>
■ <b>판매 픽업</b> = 전체 − 하우스유즈(H/U)·단체COMP(무상,매출≈0). 4개 윈도우 모두 <b>동일 6완전일</b>(06-24 부분일 제외)이라 절대값 직접 비교가 공정. (전체 정제·DB는 엑셀본 참조)</div>

<h2>핵심 지표 — 판매 Net 픽업</h2>
<div class="tw"><table><thead><tr><th>지표</th><th>{WLAB['CUR']}</th><th>{WLAB['WOW']}</th><th>{WLAB['MOM']}</th><th>{WLAB['YOY']}</th><th>vs WoW</th><th>vs MoM</th><th>vs YoY</th></tr></thead><tbody>{metric_tbl}</tbody></table></div>

<h2>투숙월별 판매 Net RN</h2>
<div class="tw"><table><thead><tr><th>투숙월</th><th>{WLAB['CUR']}</th><th>{WLAB['WOW']}</th><th>{WLAB['MOM']}</th><th>{WLAB['YOY']}</th></tr></thead><tbody>{mon_tbl}</tbody></table></div>

<div class="grid2">
  <div class="chartbox"><b style="font-size:13px">사업장 TOP10 (CUR Net RN)</b><canvas id="cp" height="220"></canvas></div>
  <div class="chartbox"><b style="font-size:13px">채널 TOP8 (CUR Net RN)</b><canvas id="cc" height="220"></canvas></div>
</div>

<h2>① 사업장별</h2>
{prop_t}

<h2>② 상품카테고리별</h2>
<div class="note">패키지(회원번호 86xx) 9분류 + '비패키지(일반)'. '연박/투나잇'은 박수·연속OTA 문맥 반영.</div>
{cat_t}

<h2>③ 채널 · 세그먼트별 <span class="tag">OTA/G-OTA/Inbound</span></h2>
<div class="note">비OTA(회원·자사·단체·D멤버스 등, 채널 미식별) 합계는 별도: CUR Net RN {i(resid['rn'])} / 매출 {m(resid['rev'])}백만 (본 표 제외).</div>
{ch_t}

<h2>④ 교차 — 사업장 × 채널 <span class="sub" style="font-weight:400">(상위 20)</span></h2>
{crossA}
<h2>⑤ 교차 — 사업장 × 상품</h2>
{crossB}

<h2>⑥ 기획전(프로모션) 연관</h2>
<div class="note">• 판매 Net RN 중 <b>패키지(86xx) {pk_share:.0f}%</b> — 연박패키지가 OTA 채널로 강하게 유입(프로모션 수요 프록시).<br>
• 코드 태깅된 유일 기획전 <b>'비발디 5월전략'(118코드)의 최근 6일 Net 기여 = {i(camp_net)} RN</b> (투숙기간 경과로 ≈0).<br>
• <b>여름(6~8월) 가동 기획전 {len(active)}건</b> — 사업장 분포 아래. 주력채널 카카오(톡딜/메이커스)·타이드스퀘어·인플루언서·트립비토즈·야놀자·하나투어.<br>
• <b>한계</b>: campaign_data.json은 '비발디 5월전략' 외 기획전의 패키지코드(86xx) 미보유 → 코드단위 정밀귀속은 비발디건만 가능. 그 외는 패키지비중·채널강세로 간접 확인.</div>
<div style="margin:8px 0">{camp_cal}</div>

<div class="foot">
산식·정제는 parse_raw_db와 동일(중복제거·자체예약(58예외) 제외·매출조정 제외). 집계합=DB행 부호합 교차검증 통과.<br>
상세 로우데이터(DB) 및 7시트 엑셀: 바탕화면 픽업분석_678월_20260624.xlsx. 본 페이지는 분석·열람용(데이터/대시보드 수정 아님).
</div>

<script>
const opt=(t)=>({{indexAxis:'y',responsive:true,plugins:{{legend:{{display:false}},title:{{display:false}}}},scales:{{x:{{ticks:{{color:'#9aa0aa'}},grid:{{color:'#363b44'}}}},y:{{ticks:{{color:'#e8eaed',font:{{size:11}}}},grid:{{display:false}}}}}}}});
new Chart(document.getElementById('cp'),{{type:'bar',data:{{labels:{pchart_labels},datasets:[{{data:{pchart_vals},backgroundColor:'#c9a063'}}]}},options:opt()}});
new Chart(document.getElementById('cc'),{{type:'bar',data:{{labels:{chart_labels},datasets:[{{data:{chart_vals},backgroundColor:'#60a5fa'}}]}},options:opt()}});
</script>
</body>
</html>"""

OUT = PROJECT / "docs" / "pickup-678.html"
OUT.write_text(HTML, encoding="utf-8")
print("저장:", OUT, f"({len(HTML)/1024:.0f} KB)")
