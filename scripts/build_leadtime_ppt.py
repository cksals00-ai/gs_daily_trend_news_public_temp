#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""build_leadtime_ppt.py — 주말 블록 사전배정 분석 PPT (소노 ICN 표준 양식)
   입력: docs/data/leadtime_share.json   출력: docs/data/gs_leadtime.pptx
   용어: 리드타임→'예약~투숙 간격', 해외 OTA→GOTA / 국내→OTA."""
import json, os
from datetime import datetime
from pathlib import Path
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE
from pptx.oxml.ns import qn

ROOT = Path(__file__).resolve().parent.parent
SKILL = Path.home() / ".claude" / "skills" / "sono-ppt-format"
TPL = SKILL / "template_base.pptx"
DATA = json.load(open(ROOT / "docs" / "data" / "leadtime_share.json", encoding="utf-8"))
OUT = ROOT / "docs" / "data" / "gs_leadtime.pptx"

DARK = RGBColor(0x22,0x22,0x22); GRAY = RGBColor(0x7F,0x7F,0x7F); RED = RGBColor(0xC0,0x00,0x00)
BLUE = RGBColor(0x1F,0x4E,0x79); WHITE = RGBColor(0xFF,0xFF,0xFF); HDRBG = RGBColor(0x7F,0x7F,0x7F)
LRED = RGBColor(0xFB,0xE9,0xE9); LGREEN = RGBColor(0xE8,0xF2,0xEA)
F_B, F_R = "대명체 굵게", "대명체 보통"

prs = Presentation(str(TPL))
for sid in list(prs.slides._sldIdLst):
    prs.part.drop_rel(sid.rId); prs.slides._sldIdLst.remove(sid)
BLANK = prs.slide_masters[0].slide_layouts[12]

def new_slide():
    sl = prs.slides.add_slide(BLANK)
    for ph in list(sl.placeholders): ph._element.getparent().remove(ph._element)
    return sl
def _set_font(run, size, bold=False, color=DARK, name=None):
    f = run.font; f.size = Pt(size); f.bold = bold; f.color.rgb = color
    nm = name or (F_B if bold else F_R); f.name = nm
    rPr = run._r.get_or_add_rPr()
    for tag in ("a:latin","a:ea","a:cs"):
        e = rPr.find(qn(tag))
        if e is None: e = rPr.makeelement(qn(tag), {}); rPr.append(e)
        e.set("typeface", nm)
def txt(slide,x,y,w,h,parts,align=PP_ALIGN.LEFT,anchor=MSO_ANCHOR.TOP,line_spacing=None):
    box = slide.shapes.add_textbox(Inches(x),Inches(y),Inches(w),Inches(h)); tf = box.text_frame
    tf.word_wrap=True; tf.vertical_anchor=anchor
    tf.margin_left=tf.margin_right=tf.margin_top=tf.margin_bottom=0
    if parts and not isinstance(parts[0], list): parts=[parts]
    for i,para in enumerate(parts):
        p = tf.paragraphs[0] if i==0 else tf.add_paragraph(); p.alignment=align
        if line_spacing: p.line_spacing=line_spacing
        for (t,s,b,c) in para:
            r=p.add_run(); r.text=t; _set_font(r,s,b,c)
    return box
def header(sl,num,title):
    txt(sl,0.19,0.07,9.8,0.44,[(f"{num}. {title}",20,True,WHITE)])
def _border(cell,color="BFBFBF",w=9525):
    tcPr=cell._tc.get_or_add_tcPr()
    for tag in ("a:lnL","a:lnR","a:lnT","a:lnB"):
        old=tcPr.find(qn(tag))
        if old is not None: tcPr.remove(old)
        ln=tcPr.makeelement(qn(tag),{"w":str(w),"cap":"flat"})
        fill=ln.makeelement(qn("a:solidFill"),{}); clr=fill.makeelement(qn("a:srgbClr"),{"val":color})
        fill.append(clr); ln.append(fill); tcPr.append(ln)
def make_table(sl,x,y,w,rows,col_w,row_h=0.26,font=9,hdr_font=9,header_rows=1,unit=None,groups=None):
    if unit:
        txt(sl,x,y-0.21,w,0.19,[(f"({unit})",8,False,GRAY)],align=PP_ALIGN.RIGHT)
    merges_h,merges_v=[],[]
    if groups:
        leaf=rows[0]; grow=[]; newleaf=[]; ci=0
        for lab,span in groups:
            if lab is None:
                grow.append(leaf[ci]); newleaf.append(("",{})); merges_v.append(ci); ci+=1
            else:
                grow.append((lab,{})); grow.extend(("",{}) for _ in range(span-1))
                for k in range(span): newleaf.append(leaf[ci+k])
                if span>1: merges_h.append((ci,ci+span-1))
                ci+=span
        rows=[grow,newleaf]+rows[1:]; header_rows=2
    nr,nc=len(rows),len(rows[0])
    shp=sl.shapes.add_table(nr,nc,Inches(x),Inches(y),Inches(w),Inches(row_h*nr)); tbl=shp.table
    tblPr=tbl._tbl.find(qn("a:tblPr"))
    if tblPr is not None: tblPr.set("firstRow","0"); tblPr.set("bandRow","0")
    tot=sum(col_w)
    for i,cw in enumerate(col_w): tbl.columns[i].width=Emu(int(Inches(w)*cw/tot))
    for ri in range(nr):
        tbl.rows[ri].height=Inches(row_h*0.8 if (groups and ri<2) else row_h)
        for ci in range(nc):
            item=rows[ri][ci]
            if item is None: continue
            text,opts=(item if isinstance(item,tuple) else (item,{}))
            cell=tbl.cell(ri,ci)
            cell.margin_left=cell.margin_right=Inches(0.03); cell.margin_top=cell.margin_bottom=Inches(0.008)
            cell.vertical_anchor=MSO_ANCHOR.MIDDLE
            if ri<header_rows: cell.fill.solid(); cell.fill.fore_color.rgb=HDRBG
            elif opts.get("fill"): cell.fill.solid(); cell.fill.fore_color.rgb=opts["fill"]
            else: cell.fill.solid(); cell.fill.fore_color.rgb=WHITE
            _border(cell)
            tfr=cell.text_frame; tfr.word_wrap=True; p=tfr.paragraphs[0]
            if ri<header_rows: p.alignment=PP_ALIGN.CENTER
            elif opts.get("align"): p.alignment=opts["align"]
            elif ci==0: p.alignment=PP_ALIGN.LEFT
            else: p.alignment=PP_ALIGN.RIGHT
            r=p.add_run(); r.text=str(text)
            if ri<header_rows: _set_font(r,hdr_font,True,WHITE)
            else: _set_font(r,opts.get("size",font),opts.get("b",False),opts.get("color",DARK))
    for (a,b) in merges_h: tbl.cell(0,a).merge(tbl.cell(0,b))
    for c in merges_v: tbl.cell(0,c).merge(tbl.cell(1,c))
    return shp

# ── 집계 헬퍼 ──
ROOMS = DATA["meta"]["rooms"]
SITES = sorted(ROOMS, key=lambda s:-ROOMS[s])
IDX = {(r["year"],r["month"],r["site"],r["seg"],r.get("basis","ALL"),r["daytype"]):r for r in DATA["rows"]}
def _basis(dt):
    # 통합 구조(INT): 주중(일 포함)/금/토/연휴/스페셜, ALL = 전체
    if dt == "ALL": return "ALL"
    return "INT"
def agg(y,site,seg,dt):
    rn=avdt=ltw=0
    for m in range(1,11):
        r=IDX.get((y,m,site,seg,_basis(dt),dt))
        if not r: continue
        rn+=r["rn"]; avdt+=r["av_dt"]
        if r["lead"] is not None: ltw+=r["lead"]*r["rn"]
    return {"rn":rn,"occ":rn/avdt if avdt else 0,"lead":ltw/rn if rn else None}
def f(n): return f"{round(n):,}"
def p1(o): return f"{o*100:.1f}%"
SHORT = lambda s: s.replace(" 비발디파크"," 비발").replace("소노펠리체 빌리지 비발","펠리체빌리지 비발")

# ── 표지 ──
sl=new_slide()
txt(sl,0,2.55,10.83,0.9,[("주말 블록 사전배정 분석",27,True,DARK)],align=PP_ALIGN.CENTER)
r=sl.shapes.add_shape(MSO_SHAPE.RECTANGLE,Inches(5.055),Inches(3.5),Inches(0.72),Inches(0.03))
r.fill.solid(); r.fill.fore_color.rgb=RED; r.line.fill.background(); r.shadow.inherit=False
txt(sl,0,3.62,10.83,0.5,[("온라인 채널(OTA·GOTA) 예약~투숙 간격 × 가용비중 · 2024~2026(1~10월)",16,False,RGBColor(0x59,0x59,0x59))],align=PP_ALIGN.CENTER)
txt(sl,0,4.25,10.83,0.3,[(datetime.now().strftime("%Y.%m.%d")+" · GS 영업기획",11,False,GRAY)],align=PP_ALIGN.CENTER)

# ── 1. 핵심 요약 ──
sl=new_slide(); header(sl,1,"핵심 요약 — 온라인(OTA·GOTA) 토요일")
yrs=[2024,2025,2026]
rowsy=[[("구분",{}),("2024",{}),("2025",{}),("2026",{}),("'24→'26",{})]]
mets=[("RN(실적)",lambda a:f(a["rn"]),"rn"),("점유",lambda a:p1(a["occ"]),"occ"),("예약~투숙 간격",lambda a:(f"{a['lead']:.0f}일" if a['lead'] else "-"),"lead")]
A={y:agg(y,"전사","ONLINE","토") for y in yrs}
for nm,fn,key in mets:
    chg="-"
    if key=="rn" and A[2024]["rn"]: chg=f"+{(A[2026]['rn']/A[2024]['rn']-1)*100:.0f}%"
    elif key=="occ": chg=f"+{(A[2026]['occ']-A[2024]['occ'])*100:.1f}%p"
    elif key=="lead" and A[2024]["lead"]: chg=f"{A[2026]['lead']-A[2024]['lead']:+.0f}일"
    rowsy.append([(nm,{}),(fn(A[2024]),{}),(fn(A[2025]),{}),(fn(A[2026]),{"b":True,"color":BLUE}),(chg,{"color":RED,"b":True})])
make_table(sl,0.45,1.3,10.0,rowsy,[2.2,1.2,1.2,1.2,1.3],row_h=0.42,font=12,hdr_font=12)
msgs=[
 ("온라인 실적은 3년 연속 증가 — 토요일 RN "+f(A[2024]['rn'])+" → "+f(A[2026]['rn'])+"실(점유 "+p1(A[2026]['occ'])+")",),
 ("예약~투숙 간격 평균 "+(f"{A[2026]['lead']:.0f}" if A[2026]['lead'] else '-')+"일 — 블록을 투숙 약 "+(f"{A[2026]['lead']:.0f}" if A[2026]['lead'] else '-')+"일 전엔 풀어야 온라인이 소진",),
 ("→ 점유 높고 간격 짧은 영업장은 블록 확대가 유리 (다음 장 랭킹)",),
]
txt(sl,0.45,3.35,10.0,2.0,[[("■ 해석",13,True,DARK)]]+[[("· ",11,False,RED),(m[0],11,False,DARK)] for m in msgs],line_spacing=1.35)
txt(sl,0.45,6.95,10.0,0.3,[("※ net 예약(취소 제외) · 가용 = 사업장 객실수 11,606실 × 요일수(2026 고정) · 1~10월",8.5,False,GRAY)])

# ── 2. 블록 기회 랭킹 ──
sl=new_slide(); header(sl,2,"블록 기회 랭킹 — 2026 토요일 · OTA+GOTA")
rank=sorted(((s,agg(2026,s,"ONLINE","토")) for s in SITES), key=lambda kv:-kv[1]["occ"])
rows2=[[("사업장",{}),("점유",{}),("예약~투숙 간격",{}),("RN(실적)",{}),("판정",{})]]
for s,a in rank:
    if a["occ"]>=0.15: verdict,fill=("확대 유리",LGREEN)
    elif a["occ"]>=0.08: verdict,fill=("적정",None)
    else: verdict,fill=("회원우선",LRED)
    lead=f"{a['lead']:.0f}일" if a["lead"] else "-"
    rows2.append([(SHORT(s),{"fill":fill}),(p1(a["occ"]),{"fill":fill,"b":a["occ"]>=0.15,"color":BLUE if a["occ"]>=0.15 else DARK}),
                  (lead,{"fill":fill}),(f(a["rn"]),{"fill":fill}),(verdict,{"fill":fill,"b":True,"color":RED if verdict=="확대 유리" else (GRAY if verdict=="회원우선" else DARK)})])
make_table(sl,0.45,1.3,10.0,rows2,[3.0,1.3,1.8,1.5,1.5],row_h=0.228,font=9.5,hdr_font=10)
txt(sl,0.45,6.95,10.0,0.3,[("※ 점유 = 온라인 RN / (객실수×토요일수) · 간격 짧을수록 빠른 소진 · 판정: 점유 15%↑ 확대 / 8~15% 적정 / 8%↓ 회원우선",8.5,False,GRAY)])

# ── 3. 사업장 × 요일 점유 ──
sl=new_slide(); header(sl,3,"사업장 × 요일 점유 — 2026 · OTA+GOTA")
dts=["주중","금","토","연휴","스페셜"]
rows3=[[("사업장",{})]+[(d,{}) for d in dts]+[("RN합",{})]]
ordered=sorted(SITES,key=lambda s:-agg(2026,s,"ONLINE","ALL")["rn"])
for s in ordered:
    cells=[(SHORT(s),{})]
    for d in dts:
        a=agg(2026,s,"ONLINE",d); o=a["occ"]
        fill=None
        if d in ("금","토","주중") and o>=0.18: fill=LGREEN
        cells.append((p1(o),{"fill":fill,"b":o>=0.18,"color":BLUE if o>=0.18 else DARK}))
    cells.append((f(agg(2026,s,"ONLINE","ALL")["rn"]),{}))
    rows3.append(cells)
make_table(sl,0.45,1.3,10.0,rows3,[3.0,1.1,1.1,1.1,1.1,1.1,1.4],row_h=0.218,font=8.5,hdr_font=9.5)
txt(sl,0.45,6.95,10.0,0.3,[("※ 통합 구조(요금시즌, 일요일 주중편입) 기준 · 셀 = 점유(온라인 RN / 객실수×요일수) · 초록=18%↑ · 연휴·스페셜은 참고",8.5,False,GRAY)])

# ── 4. 권고 ──
sl=new_slide(); header(sl,4,"권고 — 블록 운영 전략")
top=[s for s,a in rank if a["occ"]>=0.15][:6]
low=[s for s,a in rank if a["occ"]<0.08]
recs=[
 ("블록 확대 영업장", ", ".join(SHORT(s) for s in top) or "-",
  "토요일 온라인 점유 15%↑ · 간격 짧아 빠른 소진 → 회원 미소진분을 선제 배정하면 온라인이 채움"),
 ("회원 우선 유지", ", ".join(SHORT(s) for s in low) or "-",
  "온라인 점유 8%↓ · 수요 약함 → 블록보다 회원·직판 우선"),
 ("릴리즈 타이밍", "투숙 약 "+(f"{A[2026]['lead']:.0f}" if A[2026]['lead'] else '-')+"일 전",
  "온라인 예약~투숙 간격 기준 — 회원 선점 종료를 이보다 늦추면 온라인이 못 채움"),
 ("단독 기획전 레버리지", "수요 과다(점유 상위) 영업장",
  "없어서 못 파는 블록은 특정 채널(아고다·여기어때 등)에 단독 지급 → 소노 단독 기획전으로 전환"),
]
y=1.35
for t,who,desc in recs:
    r=sl.shapes.add_shape(MSO_SHAPE.RECTANGLE,Inches(0.45),Inches(y),Inches(0.08),Inches(1.05))
    r.fill.solid(); r.fill.fore_color.rgb=RED; r.line.fill.background(); r.shadow.inherit=False
    txt(sl,0.68,y+0.03,9.8,0.9,[[("■ "+t+"  ",13,True,DARK),(who,12,True,BLUE)],[(desc,11,False,RGBColor(0x40,0x40,0x40))]],line_spacing=1.25)
    y+=1.28
txt(sl,0.45,6.95,10.0,0.3,[("※ 대시보드 시뮬레이터에서 영업장·요일·블록량 입력 시 예상 소진율·판정 즉시 확인 가능",8.5,False,GRAY)])

prs.save(str(OUT))
print("✓ PPT:", OUT, f"({len(prs.slides._sldIdLst)} slides)")
