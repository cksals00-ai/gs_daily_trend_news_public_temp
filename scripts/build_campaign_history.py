#!/usr/bin/env python3
"""
build_campaign_history.py — 기획전·상품 실적 매핑 큐브 (회원명 백본, 전 연도 소급 가능)

배경/로직 (세션 합의):
- 86XX(회원번호) 온북 행을 회원명(상품명) 백본으로 집계 → 코드 등록 없이도 기획전/상품 실적 매핑.
- 계층 키:
    1) 기획전(campaign) = 패키지분류코드명(idx '패키지분류코드명')  ※ 2025·2026 신스키마에만 존재
    2) 상품계열(product) = 회원명 → parse_package_trend.normalize_series() (채널/연도/사업장/박수 제거, 전 연도)
- 분해 차원: 세그먼트(변경예약집계코드명) · 거래처(AGENT명) · 사업장(변경사업장명) · 판매월/투숙월 · 연도(YoY) · 금액.
- 온북 27(대매점/OTA) + 43(Inbound)만 사용(28/44 취소는 취소율 참고 별도, 본 큐브는 유효예약).
- 교차파일 재전송 중복은 전역 라인해시로 제거.

관행(기존 parse_campaign86와 통일):
- RN = 객실수 (이 DB는 객실수≈1, 연박은 행 분할)
- 객실매출 = 1박객실료 × 객실수 / 1.1 (VAT 제외)
- 총매출   = PKG패키지총금액 / 1.1 (없으면 판매가, 없으면 1박객실료)

출력: docs/data/campaign_history.json
CLI: --years 2025,2026 (기본) | --years all
"""
from __future__ import annotations
import fs_utils  # macOS NFD→NFC
import os, sys, json, glob, argparse, logging, re, math
from pathlib import Path
from collections import defaultdict

from parse_package_trend import normalize_series  # 회원명 → 상품계열 정규화 (canonical)
from parse_campaign86 import normalize_property    # 사업장 정규화 (canonical)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
OUTPUT_JSON = PROJECT_DIR / "docs" / "data" / "campaign_history.json"
CATALOG_JSON = PROJECT_DIR / "docs" / "data" / "campaign_catalog.json"

# ── 유사도 매칭용 토크나이저 (JS campaign_match.js 와 동일 규칙 유지) ──
_TOK_BRACKET = re.compile(r"[\[\(#<][^\]\)#>]*[\]\)#>]")          # [..] (..) #..# <..>
_TOK_NOISE   = re.compile(r"\bg-?ota\d*\b|\bota\d*\b|\bg\d{1,2}\b|\b\d{2}y\b", re.I)
_TOK_SPLIT   = re.compile(r"[^가-힣a-z0-9]+", re.I)
STOPWORDS = set("pkg ota gota g15 ro hp cc 패키지 코드 전체 전체번호 상품 통합 정규 객실 룸 박 "
                "nights night 2nights 전사 외 등 및 the and for".split())

def tokenize(name: str):
    s = (name or "").lower()
    s = _TOK_BRACKET.sub(" ", s)
    s = _TOK_NOISE.sub(" ", s)
    out = []
    for t in _TOK_SPLIT.split(s):
        if len(t) >= 2 and t not in STOPWORDS and not t.isdigit():
            out.append(t)
    return out

ALL_YEARS = ["2022", "2023", "2024", "2025", "2026"]
PRODUCT_MIN_RN = 10        # by_product 보관 임계 (long tail 은 _tail 집계로만)
AGENT_TOPN = 30            # 카탈로그/by_agent 보관 한도(나머지 _tail)

# ── 세그먼트 매핑 (변경예약집계코드명 우선, 코드 보조) ──
SEG_BY_NAME = {
    "온라인 패키지": "OTA",
    "GOTA PKG": "G-OTA",
    "GOTA R/O": "G-OTA",
    "회원PKG": "회원",
    "자사 패키지": "무기명",
    "사업장 대매점": "대매점",
    "D멤버스": "D멤버스",
    "제휴사PKG": "제휴",
}
SEG_BY_CODE = {
    "72": "OTA", "A5": "G-OTA", "A4": "G-OTA", "MP": "회원",
    "73": "무기명", "53": "대매점", "34": "D멤버스", "CP": "제휴", "58": "Inbound",
}

def classify_seg(code, name, is_inbound_file):
    if is_inbound_file or code == "58":
        return "Inbound"
    if name in SEG_BY_NAME:
        return SEG_BY_NAME[name]
    if code in SEG_BY_CODE:
        return SEG_BY_CODE[code]
    return name.strip() if name.strip() else "미분류"


def parse_agent(agent):
    """AGENT명 → (거래처명, 세그힌트). 'OTA_놀유니버스(야놀자)' → ('놀유니버스(야놀자)','OTA')"""
    a = (agent or "").strip()
    if not a:
        return ("직판/회원", None)
    seg = None
    for pre, s in (("OTA_", "OTA"), ("GOTA_", "G-OTA"), ("마케팅_", "마케팅")):
        if a.startswith(pre):
            seg = s
            a = a[len(pre):]
            break
    return (a.strip() or "직판/회원", seg)


def find_raw_db() -> Path:
    cands = [PROJECT_DIR / "data" / "raw_db", PROJECT_DIR.parents[2] / "data" / "raw_db"]
    for c in cands:
        if (c / "2026").exists() or (c / "2025").exists():
            return c
    for c in cands:
        if c.exists():
            return c
    raise FileNotFoundError(f"raw_db 없음: {cands}")


def new_node():
    return {
        "rn": 0, "room_rev": 0, "total_rev": 0, "commission": 0,
        "by_sale_month": defaultdict(int),
        "by_stay_month": defaultdict(int),
        "by_segment": defaultdict(lambda: {"rn": 0, "rev": 0}),
        "by_agent": defaultdict(lambda: {"rn": 0, "rev": 0}),
        "by_property": defaultdict(lambda: {"rn": 0, "rev": 0}),
        "by_year": defaultdict(lambda: {"rn": 0, "rev": 0}),
    }


def add_row(node, year, sm, stm, seg, agent, prop, rn, room_rev, total_rev, comm):
    node["rn"] += rn
    node["room_rev"] += room_rev
    node["total_rev"] += total_rev
    node["commission"] += comm
    if sm: node["by_sale_month"][sm] += rn
    if stm: node["by_stay_month"][stm] += rn
    node["by_segment"][seg]["rn"] += rn; node["by_segment"][seg]["rev"] += room_rev
    node["by_agent"][agent]["rn"] += rn; node["by_agent"][agent]["rev"] += room_rev
    node["by_property"][prop]["rn"] += rn; node["by_property"][prop]["rev"] += room_rev
    node["by_year"][year]["rn"] += rn; node["by_year"][year]["rev"] += room_rev


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default="2025,2026")
    args = ap.parse_args()
    years = ALL_YEARS if args.years.strip().lower() == "all" else [y.strip() for y in args.years.split(",") if y.strip()]

    raw = find_raw_db()
    logger.info(f"raw_db: {raw} / years={years}")

    by_campaign = defaultdict(new_node)   # 패키지분류코드명
    by_product = defaultdict(new_node)    # 회원명 정규화
    seg_cat = defaultdict(lambda: {"rn": 0, "rev": 0})
    agent_cat = defaultdict(lambda: {"rn": 0, "rev": 0, "seg": None})

    seen = set()  # 전역 라인해시(재전송 중복 제거)
    total_rows = 0; dup_drop = 0; no_pkgclass = 0

    for year in years:
        ydir = raw / year
        if not ydir.exists():
            continue
        for fname in sorted(os.listdir(ydir)):
            if not (fname.startswith("27.") or fname.startswith("43.")):
                continue
            is_inbound = fname.startswith("43.")
            fpath = ydir / fname
            try:
                fh = open(fpath, encoding="cp949", errors="replace")
            except Exception as e:
                logger.warning(f"  열기 실패 {fname}: {e}"); continue
            hdr = [h.strip() for h in fh.readline().rstrip("\n").split(";")]
            idx = {h: i for i, h in enumerate(hdr)}
            i_mem = idx.get("회원번호", 5); i_name = idx.get("회원명", 6)
            i_sale = idx.get("판매일자", 1); i_stay = idx.get("입실일자", 33)
            i_cprop = idx.get("변경사업장명", -1); i_prop = idx.get("영업장명", 3)
            i_cnum = idx.get("변경예약집계코드", -1); i_cname = idx.get("변경예약집계코드명", -1)
            i_agent = idx.get("AGENT명", -1)
            i_rooms = idx.get("객실수", 28); i_rate = idx.get("1박객실료", 26)
            i_pkg = idx.get("PKG패키지총금액", -1); i_sell = idx.get("판매가", -1)
            i_comm = idx.get("수수료", -1); i_pcls = idx.get("패키지분류코드명", -1)

            fcount = 0
            for line in fh:
                ls = line.rstrip("\n\r")
                p = ls.split(";")
                if len(p) <= i_mem:
                    continue
                if not p[i_mem].strip().startswith("86"):
                    continue
                h = hash(ls)
                if h in seen:
                    dup_drop += 1; continue
                seen.add(h)

                def g(i):
                    return p[i].strip() if 0 <= i < len(p) else ""
                def gi(i):
                    v = g(i)
                    try: return int(v) if v else 0
                    except ValueError: return 0

                rooms = gi(i_rooms); rn = rooms if rooms > 0 else 1
                rate = gi(i_rate)
                room_rev = int(rate * rn / 1.1)
                pkg = gi(i_pkg); sell = gi(i_sell)
                total_amt = pkg if pkg > 0 else (sell if sell > 0 else rate)
                total_rev = int(total_amt / 1.1)
                comm = gi(i_comm)

                sale = g(i_sale); sm = sale[:6] if len(sale) >= 6 else ""
                stay = g(i_stay); stm = stay[:6] if len(stay) >= 6 else ""
                seg = classify_seg(g(i_cnum), g(i_cname), is_inbound)
                agent, _ = parse_agent(g(i_agent))
                cprop = g(i_cprop); prop = normalize_property(cprop) if cprop else normalize_property(g(i_prop))
                mem_name = g(i_name)
                product = normalize_series(mem_name)
                campaign = g(i_pcls)

                add_row(by_product[product], year, sm, stm, seg, agent, prop, rn, room_rev, total_rev, comm)
                if campaign:
                    add_row(by_campaign[campaign], year, sm, stm, seg, agent, prop, rn, room_rev, total_rev, comm)
                else:
                    no_pkgclass += 1
                seg_cat[seg]["rn"] += rn; seg_cat[seg]["rev"] += room_rev
                ac = agent_cat[agent]; ac["rn"] += rn; ac["rev"] += room_rev
                total_rows += 1; fcount += 1
            logger.info(f"  {year}/{fname[:46]}: {fcount:,}행")

    logger.info(f"집계 86행: {total_rows:,} / 재전송중복제거 {dup_drop:,} / 패키지분류공란 {no_pkgclass:,}")

    # ── 직렬화 ──
    M = 1_000_000
    def fin_node(n):
        rn = n["rn"]
        def dim(d):
            return {k: {"rn": v["rn"], "rev_m": round(v["rev"]/M, 2)}
                    for k, v in sorted(d.items(), key=lambda kv: -kv[1]["rn"])}
        agents = sorted(n["by_agent"].items(), key=lambda kv: -kv[1]["rn"])
        agent_out = {k: {"rn": v["rn"], "rev_m": round(v["rev"]/M, 2)} for k, v in agents[:AGENT_TOPN]}
        tail = agents[AGENT_TOPN:]
        if tail:
            agent_out["_tail"] = {"rn": sum(v["rn"] for _, v in tail),
                                  "rev_m": round(sum(v["rev"] for _, v in tail)/M, 2),
                                  "_count": len(tail)}
        return {
            "rn": rn,
            "room_rev_m": round(n["room_rev"]/M, 2),
            "total_rev_m": round(n["total_rev"]/M, 2),
            "commission_m": round(n["commission"]/M, 2),
            "adr": round(n["room_rev"]/rn) if rn else 0,
            "by_sale_month": dict(sorted(n["by_sale_month"].items())),
            "by_stay_month": dict(sorted(n["by_stay_month"].items())),
            "by_year": {y: {"rn": v["rn"], "rev_m": round(v["rev"]/M, 2)} for y, v in sorted(n["by_year"].items())},
            "by_segment": dim(n["by_segment"]),
            "by_property": dim(n["by_property"]),
            "by_agent": agent_out,
        }

    camp_out = {k: fin_node(v) for k, v in sorted(by_campaign.items(), key=lambda kv: -kv[1]["rn"])}

    prod_items = sorted(by_product.items(), key=lambda kv: -kv[1]["rn"])
    prod_kept = [(k, v) for k, v in prod_items if v["rn"] >= PRODUCT_MIN_RN and k != "기타"]
    prod_tail = [(k, v) for k, v in prod_items if (v["rn"] < PRODUCT_MIN_RN or k == "기타")]
    prod_out = {k: fin_node(v) for k, v in prod_kept}
    tail_rn = sum(v["rn"] for _, v in prod_tail)
    tail_rev = sum(v["room_rev"] for _, v in prod_tail)

    output = {
        "meta": {
            "years": years,
            "total_rows": total_rows,
            "dedup_dropped": dup_drop,
            "rows_without_campaign": no_pkgclass,
            "campaigns": len(camp_out),
            "products_kept": len(prod_out),
            "products_tail_count": len(prod_tail),
            "products_tail_rn": tail_rn,
            "products_tail_rev_m": round(tail_rev/M, 2),
            "product_min_rn": PRODUCT_MIN_RN,
            "note": "패키지분류코드명=2025·2026만 존재 / 회원명 정규화=전 연도 / RN=객실수, 객실매출=1박객실료×객실수/1.1",
        },
        "segments": {k: {"rn": v["rn"], "rev_m": round(v["rev"]/M, 2)}
                     for k, v in sorted(seg_cat.items(), key=lambda kv: -kv[1]["rn"])},
        "agents": {k: {"rn": v["rn"], "rev_m": round(v["rev"]/M, 2)}
                   for k, v in sorted(agent_cat.items(), key=lambda kv: -kv[1]["rn"])[:60]},
        "by_campaign": camp_out,
        "by_product": prod_out,
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(output, ensure_ascii=False, indent=1), encoding="utf-8")
    logger.info(f"✓ 저장: {OUTPUT_JSON}")
    logger.info(f"  기획전(패키지분류) {len(camp_out)}개 / 상품계열 {len(prod_out)}개(+tail {len(prod_tail)}) / "
                f"총 RN {total_rows:,}")

    # ── 유사도 매칭용 카탈로그 (압축·토큰화) ──
    build_catalog(camp_out, prod_out)


def build_catalog(camp_out, prod_out):
    """campaign_history 의 노드들을 유사도 매칭용으로 압축: 토큰 + idf + 핵심 실적."""
    def mm_set(node):
        return sorted({m[4:6] for m in node.get("by_stay_month", {}) if len(m) >= 6})
    def top_dim(d, n=3):
        return [[k, v["rn"]] for k, v in list(d.items())[:n] if k != "_tail"]

    entries = []
    for name, node in camp_out.items():
        toks = tokenize(name)
        if not toks:
            continue
        entries.append({
            "name": name, "type": "campaign", "tokens": toks,
            "rn": node["rn"], "rev_m": node["room_rev_m"], "adr": node["adr"],
            "by_year": node["by_year"], "months": mm_set(node),
            "prop": next(iter(node["by_property"]), ""),
            "segs": top_dim(node["by_segment"]), "agents": top_dim(node["by_agent"]),
        })
    # 상품계열은 RN 상위 + 의미토큰 보유한 것만(노이즈 억제), 최대 300
    prod_sorted = sorted(prod_out.items(), key=lambda kv: -kv[1]["rn"])
    pc = 0
    for name, node in prod_sorted:
        if pc >= 300:
            break
        toks = tokenize(name)
        if len(toks) < 1:
            continue
        entries.append({
            "name": name, "type": "product", "tokens": toks,
            "rn": node["rn"], "rev_m": node["room_rev_m"], "adr": node["adr"],
            "by_year": node["by_year"], "months": mm_set(node),
            "prop": next(iter(node["by_property"]), ""),
            "segs": top_dim(node["by_segment"]), "agents": top_dim(node["by_agent"]),
        })
        pc += 1

    # idf (엔트리 토큰 집합 기준)
    N = len(entries)
    df = defaultdict(int)
    for e in entries:
        for t in set(e["tokens"]):
            df[t] += 1
    idf = {t: round(math.log((N + 1) / (c + 1)) + 1.0, 4) for t, c in df.items()}

    catalog = {
        "meta": {"entries": N, "campaigns": sum(1 for e in entries if e["type"] == "campaign"),
                 "products": sum(1 for e in entries if e["type"] == "product"),
                 "default_idf": round(math.log(N + 1) + 1.0, 4),
                 "note": "유사도 매칭용 토큰 카탈로그 / score=Σidf(공통토큰)/Σidf(질의토큰)+사업장·시즌 보정"},
        "idf": idf,
        "entries": entries,
    }
    CATALOG_JSON.write_text(json.dumps(catalog, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    logger.info(f"✓ 카탈로그: {CATALOG_JSON.name} ({N}엔트리, idf {len(idf)}토큰)")


if __name__ == "__main__":
    main()
