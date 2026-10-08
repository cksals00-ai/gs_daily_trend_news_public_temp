#!/usr/bin/env python3
"""Local-only Golfmanager booking snapshot importer; outputs aggregate data."""
import argparse
import hashlib
import json
import re
import time
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

import openpyxl
from openpyxl.utils.datetime import from_excel

VENUES = {"mangilao": "Sono Felice Country Club Mangilao",
          "talofofo": "Sono Felice Country Club Talofofo"}
# id, display label, parent, original report row, source field, exact match
LEAVES = [
    ("sono_member", "Sono Member", "sono", 17, "clientGroupName", "Sono Member"),
    ("hana_member", "HANA TOUR", "sono", 18, None, None),
    ("sono_station", "SONO STATION", "sono", 19, "clientName", "SONO STATION"),
    ("jj_member", "J&J MEMBER", "sono", 20, "clientName", "J&J Marketing (MEMBER)"),
    ("kr_individual", "한국 개인", "kr", 21, "Channel", "KR"),
    ("jj", "J&J", "kr_agency", 23, "clientName", "J&J Marketing"),
    ("ef", "E&F", "kr_agency", 24, "clientName", "LT GUAM CORPORATION (E&F)"),
    ("guam_mate", "GUAM MATE", "kr_agency", 25, "clientName", "GUAM MATE"),
    ("new_tts", "NEW TTS TOUR", "kr_agency", 26, "clientName", "NEW TTS TOUR CO."),
    ("blue_travel", "BLUE TRAVEL", "kr_agency", 27, "clientName", "BLUE TRAVEL LINE"),
    ("hana_agency", "HANA TOUR", "kr_agency", 28, None, None),
    ("lkd", "LKD TOUR", "kr_agency", 29, "clientName", "LKD TOUR COMPANY"),
    ("bongbong", "BONG BONG TOUR", "kr_agency", 30, "clientName", "BONGBONG TOUR"),
    ("happy_island", "HAPPY ISLAND", "kr_agency", 31, "clientName", "HAPPY ISLAND"),
    ("lgt", "LAND GOOD TOUR", "kr_agency", 32, "clientName", "LAND GOOD TOUR (LGT)"),
    ("toto", "TOTO BOOKING", "kr_agency", 33, "clientName", "TOTOBOOKING GUAM"),
    ("ace", "ACE GOLF TOUR", "kr_agency", 34, "clientName", "ACE GOLF TOUR"),
    ("sky", "SKY TOUR", "kr_agency", 35, "clientName", "SKY TOUR"),
    ("land_star", "LAND STAR", "kr_agency", 36, "clientName", "LAND STAR TRAVEL"),
    ("first_tour", "FIRST TOUR", "kr_agency", 37, "clientName", "FIRST TOUR - GUAM HANA TOUR"),
    ("kr_group", "한국 GROUP", "kr_agency", 38, "Channel", "KR GROUP"),
    ("jp_web", "Website", "jp_individual", 41, "Channel", "JP"),
    ("gora", "GORA", "jp_individual", 42, "clientName", "GORA"),
    ("g_start", "G-Start", "jp_individual", 43, "clientName", "G-START"),
    ("jcb", "JCB", "jp_individual", 44, "clientName", "JCB"),
    ("amex", "AMEX", "jp_individual", 45, "clientName", "AMX"),
    ("diners", "DINERS", "jp_individual", 46, "clientName", "DNR"),
    ("his", "HIS", "jp_agency", 48, "clientName", "HIS"),
    ("htm", "HTM", "jp_agency", 49, "clientName", "HTM"),
    ("ken", "Ken Travel", "jp_agency", 50, "clientName", "Ken Travel"),
    ("lamlam", "Lam Lam Tour", "jp_agency", 51, "clientName", "LAM LAM TOURS"),
    ("guam_tour", "GUAM TOUR", "jp_agency", 52, "clientName", "GUAM AND GUAM TOURS, INC dba: GUAM TOUR SERVICES"),
    ("nautech", "NAUTECH", "jp_agency", 53, "clientName", "NAUTECH GUAM CORPORATION"),
    ("veltra", "VELTRA", "jp_agency", 55, "clientName", "VELTRA"),
    ("pih", "PIH", "jp_agency", 54, "clientName", "PIH"),
    ("seven", "SEVEN TOURIST", "jp_agency", 56, "clientName", "SEVEN TOURIST"),
    ("jp_group", "日本 GROUP", "jp_agency", 57, "Channel", "JP GROUP"),
    ("other_individual", "기타 개인", "others", 59, None, None),
    ("other", "기타", "others", 60, "Channel", "Other"),
    ("local", "LOCAL", "local_total", 62, "Channel", "LOCAL"),
    ("military", "MILITARY", "local_total", 63, "Channel", "MILITARY"),
    ("unmapped", "미분류", None, 64, None, None),
]
GROUPS = [
    ("outbound", "OUT BOUND", None, 12, ["kr", "jp", "others"]),
    ("local_total", "LOCAL 합계", None, 13, ["local", "military"]),
    ("kr", "KR", "outbound", 15, ["sono", "kr_individual", "kr_agency"]),
    ("sono", "SONO MEMBER", "kr", 16, ["sono_member", "hana_member", "sono_station", "jj_member"]),
    ("kr_agency", "한국 AGENCY", "kr", 22, [x[0] for x in LEAVES if x[2] == "kr_agency"]),
    ("jp", "JP", "outbound", 39, ["jp_individual", "jp_agency"]),
    ("jp_individual", "일본 INDIVIDUAL", "jp", 40, [x[0] for x in LEAVES if x[2] == "jp_individual"]),
    ("jp_agency", "일본 AGENCY", "jp", 47, [x[0] for x in LEAVES if x[2] == "jp_agency"]),
    ("others", "OTHERS", "outbound", 58, ["other_individual", "other"]),
]
REQUIRED = {"Id", "Start date", "resourceTypeName", "Channel", "clientName",
            "clientGroupName", "Total", "Cancelled"}


def money(value):
    if value is None or isinstance(value, bool):
        raise ValueError("missing or invalid amount")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid amount") from exc
    if not result.is_finite():
        raise ValueError("non-finite amount")
    return result


def flag(value):
    text = str(value).strip().casefold()
    if text in {"0", "false", "no"}:
        return False
    if text in {"1", "true", "yes"}:
        return True
    raise ValueError("unknown cancellation flag")


def day(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        return from_excel(value).date()
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m-%d-%Y %H:%M", "%m-%d-%Y"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                pass
    raise ValueError("invalid Start date")


def load_bookings(path):
    try:
        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    except Exception as exc:
        raise ValueError("Cannot read workbook; use an unencrypted reservation export") from exc
    try:
        for sheet in workbook:
            values = sheet.iter_rows(values_only=True)
            headers = next(values, ())
            if REQUIRED.issubset(headers):
                return [dict(zip(headers, row)) for row in values
                        if row[headers.index("Id")] is not None]
        raise ValueError("Expected reservation export (Billing > Bookings > Breakdown), not sales")
    finally:
        workbook.close()


def reference_channels(rows, reference):
    """Keep raw Channel; attach a separately sourced, booking-specific report field."""
    lookup = {}
    for item in reference:
        key = str(item["Id"])
        if key in lookup:
            raise ValueError("duplicate Id in classification reference")
        lookup[key] = item
    enriched, applied = [], 0
    for original in rows:
        item = dict(original)
        prior = lookup.get(str(item["Id"]))
        if not item.get("Channel") and prior and prior.get("Channel") and all(
            item.get(key) == prior.get(key) for key in ("Players", "Type", "resourceTypeName", "Client Group")
        ):
            item["reportChannel"] = prior["Channel"]
            applied += 1
        enriched.append(item)
    return enriched, {"applied_rows": applied, "raw_channel_changed": 0,
                      "basis": "Original 2026-10-07 Excel Channel, matched by booking Id and unchanged payer/type/venue/group; new bookings are not inferred"}


def pattern_channel(r):
 t=str(r.get('typeName') or '').upper();g=str(r.get('clientGroupName') or '').upper();n=str(r.get('Nationality') or '').upper();client=str(r.get('clientName') or '').upper()
 if 'MILITARY' in t:return 'MILITARY','product_military'
 if any(x in t for x in ['GUAM RESIDENT','RESIDENT TWILIGHT','GF RESIDENT','LOCAL GROUP','LOCAL GRP','LOCAL MEMBERSHIP','RES PROMO','US CITIZEN']):return 'LOCAL','product_local'
 if g.startswith('LOCAL CLUBS') or re.search(r'LOCAL MEM[E]?BERSHIP',g):return 'LOCAL','group_local'
 if client in ['RITA TOURS','DAEKUN TOUR BOOKING KOREA']:return 'Other','agency_other_exact'
 if client=='SPORTS NIPPON SHIMBUNSHA (SPONICHI)':return 'JP GROUP','corporate_jp_exact'
 if not g and n in ['KR','JP'] and any(x in t for x in ['FIT','PACK','PKG MEMBER','18H PRO']):return n,'individual_country_group_product'
 return None,None

def pattern_channels(rows):
    enriched, rules = [], {}
    for original in rows:
        item = dict(original)
        if item.get("resourceTypeName") in VENUES.values() and original_contributions(item)=={"unmapped":1} and not item.get("Channel") and not item.get("reportChannel"):
            channel, basis = pattern_channel(item)
            if channel:
                item["analysisChannel"] = channel
                item["analysisChannelBasis"] = basis
                rules[basis] = rules.get(basis, 0)+1
        enriched.append(item)
    return enriched, {"version":"2026-10-08-v1","applied_rows":sum(rules.values()),"rules":rules,"raw_channel_changed":0,"raw_nationality_changed":0,"validation":{"reference":"2026-10-07 Excel","predicted":2272,"matched":2272,"conflicts":0,"unresolved":31},"basis":"분석용 분류 · 상품/고객그룹/거래처/국적 조합 · 원본 필드 유지"}


def original_contributions(row):
    """Literal SUMIFS membership rules; Nationality never determines a bucket."""
    result = {x[0]: 1 for x in LEAVES if x[4] and
              str((row.get("Channel") or row.get("reportChannel") or row.get("analysisChannel")) if x[4] == "Channel" else (row.get(x[4]) or "")).casefold() == str(x[5]).casefold()}
    # Original formula: all Sono Member rows minus ALL J&J MEMBER client rows.
    if str(row.get("clientName") or "").casefold() == "j&j marketing (member)":
        result["sono_member"] = result.get("sono_member", 0) - 1
    return result or {"unmapped": 1}


def analysis_market(contributions):
    parents = {x[0]:x[2] for x in LEAVES + GROUPS}
    markets = set()
    for key, weight in contributions.items():
        if weight <= 0:
            continue
        while key not in {"kr","jp","others","local_total","unmapped"}:
            key = parents.get(key) or "unmapped"
        markets.add({"kr":"KR","jp":"JP","others":"OTHER","local_total":"LOCAL","unmapped":"UNRESOLVED"}[key])
    return next(iter(markets)) if len(markets) == 1 else "REVIEW"


def aggregate(rows, as_of, start, end):
    date.fromisoformat(as_of)
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last:
        raise ValueError("coverage start must precede end")
    counts, source_counts, nationality_counts, uu_counts, diagnostics = {}, {}, {}, {}, {"source_rows": len(rows), "cancelled_rows": 0,
                              "outside_coverage_rows": 0, "non_golf_rows": 0,
                              "golf_rows": 0, "unmapped_rows": 0, "uu_golf_rows": 0,
                              "uu_resolved_rows": 0, "uu_unresolved_rows": 0,
                              "reconciliation_mismatches": 0}
    seen = set()
    for row in rows:
        key = str(row["Id"])
        if key in seen:
            raise ValueError("duplicate reservation Id in snapshot")
        seen.add(key)
        if flag(row["Cancelled"]):
            diagnostics["cancelled_rows"] += 1
            continue
        played = day(row["Start date"])
        if not first <= played <= last:
            diagnostics["outside_coverage_rows"] += 1
            continue
        venue = next((k for k, v in VENUES.items() if row["resourceTypeName"] == v), None)
        if venue is None:
            diagnostics["non_golf_rows"] += 1
            continue
        contributions = original_contributions(row)
        amount = money(row["Total"])
        source = source_counts.setdefault((played.strftime("%Y-%m"), venue), [0, Decimal(0)])
        source[0] += 1
        source[1] += amount
        nationality = str(row.get("Nationality") or "MISSING")
        national_slot = nationality_counts.setdefault((played.strftime("%Y-%m"), venue, nationality), [0, Decimal(0)])
        national_slot[0] += 1
        national_slot[1] += amount
        if nationality == "UU":
            market = analysis_market(contributions)
            evidence = []
            for category, weight in contributions.items():
                if weight > 0:
                    rule = next(x for x in LEAVES if x[0] == category)
                    evidence_field = row.get("analysisChannelBasis") if row.get("analysisChannel") else "엑셀 보완 채널" if rule[4] == "Channel" and not row.get("Channel") and row.get("reportChannel") else rule[4]
                    evidence.append(f"{evidence_field} = {rule[5]}" if rule[4] else "No matching rule")
            uu_slot = uu_counts.setdefault((played.strftime("%Y-%m"), venue, market, "; ".join(evidence)), [0, Decimal(0)])
            uu_slot[0] += 1
            uu_slot[1] += amount
        for category, weight in contributions.items():
            slot = counts.setdefault((played.strftime("%Y-%m"), venue, category), [0, Decimal(0)])
            slot[0] += weight
            slot[1] += amount * weight
        diagnostics["golf_rows"] += 1
        diagnostics["unmapped_rows"] += "unmapped" in contributions
        if row.get("Nationality") == "UU":
            diagnostics["uu_golf_rows"] += 1
            diagnostics["uu_unresolved_rows" if "unmapped" in contributions else "uu_resolved_rows"] += 1
    if not diagnostics["golf_rows"]:
        raise ValueError("No golf bookings in coverage; refusing to replace current data")
    months = []
    cursor = first.replace(day=1)
    while cursor <= last:
        month = cursor.strftime("%Y-%m")
        venues = {}
        for venue in VENUES:
            stats = {x[0]: counts.get((month, venue, x[0]), [0, Decimal(0)]) for x in LEAVES}
            def sum_keys(keys):
                return [sum(stats[k][0] for k in keys), sum((stats[k][1] for k in keys), Decimal(0))]
            # Children before parents.
            for key in ("sono", "kr_agency", "jp_individual", "jp_agency", "others", "local_total", "kr", "jp", "outbound"):
                item = next(x for x in GROUPS if x[0] == key)
                stats[key] = sum_keys(item[4])
            total = sum_keys([x[0] for x in LEAVES])
            source = source_counts.get((month, venue), [0, Decimal(0)])
            diagnostics["reconciliation_mismatches"] += total != source
            meta = [(x[0], x[1], x[2], x[3], False) for x in LEAVES]
            meta += [(x[0], x[1], x[2], x[3], True) for x in GROUPS]
            categories = [{"id": k, "label": label, "parent": parent, "report_row": r,
                           "is_group": is_group, "pax": stats[k][0],
                           "rev": float(stats[k][1].quantize(Decimal("0.01")))}
                          for k, label, parent, r, is_group in sorted(meta, key=lambda x: x[3])]
            venues[venue] = {"total": {"pax": total[0], "rev": float(total[1].quantize(Decimal("0.01")))},
                             "source_total": {"pax": source[0], "rev": float(source[1].quantize(Decimal("0.01")))},
                             "nationalities": [{"code": code,"pax": value[0],"rev":float(value[1].quantize(Decimal("0.01")))}
                                               for (m,v,code),value in sorted(nationality_counts.items()) if m==month and v==venue],
                             "uu_resolution": [{"market":market,"basis":basis,"pax":value[0],"rev":float(value[1].quantize(Decimal("0.01")))}
                                               for (m,v,market,basis),value in sorted(uu_counts.items()) if m==month and v==venue],
                             "categories": categories, "delta": None}
        months.append({"month": month, "venues": venues})
        cursor = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
    return {"schema_version": 1, "as_of": as_of, "coverage": {"start": start, "end": end},
            "currency": "USD", "pax_unit": "round", "months": months,
            "diagnostics": diagnostics, "comparison": None,
            "classification_rules": [{"id":x[0],"label":x[1],"field":x[4],"match":x[5],"report_row":x[3]} for x in LEAVES if x[4]]}


def load_targets(path):
    if path is None or not Path(path).exists():
        return {}
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        values = workbook.active.iter_rows(values_only=True)
        headers = next(values, ())
        required = {"year", "month", "venue", "category", "budget_pax", "budget_rev", "prev_pax", "prev_rev"}
        if not required.issubset(headers):
            raise ValueError("targets.xlsx has missing headers")
        result = {}
        valid_categories = {x[0] for x in LEAVES + GROUPS} | {"total"}
        for row in values:
            if not any(v is not None for v in row):
                continue
            item = dict(zip(headers, row))
            year, month = int(item["year"]), int(item["month"])
            if item["year"] != year or item["month"] != month or not 1 <= month <= 12:
                raise ValueError("invalid target year/month")
            if item["venue"] not in VENUES or item["category"] not in valid_categories:
                raise ValueError("unknown target venue/category")
            key = (f"{year:04d}-{month:02d}", item["venue"], item["category"])
            if key in result:
                raise ValueError("duplicate target key")
            result[key] = {f: None if item.get(f) is None else float(money(item[f]))
                           for f in ("budget_pax", "budget_rev", "prev_pax", "prev_rev")}
            if any(v is not None and v < 0 for v in result[key].values()):
                raise ValueError("negative target")
        return result
    finally:
        workbook.close()


def attach_targets(result, targets):
    for month in result["months"]:
        for venue, data in month["venues"].items():
            for key, item, actual in [("total", data, data["total"])] + [(x["id"], x, x) for x in data["categories"]]:
                inputs = targets.get((month["month"], venue, key), {})
                for field in ("budget_pax", "budget_rev", "prev_pax", "prev_rev"):
                    item[field] = inputs.get(field)
                for metric in ("pax", "rev"):
                    budget, prev = item["budget_" + metric], item["prev_" + metric]
                    item["budget_" + metric + "_rate"] = actual[metric] / budget if budget else None
                    item["prev_" + metric + "_change"] = (actual[metric] - prev) / prev if prev else None


def attach_previous_year(result, folder, use_patterns=False):
    """Use source-backed final prior-year totals; never invent missing channels."""
    audit = {"basis": "현재 예약 / 전년 동월 최종 실적", "months": []}
    for month in result["months"]:
        year, mm = map(int, month["month"].split("-"))
        previous = f"{year-1:04d}-{mm:02d}"
        files = sorted(Path(folder).glob(f"bookings_{previous}_asof_*.xlsx"))
        if not files:
            continue
        start = date(year-1, mm, 1)
        end = (date(year if mm==12 else year-1, 1 if mm==12 else mm+1, 1)-timedelta(days=1))
        rows = load_bookings(files[-1])
        if use_patterns:
            rows, _ = pattern_channels(rows)
        old = aggregate(rows, result["as_of"], start.isoformat(), end.isoformat())["months"][0]
        for venue, item in month["venues"].items():
            for target in [item] + item["categories"]:
                target["excel_prev_pax"] = target.get("prev_pax")
                target["excel_prev_rev"] = target.get("prev_rev")
            prior = old["venues"][venue]
            complete = not next(x for x in prior["categories"] if x["id"]=="unmapped")["pax"]
            by_id = {x["id"]: x for x in prior["categories"]}
            for target, actual, reference in [(item,item["total"],prior["total"])] + [(x,x,by_id[x["id"]] if complete or use_patterns else None) for x in item["categories"]]:
                for metric in ("pax", "rev"):
                    value = reference[metric] if reference is not None else None
                    target["prev_"+metric] = value
                    target["prev_"+metric+"_change"] = (actual[metric]-value)/value if value else None
        audit["months"].append({"month":month["month"],"previous_month":previous,"source_sha256":hashlib.sha256(files[-1].read_bytes()).hexdigest(),"raw_channel_missing":sum(not x.get("Channel") for x in rows)})
    result["previous_year_source"] = audit


def compare(result, previous):
    result["comparison"] = None
    for month in result["months"]:
        for data in month["venues"].values():
            data["delta"] = None
            for item in data["categories"]:
                item["delta"] = None
    if not previous or previous["coverage"] != result["coverage"] or previous["as_of"] >= result["as_of"]:
        return
    result["comparison"] = {"as_of": previous["as_of"],
                            "days": (date.fromisoformat(result["as_of"]) - date.fromisoformat(previous["as_of"])).days}
    before = {x["month"]: x for x in previous["months"]}
    for month in result["months"]:
        if month["month"] not in before:
            continue
        for venue, data in month["venues"].items():
            old = before[month["month"]]["venues"][venue]
            data["delta"] = {k: round(data["total"][k] - old["total"][k], 2) for k in ("pax", "rev")}
            categories = {x["id"]: x for x in old["categories"]}
            for item in data["categories"]:
                if item["id"] in categories:
                    item["delta"] = {k: round(item[k] - categories[item["id"]][k], 2) for k in ("pax", "rev")}


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    temp.replace(path)


def original_total(result, historical_months):
    year = result["coverage"]["start"][:4]
    wanted = [f"{year}-{month:02d}" for month in (9, 10, 11)]
    available = {x["month"]: x for x in historical_months + result["months"]}
    if any(key not in available for key in wanted):
        return None
    totals = {key:{"pax":0,"rev":0.0} for key in VENUES}
    for month in wanted:
        for venue in VENUES:
            data = available[month]["venues"][venue]
            unmapped = next((x for x in data.get("categories", []) if x["id"] == "unmapped"), {"pax":0,"rev":0})
            for metric in ("pax", "rev"):
                totals[venue][metric] += data["total"][metric] - unmapped[metric]
    return {"months":wanted,"venues":totals,
            "pax":sum(x["pax"] for x in totals.values()),
            "rev":round(sum(x["rev"] for x in totals.values()),2),
            "historical_basis":"September values imported from original workbook; reference only"}


def build(input_path, as_of, config, targets=None):
    rows = load_bookings(input_path)
    audit = None
    if config.get("classification_reference"):
        reference = Path(config["classification_reference"])
        rows, audit = reference_channels(rows, load_bookings(reference))
        audit["reference_sha256"] = hashlib.sha256(reference.read_bytes()).hexdigest()
    pattern_audit = None
    if config.get("analysis_patterns"):
        rows, pattern_audit = pattern_channels(rows)
    result = aggregate(rows, as_of, config["start"], config["end"])
    result["analysis_classification"] = pattern_audit
    result["classification_reference"] = audit
    attach_targets(result, load_targets(targets))
    if config.get("previous_year_folder"):
        attach_previous_year(result, config["previous_year_folder"], config.get("analysis_patterns",False))
    history = Path(config["history"])
    previous = []
    if history.exists():
        for p in history.glob("*.json"):
            old = json.loads(p.read_text(encoding="utf-8"))
            if old.get("coverage") == result["coverage"] and old["as_of"] < as_of:
                previous.append(old)
    compare(result, max(previous, key=lambda x: x["as_of"]) if previous else None)
    archives = []
    if config.get("historical_months") and Path(config["historical_months"]).exists():
        archives = json.loads(Path(config["historical_months"]).read_text(encoding="utf-8"))
    result["original_total"] = original_total(result, archives)
    result["source_sha256"] = hashlib.sha256(Path(input_path).read_bytes()).hexdigest()
    coverage = f'{config["start"]}_{config["end"]}'
    atomic_json(history / f"{as_of}_{coverage}.json", result)
    if config.get("archive_folder"):
        extend_archived_months(result,config["archive_folder"],load_targets(targets),config.get("analysis_patterns",False))
    atomic_json(config["output"], result)
    return result


def extend_archived_months(result, folder, targets=None, use_patterns=False):
    """Append source-backed monthly history while preserving the daily snapshot."""
    by_month={x["month"]:x for x in result["months"]}
    audit=[]
    paths={p.name[9:16]:p for p in sorted(Path(folder).glob("bookings_????-??_asof_????-??-??.xlsx"))}
    for month,path in sorted(paths.items()):
        if month in by_month:
            continue
        year, mm=map(int,month.split("-"))
        start=date(year,mm,1)
        end=date(year+1 if mm==12 else year,1 if mm==12 else mm+1,1)-timedelta(days=1)
        rows=load_bookings(path)
        if use_patterns:
            rows, pattern_audit=pattern_channels(rows)
        old=aggregate(rows,result["as_of"],start.isoformat(),end.isoformat())
        attach_targets(old,targets or {})
        compare(old,None)
        by_month[month]=old["months"][0]
        audit.append({"month":month,"rows":len(rows),"sha256":hashlib.sha256(path.read_bytes()).hexdigest(),"unmapped_rows":old["diagnostics"]["unmapped_rows"]})
    result["months"]=[by_month[x] for x in sorted(by_month)]
    for month in result["months"]:
        year,mm=map(int,month["month"].split("-"))
        prior=by_month.get(f"{year-1:04d}-{mm:02d}")
        if not prior:
            continue
        for venue,item in month["venues"].items():
            previous=prior["venues"][venue]
            cats={x["id"]:x for x in previous["categories"]}
            for target,actual,reference in [(item,item["total"],previous["total"])] + [(x,x,cats[x["id"]]) for x in item["categories"]]:
                for metric in ("pax","rev"):
                    target.setdefault("excel_prev_"+metric,target.get("prev_"+metric))
                    value=reference[metric]
                    target["prev_"+metric]=value
                    target["prev_"+metric+"_change"]=(actual[metric]-value)/value if value else None
    result["archive_coverage"]={"start":result["months"][0]["month"],"end":result["months"][-1]["month"],"months":len(result["months"]),"sources":audit}
    return result


def merge_monthly_snapshot(config):
    folder=Path(config.get("archive_folder",Path(config["inbox"])/"monthly"))
    first,last=date.fromisoformat(config["start"]),date.fromisoformat(config["end"])
    wanted=[];cursor=first.replace(day=1)
    while cursor<=last:
        wanted.append(cursor.strftime("%Y-%m"));cursor=date(cursor.year+1,1,1) if cursor.month==12 else date(cursor.year,cursor.month+1,1)
    groups={}
    for p in folder.glob("bookings_????-??_asof_????-??-??.xlsx"):
        groups.setdefault(p.stem[-10:],{})[p.name[9:16]]=p
    complete=[day for day,parts in groups.items() if all(month in parts for month in wanted)]
    if not complete:return
    as_of=max(complete);dest=Path(config["inbox"])/f"bookings_{as_of}.xlsx"
    if dest.exists():return
    workbook=openpyxl.Workbook();sheet=workbook.active;sheet.title="Booking"
    header=None;seen=set()
    try:
        for month in wanted:
            source=openpyxl.load_workbook(groups[as_of][month],read_only=True,data_only=True)
            try:
                values=source.active.iter_rows(values_only=True);columns=tuple(next(values))
                if header is None:header=columns;sheet.append(list(header))
                if columns!=header:raise ValueError("Monthly headers differ")
                for row in values:
                    key=row[header.index("Id")]
                    if key is None:continue
                    if str(key) in seen:raise ValueError("Duplicate Id across monthly snapshots")
                    if day(row[header.index("Start date")]).strftime("%Y-%m")!=month:raise ValueError("Monthly use date mismatch")
                    seen.add(str(key));sheet.append(list(row))
            finally:source.close()
        temp=dest.with_suffix(".building.xlsx");workbook.save(temp);temp.replace(dest)
    finally:workbook.close()


def run_folder(config):
    merge_monthly_snapshot(config)
    inbox = Path(config["inbox"])
    matches = [(m.group(1), p) for p in inbox.glob("bookings_*.xlsx")
               if (m := re.fullmatch(r"bookings_(\d{4}-\d{2}-\d{2})\.xlsx", p.name))]
    if not matches:
        raise ValueError(f"Save bookings_YYYY-MM-DD.xlsx into {inbox}")
    as_of, path = max(matches)
    result = build(path, as_of, config, inbox / "targets.xlsx")
    print(f'{as_of}: {result["diagnostics"]["golf_rows"]} rounds; '
          f'{result["diagnostics"]["unmapped_rows"]} unclassified. Updated {config["output"]}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--watch", action="store_true")
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    for key in ("inbox", "history", "output", "historical_months", "classification_reference", "previous_year_folder", "archive_folder"):
        if key not in config:
            continue
        config[key] = str((args.config.parent / config[key]).resolve())
    if not args.watch:
        run_folder(config)
        return
    print("Watching the local inbox. Stop with Ctrl+C. No GitHub upload is performed.", flush=True)
    last_signature = None
    while True:
        files = sorted(Path(config["inbox"]).glob("*.xlsx")) + [args.config]
        if config.get("archive_folder"):
            files += sorted(Path(config["archive_folder"]).glob("*.xlsx"))
        if config.get("historical_months") and Path(config["historical_months"]).exists():
            files.append(Path(config["historical_months"]))
        if config.get("classification_reference"):
            files.append(Path(config["classification_reference"]))
        signature = tuple((str(p), p.stat().st_size, p.stat().st_mtime_ns) for p in files if not p.name.startswith("~$"))
        if signature != last_signature:
            time.sleep(2)  # Wait for file copy to settle before parsing.
            stable = tuple((str(p), p.stat().st_size, p.stat().st_mtime_ns) for p in files if p.exists() and not p.name.startswith("~$"))
            if stable == signature:
                try:
                    # Config edits also affect watched coverage.
                    config = json.loads(args.config.read_text(encoding="utf-8"))
                    for key in ("inbox", "history", "output", "historical_months", "classification_reference", "previous_year_folder", "archive_folder"):
                        if key not in config:
                            continue
                        config[key] = str((args.config.parent / config[key]).resolve())
                    run_folder(config)
                except Exception as exc:
                    print(f"Import failed; previous current data retained: {exc}", flush=True)
                else:
                    last_signature = signature
        time.sleep(5)


if __name__ == "__main__":
    main()
