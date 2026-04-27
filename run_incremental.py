#!/usr/bin/env python3
"""
Incremental pipeline runner - processes parse_raw_db year by year,
saving intermediate state to pickle files.

Usage:
  python run_incremental.py parse <year>     — parse one year, save to pickle
  python run_incremental.py parse all        — parse all years sequentially
  python run_incremental.py merge            — merge all year pickles + build summary → db_aggregated.json
  python run_incremental.py post             — run steps 2-5 (compare, otb, insights, build)
"""
import os, sys, pickle, json, logging, importlib.util
from pathlib import Path
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = PROJECT_DIR / "scripts"
RAW_DB_DIR = PROJECT_DIR / "data" / "raw_db"
CACHE_DIR = PROJECT_DIR / "data" / ".parse_cache"
CACHE_DIR.mkdir(exist_ok=True)

def load_parse_module():
    spec = importlib.util.spec_from_file_location("parse_raw_db", SCRIPTS_DIR / "parse_raw_db.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def parse_year(year_str):
    """Parse a single year's files and save intermediate agg to pickle."""
    mod = load_parse_module()

    # Collect only files for this year
    year_dir = RAW_DB_DIR / year_str
    if not year_dir.exists():
        logger.error(f"Year directory not found: {year_dir}")
        return

    import re
    txt_files = sorted(year_dir.glob("*.txt"))
    logger.info(f"=== {year_str}년: {len(txt_files)}개 파일 ===")

    # Determine month filters (same logic as parse_raw_db.py for 2026)
    folder_type_files = defaultdict(list)
    for fp in txt_files:
        ft = mod.detect_file_type(fp.name)
        if ft:
            folder_type_files[(str(fp.parent), ft)].append(fp)

    def _is_retrans(fp):
        return bool(re.search(r'\(\d{8}-\d{8}\)', fp.name))

    file_month_filter = {}
    for (folder, ft), fps in folder_type_files.items():
        retrans = [fp for fp in fps if _is_retrans(fp)]
        snapshots = [fp for fp in fps if not _is_retrans(fp)]
        if retrans and snapshots:
            for fp in retrans:
                file_month_filter[fp] = (None, '202603')
                logger.info(f"  재전송 ≤202603: {fp.name}")
            for fp in snapshots:
                file_month_filter[fp] = ('202604', None)
                logger.info(f"  스냅샷 ≥202604: {fp.name}")

    agg = defaultdict(lambda: {'rn': 0, 'rev': 0.0, 'count': 0})
    cancel_daily_agg = defaultdict(lambda: {'rn': 0, 'rev': 0})
    pickup_daily_agg = defaultdict(lambda: {'rn': 0, 'rev': 0})
    lead_time_agg = defaultdict(lambda: {'rn': 0})
    cancel_lead_agg = defaultdict(lambda: {'rn': 0})
    stay_date_agg = defaultdict(lambda: {'rn': 0, 'rev': 0})
    total_rows = 0
    file_stats = {}
    type_labels = {"27": "FIT예약", "28": "FIT취소", "43": "IB예약", "44": "IB취소"}

    for fpath in txt_files:
        file_type = mod.detect_file_type(fpath.name)
        if not file_type:
            continue

        mfilter = file_month_filter.get(fpath)
        if mfilter == 'skip':
            continue

        min_m = max_m = None
        if isinstance(mfilter, tuple):
            min_m, max_m = mfilter

        logger.info(f"파싱: [{type_labels.get(file_type, file_type)}] {fpath.name}")

        row_count = mod.parse_and_aggregate(
            str(fpath), file_type, agg,
            min_month=min_m, max_month=max_m,
            cancel_daily_agg=cancel_daily_agg,
            pickup_daily_agg=pickup_daily_agg,
            lead_time_agg=lead_time_agg,
            cancel_lead_agg=cancel_lead_agg,
            stay_date_agg=stay_date_agg
        )
        total_rows += row_count
        file_stats[f"{year_str}/{fpath.name}"] = {
            'type': file_type, 'label': type_labels.get(file_type, file_type), 'rows': row_count
        }

    # Convert defaultdicts to regular dicts for pickling
    data = {
        'agg': dict(agg),
        'cancel_daily_agg': dict(cancel_daily_agg),
        'pickup_daily_agg': dict(pickup_daily_agg),
        'lead_time_agg': dict(lead_time_agg),
        'cancel_lead_agg': dict(cancel_lead_agg),
        'stay_date_agg': dict(stay_date_agg),
        'total_rows': total_rows,
        'file_stats': file_stats,
    }

    cache_file = CACHE_DIR / f"{year_str}.pkl"
    with open(cache_file, 'wb') as f:
        pickle.dump(data, f)

    logger.info(f"{year_str}년 완료: {total_rows:,}행, 캐시 저장: {cache_file}")

def merge_and_build():
    """Merge all year caches and build db_aggregated.json."""
    mod = load_parse_module()

    agg = defaultdict(lambda: {'rn': 0, 'rev': 0.0, 'count': 0})
    cancel_daily_agg = defaultdict(lambda: {'rn': 0, 'rev': 0})
    pickup_daily_agg = defaultdict(lambda: {'rn': 0, 'rev': 0})
    lead_time_agg = defaultdict(lambda: {'rn': 0})
    cancel_lead_agg = defaultdict(lambda: {'rn': 0})
    stay_date_agg = defaultdict(lambda: {'rn': 0, 'rev': 0})
    total_rows = 0
    all_file_stats = {}

    years = sorted([f.stem for f in CACHE_DIR.glob("*.pkl")])
    logger.info(f"병합할 연도: {years}")

    for year in years:
        cache_file = CACHE_DIR / f"{year}.pkl"
        with open(cache_file, 'rb') as f:
            data = pickle.load(f)

        for k, v in data['agg'].items():
            agg[k]['rn'] += v['rn']
            agg[k]['rev'] += v['rev']
            agg[k]['count'] += v['count']
        for k, v in data['cancel_daily_agg'].items():
            cancel_daily_agg[k]['rn'] += v['rn']
            cancel_daily_agg[k]['rev'] += v['rev']
        for k, v in data['pickup_daily_agg'].items():
            pickup_daily_agg[k]['rn'] += v['rn']
            pickup_daily_agg[k]['rev'] += v['rev']
        for k, v in data['lead_time_agg'].items():
            lead_time_agg[k]['rn'] += v['rn']
        for k, v in data['cancel_lead_agg'].items():
            cancel_lead_agg[k]['rn'] += v['rn']
        for k, v in data['stay_date_agg'].items():
            stay_date_agg[k]['rn'] += v['rn']
            stay_date_agg[k]['rev'] += v['rev']
        total_rows += data['total_rows']
        all_file_stats.update(data['file_stats'])

        logger.info(f"  {year}년: {data['total_rows']:,}행 병합")

    logger.info(f"총 행 수: {total_rows:,}")

    # Build summary using the module's function
    summary = mod.build_summary(
        agg,
        cancel_daily_agg=cancel_daily_agg,
        pickup_daily_agg=pickup_daily_agg,
        lead_time_agg=lead_time_agg,
        cancel_lead_agg=cancel_lead_agg,
        stay_date_agg=stay_date_agg
    )
    summary['file_stats'] = all_file_stats

    # YoY adjusted calculation (same as original)
    from datetime import datetime
    txt_files = sorted(
        Path(dirpath) / fname
        for dirpath, dirnames, filenames in os.walk(RAW_DB_DIR, followlinks=True)
        for fname in filenames
        if fname.lower().endswith(".txt")
    )

    today = datetime.now()
    HISTORICAL_YEARS = ["2022", "2023", "2024", "2025", "2026"]
    yoy_adjusted = {}

    for year in HISTORICAL_YEARS:
        int_year = int(year)
        try:
            base_dt = today.replace(year=int_year)
        except ValueError:
            base_dt = today.replace(year=int_year, day=28)
        base_date_str = base_dt.strftime("%Y%m%d")
        base_mmdd = base_dt.strftime("%m%d")
        logger.info(f"  {year}년 기준일: {base_date_str}")

        adj_by_month = defaultdict(lambda: {'rn': 0, 'rev': 0})
        adj_by_prop = defaultdict(lambda: defaultdict(lambda: {'rn': 0, 'rev': 0}))

        cancel_files = [fp for fp in txt_files
                        if fp.parent.name == year
                        and mod.detect_file_type(fp.name) in ("28", "44")]
        for fpath in cancel_files:
            mod.parse_yoy_adjustments(str(fpath), base_date_str, adj_by_month, adj_by_prop)

        orig_by_prop_month = defaultdict(lambda: defaultdict(lambda: {'booking_rn': 0, 'booking_rev': 0}))
        for (prop, region, month, channel, segment, btype), vals in agg.items():
            if not month.startswith(year):
                continue
            if btype != 'booking':
                continue
            if segment not in ('OTA', 'G-OTA', 'Inbound'):
                continue
            orig_by_prop_month[prop][month]['booking_rn'] += vals['rn']
            orig_by_prop_month[prop][month]['booking_rev'] += vals['rev']

        all_months_set = set(adj_by_month.keys())
        for p in orig_by_prop_month:
            all_months_set.update(orig_by_prop_month[p].keys())

        by_month = {}
        for m in sorted(all_months_set):
            orig_rn = sum(orig_by_prop_month[p].get(m, {}).get('booking_rn', 0) for p in orig_by_prop_month)
            orig_rev = sum(orig_by_prop_month[p].get(m, {}).get('booking_rev', 0) for p in orig_by_prop_month)
            adj_rn = adj_by_month[m]['rn']
            adj_rev = adj_by_month[m]['rev']
            by_month[m] = {
                'booking_rn': orig_rn + adj_rn,
                'adjustment_rn': adj_rn,
                'orig_booking_rn': orig_rn,
                'booking_rev_m': round((orig_rev + adj_rev) / 1_000_000, 2),
                'adjustment_rev_m': round(adj_rev / 1_000_000, 2),
            }

        all_props_set = set(list(orig_by_prop_month.keys()) + list(adj_by_prop.keys()))
        by_property = {}
        for p in sorted(all_props_set):
            p_orig = orig_by_prop_month.get(p, {})
            p_adj = adj_by_prop.get(p, {})
            prop_months = {}
            for m in sorted(set(list(p_orig.keys()) + list(p_adj.keys()))):
                orig_rn = p_orig.get(m, {}).get('booking_rn', 0)
                adj_rn = p_adj.get(m, {}).get('rn', 0)
                prop_months[m] = {
                    'booking_rn': orig_rn + adj_rn,
                    'adjustment_rn': adj_rn,
                }
            by_property[p] = prop_months

        total_adj = sum(v['rn'] for v in adj_by_month.values())
        logger.info(f"  {year}년 보정 합계: {total_adj:,} RNs")
        yoy_adjusted[year] = {
            'base_date': base_mmdd,
            'base_date_full': base_date_str,
            'by_month': by_month,
            'by_property': by_property,
        }

    summary['yoy_adjusted'] = yoy_adjusted

    output_path = PROJECT_DIR / "data" / "db_aggregated.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info(f"출력: {output_path}")

    # Print summary
    for year in summary['meta']['years']:
        year_months = [m for m in summary['meta']['months'] if m.startswith(year)]
        total_rn = sum(summary['monthly_total'].get(m, {}).get('net_rn', 0) for m in year_months)
        total_rev = sum(summary['monthly_total'].get(m, {}).get('net_rev', 0) for m in year_months)
        adr = round((total_rev * 1000) / total_rn) if total_rn > 0 else 0
        print(f"  {year}년: RN {total_rn:>10,} | REV {total_rev:>10,.0f}백만원 | ADR {adr:>6,}천원")

def run_post_steps():
    """Run steps 2-5."""
    import subprocess
    for script in ["compare_and_update.py", "generate_otb_data.py", "generate_insights.py", "build.py"]:
        logger.info(f"=== {script} ===")
        result = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / script)],
            cwd=str(PROJECT_DIR),
            capture_output=False
        )
        if result.returncode != 0:
            logger.error(f"{script} failed with exit code {result.returncode}")
            sys.exit(1)
        logger.info(f"{script} 완료")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "parse":
        year = sys.argv[2] if len(sys.argv) > 2 else "all"
        if year == "all":
            for y in ["2022", "2023", "2024", "2025", "2026"]:
                parse_year(y)
        else:
            parse_year(year)
    elif cmd == "merge":
        merge_and_build()
    elif cmd == "post":
        run_post_steps()
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)
