#!/usr/bin/env python3
"""Detached pipeline runner — forks a child that survives parent exit."""
import os, sys, subprocess

LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline.log")
SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")

steps = [
    ("STEP1",  "parse_raw_db.py",                  False),
    ("STEP2",  "compare_and_update.py",             False),
    ("STEP3",  "generate_otb_data.py",              False),
    ("STEP4",  "generate_fcst.py",                  False),
    ("STEP5",  "generate_campaign_data.py",         False),
    # campaign_data.json#events에 매핑된 패키지코드를 27/28 DB와 조인 → 기획전별 실적
    ("STEP5b", "generate_campaign_performance.py",  False),
    # 패키지(86XXXXXX) 회원번호 분류별 트렌드 (시리즈 분석)
    ("STEP5c", "parse_package_trend.py",            False),
    # 기획전 #196 실적 데이터 (campaign86_data.json) — 실패해도 파이프라인 계속
    ("STEP5d", "parse_campaign86.py",               True),
    ("STEP6",  "generate_insights.py",              False),
    ("STEP7",  "build_validation.py",               False),
    ("STEP8",  "build.py",                          False),
]

pid = os.fork()
if pid > 0:
    # Parent — exit immediately so bash returns
    print(f"Pipeline forked as PID {pid}")
    sys.exit(0)

# Child — detach
os.setsid()

with open(LOG, "w") as lf:
    for tag, script, optional in steps:
        lf.write(f"\n===== {tag}: {script} =====\n")
        lf.flush()
        result = subprocess.run(
            [sys.executable, os.path.join(SCRIPTS_DIR, script)],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=lf, stderr=subprocess.STDOUT
        )
        if result.returncode != 0:
            if optional:
                lf.write(f"{tag}_SKIPPED (optional, exit code {result.returncode})\n")
                lf.flush()
                continue
            lf.write(f"{tag}_FAILED (exit code {result.returncode})\n")
            lf.flush()
            break
        lf.write(f"{tag}_DONE\n")
        lf.flush()
    else:
        lf.write("\nALL_DONE\n")
        lf.flush()
