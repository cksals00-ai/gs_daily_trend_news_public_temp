#!/usr/bin/env python3
"""Detached pipeline runner — forks a child that survives parent exit."""
import os, sys, subprocess

LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline.log")
SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts")

steps = [
    ("STEP1",  "parse_raw_db.py"),
    ("STEP2",  "compare_and_update.py"),
    ("STEP3",  "generate_otb_data.py"),
    ("STEP4",  "generate_fcst.py"),
    ("STEP5",  "generate_campaign_data.py"),
    # campaign_data.json#events에 매핑된 패키지코드를 27/28 DB와 조인 → 기획전별 실적
    ("STEP5b", "generate_campaign_performance.py"),
    # 패키지(86XXXXXX) 회원번호 분류별 트렌드 (시리즈 분석)
    ("STEP5c", "parse_package_trend.py"),
    ("STEP6",  "generate_insights.py"),
    ("STEP7",  "build_validation.py"),
    ("STEP8",  "build.py"),
]

pid = os.fork()
if pid > 0:
    # Parent — exit immediately so bash returns
    print(f"Pipeline forked as PID {pid}")
    sys.exit(0)

# Child — detach
os.setsid()

with open(LOG, "w") as lf:
    for tag, script in steps:
        lf.write(f"\n===== {tag}: {script} =====\n")
        lf.flush()
        result = subprocess.run(
            [sys.executable, os.path.join(SCRIPTS_DIR, script)],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=lf, stderr=subprocess.STDOUT
        )
        if result.returncode != 0:
            lf.write(f"{tag}_FAILED (exit code {result.returncode})\n")
            lf.flush()
            break
        lf.write(f"{tag}_DONE\n")
        lf.flush()
    else:
        lf.write("\nALL_DONE\n")
        lf.flush()
