#!/usr/bin/env python3
"""
fetch_gsheet_via_chrome.py — Chrome MCP javascript_tool로 구글시트 CSV 가져오기

스케줄 태스크에서 Chrome MCP의 javascript_tool을 사용하여
구글시트 CSV를 fetch()한 결과를 로컬 파일로 저장하는 헬퍼.

이 스크립트 자체는 네트워크를 사용하지 않음 — Chrome MCP가 반환한
CSV 텍스트를 stdin 또는 --text 인자로 받아 파일로 저장만 함.

사용법 (스케줄 태스크 내):
  1. Chrome MCP javascript_tool로 fetch() → CSV text 획득
  2. 해당 text를 이 스크립트에 전달하여 로컬 저장

  또는 직접 파이썬에서:
    echo "$CSV_TEXT" | python fetch_gsheet_via_chrome.py --output /tmp/main.csv

실제 스케줄 태스크에서의 워크플로우:
  Step 1: javascript_tool → fetch(CSV_URL).then(r=>r.text())
  Step 2: 결과를 파일로 저장 (Write tool 또는 bash echo)
  Step 3: generate_campaign_data.py --csv-file /tmp/main.csv --subsheets-dir /tmp/subs/
"""
from __future__ import annotations
import sys
import argparse
from pathlib import Path


# ─── 구글시트 URL 정의 (Chrome MCP에서 fetch할 URL 참조용) ───
CAMPAIGN_SHEET = {
    "publish_id": "2PACX-1vTqe7nY8vHYVVnnGR5qrl-uubCABXtmbToAuKWziuaoms14hZ3qlJuQTBWUXDmjCOU-4hd0hp6cpO_O",
    "main_gid": "1818134248",
    "description": "GS 채널 판매 보고 (public, publish-to-web)",
}

WEEKLY_ACTIVITY_SHEET = {
    "spreadsheet_id": "1MJNoET5yTLRYejV61ZQuIaHbvYHBHYJ31eMpg7SUDEw",
    "gid": "1986408525",
    "description": "주간활동일지 DATA (private, 구글 로그인 필요)",
}


def get_campaign_csv_url() -> str:
    pid = CAMPAIGN_SHEET["publish_id"]
    gid = CAMPAIGN_SHEET["main_gid"]
    return f"https://docs.google.com/spreadsheets/d/e/{pid}/pub?gid={gid}&single=true&output=csv"


def get_campaign_pubhtml_url() -> str:
    pid = CAMPAIGN_SHEET["publish_id"]
    return f"https://docs.google.com/spreadsheets/d/e/{pid}/pubhtml"


def get_campaign_subsheet_csv_url(gid: str) -> str:
    pid = CAMPAIGN_SHEET["publish_id"]
    return f"https://docs.google.com/spreadsheets/d/e/{pid}/pub?gid={gid}&single=true&output=csv"


def get_weekly_csv_url() -> str:
    sid = WEEKLY_ACTIVITY_SHEET["spreadsheet_id"]
    gid = WEEKLY_ACTIVITY_SHEET["gid"]
    return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={gid}"


# ─── JS 코드 생성 (Chrome MCP javascript_tool용) ───

def js_fetch_csv(url: str) -> str:
    """Chrome MCP javascript_tool에서 실행할 JS 코드 반환.
    fetch()로 CSV를 가져와 텍스트로 반환."""
    return f"""
(async () => {{
  try {{
    const resp = await fetch("{url}", {{ credentials: "include" }});
    if (!resp.ok) throw new Error(`HTTP ${{resp.status}}`);
    return await resp.text();
  }} catch (e) {{
    return "FETCH_ERROR: " + e.message;
  }}
}})()
""".strip()


def js_fetch_pubhtml_gids() -> str:
    """pubhtml 페이지에서 시트명→gid 매핑을 추출하는 JS 코드."""
    url = get_campaign_pubhtml_url()
    return f"""
(async () => {{
  try {{
    const resp = await fetch("{url}");
    if (!resp.ok) throw new Error(`HTTP ${{resp.status}}`);
    const html = await resp.text();
    const re = /items\\.push\\(\\{{name:\\s*"([^"]+)",\\s*pageUrl:\\s*"([^"]+)"/gi;
    const result = {{}};
    let m;
    while ((m = re.exec(html)) !== null) {{
      const name = m[1].trim();
      const url = m[2].replace(/\\\\/\\//g, "/").replace(/\\\\x3d/g, "=").replace(/\\\\x26/g, "&");
      const gidMatch = url.match(/gid=(\\d+)/);
      if (gidMatch) result[name] = gidMatch[1];
    }}
    return JSON.stringify(result);
  }} catch (e) {{
    return "FETCH_ERROR: " + e.message;
  }}
}})()
""".strip()


def main():
    parser = argparse.ArgumentParser(description="Chrome MCP fetch helper")
    parser.add_argument("--output", "-o", required=True, help="저장할 CSV 파일 경로")
    parser.add_argument("--text", default=None, help="CSV 텍스트 (stdin 대신)")
    parser.add_argument("--print-urls", action="store_true",
                        help="Chrome MCP에서 fetch할 URL 목록 출력")
    parser.add_argument("--print-js", choices=["campaign", "weekly", "pubhtml"],
                        help="해당 시트용 JS fetch 코드 출력")
    args = parser.parse_args()

    if args.print_urls:
        print(f"campaign_csv: {get_campaign_csv_url()}")
        print(f"campaign_pubhtml: {get_campaign_pubhtml_url()}")
        print(f"weekly_csv: {get_weekly_csv_url()}")
        return

    if args.print_js:
        if args.print_js == "campaign":
            print(js_fetch_csv(get_campaign_csv_url()))
        elif args.print_js == "weekly":
            print(js_fetch_csv(get_weekly_csv_url()))
        elif args.print_js == "pubhtml":
            print(js_fetch_pubhtml_gids())
        return

    if args.text:
        data = args.text
    else:
        data = sys.stdin.read()

    if not data or data.startswith("FETCH_ERROR:"):
        print(f"ERROR: {data}", file=sys.stderr)
        sys.exit(1)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(data, encoding="utf-8")
    print(f"✓ {out} ({len(data)} bytes)")


if __name__ == "__main__":
    main()
