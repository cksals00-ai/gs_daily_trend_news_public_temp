#!/usr/bin/env python3
"""Fix all HTML navs to 8-item standard + remove package section from index.html"""
import re
from pathlib import Path

DOCS = Path("/sessions/funny-fervent-rubin/mnt/gs_daily_trend_news_public_temp/docs")

CORRECT_NAV_LINKS = """      <a class="gsn-item" href="https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/" data-gsn="trend">트렌드 리포트</a>
      <a class="gsn-item" href="https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/otb.html" data-gsn="otb">GS 실적 리포트</a>
      <a class="gsn-item" href="https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/booking-status.html" data-gsn="booking">Booking Status</a>
      <a class="gsn-item" href="https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/fcst-trend.html" data-gsn="fcst-trend">FCST 추이</a>
      <a class="gsn-item" href="https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/action_plan_dashboard.html" data-gsn="action">Action Plan</a>
      <a class="gsn-item" href="https://cksals00-ai.github.io/sono-competitor-crawler/" data-gsn="monitor">경쟁사 모니터링</a>
      <a class="gsn-item" href="https://cksals00-ai.github.io/sono-competitor-crawler/palatium.html" data-gsn="palatium">팔라티움 현황 리포트</a>
      <a class="gsn-item" href="https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/overseas.html" data-gsn="overseas">해외사업장</a>"""

CORRECT_SCRIPT = """<script>(function(){var h=location.href;document.querySelectorAll('.gsn-item[data-gsn]').forEach(function(a){var k=a.getAttribute('data-gsn'),hit=false;if(k==='trend')hit=h.indexOf('gs_daily_trend_news_public_temp')!==-1&&(h.indexOf('index.html')!==-1||h.match(/gs_daily_trend_news_public_temp\\/?$/)||h.match(/gs_daily_trend_news_public_temp\\/?#/));else if(k==='otb')hit=h.indexOf('otb.html')!==-1;else if(k==='booking')hit=h.indexOf('booking-status')!==-1;else if(k==='fcst-trend')hit=h.indexOf('fcst-trend')!==-1;else if(k==='action')hit=h.indexOf('action_plan')!==-1;else if(k==='overseas')hit=h.indexOf('overseas.html')!==-1;else if(k==='monitor')hit=h.indexOf('sono-competitor-crawler')!==-1&&h.indexOf('palatium')===-1;else if(k==='palatium')hit=h.indexOf('palatium')!==-1;if(hit)a.classList.add('active');});})();</script>"""

fixed = []
for html_file in sorted(DOCS.glob("*.html")):
    content = html_file.read_text(encoding="utf-8")
    original = content
    
    # Replace nav links: find all gsn-item links and replace block
    # Pattern: from first gsn-item to last gsn-item (inclusive)
    pattern = r'([ \t]*<a class="gsn-item"[^>]*data-gsn="[^"]*">[^<]*</a>\s*)+'
    m = re.search(pattern, content)
    if m:
        content = content[:m.start()] + CORRECT_NAV_LINKS + "\n" + content[m.end():]
    
    # Replace the active-detection script
    # Old patterns vary - find script that references gsn-item
    old_script_pattern = r'<script>\(function\(\)\{var h=location\.href.*?gsn-item.*?\}\(\)\)\.?;?</script>'
    content = re.sub(old_script_pattern, CORRECT_SCRIPT, content, flags=re.DOTALL)
    
    if content != original:
        html_file.write_text(content, encoding="utf-8")
        count = content.count('data-gsn=')
        # subtract 1 for the script reference
        nav_count = len(re.findall(r'<a class="gsn-item"', content))
        fixed.append(f"  {html_file.name}: {nav_count} nav items")

print(f"Fixed {len(fixed)} files:")
for f in fixed:
    print(f)

# Verify all files now have 8 nav items
print("\n=== Verification ===")
for html_file in sorted(DOCS.glob("*.html")):
    content = html_file.read_text(encoding="utf-8")
    nav_count = len(re.findall(r'<a class="gsn-item"', content))
    status = "OK" if nav_count == 8 else f"WRONG ({nav_count})"
    print(f"  {html_file.name}: {status}")
