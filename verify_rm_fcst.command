#!/bin/zsh
# Verification helper — starts a local HTTP server in docs/ and opens fcst-admin.html
# Created by Claude. Safe to delete after verification.

cd "$HOME/Desktop/gs_daily_trend_news_public_temp"
PORT=8765

# Start server in background, log to a file we can show
echo "Starting local server on http://localhost:${PORT}..."
(cd docs && python3 -m http.server ${PORT} >/tmp/rm_verify_server.log 2>&1) &
SERVER_PID=$!

# Give the server a moment to bind
sleep 0.8

# Open the page (Chrome preferred, fall back to default)
URL="http://localhost:${PORT}/fcst-admin.html"
if command -v open >/dev/null 2>&1; then
  open -a "Google Chrome" "$URL" 2>/dev/null || open "$URL"
fi

echo ""
echo "================================================================"
echo "  Server PID: $SERVER_PID"
echo "  URL: $URL"
echo "  Server log: /tmp/rm_verify_server.log"
echo "================================================================"
echo ""
echo "Press Enter in this window to STOP the server when done."
read -r
kill $SERVER_PID 2>/dev/null
echo "Server stopped."
