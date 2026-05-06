#!/usr/bin/env osascript

tell application "System Events"
    try
        set result to (do shell script "/usr/bin/python3 /Users/chanminpark/Desktop/gs_daily_trend_news_public_temp/create_pdf.py")
        display notification result with title "PDF Creator"
    on error errMsg
        display notification errMsg with title "PDF Creator Error"
    end try
end tell
