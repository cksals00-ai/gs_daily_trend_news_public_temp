#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.lib.units import cm
import os

output_path = "/Users/chanminpark/Desktop/gs_daily_trend_news_public_temp/GS_세일즈리포트_설치안내.pdf"

width, height = A4
c = canvas.Canvas(output_path, pagesize=A4)

# Background
c.setFillColor(colors.HexColor("#1a1d23"))
c.rect(0, 0, width, height, fill=1, stroke=0)

# Header bar
c.setFillColor(colors.HexColor("#6ba3c4"))
c.rect(0, height - 1.8*cm, width, 1.8*cm, fill=1, stroke=0)

# Title
c.setFont("Helvetica-Bold", 32)
c.setFillColor(colors.HexColor("#ffffff"))
c.drawString(1.2*cm, height - 1.2*cm, "GS Sales Report")

# Subtitle
c.setFont("Helvetica", 13)
c.setFillColor(colors.HexColor("#c9a063"))
c.drawString(1.2*cm, height - 1.6*cm, "Installation Guide")

# Content
y = height - 2.3*cm
line_height = 0.32*cm

sections = [
    ("1. App Introduction", [
        "• Inbound sales performance monitoring",
        "• Partner/Dealer calendar management",
        "• Country-wise sales analysis",
        "• Automatic daily updates"
    ]),
    ("2. Access URL", [
        "https://cksals00-ai.github.io/gs_daily_trend_news_public_temp/gs-sales-report.html"
    ]),
    ("3. iPhone Installation", [
        "1) Open Safari and visit the URL above",
        "2) Tap the Share button (square with arrow icon)",
        "3) Select 'Add to Home Screen'",
        "4) Name the app and tap Add",
        "5) Launch from home screen - opens without URL bar"
    ]),
    ("4. Android Installation", [
        "1) Open Chrome and visit the URL above",
        "2) Tap the menu (three dots) and select 'Install app'",
        "3) Or tap the app installation banner at the top",
        "4) Follow prompts to complete installation"
    ]),
    ("5. After Installation", [
        "• Opens full-screen without address bar",
        "• Automatic updates daily - stay current with latest data",
        "• Quick access from app library/home screen"
    ]),
    ("6. Key Features", [
        "• Inbound sales metrics and trend analysis",
        "• Partner performance dashboard",
        "• Country-wise booking calendar",
        "• Real-time data synchronization"
    ])
]

for section_title, items in sections:
    c.setFont("Helvetica-Bold", 13)
    c.setFillColor(colors.HexColor("#6ba3c4"))
    c.drawString(1*cm, y, section_title)
    y -= 0.35*cm

    for item in items:
        c.setFont("Helvetica", 10)
        c.setFillColor(colors.HexColor("#f0f0f0"))
        c.drawString(1.2*cm, y, item)
        y -= line_height

    y -= 0.15*cm

# Footer
c.setFont("Helvetica", 9)
c.setFillColor(colors.HexColor("#c9a063"))
c.drawString(1*cm, 0.6*cm, "For technical support, contact your team administrator")
c.setStrokeColor(colors.HexColor("#333333"))
c.setLineWidth(0.5)
c.line(1*cm, 0.55*cm, width - 1*cm, 0.55*cm)

c.save()

print(f"PDF created successfully: {output_path}")
