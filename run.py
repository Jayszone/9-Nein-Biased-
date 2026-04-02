#!/usr/bin/env python3
"""
9 (Nein) Biased — Daily Newsletter Pipeline
============================================
Run this script once daily to:
1. Fetch articles from all RSS sources
2. Cluster into top 3 stories using AI
3. Analyze each story (bias map, shared facts, framing contrast)
4. Render HTML newsletter
5. Send via Gmail (or save to file)

Usage:
  python run.py                    # Full run: generate + send email
  python run.py --save-only        # Generate HTML file only, don't send
  python run.py --preview          # Save HTML and open in browser

Setup:
  1. Copy .env.example to .env and fill in your credentials
  2. pip install feedparser requests python-dotenv
"""

import os
import sys
import argparse
import datetime
import subprocess
from dotenv import load_dotenv

from fetcher import fetch_articles
from classifier import cluster_top_stories, analyze_story
from renderer import render_newsletter
from sender import send_newsletter

load_dotenv()


def main():
    parser = argparse.ArgumentParser(description="Run the 9 (Nein) Biased newsletter pipeline")
    parser.add_argument("--save-only", action="store_true", help="Save HTML to file, skip sending")
    parser.add_argument("--preview", action="store_true", help="Save HTML and open in browser")
    parser.add_argument("--output", default="newsletter_output.html", help="Output HTML file path")
    args = parser.parse_args()

    print("=" * 60)
    print("  9 (Nein) Biased — Newsletter Pipeline")
    print(f"  {datetime.date.today().strftime('%A, %B %d, %Y')}")
    print("=" * 60)

    # ── Step 1: Fetch Articles ──────────────────────────────────
    print("\n[1/4] Fetching articles from RSS feeds...")
    articles = fetch_articles(max_per_source=8, days_back=1)

    if len(articles) < 6:
        print(f"⚠ Only {len(articles)} articles fetched. This may be too few to cluster. Continuing anyway...")

    # ── Step 2: Cluster into Top 3 Stories ─────────────────────
    print("\n[2/4] Clustering top 3 stories with AI...")
    stories = cluster_top_stories(articles, n=3)
    print(f"  Found {len(stories)} stories:")
    for i, s in enumerate(stories, 1):
        print(f"  {i}. {s['headline']} ({len(s['articles'])} sources)")

    # ── Step 3: Analyze Each Story ──────────────────────────────
    print("\n[3/4] Analyzing stories (bias scoring, facts, framing)...")
    stories_with_analysis = []
    for i, story in enumerate(stories, 1):
        print(f"  Analyzing story {i}: {story['headline'][:60]}...")
        analysis = analyze_story(story)
        stories_with_analysis.append((story, analysis))
        print(f"    ✓ {len(analysis.get('article_scores', []))} sources scored, "
              f"{len(analysis.get('factual_core', []))} shared facts found")

    # ── Step 4: Render Newsletter ────────────────────────────────
    print("\n[4/4] Rendering newsletter HTML...")
    html = render_newsletter(stories_with_analysis)

    # Save HTML to file
    output_path = args.output
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  ✓ Newsletter saved to: {output_path}")

    # Preview in browser
    if args.preview:
        try:
            subprocess.run(["open", output_path])  # macOS
        except Exception:
            try:
                subprocess.run(["xdg-open", output_path])  # Linux
            except Exception:
                print(f"  → Open {output_path} in your browser to preview")

    # Send via Gmail
    if not args.save_only and not args.preview:
        gmail_user = os.getenv("GMAIL_USER")
        gmail_password = os.getenv("GMAIL_APP_PASSWORD")
        recipients_raw = os.getenv("RECIPIENTS", "")
        recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]

        if not gmail_user or not gmail_password:
            print("\n⚠ Gmail credentials not configured in .env — skipping send.")
            print("  Set GMAIL_USER and GMAIL_APP_PASSWORD in your .env file.")
        elif not recipients:
            print("\n⚠ No RECIPIENTS configured in .env — skipping send.")
        else:
            print(f"\n[Send] Sending to {len(recipients)} recipient(s)...")
            send_newsletter(html, recipients, gmail_user, gmail_password)

    print("\n✓ Pipeline complete.")


if __name__ == "__main__":
    main()
