import requests
import os
from dotenv import load_dotenv

load_dotenv()

# Explicitly defined sources with bias — used for targeted NewsAPI queries
TARGETED_SOURCES = [
    # LEFT
    {"name": "The Atlantic", "bias": "left", "newsapi_id": "the-atlantic"},
    {"name": "Mother Jones", "bias": "left", "newsapi_id": None},
    {"name": "MSNBC", "bias": "left", "newsapi_id": "msnbc"},
    {"name": "HuffPost", "bias": "left", "newsapi_id": "the-huffington-post"},
    {"name": "Buzzfeed News", "bias": "left", "newsapi_id": "buzzfeed"},

    # CENTER
    {"name": "Reuters", "bias": "center", "newsapi_id": "reuters"},
    {"name": "Associated Press", "bias": "center", "newsapi_id": "associated-press"},
    {"name": "NPR", "bias": "center", "newsapi_id": "npm"},
    {"name": "The Hill", "bias": "center", "newsapi_id": "the-hill"},
    {"name": "Axios", "bias": "center", "newsapi_id": "axios"},
    {"name": "BBC News", "bias": "center", "newsapi_id": "bbc-news"},
    {"name": "ABC News", "bias": "center", "newsapi_id": "abc-news"},
    {"name": "CBS News", "bias": "center", "newsapi_id": "cbs-news"},
    {"name": "NBC News", "bias": "center", "newsapi_id": "nbc-news"},
    {"name": "USA Today", "bias": "center", "newsapi_id": "usa-today"},

    # RIGHT
    {"name": "Fox News", "bias": "right", "newsapi_id": "fox-news"},
    {"name": "Breitbart", "bias": "right", "newsapi_id": "breitbart-news"},
    {"name": "The Washington Times", "bias": "right", "newsapi_id": "the-washington-times"},
    {"name": "National Review", "bias": "right", "newsapi_id": "national-review"},
    {"name": "Newsmax", "bias": "right", "newsapi_id": "newsmax"},
]

BIAS_MAP = {s["name"].lower(): s["bias"] for s in TARGETED_SOURCES}
NEWSAPI_ID_MAP = {s["newsapi_id"]: s for s in TARGETED_SOURCES if s["newsapi_id"]}


def fetch_articles(max_per_source=5, days_back=1):
    """Fetch articles using NewsAPI, targeting specific left/center/right sources."""
    api_key = os.environ.get("NEWS_API_KEY", "")
    if not api_key:
        print("  ✗ NEWS_API_KEY not set in .env")
        return []

    all_articles = []
    seen_titles = set()

    def add_articles(articles_data, default_bias="center"):
        for article in articles_data:
            title = (article.get("title") or "").strip()
            if not title or title == "[Removed]":
                continue
            if title in seen_titles:
                continue
            seen_titles.add(title)

            source_name = article.get("source", {}).get("name", "Unknown")
            source_id = article.get("source", {}).get("id", "")

            # Determine bias
            bias = default_bias
            if source_id and source_id in NEWSAPI_ID_MAP:
                bias = NEWSAPI_ID_MAP[source_id]["bias"]
                source_name = NEWSAPI_ID_MAP[source_id]["name"]
            else:
                for known_name, known_bias in BIAS_MAP.items():
                    if known_name in source_name.lower():
                        bias = known_bias
                        break

            all_articles.append({
                "source_name": source_name,
                "source_bias": bias,
                "title": title,
                "summary": (article.get("description") or title)[:500].strip(),
                "link": article.get("url", ""),
                "published": article.get("publishedAt", "unknown"),
            })

    # Pull 1: Top headlines (broad)
    try:
        r = requests.get("https://newsapi.org/v2/top-headlines", params={
            "apiKey": api_key, "language": "en", "country": "us", "pageSize": 100,
        }, timeout=10)
        add_articles(r.json().get("articles", []))
        print(f"  Top headlines: {len(all_articles)} articles so far")
    except Exception as e:
        print(f"  ✗ Top headlines failed: {e}")

    # Pull 2: Targeted left sources
    left_ids = ",".join([s["newsapi_id"] for s in TARGETED_SOURCES if s["bias"] == "left" and s["newsapi_id"]])
    if left_ids:
        try:
            r = requests.get("https://newsapi.org/v2/top-headlines", params={
                "apiKey": api_key, "sources": left_ids, "pageSize": 50,
            }, timeout=10)
            before = len(all_articles)
            add_articles(r.json().get("articles", []), default_bias="left")
            print(f"  Left sources: +{len(all_articles) - before} articles")
        except Exception as e:
            print(f"  ✗ Left sources failed: {e}")

    # Pull 3: Targeted right sources
    right_ids = ",".join([s["newsapi_id"] for s in TARGETED_SOURCES if s["bias"] == "right" and s["newsapi_id"]])
    if right_ids:
        try:
            r = requests.get("https://newsapi.org/v2/top-headlines", params={
                "apiKey": api_key, "sources": right_ids, "pageSize": 50,
            }, timeout=10)
            before = len(all_articles)
            add_articles(r.json().get("articles", []), default_bias="right")
            print(f"  Right sources: +{len(all_articles) - before} articles")
        except Exception as e:
            print(f"  ✗ Right sources failed: {e}")

    # Pull 4: Politics category
    try:
        r = requests.get("https://newsapi.org/v2/top-headlines", params={
            "apiKey": api_key, "language": "en", "country": "us",
            "category": "politics", "pageSize": 50,
        }, timeout=10)
        before = len(all_articles)
        add_articles(r.json().get("articles", []))
        print(f"  Politics category: +{len(all_articles) - before} articles")
    except Exception as e:
        print(f"  ✗ Politics category failed: {e}")

    # Print bias distribution
    from collections import Counter
    bias_counts = Counter(a["source_bias"] for a in all_articles)
    print(f"\nBias distribution: Left={bias_counts['left']} | Center={bias_counts['center']} | Right={bias_counts['right']}")
    print(f"Total articles fetched: {len(all_articles)}")
    return all_articles
