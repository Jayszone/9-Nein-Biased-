import json
import re
import os
import requests
from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-sonnet-4-20250514"

def call_claude(prompt, system=None, max_tokens=4096):
    """Make a call to the Claude API."""
    headers = {
        "Content-Type": "application/json",
        "x-api-key": os.environ.get("ANTHROPIC_API_KEY", ""),
        "anthropic-version": "2023-06-01"
    }
    body = {
        "model": MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        body["system"] = system

    response = requests.post(ANTHROPIC_API_URL, headers=headers, json=body)
    response.raise_for_status()
    data = response.json()
    return data["content"][0]["text"]


def cluster_top_stories(articles, n=3):
    """Use AI to identify the top N stories and group articles by story."""
    # Build a compact article list for the prompt
    article_list = []
    for i, a in enumerate(articles):
        article_list.append(f"[{i}] {a['source_name']} ({a['source_bias']}): {a['title']}")

    article_text = "\n".join(article_list)

    prompt = f"""You are a news editor. Below is a list of articles from various news sources published today.

Your task:
1. Identify the top {n} most significant news stories covered across multiple sources.
2. For each story, group the article indices that cover it.
3. Give each story a neutral, factual headline that doesn't favor any political perspective.

ARTICLES:
{article_text}

Respond ONLY with a valid JSON array, no preamble, no markdown, no backticks. Format:
[
  {{
    "story_headline": "Neutral headline for story 1",
    "article_indices": [0, 5, 12, 18]
  }},
  ...
]

Rules:
- Only include stories covered by at least 2 different sources
- Prefer stories with coverage from BOTH left and right-leaning sources
- The headline must be objective and factual
- Return exactly {n} stories"""

    raw = call_claude(prompt)
    try:
        stories = json.loads(raw)
    except json.JSONDecodeError:
        # Try to extract JSON if there's any surrounding text
        import re
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if match:
            stories = json.loads(match.group())
        else:
            raise ValueError(f"Could not parse clustering response: {raw[:200]}")

    # Attach full article objects to each story
    result = []
    for story in stories[:n]:
        story_articles = [articles[i] for i in story["article_indices"] if i < len(articles)]
        result.append({
            "headline": story["story_headline"],
            "articles": story_articles,
        })

    return result


def analyze_story(story):
    """For a single story, score each article on opinion vs analysis, then extract shared facts."""
    articles = story["articles"]

    # Build article context
    article_blocks = []
    for i, a in enumerate(articles):
        article_blocks.append(
            f"Article {i+1} | {a['source_name']} | Bias: {a['source_bias']}\n"
            f"Title: {a['title']}\n"
            f"Summary: {a['summary']}\n"
        )
    articles_text = "\n---\n".join(article_blocks)

    prompt = f"""You are an expert media analyst for a bias-detection newsletter called "9 (Nein) Biased".

STORY: {story['headline']}

ARTICLES:
{articles_text}

Perform two tasks and return ONLY valid JSON, no preamble, no backticks:

TASK 1 — Score each article on a scale from 0.0 (pure opinion/editorial) to 1.0 (pure factual analysis).
Consider: use of loaded language, presence of verifiable facts, attribution, emotional framing.

TASK 2 — Extract the "Factual Core": 3-5 bullet points that ALL sources appear to agree on, regardless of framing.
These should be verifiable facts, not interpretations.

TASK 3 — Write a 2-sentence "Framing Contrast" that explains how left-leaning vs right-leaning sources frame this story differently. Be specific and neutral in your own tone.

JSON format:
{{
  "article_scores": [
    {{
      "source_name": "...",
      "source_bias": "left|center|right",
      "opinion_vs_analysis_score": 0.0,
      "score_reasoning": "One sentence explanation"
    }}
  ],
  "factual_core": [
    "Fact 1",
    "Fact 2",
    "Fact 3"
  ],
  "framing_contrast": "Two sentences explaining the framing difference."
}}"""

    raw = call_claude(prompt, max_tokens=2000)
    try:
        analysis = json.loads(raw)
    except json.JSONDecodeError:
        import re
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            analysis = json.loads(match.group())
        else:
            raise ValueError(f"Could not parse analysis response: {raw[:200]}")

    return analysis
