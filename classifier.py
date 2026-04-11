import json
import re
import os
import requests
from dotenv import load_dotenv


def extract_json(raw: str, expect: str = "object"):
    """Robustly extract the first complete JSON object or array from a string."""
    if not raw:
        raise ValueError("Empty response from model")

    # Strip markdown code fences
    raw = re.sub(r'```(?:json)?\s*', '', raw).strip()

    opener, closer = ('{', '}') if expect == "object" else ('[', ']')

    # Find first opener
    start = raw.find(opener)
    if start == -1:
        raise ValueError(f"No JSON {expect} found in response: {raw[:200]}")

    # Walk forward counting balanced braces to find the matching closer
    depth = 0
    in_string = False
    escape = False
    for i, ch in enumerate(raw[start:], start):
        if escape:
            escape = False
            continue
        if ch == '\\' and in_string:
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == opener:
            depth += 1
        elif ch == closer:
            depth -= 1
            if depth == 0:
                candidate = raw[start:i+1]
                return json.loads(candidate)

    raise ValueError(f"Could not find complete JSON in response: {raw[:200]}")

load_dotenv()

OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "nvidia/nemotron-3-super-120b-a12b:free"

def call_claude(prompt, system=None, max_tokens=8192):
    """Make a call via OpenRouter."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.environ.get('OPENROUTER_API_KEY', '')}",
        "HTTP-Referer": "http://localhost:8000",
        "X-Title": "9 (Nein) Biased",
    }
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    body = {
        "model": MODEL,
        "max_tokens": max_tokens,
        "messages": messages,
    }

    response = requests.post(OPENROUTER_API_URL, headers=headers, json=body, timeout=90)
    response.raise_for_status()
    data = response.json()

    if "error" in data:
        raise ValueError(f"OpenRouter error: {data['error']}")

    if "choices" not in data or not data["choices"]:
        raise ValueError(f"No choices in response. Full response: {data}")

    message = data["choices"][0]["message"]
    content = message.get("content") or message.get("reasoning") or ""
    if not content:
        raise ValueError("Model returned empty content")
    return content


def cluster_top_stories(articles, n=3):
    """Use AI to identify the top N stories and group articles by story."""
    # Cap at 45 articles to stay within model context limits
    articles = articles[:45]

    # Build a compact article list for the prompt
    article_list = []
    for i, a in enumerate(articles):
        article_list.append(f"[{i}] {a['source_name']} ({a['source_bias']}): {a['title']}")

    article_text = "\n".join(article_list)

    system = "You are a JSON API. You output only raw valid JSON with no explanation, no markdown, no backticks, no preamble. Your entire response must be parseable by json.loads()."

    prompt = f"""ARTICLES:
{article_text}

Output ONLY a JSON array of exactly {n} stories (no explanation, no markdown):
[
  {{
    "story_headline": "Neutral factual headline",
    "article_indices": [0, 5, 12]
  }}
]

Rules:
- Each story must be covered by at least 2 different sources
- Prefer stories with both left and right-leaning coverage
- Headline must be objective and factual
- Return exactly {n} items
- Start your response with [ and end with ]"""

    raw = call_claude(prompt, system=system)
    stories = extract_json(raw, expect="array")

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

    system = "You are a JSON API. You output only raw valid JSON with no explanation, no markdown, no backticks, no preamble. Your entire response must be parseable by json.loads()."

    prompt = f"""STORY: {story['headline']}

ARTICLES:
{articles_text}

Output ONLY this JSON object (no explanation, no markdown):
{{
  "article_scores": [
    {{
      "source_name": "exact source name from above",
      "source_bias": "left or center or right",
      "opinion_vs_analysis_score": 0.75,
      "score_reasoning": "one sentence"
    }}
  ],
  "factual_core": ["fact 1", "fact 2", "fact 3"],
  "framing_contrast": "Two sentences on left vs right framing."
}}

Rules:
- Include one entry in article_scores for EACH article listed above
- opinion_vs_analysis_score: 0.0 = pure opinion, 1.0 = pure fact
- factual_core: 3-5 facts ALL sources agree on
- Start your response with {{ and end with }}"""

    raw = call_claude(prompt, system=system, max_tokens=4096)
    analysis = extract_json(raw, expect="object")

    return analysis
