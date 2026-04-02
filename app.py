#!/usr/bin/env python3
"""
9 (Nein) Biased — Web App
Run: uvicorn app:app --reload --port 8000
"""
import os
import threading
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from dotenv import load_dotenv

load_dotenv()

from db import init_db, create_run, finish_run, save_story, get_latest_stories, get_story, get_all_runs, get_run_status
from fetcher import fetch_articles
from classifier import cluster_top_stories, analyze_story

init_db()

app = FastAPI(title="9 (Nein) Biased")

# Serve static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Track running pipeline
_pipeline_lock = threading.Lock()
_current_run_id: int | None = None


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    return FileResponse(os.path.join(static_dir, "index.html"))


@app.get("/api/stories")
def api_stories(limit: int = 9):
    """Return the most recent analyzed stories."""
    return get_latest_stories(limit=limit)


@app.get("/api/stories/{story_id}")
def api_story(story_id: int):
    story = get_story(story_id)
    if not story:
        raise HTTPException(status_code=404, detail="Story not found")
    return story


@app.get("/api/runs")
def api_runs():
    return get_all_runs()


@app.get("/api/runs/{run_id}/status")
def api_run_status(run_id: int):
    status = get_run_status(run_id)
    if not status:
        raise HTTPException(status_code=404, detail="Run not found")
    return status


@app.post("/api/run")
def api_trigger_run(background_tasks: BackgroundTasks):
    """Trigger a new pipeline run in the background."""
    global _current_run_id
    with _pipeline_lock:
        # Don't allow two runs at once
        if _current_run_id is not None:
            existing = get_run_status(_current_run_id)
            if existing and existing["status"] == "running":
                return {"run_id": _current_run_id, "status": "already_running"}

        run_id = create_run()
        _current_run_id = run_id

    background_tasks.add_task(_run_pipeline, run_id)
    return {"run_id": run_id, "status": "started"}


# ── Pipeline runner ────────────────────────────────────────────────────────────

def _analyze_and_save(run_id: int, position: int, story: dict):
    """Analyze a single story and save it — runs in parallel."""
    print(f"[Pipeline] Analyzing story {position}: {story['headline'][:60]}")
    analysis = analyze_story(story)
    image_url = next(
        (a.get("image_url", "") for a in story.get("articles", []) if a.get("image_url")),
        ""
    )
    save_story(run_id, position, story, analysis, image_url=image_url)
    print(f"[Pipeline] Story {position} saved")


def _run_pipeline(run_id: int):
    try:
        print(f"[Pipeline] Run {run_id} started")

        articles = fetch_articles(max_per_source=8, days_back=1)
        print(f"[Pipeline] Fetched {len(articles)} articles")

        if len(articles) < 4:
            raise ValueError(f"Too few articles ({len(articles)}) to analyze")

        stories = cluster_top_stories(articles, n=3)
        print(f"[Pipeline] Clustered {len(stories)} stories — analyzing in parallel...")

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(_analyze_and_save, run_id, i, story): i
                for i, story in enumerate(stories, 1)
            }
            for future in as_completed(futures):
                future.result()  # re-raises any exception

        finish_run(run_id)
        print(f"[Pipeline] Run {run_id} complete")

    except Exception as e:
        print(f"[Pipeline] Run {run_id} failed: {e}")
        finish_run(run_id, error=str(e))
