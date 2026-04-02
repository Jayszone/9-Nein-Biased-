import sqlite3
import json
import datetime
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "nein_biased.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            status    TEXT NOT NULL DEFAULT 'running',
            error     TEXT
        );

        CREATE TABLE IF NOT EXISTS stories (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id           INTEGER NOT NULL,
            position         INTEGER NOT NULL,
            headline         TEXT NOT NULL,
            created_at       TEXT NOT NULL,
            factual_core     TEXT NOT NULL,
            framing_contrast TEXT NOT NULL,
            article_scores   TEXT NOT NULL,
            article_count    INTEGER NOT NULL,
            image_url        TEXT NOT NULL DEFAULT '',
            FOREIGN KEY (run_id) REFERENCES runs(id)
        );
    """)
    conn.commit()
    conn.close()


def create_run() -> int:
    conn = get_conn()
    cur = conn.execute(
        "INSERT INTO runs (created_at, status) VALUES (?, 'running')",
        (datetime.datetime.utcnow().isoformat(),)
    )
    run_id = cur.lastrowid
    conn.commit()
    conn.close()
    return run_id


def finish_run(run_id: int, error: str = None):
    conn = get_conn()
    if error:
        conn.execute(
            "UPDATE runs SET status='error', error=? WHERE id=?",
            (error, run_id)
        )
    else:
        conn.execute(
            "UPDATE runs SET status='done' WHERE id=?",
            (run_id,)
        )
    conn.commit()
    conn.close()


def save_story(run_id: int, position: int, story: dict, analysis: dict, image_url: str = "") -> int:
    conn = get_conn()
    cur = conn.execute(
        """INSERT INTO stories
           (run_id, position, headline, created_at, factual_core, framing_contrast, article_scores, article_count, image_url)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            run_id,
            position,
            story["headline"],
            datetime.datetime.utcnow().isoformat(),
            json.dumps(analysis.get("factual_core", [])),
            analysis.get("framing_contrast", ""),
            json.dumps(analysis.get("article_scores", [])),
            len(story.get("articles", [])),
            image_url,
        )
    )
    story_id = cur.lastrowid
    conn.commit()
    conn.close()
    return story_id


def get_latest_stories(limit: int = 3) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT s.*, r.created_at as run_date, r.status as run_status
        FROM stories s
        JOIN runs r ON s.run_id = r.id
        WHERE r.status = 'done'
        ORDER BY r.id DESC, s.position ASC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [_row_to_dict(r) for r in rows]


def get_story(story_id: int) -> dict | None:
    conn = get_conn()
    row = conn.execute("""
        SELECT s.*, r.created_at as run_date
        FROM stories s
        JOIN runs r ON s.run_id = r.id
        WHERE s.id = ?
    """, (story_id,)).fetchone()
    conn.close()
    return _row_to_dict(row) if row else None


def get_all_runs() -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM runs ORDER BY id DESC LIMIT 20"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_run_status(run_id: int) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def _row_to_dict(row) -> dict:
    d = dict(row)
    for key in ("factual_core", "article_scores"):
        if key in d and isinstance(d[key], str):
            d[key] = json.loads(d[key])
    return d
