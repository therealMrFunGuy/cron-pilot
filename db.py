"""SQLite database layer for CronPilot."""

import sqlite3
import json
import uuid
import os
from datetime import datetime, timezone
from typing import Optional
from contextlib import contextmanager

DB_PATH = os.environ.get("CRONPILOT_DB", "/data/cronpilot/cronpilot.db")


def _ensure_dir():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def get_connection() -> sqlite3.Connection:
    _ensure_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT,
                command TEXT,
                method TEXT DEFAULT 'GET',
                headers_json TEXT DEFAULT '{}',
                body TEXT,
                schedule TEXT NOT NULL,
                retries INTEGER DEFAULT 3,
                timeout_seconds INTEGER DEFAULT 30,
                alert_url TEXT,
                alert_on TEXT DEFAULT 'failure',
                paused INTEGER DEFAULT 0,
                on_success_trigger TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                response_code INTEGER,
                response_body TEXT,
                duration_ms REAL,
                error TEXT,
                started_at TEXT,
                ended_at TEXT,
                attempt INTEGER DEFAULT 1,
                FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_runs_job_id ON runs(job_id);
            CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs(started_at);
        """)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id() -> str:
    return uuid.uuid4().hex[:12]


# --- Job CRUD ---

def create_job(data: dict) -> dict:
    job_id = new_id()
    ts = now_iso()
    with get_db() as conn:
        conn.execute(
            """INSERT INTO jobs (id, name, url, command, method, headers_json, body,
               schedule, retries, timeout_seconds, alert_url, alert_on, paused,
               on_success_trigger, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job_id,
                data["name"],
                data.get("url"),
                data.get("command"),
                data.get("method", "GET"),
                json.dumps(data.get("headers", {})),
                data.get("body"),
                data["schedule"],
                data.get("retries", 3),
                data.get("timeout_seconds", 30),
                data.get("alert_url"),
                data.get("alert_on", "failure"),
                0,
                data.get("on_success_trigger"),
                ts,
                ts,
            ),
        )
    return get_job(job_id)


def get_job(job_id: str) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        return None
    return _row_to_job(row)


def list_jobs() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
    return [_row_to_job(r) for r in rows]


def update_job(job_id: str, data: dict) -> Optional[dict]:
    existing = get_job(job_id)
    if existing is None:
        return None
    fields = []
    values = []
    allowed = [
        "name", "url", "command", "method", "body", "schedule",
        "retries", "timeout_seconds", "alert_url", "alert_on", "on_success_trigger",
    ]
    for key in allowed:
        if key in data:
            fields.append(f"{key} = ?")
            values.append(data[key])
    if "headers" in data:
        fields.append("headers_json = ?")
        values.append(json.dumps(data["headers"]))
    if not fields:
        return existing
    fields.append("updated_at = ?")
    values.append(now_iso())
    values.append(job_id)
    with get_db() as conn:
        conn.execute(f"UPDATE jobs SET {', '.join(fields)} WHERE id = ?", values)
    return get_job(job_id)


def delete_job(job_id: str) -> bool:
    with get_db() as conn:
        cursor = conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    return cursor.rowcount > 0


def set_job_paused(job_id: str, paused: bool) -> Optional[dict]:
    with get_db() as conn:
        cursor = conn.execute(
            "UPDATE jobs SET paused = ?, updated_at = ? WHERE id = ?",
            (1 if paused else 0, now_iso(), job_id),
        )
    if cursor.rowcount == 0:
        return None
    return get_job(job_id)


def _row_to_job(row) -> dict:
    d = dict(row)
    d["headers"] = json.loads(d.pop("headers_json", "{}"))
    d["paused"] = bool(d["paused"])
    return d


# --- Run CRUD ---

def create_run(job_id: str, attempt: int = 1) -> dict:
    run_id = new_id()
    ts = now_iso()
    with get_db() as conn:
        conn.execute(
            """INSERT INTO runs (id, job_id, status, attempt, started_at)
               VALUES (?, ?, 'running', ?, ?)""",
            (run_id, job_id, attempt, ts),
        )
    return {"id": run_id, "job_id": job_id, "status": "running", "attempt": attempt, "started_at": ts}


def finish_run(run_id: str, status: str, response_code: Optional[int] = None,
               response_body: Optional[str] = None, duration_ms: Optional[float] = None,
               error: Optional[str] = None):
    ts = now_iso()
    with get_db() as conn:
        conn.execute(
            """UPDATE runs SET status = ?, response_code = ?, response_body = ?,
               duration_ms = ?, error = ?, ended_at = ? WHERE id = ?""",
            (status, response_code, response_body, duration_ms, error, ts, run_id),
        )


def get_run(run_id: str) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    return dict(row) if row else None


def list_runs(job_id: str, limit: int = 50) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM runs WHERE job_id = ? ORDER BY started_at DESC LIMIT ?",
            (job_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def count_runs_today() -> int:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM runs WHERE started_at >= ?",
            (today,),
        ).fetchone()
    return row["cnt"]


def get_dashboard_stats() -> dict:
    with get_db() as conn:
        total_jobs = conn.execute("SELECT COUNT(*) as cnt FROM jobs").fetchone()["cnt"]
        active_jobs = conn.execute("SELECT COUNT(*) as cnt FROM jobs WHERE paused = 0").fetchone()["cnt"]
        total_runs = conn.execute("SELECT COUNT(*) as cnt FROM runs").fetchone()["cnt"]
        success_runs = conn.execute("SELECT COUNT(*) as cnt FROM runs WHERE status = 'success'").fetchone()["cnt"]
        failed_runs = conn.execute("SELECT COUNT(*) as cnt FROM runs WHERE status = 'failed'").fetchone()["cnt"]
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        runs_today = conn.execute(
            "SELECT COUNT(*) as cnt FROM runs WHERE started_at >= ?", (today,)
        ).fetchone()["cnt"]
    success_rate = (success_runs / total_runs * 100) if total_runs > 0 else 0.0
    return {
        "total_jobs": total_jobs,
        "active_jobs": active_jobs,
        "paused_jobs": total_jobs - active_jobs,
        "total_runs": total_runs,
        "success_runs": success_runs,
        "failed_runs": failed_runs,
        "runs_today": runs_today,
        "success_rate": round(success_rate, 1),
    }
