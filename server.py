"""CronPilot — Managed cron-job-as-a-service with monitoring, retries, and alerting."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import db
from scheduler import (
    start_scheduler,
    stop_scheduler,
    schedule_job,
    unschedule_job,
    get_next_run_times,
    MAX_JOBS_FREE,
)
from executor import execute_job
from cron_parser import validate_cron, next_run_time, next_n_runs, describe_cron

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("cronpilot")

PORT = int(os.environ.get("PORT", "8440"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    start_scheduler()
    logger.info(f"CronPilot started on port {PORT}")
    yield
    stop_scheduler()


app = FastAPI(
    title="CronPilot",
    description="Managed cron-job-as-a-service with monitoring, retries, and alerting",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Pydantic Models ---

class JobCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    url: str | None = None
    command: str | None = None
    schedule: str = Field(..., description="Cron expression, e.g. '*/5 * * * *'")
    method: str = Field(default="GET")
    headers: dict[str, str] = Field(default_factory=dict)
    body: str | None = None
    retries: int = Field(default=3, ge=0, le=10)
    timeout_seconds: int = Field(default=30, ge=1, le=300)
    alert_url: str | None = None
    alert_on: str = Field(default="failure", description="Comma-separated: failure,recovery,slow,dead_letter,all")
    on_success_trigger: str | None = Field(default=None, description="Job ID to trigger on success")


class JobUpdate(BaseModel):
    name: str | None = None
    url: str | None = None
    command: str | None = None
    schedule: str | None = None
    method: str | None = None
    headers: dict[str, str] | None = None
    body: str | None = None
    retries: int | None = Field(default=None, ge=0, le=10)
    timeout_seconds: int | None = Field(default=None, ge=1, le=300)
    alert_url: str | None = None
    alert_on: str | None = None
    on_success_trigger: str | None = None


# --- Health ---

@app.get("/health")
async def health():
    return {"status": "ok", "service": "cronpilot"}


# --- Dashboard ---

@app.get("/dashboard")
async def dashboard():
    stats = db.get_dashboard_stats()
    next_runs = get_next_run_times()
    jobs = db.list_jobs()

    upcoming = []
    for job in jobs:
        if not job["paused"]:
            job_next = next_runs.get(job["id"])
            if job_next is None:
                try:
                    nrt = next_run_time(job["schedule"])
                    job_next = nrt.isoformat()
                except Exception:
                    job_next = None
            upcoming.append({
                "job_id": job["id"],
                "name": job["name"],
                "schedule": job["schedule"],
                "schedule_description": describe_cron(job["schedule"]),
                "next_run": job_next,
            })

    upcoming.sort(key=lambda x: x["next_run"] or "9999")

    return {
        **stats,
        "upcoming": upcoming[:20],
    }


# --- Jobs CRUD ---

@app.post("/jobs", status_code=201)
async def create_job_endpoint(data: JobCreate):
    if not data.url and not data.command:
        raise HTTPException(400, "Either 'url' or 'command' must be provided")

    if not validate_cron(data.schedule):
        raise HTTPException(400, f"Invalid cron expression: {data.schedule}")

    # Free tier limit
    existing = db.list_jobs()
    if len(existing) >= MAX_JOBS_FREE:
        raise HTTPException(
            429,
            f"Free tier limit reached ({MAX_JOBS_FREE} jobs). Delete unused jobs or upgrade.",
        )

    job = db.create_job(data.model_dump())
    schedule_job(job)

    nrt = next_run_time(job["schedule"])
    job["next_run"] = nrt.isoformat()
    job["schedule_description"] = describe_cron(job["schedule"])
    return job


@app.get("/jobs")
async def list_jobs_endpoint():
    jobs = db.list_jobs()
    next_runs = get_next_run_times()
    for job in jobs:
        job["next_run"] = next_runs.get(job["id"])
        if job["next_run"] is None and not job["paused"]:
            try:
                job["next_run"] = next_run_time(job["schedule"]).isoformat()
            except Exception:
                pass
        job["schedule_description"] = describe_cron(job["schedule"])
    return {"jobs": jobs, "count": len(jobs)}


@app.get("/jobs/{job_id}")
async def get_job_endpoint(job_id: str):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "Job not found")

    recent_runs = db.list_runs(job_id, limit=10)
    next_runs = get_next_run_times()
    job["next_run"] = next_runs.get(job_id)
    if job["next_run"] is None and not job["paused"]:
        try:
            job["next_run"] = next_run_time(job["schedule"]).isoformat()
        except Exception:
            pass
    job["schedule_description"] = describe_cron(job["schedule"])
    job["recent_runs"] = recent_runs

    # Compute success rate from recent runs
    completed = [r for r in recent_runs if r["status"] in ("success", "failed", "dead", "timeout")]
    if completed:
        successes = sum(1 for r in completed if r["status"] == "success")
        job["success_rate"] = round(successes / len(completed) * 100, 1)
    else:
        job["success_rate"] = None

    return job


@app.put("/jobs/{job_id}")
async def update_job_endpoint(job_id: str, data: JobUpdate):
    if data.schedule and not validate_cron(data.schedule):
        raise HTTPException(400, f"Invalid cron expression: {data.schedule}")

    update_data = {k: v for k, v in data.model_dump().items() if v is not None}
    job = db.update_job(job_id, update_data)
    if job is None:
        raise HTTPException(404, "Job not found")

    schedule_job(job)
    return job


@app.delete("/jobs/{job_id}")
async def delete_job_endpoint(job_id: str):
    unschedule_job(job_id)
    deleted = db.delete_job(job_id)
    if not deleted:
        raise HTTPException(404, "Job not found")
    return {"deleted": True, "job_id": job_id}


# --- Job Actions ---

@app.post("/jobs/{job_id}/trigger")
async def trigger_job_endpoint(job_id: str):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "Job not found")

    run = await execute_job(job, triggered_by="manual")
    return {"triggered": True, "run": run}


@app.post("/jobs/{job_id}/pause")
async def pause_job_endpoint(job_id: str):
    job = db.set_job_paused(job_id, True)
    if job is None:
        raise HTTPException(404, "Job not found")
    unschedule_job(job_id)
    return {"paused": True, "job": job}


@app.post("/jobs/{job_id}/resume")
async def resume_job_endpoint(job_id: str):
    job = db.set_job_paused(job_id, False)
    if job is None:
        raise HTTPException(404, "Job not found")
    schedule_job(job)
    return {"resumed": True, "job": job}


# --- Runs ---

@app.get("/jobs/{job_id}/runs")
async def list_runs_endpoint(job_id: str, limit: int = Query(default=50, ge=1, le=500)):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "Job not found")

    runs = db.list_runs(job_id, limit=limit)
    return {"runs": runs, "count": len(runs), "job_id": job_id}


@app.get("/jobs/{job_id}/runs/{run_id}")
async def get_run_endpoint(job_id: str, run_id: str):
    run = db.get_run(run_id)
    if run is None or run["job_id"] != job_id:
        raise HTTPException(404, "Run not found")
    return run


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
