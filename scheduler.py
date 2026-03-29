"""Cron scheduler engine using APScheduler."""

import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import db
from executor import execute_job
from cron_parser import validate_cron

logger = logging.getLogger("cronpilot.scheduler")

# Global scheduler instance
_scheduler: AsyncIOScheduler | None = None

# Free tier limits
MAX_JOBS_FREE = int(__import__("os").environ.get("MAX_JOBS_FREE", "10"))
MAX_RUNS_PER_DAY = int(__import__("os").environ.get("MAX_RUNS_PER_DAY", "100"))


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncIOScheduler(timezone="UTC")
    return _scheduler


def start_scheduler():
    """Start the scheduler and load all active jobs."""
    scheduler = get_scheduler()
    if scheduler.running:
        return

    # Load existing jobs from DB
    jobs = db.list_jobs()
    loaded = 0
    for job in jobs:
        if not job["paused"]:
            _add_job_to_scheduler(job)
            loaded += 1

    scheduler.start()
    logger.info(f"Scheduler started with {loaded} active jobs")


def stop_scheduler():
    scheduler = get_scheduler()
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


def schedule_job(job: dict):
    """Add or update a job in the scheduler."""
    scheduler = get_scheduler()
    job_tag = f"cron_{job['id']}"

    # Remove existing if any
    existing = scheduler.get_job(job_tag)
    if existing:
        scheduler.remove_job(job_tag)

    if job.get("paused"):
        logger.info(f"Job '{job['name']}' is paused, not scheduling")
        return

    _add_job_to_scheduler(job)


def unschedule_job(job_id: str):
    """Remove a job from the scheduler."""
    scheduler = get_scheduler()
    job_tag = f"cron_{job_id}"
    existing = scheduler.get_job(job_tag)
    if existing:
        scheduler.remove_job(job_tag)
        logger.info(f"Unscheduled job {job_id}")


def _add_job_to_scheduler(job: dict):
    """Internal: add a job to APScheduler."""
    scheduler = get_scheduler()
    job_tag = f"cron_{job['id']}"

    if not validate_cron(job["schedule"]):
        logger.error(f"Invalid cron expression for job '{job['name']}': {job['schedule']}")
        return

    parts = job["schedule"].strip().split()
    if len(parts) != 5:
        logger.error(f"Cron expression must have 5 fields: {job['schedule']}")
        return

    minute, hour, day, month, day_of_week = parts

    trigger = CronTrigger(
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=day_of_week,
        timezone="UTC",
    )

    scheduler.add_job(
        _run_job,
        trigger=trigger,
        id=job_tag,
        args=[job["id"]],
        name=job["name"],
        replace_existing=True,
        misfire_grace_time=60,
    )
    logger.info(f"Scheduled job '{job['name']}' ({job['id']}): {job['schedule']}")


async def _run_job(job_id: str):
    """Callback invoked by APScheduler to run a job."""
    # Check daily run limit
    runs_today = db.count_runs_today()
    if runs_today >= MAX_RUNS_PER_DAY:
        logger.warning(f"Daily run limit ({MAX_RUNS_PER_DAY}) reached, skipping job {job_id}")
        return

    job = db.get_job(job_id)
    if job is None:
        logger.warning(f"Job {job_id} not found, removing from scheduler")
        unschedule_job(job_id)
        return

    if job.get("paused"):
        logger.debug(f"Job '{job['name']}' is paused, skipping")
        return

    logger.info(f"Running scheduled job '{job['name']}' ({job_id})")
    await execute_job(job, triggered_by="scheduler")


def get_next_run_times() -> dict[str, str | None]:
    """Get next run time for all scheduled jobs."""
    scheduler = get_scheduler()
    result = {}
    for aps_job in scheduler.get_jobs():
        job_id = aps_job.id.replace("cron_", "")
        next_run = aps_job.next_run_time
        result[job_id] = next_run.isoformat() if next_run else None
    return result
