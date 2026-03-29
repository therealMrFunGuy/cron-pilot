"""Job execution engine for CronPilot — handles HTTP calls with retries."""

import asyncio
import logging
import time
from typing import Optional

import httpx

import db
from alerts import send_alert, send_dead_letter_alert, send_recovery_alert, send_slow_alert

logger = logging.getLogger("cronpilot.executor")


async def execute_job(job: dict, triggered_by: str = "scheduler") -> dict:
    """Execute a job with retry logic. Returns the final run record."""
    max_retries = job.get("retries", 3)
    timeout_seconds = job.get("timeout_seconds", 30)
    last_run = None

    # Check if previously failing (for recovery alerts)
    recent_runs = db.list_runs(job["id"], limit=1)
    was_failing = len(recent_runs) > 0 and recent_runs[0].get("status") == "failed"

    for attempt in range(1, max_retries + 2):  # attempts = retries + 1
        run = db.create_run(job["id"], attempt=attempt)
        start = time.monotonic()

        try:
            if job.get("url"):
                result = await _execute_http(job, timeout_seconds)
            elif job.get("command"):
                result = await _execute_command(job, timeout_seconds)
            else:
                raise ValueError("Job has neither url nor command configured")

            duration_ms = (time.monotonic() - start) * 1000
            db.finish_run(
                run["id"],
                status="success",
                response_code=result.get("status_code"),
                response_body=result.get("body", "")[:10000],
                duration_ms=round(duration_ms, 2),
            )
            last_run = db.get_run(run["id"])
            logger.info(f"Job '{job['name']}' succeeded (attempt {attempt}, {duration_ms:.0f}ms)")

            # Recovery alert
            if was_failing:
                await send_recovery_alert(job, last_run)

            # Slow alert
            await send_slow_alert(job, last_run)

            # Job chaining
            if job.get("on_success_trigger"):
                await _trigger_chain(job["on_success_trigger"])

            return last_run

        except asyncio.TimeoutError:
            duration_ms = (time.monotonic() - start) * 1000
            error_msg = f"Timeout after {timeout_seconds}s"
            db.finish_run(
                run["id"],
                status="timeout" if attempt > max_retries else "failed",
                duration_ms=round(duration_ms, 2),
                error=error_msg,
            )
            last_run = db.get_run(run["id"])
            logger.warning(f"Job '{job['name']}' timed out (attempt {attempt})")

        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            error_msg = str(e)[:2000]
            status = "failed"
            db.finish_run(
                run["id"],
                status=status,
                response_code=getattr(e, "status_code", None),
                duration_ms=round(duration_ms, 2),
                error=error_msg,
            )
            last_run = db.get_run(run["id"])
            logger.warning(f"Job '{job['name']}' failed (attempt {attempt}): {error_msg[:200]}")

        # Retry with exponential backoff
        if attempt <= max_retries:
            backoff = min(2 ** (attempt - 1), 60)
            logger.info(f"Retrying job '{job['name']}' in {backoff}s...")
            await asyncio.sleep(backoff)
        else:
            # All retries exhausted — dead letter
            logger.error(f"Job '{job['name']}' dead-lettered after {attempt} attempts")
            if last_run:
                db.finish_run(last_run["id"], status="dead", error=last_run.get("error"))
                last_run = db.get_run(last_run["id"])
                await send_dead_letter_alert(job, last_run)
                await send_alert(job, last_run, alert_type="failure")

    return last_run


async def _execute_http(job: dict, timeout_seconds: int) -> dict:
    """Execute an HTTP job."""
    method = (job.get("method") or "GET").upper()
    url = job["url"]
    headers = job.get("headers", {})
    body = job.get("body")

    async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
        kwargs = {"method": method, "url": url, "headers": headers}
        if body and method in ("POST", "PUT", "PATCH"):
            # Try to detect JSON
            if isinstance(body, str):
                kwargs["content"] = body
                if "content-type" not in {k.lower() for k in headers}:
                    kwargs["headers"]["Content-Type"] = "application/json"
            else:
                kwargs["json"] = body

        resp = await client.request(**kwargs)

        if resp.status_code >= 400:
            error = httpx.HTTPStatusError(
                f"HTTP {resp.status_code}", request=resp.request, response=resp
            )
            error.status_code = resp.status_code
            raise error

        return {
            "status_code": resp.status_code,
            "body": resp.text[:10000],
        }


async def _execute_command(job: dict, timeout_seconds: int) -> dict:
    """Execute a shell command job."""
    proc = await asyncio.create_subprocess_shell(
        job["command"],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        proc.kill()
        raise

    output = stdout.decode("utf-8", errors="replace")[:10000]
    if proc.returncode != 0:
        err_output = stderr.decode("utf-8", errors="replace")[:2000]
        raise RuntimeError(f"Command exited with code {proc.returncode}: {err_output}")

    return {"status_code": 0, "body": output}


async def _trigger_chain(job_id: str):
    """Trigger a chained job on success."""
    chained_job = db.get_job(job_id)
    if chained_job and not chained_job.get("paused"):
        logger.info(f"Triggering chained job '{chained_job['name']}' ({job_id})")
        asyncio.create_task(execute_job(chained_job, triggered_by="chain"))
    elif chained_job is None:
        logger.warning(f"Chained job {job_id} not found")
