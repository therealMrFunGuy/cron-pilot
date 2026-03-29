"""Webhook alerting for CronPilot."""

import logging
import httpx
from datetime import datetime, timezone

logger = logging.getLogger("cronpilot.alerts")


async def send_alert(job: dict, run: dict, alert_type: str = "failure"):
    """Send a webhook alert for a job event.

    alert_type: 'failure', 'recovery', 'slow', 'dead_letter'
    """
    alert_url = job.get("alert_url")
    if not alert_url:
        return

    alert_on = job.get("alert_on", "failure")
    allowed_types = [t.strip() for t in alert_on.split(",")]
    if alert_type not in allowed_types and "all" not in allowed_types:
        logger.debug(f"Alert type '{alert_type}' not in allowed types {allowed_types} for job {job['id']}")
        return

    payload = {
        "event": "cronpilot.job." + alert_type,
        "job_id": job["id"],
        "job_name": job["name"],
        "status": run.get("status", "unknown"),
        "error": run.get("error"),
        "response_code": run.get("response_code"),
        "duration_ms": run.get("duration_ms"),
        "run_id": run.get("id"),
        "attempt": run.get("attempt", 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "alert_type": alert_type,
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                alert_url,
                json=payload,
                headers={"Content-Type": "application/json", "User-Agent": "CronPilot/1.0"},
            )
            logger.info(f"Alert sent to {alert_url} for job {job['name']}: {resp.status_code}")
    except Exception as e:
        logger.error(f"Failed to send alert to {alert_url}: {e}")


async def send_dead_letter_alert(job: dict, run: dict):
    """Send alert when all retries are exhausted."""
    await send_alert(job, run, alert_type="dead_letter")


async def send_recovery_alert(job: dict, run: dict):
    """Send alert when a previously failing job succeeds."""
    await send_alert(job, run, alert_type="recovery")


async def send_slow_alert(job: dict, run: dict, threshold_ms: float = 0):
    """Send alert when a job takes longer than expected."""
    duration = run.get("duration_ms", 0)
    effective_threshold = threshold_ms or (job.get("timeout_seconds", 30) * 1000 * 0.8)
    if duration > effective_threshold:
        await send_alert(job, run, alert_type="slow")
