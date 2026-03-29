"""CronPilot MCP Server — exposes cron job management as MCP tools."""

import asyncio
import json
import os
import sys

from mcp.server import Server
from mcp.server.stdio import run_server
from mcp.types import Tool, TextContent

# Ensure local imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import db
from scheduler import start_scheduler, schedule_job, unschedule_job, get_next_run_times
from executor import execute_job
from cron_parser import validate_cron, next_run_time, describe_cron

server = Server("cronpilot")


@server.list_tools()
async def list_tools():
    return [
        Tool(
            name="create_cron_job",
            description="Schedule a URL to be called on a cron schedule. Supports HTTP methods, headers, body, retries, and timeouts.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Human-readable job name"},
                    "url": {"type": "string", "description": "URL to call (for HTTP jobs)"},
                    "command": {"type": "string", "description": "Shell command to run (alternative to url)"},
                    "schedule": {"type": "string", "description": "Cron expression, e.g. '*/5 * * * *'"},
                    "method": {"type": "string", "default": "GET", "description": "HTTP method"},
                    "headers": {"type": "object", "description": "HTTP headers as key-value pairs"},
                    "body": {"type": "string", "description": "Request body for POST/PUT"},
                    "retries": {"type": "integer", "default": 3, "description": "Max retry attempts (0-10)"},
                    "timeout_seconds": {"type": "integer", "default": 30, "description": "Timeout in seconds (1-300)"},
                    "alert_url": {"type": "string", "description": "Webhook URL for failure alerts"},
                    "alert_on": {"type": "string", "default": "failure", "description": "Alert types: failure,recovery,slow,all"},
                    "on_success_trigger": {"type": "string", "description": "Job ID to chain-trigger on success"},
                },
                "required": ["name", "schedule"],
                "oneOf": [
                    {"required": ["url"]},
                    {"required": ["command"]},
                ],
            },
        ),
        Tool(
            name="list_jobs",
            description="List all scheduled cron jobs with their next run times and status.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="get_job_status",
            description="Get detailed status for a job including recent runs and success rate.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string", "description": "Job ID"},
                },
                "required": ["job_id"],
            },
        ),
        Tool(
            name="trigger_job",
            description="Manually trigger a job to run immediately.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string", "description": "Job ID to trigger"},
                },
                "required": ["job_id"],
            },
        ),
        Tool(
            name="pause_job",
            description="Pause a scheduled job so it won't run until resumed.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string", "description": "Job ID to pause"},
                },
                "required": ["job_id"],
            },
        ),
        Tool(
            name="resume_job",
            description="Resume a paused job.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string", "description": "Job ID to resume"},
                },
                "required": ["job_id"],
            },
        ),
        Tool(
            name="delete_job",
            description="Permanently delete a scheduled job and its run history.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string", "description": "Job ID to delete"},
                },
                "required": ["job_id"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict):
    try:
        if name == "create_cron_job":
            return await _create_cron_job(arguments)
        elif name == "list_jobs":
            return await _list_jobs()
        elif name == "get_job_status":
            return await _get_job_status(arguments["job_id"])
        elif name == "trigger_job":
            return await _trigger_job(arguments["job_id"])
        elif name == "pause_job":
            return await _pause_job(arguments["job_id"])
        elif name == "resume_job":
            return await _resume_job(arguments["job_id"])
        elif name == "delete_job":
            return await _delete_job(arguments["job_id"])
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def _create_cron_job(args: dict):
    if not args.get("url") and not args.get("command"):
        return [TextContent(type="text", text="Error: Either 'url' or 'command' is required")]

    if not validate_cron(args["schedule"]):
        return [TextContent(type="text", text=f"Error: Invalid cron expression: {args['schedule']}")]

    existing = db.list_jobs()
    max_jobs = int(os.environ.get("MAX_JOBS_FREE", "10"))
    if len(existing) >= max_jobs:
        return [TextContent(type="text", text=f"Error: Job limit reached ({max_jobs}). Delete unused jobs first.")]

    job = db.create_job(args)
    schedule_job(job)

    nrt = next_run_time(job["schedule"])
    desc = describe_cron(job["schedule"])

    text = (
        f"Created job '{job['name']}' (ID: {job['id']})\n"
        f"Schedule: {job['schedule']} ({desc})\n"
        f"Target: {job.get('url') or job.get('command')}\n"
        f"Retries: {job['retries']}, Timeout: {job['timeout_seconds']}s\n"
        f"Next run: {nrt.isoformat()}"
    )
    return [TextContent(type="text", text=text)]


async def _list_jobs():
    jobs = db.list_jobs()
    if not jobs:
        return [TextContent(type="text", text="No jobs configured.")]

    next_runs = get_next_run_times()
    lines = [f"{'Name':<30} {'Schedule':<20} {'Status':<10} {'Next Run'}"]
    lines.append("-" * 90)

    for job in jobs:
        status = "PAUSED" if job["paused"] else "ACTIVE"
        nr = next_runs.get(job["id"], "—")
        if nr is None and not job["paused"]:
            try:
                nr = next_run_time(job["schedule"]).isoformat()
            except Exception:
                nr = "—"
        lines.append(f"{job['name']:<30} {job['schedule']:<20} {status:<10} {nr}")
        lines.append(f"  ID: {job['id']}  Target: {job.get('url') or job.get('command', '—')}")

    return [TextContent(type="text", text="\n".join(lines))]


async def _get_job_status(job_id: str):
    job = db.get_job(job_id)
    if job is None:
        return [TextContent(type="text", text=f"Job not found: {job_id}")]

    runs = db.list_runs(job_id, limit=10)
    completed = [r for r in runs if r["status"] in ("success", "failed", "dead", "timeout")]
    success_rate = None
    if completed:
        successes = sum(1 for r in completed if r["status"] == "success")
        success_rate = round(successes / len(completed) * 100, 1)

    lines = [
        f"Job: {job['name']} (ID: {job['id']})",
        f"Status: {'PAUSED' if job['paused'] else 'ACTIVE'}",
        f"Schedule: {job['schedule']} ({describe_cron(job['schedule'])})",
        f"Target: {job.get('url') or job.get('command', '—')}",
        f"Retries: {job['retries']}, Timeout: {job['timeout_seconds']}s",
        f"Success Rate: {success_rate}%" if success_rate is not None else "Success Rate: No runs yet",
        "",
        "Recent Runs:",
    ]

    if not runs:
        lines.append("  No runs yet")
    else:
        for run in runs[:10]:
            dur = f"{run['duration_ms']:.0f}ms" if run.get("duration_ms") else "—"
            err = f" | {run['error'][:80]}" if run.get("error") else ""
            lines.append(
                f"  [{run['status'].upper():>7}] {run['started_at']} | {dur} | attempt {run['attempt']}{err}"
            )

    return [TextContent(type="text", text="\n".join(lines))]


async def _trigger_job(job_id: str):
    job = db.get_job(job_id)
    if job is None:
        return [TextContent(type="text", text=f"Job not found: {job_id}")]

    run = await execute_job(job, triggered_by="mcp_manual")
    status = run.get("status", "unknown") if run else "error"
    dur = f"{run['duration_ms']:.0f}ms" if run and run.get("duration_ms") else "—"

    text = f"Triggered '{job['name']}': {status.upper()} ({dur})"
    if run and run.get("error"):
        text += f"\nError: {run['error'][:200]}"

    return [TextContent(type="text", text=text)]


async def _pause_job(job_id: str):
    job = db.set_job_paused(job_id, True)
    if job is None:
        return [TextContent(type="text", text=f"Job not found: {job_id}")]
    unschedule_job(job_id)
    return [TextContent(type="text", text=f"Paused job '{job['name']}' ({job_id})")]


async def _resume_job(job_id: str):
    job = db.set_job_paused(job_id, False)
    if job is None:
        return [TextContent(type="text", text=f"Job not found: {job_id}")]
    schedule_job(job)
    nrt = next_run_time(job["schedule"])
    return [TextContent(type="text", text=f"Resumed job '{job['name']}' ({job_id}). Next run: {nrt.isoformat()}")]


async def _delete_job(job_id: str):
    job = db.get_job(job_id)
    name = job["name"] if job else job_id
    unschedule_job(job_id)
    deleted = db.delete_job(job_id)
    if not deleted:
        return [TextContent(type="text", text=f"Job not found: {job_id}")]
    return [TextContent(type="text", text=f"Deleted job '{name}' ({job_id}) and all its run history.")]


async def main():
    db.init_db()
    start_scheduler()
    await run_server(server)


if __name__ == "__main__":
    asyncio.run(main())
