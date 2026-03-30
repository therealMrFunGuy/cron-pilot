"""CronPilot — Managed cron-job-as-a-service with monitoring, retries, and alerting."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
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


# --- Landing Page ---

LANDING_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CronPilot - Managed Cron Jobs with Monitoring &amp; Alerting</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
  html { scroll-behavior: smooth; }
  .code-block { background: #0f172a; }
  .gradient-hero { background: linear-gradient(135deg, #0f172a 0%, #164e63 50%, #0f172a 100%); }
  .card-hover:hover { transform: translateY(-4px); box-shadow: 0 20px 40px rgba(0,0,0,0.3); }
  .card-hover { transition: transform 0.2s ease, box-shadow 0.2s ease; }
</style>
</head>
<body class="bg-slate-950 text-slate-100 antialiased">

<!-- Nav -->
<nav class="fixed top-0 w-full z-50 bg-slate-950/80 backdrop-blur-lg border-b border-slate-800">
  <div class="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
    <div class="flex items-center gap-2">
      <svg class="w-7 h-7 text-cyan-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>
      <span class="text-xl font-bold text-white tracking-tight">CronPilot</span>
    </div>
    <div class="hidden md:flex items-center gap-8 text-sm text-slate-400">
      <a href="#features" class="hover:text-cyan-400 transition">Features</a>
      <a href="#pricing" class="hover:text-cyan-400 transition">Pricing</a>
      <a href="#api" class="hover:text-cyan-400 transition">Docs</a>
      <a href="https://github.com/therealMrFunGuy/cron-pilot" target="_blank" class="hover:text-cyan-400 transition">GitHub</a>
    </div>
  </div>
</nav>

<!-- Hero -->
<section class="gradient-hero pt-32 pb-20 px-6">
  <div class="max-w-4xl mx-auto text-center">
    <div class="inline-block px-4 py-1.5 mb-6 rounded-full border border-cyan-800 bg-cyan-950/50 text-cyan-400 text-sm font-medium">
      Open-source cron-as-a-service
    </div>
    <h1 class="text-4xl sm:text-5xl lg:text-6xl font-extrabold tracking-tight leading-tight">
      Managed Cron Jobs with<br>
      <span class="text-transparent bg-clip-text bg-gradient-to-r from-cyan-400 to-teal-400">Monitoring &amp; Alerting</span>
    </h1>
    <p class="mt-6 text-lg sm:text-xl text-slate-400 max-w-2xl mx-auto leading-relaxed">
      Never miss a cron job again. Schedule HTTP calls and shell commands with automatic retries,
      webhook alerts on failure, and full run history &mdash; all through a simple REST API.
    </p>
    <div class="mt-10 flex flex-col sm:flex-row gap-4 justify-center">
      <a href="#api" class="px-8 py-3 rounded-lg bg-cyan-500 hover:bg-cyan-400 text-slate-950 font-semibold transition">
        View API Docs
      </a>
      <a href="https://github.com/therealMrFunGuy/cron-pilot" target="_blank"
         class="px-8 py-3 rounded-lg border border-slate-700 hover:border-cyan-600 text-slate-300 hover:text-white font-semibold transition">
        Star on GitHub
      </a>
    </div>
  </div>
</section>

<!-- Code Examples -->
<section class="py-20 px-6 bg-slate-900/50">
  <div class="max-w-5xl mx-auto">
    <h2 class="text-3xl font-bold text-center mb-4">Get started in seconds</h2>
    <p class="text-slate-400 text-center mb-12 max-w-xl mx-auto">A simple REST API you can call from anywhere &mdash; curl, Python, your CI pipeline, or an MCP client.</p>

    <div class="grid lg:grid-cols-2 gap-6">
      <!-- Create a job -->
      <div class="code-block rounded-xl p-6 border border-slate-800">
        <div class="text-xs text-cyan-400 font-semibold uppercase tracking-wider mb-3">Create a scheduled job</div>
        <pre class="text-sm text-slate-300 overflow-x-auto whitespace-pre"><code>curl -X POST http://localhost:8440/jobs \\
  -H "Content-Type: application/json" \\
  -d '{
    "name": "health-check",
    "url": "https://myapp.com/health",
    "schedule": "*/5 * * * *",
    "retries": 3,
    "alert_url": "https://hooks.slack.com/..."
  }'</code></pre>
      </div>

      <!-- Trigger manually -->
      <div class="code-block rounded-xl p-6 border border-slate-800">
        <div class="text-xs text-cyan-400 font-semibold uppercase tracking-wider mb-3">Trigger a job manually</div>
        <pre class="text-sm text-slate-300 overflow-x-auto whitespace-pre"><code>curl -X POST http://localhost:8440/jobs/abc123/trigger

# Response:
{
  "triggered": true,
  "run": {
    "id": "run_01",
    "status": "success",
    "duration_ms": 247
  }
}</code></pre>
      </div>

      <!-- Run history -->
      <div class="code-block rounded-xl p-6 border border-slate-800">
        <div class="text-xs text-cyan-400 font-semibold uppercase tracking-wider mb-3">Check run history</div>
        <pre class="text-sm text-slate-300 overflow-x-auto whitespace-pre"><code>curl http://localhost:8440/jobs/abc123/runs?limit=5

# Response:
{
  "runs": [
    {"status": "success", "duration_ms": 210},
    {"status": "success", "duration_ms": 198},
    {"status": "failed",  "attempt": 3}
  ],
  "count": 3
}</code></pre>
      </div>

      <!-- MCP Config -->
      <div class="code-block rounded-xl p-6 border border-slate-800">
        <div class="text-xs text-cyan-400 font-semibold uppercase tracking-wider mb-3">MCP client config</div>
        <pre class="text-sm text-slate-300 overflow-x-auto whitespace-pre"><code>{
  "mcpServers": {
    "cronpilot": {
      "command": "uvx",
      "args": [
        "mcp-server-cronpilot"
      ],
      "env": {
        "CRONPILOT_PORT": "8440"
      }
    }
  }
}</code></pre>
      </div>
    </div>
  </div>
</section>

<!-- Features -->
<section id="features" class="py-20 px-6">
  <div class="max-w-5xl mx-auto">
    <h2 class="text-3xl font-bold text-center mb-4">Everything you need</h2>
    <p class="text-slate-400 text-center mb-12 max-w-xl mx-auto">Production-ready cron scheduling with the reliability features your jobs deserve.</p>

    <div class="grid sm:grid-cols-2 gap-6">
      <div class="card-hover bg-slate-900 border border-slate-800 rounded-xl p-8">
        <div class="w-12 h-12 rounded-lg bg-cyan-950 flex items-center justify-center mb-4">
          <svg class="w-6 h-6 text-cyan-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>
        </div>
        <h3 class="text-lg font-semibold mb-2">Cron Scheduling</h3>
        <p class="text-slate-400 text-sm leading-relaxed">Full cron expression support with human-readable descriptions. Schedule HTTP requests or shell commands on any interval.</p>
      </div>

      <div class="card-hover bg-slate-900 border border-slate-800 rounded-xl p-8">
        <div class="w-12 h-12 rounded-lg bg-teal-950 flex items-center justify-center mb-4">
          <svg class="w-6 h-6 text-teal-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>
        </div>
        <h3 class="text-lg font-semibold mb-2">Auto-Retries</h3>
        <p class="text-slate-400 text-sm leading-relaxed">Configurable retry policies with exponential backoff. Failed jobs retry up to 10 times before hitting the dead letter queue.</p>
      </div>

      <div class="card-hover bg-slate-900 border border-slate-800 rounded-xl p-8">
        <div class="w-12 h-12 rounded-lg bg-emerald-950 flex items-center justify-center mb-4">
          <svg class="w-6 h-6 text-emerald-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9"/></svg>
        </div>
        <h3 class="text-lg font-semibold mb-2">Webhook Alerts</h3>
        <p class="text-slate-400 text-sm leading-relaxed">Get notified on failure, recovery, slow runs, or dead letters. Send alerts to Slack, Discord, or any webhook endpoint.</p>
      </div>

      <div class="card-hover bg-slate-900 border border-slate-800 rounded-xl p-8">
        <div class="w-12 h-12 rounded-lg bg-sky-950 flex items-center justify-center mb-4">
          <svg class="w-6 h-6 text-sky-400" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01M9 16h.01"/></svg>
        </div>
        <h3 class="text-lg font-semibold mb-2">Run History</h3>
        <p class="text-slate-400 text-sm leading-relaxed">Full execution logs with status, duration, response codes, and output. Debug failures and track reliability over time.</p>
      </div>
    </div>
  </div>
</section>

<!-- Pricing -->
<section id="pricing" class="py-20 px-6 bg-slate-900/50">
  <div class="max-w-5xl mx-auto">
    <h2 class="text-3xl font-bold text-center mb-4">Simple pricing</h2>
    <p class="text-slate-400 text-center mb-12 max-w-xl mx-auto">Start free, scale when you need to.</p>

    <div class="grid md:grid-cols-3 gap-6">
      <!-- Free -->
      <div class="card-hover bg-slate-900 border border-slate-800 rounded-xl p-8">
        <h3 class="text-lg font-semibold mb-1">Free</h3>
        <div class="text-3xl font-extrabold mb-1">$0</div>
        <p class="text-slate-500 text-sm mb-6">Forever</p>
        <ul class="space-y-3 text-sm text-slate-400 mb-8">
          <li class="flex items-center gap-2"><svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg>5 scheduled jobs</li>
          <li class="flex items-center gap-2"><svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg>3 retries per run</li>
          <li class="flex items-center gap-2"><svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg>Webhook alerts</li>
          <li class="flex items-center gap-2"><svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg>Community support</li>
        </ul>
        <a href="#api" class="block text-center py-2.5 rounded-lg border border-slate-700 text-slate-300 hover:border-cyan-600 hover:text-white text-sm font-medium transition">Get Started</a>
      </div>

      <!-- Pro -->
      <div class="card-hover bg-slate-900 border-2 border-cyan-500 rounded-xl p-8 relative">
        <div class="absolute -top-3 left-1/2 -translate-x-1/2 px-3 py-0.5 bg-cyan-500 text-slate-950 text-xs font-bold rounded-full uppercase tracking-wider">Popular</div>
        <h3 class="text-lg font-semibold mb-1">Pro</h3>
        <div class="text-3xl font-extrabold mb-1">$16<span class="text-base font-normal text-slate-400">/mo</span></div>
        <p class="text-slate-500 text-sm mb-6">Per instance</p>
        <ul class="space-y-3 text-sm text-slate-400 mb-8">
          <li class="flex items-center gap-2"><svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg>100 scheduled jobs</li>
          <li class="flex items-center gap-2"><svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg>Custom retry policies</li>
          <li class="flex items-center gap-2"><svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg>Slack &amp; Discord alerts</li>
          <li class="flex items-center gap-2"><svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg>Priority support</li>
        </ul>
        <a href="#api" class="block text-center py-2.5 rounded-lg bg-cyan-500 hover:bg-cyan-400 text-slate-950 text-sm font-semibold transition">Upgrade to Pro</a>
      </div>

      <!-- Enterprise -->
      <div class="card-hover bg-slate-900 border border-slate-800 rounded-xl p-8">
        <h3 class="text-lg font-semibold mb-1">Enterprise</h3>
        <div class="text-3xl font-extrabold mb-1">Custom</div>
        <p class="text-slate-500 text-sm mb-6">Contact us</p>
        <ul class="space-y-3 text-sm text-slate-400 mb-8">
          <li class="flex items-center gap-2"><svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg>Unlimited jobs</li>
          <li class="flex items-center gap-2"><svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg>SLA guarantee</li>
          <li class="flex items-center gap-2"><svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg>Custom integrations</li>
          <li class="flex items-center gap-2"><svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg>Dedicated support</li>
        </ul>
        <a href="mailto:hello@rjctdlabs.xyz" class="block text-center py-2.5 rounded-lg border border-slate-700 text-slate-300 hover:border-cyan-600 hover:text-white text-sm font-medium transition">Contact Sales</a>
      </div>
    </div>
  </div>
</section>

<!-- API Reference -->
<section id="api" class="py-20 px-6">
  <div class="max-w-5xl mx-auto">
    <h2 class="text-3xl font-bold text-center mb-4">API Reference</h2>
    <p class="text-slate-400 text-center mb-12 max-w-xl mx-auto">Four endpoints to manage your cron jobs programmatically.</p>

    <div class="space-y-4">
      <div class="code-block rounded-xl p-6 border border-slate-800">
        <div class="flex flex-wrap items-center gap-3 mb-3">
          <span class="px-2.5 py-1 rounded text-xs font-bold bg-green-900/50 text-green-400 border border-green-800">POST</span>
          <code class="text-sm text-white font-mono">/jobs</code>
          <span class="text-sm text-slate-500">Create a new scheduled job</span>
        </div>
        <p class="text-sm text-slate-400 leading-relaxed">Accepts <code class="text-cyan-400">name</code>, <code class="text-cyan-400">url</code> or <code class="text-cyan-400">command</code>, <code class="text-cyan-400">schedule</code> (cron expression), <code class="text-cyan-400">retries</code>, <code class="text-cyan-400">timeout_seconds</code>, and <code class="text-cyan-400">alert_url</code>. Returns the created job with its ID and next scheduled run time.</p>
      </div>

      <div class="code-block rounded-xl p-6 border border-slate-800">
        <div class="flex flex-wrap items-center gap-3 mb-3">
          <span class="px-2.5 py-1 rounded text-xs font-bold bg-blue-900/50 text-blue-400 border border-blue-800">GET</span>
          <code class="text-sm text-white font-mono">/jobs/{job_id}</code>
          <span class="text-sm text-slate-500">Get job details and recent runs</span>
        </div>
        <p class="text-sm text-slate-400 leading-relaxed">Returns the full job configuration, next run time, schedule description, the 10 most recent runs, and computed success rate.</p>
      </div>

      <div class="code-block rounded-xl p-6 border border-slate-800">
        <div class="flex flex-wrap items-center gap-3 mb-3">
          <span class="px-2.5 py-1 rounded text-xs font-bold bg-green-900/50 text-green-400 border border-green-800">POST</span>
          <code class="text-sm text-white font-mono">/jobs/{job_id}/trigger</code>
          <span class="text-sm text-slate-500">Trigger a job immediately</span>
        </div>
        <p class="text-sm text-slate-400 leading-relaxed">Executes the job on demand regardless of its schedule. Useful for testing or manual intervention. Returns the run result.</p>
      </div>

      <div class="code-block rounded-xl p-6 border border-slate-800">
        <div class="flex flex-wrap items-center gap-3 mb-3">
          <span class="px-2.5 py-1 rounded text-xs font-bold bg-blue-900/50 text-blue-400 border border-blue-800">GET</span>
          <code class="text-sm text-white font-mono">/jobs/{job_id}/runs</code>
          <span class="text-sm text-slate-500">List execution history</span>
        </div>
        <p class="text-sm text-slate-400 leading-relaxed">Returns paginated run history with status, duration, attempt count, and output. Supports <code class="text-cyan-400">?limit=</code> query parameter (1-500, default 50).</p>
      </div>
    </div>

    <p class="text-center mt-8 text-sm text-slate-500">
      Full interactive docs available at <a href="/docs" class="text-cyan-400 hover:text-cyan-300 underline underline-offset-2">/docs</a> (Swagger UI) and <a href="/redoc" class="text-cyan-400 hover:text-cyan-300 underline underline-offset-2">/redoc</a> (ReDoc).
    </p>
  </div>
</section>

<!-- Footer -->
<footer class="border-t border-slate-800 py-12 px-6">
  <div class="max-w-5xl mx-auto flex flex-col md:flex-row items-center justify-between gap-6">
    <div class="flex items-center gap-2 text-slate-500 text-sm">
      <svg class="w-5 h-5 text-cyan-500" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>
      <span>CronPilot</span>
      <span class="text-slate-700 mx-1">&middot;</span>
      <span>Powered by <a href="https://rjctdlabs.xyz" target="_blank" class="text-cyan-500 hover:text-cyan-400 transition">rjctdlabs.xyz</a></span>
    </div>
    <div class="flex items-center gap-6 text-sm text-slate-500">
      <a href="https://github.com/therealMrFunGuy/cron-pilot" target="_blank" class="hover:text-cyan-400 transition">GitHub</a>
      <a href="https://pypi.org/project/mcp-server-cronpilot/" target="_blank" class="hover:text-cyan-400 transition">PyPI</a>
      <a href="/docs" class="hover:text-cyan-400 transition">API Docs</a>
    </div>
  </div>
</footer>

</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def landing_page():
    return HTMLResponse(content=LANDING_HTML)


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
