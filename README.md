# CronPilot

Managed cron-job-as-a-service with monitoring, retries, and alerting.

## Quick Start

```bash
# Docker
docker compose up -d

# Local
pip install -r requirements.txt
python server.py
```

## API (port 8440)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /health | Health check |
| GET | /dashboard | Summary stats + upcoming runs |
| POST | /jobs | Create job |
| GET | /jobs | List all jobs |
| GET | /jobs/{id} | Job details + recent runs |
| PUT | /jobs/{id} | Update job |
| DELETE | /jobs/{id} | Delete job |
| POST | /jobs/{id}/trigger | Manual trigger |
| POST | /jobs/{id}/pause | Pause job |
| POST | /jobs/{id}/resume | Resume job |
| GET | /jobs/{id}/runs | Run history |
| GET | /jobs/{id}/runs/{run_id} | Run details |

## MCP Server

```bash
python mcp_server.py
```

Tools: `create_cron_job`, `list_jobs`, `get_job_status`, `trigger_job`, `pause_job`, `resume_job`, `delete_job`

## Features

- Cron scheduling via APScheduler + croniter
- HTTP and shell command jobs
- Exponential backoff retries (1s, 2s, 4s, 8s...)
- Timeout enforcement
- Webhook alerts on failure/recovery/slow/dead-letter
- Job chaining (on_success_trigger)
- SQLite persistence
- Free tier: 10 jobs, 100 runs/day
