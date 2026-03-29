FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /data/cronpilot

ENV CRONPILOT_DB=/data/cronpilot/cronpilot.db
ENV PORT=8504
ENV MAX_JOBS_FREE=10
ENV MAX_RUNS_PER_DAY=100
ENV DEFAULT_TIMEOUT=30

EXPOSE 8504

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8504/health || exit 1

CMD ["python", "server.py"]
