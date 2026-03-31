# gunicorn.conf.py
# ──────────────────────────────────────────────────────────────────────────────
# Render free tier has ~512MB RAM.
# This config keeps memory low and prevents worker timeout kills that cause
# the "Unexpected end of JSON input" error on the frontend.
# ──────────────────────────────────────────────────────────────────────────────

# 1 worker on free tier — 2 workers would double memory usage and cause OOM kills
workers = 1

# sync worker is the most memory-efficient for CPU-bound pandas/Excel processing
worker_class = 'sync'

# 5 minutes — NMB 10k-row processing takes ~2-3 min including Sheets API calls.
# The default gunicorn timeout is 30s, which is what causes mid-response kills.
timeout = 300

# How long a worker waits for the next request before it's recycled
keepalive = 5

# Restart the worker process every N requests to prevent memory accumulation
# across repeated uses throughout the day. With 20+ uses/day this matters.
max_requests = 30
max_requests_jitter = 5  # randomise slightly to avoid all workers restarting at once

# Do NOT share app object across workers — each gets its own clean memory space
preload_app = False

# Bind to the port Render provides via the PORT env variable, fallback to 5000
import os
bind = f"0.0.0.0:{os.environ.get('PORT', '5000')}"

# Log to stdout so Render dashboard shows them
accesslog = '-'
errorlog = '-'
loglevel = 'info'
