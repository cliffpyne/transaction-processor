import multiprocessing
import os

# Server socket
bind = "0.0.0.0:10000"

# Worker processes
# 6 sync workers — cheap headroom on the 8 GB VPS (~150 MB per
# worker → ~900 MB total). Any single slow request (big NMB CSV,
# a background sweep) can't monopolize enough workers to starve
# the CRDB/NMB upload endpoints. Bumped from 2 → 6 after the
# retry-sweep spam incident starved uploads at 2 workers.
workers = 6
worker_class = 'sync'
threads = 1

# Timeout settings
timeout = 300  # 5 minutes for PDF processing
graceful_timeout = 30
keepalive = 5

# Memory management
max_requests = 100  # Restart worker after 100 requests to prevent memory leaks
max_requests_jitter = 10

# Logging
accesslog = '-'
errorlog = '-'
loglevel = 'info'

# Process naming
proc_name = 'transaction-processor'

# Worker tmp directory (helps with memory)
worker_tmp_dir = '/dev/shm' if os.path.exists('/dev/shm') else None
