import multiprocessing
import os

# Server socket
bind = "0.0.0.0:10000"

# Worker processes
# 2 sync workers so no single slow request (retry sweeps, big NMB
# CSV uploads) can block real customer traffic — the other worker
# stays available. Per-worker RSS is ~150 MB, so 2 workers ~= 300 MB
# on an 8 GB VPS — plenty of headroom.
workers = 2
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
