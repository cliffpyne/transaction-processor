# transaction-processor — Cold-start brief for Claude Code sessions

## What this is

Python Flask service that handles **plate-key resolution** for ambiguous bank transactions — when a bank ref could map to multiple customers by plate number, this app runs the "choose_plate" review flow.

Per memory (`project_review_threshold`):
> "choose_plate review skipped when > MAX_REVIEW_CANDIDATES candidates (env var, default 2)"

Meaning: when a transaction fires and BRAIN can't unambiguously resolve the customer (2+ matches), this service either surfaces a review UI or auto-skips based on the threshold.

## Where it runs

- **Deploy target**: **Render** → `https://transaction-processor.onrender.com` (verified reachable — service is up)
- **Framework**: Python 3.11 + Flask + gunicorn (`render.yaml` service `transaction-processor`)
- **Config**:
  ```yaml
  buildCommand: pip install -r requirements.txt
  startCommand: gunicorn app:app -c gunicorn_config.py
  envVars:
    - PYTHON_VERSION=3.11.0
    - WEB_CONCURRENCY=1
  ```
- **Git remote**: `git@github.com:cliffpyne/transaction-processor.git` — branch `main`

## How to deploy changes

```
git add <files>
git commit -m "..."
git push origin main   # Render auto-deploys on push
```

**Deploy safety**: same as BRAIN — don't push during payment tick fires (check BRAIN's `auto_upload_locks`). Since this service is called during transaction resolution, a mid-deploy restart could 500 a live BRAIN fire.

## Key files

- `app.py` — Flask app entry
- `app.py.bak-20260611-182907` — a backup file from an earlier date (safe to leave or archive)
- `auth.py` — auth middleware
- `google.json` — Google service account credentials **(committed to repo — sensitive; treat with care, but existing pattern)**
- `iliyopata_writer.py` — likely writes "iliyopata" (Swahili for "found") records
- `migrate_sheets_to_supabase.py` — migration script (Sheets → Supabase, one-shot or historical)
- `gunicorn.conf.py`, `gunicorn_config.py` — worker config
- `mobile/` — mobile-view templates
- `HANDOFF.md` — read this if you need historical context

## Env vars

- `SECRET_KEY` — Flask session (Render auto-generates)
- `MAX_REVIEW_CANDIDATES` — threshold for choose_plate review (default 2)
- Any Supabase/DB URLs if migration is active

## Integration

- **Called by BRAIN** — when the invoice-payment matcher hits multiple plate candidates
- **Reads Google Sheets** for plate → customer mappings
- **Writes to Supabase** (based on migrate_sheets_to_supabase.py naming — verify with operator whether this migration is complete or ongoing)

## Operator preferences

- **Pushback welcome** on cowpath approaches
- **Speak during long tasks** — 1-line check-in every ~5 min

## 2026-07-21 session — no direct changes here

Not touched this session. BRAIN's payment-batches / m6pm-automation received the day's fixes (preflight sanitizer, session-end K, watcher wait-all-channels). If a resolution ambiguity surfaces in BRAIN's IP-algorithm output that would previously have hit this service, it should still route here the same way — no change to the resolver contract.

## Related

- **EleganskyBrain** — main app that calls this service. See its CLAUDE.md.
- **invoice-payment-app** — sacred payment allocation algorithm origin. See its CLAUDE.md.
