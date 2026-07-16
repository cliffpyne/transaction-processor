#!/usr/bin/env python3
"""
retry_ref_not_found.py — periodic sweep that re-checks recent
`ref_not_found` sms_events to see whether the puller has since
captured the transaction. Runs from a systemd timer OUT OF BAND
from the Flask app, so it never queues on the sync gunicorn worker
and cannot block real customer traffic.

Contract (matches the timing-race case the user asked for):

  - Window: events processed between max_age_min and min_age_min
    ago. Defaults: 5–1440 min. The 5-min buffer gives the bank
    puller time to actually land the transaction after the customer
    SMS arrives; the 24-h cap stops us re-checking week-old typos
    forever.

  - Batch cap: MAX_EVENTS per fire (default 50). Prevents any one
    fire from doing hundreds of DB round-trips or holding a Google
    Sheets quota slot too long.

  - Time budget: TIME_BUDGET_SEC per fire (default 45). Whatever
    is left after the budget is picked up by the next timer fire.

  - Idempotency: uses the same _sms_event_insert dedup as the Flask
    endpoint — a re-retry that stays ref_not_found doesn't leave a
    second identical row; a successful rescue writes a fresh
    outcome='rescued' row.

Environment: expects the same .env the Flask app uses
(SUPABASE_URL, SUPABASE_SERVICE_KEY, MIGRATION_TOKEN,
GOOGLE_CREDENTIALS_JSON).

Exit codes:
  0 — completed cleanly (any number of retries, including zero)
  1 — configuration error (missing env)
  2 — Supabase unreachable / read failed
"""

from __future__ import annotations

import os
import sys
import time
import json
import re
from datetime import datetime, timedelta, timezone

import requests

MAX_EVENTS = int(os.environ.get('RETRY_MAX_EVENTS', '50'))
TIME_BUDGET_SEC = int(os.environ.get('RETRY_TIME_BUDGET_SEC', '45'))
MIN_AGE_MIN = int(os.environ.get('RETRY_MIN_AGE_MIN', '5'))
MAX_AGE_MIN = int(os.environ.get('RETRY_MAX_AGE_MIN', '1440'))


def load_env_file(path: str) -> None:
    """Minimal .env loader — reads KEY=VALUE lines, skips comments.
    Handles values with '=' inside them. Does not handle quoted
    multi-line values. Sufficient for our secrets file."""
    if not os.path.isfile(path):
        return
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            k = k.strip()
            v = v.strip()
            # Strip matching outer quotes if present
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
                v = v[1:-1]
            os.environ.setdefault(k, v)


def _post_rescue(base_url: str, token: str, sender: str | None,
                 body: str, received_at: str | None) -> tuple[int, dict]:
    """POST the event back through /api/sms-rescue on the same host.
    This DOES go through gunicorn, but each retry is a small ~50 ms
    call and we do them one at a time with pauses — the sync worker
    handles it fine as long as MAX_EVENTS stays small.

    The alternative — reimplementing the whole rescue flow (Supabase
    query, source_tab checks, atomic PATCH, iliyopata_writer,
    sms_events insert) — duplicates a lot of subtle logic and risks
    drifting from the endpoint. Small POSTs at low rate is safer.
    """
    try:
        r = requests.post(
            f'{base_url.rstrip("/")}/api/sms-rescue',
            headers={'X-Migration-Token': token,
                     'Content-Type': 'application/json'},
            json={'message': body or '', 'sender': sender,
                  'received_at': received_at},
            timeout=20,
        )
        try:
            data = r.json()
        except Exception:
            data = {}
        return r.status_code, data
    except requests.RequestException as e:
        return 0, {'error': f'network: {str(e)[:120]}'}


def main() -> int:
    load_env_file('/home/eleg/transaction-processor/.env')
    load_env_file('.env')

    supa_url = os.environ.get('SUPABASE_URL', '').rstrip('/')
    supa_key = os.environ.get('SUPABASE_SERVICE_KEY', '')
    mig_token = os.environ.get('MIGRATION_TOKEN', '')
    if not (supa_url and supa_key and mig_token):
        print('missing env: need SUPABASE_URL, SUPABASE_SERVICE_KEY, '
              'MIGRATION_TOKEN', file=sys.stderr)
        return 1

    base_url = os.environ.get('LOCAL_BASE_URL', 'http://127.0.0.1:10000')

    # Age-window filter — same defaults as the old Flask endpoint.
    now = datetime.now(timezone.utc)
    upper = (now - timedelta(minutes=MIN_AGE_MIN)).strftime(
        '%Y-%m-%dT%H:%M:%S')
    lower = (now - timedelta(minutes=MAX_AGE_MIN)).strftime(
        '%Y-%m-%dT%H:%M:%S')

    h = {'apikey': supa_key, 'Authorization': f'Bearer {supa_key}'}
    # Oldest-first: any timing-race event that's going to resolve
    # has probably done so by now if it hasn't in weeks. Sort ascending
    # so we clear the backlog head-to-tail.
    try:
        r = requests.get(
            f'{supa_url}/rest/v1/sms_events',
            params={
                'select': 'id,sender,body,received_at,processed_at',
                'outcome': 'eq.ref_not_found',
                'processed_at': f'gte.{lower}',
                'order': 'processed_at.asc',
                'limit': str(MAX_EVENTS),
            },
            headers={**h, 'Range': f'0-{MAX_EVENTS - 1}',
                     'Range-Unit': 'items'},
            timeout=30,
        )
    except requests.RequestException as e:
        print(f'supabase read failed: {e}', file=sys.stderr)
        return 2
    if r.status_code not in (200, 206):
        print(f'supabase HTTP {r.status_code}: {r.text[:200]}',
              file=sys.stderr)
        return 2

    # Enforce the upper bound locally — PostgREST doesn't accept two
    # conditions on the same field via a plain dict params, so we do
    # a lower-bound-only server-side query then trim here.
    events = [
        e for e in r.json()
        if (e.get('processed_at') or '')[:19] <= upper
    ]

    if not events:
        print(f'no events in retry window '
              f'({MIN_AGE_MIN}-{MAX_AGE_MIN} min old)')
        return 0

    start = time.monotonic()
    tally = {'checked': 0, 'rescued': 0, 'ref_in_passed': 0,
             'still_ref_not_found': 0, 'already_rescued': 0,
             'plate_unknown': 0, 'extract_failed': 0,
             'server_error': 0, 'network': 0, 'other': 0,
             'time_capped': False}

    for e in events:
        if time.monotonic() - start > TIME_BUDGET_SEC:
            tally['time_capped'] = True
            break
        tally['checked'] += 1
        status, data = _post_rescue(base_url, mig_token,
                                    e.get('sender'), e.get('body') or '',
                                    e.get('received_at'))
        err = (data.get('error') or '').lower()
        if status == 200 and data.get('rescued'):
            tally['rescued'] += 1
        elif status == 409 and err == 'already_rescued':
            tally['already_rescued'] += 1
        elif status == 409 and err == 'ref_in_passed':
            tally['ref_in_passed'] += 1
        elif status == 404 and err == 'ref_not_found':
            tally['still_ref_not_found'] += 1
        elif status == 404 and err == 'plate_not_in_records':
            tally['plate_unknown'] += 1
        elif status == 400:
            tally['extract_failed'] += 1
        elif status == 500:
            tally['server_error'] += 1
        elif status == 0:
            tally['network'] += 1
        else:
            tally['other'] += 1
        # tiny pause so we never hammer the single sync worker
        time.sleep(0.05)

    print(json.dumps({'window_min': [MIN_AGE_MIN, MAX_AGE_MIN],
                      'runtime_sec': round(time.monotonic() - start, 2),
                      'tally': tally}))
    return 0


if __name__ == '__main__':
    sys.exit(main())
