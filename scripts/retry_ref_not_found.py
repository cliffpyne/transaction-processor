#!/usr/bin/env python3
"""
retry_ref_not_found.py — periodic sweep that re-checks recent
`ref_not_found` sms_events to see whether the puller has since
captured the transaction.

Runs from a systemd timer OUT OF BAND from the Flask app. Talks
DIRECTLY to Supabase and Google Sheets — zero HTTP calls to
gunicorn, zero worker contention. The previous version POSTed
each event to /api/sms-rescue, which despite being "background"
still tied up a gunicorn worker per call and starved the CRDB
upload endpoint (→ 90 s timeouts → OTP burn on the puller). This
version cannot cause that class of interference.

Contract:

  - Window: events processed between max_age_min and min_age_min
    ago (defaults 5 – 1440 min).

  - Batch cap: RETRY_MAX_EVENTS per fire (default 30 now — smaller
    than the old 50 because we no longer share workers, so the
    trade-off flipped: we can go slower and be even lower-impact).

  - Time budget: RETRY_TIME_BUDGET_SEC (default 45 s).

  - Rescue logic mirrors /api/sms-rescue in app.py. The Flask
    endpoint stays the primary entry point for live customer
    SMS — this script only walks the ref_not_found backlog.

Environment expected in /home/eleg/transaction-processor/.env:
  SUPABASE_URL, SUPABASE_SERVICE_KEY,
  GOOGLE_CREDENTIALS_JSON.

Exit codes:
  0 — completed cleanly
  1 — configuration error
  2 — Supabase unreachable
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

# Let this script import iliyopata_writer from the app root.
_APP_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

import iliyopata_writer  # noqa: E402


MAX_EVENTS = int(os.environ.get('RETRY_MAX_EVENTS', '100'))
TIME_BUDGET_SEC = int(os.environ.get('RETRY_TIME_BUDGET_SEC', '90'))
MIN_AGE_MIN = int(os.environ.get('RETRY_MIN_AGE_MIN', '5'))
MAX_AGE_MIN = int(os.environ.get('RETRY_MAX_AGE_MIN', '1440'))
# Outcomes eligible for retry. ref_not_found handles the timing-race
# case (customer texted before puller landed the tx). plate_not_in_records
# handles the case where the plate was missing from Supabase customers
# but was later added by the sync_customers_from_sheet.py timer.
RETRY_OUTCOMES = tuple(
    x.strip() for x in os.environ.get(
        'RETRY_OUTCOMES',
        'ref_not_found,plate_not_in_records'
    ).split(',') if x.strip()
)

# Match app.py's constants (kept in sync manually — same as they always
# were between app.py and iliyopata_writer.py).
_FAILED_SOURCE_TABS = {'CRDBFAILED', 'NMBFAILED', 'IPHONEFAILED'}
_ILIYOPATA_TARGET_FROM_CUSTOMER = {
    'IPHONE_RECORDS': 'IPHONEILIYOPATA',
    'BODA_RECORDS':   'BODAILIYOPATA',
    'SAVCOM_RECORDS': 'BODAILIYOPATA',
}


def _load_env_file(path: str) -> None:
    if not os.path.isfile(path):
        return
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            v = v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
                v = v[1:-1]
            os.environ.setdefault(k.strip(), v)


def _sms_event_insert(url: str, h: dict, *, sender: str | None,
                      body: str, received_at: str | None,
                      http_status: int, outcome: str,
                      plate: str | None, ref: str | None,
                      row_id: int | None, source_tab: str | None,
                      error_detail: str | None) -> None:
    """Insert one sms_events row. Uses the same 60-second dedup guard
    the Flask endpoint uses so a retry that stays ref_not_found doesn't
    stack a second identical row."""
    # 60-sec dedup on (sender, body, outcome)
    since = (datetime.now(timezone.utc)
             - timedelta(seconds=60)).strftime('%Y-%m-%dT%H:%M:%S')
    try:
        r = requests.get(
            f'{url}/rest/v1/sms_events',
            params={
                'select': 'id',
                'sender': f'eq.{sender or ""}',
                'body': f'eq.{body}',
                'outcome': f'eq.{outcome}',
                'processed_at': f'gte.{since}',
                'limit': '1',
            },
            headers=h, timeout=10,
        )
        if r.ok and r.json():
            return
    except requests.RequestException:
        pass  # if the dedup read fails, we still insert; harmless

    try:
        requests.post(
            f'{url}/rest/v1/sms_events',
            headers={**h, 'Content-Type': 'application/json'},
            json={
                'sender': sender, 'body': body, 'received_at': received_at,
                'http_status': http_status, 'outcome': outcome,
                'extracted_plate': plate, 'extracted_ref': ref,
                'rescued_row_id': row_id,
                'rescued_source_tab': source_tab,
                'error_detail': error_detail,
            },
            timeout=10,
        )
    except requests.RequestException:
        pass  # best-effort — we already have the DB write above


def _rescue_one(url: str, h: dict, ev: dict) -> str:
    """Rescue one event directly, mirroring /api/sms-rescue in app.py
    but talking to Supabase + Sheets directly instead of through
    Flask. Returns the outcome name."""
    sender = ev.get('sender')
    body = ev.get('body') or ''
    received_at = ev.get('received_at')
    ref = (ev.get('extracted_ref') or '').strip()
    plate = (ev.get('extracted_plate') or '').strip()

    if not ref:
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=400,
                          outcome='extract_failed',
                          plate=plate, ref=None,
                          row_id=None, source_tab=None,
                          error_detail='no ref on sms_event')
        return 'extract_failed'

    # 1. Fetch transaction by ref (case-insensitive)
    try:
        r = requests.get(
            f'{url}/rest/v1/transactions',
            params={
                'ref_number': f'ilike.{ref}',
                'select':
                    'id,source_tab,transaction_date,customer_name,bank,'
                    'description,credit_amount,identifier,ref_number,'
                    'customer_id,rescue_locked_at',
                'limit': '1',
            },
            headers=h, timeout=15,
        )
    except requests.RequestException as e:
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=500,
                          outcome='server_error',
                          plate=plate, ref=ref,
                          row_id=None, source_tab=None,
                          error_detail=f'tx fetch: {str(e)[:120]}')
        return 'server_error'
    if not r.ok:
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=500,
                          outcome='server_error',
                          plate=plate, ref=ref,
                          row_id=None, source_tab=None,
                          error_detail=f'tx fetch http {r.status_code}')
        return 'server_error'
    tx_rows = r.json()
    if not tx_rows:
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=404,
                          outcome='ref_not_found',
                          plate=plate, ref=ref,
                          row_id=None, source_tab=None,
                          error_detail=None)
        return 'ref_not_found'
    tx = tx_rows[0]

    # 2. Already rescued?
    if (tx.get('rescue_locked_at') or
            tx['source_tab'] in {'BODAILIYOPATA', 'IPHONEILIYOPATA'}):
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=409,
                          outcome='already_rescued',
                          plate=plate, ref=ref,
                          row_id=tx['id'],
                          source_tab=tx['source_tab'],
                          error_detail=None)
        return 'already_rescued'

    # 3. Ref exists but sits in a PASSED tab — no rescue needed.
    if tx['source_tab'] not in _FAILED_SOURCE_TABS:
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=409,
                          outcome='ref_in_passed',
                          plate=plate, ref=ref,
                          row_id=tx['id'],
                          source_tab=tx['source_tab'],
                          error_detail=None)
        return 'ref_in_passed'

    # 4. Customer lookup by plate.
    try:
        r = requests.get(
            f'{url}/rest/v1/customers',
            params={
                'plate': f'eq.{plate}',
                'select': 'id,name,plate,customer_id,source_tab',
                'limit': '1',
            },
            headers=h, timeout=15,
        )
    except requests.RequestException as e:
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=500,
                          outcome='server_error',
                          plate=plate, ref=ref,
                          row_id=None, source_tab=None,
                          error_detail=f'cust fetch: {str(e)[:120]}')
        return 'server_error'
    if not r.ok:
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=500,
                          outcome='server_error',
                          plate=plate, ref=ref,
                          row_id=None, source_tab=None,
                          error_detail=f'cust fetch http {r.status_code}')
        return 'server_error'
    cust_rows = r.json()
    if not cust_rows:
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=404,
                          outcome='plate_not_in_records',
                          plate=plate, ref=ref,
                          row_id=None, source_tab=None,
                          error_detail=None)
        return 'plate_not_in_records'
    cust = cust_rows[0]
    target_tab = _ILIYOPATA_TARGET_FROM_CUSTOMER.get(cust['source_tab'])
    if not target_tab:
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=400,
                          outcome='server_error',
                          plate=plate, ref=ref,
                          row_id=None, source_tab=None,
                          error_detail=f'unknown cust src {cust["source_tab"]}')
        return 'server_error'

    # 5. Atomic PATCH — same shape as app.py.
    now_utc = datetime.utcnow()
    now_eat = now_utc + timedelta(hours=3)
    update = {
        'old_transaction_date': tx.get('transaction_date'),
        'transaction_date':     now_eat.strftime('%d.%m.%Y %H:%M:%S'),
        'transaction_day':      now_eat.strftime('%Y-%m-%d'),
        'customer_name':        cust['name'],
        'source_tab':           target_tab,
        'moved_by_username':    'sms_rescue_retry',
        'moved_at':             now_utc.isoformat() + 'Z',
        'rescue_locked_at':     now_utc.isoformat() + 'Z',
        'identifier':           tx.get('identifier') or cust.get('plate') or '',
    }
    try:
        r = requests.patch(
            f'{url}/rest/v1/transactions'
            f'?id=eq.{tx["id"]}&rescue_locked_at=is.null',
            headers={**h, 'Content-Type': 'application/json',
                     'Prefer': 'return=representation'},
            json=update, timeout=15,
        )
    except requests.RequestException as e:
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=500,
                          outcome='server_error',
                          plate=plate, ref=ref,
                          row_id=tx['id'], source_tab=None,
                          error_detail=f'patch: {str(e)[:120]}')
        return 'server_error'
    if not r.ok:
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=500,
                          outcome='server_error',
                          plate=plate, ref=ref,
                          row_id=tx['id'], source_tab=None,
                          error_detail=f'patch http {r.status_code}')
        return 'server_error'
    if not (r.json() or []):
        # Someone else beat us to the rescue between the read and the
        # PATCH (concurrent SMS or UI-button rescue).
        _sms_event_insert(url, h, sender=sender, body=body,
                          received_at=received_at, http_status=409,
                          outcome='already_rescued',
                          plate=plate, ref=ref,
                          row_id=tx['id'], source_tab=None,
                          error_detail='concurrent lock')
        return 'already_rescued'

    # 6. Sheet writes via iliyopata_writer — same call the Flask
    #    endpoint makes, so ILIYOPATAAUTO + PASSED + FAILED-marker
    #    all happen exactly as they always have.
    try:
        sheet_result = iliyopata_writer.append_iliyopata_row(
            origin_source_tab=tx['source_tab'],
            tx=tx,
            customer=cust,
            new_date_text=update['transaction_date'],
        )
        sheet_err = (None if sheet_result.get('ok')
                     else sheet_result.get('error'))
    except Exception as e:  # noqa: BLE001
        sheet_err = str(e)[:200]

    # 7. Final sms_events row.
    _sms_event_insert(url, h, sender=sender, body=body,
                      received_at=received_at, http_status=200,
                      outcome='rescued',
                      plate=plate, ref=ref,
                      row_id=tx['id'], source_tab=target_tab,
                      error_detail=sheet_err)
    return 'rescued'


def main() -> int:
    _load_env_file('/home/eleg/transaction-processor/.env')
    _load_env_file('.env')

    supa_url = os.environ.get('SUPABASE_URL', '').rstrip('/')
    supa_key = (os.environ.get('SUPABASE_SERVICE_KEY', '')
                or os.environ.get('SUPABASE_SERVICE_ROLE_KEY', ''))
    if not (supa_url and supa_key):
        print('missing env: need SUPABASE_URL and '
              'SUPABASE_SERVICE_KEY',
              file=sys.stderr)
        return 1

    h = {'apikey': supa_key, 'Authorization': f'Bearer {supa_key}'}

    # Age window: process events between MAX_AGE_MIN and MIN_AGE_MIN
    # minutes old, oldest first.
    now = datetime.now(timezone.utc)
    upper = (now - timedelta(minutes=MIN_AGE_MIN)).strftime(
        '%Y-%m-%dT%H:%M:%S')
    lower = (now - timedelta(minutes=MAX_AGE_MIN)).strftime(
        '%Y-%m-%dT%H:%M:%S')

    outcome_filter = f'in.({",".join(RETRY_OUTCOMES)})'
    try:
        r = requests.get(
            f'{supa_url}/rest/v1/sms_events',
            params={
                'select':
                    'id,sender,body,received_at,processed_at,'
                    'extracted_plate,extracted_ref',
                'outcome': outcome_filter,
                'processed_at': f'gte.{lower}',
                # NEWEST FIRST — critical for the timing-race case.
                # With oldest-first + a batch cap, the sweep gets pinned
                # on the oldest never-resolvable events (typos, m-pesa
                # refs, txns the puller never fetched) and never advances
                # to newer events that actually can resolve. Newest-first
                # ensures recent SMSes always get retried within minutes.
                # Old events either resolve or naturally age out of the
                # 24h window.
                'order': 'processed_at.desc',
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

    raw_events = [
        e for e in r.json()
        if (e.get('processed_at') or '')[:19] <= upper
    ]
    # Collapse (sender, body) duplicates — retry loops in the past created
    # many identical log rows for the same message. Keep ONLY the newest
    # row per unique (sender, body) so we spend our per-fire slots on
    # unique customer messages, not churn. The events came in newest-first
    # order, so the first occurrence of each key is the newest.
    seen_keys = set()
    events = []
    for e in raw_events:
        key = (e.get('sender') or '', e.get('body') or '')
        if key in seen_keys:
            continue
        seen_keys.add(key)
        events.append(e)
    dedup_ratio = f'{len(events)}/{len(raw_events)}' if raw_events else '0/0'
    if not events:
        print(f'no events in retry window '
              f'({MIN_AGE_MIN}-{MAX_AGE_MIN} min)')
        return 0

    start = time.monotonic()
    tally: dict[str, Any] = {
        'checked': 0, 'rescued': 0, 'ref_in_passed': 0,
        'still_ref_not_found': 0, 'already_rescued': 0,
        'plate_unknown': 0, 'extract_failed': 0,
        'server_error': 0, 'other': 0, 'time_capped': False,
    }
    for ev in events:
        if time.monotonic() - start > TIME_BUDGET_SEC:
            tally['time_capped'] = True
            break
        tally['checked'] += 1
        outcome = _rescue_one(supa_url, h, ev)
        if outcome == 'rescued':
            tally['rescued'] += 1
        elif outcome == 'ref_in_passed':
            tally['ref_in_passed'] += 1
        elif outcome == 'ref_not_found':
            tally['still_ref_not_found'] += 1
        elif outcome == 'already_rescued':
            tally['already_rescued'] += 1
        elif outcome == 'plate_not_in_records':
            tally['plate_unknown'] += 1
        elif outcome == 'extract_failed':
            tally['extract_failed'] += 1
        elif outcome == 'server_error':
            tally['server_error'] += 1
        else:
            tally['other'] += 1
        # Small pause between events so we never hammer Supabase
        # even if it's under load elsewhere.
        time.sleep(0.05)

    print(json.dumps({
        'window_min': [MIN_AGE_MIN, MAX_AGE_MIN],
        'dedup': dedup_ratio,
        'runtime_sec': round(time.monotonic() - start, 2),
        'tally': tally,
    }))
    return 0


if __name__ == '__main__':
    sys.exit(main())
