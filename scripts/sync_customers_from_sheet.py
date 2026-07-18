#!/usr/bin/env python3
"""
sync_customers_from_sheet.py — upsert customers from the pikipiki
records Google Sheet into the Supabase `customers` table.

The rescue path (both live SMS at /api/sms-rescue and the periodic
retry_ref_not_found.py) looks plates up ONLY in Supabase customers.
If a customer was recently added to pikipiki records but not yet
sync'd, their SMS returns 'plate_not_in_records' and the rescue
sits pending until the sync catches up. This script runs from a
systemd timer every 30 min to close that gap automatically.

Source sheet:
  Sheet ID: 1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA
  Tabs read:
    - 'pikipiki records'   → source_tab = BODA_RECORDS
       col 1 = plate, col 2 = name/blob, col 3 = phone
    - 'pikipiki records2'  → source_tab = SAVCOM_RECORDS
       col 1 = plate, col 2 = name, col 3 = phone, col 4 = customer_id
    - 'iphone_records'     → source_tab = IPHONE_RECORDS
       col 0 = name, col 1 = whatsapp phone, col 2 = money phone

Target: public.customers (Supabase). Upsert on (source_tab, plate)
for BODA/SAV, on (source_tab, phone) for IPHONE (iPhone customers
have no plate).

Runs OUT OF BAND from Flask — talks to Supabase + Sheets directly.
Zero HTTP calls to gunicorn.

Env expected in /home/eleg/transaction-processor/.env:
  SUPABASE_URL, SUPABASE_SERVICE_KEY, GOOGLE_CREDENTIALS_JSON.

Exit codes:
  0 — completed cleanly (any deltas)
  1 — configuration error
  2 — Sheets or Supabase unreachable
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from typing import Iterable

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build


SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'

# Plate shape: MC + 3 digits + 3 letters. Boda plates. Reject anything
# else so a header/blob cell that happens to sit in the plate column
# doesn't create garbage customer rows.
_PLATE_RX = re.compile(r'^MC\d{3}[A-Z]{3}$')


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


def _read_tab(svc, tab: str) -> list[list]:
    r = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{tab}'!A:Z",
        valueRenderOption='UNFORMATTED_VALUE',
    ).execute()
    return r.get('values', [])


def _normalize_plate(v: str) -> str | None:
    """Return the plate in canonical `MC###XXX` form, or None if the
    cell isn't a plate. Strips spaces (`MC 133 EJP`), uppercases."""
    if v is None:
        return None
    p = re.sub(r'\s+', '', str(v)).upper()
    if _PLATE_RX.match(p):
        return p
    return None


def _normalize_phone(v: str) -> str | None:
    """Return a phone as digits only, at least 9 digits. Handles
    numbers-formatted-as-integers, leading '+', spaces, and trailing
    commas from the iphone_records tab (`0615233106,` → `0615233106`)."""
    if v is None:
        return None
    s = re.sub(r'\D', '', str(v))
    if len(s) < 9:
        return None
    return s


def _rows_from_pikipiki(rows: list[list], source_tab: str
                        ) -> Iterable[dict]:
    """pikipiki records / pikipiki records2 layout:
       col 0 = serial id, col 1 = plate, col 2 = name(+phone blob),
       col 3 = phone, col 4 = SAV customer_id (records2 only).
    """
    for row in rows:
        if len(row) < 3:
            continue
        plate = _normalize_plate(row[1] if len(row) > 1 else '')
        if not plate:
            continue
        # Name: col 2 sometimes has "NAME 0674299966" — strip trailing
        # digit run since the phone lives in col 3 anyway.
        name = re.sub(r'\s*\d{9,}\s*$', '',
                      str(row[2] if len(row) > 2 else '')).strip()
        phone = _normalize_phone(row[3] if len(row) > 3 else '')
        cust_id = (str(row[4]).strip()
                   if source_tab == 'SAVCOM_RECORDS'
                       and len(row) > 4 and row[4]
                   else None)
        yield {
            'plate':       plate,
            'name':        name or '',
            'phone':       phone,
            'customer_id': cust_id,
            'source_tab':  source_tab,
        }


def _rows_from_iphone(rows: list[list]) -> Iterable[dict]:
    """iphone_records layout:
       col 0 = name, col 1 = whatsapp phone, col 2 = money phone.
       No plate. Key = (source_tab, phone). We take the whatsapp phone
       as primary; if only the money phone is present, use that.
    """
    for row in rows:
        if not row:
            continue
        name = str(row[0] if len(row) > 0 else '').strip()
        phone = _normalize_phone(row[1] if len(row) > 1 else '')
        if not phone:
            phone = _normalize_phone(row[2] if len(row) > 2 else '')
        if not (name and phone):
            continue
        yield {
            'plate':       None,
            'name':        name,
            'phone':       phone,
            'customer_id': None,
            'source_tab':  'IPHONE_RECORDS',
        }


def _existing_keys(url: str, h: dict, source_tab: str,
                   key_col: str) -> set[str]:
    """Return the set of existing key values (plate or phone) already
    in Supabase customers for this source_tab. We compute the delta
    against this locally, then only INSERT the new rows. Avoids the
    need for a real (source_tab, plate) UNIQUE index on the DB side
    while still being idempotent."""
    keys: set[str] = set()
    for offset in range(0, 200_000, 1000):
        try:
            r = requests.get(
                f'{url}/rest/v1/customers',
                params={
                    'select': key_col,
                    'source_tab': f'eq.{source_tab}',
                },
                headers={**h, 'Range-Unit': 'items',
                         'Range': f'{offset}-{offset + 999}'},
                timeout=30,
            )
        except requests.RequestException as e:
            print(f'read existing failed: {e}', file=sys.stderr)
            return keys
        if r.status_code not in (200, 206):
            return keys
        chunk = r.json()
        for row in chunk:
            v = str(row.get(key_col) or '').strip().upper()
            if v:
                keys.add(v)
        if len(chunk) < 1000:
            break
    return keys


def _insert_batch(url: str, h: dict, records: list[dict]) -> int:
    """Plain INSERT via PostgREST — no upsert semantics needed because
    we already filtered against existing keys. Returns rows written."""
    if not records:
        return 0
    try:
        r = requests.post(
            f'{url}/rest/v1/customers',
            headers={**h, 'Content-Type': 'application/json',
                     'Prefer': 'return=minimal'},
            json=records, timeout=45,
        )
    except requests.RequestException as e:
        print(f'insert failed: {e}', file=sys.stderr)
        return 0
    if r.status_code not in (200, 201, 204):
        print(f'insert HTTP {r.status_code}: {r.text[:300]}',
              file=sys.stderr)
        return 0
    return len(records)


def main() -> int:
    _load_env_file('/home/eleg/transaction-processor/.env')
    _load_env_file('.env')

    supa_url = os.environ.get('SUPABASE_URL', '').rstrip('/')
    supa_key = (os.environ.get('SUPABASE_SERVICE_KEY', '')
                or os.environ.get('SUPABASE_SERVICE_ROLE_KEY', ''))
    creds_raw = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')
    if not (supa_url and supa_key and creds_raw):
        print('missing env: SUPABASE_URL / SUPABASE_SERVICE_KEY / '
              'GOOGLE_CREDENTIALS_JSON', file=sys.stderr)
        return 1

    try:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(creds_raw),
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'],
        )
        svc = build('sheets', 'v4', credentials=creds,
                    cache_discovery=False)
    except Exception as e:
        print(f'google auth failed: {e}', file=sys.stderr)
        return 2

    h = {'apikey': supa_key, 'Authorization': f'Bearer {supa_key}'}

    start = time.monotonic()
    stats = {'boda': 0, 'sav': 0, 'iphone': 0}
    errors = []

    # BODA / SAVCOM tabs — delta-insert only. Existing plates are left
    # untouched (names/phones on the sheet may have been curated in
    # Supabase and we don't want to stomp them).
    for tab, source in [
        ('pikipiki records', 'BODA_RECORDS'),
        ('pikipiki records2', 'SAVCOM_RECORDS'),
    ]:
        try:
            rows = _read_tab(svc, tab)
        except Exception as e:
            errors.append(f'{tab} read: {str(e)[:120]}')
            continue
        seen = {}
        for rec in _rows_from_pikipiki(rows, source):
            seen[rec['plate']] = rec
        existing = _existing_keys(supa_url, h, source, 'plate')
        new_records = [r for k, r in seen.items()
                       if k not in existing]
        wrote = 0
        for i in range(0, len(new_records), 500):
            wrote += _insert_batch(supa_url, h, new_records[i:i + 500])
        stats['sav' if source == 'SAVCOM_RECORDS' else 'boda'] = wrote

    # IPHONE tab — delta-insert by phone.
    try:
        rows = _read_tab(svc, 'iphone_records')
        seen = {}
        for rec in _rows_from_iphone(rows):
            seen[rec['phone']] = rec
        existing = _existing_keys(supa_url, h, 'IPHONE_RECORDS', 'phone')
        new_records = [r for k, r in seen.items()
                       if k not in existing]
        wrote = 0
        for i in range(0, len(new_records), 500):
            wrote += _insert_batch(supa_url, h, new_records[i:i + 500])
        stats['iphone'] = wrote
    except Exception as e:
        errors.append(f'iphone_records: {str(e)[:120]}')

    print(json.dumps({
        'runtime_sec': round(time.monotonic() - start, 2),
        'upserted': stats,
        'errors': errors,
    }))
    return 0 if not errors else 2


if __name__ == '__main__':
    sys.exit(main())
