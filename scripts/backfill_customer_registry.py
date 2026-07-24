#!/usr/bin/env python3
"""
backfill_customer_registry.py — copy pikipiki records + pikipiki
records2 into the customer_registry table (start_date + loan_amount_tsh
stay NULL — those aren't on the sheet).

Runs once after 002_customer_registry.sql has been applied.
Idempotent within a single run (dedupes by plate within each source),
but does NOT prevent re-runs from creating duplicates — the first run
should be the only one. To re-seed cleanly, TRUNCATE customer_registry
first via the Supabase SQL editor.

Env expected in /home/eleg/transaction-processor/.env:
  SUPABASE_URL, SUPABASE_SERVICE_KEY, GOOGLE_CREDENTIALS_JSON.

Exit codes:
  0 — completed
  1 — configuration error
  2 — Sheets or Supabase unreachable
"""

from __future__ import annotations

import json
import os
import re
import sys
import time

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build


SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'

# Plate shape guard — MC + 3 digits + 3 letters. Rejects headers and
# garbage cells that would create bogus customer rows.
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


def _normalize_plate(v) -> str | None:
    if v is None:
        return None
    p = re.sub(r'\s+', '', str(v)).upper()
    return p if _PLATE_RX.match(p) else None


def _normalize_phone(v) -> str | None:
    """Digits-only phone, at least 9 digits."""
    if v is None:
        return None
    s = re.sub(r'\D', '', str(v))
    return s if len(s) >= 9 else None


def _clean_name(v) -> str:
    """Strip trailing phone digits blob from name cells like
    'NJAUKA BAKARI MOHAMED 0674299966'."""
    if v is None:
        return ''
    return re.sub(r'\s*\d{9,}\s*$', '', str(v)).strip()


def _rows_from_pikipiki(rows: list[list], customer_type: str
                        ) -> list[dict]:
    """pikipiki records / pikipiki records2 layout:
        col 0 = serial id (mostly blank)
        col 1 = plate  → customer_registry.plate
        col 2 = name   → customer_registry.customer_name
        col 3 = phone OR depositor name (per-row)
        col 4 = (records2 only) SAV customer_id
    Col 3 is a phone when all digits, otherwise treated as bank_account_name.
    Multiple sheet rows may share the same plate (different depositors);
    we keep them all — one customer_registry row per sheet row.
    """
    out = []
    for row in rows:
        plate = _normalize_plate(row[1] if len(row) > 1 else '')
        if not plate:
            continue
        name = _clean_name(row[2] if len(row) > 2 else '')
        if not name:
            continue

        col_d = str(row[3]).strip() if len(row) > 3 and row[3] not in ('', 0) else ''
        phone = _normalize_phone(col_d) if col_d else None
        bank_account_name = None
        if col_d and not phone and any(c.isalpha() for c in col_d):
            bank_account_name = col_d.upper()

        sav_id = None
        if customer_type == 'savcom' and len(row) > 4 and row[4]:
            sav_id = str(row[4]).strip()

        out.append({
            'customer_name':     name,
            'plate':             plate,
            'phone':             phone,
            'bank_account_name': bank_account_name,
            'customer_type':     customer_type,
            'sav_customer_id':   sav_id,
            'created_by':        'backfill_customer_registry.py',
        })
    return out


def _insert_batch(url: str, h: dict, records: list[dict]) -> int:
    if not records:
        return 0
    try:
        r = requests.post(
            f'{url}/rest/v1/customer_registry',
            headers={**h, 'Content-Type': 'application/json',
                     'Prefer': 'return=minimal'},
            json=records, timeout=60,
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

    # customer_registry lives in a SEPARATE Supabase project — not the
    # one that holds transactions/sms_events/customers. Reads from the
    # _REGISTRY env vars so the existing pipeline stays untouched.
    supa_url = os.environ.get('SUPABASE_URL_REGISTRY', '').rstrip('/')
    supa_key = os.environ.get('SUPABASE_SERVICE_KEY_REGISTRY', '')
    creds_raw = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')
    if not (supa_url and supa_key and creds_raw):
        print('missing env: need SUPABASE_URL_REGISTRY, '
              'SUPABASE_SERVICE_KEY_REGISTRY, GOOGLE_CREDENTIALS_JSON',
              file=sys.stderr)
        return 1

    try:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(creds_raw),
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'],
        )
        svc = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f'google auth failed: {e}', file=sys.stderr)
        return 2

    h = {'apikey': supa_key, 'Authorization': f'Bearer {supa_key}'}
    start = time.monotonic()
    stats = {'boda': 0, 'savcom': 0}
    errors: list[str] = []

    for tab, ctype in [
        ('pikipiki records',  'boda'),
        ('pikipiki records2', 'savcom'),
    ]:
        try:
            r = svc.spreadsheets().values().get(
                spreadsheetId=SHEET_ID, range=f"'{tab}'!A:Z",
                valueRenderOption='UNFORMATTED_VALUE',
            ).execute()
            rows = r.get('values', [])
        except Exception as e:
            errors.append(f'{tab} read: {str(e)[:120]}')
            continue
        records = _rows_from_pikipiki(rows, ctype)
        wrote = 0
        for i in range(0, len(records), 500):
            wrote += _insert_batch(supa_url, h, records[i:i + 500])
        stats[ctype] = wrote

    print(json.dumps({
        'runtime_sec': round(time.monotonic() - start, 2),
        'inserted':    stats,
        'errors':      errors,
    }))
    return 0 if not errors else 2


if __name__ == '__main__':
    sys.exit(main())
