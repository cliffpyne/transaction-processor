#!/usr/bin/env python3
"""
backfill_iphone_registry.py — copy IPHONE_RECORDS into customer_registry.

Source: separate Sheet `1Y2cOyObQvP502kvEbC-uGDP-3Sf5X9JKnDDYmR0BPRQ`,
        tab `IPHONE_RECORDS`, cols A..Z.
        col A = customer_name; cols B..Z = phone numbers (any count).
        Each customer is ONE row in the sheet; multiple phones live across
        cols B+.

Target: customer_registry (SUPABASE_URL_REGISTRY project) — one row per
        customer, with `phone` = primary (first non-empty phone) and
        `phones` = the full deduped list.

Idempotent-per-run only. To re-seed cleanly, delete existing iphone rows
first via the Supabase SQL editor:
    DELETE FROM customer_registry WHERE customer_type = 'iphone';

Env expected in /home/eleg/transaction-processor/.env:
  SUPABASE_URL_REGISTRY, SUPABASE_SERVICE_KEY_REGISTRY,
  GOOGLE_CREDENTIALS_JSON.
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


IPHONE_SHEET_ID = '1Y2cOyObQvP502kvEbC-uGDP-3Sf5X9JKnDDYmR0BPRQ'
IPHONE_TAB = 'IPHONE_RECORDS'


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


def _normalize_phone(raw) -> str | None:
    """Digits-only phone, at least 9 digits. Same rule as the plate resolver
    uses on the transaction side, so lookups match cleanly."""
    if raw is None:
        return None
    s = re.sub(r'\D', '', str(raw))
    if len(s) < 9:
        return None
    # Standardise to 255XXXXXXXXX (Tanzania). Accept 0752…, 255752…, or 752…
    if s.startswith('255') and len(s) == 12:
        return s
    if s.startswith('0') and len(s) == 10:
        return '255' + s[1:]
    if len(s) == 9:
        return '255' + s
    return s


def _clean_name(raw) -> str:
    if raw is None:
        return ''
    return re.sub(r'\s+', ' ', str(raw)).strip()


def _rows_from_iphone(rows: list[list]) -> list[dict]:
    """Convert sheet rows into customer_registry payloads.

    Sheet layout:
        col A = customer_name
        cols B..Z = phone numbers (variable count per row)

    Skips rows without a name. Dedups phones within a customer, drops
    unparseable ones. Multi-row customers (same name across rows) are NOT
    merged — if the same name appears twice, we insert twice. That's
    a data-hygiene call for the operator to make in the UI.
    """
    out: list[dict] = []
    # Skip header row (row 0) — col A there is the string 'CUSTOMER NAME'
    # or similar. If the first row's A cell is a real name, no harm done —
    # backfill_customer_registry.py has the same convention.
    start = 1 if rows and rows[0] and str(rows[0][0]).strip().upper() in (
        'CUSTOMER NAME', 'NAME', 'NAMES', 'CUSTOMER',
    ) else 0
    for row in rows[start:]:
        if not row:
            continue
        name = _clean_name(row[0] if len(row) > 0 else '')
        if not name:
            continue
        phones: list[str] = []
        seen: set[str] = set()
        for cell in row[1:]:
            # Support cells with multiple phones separated by comma/semicolon
            # (defensive — some IPHONE_RECORDS rows have "0752…, 0754…").
            for piece in re.split(r'[,;/]+', str(cell or '')):
                norm = _normalize_phone(piece)
                if norm and norm not in seen:
                    seen.add(norm)
                    phones.append(norm)
        if not phones:
            # Name-only row — still insert so the operator can add phones
            # via the UI later. iPhone customer with no known phone can't
            # be matched but keeps their record in place.
            phones = []
        out.append({
            'customer_name':     name,
            'plate':             None,
            'phone':             phones[0] if phones else None,
            'phones':            phones,
            'bank_account_name': None,
            'customer_type':     'iphone',
            'sav_customer_id':   None,
            'created_by':        'backfill_iphone_registry.py',
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
        print(f'insert HTTP {r.status_code}: {r.text[:400]}',
              file=sys.stderr)
        return 0
    return len(records)


def main() -> int:
    _load_env_file('/home/eleg/transaction-processor/.env')
    _load_env_file('.env')

    supa_url  = os.environ.get('SUPABASE_URL_REGISTRY', '').rstrip('/')
    supa_key  = os.environ.get('SUPABASE_SERVICE_KEY_REGISTRY', '')
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

    try:
        r = svc.spreadsheets().values().get(
            spreadsheetId=IPHONE_SHEET_ID, range=f"'{IPHONE_TAB}'!A:Z",
            valueRenderOption='UNFORMATTED_VALUE',
        ).execute()
        rows = r.get('values', [])
    except Exception as e:
        print(f'iphone sheet read failed: {e}', file=sys.stderr)
        return 2

    records = _rows_from_iphone(rows)
    wrote = 0
    for i in range(0, len(records), 500):
        wrote += _insert_batch(supa_url, h, records[i:i + 500])

    total_phones = sum(len(r.get('phones') or []) for r in records)
    print(json.dumps({
        'runtime_sec':      round(time.monotonic() - start, 2),
        'customers_read':   len(records),
        'customers_wrote':  wrote,
        'phone_entries':    total_phones,
    }))
    return 0 if wrote == len(records) else 2


if __name__ == '__main__':
    sys.exit(main())
