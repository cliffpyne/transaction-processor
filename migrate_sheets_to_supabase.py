#!/usr/bin/env python3
"""
migrate_sheets_to_supabase.py

One-off migration of every Google Sheet tab into Supabase.

Reads:
  Env GOOGLE_CREDENTIALS_JSON  — service account JSON (same one the app uses)
  Env SUPABASE_URL             — https://<ref>.supabase.co
  Env SUPABASE_SERVICE_KEY     — service_role secret from Supabase → API

Writes:
  - Every row from every listed tab into `transactions` or `customers`.
  - Batches of 500 via PostgREST.
  - Idempotent per (source_tab, original_id) so a second run does nothing.
  - Prints per-tab counts + a duplicate-ref audit at the end.

Prereqs:
  1. Run schema.sql in Supabase → SQL Editor first.
  2. `pip install requests` (already transitively there via googleapiclient).

Usage:
  export SUPABASE_URL='https://npornslyozuxxigeoqgi.supabase.co'
  export SUPABASE_SERVICE_KEY='sb_secret_...'
  export GOOGLE_CREDENTIALS_JSON='{...}'   # or read from google.json
  python3 migrate_sheets_to_supabase.py
"""

import json
import os
import re
import sys
import time
from collections import Counter
from datetime import date, datetime

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Config ─────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://npornslyozuxxigeoqgi.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

# Fall back to reading google.json from disk if env var is empty
GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS_JSON')
if not GOOGLE_CREDS:
    gpath = os.path.join(os.path.dirname(__file__), 'google.json')
    if os.path.exists(gpath):
        with open(gpath) as f:
            GOOGLE_CREDS = f.read()

if not SUPABASE_KEY:
    print('❌ SUPABASE_SERVICE_KEY not set'); sys.exit(1)
if not GOOGLE_CREDS:
    print('❌ GOOGLE_CREDENTIALS_JSON not set and google.json missing'); sys.exit(1)

# ── Sheet IDs (same constants as app.py) ───────────────────────────────────
PASSED_SHEET_ID   = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'
PIKIPIKI_SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'
IPHONE_SHEET_ID   = '1Y2cOyObQvP502kvEbC-uGDP-3Sf5X9JKnDDYmR0BPRQ'
NMB_SHEET_ID      = '1YchOygtfVyVNgz37sGX_KKud_Wr9KQsIkQKn_tEdbek'

# (sheet_id, actual_tab_on_sheet, source_tab_label, variant)
TRANSACTION_TABS = [
    (PASSED_SHEET_ID, 'PASSED',            'PASSED',             'passed_9col'),
    (PASSED_SHEET_ID, 'PASSED_SAV',        'PASSED_SAV',         'passed_9col'),
    (PASSED_SHEET_ID, 'FAILED',            'FAILED',             'failed_8col'),
    (PASSED_SHEET_ID, 'PASSED_SAV_NMB',    'PASSED_SAV_NMB_OLD', 'passed_9col'),
    (PASSED_SHEET_ID, 'FAILED_NMB',        'FAILED_NMB_OLD',     'failed_8col'),
    (NMB_SHEET_ID,    'PASSED',            'PASSED_NMB',         'passed_9col'),
    (NMB_SHEET_ID,    'PASSED_SAV_NMB',    'PASSED_SAV_NMB',     'passed_9col'),
    (NMB_SHEET_ID,    'FAILED_NMB',        'FAILED_NMB',         'failed_8col'),
    (IPHONE_SHEET_ID, 'BANK_PASSED',       'BANK_PASSED',        'passed_9col'),
    (IPHONE_SHEET_ID, 'BANK_FAILED',       'BANK_FAILED',        'failed_8col'),
]

# (sheet_id, tab_name, source_tab_label, variant)
CUSTOMER_TABS = [
    (PIKIPIKI_SHEET_ID, 'pikipiki records',  'pikipiki_records',  'pikipiki'),
    (PIKIPIKI_SHEET_ID, 'pikipiki records2', 'pikipiki_records2', 'pikipiki'),
    (IPHONE_SHEET_ID,   'IPHONE_RECORDS',    'IPHONE_RECORDS',    'iphone'),
]

BATCH = 500

SUPA_HEADERS = {
    'apikey':        SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type':  'application/json',
    'Prefer':        'return=minimal,resolution=merge-duplicates',
}


# ── Helpers ────────────────────────────────────────────────────────────────
def parse_transaction_day(s):
    """Best-effort DATE parse from every sheet-date format we know.
    Returns 'YYYY-MM-DD' string (Supabase-friendly) or None.
    """
    if not s:
        return None
    s = str(s).strip()

    # ISO: 2026-06-11 or 2026-06-11 15:29:16
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        try: return date(int(m[1]), int(m[2]), int(m[3])).isoformat()
        except ValueError: return None

    # DD.MM.YYYY [HH:MM:SS]  (NMB happy path)
    m = re.match(r'^(\d{2})\.(\d{2})\.(\d{4})', s)
    if m:
        try: return date(int(m[3]), int(m[2]), int(m[1])).isoformat()
        except ValueError: return None

    # DD/MM/YYYY
    m = re.match(r'^(\d{2})/(\d{2})/(\d{4})', s)
    if m:
        try: return date(int(m[3]), int(m[2]), int(m[1])).isoformat()
        except ValueError: return None

    # DD-Mon-YYYY, DD Mon YYYY, DD-Mon-YY  (CSV/PDF fallback)
    first_token = s.split()[0] if s else ''
    for fmt in ('%d-%b-%Y', '%d-%b-%y'):
        try: return datetime.strptime(first_token, fmt).date().isoformat()
        except ValueError: pass
    # Space-separated form: '11 Jun 2026'
    try: return datetime.strptime(' '.join(s.split()[:3]), '%d %b %Y').date().isoformat()
    except (ValueError, IndexError): pass

    return None


def parse_int(v):
    if v is None: return None
    s = str(v).strip()
    if not s: return None
    try: return int(s)
    except ValueError: return None


def parse_num(v):
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    s = str(v).replace(',', '').replace(' ', '').strip()
    if not s: return None
    try: return float(s)
    except ValueError: return None


def s_or_none(v):
    if v is None: return None
    s = str(v).strip()
    return s if s else None


# ── Google Sheets read ─────────────────────────────────────────────────────
def get_sheets():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDS),
        scopes=['https://www.googleapis.com/auth/spreadsheets'],
    )
    return build('sheets', 'v4', credentials=creds)


def read_tab(service, sheet_id, tab_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!A:I",
    ).execute()
    return result.get('values', [])


# ── Row converters ─────────────────────────────────────────────────────────
def row_to_transaction(row, source_tab, source_sheet_id, variant):
    def cell(i):
        return row[i] if i < len(row) else None

    original_id = parse_int(cell(0))
    if original_id is None:
        return None  # header or blank

    tx_date_text = s_or_none(cell(1))
    bank         = s_or_none(cell(2))
    description  = str(cell(3) or '')
    credit       = parse_num(cell(4))
    identifier   = s_or_none(cell(5))

    if variant == 'passed_9col':
        customer_name = s_or_none(cell(6))
        ref_number    = s_or_none(cell(7))
        customer_id   = s_or_none(cell(8))
        fail_reason   = None
    else:  # failed_8col
        customer_name = None
        fail_reason   = s_or_none(cell(6))
        ref_number    = s_or_none(cell(7))
        customer_id   = None

    is_fuzzy = variant == 'passed_9col' and identifier is not None and ',' in identifier

    return {
        'original_id':      original_id,
        'transaction_date': tx_date_text,
        'transaction_day':  parse_transaction_day(tx_date_text),
        'posting_date':     None,
        'bank':             bank or 'UNKNOWN',
        'description':      description,
        'credit_amount':    credit,
        'identifier':       identifier,
        'customer_name':    customer_name,
        'ref_number':       ref_number,
        'customer_id':      customer_id,
        'fail_reason':      fail_reason,
        'is_fuzzy_rescued': is_fuzzy,
        'source_tab':       source_tab,
        'source_sheet_id':  source_sheet_id,
    }


def row_to_customers(row, source_tab, variant):
    def cell(i):
        return row[i] if i < len(row) else None

    if variant == 'pikipiki':
        plate       = str(cell(1) or '').replace(' ', '').upper() or None
        name        = s_or_none(cell(2))
        phone       = str(cell(3) or '').replace(' ', '').replace('-', '') or None
        customer_id = s_or_none(cell(4))
        if not plate and not phone and not name:
            return []
        return [{
            'plate':       plate,
            'phone':       phone,
            'name':        name,
            'customer_id': customer_id,
            'source_tab':  source_tab,
        }]

    if variant == 'iphone':
        name = s_or_none(cell(0))
        if not name:
            return []
        rows = []
        for i in (1, 2):
            raw = s_or_none(cell(i))
            if not raw:
                continue
            rows.append({
                'plate':       None,
                'phone':       raw,   # keep raw string — normalized on read in the app
                'name':        name,
                'customer_id': None,
                'source_tab':  source_tab,
            })
        return rows

    return []


# ── Supabase write ─────────────────────────────────────────────────────────
def post_batch(table, rows):
    if not rows:
        return
    on_conflict = ''
    if table == 'transactions':
        on_conflict = '?on_conflict=source_tab,original_id'

    for attempt in range(4):
        r = requests.post(
            f'{SUPABASE_URL}/rest/v1/{table}{on_conflict}',
            headers=SUPA_HEADERS,
            json=rows,
            timeout=90,
        )
        if r.ok:
            return
        # Retry on transient 5xx
        if 500 <= r.status_code < 600:
            print(f'  … {r.status_code}, retrying in {2**attempt}s')
            time.sleep(2 ** attempt)
            continue
        raise RuntimeError(f'{table} write {r.status_code}: {r.text[:400]}')
    raise RuntimeError(f'{table} write failed after retries')


# ── Per-tab driver ─────────────────────────────────────────────────────────
def migrate_transaction_tab(service, sheet_id, tab_name, source_tab, variant):
    print(f'\n📥 transactions ← {source_tab}   ({sheet_id[:8]}…/"{tab_name}")')
    values = read_tab(service, sheet_id, tab_name)
    if not values:
        print('   (empty)'); return 0

    header, *rows = values
    print(f'   header: {header[:9]}')

    batch, sent, skipped = [], 0, 0
    for row in rows:
        rec = row_to_transaction(row, source_tab, sheet_id, variant)
        if rec is None:
            skipped += 1
            continue
        batch.append(rec)
        if len(batch) >= BATCH:
            post_batch('transactions', batch)
            sent += len(batch); batch = []
            print(f'   …{sent} rows')
    if batch:
        post_batch('transactions', batch)
        sent += len(batch)
    print(f'   ✅ {sent} rows (skipped {skipped})')
    return sent


def migrate_customer_tab(service, sheet_id, tab_name, source_tab, variant):
    print(f'\n📥 customers    ← {source_tab}   ({sheet_id[:8]}…/"{tab_name}")')
    values = read_tab(service, sheet_id, tab_name)
    if not values:
        print('   (empty)'); return 0

    header, *rows = values
    print(f'   header: {header[:5]}')

    batch, sent = [], 0
    for row in rows:
        for rec in row_to_customers(row, source_tab, variant):
            batch.append(rec)
        if len(batch) >= BATCH:
            post_batch('customers', batch)
            sent += len(batch); batch = []
            print(f'   …{sent} rows')
    if batch:
        post_batch('customers', batch)
        sent += len(batch)
    print(f'   ✅ {sent} rows')
    return sent


# ── Post-migration audit ───────────────────────────────────────────────────
def audit_ref_dupes():
    print('\n🔍 auditing ref_number duplicates across every migrated tab…')
    # Paginate — Supabase caps a single response at ~1000 rows unless we range
    counts = Counter()
    offset = 0
    page_size = 1000
    while True:
        r = requests.get(
            f'{SUPABASE_URL}/rest/v1/transactions'
            f'?select=ref_number&ref_number=not.is.null&ref_number=neq.',
            headers={
                'apikey':        SUPABASE_KEY,
                'Authorization': f'Bearer {SUPABASE_KEY}',
                'Range-Unit':    'items',
                'Range':         f'{offset}-{offset + page_size - 1}',
            },
            timeout=90,
        )
        if not r.ok:
            print(f'   ! query failed: {r.status_code} {r.text[:200]}')
            return
        rows = r.json()
        if not rows:
            break
        for row in rows:
            ref = row.get('ref_number')
            if ref:
                counts[ref] += 1
        if len(rows) < page_size:
            break
        offset += page_size
        print(f'   scanned {offset:,}…')

    dupes = {ref: c for ref, c in counts.items() if c > 1}
    if not dupes:
        print('   ✅ no historical ref leaks — safe to run the UNIQUE index step now')
        return

    print(f'   ⚠️  {len(dupes):,} refs appear more than once in the migrated data')
    print(f'      (this is the historical dedup leak — every extra copy = money burned)')
    for ref, c in sorted(dupes.items(), key=lambda x: -x[1])[:25]:
        print(f'      {ref}  ×{c}')
    if len(dupes) > 25:
        print(f'      … and {len(dupes) - 25} more (query in Studio for full list)')


# ── Main ───────────────────────────────────────────────────────────────────
def main():
    print(f'🔗 Supabase: {SUPABASE_URL}')
    service = get_sheets()

    tx_total = 0
    for sheet_id, tab, source_tab, variant in TRANSACTION_TABS:
        tx_total += migrate_transaction_tab(service, sheet_id, tab, source_tab, variant)

    cust_total = 0
    for sheet_id, tab, source_tab, variant in CUSTOMER_TABS:
        cust_total += migrate_customer_tab(service, sheet_id, tab, source_tab, variant)

    print(f'\n📊 Done: {tx_total:,} transactions, {cust_total:,} customers migrated')
    audit_ref_dupes()


if __name__ == '__main__':
    main()
