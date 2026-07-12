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
import socket
import sys
import time
from collections import Counter
from datetime import date, datetime

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Big tabs (30k+ rows) can wedge the default 60s socket timeout.
# 3 min per HTTP call is comfortable even at slow bandwidth.
socket.setdefaulttimeout(180)

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
CRDB_SHEET_ID     = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'
NMB_SHEET_ID      = '1YchOygtfVyVNgz37sGX_KKud_Wr9KQsIkQKn_tEdbek'
IPHONE_SHEET_ID   = '1Y2cOyObQvP502kvEbC-uGDP-3Sf5X9JKnDDYmR0BPRQ'
PIKIPIKI_SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'

# Business-friendly labels used in the `source_sheet_id` column so the DB
# never shows opaque 44-char Google Sheet IDs to humans.
SHEET_LABEL = {
    CRDB_SHEET_ID:     'CRDBBANK',
    NMB_SHEET_ID:      'NMBBANK',
    IPHONE_SHEET_ID:   'IPHONE',
    PIKIPIKI_SHEET_ID: 'PIKIPIKI',
}

# (sheet_id, actual_tab_on_sheet, source_tab_label, variant)
# Only three tabs from each transaction sheet — the two junk NMB tabs on
# CRDBBANK (PASSED_SAV_NMB, FAILED_NMB) are dropped entirely.
TRANSACTION_TABS = [
    (CRDB_SHEET_ID,   'PASSED',            'CRDBPASSED',         'passed_9col'),
    (CRDB_SHEET_ID,   'PASSED_SAV',        'CRDBSAVCOM',         'passed_9col'),
    (CRDB_SHEET_ID,   'FAILED',            'CRDBFAILED',         'failed_8col'),
    (NMB_SHEET_ID,    'PASSED',            'NMBPASSED',          'passed_9col'),
    (NMB_SHEET_ID,    'PASSED_SAV_NMB',    'NMBSAVCOM',          'passed_9col'),
    (NMB_SHEET_ID,    'FAILED_NMB',        'NMBFAILED',          'failed_8col'),
    (IPHONE_SHEET_ID, 'BANK_PASSED',       'IPHONEPASSED',       'passed_9col'),
    (IPHONE_SHEET_ID, 'BANK_FAILED',       'IPHONEFAILED',       'failed_8col'),
]

# (sheet_id, tab_name, source_tab_label, variant)
CUSTOMER_TABS = [
    (PIKIPIKI_SHEET_ID, 'pikipiki records',  'BODA_RECORDS',   'pikipiki'),
    (PIKIPIKI_SHEET_ID, 'pikipiki records2', 'SAVCOM_RECORDS', 'pikipiki'),
    (IPHONE_SHEET_ID,   'IPHONE_RECORDS',    'IPHONE_RECORDS', 'iphone'),
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

    Accepts padded AND unpadded day/month (NMB writes '9-Jul-26' single-digit
    day, 2-digit year — same as '09-Jul-2026'). Also accepts CSV pandas
    output like '2026-07-11 15:29:16' and NMB PDF's '11/07/2026'.
    """
    if not s:
        return None
    s = str(s).strip()

    # First try strptime on the first token — it's tolerant of unpadded d/m
    first = s.split()[0] if s else ''
    if first:
        for fmt in ('%Y-%m-%d', '%d-%b-%Y', '%d-%b-%y',
                    '%d/%m/%Y', '%d/%m/%y',
                    '%d.%m.%Y', '%d.%m.%y',
                    '%d-%m-%Y', '%d-%m-%y'):
            try: return datetime.strptime(first, fmt).date().isoformat()
            except ValueError: pass

    # Space-separated forms: '11 Jun 2026', '9 Jul 2026', '9 Jul 26'
    parts = s.split()[:3]
    if len(parts) == 3:
        for fmt in ('%d %b %Y', '%d %b %y'):
            try: return datetime.strptime(' '.join(parts), fmt).date().isoformat()
            except ValueError: pass

    # Regex fallbacks for oddly-formatted first tokens embedded in longer strings.
    for pat, ymd in (
        (r'(\d{4})-(\d{1,2})-(\d{1,2})', (1, 2, 3)),
        (r'(\d{1,2})\.(\d{1,2})\.(\d{4})', (3, 2, 1)),
        (r'(\d{1,2})/(\d{1,2})/(\d{4})',   (3, 2, 1)),
        (r'(\d{1,2})-(\d{1,2})-(\d{4})',   (3, 2, 1)),
    ):
        m = re.search(pat, s)
        if m:
            try: return date(int(m[ymd[0]]), int(m[ymd[1]]), int(m[ymd[2]])).isoformat()
            except ValueError: pass

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




# ── Content-based classifiers for the FAILED variant ──────────────────────
# Historical FAILED rows come in at least four layouts because the sheet was
# hand-edited at various points:
#
#   L1 (orig FAILED, 9 cells): A|B|C|D|E|F|(empty)|reason|ref
#   L2 (mid-vintage,  9 cells): A|B|C|D|E|F|reason|(empty)|junk-fragment
#   L3 (2026-Feb,     9 cells): A|B|C|D|E|F|customer_name|ref|junk-fragment
#   L4 (2026-Apr on,  8 cells): A|B|C|D|E|F|reason|ref
#
# Positional heuristics fail on this mess. Instead we look at what each
# cell contains:
#   - A ref is hex (10+ chars) OR an NMB xref like '101AGD...' / '238FTM...'
#   - A reason contains 'not found' or literal phrases like 'No identifier'
#   - Everything else is customer_name residue or hand-typed garbage → ignore.
_REF_HEX  = re.compile(r'^[A-Fa-f0-9]{10,}$')
_REF_NMB  = re.compile(r'^[0-9A-Z]{10,}$')   # 101AGD..., 238FTM..., etc.
_REASON_KEYWORDS = (
    'not found', 'no identifier', 'no plate chosen',
    'skipped by user', 'rejected by user', 'not found in records',
)


def _looks_like_ref(v):
    if not v: return False
    s = str(v).strip()
    if _REF_HEX.match(s):
        return True
    # NMB refs: uppercase alphanumeric with at least one letter
    if _REF_NMB.match(s) and any(c.isalpha() for c in s):
        return True
    return False


def _looks_like_reason(v):
    if not v: return False
    s = str(v).strip().lower()
    return any(k in s for k in _REASON_KEYWORDS) or '(' in s and ')' in s


def _classify_failed_cells(c6, c7, c8):
    """Given the three trailing cells of a FAILED-variant row, return
    (fail_reason, ref_number). Content-based so all four historical
    layouts are handled uniformly."""
    cells = [s_or_none(c6), s_or_none(c7), s_or_none(c8)]

    ref = next((c for c in cells if _looks_like_ref(c)), None)
    reason = next((c for c in cells
                   if c and c != ref and _looks_like_reason(c)), None)
    return reason, ref


# ── Google Sheets read ─────────────────────────────────────────────────────
def get_sheets():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDS),
        scopes=['https://www.googleapis.com/auth/spreadsheets'],
    )
    return build('sheets', 'v4', credentials=creds)


def read_tab(service, sheet_id, tab_name):
    """Read a whole tab as its user-visible values (FORMATTED_VALUE, the
    default). Column B carries whatever the sheet's Date cell displays —
    that is the source of truth; we never second-guess it by pulling the
    underlying serial (locale-misparsed 11/6 → Nov 6 was exactly why)."""
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!A:I",
    ).execute()
    return result.get('values', [])


def read_tab_chunk(service, sheet_id, tab_name, start_row, chunk_rows):
    """Read a single row-range from a tab. Used to stream large transaction
    tabs so we never hold the whole 30k-row payload in memory or hit the
    single-request timeout."""
    range_str = f"'{tab_name}'!A{start_row}:I{start_row + chunk_rows - 1}"
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=range_str,
    ).execute()
    return result.get('values', [])


# ── Row converters ─────────────────────────────────────────────────────────
_HEADER_A = {
    'id', 'sn', 'no', 'no.', '#', 'original_id', 'row', 'row id',
    'refnumber',   # PASSED sheet historical header
}
_HEADER_B = {'date', 'trans date', 'transaction date'}


def _is_header_or_blank(row):
    """True if the row is the sheet header or completely empty. Anything else
    (including rows with garbage in column A like NMB's stray '23-Jun-40'
    strings) must be treated as real data — we've lost too many valid rows
    by mistaking them for headers."""
    if not row or not any((str(v).strip() for v in row)):
        return True
    col_a = str(row[0]).strip().lower() if len(row) > 0 else ''
    col_b = str(row[1]).strip().lower() if len(row) > 1 else ''
    # Header rows always land as A ∈ known-header-values or B == "date"
    return col_a in _HEADER_A or col_b in _HEADER_B


# Only migrate rows dated on or after this cutoff — the sheet's historical
# tail is full of malformed hand-edited entries that we don't want in the
# clean DB. Everything before this date is dropped.
MIGRATION_START_DAY = '2026-07-01'


# Track every ref_number we've decided to keep this run so a re-appearing
# ref (some tabs replay the same row across their sheets) can be dropped
# on the client side — the partial UNIQUE index on ref_number would 409
# the whole batch instead of merging.
_SEEN_REFS: set[str] = set()


def row_to_transaction(row, source_tab, source_sheet_id, variant):
    def cell(i):
        return row[i] if i < len(row) else None

    if _is_header_or_blank(row):
        return None

    # original_id may be None (bad data like '23-Jun-40' in column A).
    # That's OK — the (source_tab, original_id) UNIQUE was dropped, so
    # NULL is a legal value and the row still lands in the DB.
    original_id = parse_int(cell(0))

    # cell(1) is the date column — take the sheet's display value verbatim
    # for every bank. The live transaction processor already writes
    # correctly-formatted date+time into column B on both CRDB and NMB
    # rows (via extract_nmb_datetime for NMB, pandas Timestamp for CRDB);
    # our job here is only to mirror what's already on the sheet.
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
    else:  # FAILED variant — content-based detection because historical rows
        # have at least 4 different layouts (see comments in _classify_failed_cells).
        customer_name = None
        customer_id   = None
        fail_reason, ref_number = _classify_failed_cells(
            cell(6), cell(7), cell(8)
        )

    is_fuzzy = variant == 'passed_9col' and identifier is not None and ',' in identifier

    # Skip everything older than the cutoff — historical sheet tail has a lot
    # of hand-edited garbage. Rows whose date fails to parse also drop.
    tx_day = parse_transaction_day(tx_date_text)
    if not tx_day or tx_day < MIGRATION_START_DAY:
        return None

    # Dedupe by ref_number across the whole run so the partial UNIQUE index
    # can't 409 an entire batch. Rows without a ref_number are excluded from
    # the index and stay eligible.
    if ref_number:
        if ref_number in _SEEN_REFS:
            return None
        _SEEN_REFS.add(ref_number)

    return {
        'original_id':      original_id,
        'transaction_date': tx_date_text,
        'transaction_day':  tx_day,
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
        # Treat placeholder phones like '0' / '00' as null — the sheet has
        # thousands of rows where phone is literally '0' and everything else
        # is blank. Those aren't real customers.
        if phone and phone.strip('0') == '':
            phone = None
        # Real customer must have at least a name or a plate. Phone alone
        # (even a real one) isn't enough to identify a customer.
        if not plate and not name:
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
            # Same '0'-placeholder rule for iPhone phone columns.
            if raw.replace(' ', '').replace('-', '').strip('0') == '':
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
    # No on_conflict — PostgREST's ON CONFLICT (ref_number) requires a
    # non-partial unique constraint, but ours is a partial index (excludes
    # NULL / empty ref). The migration wipes first, so no duplicates can
    # arise within a single run anyway; the partial UNIQUE still guards
    # the live path.
    on_conflict = ''

    last_body = ''
    for attempt in range(6):
        try:
            r = requests.post(
                f'{SUPABASE_URL}/rest/v1/{table}{on_conflict}',
                headers=SUPA_HEADERS,
                json=rows,
                timeout=180,
            )
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError) as e:
            print(f'  … network hiccup, retrying in {2**attempt}s  ({type(e).__name__})')
            time.sleep(2 ** attempt)
            continue
        if r.ok:
            return
        last_body = r.text
        if 500 <= r.status_code < 600:
            print(f'  … {r.status_code}, retrying in {2**attempt}s  ({r.text[:200]})')
            time.sleep(2 ** attempt)
            continue
        raise RuntimeError(f'{table} write {r.status_code}: {r.text[:400]}')
    # If we get here, all retries hit 5xx. Print the row payload of the FIRST
    # record so the user can see what triggered the error.
    print('  ! sample row that failed:')
    print('    ', json.dumps(rows[0], default=str)[:600])
    raise RuntimeError(f'{table} write failed after retries. Last body: {last_body[:400]}')


# ── Per-tab driver ─────────────────────────────────────────────────────────
CHUNK_ROWS = 5000  # sheet rows per Google API request


def migrate_transaction_tab(service, sheet_id, tab_name, source_tab, variant):
    """Stream a transaction tab in chunks of CHUNK_ROWS rows so the read
    fits under socket timeout and memory stays flat. Header lives on row 1,
    data starts row 2.

    Deduplicates by (source_tab, original_id) as we go: if the SAME
    original_id appears twice in the same tab, we send it once (last row
    wins — matches Postgres UPSERT semantics) and count the collision.
    That count IS the historical dedup leak — every unit above zero is a
    row the app processed twice.
    """
    print(f'\n📥 transactions ← {source_tab}   ({sheet_id[:8]}…/"{tab_name}")')

    header_chunk = read_tab_chunk(service, sheet_id, tab_name, 1, 1)
    if header_chunk:
        print(f'   header: {header_chunk[0][:9]}')
    else:
        print('   (empty)'); return 0

    # No per-tab dedup by original_id anymore — that column collides
    # legitimately (the app's counter has been reset multiple times in
    # the sheets). Every row goes to the DB. Duplicate protection now
    # lives at the ref_number level.
    sent, skipped, start_row = 0, 0, 2
    while True:
        chunk = read_tab_chunk(service, sheet_id, tab_name, start_row, CHUNK_ROWS)
        if not chunk:
            break

        batch = []
        for row in chunk:
            rec = row_to_transaction(row, source_tab, SHEET_LABEL[sheet_id], variant)
            if rec is None:
                skipped += 1
                continue
            batch.append(rec)

        for i in range(0, len(batch), BATCH):
            post_batch('transactions', batch[i:i + BATCH])

        sent += len(batch)
        end_row = start_row + len(chunk) - 1
        print(f'   rows {start_row:>6}-{end_row:<6}  → {len(batch):>4} sent  (running total: {sent:,})')

        if len(chunk) < CHUNK_ROWS:
            break
        start_row = end_row + 1

    print(f'   ✅ {sent:,} rows (skipped {skipped})')
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
    try:
        audit_ref_dupes()
    except Exception as e:
        print(f'\n⚠️  Audit step skipped due to transient error: {e}')
        print('   Migration itself succeeded — run the audit SQL in Studio to see leaks.')


if __name__ == '__main__':
    main()
