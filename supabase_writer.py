"""
supabase_writer.py — thin dual-write helper for app.py

Contract:
  - append_to_sheet() in app.py calls append() below after a successful
    Google Sheets update. This mirrors the same rows into Supabase.
  - Never raises. Any error is logged and swallowed so a Supabase outage
    can never break the sheet write path (which is still the source of
    truth during the dual-write phase).
  - No-op when the WRITE_TO_SUPABASE env var is not truthy.
  - No-op when SUPABASE_URL or SUPABASE_SERVICE_KEY are missing.

Env vars:
  SUPABASE_URL           https://<ref>.supabase.co
  SUPABASE_SERVICE_KEY   service_role secret from Supabase → API
  WRITE_TO_SUPABASE      '1' / 'true' / 'yes'  to enable (default off)
"""

import os
import re
import traceback
from datetime import date, datetime

import requests

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')
ENABLED      = os.environ.get('WRITE_TO_SUPABASE', 'false').lower() in ('1', 'true', 'yes')

_HEADERS = {
    'apikey':        SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type':  'application/json',
    'Prefer':        'return=minimal,resolution=merge-duplicates',
}

# Which logical tab uses the 8-column FAILED variant
_FAILED_TABS = {
    'FAILED', 'FAILED_NMB', 'FAILED_NMB_OLD', 'BANK_FAILED',
}

# Translate app.py's logical sheet_name → the business-friendly source_tab
# value stored in Supabase. This MUST match the naming used by the migration
# script (migrate_sheets_to_supabase.py). If a tab is not in this map, the
# write is silently dropped (used for the two decommissioned _OLD NMB tabs).
_TAB_RENAME = {
    'PASSED':         'CRDBPASSED',
    'PASSED_SAV':     'CRDBSAVCOM',
    'FAILED':         'CRDBFAILED',
    'PASSED_NMB':     'NMBPASSED',
    'PASSED_SAV_NMB': 'NMBSAVCOM',
    'FAILED_NMB':     'NMBFAILED',
    'BANK_PASSED':    'IPHONEPASSED',
    'BANK_FAILED':    'IPHONEFAILED',
    # PASSED_SAV_NMB_OLD, FAILED_NMB_OLD deliberately absent → mirror skipped
}

# Which "bank" (source_sheet_id business label) each logical tab writes to.
_TAB_TO_BANK = {
    'PASSED':         'CRDBBANK',
    'PASSED_SAV':     'CRDBBANK',
    'FAILED':         'CRDBBANK',
    'PASSED_NMB':     'NMBBANK',
    'PASSED_SAV_NMB': 'NMBBANK',
    'FAILED_NMB':     'NMBBANK',
    'BANK_PASSED':    'IPHONE',
    'BANK_FAILED':    'IPHONE',
}


def _parse_day(s):
    """Best-effort DATE parse from any sheet-date string. Returns
    'YYYY-MM-DD' or None. Same logic as migrate_sheets_to_supabase.py.

    Accepts padded AND unpadded day/month (NMB writes '9-Jul-26' with
    single-digit day / 2-digit year — same as '09-Jul-2026'). Falls back
    to embedded-date regex before giving up."""
    if not s:
        return None
    s = str(s).strip()

    first = s.split()[0] if s else ''
    if first:
        for fmt in ('%Y-%m-%d', '%d-%b-%Y', '%d-%b-%y',
                    '%d/%m/%Y', '%d/%m/%y',
                    '%d.%m.%Y', '%d.%m.%y',
                    '%d-%m-%Y', '%d-%m-%y'):
            try: return datetime.strptime(first, fmt).date().isoformat()
            except ValueError: pass

    parts = s.split()[:3]
    if len(parts) == 3:
        for fmt in ('%d %b %Y', '%d %b %y'):
            try: return datetime.strptime(' '.join(parts), fmt).date().isoformat()
            except ValueError: pass

    for pat, ymd in (
        (r'(\d{4})-(\d{1,2})-(\d{1,2})',   (1, 2, 3)),
        (r'(\d{1,2})\.(\d{1,2})\.(\d{4})', (3, 2, 1)),
        (r'(\d{1,2})/(\d{1,2})/(\d{4})',   (3, 2, 1)),
        (r'(\d{1,2})-(\d{1,2})-(\d{4})',   (3, 2, 1)),
    ):
        m = re.search(pat, s)
        if m:
            try: return date(int(m[ymd[0]]), int(m[ymd[1]]), int(m[ymd[2]])).isoformat()
            except ValueError: pass
    return None


def _num(v):
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    s = str(v).replace(',', '').replace(' ', '').strip()
    if not s: return None
    try: return float(s)
    except ValueError: return None


def _int(v):
    if v is None: return None
    if isinstance(v, int): return v
    s = str(v).strip()
    if not s: return None
    try: return int(s)
    except ValueError: return None


def _s(v):
    if v is None: return None
    s = str(v).strip()
    return s if s else None


def _row_to_record_9col(row, source_tab, source_sheet_id):
    row = list(row) + [None] * max(0, 9 - len(row))
    identifier = _s(row[5])
    return {
        'original_id':      _int(row[0]),
        'transaction_date': _s(row[1]),
        'transaction_day':  _parse_day(row[1]),
        'posting_date':     None,
        'bank':             _s(row[2]) or 'UNKNOWN',
        'description':      str(row[3] or ''),
        'credit_amount':    _num(row[4]),
        'identifier':       identifier,
        'customer_name':    _s(row[6]),
        'ref_number':       _s(row[7]),
        'customer_id':      _s(row[8]),
        'fail_reason':      None,
        'is_fuzzy_rescued': bool(identifier and ',' in identifier),
        'source_tab':       source_tab,
        'source_sheet_id':  source_sheet_id,
    }


def _row_to_record_8col(row, source_tab, source_sheet_id):
    row = list(row) + [None] * max(0, 8 - len(row))
    return {
        'original_id':      _int(row[0]),
        'transaction_date': _s(row[1]),
        'transaction_day':  _parse_day(row[1]),
        'posting_date':     None,
        'bank':             _s(row[2]) or 'UNKNOWN',
        'description':      str(row[3] or ''),
        'credit_amount':    _num(row[4]),
        'identifier':       _s(row[5]),
        'customer_name':    None,
        'ref_number':       _s(row[7]),
        'customer_id':      None,
        'fail_reason':      _s(row[6]),
        'is_fuzzy_rescued': False,
        'source_tab':       source_tab,
        'source_sheet_id':  source_sheet_id,
    }


def append(logical_tab, sheet_ids, rows):
    """
    Mirror rows into Supabase's `transactions` table.

    logical_tab: the same string app.py passes to append_to_sheet
                 ('PASSED', 'PASSED_NMB', 'BANK_FAILED', …). Translated
                 via _TAB_RENAME to the business-friendly source_tab
                 column value (e.g. 'CRDBPASSED').
    sheet_ids:   IGNORED — kept in the signature for backwards compat.
                 source_sheet_id now comes from _TAB_TO_BANK instead.
    rows:        list of lists — the exact row payload app.py built.

    Silently drops the mirror if logical_tab is not in _TAB_RENAME
    (e.g. the decommissioned _OLD NMB tabs).
    """
    if not ENABLED or not SUPABASE_URL or not SUPABASE_KEY:
        return
    if not rows:
        return

    new_source_tab = _TAB_RENAME.get(logical_tab)
    if not new_source_tab:
        return  # Unknown / deprecated tab — skip mirror

    source_sheet_id = _TAB_TO_BANK.get(logical_tab, '')

    try:
        if logical_tab in _FAILED_TABS:
            records = [_row_to_record_8col(r, new_source_tab, source_sheet_id) for r in rows]
        else:
            records = [_row_to_record_9col(r, new_source_tab, source_sheet_id) for r in rows]

        # No on_conflict — PostgREST needs a non-partial unique constraint
        # to accept ON CONFLICT (ref_number), and ours is partial (excludes
        # NULL / empty ref). App-side dedup on ref_number prevents duplicate
        # writes; the partial UNIQUE is a hard backstop that will 409 any
        # duplicate that slips through, which we log below.
        r = requests.post(
            f'{SUPABASE_URL}/rest/v1/transactions',
            headers=_HEADERS,
            json=records,
            timeout=15,
        )
        if not r.ok:
            print(f'  ⚠️ Supabase mirror {new_source_tab} → {r.status_code}: {r.text[:200]}')
        else:
            print(f'  📡 Supabase mirror: {len(records)} rows → {new_source_tab}')
    except Exception as e:
        print(f'  ⚠️ Supabase mirror exception ({new_source_tab}): {e}')
        traceback.print_exc()
