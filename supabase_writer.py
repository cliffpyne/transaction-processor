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

# Sheet-ID resolver — MUST stay in sync with app.py's _resolve_sheet.
# Maps logical tab name → which sheet ID it lives on (looked up in the
# `sheet_ids` dict the caller passes in).
_SHEET_ROUTING = {
    'BANK_PASSED':        'IPHONE',
    'BANK_FAILED':        'IPHONE',
    'PASSED_NMB':         'NMB',
    'PASSED_SAV_NMB':     'NMB',
    'FAILED_NMB':         'NMB',
    'PASSED_SAV_NMB_OLD': 'PASSED',
    'FAILED_NMB_OLD':     'PASSED',
    # anything else → 'PASSED'
}


def _parse_day(s):
    """Best-effort DATE parse from any sheet-date string. Returns
    'YYYY-MM-DD' or None. Same logic as the migration script."""
    if not s:
        return None
    s = str(s).strip()

    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        try: return date(int(m[1]), int(m[2]), int(m[3])).isoformat()
        except ValueError: return None

    m = re.match(r'^(\d{2})\.(\d{2})\.(\d{4})', s)
    if m:
        try: return date(int(m[3]), int(m[2]), int(m[1])).isoformat()
        except ValueError: return None

    m = re.match(r'^(\d{2})/(\d{2})/(\d{4})', s)
    if m:
        try: return date(int(m[3]), int(m[2]), int(m[1])).isoformat()
        except ValueError: return None

    first_token = s.split()[0] if s else ''
    for fmt in ('%d-%b-%Y', '%d-%b-%y'):
        try: return datetime.strptime(first_token, fmt).date().isoformat()
        except ValueError: pass
    try:
        return datetime.strptime(' '.join(s.split()[:3]), '%d %b %Y').date().isoformat()
    except (ValueError, IndexError):
        pass
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
                 ('PASSED', 'PASSED_NMB', 'BANK_FAILED', …). Becomes
                 the row's source_tab column.
    sheet_ids:   {'PASSED': <id>, 'NMB': <id>, 'IPHONE': <id>}
    rows:        list of lists — the exact row payload app.py built.
    """
    if not ENABLED or not SUPABASE_URL or not SUPABASE_KEY:
        return
    if not rows:
        return

    try:
        kind = _SHEET_ROUTING.get(logical_tab, 'PASSED')
        source_sheet_id = sheet_ids.get(kind, '')

        if logical_tab in _FAILED_TABS:
            records = [_row_to_record_8col(r, logical_tab, source_sheet_id) for r in rows]
        else:
            records = [_row_to_record_9col(r, logical_tab, source_sheet_id) for r in rows]

        r = requests.post(
            f'{SUPABASE_URL}/rest/v1/transactions?on_conflict=source_tab,original_id',
            headers=_HEADERS,
            json=records,
            timeout=15,
        )
        if not r.ok:
            print(f'  ⚠️ Supabase mirror {logical_tab} → {r.status_code}: {r.text[:200]}')
        else:
            print(f'  📡 Supabase mirror: {len(records)} rows → {logical_tab}')
    except Exception as e:
        print(f'  ⚠️ Supabase mirror exception ({logical_tab}): {e}')
        traceback.print_exc()
