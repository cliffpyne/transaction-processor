"""
iliyopata_writer.py — mirror rescued rows into an ILIYOPATA tab on the
appropriate bank sheet (CRDB / NMB / iPhone).

Contract:
  - Both `/api/sms-rescue` (app.py) and `/api/transactions/<id>/rescue`
    (ui_blueprint.py) call `append_iliyopata_row()` after they've PATCHed
    the DB row. Sheet write is best-effort — a Google API failure is
    logged but never raises, because the DB is already the source of
    truth for the rescue.

  - The original FAILED row on BANK_FAILED stays in place so the audit
    trail is preserved; ILIYOPATA is an append-only view of rescues.

Row layout mirrors the 9-column PASSED variant so tooling downstream
(reconciliation, invoice-processor) can read either tab with the same
schema:

  A = id (auto-increment per ILIYOPATA tab)
  B = new transaction_date  (DD.MM.YYYY HH:MM:SS, stamped at rescue)
  C = bank                  (CRDB | NMB | IPHONE)
  D = description           (original)
  E = credit_amount
  F = identifier / plate    (the picked customer's plate/phone)
  G = customer_name         (the picked customer's name)
  H = ref_number
  I = customer_id           (SAV customer_id if any)
"""

import json
import os
import traceback

from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Same IDs as app.py's constants — kept in sync manually. If either
# changes, both files need updating.
PASSED_SHEET_ID = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'  # CRDB
IPHONE_SHEET_ID = '1Y2cOyObQvP502kvEbC-uGDP-3Sf5X9JKnDDYmR0BPRQ'
NMB_SHEET_ID    = '1YchOygtfVyVNgz37sGX_KKud_Wr9KQsIkQKn_tEdbek'

# The origin source_tab (before rescue) tells us which bank sheet to write
# the ILIYOPATA row into. IPHONE gets its own ILIYOPATA tab even though it
# rides on CRDB rails, because the iPhone workflow already has its own tab
# family — a rescued iPhone row belongs with other iPhone data.
_ORIGIN_TO_SHEET = {
    'CRDBFAILED':   ('CRDB',   PASSED_SHEET_ID),
    'NMBFAILED':    ('NMB',    NMB_SHEET_ID),
    'IPHONEFAILED': ('IPHONE', IPHONE_SHEET_ID),
}

# PASSED tab per (origin bank, customer source_tab). SAVCOM customers
# have their own dedicated passed tab per bank sheet — PASSED_SAV on
# CRDB, PASSED_SAV_NMB on NMB. Rescues for SAVCOM customers must land
# there, not in the regular PASSED tab, or accounting can't tell BODA
# vs SAVCOM apart.
_PASSED_TAB_MAP = {
    ('CRDB',   'BODA_RECORDS'):   'PASSED',
    ('CRDB',   'SAVCOM_RECORDS'): 'PASSED_SAV',
    ('NMB',    'BODA_RECORDS'):   'PASSED',
    ('NMB',    'SAVCOM_RECORDS'): 'PASSED_SAV_NMB',
    ('IPHONE', 'IPHONE_RECORDS'): 'BANK_PASSED',
}


def _passed_tab_for(bank_label: str, customer_source_tab: str | None) -> str | None:
    """Return the PASSED tab name for this (bank, customer) combo,
    or None if we can't classify (row still lands in DB + ILIYOPATA;
    just no PASSED mirror). Fallback for a BODA-labelled customer
    with an odd origin uses the regular PASSED tab."""
    src = customer_source_tab or 'BODA_RECORDS'
    return _PASSED_TAB_MAP.get((bank_label, src)) \
        or _PASSED_TAB_MAP.get((bank_label, 'BODA_RECORDS'))

# FAILED tab per bank sheet — where the row still sits after rescue. We
# stamp column I on that row so accounting can see at a glance which rows
# have been rescued.
_FAILED_TAB = {
    'CRDB':   'FAILED',
    'NMB':    'FAILED_NMB',
    'IPHONE': 'BANK_FAILED',
}

ILIYOPATA_TAB = 'ILIYOPATAAUTO'
# Column letter to stamp the rescue marker into on the FAILED row.
# FAILED rows use A..H; I is unused → free for the marker.
FAILED_MARKER_COL = 'I'


def _service():
    creds_raw = os.environ.get('GOOGLE_CREDENTIALS_JSON') or ''
    if not creds_raw:
        raise RuntimeError('GOOGLE_CREDENTIALS_JSON not set')
    creds_dict = json.loads(creds_raw)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES,
    )
    return build('sheets', 'v4', credentials=creds)


def _scan_tab(service, sheet_id):
    """Read A:I of the ILIYOPATA tab and figure out two things:

      - biggest_id  = largest integer found in ANY of columns A..I (some
                      old rows have the id in column E because of a
                      historical Sheets append() misalignment on NMB —
                      we treat any integer that looks like an id as one
                      so we don't reuse it).
      - next_row    = 1-based row number of the first fully-empty row,
                      so the next update lands there without gaps and
                      without touching existing data.

    No header assumption — the ILIYOPATA tabs don't have a header row,
    so we scan every row starting from row 1.

    Falls back to (biggest_id=0, next_row=1) if the read fails.
    """
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"'{ILIYOPATA_TAB}'!A:I",
            valueRenderOption='UNFORMATTED_VALUE',
        ).execute()
    except Exception:
        return 0, 1
    values = resp.get('values', [])
    biggest = 0
    last_used_row = 0  # 1-based row number of the last row with any data
    for i, row in enumerate(values, start=1):
        if any(str(c).strip() for c in row):
            last_used_row = i
        for cell in row:
            try:
                v = int(cell)
            except (ValueError, TypeError):
                continue
            # Only integers in a plausible id range — skip amounts / phone
            # numbers / anything huge. Ids stay well under 100k.
            if 0 < v < 100_000 and v > biggest:
                biggest = v
    return biggest, last_used_row + 1


def _mark_failed_row_rescued(service, sheet_id, failed_tab, ref, marker_text):
    """Stamp the rescue marker into column I of the FAILED row that
    matches `ref`. FAILED rows use A..H, so I is free.

    Ref match is case-insensitive because bank refs are alphanumeric and
    customer texts sometimes ALL-CAPS them. Returns {'ok': True, 'row': N}
    on hit, {'ok': False, ...} if the ref isn't found (row already gone
    or ref differs).
    """
    try:
        r = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"'{failed_tab}'!H:H",
            valueRenderOption='UNFORMATTED_VALUE',
        ).execute()
    except Exception as e:
        return {'ok': False, 'error': f'read_failed: {str(e)[:120]}'}
    needle = (ref or '').strip().lower()
    if not needle:
        return {'ok': False, 'error': 'empty_ref'}
    for i, row in enumerate(r.get('values', []), start=1):
        if not row:
            continue
        if str(row[0]).strip().lower() == needle:
            try:
                service.spreadsheets().values().update(
                    spreadsheetId=sheet_id,
                    range=f"'{failed_tab}'!{FAILED_MARKER_COL}{i}",
                    valueInputOption='USER_ENTERED',
                    body={'values': [[marker_text]]},
                ).execute()
                return {'ok': True, 'row': i}
            except Exception as e:
                return {'ok': False, 'error': f'update_failed: {str(e)[:120]}'}
    return {'ok': False, 'error': 'ref_not_in_failed_tab'}


def _passed_last_id(service, sheet_id, passed_tab):
    """Largest integer in column A of the PASSED tab, or 0 if empty/unreadable."""
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"'{passed_tab}'!A:A",
            valueRenderOption='UNFORMATTED_VALUE',
        ).execute()
    except Exception:
        return 0
    biggest = 0
    for row in resp.get('values', []):
        if not row:
            continue
        try:
            v = int(row[0])
            if v > biggest:
                biggest = v
        except (ValueError, TypeError):
            continue
    return biggest


def append_iliyopata_row(*, origin_source_tab, tx, customer, new_date_text):
    """Mirror a rescued row into TWO tabs on the bank's Google Sheet:

      1. ILIYOPATAAUTO — audit trail of every rescue (9 cols with customer_id)
      2. PASSED (or BANK_PASSED for iPhone) — the same row makes it into
         the primary success tab too (8 cols, no customer_id) so downstream
         tooling that only reads PASSED sees rescued transactions as
         successful. The row uses the ILIYOPATA timestamp (the exact time
         of rescue) — not the original bank timestamp.

    The ILIYOPATA write uses values.update() with an explicit A{n}:I{n}
    range instead of values.append('A:I') because on the NMB sheet the
    append+INSERT_ROWS behaviour was mis-detecting the table and inserting
    rows shifted 4 columns to the right. Explicit-range update bypasses
    Sheets' table-detection entirely.

    Args:
      origin_source_tab: the source_tab of the FAILED row we just rescued
                        (CRDBFAILED | NMBFAILED | IPHONEFAILED).
      tx: dict from Supabase with description, credit_amount, identifier,
          ref_number, customer_id (may be None).
      customer: dict from customers with name, plate (used as the row's
                identifier if the tx didn't carry one).
      new_date_text: the DD.MM.YYYY HH:MM:SS string we just stamped into
                     transactions.transaction_date.

    Returns: {'ok': True, 'sheet': 'CRDB', 'appended_id': N,
              'passed_id': M} on success.
             {'ok': False, 'error': '…'} on any Google error — DB write
             already succeeded so this is purely observability.
    """
    binding = _ORIGIN_TO_SHEET.get(origin_source_tab)
    if not binding:
        return {'ok': False, 'error': f'unknown origin {origin_source_tab}'}
    bank_label, sheet_id = binding
    passed_tab = _passed_tab_for(bank_label, customer.get('source_tab'))

    try:
        service = _service()
        biggest_id, next_row = _scan_tab(service, sheet_id)
        next_id = biggest_id + 1

        # ILIYOPATA 9-col row — with customer_id
        ily_row = [
            next_id,
            new_date_text or '',
            bank_label,
            tx.get('description') or '',
            tx.get('credit_amount') if tx.get('credit_amount') is not None else '',
            tx.get('identifier') or customer.get('plate') or '',
            customer.get('name') or '',
            tx.get('ref_number') or '',
            tx.get('customer_id') or customer.get('customer_id') or '',
        ]
        # Explicit-range update — writes exactly at A{n}:I{n}, no table detection.
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"'{ILIYOPATA_TAB}'!A{next_row}:I{next_row}",
            valueInputOption='USER_ENTERED',
            body={'values': [ily_row]},
        ).execute()

        # PASSED 8-col row — same data minus customer_id.
        passed_id = None
        passed_err = None
        if passed_tab:
            try:
                passed_id = _passed_last_id(service, sheet_id, passed_tab) + 1
                passed_row = [
                    passed_id,
                    new_date_text or '',
                    bank_label,
                    tx.get('description') or '',
                    tx.get('credit_amount') if tx.get('credit_amount') is not None else '',
                    tx.get('identifier') or customer.get('plate') or '',
                    customer.get('name') or '',
                    tx.get('ref_number') or '',
                ]
                # append() is fine on PASSED — those tabs have thousands of
                # rows and Sheets' table detection works correctly on them.
                service.spreadsheets().values().append(
                    spreadsheetId=sheet_id,
                    range=f"'{passed_tab}'!A:H",
                    valueInputOption='USER_ENTERED',
                    insertDataOption='INSERT_ROWS',
                    body={'values': [passed_row]},
                ).execute()
            except Exception as e:
                # PASSED write is a secondary mirror — log but do not fail
                # the whole call. ILIYOPATA already succeeded above.
                traceback.print_exc()
                passed_err = str(e)[:200]

        # Stamp the source FAILED row so accounting can see rescued rows
        # in-tab without cross-checking ILIYOPATA. Best-effort — a failure
        # here just means no marker; the DB and other sheet writes are
        # already durable.
        failed_tab = _FAILED_TAB.get(bank_label)
        marker_result = None
        if failed_tab:
            ref = tx.get('ref_number') or ''
            marker_text = f"RESCUED @ {new_date_text or ''}".strip()
            marker_result = _mark_failed_row_rescued(
                service, sheet_id, failed_tab, ref, marker_text,
            )

        return {
            'ok': True,
            'sheet': bank_label,
            'appended_id': next_id,
            'passed_id': passed_id,
            'passed_err': passed_err,
            'failed_marker': marker_result,
        }
    except Exception as e:
        traceback.print_exc()
        return {'ok': False, 'error': str(e)[:200]}
