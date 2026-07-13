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

ILIYOPATA_TAB = 'ILIYOPATAAUTO'


def _service():
    creds_raw = os.environ.get('GOOGLE_CREDENTIALS_JSON') or ''
    if not creds_raw:
        raise RuntimeError('GOOGLE_CREDENTIALS_JSON not set')
    creds_dict = json.loads(creds_raw)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES,
    )
    return build('sheets', 'v4', credentials=creds)


def _next_id(service, sheet_id):
    """Read column A of the ILIYOPATA tab (UNFORMATTED_VALUE so numeric
    cells come back as numbers regardless of any date-format display),
    take the largest integer, add 1. Starts at 1 if the tab is empty."""
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"'{ILIYOPATA_TAB}'!A:A",
            valueRenderOption='UNFORMATTED_VALUE',
        ).execute()
    except Exception:
        return 1
    values = resp.get('values', [])
    biggest = 0
    for row in values[1:]:  # skip header
        if not row:
            continue
        try:
            v = int(row[0])
            if v > biggest:
                biggest = v
        except (ValueError, TypeError):
            continue
    return biggest + 1


def append_iliyopata_row(*, origin_source_tab, tx, customer, new_date_text):
    """Append one row to the ILIYOPATA tab of the correct bank sheet.

    Args:
      origin_source_tab: the source_tab of the FAILED row we just rescued
                        (CRDBFAILED | NMBFAILED | IPHONEFAILED).
      tx: dict from Supabase with description, credit_amount, identifier,
          ref_number, customer_id (may be None).
      customer: dict from customers with name, plate (used as the row's
                identifier if the tx didn't carry one).
      new_date_text: the DD.MM.YYYY HH:MM:SS string we just stamped into
                     transactions.transaction_date.

    Returns: {'ok': True, 'sheet': 'CRDB', 'appended_id': N} on success.
             {'ok': False, 'error': '…'} on any Google error — DB write
             already succeeded so this is purely observability.
    """
    binding = _ORIGIN_TO_SHEET.get(origin_source_tab)
    if not binding:
        return {'ok': False, 'error': f'unknown origin {origin_source_tab}'}
    bank_label, sheet_id = binding

    try:
        service = _service()
        next_id = _next_id(service, sheet_id)
        row = [
            next_id,                                          # A id
            new_date_text or '',                              # B date+time
            bank_label,                                       # C bank
            tx.get('description') or '',                      # D description
            tx.get('credit_amount') if tx.get('credit_amount') is not None else '',
                                                              # E amount
            tx.get('identifier') or customer.get('plate') or '',  # F plate/id
            customer.get('name') or '',                       # G customer
            tx.get('ref_number') or '',                       # H ref
            tx.get('customer_id') or customer.get('customer_id') or '',  # I cust_id
        ]
        service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=f"'{ILIYOPATA_TAB}'!A:I",
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body={'values': [row]},
        ).execute()
        return {'ok': True, 'sheet': bank_label, 'appended_id': next_id}
    except Exception as e:
        traceback.print_exc()
        return {'ok': False, 'error': str(e)[:200]}
