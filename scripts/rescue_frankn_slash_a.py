#!/usr/bin/env python3
"""
rescue_frankn_slash_a.py — one-shot backfill that rescues the 39
CRDBFAILED transactions the old depositor regex missed because their
descriptions had `TO FRANKN/A` (no space between FRANK and N/A).

The regex fix in commit `63e09de` stops the bleeding for new rows;
this script cleans up the historical backlog by mirroring exactly what
`/api/transactions/<id>/rescue` does — but doing it in batch, driven by
the customer_registry (not the customers table), and applying the NEW
depositor extractor.

For each CRDBFAILED row whose description matches the pattern:
  1. Apply the new depositor regex → get depositor_name.
  2. Look up depositor_name (uppercased) in the registry-loaded
     depositor_lookup → get (plate, customer_name).
  3. PATCH the transactions row: source_tab=BODAILIYOPATA,
     customer_name, identifier=plate, rescue_locked_at=now,
     moved_by_username='batch-frankn-rescue'.
  4. Best-effort append into ILIYOPATA_CRDB on the CRDB sheet.

Default is DRY-RUN — prints what it WOULD do. Pass `--live` to actually
mutate.

Environment expected in /home/eleg/transaction-processor/.env:
  SUPABASE_URL, SUPABASE_SERVICE_KEY,
  SUPABASE_URL_REGISTRY, SUPABASE_SERVICE_KEY_REGISTRY,
  GOOGLE_CREDENTIALS_JSON.

Exit codes:
  0 — completed cleanly (dry-run or live)
  1 — configuration error
  2 — Supabase unreachable
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import requests

# Import app modules from the repo root.
_APP_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

import iliyopata_writer  # noqa: E402


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


# Same regex as app.py's _DEPOSITOR_RX (post-fix)
_DEPOSITOR_RX = re.compile(
    r'\bFROM\s+([A-Z][A-Z\s.\'"-]{2,80}?)\s+TO\s+FRANK',
    re.IGNORECASE,
)


def _extract_depositor(desc: str) -> str | None:
    m = _DEPOSITOR_RX.search(str(desc or ''))
    if not m:
        return None
    name = re.sub(r'\s+', ' ', m.group(1)).strip().upper()
    return name or None


def _load_depositor_lookup_from_registry(url: str, key: str) -> dict:
    """Read customer_registry BODA rows with non-null bank_account_name
    and build {UPPER(bank_account_name): (plate, customer_name)}.
    Same shape as app.py's depositor_lookup."""
    h = {'apikey': key, 'Authorization': f'Bearer {key}'}
    out: dict = {}
    step = 1000
    offset = 0
    while True:
        try:
            r = requests.get(
                f'{url}/rest/v1/customer_registry',
                params={
                    'select':            'customer_name,plate,bank_account_name',
                    'customer_type':     'eq.boda',
                    'bank_account_name': 'not.is.null',
                    'order':             'id.asc',
                },
                headers={**h, 'Range-Unit': 'items',
                         'Range': f'{offset}-{offset + step - 1}'},
                timeout=30,
            )
        except requests.RequestException as e:
            print(f'registry fetch failed at {offset}: {e}', file=sys.stderr)
            break
        if r.status_code not in (200, 206):
            print(f'registry HTTP {r.status_code}: {r.text[:200]}',
                  file=sys.stderr)
            break
        rows = r.json() or []
        for row in rows:
            name  = (row.get('customer_name') or '').strip()
            plate = re.sub(r'\s+', '', str(row.get('plate') or '')).upper()
            bank  = (row.get('bank_account_name') or '').strip().upper()
            if bank and plate and name:
                out[bank] = (plate, name)
        if len(rows) < step:
            break
        offset += step
    return out


def _fetch_frankn_failed(url: str, key: str) -> list[dict]:
    """Every CRDBFAILED transaction whose description contains
    `TO FRANKN/A` (no space)."""
    h = {'apikey': key, 'Authorization': f'Bearer {key}'}
    all_rows: list[dict] = []
    step = 500
    offset = 0
    while True:
        try:
            r = requests.get(
                f'{url}/rest/v1/transactions',
                params={
                    'select': ('id,transaction_date,transaction_day,bank,'
                               'source_tab,description,credit_amount,'
                               'ref_number,identifier,customer_name,'
                               'rescue_locked_at'),
                    'source_tab':      'eq.CRDBFAILED',
                    'description':     'ilike.*TO FRANKN/A*',
                    'rescue_locked_at':'is.null',
                    'order':           'transaction_date.desc',
                },
                headers={**h, 'Range-Unit': 'items',
                         'Range': f'{offset}-{offset + step - 1}'},
                timeout=30,
            )
        except requests.RequestException as e:
            print(f'transactions fetch failed at {offset}: {e}', file=sys.stderr)
            return all_rows
        if r.status_code not in (200, 206):
            print(f'transactions HTTP {r.status_code}: {r.text[:200]}',
                  file=sys.stderr)
            return all_rows
        rows = r.json() or []
        all_rows.extend(rows)
        if len(rows) < step:
            break
        offset += step
    return all_rows


def _patch_rescue(url: str, key: str, tx_id: int, update: dict) -> tuple[bool, str]:
    """Conditional PATCH — only if rescue_locked_at IS NULL. Returns
    (ok, message)."""
    h = {'apikey': key, 'Authorization': f'Bearer {key}',
         'Content-Type': 'application/json',
         'Prefer': 'return=representation'}
    try:
        r = requests.patch(
            f'{url}/rest/v1/transactions'
            f'?id=eq.{tx_id}&rescue_locked_at=is.null',
            headers=h, json=update, timeout=15,
        )
    except requests.RequestException as e:
        return False, f'PATCH exception: {e}'
    if not r.ok:
        return False, f'HTTP {r.status_code}: {r.text[:200]}'
    if not (r.json() or []):
        return False, 'already_rescued (0 rows updated)'
    return True, 'ok'


def main() -> int:
    _load_env_file('/home/eleg/transaction-processor/.env')
    _load_env_file('.env')

    live = '--live' in sys.argv

    url_main = os.environ.get('SUPABASE_URL', '').rstrip('/')
    key_main = os.environ.get('SUPABASE_SERVICE_KEY', '')
    url_reg  = os.environ.get('SUPABASE_URL_REGISTRY', '').rstrip('/')
    key_reg  = os.environ.get('SUPABASE_SERVICE_KEY_REGISTRY', '')
    if not (url_main and key_main and url_reg and key_reg):
        print('missing env: need SUPABASE_URL, SUPABASE_SERVICE_KEY, '
              'SUPABASE_URL_REGISTRY, SUPABASE_SERVICE_KEY_REGISTRY',
              file=sys.stderr)
        return 1

    print(f"── mode: {'LIVE — will mutate' if live else 'DRY RUN — no changes'} ──")

    dep = _load_depositor_lookup_from_registry(url_reg, key_reg)
    print(f"depositor_lookup: {len(dep)} entries loaded from registry")

    rows = _fetch_frankn_failed(url_main, key_main)
    print(f"CRDBFAILED rows with TO FRANKN/A: {len(rows)}")

    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    now_eat = now_utc + timedelta(hours=3)
    now_disp  = now_eat.strftime('%d.%m.%Y %H:%M:%S')
    today_eat = now_eat.strftime('%Y-%m-%d')

    resolved: list[dict] = []
    unresolved: list[dict] = []
    for tx in rows:
        name = _extract_depositor(tx['description'])
        if not name:
            unresolved.append({**tx, 'why': 'regex still no match'})
            continue
        hit = dep.get(name)
        if not hit:
            unresolved.append({**tx, 'why': f'depositor "{name}" not in registry.bank_account_name'})
            continue
        plate, customer_name = hit
        resolved.append({
            'id':             tx['id'],
            'transaction_day': tx.get('transaction_day') or '',
            'original_date':  tx.get('transaction_date'),
            'depositor':      name,
            'plate':          plate,
            'customer_name':  customer_name,
            'credit_amount':  tx.get('credit_amount'),
            'ref_number':     tx.get('ref_number'),
            '_tx':            tx,
        })

    print(f"\n── would rescue: {len(resolved)} ──")
    for r in resolved:
        print(f"  #{r['id']}  {r['transaction_day']}  "
              f"{r['depositor'][:35]:<35} → "
              f"{r['plate']:<10} {r['customer_name'][:30]:<30} "
              f"amount={r['credit_amount']}  ref={r['ref_number']}")

    if unresolved:
        print(f"\n── could not resolve: {len(unresolved)} ──")
        for u in unresolved[:20]:
            print(f"  #{u['id']}  {u.get('why','?')}")
        if len(unresolved) > 20:
            print(f"  … and {len(unresolved) - 20} more")

    if not live:
        print("\nDRY RUN complete. Re-run with --live to actually rescue.")
        return 0

    # LIVE: PATCH + iliyopata append
    print("\n── applying rescues ──")
    ok_count = 0
    fail_count = 0
    for r in resolved:
        tx = r['_tx']
        update = {
            'old_transaction_date': tx.get('transaction_date'),
            'transaction_date':     now_disp,
            'transaction_day':      today_eat,
            'customer_name':        r['customer_name'],
            'identifier':           r['plate'],
            'source_tab':           'BODAILIYOPATA',
            'moved_by_username':    'batch-frankn-rescue',
            'moved_at':             now_utc.isoformat().replace('+00:00', 'Z'),
            'rescue_locked_at':     now_utc.isoformat().replace('+00:00', 'Z'),
        }
        ok, msg = _patch_rescue(url_main, key_main, r['id'], update)
        if not ok:
            print(f"  ✗ #{r['id']}: {msg}")
            fail_count += 1
            continue
        # Best-effort ILIYOPATA sheet append
        try:
            sheet_result = iliyopata_writer.append_iliyopata_row(
                origin_source_tab=tx['source_tab'],
                tx=tx,
                customer={
                    'name':  r['customer_name'],
                    'plate': r['plate'],
                    # Customers-table customer_id (numeric SAV id) is not
                    # known here — leave blank; sheet layout tolerates it.
                    'customer_id': '',
                    'source_tab':  'BODA_RECORDS',
                },
                new_date_text=now_disp,
            )
        except Exception as e:
            sheet_result = f'sheet append raised: {e}'
        print(f"  ✓ #{r['id']} → {r['customer_name'][:30]:<30}  sheet={sheet_result}")
        ok_count += 1

    print(f"\n── done: {ok_count} rescued, {fail_count} failed ──")
    return 0


if __name__ == '__main__':
    sys.exit(main())
