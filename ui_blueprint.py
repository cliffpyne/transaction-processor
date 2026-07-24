"""
ui_blueprint.py — Flask blueprint for the /records UI + /api/* REST API.

Everything the UI touches goes through here. Auth is enforced via
Flask-Login (session cookies). Roles: admin > editor > viewer.

Table access matrix:
                        view  add  edit  delete
  customers             all   A/E  A/E   admin
  transactions          all   —    —     —
  dedup_alerts          all   —    —     —
  users                 admin admin admin admin
  record_edits (audit)  admin —    —     —
"""

import json
import os
from datetime import datetime, timedelta

import bcrypt
import requests
from flask import (Blueprint, jsonify, redirect, render_template, request,
                   url_for)
from flask_login import current_user, login_required, login_user, logout_user

from auth import User, check_password, require_role

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

_H = {
    'apikey':        SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type':  'application/json',
}

# customer_registry lives in a SEPARATE Supabase project (see the migration
# in scripts/002_customer_registry.sql). Kept isolated from the main
# `customers` helper table so the rescue pipeline stays on the original
# project untouched. Falls back to the main project if the _REGISTRY vars
# aren't set — useful for local dev.
SUPABASE_URL_REGISTRY = (
    os.environ.get('SUPABASE_URL_REGISTRY', SUPABASE_URL).rstrip('/')
)
SUPABASE_KEY_REGISTRY = (
    os.environ.get('SUPABASE_SERVICE_KEY_REGISTRY', SUPABASE_KEY)
)
_H_REGISTRY = {
    'apikey':        SUPABASE_KEY_REGISTRY,
    'Authorization': f'Bearer {SUPABASE_KEY_REGISTRY}',
    'Content-Type':  'application/json',
}


ui = Blueprint('ui', __name__)


# ── Table config ─────────────────────────────────────────────────────────────
# Tables the UI knows about. `editable` gates PATCH/DELETE via role check.
# `search_cols` are text columns joined with OR when the `search=` param is set.
TABLES = {
    'customers': {
        'columns':     ['id', 'plate', 'phone', 'name', 'customer_id',
                        'source_tab', 'created_at'],
        'search_cols': ['plate', 'phone', 'name', 'customer_id', 'source_tab'],
        'editable':    ['plate', 'phone', 'name', 'customer_id', 'source_tab'],
        'sort_default':'id.desc',
    },
    'transactions': {
        'columns':     ['id', 'original_id', 'transaction_date',
                        'transaction_day', 'posting_date', 'bank',
                        'description', 'credit_amount', 'identifier',
                        'customer_name', 'ref_number', 'customer_id',
                        'fail_reason', 'is_fuzzy_rescued', 'source_tab',
                        'source_sheet_id', 'created_at',
                        'old_transaction_date', 'moved_by_user_id',
                        'moved_by_username', 'moved_at'],
        'search_cols': ['description', 'identifier', 'customer_name',
                        'ref_number', 'source_tab', 'bank'],
        'editable':    [],  # never edited via UI
        'sort_default':'id.desc',
    },
    'dedup_alerts': {
        'columns':     ['id', 'ref_number', 'source_tab', 'description',
                        'caught_at'],
        'search_cols': ['ref_number', 'source_tab', 'description'],
        'editable':    [],
        'sort_default':'id.desc',
    },
    'users': {
        'columns':     ['id', 'username', 'full_name', 'role',
                        'created_at', 'last_login_at'],
        'search_cols': ['username', 'full_name', 'role'],
        'editable':    ['username', 'full_name', 'role'],   # password via separate endpoint
        'sort_default':'id.asc',
    },
    'record_edits': {
        'columns':     ['id', 'username', 'action', 'table_name', 'row_id',
                        'before_json', 'after_json', 'at'],
        'search_cols': ['username', 'action', 'table_name'],
        'editable':    [],
        'sort_default':'id.desc',
    },
    'sms_events': {
        'columns':     ['id', 'sender', 'body', 'received_at', 'processed_at',
                        'http_status', 'outcome', 'extracted_plate',
                        'extracted_ref', 'rescued_row_id',
                        'rescued_source_tab', 'error_detail'],
        'search_cols': ['sender', 'body', 'extracted_plate', 'extracted_ref',
                        'outcome'],
        'editable':    [],
        'sort_default':'processed_at.desc',
    },
}


# ── Auth pages ───────────────────────────────────────────────────────────────
@ui.route('/login', methods=['GET', 'POST'])
def login_page():
    if request.method == 'POST':
        u = (request.form.get('username') or '').strip()
        p = request.form.get('password') or ''
        user = check_password(u, p)
        if user is None:
            return render_template('login.html', error='Invalid username or password'), 401
        login_user(user, remember=True)
        return redirect('/home')
    if current_user.is_authenticated:
        return redirect('/home')
    return render_template('login.html', error=None)


@ui.route('/logout')
def logout_page():
    logout_user()
    return redirect('/login')


# Backwards-compat: any lingering /records bookmarks land on /home.
@ui.route('/records')
@ui.route('/records/<path:sub>')
def _records_compat(sub=None):
    target = '/home' + (('/' + sub) if sub else '') + (request.query_string.decode() and '?' + request.query_string.decode() or '')
    return redirect(target, code=301)


# ── SPA shell ────────────────────────────────────────────────────────────────
_HOME_SUBPAGES = {
    'customers':          'customers_page.html',
    'transactions':       'transactions_page.html',
    'sms':                'sms_events_page.html',
    'customers-registry': 'customers_registry_page.html',
    # dedup_alerts, users, record_edits — added as pages ship
}


@ui.route('/home')
@ui.route('/home/<path:sub>')
@login_required
def home_page(sub=None):
    template = _HOME_SUBPAGES.get((sub or '').strip('/').split('/')[0], 'home.html')
    return render_template(template,
                           username=current_user.username,
                           full_name=current_user.full_name,
                           role=current_user.role)


# ── REST API — generic list endpoint ────────────────────────────────────────
def _paginated_query(table: str, cfg: dict, always_where=None):
    """Turn Tabulator's query params into a PostgREST query.

    `always_where` — optional list of PostgREST filter fragments (e.g.
    'or=(name.not.is.null,plate.not.is.null)') that are ANDed onto every
    request. Used by /api/customers to hide garbage rows the sheet
    import created before we tightened the row validator.
    """
    page = max(1, int(request.args.get('page', 1)))
    size = min(2000, max(1, int(request.args.get('size', 50))))
    offset = (page - 1) * size
    end = offset + size - 1

    # Sort — Tabulator sends sort[0][field] & sort[0][dir]
    sort_field = request.args.get('sort[0][field]')
    sort_dir   = request.args.get('sort[0][dir]', 'asc')
    order = f'{sort_field}.{sort_dir}' if sort_field else cfg['sort_default']

    # Build PostgREST query
    parts = ['select=' + ','.join(cfg['columns']),
             f'order={order}']
    if always_where:
        parts.extend(always_where)

    # Global search — OR across configured text columns using `ilike`
    q = (request.args.get('search') or '').strip()
    if q and cfg['search_cols']:
        escaped = q.replace(',', '').replace('*','%').replace(' ', '%')
        or_terms = ','.join(f'{col}.ilike.*{escaped}*' for col in cfg['search_cols'])
        parts.append(f'or=({or_terms})')

    # Column filters — Tabulator sends filter[0][field] / [type] / [value]
    i = 0
    while True:
        field = request.args.get(f'filter[{i}][field]')
        if not field:
            break
        value = request.args.get(f'filter[{i}][value]', '').strip()
        ftype = request.args.get(f'filter[{i}][type]', 'like')
        if value and field in cfg['columns']:
            if ftype in ('like', 'ilike'):
                parts.append(f'{field}=ilike.*{value.replace("*", "%")}*')
            elif ftype == 'in':
                # `value` is a comma-separated list of exact matches.
                # PostgREST wants field=in.(a,b,c).
                items = ','.join(v.strip() for v in value.split(',') if v.strip())
                if items:
                    parts.append(f'{field}=in.({items})')
            elif ftype in ('gte', 'lte', 'gt', 'lt', 'neq'):
                # Range/comparison — PostgREST: field=<op>.<value>
                parts.append(f'{field}={ftype}.{value}')
            else:
                parts.append(f'{field}=eq.{value}')
        i += 1

    q_string = '&'.join(parts)
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/{table}?{q_string}',
        headers={**_H,
                 'Range-Unit': 'items',
                 'Range':      f'{offset}-{end}',
                 'Prefer':     'count=exact'},
        timeout=30,
    )
    if not r.ok:
        return jsonify({'error': 'query_failed',
                        'status': r.status_code,
                        'body': r.text[:400]}), 500

    total = int(r.headers.get('Content-Range', f'0-0/0').split('/')[-1] or 0)
    last_page = (total + size - 1) // size if size > 0 else 1
    return jsonify({'data': r.json(),
                    'last_row': total,
                    'last_page': max(1, last_page),
                    'total': total})


# ── Audit-log helper ─────────────────────────────────────────────────────────
def _audit(action: str, table_name: str, row_id: int,
           before: dict | None = None, after: dict | None = None):
    try:
        requests.post(
            f'{SUPABASE_URL}/rest/v1/record_edits',
            headers={**_H, 'Prefer': 'return=minimal'},
            json={
                'user_id':     current_user.id,
                'username':    current_user.username,
                'action':      action,
                'table_name':  table_name,
                'row_id':      row_id,
                'before_json': before,
                'after_json':  after,
            },
            timeout=5,
        )
    except Exception:
        pass  # audit is best-effort; never break the primary write


# ── customers CRUD ───────────────────────────────────────────────────────────
# Sheet imports before we tightened row_to_customers produced ~9k Boda rows
# where the only populated field was a placeholder phone of "0". Hide those:
# a real customer must have either a name or a plate. Phone alone doesn't
# count.
_CUSTOMER_VALID_ROW = ['or=(name.not.is.null,plate.not.is.null)']


@ui.route('/api/customers', methods=['GET'])
@login_required
def customers_list():
    return _paginated_query('customers', TABLES['customers'],
                            always_where=_CUSTOMER_VALID_ROW)




@ui.route('/api/customers', methods=['POST'])
@require_role('admin', 'editor')
def customers_create():
    payload = request.get_json(silent=True) or {}
    body = {k: payload.get(k) for k in TABLES['customers']['editable']
            if k in payload}
    r = requests.post(f'{SUPABASE_URL}/rest/v1/customers',
                      headers={**_H, 'Prefer': 'return=representation'},
                      json=body, timeout=15)
    if not r.ok:
        return jsonify({'error': r.text[:400]}), r.status_code
    created = r.json()[0] if r.json() else {}
    _audit('INSERT', 'customers', created.get('id', 0), after=created)
    return jsonify(created), 201


@ui.route('/api/customers/<int:row_id>', methods=['PATCH'])
@require_role('admin', 'editor')
def customers_update(row_id):
    # Fetch current state for audit before/after
    b = requests.get(f'{SUPABASE_URL}/rest/v1/customers?id=eq.{row_id}',
                     headers=_H, timeout=10).json()
    before = b[0] if b else None
    payload = request.get_json(silent=True) or {}
    body = {k: payload.get(k) for k in TABLES['customers']['editable']
            if k in payload}
    r = requests.patch(f'{SUPABASE_URL}/rest/v1/customers?id=eq.{row_id}',
                       headers={**_H, 'Prefer': 'return=representation'},
                       json=body, timeout=15)
    if not r.ok:
        return jsonify({'error': r.text[:400]}), r.status_code
    after = r.json()[0] if r.json() else {}
    _audit('UPDATE', 'customers', row_id, before=before, after=after)
    return jsonify(after)


@ui.route('/api/customers/<int:row_id>', methods=['DELETE'])
@require_role('admin')
def customers_delete(row_id):
    b = requests.get(f'{SUPABASE_URL}/rest/v1/customers?id=eq.{row_id}',
                     headers=_H, timeout=10).json()
    before = b[0] if b else None
    r = requests.delete(f'{SUPABASE_URL}/rest/v1/customers?id=eq.{row_id}',
                        headers={**_H, 'Prefer': 'return=minimal'}, timeout=15)
    if not r.ok:
        return jsonify({'error': r.text[:400]}), r.status_code
    _audit('DELETE', 'customers', row_id, before=before)
    return jsonify({'deleted': True})


# ── transactions (read + rescue) ─────────────────────────────────────────────
# Garbage sheet rows sometimes land with bank='UNKNOWN' (migration default when
# the bank cell is empty). Real transactions are always CRDB or NMB, so hide
# UNKNOWN from the UI feed to keep the table clean.
_TXN_VALID_ROW = ["bank=in.(CRDB,NMB)"]


@ui.route('/api/transactions', methods=['GET'])
@login_required
def transactions_list():
    return _paginated_query('transactions', TABLES['transactions'],
                            always_where=_TXN_VALID_ROW)


# Search customers for the rescue picker. No product filter — an iPhone
# FAILED often turns out to be a Boda customer (they forgot the plate/phone
# in the deposit narration), so officers must be able to pick anyone.
@ui.route('/api/customers/search', methods=['GET'])
@login_required
def customers_search():
    q = (request.args.get('q') or '').strip()
    if not q:
        return jsonify({'data': []})
    # OR across name / phone / plate / customer_id (SAVCOM ID)
    escaped = q.replace(',', '').replace('*', '%').replace(' ', '%')
    or_terms = ','.join(
        f'{col}.ilike.*{escaped}*'
        for col in ('name', 'phone', 'plate', 'customer_id')
    )
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/customers'
        f'?select=id,name,phone,plate,customer_id,source_tab'
        f'&or=({or_terms})'
        f'&or=(name.not.is.null,plate.not.is.null)'  # skip garbage rows
        f'&order=name.asc.nullslast'
        f'&limit=20',
        headers=_H, timeout=15,
    )
    if not r.ok:
        return jsonify({'error': r.text[:400]}), 500
    return jsonify({'data': r.json()})


# Rescue: move a FAILED transaction to *ILIYOPATA, stamping the picked
# customer's name and rewriting transaction_date to now. Any logged-in user
# can do this (branch officers are viewers).
_ILIYOPATA_TARGET = {
    'IPHONE_RECORDS': 'IPHONEILIYOPATA',
    # BODA_RECORDS + SAVCOM_RECORDS both collapse to BODAILIYOPATA — bank
    # column already tells you CRDB vs NMB.
    'BODA_RECORDS':   'BODAILIYOPATA',
    'SAVCOM_RECORDS': 'BODAILIYOPATA',
}
_FAILED_SOURCE_TABS = {'CRDBFAILED', 'NMBFAILED', 'IPHONEFAILED'}


@ui.route('/api/transactions/<int:row_id>/rescue', methods=['POST'])
@login_required
def transactions_rescue(row_id):
    payload = request.get_json(silent=True) or {}
    customer_id = payload.get('customer_id')
    if not customer_id:
        return jsonify({'error': 'customer_id required'}), 400

    # Fetch txn + customer — pull every field the ILIYOPATA sheet append
    # needs so we don't have to re-fetch after PATCH.
    tx_r = requests.get(
        f'{SUPABASE_URL}/rest/v1/transactions?id=eq.{row_id}'
        '&select=id,source_tab,transaction_date,customer_name,ref_number,'
        'bank,description,credit_amount,identifier,customer_id,'
        'rescue_locked_at',
        headers=_H, timeout=15,
    )
    cust_r = requests.get(
        f'{SUPABASE_URL}/rest/v1/customers?id=eq.{customer_id}'
        '&select=id,name,plate,customer_id,source_tab',
        headers=_H, timeout=15,
    )
    if not tx_r.ok or not cust_r.ok:
        return jsonify({'error': 'lookup_failed'}), 500

    tx = (tx_r.json() or [None])[0]
    cust = (cust_r.json() or [None])[0]
    if not tx:
        return jsonify({'error': 'transaction not found'}), 404
    if not cust:
        return jsonify({'error': 'customer not found'}), 404
    if tx.get('rescue_locked_at'):
        return jsonify({'error': 'already_rescued',
                        'rescue_locked_at': tx['rescue_locked_at']}), 409
    if tx['source_tab'] not in _FAILED_SOURCE_TABS:
        return jsonify({'error': 'not a failed row',
                        'current_state': tx['source_tab']}), 409
    target_tab = _ILIYOPATA_TARGET.get(cust['source_tab'])
    if not target_tab:
        return jsonify({'error': 'unknown customer source_tab',
                        'source_tab': cust['source_tab']}), 400

    # Display fields stamped in EAT (Tanzania, UTC+3); timestamptz fields
    # (moved_at, rescue_locked_at) stay in UTC ISO.
    now_utc = datetime.utcnow()
    now_eat = now_utc + timedelta(hours=3)
    now_disp  = now_eat.strftime('%d.%m.%Y %H:%M:%S')
    today_eat = now_eat.strftime('%Y-%m-%d')

    update = {
        'old_transaction_date': tx.get('transaction_date'),
        'transaction_date':     now_disp,
        'transaction_day':      today_eat,
        'customer_name':        cust.get('name'),
        'source_tab':           target_tab,
        'moved_by_user_id':     int(current_user.id),
        'moved_by_username':    current_user.username,
        'moved_at':             now_utc.isoformat() + 'Z',
        'rescue_locked_at':     now_utc.isoformat() + 'Z',
    }
    # Atomic conditional PATCH — only touches the row if it isn't
    # already locked. Simultaneous UI + SMS rescues on the same id
    # can't both succeed; the loser gets 0 rows updated → 409.
    r = requests.patch(
        f'{SUPABASE_URL}/rest/v1/transactions?id=eq.{row_id}'
        '&rescue_locked_at=is.null',
        headers={**_H, 'Prefer': 'return=representation'},
        json=update, timeout=15,
    )
    if not r.ok:
        return jsonify({'error': r.text[:400]}), 500
    after_rows = r.json() or []
    if not after_rows:
        return jsonify({'error': 'already_rescued',
                        'row_id': row_id}), 409
    after = after_rows[0]
    _audit('RESCUE', 'transactions', row_id, before=tx, after=after)

    # Mirror the rescue into the bank sheet's ILIYOPATA tab. Best-effort:
    # a Google API hiccup doesn't roll back the DB write above; we return
    # the sheet result inline so the UI can toast a warning if wanted.
    import iliyopata_writer
    sheet_result = iliyopata_writer.append_iliyopata_row(
        origin_source_tab=tx['source_tab'],
        tx=tx,
        customer=cust,
        new_date_text=now_disp,
    )
    if isinstance(after, dict):
        after['sheet'] = sheet_result
    return jsonify(after)


# ── dedup_alerts (read-only) ─────────────────────────────────────────────────
@ui.route('/api/dedup_alerts', methods=['GET'])
@login_required
def dedup_alerts_list():
    return _paginated_query('dedup_alerts', TABLES['dedup_alerts'])


# ── customer_registry (authoritative customer records, separate project) ────
# Talks to SUPABASE_URL_REGISTRY / SUPABASE_SERVICE_KEY_REGISTRY, not the
# main project. Kept off _paginated_query on purpose — that helper is
# hard-wired to the main SUPABASE_URL, and adding a project arg to it just
# for one table would ripple through every existing endpoint.
_REGISTRY_EDITABLE_COLS = [
    'customer_name', 'plate', 'phone', 'bank_account_name',
    'start_date', 'loan_amount_tsh', 'customer_type',
    'sav_customer_id', 'notes',
]


def _registry_pick(payload: dict) -> dict:
    """Whitelist + light coerce for POST/PATCH bodies."""
    body: dict = {}
    for k in _REGISTRY_EDITABLE_COLS:
        if k not in payload:
            continue
        v = payload.get(k)
        if isinstance(v, str):
            v = v.strip() or None
        body[k] = v
    return body


@ui.route('/api/customer_registry', methods=['GET'])
@login_required
def customer_registry_list():
    """Paginated list with optional filters:
       ?page=1&size=25&search=...&customer_type=boda
       search is OR'd across customer_name, plate, phone, bank_account_name."""
    try:
        page = max(1, int(request.args.get('page', 1)))
        size = max(1, min(1000, int(request.args.get('size', 25))))
    except ValueError:
        return jsonify({'error': 'page/size must be integers'}), 400
    lower = (page - 1) * size
    upper = lower + size - 1

    params: list[tuple[str, str]] = [('select', '*'),
                                     ('order', 'id.desc')]

    ctype = (request.args.get('customer_type') or '').strip()
    if ctype in ('boda', 'savcom', 'iphone'):
        params.append(('customer_type', f'eq.{ctype}'))

    search = (request.args.get('search') or '').strip()
    if search:
        # PostgREST OR filter across the 4 text columns worth searching.
        # % as wildcard so partial matches work.
        pattern = search.replace(',', '')
        or_terms = ','.join([
            f'customer_name.ilike.*{pattern}*',
            f'plate.ilike.*{pattern}*',
            f'phone.ilike.*{pattern}*',
            f'bank_account_name.ilike.*{pattern}*',
        ])
        params.append(('or', f'({or_terms})'))

    try:
        r = requests.get(
            f'{SUPABASE_URL_REGISTRY}/rest/v1/customer_registry',
            params=params,
            headers={**_H_REGISTRY,
                     'Range-Unit': 'items',
                     'Range': f'{lower}-{upper}',
                     'Prefer': 'count=exact'},
            timeout=15,
        )
    except requests.RequestException as e:
        return jsonify({'error': f'registry unreachable: {e}'}), 502
    if r.status_code not in (200, 206):
        return jsonify({'error': r.text[:400]}), r.status_code

    total = 0
    cr = r.headers.get('content-range', '')
    if '/' in cr:
        try:
            total = int(cr.split('/')[-1])
        except ValueError:
            total = 0
    last_page = max(1, (total + size - 1) // size)
    return jsonify({
        'data':      r.json(),
        'total':     total,
        'page':      page,
        'size':      size,
        'last_page': last_page,
    })


@ui.route('/api/customer_registry', methods=['POST'])
@require_role('admin', 'editor')
def customer_registry_create():
    payload = request.get_json(silent=True) or {}
    body = _registry_pick(payload)
    if not body.get('customer_name'):
        return jsonify({'error': 'customer_name is required'}), 400
    body.setdefault('customer_type', 'boda')
    body['created_by'] = (
        current_user.username if getattr(current_user, 'is_authenticated', False)
        else None
    )
    try:
        r = requests.post(
            f'{SUPABASE_URL_REGISTRY}/rest/v1/customer_registry',
            headers={**_H_REGISTRY, 'Prefer': 'return=representation'},
            json=body, timeout=15,
        )
    except requests.RequestException as e:
        return jsonify({'error': f'registry unreachable: {e}'}), 502
    if not r.ok:
        return jsonify({'error': r.text[:400]}), r.status_code
    created = r.json()[0] if r.json() else {}
    return jsonify(created), 201


@ui.route('/api/customer_registry/<int:row_id>', methods=['PATCH'])
@require_role('admin', 'editor')
def customer_registry_update(row_id):
    payload = request.get_json(silent=True) or {}
    body = _registry_pick(payload)
    if not body:
        return jsonify({'error': 'no editable fields in payload'}), 400
    try:
        r = requests.patch(
            f'{SUPABASE_URL_REGISTRY}/rest/v1/customer_registry?id=eq.{row_id}',
            headers={**_H_REGISTRY, 'Prefer': 'return=representation'},
            json=body, timeout=15,
        )
    except requests.RequestException as e:
        return jsonify({'error': f'registry unreachable: {e}'}), 502
    if not r.ok:
        return jsonify({'error': r.text[:400]}), r.status_code
    after = r.json()[0] if r.json() else {}
    return jsonify(after)


@ui.route('/api/customer_registry/summary', methods=['GET'])
@login_required
def customer_registry_summary():
    """Counts per customer_type for the dashboard cards."""
    stats = {'total': 0, 'boda': 0, 'savcom': 0, 'iphone': 0}
    for key, filt in (
        ('total',  {}),
        ('boda',   {'customer_type': 'eq.boda'}),
        ('savcom', {'customer_type': 'eq.savcom'}),
        ('iphone', {'customer_type': 'eq.iphone'}),
    ):
        params = {'select': 'id', **filt}
        try:
            r = requests.get(
                f'{SUPABASE_URL_REGISTRY}/rest/v1/customer_registry',
                params=params,
                headers={**_H_REGISTRY, 'Range': '0-0',
                         'Prefer': 'count=exact'},
                timeout=10,
            )
            cr = r.headers.get('content-range') or ''
            stats[key] = int(cr.split('/')[-1]) if '/' in cr else 0
        except (requests.RequestException, ValueError):
            pass
    return jsonify(stats)


# ── sms_events (read-only, audit) ────────────────────────────────────────────
@ui.route('/api/sms_events', methods=['GET'])
@login_required
def sms_events_list():
    return _paginated_query('sms_events', TABLES['sms_events'])


@ui.route('/api/sms_events/summary', methods=['GET'])
@login_required
def sms_events_summary():
    """Today's SMS stats for the dashboard cards. Returns:
      {sent, rescued, ref_in_passed, ref_not_found}
    where `sent` is the total count of events processed today (regardless
    of outcome). "Today" is measured in EAT (UTC+3) to match Tanzania
    wall-clock — customers care about their local day, not UTC."""
    import os, requests
    from datetime import datetime, timedelta, timezone
    url = os.environ.get('SUPABASE_URL', '').rstrip('/')
    key = os.environ.get('SUPABASE_SERVICE_KEY', '') \
          or os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
    if not (url and key):
        return jsonify({'error': 'supabase_env_missing'}), 500
    hdr = {'apikey': key, 'Authorization': f'Bearer {key}',
           'Prefer': 'count=exact'}

    # Start of today in EAT = start of today's UTC day - 3h
    eat = timezone(timedelta(hours=3))
    today_start_eat = datetime.now(eat).replace(
        hour=0, minute=0, second=0, microsecond=0)
    since = today_start_eat.astimezone(timezone.utc).strftime(
        '%Y-%m-%dT%H:%M:%S')

    def count(extra_params: dict) -> int:
        params = {'select': 'id',
                  'processed_at': f'gte.{since}'}
        params.update(extra_params)
        try:
            r = requests.get(
                f'{url}/rest/v1/sms_events',
                params=params,
                headers={**hdr, 'Range': '0-0'},
                timeout=15,
            )
            cr = r.headers.get('content-range') or ''
            return int(cr.split('/')[-1]) if '/' in cr else 0
        except Exception:
            return 0

    return jsonify({
        'day_start_eat': today_start_eat.isoformat(),
        'sent':          count({}),
        'rescued':       count({'outcome': 'eq.rescued'}),
        'ref_in_passed': count({'outcome': 'eq.ref_in_passed'}),
        'ref_not_found': count({'outcome': 'eq.ref_not_found'}),
    })


# ── users (admin only) ───────────────────────────────────────────────────────
@ui.route('/api/users', methods=['GET'])
@require_role('admin')
def users_list():
    return _paginated_query('users', TABLES['users'])


@ui.route('/api/users', methods=['POST'])
@require_role('admin')
def users_create():
    payload = request.get_json(silent=True) or {}
    username = (payload.get('username') or '').strip()
    password = payload.get('password') or ''
    full_name = (payload.get('full_name') or '').strip()
    role = payload.get('role', 'viewer')
    if not username or not password or not full_name:
        return jsonify({'error': 'username, password, full_name required'}), 400
    if role not in ('admin', 'editor', 'viewer'):
        return jsonify({'error': 'invalid role'}), 400
    body = {
        'username': username,
        'password_hash': bcrypt.hashpw(password.encode(), bcrypt.gensalt(12)).decode(),
        'full_name': full_name,
        'role': role,
    }
    r = requests.post(f'{SUPABASE_URL}/rest/v1/users',
                      headers={**_H, 'Prefer': 'return=representation'},
                      json=body, timeout=15)
    if not r.ok:
        return jsonify({'error': r.text[:400]}), r.status_code
    created = r.json()[0] if r.json() else {}
    # Never return password_hash
    created.pop('password_hash', None)
    _audit('INSERT', 'users', created.get('id', 0),
           after={k: v for k, v in created.items() if k != 'password_hash'})
    return jsonify(created), 201


@ui.route('/api/users/<int:row_id>', methods=['PATCH'])
@require_role('admin')
def users_update(row_id):
    payload = request.get_json(silent=True) or {}
    body = {}
    for k in ('username', 'full_name', 'role'):
        if k in payload:
            body[k] = payload[k]
    if 'password' in payload and payload['password']:
        body['password_hash'] = bcrypt.hashpw(
            payload['password'].encode(), bcrypt.gensalt(12)
        ).decode()
    if body.get('role') and body['role'] not in ('admin', 'editor', 'viewer'):
        return jsonify({'error': 'invalid role'}), 400

    b = requests.get(f'{SUPABASE_URL}/rest/v1/users?id=eq.{row_id}',
                     headers=_H, timeout=10).json()
    before = b[0] if b else None
    r = requests.patch(f'{SUPABASE_URL}/rest/v1/users?id=eq.{row_id}',
                       headers={**_H, 'Prefer': 'return=representation'},
                       json=body, timeout=15)
    if not r.ok:
        return jsonify({'error': r.text[:400]}), r.status_code
    after = r.json()[0] if r.json() else {}
    if before: before.pop('password_hash', None)
    after.pop('password_hash', None)
    _audit('UPDATE', 'users', row_id, before=before, after=after)
    return jsonify(after)


@ui.route('/api/users/<int:row_id>', methods=['DELETE'])
@require_role('admin')
def users_delete(row_id):
    if row_id == current_user.id:
        return jsonify({'error': "can't delete your own account"}), 400
    b = requests.get(f'{SUPABASE_URL}/rest/v1/users?id=eq.{row_id}',
                     headers=_H, timeout=10).json()
    before = b[0] if b else None
    if before: before.pop('password_hash', None)
    r = requests.delete(f'{SUPABASE_URL}/rest/v1/users?id=eq.{row_id}',
                        headers={**_H, 'Prefer': 'return=minimal'}, timeout=15)
    if not r.ok:
        return jsonify({'error': r.text[:400]}), r.status_code
    _audit('DELETE', 'users', row_id, before=before)
    return jsonify({'deleted': True})


# ── audit log (admin only) ───────────────────────────────────────────────────
@ui.route('/api/record_edits', methods=['GET'])
@require_role('admin')
def audit_list():
    return _paginated_query('record_edits', TABLES['record_edits'])


# ── Session identity for the client so it can hide admin-only nav ────────────
@ui.route('/api/me', methods=['GET'])
@login_required
def whoami():
    return jsonify({
        'id':        current_user.id,
        'username':  current_user.username,
        'full_name': current_user.full_name,
        'role':      current_user.role,
    })
