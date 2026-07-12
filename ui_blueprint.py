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
from datetime import datetime

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
    'customers':    'customers_page.html',
    'transactions': 'transactions_page.html',
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
@ui.route('/api/transactions', methods=['GET'])
@login_required
def transactions_list():
    return _paginated_query('transactions', TABLES['transactions'])


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

    # Fetch txn + customer in parallel
    tx_r = requests.get(
        f'{SUPABASE_URL}/rest/v1/transactions?id=eq.{row_id}'
        '&select=id,source_tab,transaction_date,customer_name,ref_number',
        headers=_H, timeout=15,
    )
    cust_r = requests.get(
        f'{SUPABASE_URL}/rest/v1/customers?id=eq.{customer_id}'
        '&select=id,name,source_tab',
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
    if tx['source_tab'] not in _FAILED_SOURCE_TABS:
        return jsonify({'error': 'not a failed row',
                        'current_state': tx['source_tab']}), 409
    target_tab = _ILIYOPATA_TARGET.get(cust['source_tab'])
    if not target_tab:
        return jsonify({'error': 'unknown customer source_tab',
                        'source_tab': cust['source_tab']}), 400

    now = datetime.utcnow()
    now_iso   = now.strftime('%Y-%m-%d %H:%M:%S')
    today_iso = now.strftime('%Y-%m-%d')

    update = {
        'old_transaction_date': tx.get('transaction_date'),
        'transaction_date':     now_iso,
        'transaction_day':      today_iso,
        'customer_name':        cust.get('name'),
        'source_tab':           target_tab,
        'moved_by_user_id':     int(current_user.id),
        'moved_by_username':    current_user.username,
        'moved_at':             now.isoformat() + 'Z',
    }
    r = requests.patch(
        f'{SUPABASE_URL}/rest/v1/transactions?id=eq.{row_id}',
        headers={**_H, 'Prefer': 'return=representation'},
        json=update, timeout=15,
    )
    if not r.ok:
        return jsonify({'error': r.text[:400]}), 500
    after = (r.json() or [None])[0]
    _audit('RESCUE', 'transactions', row_id, before=tx, after=after)
    return jsonify(after)


# ── dedup_alerts (read-only) ─────────────────────────────────────────────────
@ui.route('/api/dedup_alerts', methods=['GET'])
@login_required
def dedup_alerts_list():
    return _paginated_query('dedup_alerts', TABLES['dedup_alerts'])


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
