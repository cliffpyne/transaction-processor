"""
auth.py — Flask-Login glue backed by the Supabase `users` table.

Auth model:
  - Session cookies (not JWT). Flask-Login handles issuance + validation.
  - Password check via bcrypt against the stored hash.
  - Roles: 'admin' | 'editor' | 'viewer'. Enforced via require_role().
  - login_required + role decorators used by ui_blueprint.py routes.
"""

import functools
import os

import bcrypt
import requests
from flask import jsonify, redirect, request, url_for
from flask_login import LoginManager, UserMixin, current_user

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

_HEADERS = {
    'apikey':        SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type':  'application/json',
}

login_manager = LoginManager()
login_manager.login_view = '/login'


class User(UserMixin):
    """Wraps a single row from the Supabase `users` table."""
    def __init__(self, row: dict):
        self.id            = row['id']
        self.username      = row['username']
        self.full_name     = row.get('full_name', '')
        self.role          = row.get('role', 'viewer')
        self.password_hash = row.get('password_hash', '')

    def get_id(self):
        return str(self.id)


def _fetch_user(*, user_id: int | None = None, username: str | None = None):
    if user_id is None and not username:
        return None
    q = f'id=eq.{user_id}' if user_id is not None else f'username=eq.{username}'
    try:
        r = requests.get(
            f'{SUPABASE_URL}/rest/v1/users?select=id,username,full_name,role,password_hash&{q}&limit=1',
            headers=_HEADERS, timeout=10,
        )
        if not r.ok:
            return None
        rows = r.json()
        return rows[0] if rows else None
    except Exception:
        return None


@login_manager.user_loader
def load_user(user_id: str):
    try:
        row = _fetch_user(user_id=int(user_id))
    except (TypeError, ValueError):
        return None
    return User(row) if row else None


@login_manager.unauthorized_handler
def _unauth():
    # JSON for API routes, redirect for HTML pages
    if request.path.startswith('/api/'):
        return jsonify({'error': 'unauthorized'}), 401
    return redirect('/login')


def check_password(username: str, plain_password: str):
    """Return a User on success, None on any failure. Never leaks WHY."""
    row = _fetch_user(username=username)
    if not row:
        return None
    stored = (row.get('password_hash') or '').encode('utf-8')
    if not stored:
        return None
    try:
        if bcrypt.checkpw(plain_password.encode('utf-8'), stored):
            _mark_login(row['id'])
            return User(row)
    except (ValueError, TypeError):
        return None
    return None


def _mark_login(user_id: int):
    try:
        requests.patch(
            f'{SUPABASE_URL}/rest/v1/users?id=eq.{user_id}',
            headers={**_HEADERS, 'Prefer': 'return=minimal'},
            json={'last_login_at': 'now()'}, timeout=5,
        )
    except Exception:
        pass


def require_role(*allowed: str):
    """Decorator: `@require_role('admin')` or `@require_role('admin','editor')`."""
    def deco(fn):
        @functools.wraps(fn)
        def wrap(*a, **kw):
            if not current_user.is_authenticated:
                return jsonify({'error': 'unauthorized'}), 401
            if current_user.role not in allowed:
                return jsonify({'error': 'forbidden',
                                'need': list(allowed),
                                'have': current_user.role}), 403
            return fn(*a, **kw)
        return wrap
    return deco
