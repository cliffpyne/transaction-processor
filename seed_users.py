#!/usr/bin/env python3
"""
seed_users.py — one-off script that inserts the 4 initial UI accounts
into the `users` table. Idempotent (upserts by username so re-running is
safe if you re-hash a password).

Env:
  SUPABASE_URL           https://<ref>.supabase.co
  SUPABASE_SERVICE_KEY   service_role secret from Supabase → API
"""

import os
import sys
import bcrypt
import requests

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://npornslyozuxxigeoqgi.supabase.co').rstrip('/')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

if not SUPABASE_KEY:
    print('❌ SUPABASE_SERVICE_KEY not set'); sys.exit(1)

USERS = [
    # (username, plain_password, full_name, role)
    ('Fmlaki',    'Mlaki@1234',            'Frank Mlaki',            'admin'),
    ('Cdenis',    '0713227668Cliford_',    'Clifford Dennis',        'admin'),
    ('Ongozi',    'Mlaki@4321',            'Oscar Ngozi',            'editor'),
    ('Elegensky', 'Admin@1234',            'Elegansky Microfinance', 'viewer'),

    # Viewer accounts — username = full name in CAPS, password = first
    # name in CAPS. Same-first-name viewers share the first-name password
    # by design (usernames stay distinct).
    ('NATUJAEL MGONJA',       'NATUJAEL', 'Natujael Mgonja',       'viewer'),
    ('AGRICOLA BODA',         'AGRICOLA', 'Agricola Boda',         'viewer'),
    ('VICTORIA FRANK BODA',   'VICTORIA', 'Victoria Frank Boda',   'viewer'),
    ('MONICA BODA',           'MONICA',   'Monica Boda',           'viewer'),
    ('EDITHA BODA NEW',       'EDITHA',   'Editha Boda New',       'viewer'),
    ('APRUNA THOMAS BODA',    'APRUNA',   'Apruna Thomas Boda',    'viewer'),
    ('EPPIFANY KALUA',        'EPPIFANY', 'Eppifany Kalua',        'viewer'),
    ('MWASITI JUMANNE BODA',  'MWASITI',  'Mwasiti Jumanne Boda',  'viewer'),
    ('MWANZANI SAID',         'MWANZANI', 'Mwanzani Said',         'viewer'),
    ('EDITHA KAMANZI BODA',   'EDITHA',   'Editha Kamanzi Boda',   'viewer'),
]

HEADERS = {
    'apikey':        SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type':  'application/json',
    'Prefer':        'return=representation,resolution=merge-duplicates',
}


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode('utf-8'), bcrypt.gensalt(rounds=12)).decode('utf-8')


def main():
    records = [
        {
            'username':      u,
            'password_hash': hash_password(p),
            'full_name':     n,
            'role':          r,
        }
        for u, p, n, r in USERS
    ]

    print(f'Seeding {len(records)} accounts into {SUPABASE_URL}/users …')
    r = requests.post(
        f'{SUPABASE_URL}/rest/v1/users?on_conflict=username',
        headers=HEADERS, json=records, timeout=60,
    )
    if not r.ok:
        print(f'❌ HTTP {r.status_code}: {r.text[:400]}'); sys.exit(1)

    for u in r.json():
        print(f'  ✅ {u["username"]:12s}  {u["role"]:8s}  ({u["full_name"]})')


if __name__ == '__main__':
    main()
