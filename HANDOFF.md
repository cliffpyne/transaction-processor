# Elegansky Records Console — Backend API Contract

You're building the UI. This document describes the backend the UI must
talk to. Every endpoint, every request shape, every response shape, and
the auth/role requirement. Nothing about *how* to design the UI — that's
the owner's call and he'll give you the visual direction himself.

## Base URL

`https://transactions.eleganskyboda.com`

Local dev: `http://localhost:5000` after `python3 app.py` with the env
vars listed at the bottom.

## Auth

- **Session cookies only.** No JWT. Login sets a Flask-Login session
  cookie via `Set-Cookie: session=...`, the browser sends it back with
  every subsequent request.
- All `/api/*` and `/records*` routes require a valid session.
- If the session is missing / expired:
  - `/api/*` returns `401 {"error":"unauthorized"}` (JSON)
  - `/records*` returns `302 Location: /login` (redirect)
- Roles: `admin` | `editor` | `viewer`. Role is checked per-endpoint (see
  each endpoint below). On role mismatch:
  `403 {"error":"forbidden","need":[...],"have":"<role>"}`

## The pages

Two Jinja-rendered pages. That's it. Everything else is REST.

### `GET /login`

- Renders `templates/login.html`.
- Passes to Jinja: `error` (str or None).
- If `error` is truthy, show it in the UI as an error message.
- If the user is already signed in, redirects to `/records`.

### `POST /login`

Form-encoded body (Content-Type: `application/x-www-form-urlencoded`):

```
username=<str>&password=<str>
```

Both fields required. On success:
- Sets session cookie.
- Redirects `302` to `/records`.

On failure:
- Renders `templates/login.html` with `error="Invalid username or password"`, status `401`.

### `GET /logout`

Clears session, redirects `302` to `/login`. No auth required.

### `GET /records` and `GET /records/<anything>`

- Requires session.
- Renders `templates/records.html`.
- Passes to Jinja: `username` (str), `full_name` (str), `role` (str, one
  of admin/editor/viewer).
- Everything from this point is single-page — no full page reloads. The
  UI you build should fetch data via the REST endpoints below.

## REST — introspection

### `GET /api/me`

Returns the current user. Requires session.

Response 200:
```json
{
  "id": 1,
  "username": "Fmlaki",
  "full_name": "Frank Mlaki",
  "role": "admin"
}
```

Useful if the UI wants to know the role without reading a Jinja var.

## REST — list endpoints (paginated)

Five list endpoints, one per table. Same query-param shape, same
response shape.

### Endpoints

- `GET /api/customers`      — customer records (all roles)
- `GET /api/transactions`   — bank transactions (all roles)
- `GET /api/dedup_alerts`   — dedup catches (all roles)
- `GET /api/users`          — the 30-user team roster (**admin only**)
- `GET /api/record_edits`   — audit log of every write (**admin only**)

### Query parameters (all optional)

| Param | Type | Default | Meaning |
|---|---|---|---|
| `page` | int ≥ 1 | 1 | 1-indexed page number |
| `size` | int 1–200 | 50 | rows per page |
| `sort[0][field]` | str | table-specific | column name to sort by |
| `sort[0][dir]` | `asc` \| `desc` | `asc` | sort direction |
| `filter[i][field]` | str | — | column name to filter (repeat for multiple, `i=0,1,2,...`) |
| `filter[i][value]` | str | — | value to match |
| `filter[i][type]` | `like` \| `ilike` \| `eq` | `like` | filter operator (`like`/`ilike` do case-insensitive substring; `eq` is exact) |
| `search` | str | — | global text search across the table's search columns (see below) |

### Response 200

```json
{
  "data":      [ { ...row1... }, { ...row2... }, ... ],
  "last_row":  12305,
  "last_page": 247,
  "total":     12305
}
```

- `data` is the current page's rows, exactly as they exist in the DB
  (all columns).
- `last_row` and `total` are equal (both hold the total row count
  matching the current filters, ignoring pagination).
- `last_page` = ceil(total / size).

### Error response 500

```json
{ "error": "query_failed", "status": <upstream_code>, "body": "<first 400 chars>" }
```

### Columns available per table

**`customers`** — plate/phone lookup for the transaction router.

| column | type | notes |
|---|---|---|
| `id` | bigint | primary key |
| `plate` | text | may be null (iPhone-only customers have no plate) |
| `phone` | text | may be null |
| `name` | text | |
| `customer_id` | text | non-null only for `SAVCOM_RECORDS` |
| `source_tab` | text | one of `BODA_RECORDS`, `SAVCOM_RECORDS`, `IPHONE_RECORDS` |
| `created_at` | timestamptz | UTC-stored, Africa/Dar_es_Salaam displayed |

Global `search` matches: `plate`, `phone`, `name`, `customer_id`, `source_tab`.
Default sort: `id.desc`.
Editable fields (POST/PATCH body): `plate`, `phone`, `name`, `customer_id`, `source_tab`.

**`transactions`** — 100k+ bank transaction records.

| column | type | notes |
|---|---|---|
| `id` | bigint | primary key |
| `original_id` | int | ID column from the original sheet |
| `transaction_date` | text | exact date string from the sheet (display value) |
| `transaction_day` | date | parsed date for GROUP BY |
| `posting_date` | date | NULL — reserved for a future invoice-paying app |
| `bank` | text | `CRDB` or `NMB` |
| `description` | text | full bank message |
| `credit_amount` | numeric(14,2) | |
| `identifier` | text | phone or plate that was matched |
| `customer_name` | text | may be null on FAILED rows |
| `ref_number` | text | may be null |
| `customer_id` | text | populated only on SAVCOM rows |
| `fail_reason` | text | populated only on FAILED rows |
| `is_fuzzy_rescued` | boolean | true for green-highlighted fuzzy matches |
| `source_tab` | text | one of `CRDBPASSED`, `CRDBSAVCOM`, `CRDBFAILED`, `NMBPASSED`, `NMBSAVCOM`, `NMBFAILED`, `IPHONEPASSED`, `IPHONEFAILED` |
| `source_sheet_id` | text | one of `CRDBBANK`, `NMBBANK`, `IPHONE` |
| `created_at` | timestamptz | |

Global `search` matches: `description`, `identifier`, `customer_name`, `ref_number`, `source_tab`, `bank`.
Default sort: `id.desc`.
**Not editable via API.**

**`dedup_alerts`** — populated when a duplicate ref sneaks past the
app-level dedup. Currently empty; will start filling once a UNIQUE
constraint is added on `ref_number`.

| column | type |
|---|---|
| `id` | bigint |
| `ref_number` | text |
| `source_tab` | text |
| `description` | text |
| `caught_at` | timestamptz |

Search: `ref_number`, `source_tab`, `description`. Sort: `id.desc`. Not editable.

**`users`** — the 30-user team roster (admin only).

| column | type | notes |
|---|---|---|
| `id` | bigint | |
| `username` | text | unique |
| `full_name` | text | |
| `role` | text | `admin` \| `editor` \| `viewer` |
| `created_at` | timestamptz | |
| `last_login_at` | timestamptz | |

`password_hash` is never returned by the API. Search: `username`, `full_name`, `role`. Sort: `id.asc`. Editable: `username`, `full_name`, `role`, and a special `password` field (write-only, server bcrypts it).

**`record_edits`** — full audit log of every INSERT/UPDATE/DELETE that hit the DB via the UI (admin only).

| column | type | notes |
|---|---|---|
| `id` | bigint | |
| `username` | text | who did it |
| `action` | text | `INSERT` \| `UPDATE` \| `DELETE` |
| `table_name` | text | which table |
| `row_id` | bigint | which row |
| `before_json` | jsonb | full row before the change (null on INSERT) |
| `after_json` | jsonb | full row after the change (null on DELETE) |
| `at` | timestamptz | |

Search: `username`, `action`, `table_name`. Sort: `id.desc`. Not editable.

## REST — write endpoints

### customers

- **`POST /api/customers`** (roles: `admin`, `editor`)
  Body (JSON): any subset of editable fields.
  ```json
  { "plate": "MC571FGQ", "phone": "0752…", "name": "Jane Doe",
    "customer_id": "…", "source_tab": "BODA_RECORDS" }
  ```
  Response 201: full row created.
  Server auto-fills `id`, `created_at`.
- **`PATCH /api/customers/<id>`** (roles: `admin`, `editor`)
  Body: subset of editable fields. Only sent fields are updated.
  Response 200: full row after update.
- **`DELETE /api/customers/<id>`** (role: `admin`)
  Response 200: `{"deleted": true}`

### users

- **`POST /api/users`** (role: `admin`)
  Body:
  ```json
  { "username": "…", "password": "…", "full_name": "…", "role": "editor" }
  ```
  All four required. Server bcrypts the password before storing.
  Role must be `admin` / `editor` / `viewer` else 400.
  Response 201: full row (no `password_hash`).
- **`PATCH /api/users/<id>`** (role: `admin`)
  Body: any subset of `username`, `full_name`, `role`. Send a
  `password` field (write-only) to change it — omit to keep existing.
  Response 200: full row after update.
- **`DELETE /api/users/<id>`** (role: `admin`)
  Cannot delete self: returns 400 with `{"error":"can't delete your own account"}`.
  Response 200: `{"deleted": true}`

### Common write-error responses

- `400 {"error":"<explanation>"}` — bad request (missing required, bad role name, self-delete)
- `401 {"error":"unauthorized"}` — no session
- `403 {"error":"forbidden","need":[…],"have":"<role>"}` — role too low
- `500 {"error":"<upstream body first 400 chars>"}` — DB write failed

Every successful write is recorded in `record_edits` with the current
user and the row's before/after JSON. No action needed from the UI.

## Session behaviour to know

- Sessions are cookie-based; the browser handles them automatically for
  same-origin fetches. Use `credentials: "same-origin"` on any `fetch()`
  (that's the default; nothing to configure).
- Session lifetime is Flask's default — a few hours of inactivity, plus
  it's cleared on logout. The UI should catch 401s and redirect to
  `/login`.

## Environment (already set on Render)

- `SUPABASE_URL=https://npornslyozuxxigeoqgi.supabase.co`
- `SUPABASE_SERVICE_KEY=sb_secret_…` (server-only, never exposed to browser)
- `GOOGLE_CREDENTIALS_JSON` — used only by the /admin routes for the migration
- `SECRET_KEY` — Flask session encryption key
- `WRITE_TO_SUPABASE=true`
- `MIGRATION_TOKEN=…` — protects the /admin routes, unrelated to the UI

The UI doesn't read env vars. All values it needs come from Jinja or from `/api/me`.

## Seed accounts already in the DB

| Username | Role |
|---|---|
| Fmlaki | admin |
| Cdenis | admin |
| Ongozi | editor |
| Elegensky | viewer |

Ask the owner for the current passwords.

## Repo

`git@github.com:cliffpyne/transaction-processor.git`, branch `main`.
Any push to `main` auto-deploys to Render.

Contact: the owner. He'll tell you which parts of the UI he wants how.
