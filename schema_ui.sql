-- ─────────────────────────────────────────────────────────────────────────────
-- UI schema — users + audit log
-- Paste into Supabase SQL Editor, run once. Idempotent.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS users (
  id             bigserial PRIMARY KEY,
  username       text UNIQUE NOT NULL,
  password_hash  text NOT NULL,
  full_name      text NOT NULL,
  role           text NOT NULL CHECK (role IN ('admin','editor','viewer')),
  created_at     timestamptz NOT NULL DEFAULT now(),
  last_login_at  timestamptz
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

-- Audit log — every write from the UI (INSERT / UPDATE / DELETE) lands here
-- with the row's before/after JSON so admins can see who did what.
CREATE TABLE IF NOT EXISTS record_edits (
  id           bigserial PRIMARY KEY,
  user_id      bigint      NOT NULL REFERENCES users(id),
  username     text        NOT NULL,
  action       text        NOT NULL CHECK (action IN ('INSERT','UPDATE','DELETE')),
  table_name   text        NOT NULL,
  row_id       bigint      NOT NULL,
  before_json  jsonb,
  after_json   jsonb,
  at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_edits_user  ON record_edits(user_id);
CREATE INDEX IF NOT EXISTS idx_edits_table ON record_edits(table_name);
CREATE INDEX IF NOT EXISTS idx_edits_at    ON record_edits(at DESC);

-- Enable RLS. The app talks via Service Role Key which bypasses; anon key
-- (if it ever leaks) gets zero access.
ALTER TABLE users        ENABLE ROW LEVEL SECURITY;
ALTER TABLE record_edits ENABLE ROW LEVEL SECURITY;
