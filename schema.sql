-- ─────────────────────────────────────────────────────────────────────────────
-- transaction-processor → Supabase schema
--
-- Paste this into Supabase → SQL Editor → Run.
-- Idempotent — safe to run more than once (uses IF NOT EXISTS everywhere).
--
-- One-liner explanation of every table:
--   transactions  — every row from every PASSED/FAILED/SAV/BANK_* tab, unified
--   customers     — pikipiki records + pikipiki records2 + IPHONE_RECORDS
--   dedup_alerts  — audit trail: fires whenever the app's in-code dedup misses
--                   a duplicate and the DB unique index catches it instead
-- ─────────────────────────────────────────────────────────────────────────────

-- Default all display times to Dar es Salaam. timestamptz still stores UTC
-- internally; this just changes how it renders for humans + how NOW() /
-- CURRENT_DATE resolve.
ALTER DATABASE postgres SET TIMEZONE = 'Africa/Dar_es_Salaam';


-- ── transactions ───────────────────────────────────────────────────────────
-- Column-by-column mapping to the sheet tabs (single writer, so we know
-- every column shape exactly):
--
--   9-col PASSED-variant tabs (PASSED, PASSED_SAV, PASSED_NMB,
--     PASSED_SAV_NMB, PASSED_SAV_NMB_OLD, BANK_PASSED):
--       A→original_id  B→transaction_date  C→bank  D→description
--       E→credit_amount  F→identifier  G→customer_name  H→ref_number
--       I→customer_id (SAV only, else '')
--
--   8-col FAILED-variant tabs (FAILED, FAILED_NMB, FAILED_NMB_OLD,
--     BANK_FAILED):
--       A→original_id  B→transaction_date  C→bank  D→description
--       E→credit_amount  F→identifier  G→fail_reason  H→ref_number
--
--   Fuzzy-rescued rows sit inside the PASSED-variant tabs, distinguished by
--   is_fuzzy_rescued=true (identifier is a comma-joined plate list, name
--   column holds "PLATE=Name, PLATE=Name" pairs).
CREATE TABLE IF NOT EXISTS transactions (
  id                bigserial PRIMARY KEY,
  original_id       integer,                    -- column A on the sheet
  transaction_date  text,                       -- sheet col B verbatim (audit display)
  transaction_day   date,                       -- parsed from transaction_date, for GROUP BY
  posting_date      date,                       -- RESERVED — filled by the invoice-paying app
  bank              text NOT NULL,              -- 'CRDB' | 'NMB'
  description       text,
  credit_amount     numeric(14,2),
  identifier        text,
  customer_name     text,
  ref_number        text,
  customer_id       text,
  fail_reason       text,
  is_fuzzy_rescued  boolean NOT NULL DEFAULT false,
  source_tab        text NOT NULL,              -- 'PASSED' | 'PASSED_SAV_NMB' | 'BANK_FAILED' | …
  source_sheet_id   text NOT NULL,              -- the Google Sheet ID it came from
  created_at        timestamptz NOT NULL DEFAULT now()
);

-- ── Iliyopata (rescued failed rows) ──────────────────────────────────────
-- When an officer picks the correct customer for a *FAILED row, we rewrite
-- transaction_date to now so the downstream invoice processor sees it as a
-- fresh transaction and won't skip it as stale, preserve the original in
-- old_transaction_date, and record who moved it for audit.
ALTER TABLE transactions
  ADD COLUMN IF NOT EXISTS old_transaction_date text,
  ADD COLUMN IF NOT EXISTS moved_by_user_id     bigint,
  ADD COLUMN IF NOT EXISTS moved_by_username    text,
  ADD COLUMN IF NOT EXISTS moved_at             timestamptz,
  -- Non-null value = this FAILED row has already been rescued and can
  -- never be moved to ILIYOPATA again. The rescue endpoints set this
  -- via PATCH ?rescue_locked_at=is.null so the update is atomic —
  -- simultaneous UI + SMS rescues on the same row can't both succeed.
  ADD COLUMN IF NOT EXISTS rescue_locked_at     timestamptz;

CREATE INDEX IF NOT EXISTS idx_tx_moved_at
  ON transactions(moved_at DESC)
  WHERE moved_at IS NOT NULL;


-- Fast dedup lookup (the hot path — fires 288×/day)
CREATE INDEX IF NOT EXISTS idx_tx_ref
  ON transactions(ref_number);

-- Daily reconciliation query: GROUP BY (bank, source_tab, transaction_day)
CREATE INDEX IF NOT EXISTS idx_tx_audit
  ON transactions(bank, source_tab, transaction_day);

-- Recent-activity queries in Supabase Studio
CREATE INDEX IF NOT EXISTS idx_tx_created
  ON transactions(created_at DESC);

-- Idempotency for the migration script + re-runs (source_tab, original_id).
-- Non-partial so PostgREST's ON CONFLICT clause matches it. Rows with
-- NULL original_id are filtered out before writing (headers/blank rows).
CREATE UNIQUE INDEX IF NOT EXISTS ux_tx_source_original
  ON transactions(source_tab, original_id);

-- Partial UNIQUE on ref_number — our primary DB-level dedup guard. NULLs
-- and empty strings are excluded so failed rows (which often have no ref)
-- can still land. Runs in the app path via ?on_conflict=ref_number
-- upserts, so any duplicate write merges instead of failing.
--
-- Before applying, deduplicate existing rows (keep newest per ref_number):
--
--   DELETE FROM transactions t
--    USING transactions t2
--    WHERE t.ref_number = t2.ref_number
--      AND t.ref_number IS NOT NULL
--      AND t.ref_number <> ''
--      AND t.id < t2.id;
CREATE UNIQUE INDEX IF NOT EXISTS ux_tx_ref_unique
  ON transactions(ref_number)
  WHERE ref_number IS NOT NULL AND ref_number <> '';


-- ── customers ──────────────────────────────────────────────────────────────
-- pikipiki records:      col B plate | C name | D phone     (col E ignored)
-- pikipiki records2:     col B plate | C name | D phone | E customer_id
-- IPHONE_RECORDS:        col A name  | B phone1 | C phone2  (2 rows per name)
CREATE TABLE IF NOT EXISTS customers (
  id           bigserial PRIMARY KEY,
  plate        text,                            -- null for iphone customers
  phone        text,
  name         text,
  customer_id  text,                            -- records2 SAV only
  source_tab   text NOT NULL,                   -- 'pikipiki_records' | 'pikipiki_records2' | 'IPHONE_RECORDS'
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cust_plate ON customers(plate);
CREATE INDEX IF NOT EXISTS idx_cust_phone ON customers(phone);
CREATE INDEX IF NOT EXISTS idx_cust_tab   ON customers(source_tab);


-- ── sms_events ─────────────────────────────────────────────────────────────
-- Every /api/sms-rescue call — success or failure — lands here for audit.
-- The mobile app forwards raw SMS bytes; the server extracts + rescues +
-- also logs, so the DB is the single source of truth on what came in.
CREATE TABLE IF NOT EXISTS sms_events (
  id             bigserial PRIMARY KEY,
  sender         text,                   -- sender address the phone saw
  body           text NOT NULL,          -- raw SMS body verbatim
  received_at    timestamptz,            -- when the phone received it
  processed_at   timestamptz NOT NULL DEFAULT now(),
  http_status    integer,                -- 200 / 400 / 404 / 409 / 500
  outcome        text,                   -- rescued | already_rescued |
                                         -- not_a_failed_row | ref_not_found |
                                         -- plate_not_in_records |
                                         -- extract_failed | server_error
  extracted_plate text,
  extracted_ref   text,
  rescued_row_id  bigint,                -- transactions.id if we rescued
  rescued_source_tab text,               -- BODAILIYOPATA | IPHONEILIYOPATA
  error_detail   text                    -- server error text, if any
);

CREATE INDEX IF NOT EXISTS idx_sms_events_processed
  ON sms_events(processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_sms_events_outcome
  ON sms_events(outcome);
CREATE INDEX IF NOT EXISTS idx_sms_events_ref
  ON sms_events(extracted_ref)
  WHERE extracted_ref IS NOT NULL;


-- ── dedup_alerts ───────────────────────────────────────────────────────────
-- Populated by the dual-write code whenever Postgres rejects a duplicate
-- ref_number (once the UNIQUE index is enabled after backfill). Empty =
-- in-code dedup is watertight. Growing = a leak we can fix.
CREATE TABLE IF NOT EXISTS dedup_alerts (
  id           bigserial PRIMARY KEY,
  ref_number   text NOT NULL,
  source_tab   text,
  description  text,
  caught_at    timestamptz NOT NULL DEFAULT now()
);


-- ── Row Level Security ─────────────────────────────────────────────────────
-- We use the Service Role Key from the app — that bypasses RLS entirely.
-- Enabling RLS with no policies locks the anon key out of the data, so if
-- the anon key ever leaks it's still safe.
ALTER TABLE transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE customers    ENABLE ROW LEVEL SECURITY;
ALTER TABLE dedup_alerts ENABLE ROW LEVEL SECURITY;
