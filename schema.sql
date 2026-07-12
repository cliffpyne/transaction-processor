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

-- Deliberately NOT adding a UNIQUE on ref_number yet — we run the backfill
-- FIRST, then query for historical dedup leaks, then add it once cleaned:
--
--   CREATE UNIQUE INDEX ux_tx_ref_unique ON transactions(ref_number)
--     WHERE ref_number IS NOT NULL AND ref_number <> '';


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


-- ── v_customers_enriched ──────────────────────────────────────────────────
-- LEFT JOINs each customer to the aggregate stats of transactions that
-- match on customer_name (case/space-insensitive) OR customer_id (SAV only).
-- Powers the Customers UI page — one row per customer with:
--   total_paid_tzs, txn_count, last_txn_day, first_txn_day, banks_used
CREATE OR REPLACE VIEW v_customers_enriched AS
WITH tx_by_name AS (
  SELECT
    lower(regexp_replace(coalesce(customer_name, ''), '\s+', ' ', 'g')) AS name_key,
    customer_id,
    SUM(credit_amount)::numeric(14,2)  AS total_paid_tzs,
    COUNT(*)                            AS txn_count,
    MAX(transaction_day)                AS last_txn_day,
    MIN(transaction_day)                AS first_txn_day,
    string_agg(DISTINCT bank, ',' ORDER BY bank) AS banks_used
  FROM transactions
  WHERE source_tab IN ('PASSED', 'PASSED_SAV_NMB', 'PASSED_NMB', 'CRDBPASSED', 'NMBPASSED')
  GROUP BY 1, 2
)
SELECT
  c.id,
  c.name,
  c.phone,
  c.plate,
  c.customer_id,
  c.source_tab,
  c.created_at,
  COALESCE(t.total_paid_tzs, 0)::numeric(14,2) AS total_paid_tzs,
  COALESCE(t.txn_count, 0)                     AS txn_count,
  t.last_txn_day,
  t.first_txn_day,
  t.banks_used
FROM customers c
LEFT JOIN tx_by_name t
  ON t.name_key = lower(regexp_replace(coalesce(c.name, ''), '\s+', ' ', 'g'))
 AND (t.customer_id IS NOT DISTINCT FROM c.customer_id
      OR (t.customer_id IS NULL AND c.customer_id IS NULL));


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
