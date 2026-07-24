-- =============================================================================
-- Migration 003: add customer_registry.phones (text[]) for multi-phone iPhone
--
-- iPhone customers routinely have >1 phone number (2 today, could be more
-- tomorrow). Keeping them as one row per customer with a `phones` array is
-- cleaner than N-rows-per-customer duplication and lets the lookup do a
-- single ANY() match. The singular `phone` column stays and holds the
-- primary/first number so anything already reading it continues to work.
--
-- Run once via the Supabase SQL editor. Idempotent.
-- =============================================================================

ALTER TABLE customer_registry
    ADD COLUMN IF NOT EXISTS phones text[] NOT NULL DEFAULT '{}';

-- GIN index so `phones @> ARRAY['<phone>']` and `<phone> = ANY(phones)` are
-- both fast lookups even at 10k+ rows.
CREATE INDEX IF NOT EXISTS idx_reg_phones_gin
    ON customer_registry USING GIN (phones);

-- Backfill: seed phones[] with the singular phone for every row that already
-- has one. Idempotent — the WHERE clause skips rows where the singular phone
-- is already present in the array (e.g. after a re-run).
UPDATE customer_registry
   SET phones = ARRAY[phone]
 WHERE phone IS NOT NULL
   AND phone <> ''
   AND NOT (phones @> ARRAY[phone]);
