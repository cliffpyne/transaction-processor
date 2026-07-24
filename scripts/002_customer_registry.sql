-- =============================================================================
-- Migration 002: customer_registry — authoritative customer records
--
-- This table is the source of truth for who a customer is (name, plate,
-- phone, loan details). It is INTENTIONALLY separate from the existing
-- `customers` table — that one is a rescue-time plate/phone lookup helper
-- populated by scripts/sync_customers_from_sheet.py, whereas this one is
-- what the portal UI creates and edits.
--
-- Run once via the Supabase SQL editor. Idempotent — safe to re-run.
-- =============================================================================

CREATE TABLE IF NOT EXISTS customer_registry (
    id                bigserial PRIMARY KEY,
    customer_name     text NOT NULL,
    plate             text,             -- nullable: iPhone customers have no plate
    phone             text,             -- customer's own phone (255XXXXXXXXX)
    bank_account_name text,             -- name on their bank account, if they deposit
                                        -- using their own name (col D of pikipiki records
                                        -- when it holds a name instead of a phone)
    start_date        date,             -- loan start date; NULL for legacy rows we
                                        -- couldn't determine at backfill time
    loan_amount_tsh   numeric(12, 2),   -- loan principal in TZS; NULL for legacy
    customer_type     text NOT NULL DEFAULT 'boda'
                      CHECK (customer_type IN ('boda', 'savcom', 'iphone')),
    sav_customer_id   text,             -- SAVCOM: customer_id from pikipiki records2 col E
    notes             text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    created_by        text              -- portal username who created (nullable for backfill)
);

CREATE INDEX IF NOT EXISTS idx_reg_plate      ON customer_registry(plate);
CREATE INDEX IF NOT EXISTS idx_reg_phone      ON customer_registry(phone);
CREATE INDEX IF NOT EXISTS idx_reg_type       ON customer_registry(customer_type);
CREATE INDEX IF NOT EXISTS idx_reg_name       ON customer_registry(customer_name);
CREATE INDEX IF NOT EXISTS idx_reg_bank_name  ON customer_registry(bank_account_name)
    WHERE bank_account_name IS NOT NULL;

-- Auto-touch updated_at on any UPDATE
CREATE OR REPLACE FUNCTION touch_customer_registry_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_customer_registry_touch ON customer_registry;
CREATE TRIGGER trg_customer_registry_touch
    BEFORE UPDATE ON customer_registry
    FOR EACH ROW EXECUTE FUNCTION touch_customer_registry_updated_at();

-- RLS on — service_role bypasses it, so the Flask app (which uses
-- SUPABASE_SERVICE_KEY) reads/writes freely. Any future direct-anon
-- access is opt-in via new policies.
ALTER TABLE customer_registry ENABLE ROW LEVEL SECURITY;
