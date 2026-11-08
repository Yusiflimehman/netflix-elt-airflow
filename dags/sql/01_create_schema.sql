-- =====================================================================
-- 01_create_schema.sql
-- Idempotent DDL for all layers (safe to run on every DAG run).
--   raw_*   : source data exactly as received (all TEXT), one batch per run
--   stg_*   : typed, trimmed copy of the current batch
--   dim_* / customer / subscription / viewing_activity : normalized model (3NF)
--   dq_*    : data-quality results per batch
-- =====================================================================

PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------------
-- Run context: the Python task writes the current batch_id here so that
-- every SQL file can stay parameter-free.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_run_context (
    batch_id TEXT NOT NULL
);

-- ---------------------------------------------------------------------
-- RAW layer (E + L of ELT): no types, no cleaning, full history kept.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_netflix_subscriptions (
    batch_id              TEXT    NOT NULL,
    loaded_at             TEXT    NOT NULL,
    source_file           TEXT    NOT NULL,
    source_row_number     INTEGER NOT NULL,
    user_id               TEXT,
    subscription_type     TEXT,
    monthly_revenue       TEXT,
    join_date             TEXT,
    last_payment_date     TEXT,
    country               TEXT,
    age                   TEXT,
    gender                TEXT,
    device                TEXT,
    plan_duration         TEXT,
    active_profiles       TEXT,
    household_profile_ind TEXT,
    movies_watched        TEXT,
    series_watched        TEXT,
    PRIMARY KEY (batch_id, source_row_number)
);

CREATE VIEW IF NOT EXISTS v_raw_current_batch AS
SELECT r.*
FROM raw_netflix_subscriptions r
WHERE r.batch_id = (SELECT batch_id FROM etl_run_context);

-- ---------------------------------------------------------------------
-- STAGING layer (T of ELT, step 1): typed + standardized current batch.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_netflix_subscriptions (
    batch_id              TEXT    NOT NULL,
    user_id               INTEGER,
    subscription_type     TEXT,
    monthly_revenue       NUMERIC,
    join_date             TEXT,      -- ISO yyyy-mm-dd, NULL if unparseable
    last_payment_date     TEXT,      -- ISO yyyy-mm-dd, NULL if unparseable
    country               TEXT,
    age                   INTEGER,
    gender                TEXT,
    device                TEXT,
    plan_duration_months  INTEGER,
    active_profiles       INTEGER,
    household_profile_ind INTEGER,
    movies_watched        INTEGER,
    series_watched        INTEGER
);

-- ---------------------------------------------------------------------
-- NORMALIZED MODEL (3NF)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    country_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_subscription_plan (
    plan_id    INTEGER PRIMARY KEY,
    plan_name  TEXT    NOT NULL UNIQUE,
    tier_order INTEGER NOT NULL UNIQUE      -- 1 = cheapest tier
);

-- Reference data: the allowed plans are a business decision, not data.
INSERT OR IGNORE INTO dim_subscription_plan (plan_id, plan_name, tier_order) VALUES
    (1, 'Basic',    1),
    (2, 'Standard', 2),
    (3, 'Premium',  3);

CREATE TABLE IF NOT EXISTS dim_device (
    device_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    device_name TEXT NOT NULL UNIQUE
);

-- Person-level attributes (who the customer is).
CREATE TABLE IF NOT EXISTS customer (
    customer_id      INTEGER PRIMARY KEY,                -- = source "User ID"
    age              INTEGER CHECK (age IS NULL OR age BETWEEN 18 AND 100),
    gender           TEXT,
    country_id       INTEGER NOT NULL REFERENCES dim_country (country_id),
    last_batch_id    TEXT    NOT NULL
);

-- Contract-level attributes (what the customer bought and pays).
CREATE TABLE IF NOT EXISTS subscription (
    subscription_id       INTEGER PRIMARY KEY,           -- one contract per User ID in the source
    customer_id           INTEGER NOT NULL UNIQUE REFERENCES customer (customer_id),
    plan_id               INTEGER NOT NULL REFERENCES dim_subscription_plan (plan_id),
    monthly_revenue       NUMERIC NOT NULL CHECK (monthly_revenue > 0),
    join_date             TEXT    NOT NULL,
    last_payment_date     TEXT    NOT NULL,
    plan_duration_months  INTEGER NOT NULL CHECK (plan_duration_months > 0),
    active_profiles       INTEGER NOT NULL CHECK (active_profiles >= 1),
    household_profile_ind INTEGER NOT NULL CHECK (household_profile_ind IN (1, 2)),
    last_batch_id         TEXT    NOT NULL,
    CHECK (last_payment_date >= join_date)
);

-- Usage facts (how the subscription is consumed).
CREATE TABLE IF NOT EXISTS viewing_activity (
    subscription_id INTEGER PRIMARY KEY REFERENCES subscription (subscription_id),
    device_id       INTEGER NOT NULL REFERENCES dim_device (device_id),
    movies_watched  INTEGER NOT NULL CHECK (movies_watched >= 0),
    series_watched  INTEGER NOT NULL CHECK (series_watched >= 0),
    last_batch_id   TEXT    NOT NULL
);

-- ---------------------------------------------------------------------
-- DATA QUALITY results (one row per check per batch)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_check_results (
    batch_id    TEXT    NOT NULL,
    check_name  TEXT    NOT NULL,
    severity    TEXT    NOT NULL CHECK (severity IN ('CRITICAL', 'WARNING', 'INFO')),
    failed_rows INTEGER NOT NULL,
    description TEXT    NOT NULL,
    checked_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (batch_id, check_name)
);
