"""
DSSY Neural AI Governance — Neon PostgreSQL Schema Setup
=========================================================
Run this script once to create the full DSSY schema on your Neon database.

Usage:
    python create_neon_schema.py

    It will prompt for the Neon connection URL if not set in the environment.
    Paste your URL in this format:
        postgresql://user:password@host.neon.tech/dbname?sslmode=require

Requirements:
    pip install psycopg2-binary

What this script creates:
    Core dimension tables  : districts, talukas, villages, banks, categories
    Beneficiary table      : beneficiaries (with all demographic + FK columns)
    Payment table          : payments (with direct payment_date column — no separate dates join needed)
    Status history table   : beneficiary_status_history (tracks every status change with timestamp)
    Enrollment table       : scheme_enrollments (enrollment start/end per beneficiary per category)
    Payment summary view   : payment_summary_monthly (materialized-style view for YoY comparisons)
    Registration summary   : beneficiary_registration_summary_monthly (chart-ready trend view)
    Current summary        : beneficiary_summary_current (chart-ready slice-and-dice view)
    Fiscal periods table   : fiscal_periods (April–March fiscal years for govt reporting)
    Audit log table        : audit_log (tracks every INSERT/UPDATE on beneficiaries)
    Officers table         : officers (who approved/processed what — admin performance queries)
    Category amount history: category_amount_history (monthly_amount changes over years)
    Forecast table         : payment_forecasts (stores forecast outputs for minister dashboards)
    NLP query log          : analytics_query_log (stores NL question → SQL/chart trace)
    Saved dashboard state  : dashboard_views (reusable dynamic dashboard definitions)
    RAG tables             : document_chunks (pgvector), conversation_context (multi-turn sessions)

    Seed data              : Districts (North Goa, South Goa), known DSSY categories with amounts,
                             fiscal periods 2018–2030, sample officer roles

All DDL is idempotent — safe to run multiple times.

Running this script will DROP all existing DSSY objects first, then recreate them.
"""

import os
import re
import sys
from pathlib import Path

# Fix Windows console encoding for Unicode characters (arrows, em-dashes, etc.)
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Auto-load .env from project root
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

# ── Dependency check ──────────────────────────────────────────────────────────
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("\n[ERROR] psycopg2 not installed.")
    print("Run:  pip install psycopg2-binary\n")
    sys.exit(1)

# ── Connection URL ─────────────────────────────────────────────────────────────
def get_url() -> str:
    url = os.environ.get("NEON_DATABASE_URL", "").strip().strip('"').strip("'")
    if not url:
        print("[ERROR] NEON_DATABASE_URL not found in .env or environment. Exiting.")
        sys.exit(1)
    print(f"[INFO] Using NEON_DATABASE_URL from .env")
    return url


def fix_url(url: str) -> str:
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if "sslmode" not in url:
        sep = "&" if "?" in url else "?"
        url += sep + "sslmode=require"
    if "connect_timeout" not in url:
        url += "&connect_timeout=15"
    return url


# ── DROP ALL ──────────────────────────────────────────────────────────────────
# Drops everything so we start from a clean slate every time.

DROP_ALL = """
-- Drop materialized views first (they depend on tables)
DROP MATERIALIZED VIEW IF EXISTS payment_summary_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS beneficiary_summary_current CASCADE;
DROP MATERIALIZED VIEW IF EXISTS beneficiary_registration_summary_monthly CASCADE;

-- Drop triggers (must happen before functions)
DROP TRIGGER IF EXISTS trg_categories_updated_at ON categories;
DROP TRIGGER IF EXISTS trg_beneficiaries_updated_at ON beneficiaries;
DROP TRIGGER IF EXISTS trg_dashboard_views_updated_at ON dashboard_views;
DROP TRIGGER IF EXISTS trg_beneficiaries_sync_amount ON beneficiaries;
DROP TRIGGER IF EXISTS trg_payments_assign_fiscal_period ON payments;
DROP TRIGGER IF EXISTS trg_beneficiaries_status_history_insert ON beneficiaries;
DROP TRIGGER IF EXISTS trg_beneficiaries_status_history_update ON beneficiaries;
DROP TRIGGER IF EXISTS trg_beneficiaries_audit_insert ON beneficiaries;
DROP TRIGGER IF EXISTS trg_beneficiaries_audit_update ON beneficiaries;
DROP TRIGGER IF EXISTS trg_beneficiaries_audit_delete ON beneficiaries;

-- Drop functions
DROP FUNCTION IF EXISTS set_updated_at() CASCADE;
DROP FUNCTION IF EXISTS sync_beneficiary_monthly_amount() CASCADE;
DROP FUNCTION IF EXISTS assign_payment_fiscal_period() CASCADE;
DROP FUNCTION IF EXISTS log_beneficiary_status_change() CASCADE;
DROP FUNCTION IF EXISTS audit_beneficiaries() CASCADE;

-- Drop all tables (order matters due to foreign keys — children first)
DROP TABLE IF EXISTS conversation_context CASCADE;
DROP TABLE IF EXISTS document_chunks CASCADE;
DROP TABLE IF EXISTS dashboard_views CASCADE;
DROP TABLE IF EXISTS analytics_query_log CASCADE;
DROP TABLE IF EXISTS payment_forecasts CASCADE;
DROP TABLE IF EXISTS category_amount_history CASCADE;
DROP TABLE IF EXISTS audit_log CASCADE;
DROP TABLE IF EXISTS life_certificates CASCADE;
DROP TABLE IF EXISTS scheme_enrollments CASCADE;
DROP TABLE IF EXISTS beneficiary_status_history CASCADE;
DROP TABLE IF EXISTS payment_summary CASCADE;
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS payment_batches CASCADE;
DROP TABLE IF EXISTS beneficiaries CASCADE;
DROP TABLE IF EXISTS officers CASCADE;
DROP TABLE IF EXISTS fiscal_periods CASCADE;
DROP TABLE IF EXISTS villages CASCADE;
DROP TABLE IF EXISTS banks CASCADE;
DROP TABLE IF EXISTS talukas CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS districts CASCADE;
"""


# ── DDL ───────────────────────────────────────────────────────────────────────

DDL = """

-- ═══════════════════════════════════════════════════════════════════
-- EXTENSIONS
-- ═══════════════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- for ILIKE fast search on names


-- ═══════════════════════════════════════════════════════════════════
-- DIMENSION TABLES
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS districts (
    district_id     SERIAL PRIMARY KEY,
    district_code   VARCHAR(10)  UNIQUE NOT NULL,
    district_name   VARCHAR(100) NOT NULL,
    state           VARCHAR(100) DEFAULT 'Goa',
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS talukas (
    taluka_id       SERIAL PRIMARY KEY,
    taluka_code     VARCHAR(20)  UNIQUE NOT NULL,
    taluka_name     VARCHAR(100) NOT NULL,
    district_id     INTEGER      NOT NULL REFERENCES districts(district_id),
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS villages (
    village_id      SERIAL PRIMARY KEY,
    village_code    VARCHAR(10)  UNIQUE,
    village_name    VARCHAR(150) NOT NULL,
    taluka_id       INTEGER      NOT NULL REFERENCES talukas(taluka_id),
    population      INTEGER,
    pincode         VARCHAR(10),
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS banks (
    bank_id         SERIAL PRIMARY KEY,
    bank_name       VARCHAR(150) NOT NULL,
    ifsc_prefix     VARCHAR(10),
    branch_name     VARCHAR(150),
    city            VARCHAR(100),
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

-- Categories with full history support (amount changes over years)
CREATE TABLE IF NOT EXISTS categories (
    category_id     SERIAL PRIMARY KEY,
    category_code   VARCHAR(20)  UNIQUE NOT NULL,
    category_name   VARCHAR(100) NOT NULL,
    description     TEXT,
    current_monthly_amount  DECIMAL(10,2) NOT NULL,
    disability_percentage   INTEGER,       -- NULL for non-disability categories; 40, 80, 90 etc.
    is_active       BOOLEAN      DEFAULT TRUE,
    introduced_year INTEGER,
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  DEFAULT NOW()
);

-- Tracks when monthly_amount changed — fixes YoY payout comparison hallucinations
CREATE TABLE IF NOT EXISTS category_amount_history (
    id              SERIAL PRIMARY KEY,
    category_id     INTEGER      NOT NULL REFERENCES categories(category_id),
    monthly_amount  DECIMAL(10,2) NOT NULL,
    effective_from  DATE         NOT NULL,
    effective_to    DATE,                  -- NULL = currently active rate
    changed_by      VARCHAR(100),
    reason          TEXT,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cat_amt_hist_cat ON category_amount_history(category_id);
CREATE INDEX IF NOT EXISTS idx_cat_amt_hist_date ON category_amount_history(effective_from);
CREATE UNIQUE INDEX IF NOT EXISTS uq_cat_amt_hist_effective
    ON category_amount_history(category_id, effective_from);


-- ═══════════════════════════════════════════════════════════════════
-- FISCAL PERIODS
-- ═══════════════════════════════════════════════════════════════════
-- Govt of Goa works on April–March fiscal year.
-- This table lets any query join on payment_date to get fiscal context.

CREATE TABLE IF NOT EXISTS fiscal_periods (
    fiscal_period_id    SERIAL PRIMARY KEY,
    fiscal_year         INTEGER      NOT NULL,  -- e.g. 2024 means FY 2024-25
    fiscal_year_label   VARCHAR(10)  NOT NULL,  -- e.g. '2024-25'
    quarter             SMALLINT     NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    quarter_label       VARCHAR(10)  NOT NULL,  -- e.g. 'Q1 FY25'
    period_start        DATE         NOT NULL,
    period_end          DATE         NOT NULL,
    is_current          BOOLEAN      DEFAULT FALSE,
    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (fiscal_year, quarter)
);

CREATE INDEX IF NOT EXISTS idx_fp_start ON fiscal_periods(period_start);
CREATE INDEX IF NOT EXISTS idx_fp_end   ON fiscal_periods(period_end);


-- ═══════════════════════════════════════════════════════════════════
-- OFFICERS
-- ═══════════════════════════════════════════════════════════════════
-- Needed for administrative performance queries:
-- "Which officer processed the most approvals in Q3?"

CREATE TABLE IF NOT EXISTS officers (
    officer_id      SERIAL PRIMARY KEY,
    officer_code    VARCHAR(20)  UNIQUE NOT NULL,
    full_name       VARCHAR(150) NOT NULL,
    designation     VARCHAR(100),
    department      VARCHAR(100) DEFAULT 'Social Welfare',
    district_id     INTEGER      REFERENCES districts(district_id),
    taluka_id       INTEGER      REFERENCES talukas(taluka_id),
    email           VARCHAR(150),
    is_active       BOOLEAN      DEFAULT TRUE,
    joined_date     DATE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);


-- ═══════════════════════════════════════════════════════════════════
-- CORE BENEFICIARIES TABLE
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS beneficiaries (
    beneficiary_id      SERIAL PRIMARY KEY,
    beneficiary_code    VARCHAR(20)  UNIQUE NOT NULL,  -- BEN-00001 format

    -- Personal details
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100),
    gender              VARCHAR(10)  NOT NULL CHECK (gender IN ('Male','Female','Other')),
    date_of_birth       DATE,
    age                 INTEGER,

    -- Location
    district_id         INTEGER      NOT NULL REFERENCES districts(district_id),
    taluka_id           INTEGER      NOT NULL REFERENCES talukas(taluka_id),
    village_id          INTEGER      REFERENCES villages(village_id),
    address             TEXT,
    pincode             VARCHAR(10),

    -- Scheme
    category_id         INTEGER      NOT NULL REFERENCES categories(category_id),
    current_monthly_amount DECIMAL(10,2),   -- denormalised for fast queries; synced from category

    -- Bank
    bank_id             INTEGER      REFERENCES banks(bank_id),
    account_number      VARCHAR(30),
    ifsc_code           VARCHAR(20),

    -- Identity
    aadhaar_number      VARCHAR(12)  UNIQUE,
    phone_number        VARCHAR(15),

    -- Registration
    registration_date   DATE         NOT NULL,
    registration_year   INTEGER GENERATED ALWAYS AS (DATE_PART('year', registration_date)::INTEGER) STORED,
    registered_by       INTEGER      REFERENCES officers(officer_id),

    -- Status
    status              VARCHAR(20)  NOT NULL DEFAULT 'Active'
                            CHECK (status IN ('Active','Inactive','Deceased')),
    status_changed_at   TIMESTAMPTZ,
    status_changed_by   INTEGER      REFERENCES officers(officer_id),
    inactivation_reason VARCHAR(200),

    -- Digitisation flag
    is_digitised        BOOLEAN      DEFAULT TRUE,
    digitised_on        DATE,

    -- Audit
    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_ben_status       ON beneficiaries(status);
CREATE INDEX IF NOT EXISTS idx_ben_district     ON beneficiaries(district_id);
CREATE INDEX IF NOT EXISTS idx_ben_taluka       ON beneficiaries(taluka_id);
CREATE INDEX IF NOT EXISTS idx_ben_village      ON beneficiaries(village_id);
CREATE INDEX IF NOT EXISTS idx_ben_category     ON beneficiaries(category_id);
CREATE INDEX IF NOT EXISTS idx_ben_gender       ON beneficiaries(gender);
CREATE INDEX IF NOT EXISTS idx_ben_reg_year     ON beneficiaries(registration_year);
CREATE INDEX IF NOT EXISTS idx_ben_reg_date     ON beneficiaries(registration_date);
CREATE INDEX IF NOT EXISTS idx_ben_dob          ON beneficiaries(date_of_birth);
CREATE INDEX IF NOT EXISTS idx_ben_age          ON beneficiaries(age);
CREATE INDEX IF NOT EXISTS idx_ben_status_dist  ON beneficiaries(status, district_id);
CREATE INDEX IF NOT EXISTS idx_ben_status_cat   ON beneficiaries(status, category_id);
CREATE INDEX IF NOT EXISTS idx_ben_status_tal   ON beneficiaries(status, taluka_id);
-- Full-text search on names
CREATE INDEX IF NOT EXISTS idx_ben_fname_trgm   ON beneficiaries USING GIN (first_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_ben_lname_trgm   ON beneficiaries USING GIN (last_name gin_trgm_ops);


-- ═══════════════════════════════════════════════════════════════════
-- BENEFICIARY STATUS HISTORY
-- ═══════════════════════════════════════════════════════════════════
-- Fixes: "How many beneficiaries were added after 2020?" — now answered
-- from registration_date on beneficiaries, but status changes are tracked here.
-- Also fixes: "beneficiaries who became inactive in 2023" type queries.

CREATE TABLE IF NOT EXISTS beneficiary_status_history (
    id                  SERIAL PRIMARY KEY,
    beneficiary_id      INTEGER      NOT NULL REFERENCES beneficiaries(beneficiary_id),
    old_status          VARCHAR(20),
    new_status          VARCHAR(20)  NOT NULL,
    changed_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    changed_by          INTEGER      REFERENCES officers(officer_id),
    reason              VARCHAR(200),
    remarks             TEXT
);

CREATE INDEX IF NOT EXISTS idx_bsh_ben      ON beneficiary_status_history(beneficiary_id);
CREATE INDEX IF NOT EXISTS idx_bsh_date     ON beneficiary_status_history(changed_at);
CREATE INDEX IF NOT EXISTS idx_bsh_status   ON beneficiary_status_history(new_status);
CREATE INDEX IF NOT EXISTS idx_bsh_year     ON beneficiary_status_history((DATE_PART('year', changed_at AT TIME ZONE 'UTC')::INTEGER));


-- ═══════════════════════════════════════════════════════════════════
-- SCHEME ENROLLMENTS
-- ═══════════════════════════════════════════════════════════════════
-- Fixes: "beneficiaries enrolled in Widow category before 2022" —
-- currently impossible because beneficiaries only has category_id with no dates.
-- Also handles category transfers (e.g. Widow → Senior Citizen on turning 60).

CREATE TABLE IF NOT EXISTS scheme_enrollments (
    enrollment_id       SERIAL PRIMARY KEY,
    beneficiary_id      INTEGER      NOT NULL REFERENCES beneficiaries(beneficiary_id),
    category_id         INTEGER      NOT NULL REFERENCES categories(category_id),
    enrollment_date     DATE         NOT NULL,
    enrollment_year     INTEGER GENERATED ALWAYS AS (DATE_PART('year', enrollment_date)::INTEGER) STORED,
    end_date            DATE,                  -- NULL = currently enrolled
    end_reason          VARCHAR(200),
    monthly_amount_at_enrollment DECIMAL(10,2), -- amount locked at time of enrollment
    approved_by         INTEGER      REFERENCES officers(officer_id),
    is_current          BOOLEAN      DEFAULT TRUE,
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_enroll_ben       ON scheme_enrollments(beneficiary_id);
CREATE INDEX IF NOT EXISTS idx_enroll_cat       ON scheme_enrollments(category_id);
CREATE INDEX IF NOT EXISTS idx_enroll_date      ON scheme_enrollments(enrollment_date);
CREATE INDEX IF NOT EXISTS idx_enroll_year      ON scheme_enrollments(enrollment_year);
CREATE INDEX IF NOT EXISTS idx_enroll_current   ON scheme_enrollments(is_current) WHERE is_current = TRUE;
CREATE UNIQUE INDEX IF NOT EXISTS uq_enroll_one_current
    ON scheme_enrollments(beneficiary_id)
    WHERE is_current = TRUE;


-- ═══════════════════════════════════════════════════════════════════
-- PAYMENTS TABLE
-- ═══════════════════════════════════════════════════════════════════
-- Key fix: payments now has a direct payment_date column (DATE).
-- No more date_id join needed — "compare last 3 years payments" just uses
--   WHERE EXTRACT(YEAR FROM payment_date) IN (2023, 2024, 2025)
-- This eliminates the most common hallucination source.

CREATE TABLE IF NOT EXISTS payments (
    payment_id          SERIAL PRIMARY KEY,
    beneficiary_id      INTEGER      NOT NULL REFERENCES beneficiaries(beneficiary_id),

    -- Direct date columns — no join needed for filtering
    payment_date        DATE         NOT NULL,
    payment_month       SMALLINT     GENERATED ALWAYS AS (DATE_PART('month', payment_date)::SMALLINT) STORED,
    payment_year        SMALLINT     GENERATED ALWAYS AS (DATE_PART('year', payment_date)::SMALLINT) STORED,
    fiscal_period_id    INTEGER      REFERENCES fiscal_periods(fiscal_period_id),

    -- Amount
    amount              DECIMAL(10,2) NOT NULL,
    expected_amount     DECIMAL(10,2),         -- what should have been paid (for shortfall queries)

    -- Status
    status              VARCHAR(20)  NOT NULL DEFAULT 'Paid'
                            CHECK (status IN ('Paid','Pending','Failed','Reversed')),

    -- Payment method
    payment_method      VARCHAR(50)  DEFAULT 'bank_transfer',
    transaction_id      VARCHAR(100) UNIQUE,
    bank_id             INTEGER      REFERENCES banks(bank_id),

    -- Processing
    processed_by        INTEGER      REFERENCES officers(officer_id),
    remarks             TEXT,
    failure_reason      VARCHAR(200),

    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

-- Indexes tuned for the most common analytics queries
CREATE INDEX IF NOT EXISTS idx_pay_ben_id       ON payments(beneficiary_id);
CREATE INDEX IF NOT EXISTS idx_pay_date         ON payments(payment_date);
CREATE INDEX IF NOT EXISTS idx_pay_year         ON payments(payment_year);
CREATE INDEX IF NOT EXISTS idx_pay_month        ON payments(payment_month);
CREATE INDEX IF NOT EXISTS idx_pay_year_month   ON payments(payment_year, payment_month);
CREATE INDEX IF NOT EXISTS idx_pay_status       ON payments(status);
CREATE INDEX IF NOT EXISTS idx_pay_status_year  ON payments(status, payment_year);
CREATE INDEX IF NOT EXISTS idx_pay_fiscal       ON payments(fiscal_period_id);


-- ═══════════════════════════════════════════════════════════════════
-- PAYMENT BATCHES
-- ═══════════════════════════════════════════════════════════════════
-- One batch per monthly ECS run (72 rows for 6 fiscal years).
-- Links to individual payments via batch_id — lets queries answer:
-- "Which batch had the most failures?", "Total disbursed in April 2025 batch?"

CREATE TABLE IF NOT EXISTS payment_batches (
    batch_id            SERIAL PRIMARY KEY,
    batch_reference     VARCHAR(30)   UNIQUE NOT NULL,   -- e.g. BATCH/2024/04
    payment_month       SMALLINT      NOT NULL CHECK (payment_month BETWEEN 1 AND 12),
    payment_year        SMALLINT      NOT NULL,
    fiscal_year         INTEGER,                          -- e.g. 2024 = FY 2024-25
    fiscal_year_label   VARCHAR(10),                      -- e.g. '2024-25'
    batch_status        VARCHAR(20)   NOT NULL DEFAULT 'Completed'
                            CHECK (batch_status IN ('Draft','Processing','Completed','Failed','Reversed')),
    total_beneficiaries INTEGER       DEFAULT 0,
    total_amount        NUMERIC(18,2) DEFAULT 0,
    paid_count          INTEGER       DEFAULT 0,
    failed_count        INTEGER       DEFAULT 0,
    pending_count       INTEGER       DEFAULT 0,
    initiated_by        INTEGER       REFERENCES officers(officer_id),
    initiated_at        TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    remarks             TEXT,
    created_at          TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE (payment_year, payment_month)
);

CREATE INDEX IF NOT EXISTS idx_pb_year        ON payment_batches(payment_year);
CREATE INDEX IF NOT EXISTS idx_pb_year_month  ON payment_batches(payment_year, payment_month);
CREATE INDEX IF NOT EXISTS idx_pb_status      ON payment_batches(batch_status);
CREATE INDEX IF NOT EXISTS idx_pb_fiscal      ON payment_batches(fiscal_year);

-- FK: add batch_id to payments table so each payment row links to its batch
ALTER TABLE payments ADD COLUMN IF NOT EXISTS batch_id INTEGER REFERENCES payment_batches(batch_id);
CREATE INDEX IF NOT EXISTS idx_pay_batch      ON payments(batch_id);


-- ═══════════════════════════════════════════════════════════════════
-- LIFE CERTIFICATES
-- ═══════════════════════════════════════════════════════════════════
-- Annual life-certificate submission by every active beneficiary (due April/May).
-- Missing submission → payment_suspended = TRUE → payments stop.
-- Answers: "Which talukas have lowest life-cert compliance?",
--          "How many beneficiaries have not submitted for 2025?",
--          "Show late submissions by category"

CREATE TABLE IF NOT EXISTS life_certificates (
    cert_id             SERIAL PRIMARY KEY,
    beneficiary_id      INTEGER       NOT NULL REFERENCES beneficiaries(beneficiary_id),

    -- Submission details
    submission_date     DATE          NOT NULL,
    due_month           SMALLINT      NOT NULL DEFAULT 4,    -- April
    due_year            SMALLINT      NOT NULL,
    fiscal_year         INTEGER,                              -- FY the cert covers

    -- Lateness
    is_late_submission  BOOLEAN       NOT NULL DEFAULT FALSE,
    days_late           INTEGER       DEFAULT 0,             -- 0 if on time

    -- Verification
    issued_by_type      VARCHAR(50)                          -- 'Bank_Manager' | 'Gazetted_Officer' | 'Aadhaar_eKYC'
                            CHECK (issued_by_type IN (
                                'Bank_Manager','Gazetted_Officer','Aadhaar_eKYC',
                                'Post_Office','Tahsildar','Other'
                            )),
    verified_by         INTEGER       REFERENCES officers(officer_id),
    verification_date   DATE,

    -- Consequence flag — set TRUE by batch job if cert not received by cut-off
    payment_suspended   BOOLEAN       NOT NULL DEFAULT FALSE,
    suspension_date     DATE,                                -- when suspension was triggered
    reinstatement_date  DATE,                                -- when payments resumed

    remarks             TEXT,
    created_at          TIMESTAMPTZ   DEFAULT NOW(),

    -- One cert per beneficiary per due_year
    UNIQUE (beneficiary_id, due_year)
);

CREATE INDEX IF NOT EXISTS idx_lc_ben         ON life_certificates(beneficiary_id);
CREATE INDEX IF NOT EXISTS idx_lc_due_year    ON life_certificates(due_year);
CREATE INDEX IF NOT EXISTS idx_lc_sub_date    ON life_certificates(submission_date);
CREATE INDEX IF NOT EXISTS idx_lc_suspended   ON life_certificates(payment_suspended) WHERE payment_suspended = TRUE;
CREATE INDEX IF NOT EXISTS idx_lc_late        ON life_certificates(is_late_submission) WHERE is_late_submission = TRUE;
-- Join path: life_certificates → beneficiaries → talukas/districts for compliance reports
CREATE INDEX IF NOT EXISTS idx_lc_year_ben    ON life_certificates(due_year, beneficiary_id);


-- ═══════════════════════════════════════════════════════════════════
-- PAYMENT SUMMARY (pre-aggregated table — the AI's primary payment source)
-- ═══════════════════════════════════════════════════════════════════
-- One row per (year, month, district, taluka, category).
-- Seeded by seed_dssy.py with 6 years of data (~6k rows).
-- Gemini queries THIS table for "last 3 years payments", "YoY comparison", etc.
-- The raw payments table only has last 6 months; this has 6 full fiscal years.

CREATE TABLE IF NOT EXISTS payment_summary (
    summary_id          SERIAL PRIMARY KEY,
    payment_year        SMALLINT      NOT NULL,
    payment_month       SMALLINT      NOT NULL,
    month_name          VARCHAR(15),
    fiscal_year         INTEGER,
    fiscal_year_label   VARCHAR(10),
    quarter             SMALLINT,
    district_id         INTEGER       REFERENCES districts(district_id),
    taluka_id           INTEGER       REFERENCES talukas(taluka_id),
    category_id         INTEGER       REFERENCES categories(category_id),
    total_beneficiaries INTEGER       DEFAULT 0,
    paid_count          INTEGER       DEFAULT 0,
    pending_count       INTEGER       DEFAULT 0,
    failed_count        INTEGER       DEFAULT 0,
    on_hold_count       INTEGER       DEFAULT 0,
    total_base_amount   NUMERIC(18,2) DEFAULT 0,
    total_net_amount    NUMERIC(18,2) DEFAULT 0,
    male_count          INTEGER       DEFAULT 0,
    female_count        INTEGER       DEFAULT 0,
    last_updated_at     TIMESTAMPTZ   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ps_year         ON payment_summary(payment_year);
CREATE INDEX IF NOT EXISTS idx_ps_year_month   ON payment_summary(payment_year, payment_month);
CREATE INDEX IF NOT EXISTS idx_ps_district     ON payment_summary(district_id);
CREATE INDEX IF NOT EXISTS idx_ps_category     ON payment_summary(category_id);
CREATE INDEX IF NOT EXISTS idx_ps_taluka       ON payment_summary(taluka_id);
CREATE INDEX IF NOT EXISTS idx_ps_composite    ON payment_summary(payment_year, payment_month, district_id, category_id);


-- ═══════════════════════════════════════════════════════════════════
-- PAYMENT SUMMARY MONTHLY (materialized view from raw payments)
-- ═══════════════════════════════════════════════════════════════════
-- This view pre-aggregates the raw payments table by year/month/category/district.
-- Useful for cross-checking against the payment_summary table.

CREATE MATERIALIZED VIEW IF NOT EXISTS payment_summary_monthly AS
SELECT
    p.payment_year,
    p.payment_month,
    TO_CHAR(DATE_TRUNC('month', p.payment_date), 'YYYY-MM') AS year_month,
    b.district_id,
    d.district_name,
    b.category_id,
    c.category_name,
    COUNT(*)                          AS payment_count,
    COUNT(*) FILTER (WHERE p.status = 'paid')    AS paid_count,
    COUNT(*) FILTER (WHERE p.status = 'pending') AS pending_count,
    COUNT(*) FILTER (WHERE p.status = 'failed')  AS failed_count,
    SUM(p.amount)                     AS total_amount,
    SUM(p.amount) FILTER (WHERE p.status = 'paid')   AS paid_amount,
    SUM(p.amount) FILTER (WHERE p.status = 'pending') AS pending_amount,
    COUNT(DISTINCT p.beneficiary_id)  AS unique_beneficiaries,
    ROUND(
        COUNT(*) FILTER (WHERE p.status = 'paid') * 100.0 / NULLIF(COUNT(*), 0),
        2
    )                                 AS compliance_pct
FROM payments p
JOIN beneficiaries b ON p.beneficiary_id = b.beneficiary_id
JOIN districts     d ON b.district_id    = d.district_id
JOIN categories    c ON b.category_id    = c.category_id
GROUP BY
    p.payment_year, p.payment_month,
    DATE_TRUNC('month', p.payment_date),
    b.district_id, d.district_name,
    b.category_id, c.category_name;

CREATE UNIQUE INDEX IF NOT EXISTS idx_psm_unique
    ON payment_summary_monthly(payment_year, payment_month, district_id, category_id);
CREATE INDEX IF NOT EXISTS idx_psm_year         ON payment_summary_monthly(payment_year);
CREATE INDEX IF NOT EXISTS idx_psm_year_month   ON payment_summary_monthly(payment_year, payment_month);
CREATE INDEX IF NOT EXISTS idx_psm_district     ON payment_summary_monthly(district_id);
CREATE INDEX IF NOT EXISTS idx_psm_category     ON payment_summary_monthly(category_id);


-- ═══════════════════════════════════════════════════════════════════
-- BENEFICIARY SUMMARY (current-state aggregate for dashboards)
-- ═══════════════════════════════════════════════════════════════════
-- This view gives the UI a chart-ready table for category/district/taluka/gender
-- slices without forcing the LLM to assemble the same GROUP BY repeatedly.

CREATE MATERIALIZED VIEW IF NOT EXISTS beneficiary_summary_current AS
SELECT
    b.status,
    b.gender,
    b.district_id,
    d.district_name,
    b.taluka_id,
    t.taluka_name,
    b.category_id,
    c.category_name,
    COUNT(*)                    AS beneficiary_count,
    AVG(b.age)::NUMERIC(10,2)   AS avg_age,
    SUM(COALESCE(b.current_monthly_amount, c.current_monthly_amount)) AS total_monthly_liability
FROM beneficiaries b
JOIN districts  d ON b.district_id = d.district_id
JOIN talukas    t ON b.taluka_id   = t.taluka_id
JOIN categories c ON b.category_id = c.category_id
GROUP BY
    b.status, b.gender,
    b.district_id, d.district_name,
    b.taluka_id, t.taluka_name,
    b.category_id, c.category_name;

CREATE INDEX IF NOT EXISTS idx_bsc_status       ON beneficiary_summary_current(status);
CREATE INDEX IF NOT EXISTS idx_bsc_district     ON beneficiary_summary_current(district_id);
CREATE INDEX IF NOT EXISTS idx_bsc_taluka       ON beneficiary_summary_current(taluka_id);
CREATE INDEX IF NOT EXISTS idx_bsc_category     ON beneficiary_summary_current(category_id);


-- ═══════════════════════════════════════════════════════════════════
-- BENEFICIARY REGISTRATION SUMMARY (trend view for charts and follow-ups)
-- ═══════════════════════════════════════════════════════════════════
-- This eliminates ambiguity for questions like:
-- "show registrations for last 3 years", "compare 2023 vs 2024 vs 2025",
-- "show monthly trend for widows in North Goa".

CREATE MATERIALIZED VIEW IF NOT EXISTS beneficiary_registration_summary_monthly AS
SELECT
    EXTRACT(YEAR FROM b.registration_date)::INTEGER  AS registration_year,
    EXTRACT(MONTH FROM b.registration_date)::INTEGER AS registration_month,
    TO_CHAR(DATE_TRUNC('month', b.registration_date), 'YYYY-MM') AS year_month,
    b.district_id,
    d.district_name,
    b.category_id,
    c.category_name,
    b.status,
    COUNT(*) AS registrations
FROM beneficiaries b
JOIN districts  d ON b.district_id = d.district_id
JOIN categories c ON b.category_id = c.category_id
GROUP BY
    DATE_TRUNC('month', b.registration_date),
    EXTRACT(YEAR FROM b.registration_date),
    EXTRACT(MONTH FROM b.registration_date),
    b.district_id, d.district_name,
    b.category_id, c.category_name,
    b.status;

CREATE INDEX IF NOT EXISTS idx_brsm_year        ON beneficiary_registration_summary_monthly(registration_year);
CREATE INDEX IF NOT EXISTS idx_brsm_year_month  ON beneficiary_registration_summary_monthly(registration_year, registration_month);
CREATE INDEX IF NOT EXISTS idx_brsm_district    ON beneficiary_registration_summary_monthly(district_id);
CREATE INDEX IF NOT EXISTS idx_brsm_category    ON beneficiary_registration_summary_monthly(category_id);


-- ═══════════════════════════════════════════════════════════════════
-- FORECAST OUTPUTS
-- ═══════════════════════════════════════════════════════════════════
-- Stores externally generated forecast results (ARIMA, Prophet, regression, etc.)
-- so minister dashboards can query projections using the same NL-to-SQL layer.

CREATE TABLE IF NOT EXISTS payment_forecasts (
    forecast_id          SERIAL PRIMARY KEY,
    forecast_type        VARCHAR(50)   NOT NULL,  -- payment | beneficiaries | category_liability
    forecast_grain       VARCHAR(30)   NOT NULL,  -- monthly | quarterly | yearly
    model_name           VARCHAR(100)  NOT NULL,
    version_tag          VARCHAR(40),
    district_id          INTEGER       REFERENCES districts(district_id),
    taluka_id            INTEGER       REFERENCES talukas(taluka_id),
    category_id          INTEGER       REFERENCES categories(category_id),
    forecast_period_start DATE         NOT NULL,
    forecast_period_end   DATE         NOT NULL,
    predicted_value      NUMERIC(18,2) NOT NULL,
    lower_bound          NUMERIC(18,2),
    upper_bound          NUMERIC(18,2),
    confidence_pct       NUMERIC(5,2),
    input_snapshot_date  DATE,
    metadata             JSONB         DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pf_type_period   ON payment_forecasts(forecast_type, forecast_period_start);
CREATE INDEX IF NOT EXISTS idx_pf_district      ON payment_forecasts(district_id);
CREATE INDEX IF NOT EXISTS idx_pf_taluka        ON payment_forecasts(taluka_id);
CREATE INDEX IF NOT EXISTS idx_pf_category      ON payment_forecasts(category_id);


-- ═══════════════════════════════════════════════════════════════════
-- AUDIT LOG
-- ═══════════════════════════════════════════════════════════════════
-- Tracks every INSERT/UPDATE on beneficiaries with actor + timestamp.
-- Supports: "how many records were updated in March 2025?"

CREATE TABLE IF NOT EXISTS audit_log (
    id              BIGSERIAL    PRIMARY KEY,
    table_name      VARCHAR(50)  NOT NULL,
    record_id       INTEGER      NOT NULL,
    action          VARCHAR(10)  NOT NULL CHECK (action IN ('INSERT','UPDATE','DELETE')),
    changed_fields  JSONB,                 -- {"status": ["active","inactive"]}
    old_values      JSONB,
    new_values      JSONB,
    performed_by    INTEGER      REFERENCES officers(officer_id),
    performed_at    TIMESTAMPTZ  DEFAULT NOW(),
    ip_address      INET,
    session_info    TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_table      ON audit_log(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_record     ON audit_log(table_name, record_id);
CREATE INDEX IF NOT EXISTS idx_audit_date       ON audit_log(performed_at);
CREATE INDEX IF NOT EXISTS idx_audit_action     ON audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_year       ON audit_log((DATE_PART('year', performed_at AT TIME ZONE 'UTC')::INTEGER));


-- ═══════════════════════════════════════════════════════════════════
-- RAG: DOCUMENT CHUNKS (pgvector)
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS document_chunks (
    id              SERIAL PRIMARY KEY,
    doc_name        VARCHAR(200)  NOT NULL,
    chunk_index     INTEGER       NOT NULL,
    chunk_text      TEXT          NOT NULL,
    embedding       vector(768),
    metadata        JSONB         DEFAULT '{}',
    search_vector   TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', chunk_text)) STORED,
    created_at      TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE (doc_name, chunk_index)
);

CREATE INDEX IF NOT EXISTS doc_chunks_emb_idx    ON document_chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 50);
CREATE INDEX IF NOT EXISTS doc_chunks_search_idx ON document_chunks USING GIN (search_vector);
CREATE INDEX IF NOT EXISTS doc_chunks_doc_idx    ON document_chunks(doc_name);


-- ═══════════════════════════════════════════════════════════════════
-- CONVERSATION CONTEXT (multi-turn sessions)
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS conversation_context (
    id                  SERIAL PRIMARY KEY,
    session_id          TEXT         NOT NULL,
    question            TEXT,
    resolved_question   TEXT,
    answer              TEXT,
    intent              TEXT,
    sql_data            TEXT,        -- JSON-encoded list[dict] of raw SQL results
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_conv_session ON conversation_context(session_id, created_at DESC);


-- ═══════════════════════════════════════════════════════════════════
-- ANALYTICS QUERY LOG
-- ═══════════════════════════════════════════════════════════════════
-- Gives you traceability for hallucination analysis and lets the product team
-- inspect what users asked, how it was routed, and what SQL/chart was generated.

CREATE TABLE IF NOT EXISTS analytics_query_log (
    id                  BIGSERIAL PRIMARY KEY,
    session_id          TEXT,
    user_question       TEXT         NOT NULL,
    resolved_question   TEXT,
    intent              VARCHAR(20),             -- SQL | RAG | EDGE | FORECAST
    generated_sql       TEXT,
    chart_type          VARCHAR(20),
    route_status        VARCHAR(20),             -- success | fallback | failed
    row_count           INTEGER      DEFAULT 0,
    execution_time_ms   INTEGER,
    confidence_label    VARCHAR(20),
    result_preview      JSONB,
    error_message       TEXT,
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aql_session      ON analytics_query_log(session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_aql_intent       ON analytics_query_log(intent, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_aql_status       ON analytics_query_log(route_status, created_at DESC);


-- ═══════════════════════════════════════════════════════════════════
-- SAVED DASHBOARD VIEWS
-- ═══════════════════════════════════════════════════════════════════
-- Stores dynamic dashboard presets so officers can save/re-open chart states
-- driven by natural-language questions.

CREATE TABLE IF NOT EXISTS dashboard_views (
    dashboard_view_id    SERIAL PRIMARY KEY,
    view_name            VARCHAR(150) NOT NULL,
    view_scope           VARCHAR(30)  DEFAULT 'department', -- department | officer | minister
    created_by           INTEGER      REFERENCES officers(officer_id),
    nl_prompt            TEXT,
    generated_sql        TEXT,
    chart_type           VARCHAR(20),
    layout_config        JSONB        DEFAULT '{}'::jsonb,
    filter_config        JSONB        DEFAULT '{}'::jsonb,
    is_default           BOOLEAN      DEFAULT FALSE,
    is_active            BOOLEAN      DEFAULT TRUE,
    created_at           TIMESTAMPTZ  DEFAULT NOW(),
    updated_at           TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dv_scope         ON dashboard_views(view_scope, is_active);
CREATE INDEX IF NOT EXISTS idx_dv_created_by    ON dashboard_views(created_by);


-- ═══════════════════════════════════════════════════════════════════
-- AUTOMATION FUNCTIONS & TRIGGERS
-- ═══════════════════════════════════════════════════════════════════
-- These keep the schema honest so the analytics layer sees clean data:
--   - updated_at is maintained automatically
--   - beneficiary amount defaults from category
--   - fiscal_period_id is auto-derived from payment_date
--   - status changes are written to status history
--   - beneficiary inserts/updates/deletes are written to audit_log

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sync_beneficiary_monthly_amount()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.current_monthly_amount IS NULL THEN
        SELECT c.current_monthly_amount
          INTO NEW.current_monthly_amount
          FROM categories c
         WHERE c.category_id = NEW.category_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION assign_payment_fiscal_period()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.payment_date IS NOT NULL THEN
        SELECT fp.fiscal_period_id
          INTO NEW.fiscal_period_id
          FROM fiscal_periods fp
         WHERE NEW.payment_date BETWEEN fp.period_start AND fp.period_end
         LIMIT 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION log_beneficiary_status_change()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO beneficiary_status_history (
            beneficiary_id, old_status, new_status, changed_at, changed_by, reason, remarks
        )
        VALUES (
            NEW.beneficiary_id,
            NULL,
            NEW.status,
            COALESCE(NEW.status_changed_at, NOW()),
            NEW.status_changed_by,
            NEW.inactivation_reason,
            'Initial beneficiary status'
        );
        RETURN NEW;
    END IF;

    IF OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO beneficiary_status_history (
            beneficiary_id, old_status, new_status, changed_at, changed_by, reason, remarks
        )
        VALUES (
            NEW.beneficiary_id,
            OLD.status,
            NEW.status,
            COALESCE(NEW.status_changed_at, NOW()),
            NEW.status_changed_by,
            NEW.inactivation_reason,
            'Status changed through beneficiaries table'
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION audit_beneficiaries()
RETURNS TRIGGER AS $$
DECLARE
    actor_id INTEGER;
BEGIN
    actor_id := COALESCE(
        CASE WHEN TG_OP = 'DELETE' THEN OLD.status_changed_by ELSE NEW.status_changed_by END,
        CASE WHEN TG_OP = 'DELETE' THEN OLD.registered_by ELSE NEW.registered_by END
    );

    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_log (
            table_name, record_id, action, new_values, performed_by, session_info
        )
        VALUES (
            'beneficiaries',
            NEW.beneficiary_id,
            'INSERT',
            to_jsonb(NEW),
            actor_id,
            'schema_trigger'
        );
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log (
            table_name, record_id, action, old_values, new_values, changed_fields, performed_by, session_info
        )
        VALUES (
            'beneficiaries',
            NEW.beneficiary_id,
            'UPDATE',
            to_jsonb(OLD),
            to_jsonb(NEW),
            jsonb_build_object(
                'status', jsonb_build_array(OLD.status, NEW.status),
                'category_id', jsonb_build_array(OLD.category_id, NEW.category_id),
                'district_id', jsonb_build_array(OLD.district_id, NEW.district_id),
                'taluka_id', jsonb_build_array(OLD.taluka_id, NEW.taluka_id)
            ),
            actor_id,
            'schema_trigger'
        );
        RETURN NEW;
    ELSE
        INSERT INTO audit_log (
            table_name, record_id, action, old_values, performed_by, session_info
        )
        VALUES (
            'beneficiaries',
            OLD.beneficiary_id,
            'DELETE',
            to_jsonb(OLD),
            actor_id,
            'schema_trigger'
        );
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS trg_categories_updated_at ON categories;
CREATE TRIGGER trg_categories_updated_at
BEFORE UPDATE ON categories
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_beneficiaries_updated_at ON beneficiaries;
CREATE TRIGGER trg_beneficiaries_updated_at
BEFORE UPDATE ON beneficiaries
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_dashboard_views_updated_at ON dashboard_views;
CREATE TRIGGER trg_dashboard_views_updated_at
BEFORE UPDATE ON dashboard_views
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_beneficiaries_sync_amount ON beneficiaries;
CREATE TRIGGER trg_beneficiaries_sync_amount
BEFORE INSERT OR UPDATE OF category_id, current_monthly_amount ON beneficiaries
FOR EACH ROW
EXECUTE FUNCTION sync_beneficiary_monthly_amount();

DROP TRIGGER IF EXISTS trg_payments_assign_fiscal_period ON payments;
CREATE TRIGGER trg_payments_assign_fiscal_period
BEFORE INSERT OR UPDATE OF payment_date ON payments
FOR EACH ROW
EXECUTE FUNCTION assign_payment_fiscal_period();

DROP TRIGGER IF EXISTS trg_beneficiaries_status_history_insert ON beneficiaries;
CREATE TRIGGER trg_beneficiaries_status_history_insert
AFTER INSERT ON beneficiaries
FOR EACH ROW
EXECUTE FUNCTION log_beneficiary_status_change();

DROP TRIGGER IF EXISTS trg_beneficiaries_status_history_update ON beneficiaries;
CREATE TRIGGER trg_beneficiaries_status_history_update
AFTER UPDATE OF status, status_changed_at, status_changed_by, inactivation_reason ON beneficiaries
FOR EACH ROW
EXECUTE FUNCTION log_beneficiary_status_change();

DROP TRIGGER IF EXISTS trg_beneficiaries_audit_insert ON beneficiaries;
CREATE TRIGGER trg_beneficiaries_audit_insert
AFTER INSERT ON beneficiaries
FOR EACH ROW
EXECUTE FUNCTION audit_beneficiaries();

DROP TRIGGER IF EXISTS trg_beneficiaries_audit_update ON beneficiaries;
CREATE TRIGGER trg_beneficiaries_audit_update
AFTER UPDATE ON beneficiaries
FOR EACH ROW
EXECUTE FUNCTION audit_beneficiaries();

DROP TRIGGER IF EXISTS trg_beneficiaries_audit_delete ON beneficiaries;
CREATE TRIGGER trg_beneficiaries_audit_delete
AFTER DELETE ON beneficiaries
FOR EACH ROW
EXECUTE FUNCTION audit_beneficiaries();

"""

# ── Seed data ─────────────────────────────────────────────────────────────────

SEED_SQL = """

-- ── Districts ──────────────────────────────────────────────────────────────────
INSERT INTO districts (district_code, district_name, state) VALUES
    ('NGO', 'North Goa', 'Goa'),
    ('SGO', 'South Goa', 'Goa')
ON CONFLICT (district_code) DO NOTHING;


-- ── Talukas ────────────────────────────────────────────────────────────────────
INSERT INTO talukas (taluka_code, taluka_name, district_id) VALUES
    ('TK-TISWADI',   'Tiswadi',     1),
    ('TK-BARDEZ',    'Bardez',      1),
    ('TK-PERNEM',    'Pernem',      1),
    ('TK-BICHOLIM',  'Bicholim',    1),
    ('TK-SATARI',    'Satari',      1),
    ('TK-PONDA',     'Ponda',       1),
    ('TK-SALCETE',   'Salcete',     2),
    ('TK-MORMUGAO',  'Mormugao',    2),
    ('TK-QUEPEM',    'Quepem',      2),
    ('TK-SANGUEM',   'Sanguem',     2),
    ('TK-CANACONA',  'Canacona',    2),
    ('TK-DHARBANDORA','Dharbandora',2)
ON CONFLICT (taluka_code) DO NOTHING;


-- ── Categories (all DSSY categories including sub-categories) ──────────────────
INSERT INTO categories (category_code, category_name, description, current_monthly_amount, disability_percentage, is_active, introduced_year) VALUES
    ('SC',       'Senior Citizen',          'Citizens aged 60 and above',                              2000.00, NULL, TRUE, 2001),
    ('WD',       'Widow',                   'Widowed women below 65 years',                            2000.00, NULL, TRUE, 2001),
    ('SW',       'Single Woman',            'Unmarried / abandoned / separated women',                 2000.00, NULL, TRUE, 2001),
    ('DIS-40',   'Disabled 40%',            'Persons with 40% or more disability',                     2000.00, 40,   TRUE, 2001),
    ('DIS-80',   'Disabled 80%',            'Persons with 80% or more disability',                     2500.00, 80,   TRUE, 2001),
    ('DIS-90',   'Disabled 90%',            'Persons with 90% or more disability',                     3000.00, 90,   TRUE, 2013),
    ('HIV',      'HIV/AIDS',                'Persons living with HIV/AIDS',                            2500.00, NULL, TRUE, 2001),
    ('LEPROSY',  'Leprosy',                 'Persons cured of leprosy with deformities',               2000.00, NULL, TRUE, 2016),
    ('DEAF',     'Deaf and Dumb',           'Persons with speech and hearing impairment',              2000.00, NULL, TRUE, 2016),
    ('CANCER',   'Cancer Patient',          'Patients diagnosed with cancer',                          3500.00, NULL, TRUE, 2021),
    ('KIDNEY',   'Kidney Failure',          'Patients on dialysis or with kidney failure',             3500.00, NULL, TRUE, 2021),
    ('SICKLE',   'Sickle Cell',             'Persons with sickle cell disease',                        2000.00, NULL, TRUE, 2021)
ON CONFLICT (category_code) DO NOTHING;


-- ── Banks (common Goa banks used for pension disbursement) ───────────────────
INSERT INTO banks (bank_name, ifsc_prefix, branch_name, city) VALUES
    ('State Bank of India',         'SBIN',  'Panaji Main',      'Panaji'),
    ('Bank of Baroda',              'BARB',  'Margao Branch',    'Margao'),
    ('Corporation Bank',            'CORP',  'Mapusa Branch',    'Mapusa'),
    ('Canara Bank',                 'CNRB',  'Vasco Branch',     'Vasco da Gama'),
    ('Union Bank of India',         'UBIN',  'Ponda Branch',     'Ponda'),
    ('Bank of India',               'BKID',  'Panaji Branch',    'Panaji'),
    ('Central Bank of India',       'CBIN',  'Margao Branch',    'Margao'),
    ('Indian Overseas Bank',        'IOBA',  'Mapusa Branch',    'Mapusa'),
    ('Punjab National Bank',        'PUNB',  'Panaji Branch',    'Panaji'),
    ('Goa State Co-op Bank',        'GSCB',  'Head Office',      'Panaji')
ON CONFLICT DO NOTHING;


-- ── Category amount history (so YoY payout comparisons work correctly) ────────
INSERT INTO category_amount_history (category_id, monthly_amount, effective_from, effective_to, reason)
SELECT category_id, 1000.00, '2001-01-01', '2012-12-31', 'Original amount at scheme launch'
FROM categories WHERE category_code = 'SC'
ON CONFLICT DO NOTHING;

INSERT INTO category_amount_history (category_id, monthly_amount, effective_from, effective_to, reason)
SELECT category_id, 1500.00, '2013-01-01', '2019-12-31', 'Revised as per 2013 notification'
FROM categories WHERE category_code = 'SC'
ON CONFLICT DO NOTHING;

INSERT INTO category_amount_history (category_id, monthly_amount, effective_from, effective_to, reason)
SELECT category_id, 2000.00, '2020-01-01', NULL, 'Current rate effective from 2020 digitisation'
FROM categories WHERE category_code = 'SC'
ON CONFLICT DO NOTHING;


-- ── Fiscal periods (FY 2018–2030, April–March) ────────────────────────────────
INSERT INTO fiscal_periods (fiscal_year, fiscal_year_label, quarter, quarter_label, period_start, period_end, is_current)
SELECT
    fy,
    fy::TEXT || '-' || (fy+1-2000)::TEXT AS fiscal_year_label,
    q,
    'Q' || q || ' FY' || (fy+1-2000)::TEXT AS quarter_label,
    CASE q
        WHEN 1 THEN MAKE_DATE(fy, 4, 1)
        WHEN 2 THEN MAKE_DATE(fy, 7, 1)
        WHEN 3 THEN MAKE_DATE(fy, 10, 1)
        WHEN 4 THEN MAKE_DATE(fy+1, 1, 1)
    END AS period_start,
    CASE q
        WHEN 1 THEN MAKE_DATE(fy, 6, 30)
        WHEN 2 THEN MAKE_DATE(fy, 9, 30)
        WHEN 3 THEN MAKE_DATE(fy, 12, 31)
        WHEN 4 THEN MAKE_DATE(fy+1, 3, 31)
    END AS period_end,
    (MAKE_DATE(fy, 4, 1) <= CURRENT_DATE AND MAKE_DATE(fy+1, 3, 31) >= CURRENT_DATE) AS is_current
FROM
    generate_series(2018, 2030) AS fy,
    generate_series(1, 4)       AS q
ON CONFLICT (fiscal_year, quarter) DO NOTHING;


-- ── Payment batches (FY 2020-21 to 2025-26, one per month) ───────────────────
-- Generates 72 batch rows (April 2020 – March 2026) using generate_series
INSERT INTO payment_batches (
    batch_reference, payment_month, payment_year, fiscal_year, fiscal_year_label,
    batch_status, total_beneficiaries, total_amount, initiated_at, completed_at
)
SELECT
    'BATCH/' || gs::TEXT || '/' || LPAD(gm::TEXT, 2, '0') AS batch_reference,
    gm::SMALLINT  AS payment_month,
    gs::SMALLINT  AS payment_year,
    CASE WHEN gm >= 4 THEN gs ELSE gs - 1 END AS fiscal_year,
    CASE WHEN gm >= 4
         THEN gs::TEXT || '-' || (gs+1-2000)::TEXT
         ELSE (gs-1)::TEXT || '-' || (gs-2000)::TEXT
    END AS fiscal_year_label,
    'Completed' AS batch_status,
    140000 AS total_beneficiaries,
    700000000.00 AS total_amount,   -- ~Rs 70 cr / month placeholder
    MAKE_DATE(gs, gm, 1)::TIMESTAMPTZ AS initiated_at,
    (MAKE_DATE(gs, gm, 1) + INTERVAL '3 days')::TIMESTAMPTZ AS completed_at
FROM
    generate_series(2020, 2026) AS gs,
    generate_series(1, 12)      AS gm
WHERE
    MAKE_DATE(gs, gm, 1) >= DATE '2020-04-01'
    AND MAKE_DATE(gs, gm, 1) <= CURRENT_DATE
ORDER BY gs, gm
ON CONFLICT (payment_year, payment_month) DO NOTHING;


-- ── Sample officers (roles only — no personal data) ───────────────────────────
INSERT INTO officers (officer_code, full_name, designation, department) VALUES
    ('OFC-DSW-01', 'District Social Welfare Officer',    'DSWO',      'Social Welfare'),
    ('OFC-DSW-02', 'Assistant Social Welfare Officer',   'ASWO',      'Social Welfare'),
    ('OFC-DSW-03', 'Data Entry Operator',                'DEO',       'Social Welfare'),
    ('OFC-DSW-04', 'Accounts Officer',                   'AO',        'Finance'),
    ('OFC-DSW-05', 'Taluka Social Welfare Officer',      'TSWO',      'Social Welfare')
ON CONFLICT (officer_code) DO NOTHING;

"""

REFRESH_VIEWS = """
REFRESH MATERIALIZED VIEW payment_summary_monthly;
REFRESH MATERIALIZED VIEW beneficiary_summary_current;
REFRESH MATERIALIZED VIEW beneficiary_registration_summary_monthly;
"""


# ── SQL splitter (dollar-quote aware) ─────────────────────────────────────────

def _split_sql(sql: str) -> list[str]:
    """Split a SQL script on semicolons, but NOT inside $$ ... $$ blocks."""
    statements: list[str] = []
    current: list[str] = []
    in_dollar_block = False

    for line in sql.splitlines():
        stripped_for_check = re.sub(r'--.*$', '', line)   # ignore comments
        current.append(line)

        # Count $$ occurrences to track enter/exit of dollar-quoted blocks
        dollar_count = stripped_for_check.count('$$')
        if dollar_count % 2 == 1:
            in_dollar_block = not in_dollar_block

        # Only split on semicolons when we're NOT inside a $$ block
        if not in_dollar_block and ';' in stripped_for_check:
            stmt = '\n'.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []

    # Catch any trailing text
    if current:
        stmt = '\n'.join(current).strip()
        if stmt:
            statements.append(stmt)

    return statements


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    url = fix_url(get_url())
    print("\n[INFO] Connecting to Neon…")

    try:
        conn = psycopg2.connect(url)
        conn.autocommit = False
        cur = conn.cursor()
        print("[OK]   Connected.\n")
    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")
        sys.exit(1)

    steps = [
        ("Dropping existing objects",       DROP_ALL),
        ("Creating extensions and tables",  DDL),
        ("Seeding reference data",          SEED_SQL),
    ]

    for label, sql in steps:
        print(f"[...] {label}…")
        statements = _split_sql(sql)
        for stmt in statements:
            try:
                cur.execute(stmt)
                conn.commit()
            except Exception as e:
                conn.rollback()
                # DROP IF EXISTS failures are non-fatal
                if label.startswith("Dropping"):
                    print(f"[WARN] {e}")
                    continue
                print(f"[ERROR] {label} failed: {e}")
                print(f"[ERROR] Failed statement:\n{stmt[:800]}")
                cur.close()
                conn.close()
                sys.exit(1)
        print(f"[OK]   {label} done.")

    # Refresh materialized views (will be empty until data is loaded — that's fine)
    print("[...] Refreshing materialized views…")
    conn.autocommit = True
    for stmt in _split_sql(REFRESH_VIEWS):
        try:
            cur.execute(stmt)
            print(f"  [OK] {stmt.strip()[:60]}")
        except Exception as e:
            # View may be empty on first run — not fatal
            print(f"  [WARN] View refresh skipped (no data yet): {e}")

    cur.close()
    conn.close()

    print("\n" + "═" * 60)
    print("  Schema setup complete!")
    print("═" * 60)
    print("""
Tables created:
  districts              — North Goa, South Goa
  talukas                — 12 Goa talukas
  villages               — (empty, load from your data)
  banks                  — (empty, load from your data)
  categories             — 12 DSSY categories with amounts
  category_amount_history— amount change timeline per category
  fiscal_periods         — FY 2018-2030, April-March quarters
  officers               — 5 role-level entries
  beneficiaries          — core table (age auto-computed from DOB)
  beneficiary_status_history — every status change with timestamp
  scheme_enrollments     — enrollment start/end per category
  payments               — with direct payment_date (no date_id join!) + batch_id FK
  payment_batches        — one ECS batch row per month (FY 2020-21 onwards)
  life_certificates      — annual life-cert submissions; payment_suspended flag
  payment_forecasts      — stored forecast outputs for minister dashboards
  analytics_query_log    — NL question / SQL / chart execution trace
  dashboard_views        — saved dynamic dashboard definitions
  audit_log              — INSERT/UPDATE/DELETE tracking

Materialized views:
  payment_summary_monthly — pre-aggregated by year/month/district/category
  beneficiary_summary_current — current chart-ready beneficiary slices
  beneficiary_registration_summary_monthly — registration trend slices

RAG & session tables:
  document_chunks        — pgvector 768-dim, hybrid search ready
  conversation_context   — multi-turn session history

Next steps:
  1. Seed synthetic data:  python scripts/seed_dssy.py
  2. Start the app:        python -m backend.main
""")


if __name__ == "__main__":
    run()
