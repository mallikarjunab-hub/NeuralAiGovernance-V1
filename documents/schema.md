# DSSY Database Schema Reference

## Neural AI Governance v3.0 — Neon PostgreSQL

**Database:** Neon PostgreSQL (Serverless)
**Extensions:** pgvector (768-dim embeddings), pg_trgm (fuzzy name search)
**Total Seeded Rows:** ~875,000
**Schema Source:** `scripts/create_neon_schema.py`
**Seed Script:** `scripts/seed_dssy.py`

---

## Quick Reference

| # | Table | Rows | Purpose |
|---|-------|------|---------|
| 1 | `beneficiaries` | 300,000 | Core beneficiary records |
| 2 | `districts` | 2 | North Goa, South Goa |
| 3 | `talukas` | 12 | 6 per district |
| 4 | `villages` | 121 | Real Goa villages |
| 5 | `categories` | 12 | DSSY scheme categories |
| 6 | `banks` | 10 | Banks used for pension ECS |
| 7 | `payments` | ~35,000 | Individual payment records (last 6 months only) |
| 8 | `payment_summary` | ~1,680 | Pre-aggregated payments (6 years, by year/month/district/taluka/category) |
| 9 | `payment_batches` | 72 | Monthly ECS batch runs (FY 2020-21 to 2025-26) |
| 10 | `life_certificates` | ~130,000 | Annual compliance certificates (2022-2025) |
| 11 | `scheme_enrollments` | ~90,000 | Category enrollment history (30% have prior category) |
| 12 | `beneficiary_status_history` | ~318,000 | Status transitions (Active/Inactive/Deceased) |
| 13 | `fiscal_periods` | ~52 | April-March fiscal year quarters (2018-2030) |
| 14 | `officers` | ~20 | Admins who process approvals |
| 15 | `category_amount_history` | ~20 | Monthly amount changes over years |
| 16 | `payment_forecasts` | 0 | Placeholder for ARIMA/Prophet forecast outputs |
| 17 | `audit_log` | auto | Trigger-populated audit trail for beneficiaries |
| 18 | `document_chunks` | auto | RAG: pgvector embeddings for scheme knowledge |
| 19 | `conversation_context` | auto | Multi-turn chat session memory |
| 20 | `analytics_query_log` | 0 | NL query tracing (question -> SQL -> result) |
| 21 | `dashboard_views` | 0 | Saved dynamic dashboard presets |

### Materialized Views

| View | Purpose |
|------|---------|
| `payment_summary_monthly` | Auto-aggregated from raw `payments` table by year/month/district/category |
| `beneficiary_summary_current` | Current-state aggregate by status/gender/district/taluka/category |
| `beneficiary_registration_summary_monthly` | Registration trend by year/month/district/category/status |

---

## Extensions

```sql
CREATE EXTENSION IF NOT EXISTS vector;      -- pgvector for 768-dim embeddings
CREATE EXTENSION IF NOT EXISTS pg_trgm;     -- Trigram for ILIKE fast search on names
```

---

## Table Details

### 1. `beneficiaries` — 300,000 rows

The core table. Every DSSY beneficiary has one row.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `beneficiary_id` | SERIAL | **PK** | Auto-increment ID |
| `beneficiary_code` | VARCHAR(20) | UNIQUE, NOT NULL | Format: `BEN-00001` |
| `first_name` | VARCHAR(100) | NOT NULL | First name |
| `last_name` | VARCHAR(100) | | Last name |
| `gender` | VARCHAR(10) | NOT NULL, CHECK | `'Male'`, `'Female'`, `'Other'` |
| `date_of_birth` | DATE | | Birth date |
| `age` | INTEGER | | Current age |
| `district_id` | INTEGER | **FK** -> districts, NOT NULL | North Goa (1) or South Goa (2) |
| `taluka_id` | INTEGER | **FK** -> talukas, NOT NULL | 1-12 |
| `village_id` | INTEGER | **FK** -> villages | 1-121 |
| `address` | TEXT | | Full address (**PII** — never SELECT in AI queries) |
| `pincode` | VARCHAR(10) | | Postal code |
| `category_id` | INTEGER | **FK** -> categories, NOT NULL | Scheme category (1-12) |
| `current_monthly_amount` | DECIMAL(10,2) | | Rs/month — auto-synced from category via trigger |
| `bank_id` | INTEGER | **FK** -> banks | Bank where pension is deposited |
| `account_number` | VARCHAR(30) | | Bank account number (**PII** — never SELECT) |
| `ifsc_code` | VARCHAR(20) | | Bank IFSC |
| `aadhaar_number` | VARCHAR(12) | UNIQUE | 12-digit Aadhaar (**PII** — never SELECT) |
| `phone_number` | VARCHAR(15) | | Mobile number (**PII** — never SELECT) |
| `registration_date` | DATE | NOT NULL | When they enrolled in DSSY |
| `registration_year` | INTEGER | GENERATED ALWAYS AS STORED | Auto-derived: `DATE_PART('year', registration_date)` |
| `registered_by` | INTEGER | **FK** -> officers | Officer who registered |
| `status` | VARCHAR(20) | NOT NULL, CHECK, DEFAULT `'Active'` | `'Active'`, `'Inactive'`, `'Deceased'` |
| `status_changed_at` | TIMESTAMPTZ | | When status last changed |
| `status_changed_by` | INTEGER | **FK** -> officers | Officer who changed status |
| `inactivation_reason` | VARCHAR(200) | | Why inactive/deceased |
| `is_digitised` | BOOLEAN | DEFAULT TRUE | Legacy digitisation flag |
| `digitised_on` | DATE | | When record was digitised |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | Row creation timestamp |
| `updated_at` | TIMESTAMPTZ | DEFAULT NOW() | Auto-updated via trigger |

**Expected Counts (seeded):**
- Total: ~300,000
- Active: ~282,000 (94%)
- Inactive: ~12,600 (4.2%)
- Deceased: ~6,000 (2%)
- North Goa: ~47%, South Goa: ~53%

**Indexes:**
- `idx_ben_status` — status
- `idx_ben_district` — district_id
- `idx_ben_taluka` — taluka_id
- `idx_ben_village` — village_id
- `idx_ben_category` — category_id
- `idx_ben_gender` — gender
- `idx_ben_reg_year` — registration_year
- `idx_ben_reg_date` — registration_date
- `idx_ben_dob` — date_of_birth
- `idx_ben_age` — age
- `idx_ben_status_dist` — (status, district_id) composite
- `idx_ben_status_cat` — (status, category_id) composite
- `idx_ben_status_tal` — (status, taluka_id) composite
- `idx_ben_fname_trgm` — GIN trigram on first_name (fuzzy search)
- `idx_ben_lname_trgm` — GIN trigram on last_name (fuzzy search)

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS beneficiaries (
    beneficiary_id      SERIAL PRIMARY KEY,
    beneficiary_code    VARCHAR(20)  UNIQUE NOT NULL,
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100),
    gender              VARCHAR(10)  NOT NULL CHECK (gender IN ('Male','Female','Other')),
    date_of_birth       DATE,
    age                 INTEGER,
    district_id         INTEGER      NOT NULL REFERENCES districts(district_id),
    taluka_id           INTEGER      NOT NULL REFERENCES talukas(taluka_id),
    village_id          INTEGER      REFERENCES villages(village_id),
    address             TEXT,
    pincode             VARCHAR(10),
    category_id         INTEGER      NOT NULL REFERENCES categories(category_id),
    current_monthly_amount DECIMAL(10,2),
    bank_id             INTEGER      REFERENCES banks(bank_id),
    account_number      VARCHAR(30),
    ifsc_code           VARCHAR(20),
    aadhaar_number      VARCHAR(12)  UNIQUE,
    phone_number        VARCHAR(15),
    registration_date   DATE         NOT NULL,
    registration_year   INTEGER GENERATED ALWAYS AS (DATE_PART('year', registration_date)::INTEGER) STORED,
    registered_by       INTEGER      REFERENCES officers(officer_id),
    status              VARCHAR(20)  NOT NULL DEFAULT 'Active'
                            CHECK (status IN ('Active','Inactive','Deceased')),
    status_changed_at   TIMESTAMPTZ,
    status_changed_by   INTEGER      REFERENCES officers(officer_id),
    inactivation_reason VARCHAR(200),
    is_digitised        BOOLEAN      DEFAULT TRUE,
    digitised_on        DATE,
    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  DEFAULT NOW()
);
```

---

### 2. `districts` — 2 rows

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `district_id` | SERIAL | **PK** | 1 = North Goa, 2 = South Goa |
| `district_code` | VARCHAR(10) | UNIQUE, NOT NULL | `'NGO'`, `'SGO'` |
| `district_name` | VARCHAR(100) | NOT NULL | `'North Goa'`, `'South Goa'` |
| `state` | VARCHAR(100) | DEFAULT `'Goa'` | Always `'Goa'` |
| `is_active` | BOOLEAN | DEFAULT TRUE | Whether district is active |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | Row creation timestamp |

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS districts (
    district_id     SERIAL PRIMARY KEY,
    district_code   VARCHAR(10)  UNIQUE NOT NULL,
    district_name   VARCHAR(100) NOT NULL,
    state           VARCHAR(100) DEFAULT 'Goa',
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);
```

---

### 3. `talukas` — 12 rows

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `taluka_id` | SERIAL | **PK** | 1-12 |
| `taluka_code` | VARCHAR(20) | UNIQUE, NOT NULL | e.g. `'TK-TISWADI'` |
| `taluka_name` | VARCHAR(100) | NOT NULL | e.g. `'Tiswadi'`, `'Bardez'` |
| `district_id` | INTEGER | **FK** -> districts, NOT NULL | 1 or 2 |
| `is_active` | BOOLEAN | DEFAULT TRUE | Whether taluka is active |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | Row creation timestamp |

**Values:**
- North Goa (district_id=1): Tiswadi, Bardez, Pernem, Bicholim, Satari, Ponda
- South Goa (district_id=2): Salcete, Mormugao, Quepem, Sanguem, Canacona, Dharbandora

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS talukas (
    taluka_id       SERIAL PRIMARY KEY,
    taluka_code     VARCHAR(20)  UNIQUE NOT NULL,
    taluka_name     VARCHAR(100) NOT NULL,
    district_id     INTEGER      NOT NULL REFERENCES districts(district_id),
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);
```

---

### 4. `villages` — 121 rows

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `village_id` | SERIAL | **PK** | 1-121 |
| `village_code` | VARCHAR(10) | UNIQUE | e.g. `'VIL-001'` |
| `village_name` | VARCHAR(150) | NOT NULL | Real Goa village names |
| `taluka_id` | INTEGER | **FK** -> talukas, NOT NULL | Parent taluka |
| `population` | INTEGER | | Village population |
| `pincode` | VARCHAR(10) | | Postal code |
| `is_active` | BOOLEAN | DEFAULT TRUE | Whether village is active |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | Row creation timestamp |

**DDL:**

```sql
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
```

---

### 5. `categories` — 12 rows

DSSY scheme beneficiary categories with current monthly pension amounts.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `category_id` | SERIAL | **PK** | 1-12 |
| `category_code` | VARCHAR(20) | UNIQUE, NOT NULL | `'SC'`, `'WD'`, `'SW'`, etc. |
| `category_name` | VARCHAR(100) | NOT NULL | Full name |
| `description` | TEXT | | Category description |
| `current_monthly_amount` | DECIMAL(10,2) | NOT NULL | Rs/month |
| `disability_percentage` | INTEGER | | NULL for non-disability; 40, 80, 90 |
| `is_active` | BOOLEAN | DEFAULT TRUE | Whether category is still active |
| `introduced_year` | INTEGER | | Year category was added to DSSY |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | Row creation timestamp |
| `updated_at` | TIMESTAMPTZ | DEFAULT NOW() | Auto-updated via trigger |

**Seeded Values:**

| ID | Code | Name | Rs/Month | Disability % | Introduced | Active Count (~) |
|----|------|------|----------|--------------|------------|-----------------|
| 1 | SC | Senior Citizen | 2,000 | — | 2001 | 163,560 |
| 2 | WD | Widow | 2,000 | — | 2001 | 70,500 |
| 3 | SW | Single Woman | 2,000 | — | 2001 | 16,920 |
| 4 | DIS-40 | Disabled 40% | 2,000 | 40 | 2001 | 11,844 |
| 5 | DIS-80 | Disabled 80% | 2,500 | 80 | 2001 | 6,486 |
| 6 | DIS-90 | Disabled 90% | 3,000 | 90 | 2013 | 4,230 |
| 7 | HIV | HIV/AIDS | 2,500 | — | 2001 | 8,460 |
| 8 | LEPROSY | Leprosy | 2,000 | — | 2016 | sparse |
| 9 | DEAF | Deaf and Dumb | 2,000 | — | 2016 | sparse |
| 10 | CANCER | Cancer Patient | 3,500 | — | 2021 | sparse |
| 11 | KIDNEY | Kidney Failure | 3,500 | — | 2021 | sparse |
| 12 | SICKLE | Sickle Cell | 2,000 | — | 2021 | sparse |

> Note: Categories 1-7 are the primary seeded categories. Categories 8-12 exist in the schema but are sparsely seeded.

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS categories (
    category_id     SERIAL PRIMARY KEY,
    category_code   VARCHAR(20)  UNIQUE NOT NULL,
    category_name   VARCHAR(100) NOT NULL,
    description     TEXT,
    current_monthly_amount  DECIMAL(10,2) NOT NULL,
    disability_percentage   INTEGER,
    is_active       BOOLEAN      DEFAULT TRUE,
    introduced_year INTEGER,
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  DEFAULT NOW()
);
```

---

### 6. `banks` — 10 rows

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `bank_id` | SERIAL | **PK** | 1-10 |
| `bank_name` | VARCHAR(150) | NOT NULL | e.g. `'State Bank of India'` |
| `ifsc_prefix` | VARCHAR(10) | | e.g. `'SBIN'` |
| `branch_name` | VARCHAR(150) | | Branch name |
| `city` | VARCHAR(100) | | City |
| `is_active` | BOOLEAN | DEFAULT TRUE | Whether bank is active |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | Row creation timestamp |

**Seeded Values:**

| ID | Bank Name | IFSC Prefix | Branch | City |
|----|-----------|-------------|--------|------|
| 1 | State Bank of India | SBIN | Panaji Main | Panaji |
| 2 | Bank of Baroda | BARB | Margao Branch | Margao |
| 3 | Corporation Bank | CORP | Mapusa Branch | Mapusa |
| 4 | Canara Bank | CNRB | Vasco Branch | Vasco da Gama |
| 5 | Union Bank of India | UBIN | Ponda Branch | Ponda |
| 6 | Bank of India | BKID | Panaji Branch | Panaji |
| 7 | Central Bank of India | CBIN | Margao Branch | Margao |
| 8 | Indian Overseas Bank | IOBA | Mapusa Branch | Mapusa |
| 9 | Punjab National Bank | PUNB | Panaji Branch | Panaji |
| 10 | Goa State Co-op Bank | GSCB | Head Office | Panaji |

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS banks (
    bank_id         SERIAL PRIMARY KEY,
    bank_name       VARCHAR(150) NOT NULL,
    ifsc_prefix     VARCHAR(10),
    branch_name     VARCHAR(150),
    city            VARCHAR(100),
    is_active       BOOLEAN      DEFAULT TRUE,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);
```

---

### 7. `payments` — ~35,000 rows

Individual payment records. **Only last 6 months of data** — use `payment_summary` for historical/yearly queries.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `payment_id` | SERIAL | **PK** | |
| `beneficiary_id` | INTEGER | **FK** -> beneficiaries, NOT NULL | |
| `payment_date` | DATE | NOT NULL | Actual payment date |
| `payment_month` | SMALLINT | GENERATED ALWAYS AS STORED | Auto: `DATE_PART('month', payment_date)` |
| `payment_year` | SMALLINT | GENERATED ALWAYS AS STORED | Auto: `DATE_PART('year', payment_date)` |
| `fiscal_period_id` | INTEGER | **FK** -> fiscal_periods | Auto-assigned via trigger |
| `amount` | DECIMAL(10,2) | NOT NULL | Amount paid |
| `expected_amount` | DECIMAL(10,2) | | What should have been paid (for shortfall queries) |
| `status` | VARCHAR(20) | NOT NULL, CHECK, DEFAULT `'Paid'` | `'Paid'`, `'Pending'`, `'Failed'`, `'Reversed'` |
| `payment_method` | VARCHAR(50) | DEFAULT `'bank_transfer'` | |
| `transaction_id` | VARCHAR(100) | UNIQUE | Transaction reference |
| `bank_id` | INTEGER | **FK** -> banks | |
| `batch_id` | INTEGER | **FK** -> payment_batches | Linked ECS batch (added via ALTER TABLE) |
| `processed_by` | INTEGER | **FK** -> officers | |
| `remarks` | TEXT | | |
| `failure_reason` | VARCHAR(200) | | Reason for failed payment |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | |

**Indexes:**
- `idx_pay_ben_id` — beneficiary_id
- `idx_pay_date` — payment_date
- `idx_pay_year` — payment_year
- `idx_pay_month` — payment_month
- `idx_pay_year_month` — (payment_year, payment_month)
- `idx_pay_status` — status
- `idx_pay_status_year` — (status, payment_year)
- `idx_pay_fiscal` — fiscal_period_id
- `idx_pay_batch` — batch_id

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS payments (
    payment_id          SERIAL PRIMARY KEY,
    beneficiary_id      INTEGER      NOT NULL REFERENCES beneficiaries(beneficiary_id),
    payment_date        DATE         NOT NULL,
    payment_month       SMALLINT     GENERATED ALWAYS AS (DATE_PART('month', payment_date)::SMALLINT) STORED,
    payment_year        SMALLINT     GENERATED ALWAYS AS (DATE_PART('year', payment_date)::SMALLINT) STORED,
    fiscal_period_id    INTEGER      REFERENCES fiscal_periods(fiscal_period_id),
    amount              DECIMAL(10,2) NOT NULL,
    expected_amount     DECIMAL(10,2),
    status              VARCHAR(20)  NOT NULL DEFAULT 'Paid'
                            CHECK (status IN ('Paid','Pending','Failed','Reversed')),
    payment_method      VARCHAR(50)  DEFAULT 'bank_transfer',
    transaction_id      VARCHAR(100) UNIQUE,
    bank_id             INTEGER      REFERENCES banks(bank_id),
    processed_by        INTEGER      REFERENCES officers(officer_id),
    remarks             TEXT,
    failure_reason      VARCHAR(200),
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

-- batch_id added via ALTER TABLE after payment_batches is created
ALTER TABLE payments ADD COLUMN IF NOT EXISTS batch_id INTEGER REFERENCES payment_batches(batch_id);
```

---

### 8. `payment_summary` — ~1,680 rows

**The AI's primary table for payment queries.** Pre-aggregated by year/month/district/taluka/category. Covers 6 fiscal years (FY 2020-21 through FY 2025-26).

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `summary_id` | SERIAL | **PK** | |
| `payment_year` | SMALLINT | NOT NULL | e.g. 2024 |
| `payment_month` | SMALLINT | NOT NULL | 1-12 |
| `month_name` | VARCHAR(15) | | e.g. `'January'` |
| `fiscal_year` | INTEGER | | e.g. 2024 = FY 2024-25 |
| `fiscal_year_label` | VARCHAR(10) | | e.g. `'2024-25'` |
| `quarter` | SMALLINT | | 1-4 |
| `district_id` | INTEGER | **FK** -> districts | |
| `taluka_id` | INTEGER | **FK** -> talukas | |
| `category_id` | INTEGER | **FK** -> categories | |
| `total_beneficiaries` | INTEGER | DEFAULT 0 | Total in this slice |
| `paid_count` | INTEGER | DEFAULT 0 | Successfully paid |
| `pending_count` | INTEGER | DEFAULT 0 | Payment pending |
| `failed_count` | INTEGER | DEFAULT 0 | Payment failed |
| `on_hold_count` | INTEGER | DEFAULT 0 | On hold |
| `total_base_amount` | NUMERIC(18,2) | DEFAULT 0 | Expected total amount |
| `total_net_amount` | NUMERIC(18,2) | DEFAULT 0 | Actually paid amount |
| `male_count` | INTEGER | DEFAULT 0 | Male beneficiaries |
| `female_count` | INTEGER | DEFAULT 0 | Female beneficiaries |
| `last_updated_at` | TIMESTAMPTZ | DEFAULT NOW() | |

**Key Rule:** Always use this table (not `payments`) for:
- "Last 3 years payments"
- "Year-over-year comparison"
- "District-wise payout by year"
- "Category-wise payment trend"
- Any query spanning more than 6 months

**Indexes:**
- `idx_ps_year` — payment_year
- `idx_ps_year_month` — (payment_year, payment_month)
- `idx_ps_district` — district_id
- `idx_ps_category` — category_id
- `idx_ps_taluka` — taluka_id
- `idx_ps_composite` — (payment_year, payment_month, district_id, category_id)

**DDL:**

```sql
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
```

---

### 9. `payment_batches` — 72 rows

One row per monthly ECS disbursement run. 12 months x 6 fiscal years.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `batch_id` | SERIAL | **PK** | |
| `batch_reference` | VARCHAR(30) | UNIQUE, NOT NULL | e.g. `'BATCH/2024/04'` |
| `payment_month` | SMALLINT | NOT NULL, CHECK 1-12 | |
| `payment_year` | SMALLINT | NOT NULL | |
| `fiscal_year` | INTEGER | | e.g. 2024 = FY 2024-25 |
| `fiscal_year_label` | VARCHAR(10) | | e.g. `'2024-25'` |
| `batch_status` | VARCHAR(20) | NOT NULL, CHECK, DEFAULT `'Completed'` | `'Draft'`, `'Processing'`, `'Completed'`, `'Failed'`, `'Reversed'` |
| `total_beneficiaries` | INTEGER | DEFAULT 0 | |
| `total_amount` | NUMERIC(18,2) | DEFAULT 0 | |
| `paid_count` | INTEGER | DEFAULT 0 | |
| `failed_count` | INTEGER | DEFAULT 0 | |
| `pending_count` | INTEGER | DEFAULT 0 | |
| `initiated_by` | INTEGER | **FK** -> officers | |
| `initiated_at` | TIMESTAMPTZ | | |
| `completed_at` | TIMESTAMPTZ | | |
| `remarks` | TEXT | | |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | |

**Unique Constraint:** (payment_year, payment_month) — one batch per month

**Indexes:**
- `idx_pb_year` — payment_year
- `idx_pb_year_month` — (payment_year, payment_month)
- `idx_pb_status` — batch_status
- `idx_pb_fiscal` — fiscal_year

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS payment_batches (
    batch_id            SERIAL PRIMARY KEY,
    batch_reference     VARCHAR(30)   UNIQUE NOT NULL,
    payment_month       SMALLINT      NOT NULL CHECK (payment_month BETWEEN 1 AND 12),
    payment_year        SMALLINT      NOT NULL,
    fiscal_year         INTEGER,
    fiscal_year_label   VARCHAR(10),
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
```

---

### 10. `life_certificates` — ~130,000 rows

Annual compliance: every active beneficiary must submit in April/May. Covers 2022-2025.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `cert_id` | SERIAL | **PK** | |
| `beneficiary_id` | INTEGER | **FK** -> beneficiaries, NOT NULL | |
| `submission_date` | DATE | NOT NULL | When submitted |
| `due_month` | SMALLINT | NOT NULL, DEFAULT 4 | 4 = April |
| `due_year` | SMALLINT | NOT NULL | Year the cert covers |
| `fiscal_year` | INTEGER | | FY the cert covers |
| `is_late_submission` | BOOLEAN | NOT NULL, DEFAULT FALSE | TRUE if after May 31 |
| `days_late` | INTEGER | DEFAULT 0 | 0 if on time |
| `issued_by_type` | VARCHAR(50) | CHECK | `'Bank_Manager'`, `'Gazetted_Officer'`, `'Aadhaar_eKYC'`, `'Post_Office'`, `'Tahsildar'`, `'Other'` |
| `verified_by` | INTEGER | **FK** -> officers | |
| `verification_date` | DATE | | |
| `payment_suspended` | BOOLEAN | NOT NULL, DEFAULT FALSE | TRUE = payments stopped |
| `suspension_date` | DATE | | When suspended |
| `reinstatement_date` | DATE | | When payments resumed |
| `remarks` | TEXT | | |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | |

**Unique Constraint:** (beneficiary_id, due_year) — one cert per beneficiary per year

**Indexes:**
- `idx_lc_ben` — beneficiary_id
- `idx_lc_due_year` — due_year
- `idx_lc_sub_date` — submission_date
- `idx_lc_suspended` — payment_suspended (partial: WHERE payment_suspended = TRUE)
- `idx_lc_late` — is_late_submission (partial: WHERE is_late_submission = TRUE)
- `idx_lc_year_ben` — (due_year, beneficiary_id)

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS life_certificates (
    cert_id             SERIAL PRIMARY KEY,
    beneficiary_id      INTEGER       NOT NULL REFERENCES beneficiaries(beneficiary_id),
    submission_date     DATE          NOT NULL,
    due_month           SMALLINT      NOT NULL DEFAULT 4,
    due_year            SMALLINT      NOT NULL,
    fiscal_year         INTEGER,
    is_late_submission  BOOLEAN       NOT NULL DEFAULT FALSE,
    days_late           INTEGER       DEFAULT 0,
    issued_by_type      VARCHAR(50)
                            CHECK (issued_by_type IN (
                                'Bank_Manager','Gazetted_Officer','Aadhaar_eKYC',
                                'Post_Office','Tahsildar','Other'
                            )),
    verified_by         INTEGER       REFERENCES officers(officer_id),
    verification_date   DATE,
    payment_suspended   BOOLEAN       NOT NULL DEFAULT FALSE,
    suspension_date     DATE,
    reinstatement_date  DATE,
    remarks             TEXT,
    created_at          TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE (beneficiary_id, due_year)
);
```

---

### 11. `scheme_enrollments` — ~90,000 rows

Tracks category enrollment history. 30% of beneficiaries have a prior category (e.g. Widow -> Senior Citizen on turning 60).

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `enrollment_id` | SERIAL | **PK** | |
| `beneficiary_id` | INTEGER | **FK** -> beneficiaries, NOT NULL | |
| `category_id` | INTEGER | **FK** -> categories, NOT NULL | |
| `enrollment_date` | DATE | NOT NULL | |
| `enrollment_year` | INTEGER | GENERATED ALWAYS AS STORED | Auto: `DATE_PART('year', enrollment_date)` |
| `end_date` | DATE | | NULL = currently enrolled |
| `end_reason` | VARCHAR(200) | | Why enrollment ended |
| `monthly_amount_at_enrollment` | DECIMAL(10,2) | | Amount locked at enrollment time |
| `approved_by` | INTEGER | **FK** -> officers | |
| `is_current` | BOOLEAN | DEFAULT TRUE | |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | |

**Unique Partial Index:** `uq_enroll_one_current` — one current enrollment per beneficiary (`WHERE is_current = TRUE`)

**Indexes:**
- `idx_enroll_ben` — beneficiary_id
- `idx_enroll_cat` — category_id
- `idx_enroll_date` — enrollment_date
- `idx_enroll_year` — enrollment_year
- `idx_enroll_current` — is_current (partial: WHERE is_current = TRUE)

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS scheme_enrollments (
    enrollment_id       SERIAL PRIMARY KEY,
    beneficiary_id      INTEGER      NOT NULL REFERENCES beneficiaries(beneficiary_id),
    category_id         INTEGER      NOT NULL REFERENCES categories(category_id),
    enrollment_date     DATE         NOT NULL,
    enrollment_year     INTEGER GENERATED ALWAYS AS (DATE_PART('year', enrollment_date)::INTEGER) STORED,
    end_date            DATE,
    end_reason          VARCHAR(200),
    monthly_amount_at_enrollment DECIMAL(10,2),
    approved_by         INTEGER      REFERENCES officers(officer_id),
    is_current          BOOLEAN      DEFAULT TRUE,
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_enroll_one_current
    ON scheme_enrollments(beneficiary_id)
    WHERE is_current = TRUE;
```

---

### 12. `beneficiary_status_history` — ~318,000 rows

Every status transition: initial registration + any Active->Inactive->Deceased changes. Auto-populated by trigger on `beneficiaries`.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `id` | SERIAL | **PK** | |
| `beneficiary_id` | INTEGER | **FK** -> beneficiaries, NOT NULL | |
| `old_status` | VARCHAR(20) | | NULL for initial insert |
| `new_status` | VARCHAR(20) | NOT NULL | `'Active'`, `'Inactive'`, `'Deceased'` |
| `changed_at` | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | |
| `changed_by` | INTEGER | **FK** -> officers | |
| `reason` | VARCHAR(200) | | Reason for change |
| `remarks` | TEXT | | |

**Indexes:**
- `idx_bsh_ben` — beneficiary_id
- `idx_bsh_date` — changed_at
- `idx_bsh_status` — new_status
- `idx_bsh_year` — expression index on `DATE_PART('year', changed_at AT TIME ZONE 'UTC')`

**DDL:**

```sql
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
```

---

### 13. `fiscal_periods` — ~52 rows

April-March fiscal year quarters. Allows any payment_date to be mapped to fiscal context.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `fiscal_period_id` | SERIAL | **PK** | |
| `fiscal_year` | INTEGER | NOT NULL | e.g. 2024 = FY 2024-25 |
| `fiscal_year_label` | VARCHAR(10) | NOT NULL | e.g. `'2024-25'` |
| `quarter` | SMALLINT | NOT NULL, CHECK 1-4 | Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar |
| `quarter_label` | VARCHAR(10) | NOT NULL | e.g. `'Q1 FY25'` |
| `period_start` | DATE | NOT NULL | Quarter start date |
| `period_end` | DATE | NOT NULL | Quarter end date |
| `is_current` | BOOLEAN | DEFAULT FALSE | |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | |

**Unique Constraint:** (fiscal_year, quarter)

**Indexes:**
- `idx_fp_start` — period_start
- `idx_fp_end` — period_end

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS fiscal_periods (
    fiscal_period_id    SERIAL PRIMARY KEY,
    fiscal_year         INTEGER      NOT NULL,
    fiscal_year_label   VARCHAR(10)  NOT NULL,
    quarter             SMALLINT     NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    quarter_label       VARCHAR(10)  NOT NULL,
    period_start        DATE         NOT NULL,
    period_end          DATE         NOT NULL,
    is_current          BOOLEAN      DEFAULT FALSE,
    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (fiscal_year, quarter)
);
```

---

### 14. `officers` — ~20 rows

Administrative officers who process approvals, verify certificates, etc.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `officer_id` | SERIAL | **PK** | |
| `officer_code` | VARCHAR(20) | UNIQUE, NOT NULL | |
| `full_name` | VARCHAR(150) | NOT NULL | |
| `designation` | VARCHAR(100) | | e.g. `'DSWO'`, `'ASWO'`, `'DEO'` |
| `department` | VARCHAR(100) | DEFAULT `'Social Welfare'` | |
| `district_id` | INTEGER | **FK** -> districts | |
| `taluka_id` | INTEGER | **FK** -> talukas | |
| `email` | VARCHAR(150) | | |
| `is_active` | BOOLEAN | DEFAULT TRUE | |
| `joined_date` | DATE | | When the officer joined |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | |

**Seeded Values:**

| Code | Name (Role) | Designation | Department |
|------|-------------|-------------|------------|
| OFC-DSW-01 | District Social Welfare Officer | DSWO | Social Welfare |
| OFC-DSW-02 | Assistant Social Welfare Officer | ASWO | Social Welfare |
| OFC-DSW-03 | Data Entry Operator | DEO | Social Welfare |
| OFC-DSW-04 | Accounts Officer | AO | Finance |
| OFC-DSW-05 | Taluka Social Welfare Officer | TSWO | Social Welfare |

**DDL:**

```sql
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
```

---

### 15. `category_amount_history` — ~20 rows

Tracks when monthly pension amounts changed (e.g. Senior Citizen increase in 2013 and 2020).

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `id` | SERIAL | **PK** | |
| `category_id` | INTEGER | **FK** -> categories, NOT NULL | |
| `monthly_amount` | DECIMAL(10,2) | NOT NULL | |
| `effective_from` | DATE | NOT NULL | |
| `effective_to` | DATE | | NULL = currently active rate |
| `changed_by` | VARCHAR(100) | | |
| `reason` | TEXT | | |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | |

**Unique Constraint:** (category_id, effective_from)

**Indexes:**
- `idx_cat_amt_hist_cat` — category_id
- `idx_cat_amt_hist_date` — effective_from

**Seeded Example (Senior Citizen):**

| Amount | Effective From | Effective To | Reason |
|--------|---------------|-------------|--------|
| Rs 1,000 | 2001-01-01 | 2012-12-31 | Original amount at scheme launch |
| Rs 1,500 | 2013-01-01 | 2019-12-31 | Revised as per 2013 notification |
| Rs 2,000 | 2020-01-01 | NULL (current) | Current rate effective from 2020 digitisation |

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS category_amount_history (
    id              SERIAL PRIMARY KEY,
    category_id     INTEGER      NOT NULL REFERENCES categories(category_id),
    monthly_amount  DECIMAL(10,2) NOT NULL,
    effective_from  DATE         NOT NULL,
    effective_to    DATE,
    changed_by      VARCHAR(100),
    reason          TEXT,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_cat_amt_hist_effective
    ON category_amount_history(category_id, effective_from);
```

---

### 16. `payment_forecasts` — 0 rows (placeholder)

For storing externally generated forecast outputs (ARIMA, Prophet, regression).

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `forecast_id` | SERIAL | **PK** | |
| `forecast_type` | VARCHAR(50) | NOT NULL | `'payment'`, `'beneficiaries'`, `'category_liability'` |
| `forecast_grain` | VARCHAR(30) | NOT NULL | `'monthly'`, `'quarterly'`, `'yearly'` |
| `model_name` | VARCHAR(100) | NOT NULL | |
| `version_tag` | VARCHAR(40) | | |
| `district_id` | INTEGER | **FK** -> districts | |
| `taluka_id` | INTEGER | **FK** -> talukas | |
| `category_id` | INTEGER | **FK** -> categories | |
| `forecast_period_start` | DATE | NOT NULL | |
| `forecast_period_end` | DATE | NOT NULL | |
| `predicted_value` | NUMERIC(18,2) | NOT NULL | |
| `lower_bound` | NUMERIC(18,2) | | |
| `upper_bound` | NUMERIC(18,2) | | |
| `confidence_pct` | NUMERIC(5,2) | | |
| `input_snapshot_date` | DATE | | |
| `metadata` | JSONB | DEFAULT `'{}'` | |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | |

**Indexes:**
- `idx_pf_type_period` — (forecast_type, forecast_period_start)
- `idx_pf_district` — district_id
- `idx_pf_taluka` — taluka_id
- `idx_pf_category` — category_id

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS payment_forecasts (
    forecast_id          SERIAL PRIMARY KEY,
    forecast_type        VARCHAR(50)   NOT NULL,
    forecast_grain       VARCHAR(30)   NOT NULL,
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
```

---

### 17. `audit_log` — auto-populated

Trigger-populated. Every INSERT/UPDATE/DELETE on `beneficiaries` is logged here.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `id` | BIGSERIAL | **PK** | |
| `table_name` | VARCHAR(50) | NOT NULL | Always `'beneficiaries'` for now |
| `record_id` | INTEGER | NOT NULL | beneficiary_id |
| `action` | VARCHAR(10) | NOT NULL, CHECK | `'INSERT'`, `'UPDATE'`, `'DELETE'` |
| `changed_fields` | JSONB | | e.g. `{"status": ["Active","Inactive"]}` |
| `old_values` | JSONB | | Full old row as JSON |
| `new_values` | JSONB | | Full new row as JSON |
| `performed_by` | INTEGER | **FK** -> officers | |
| `performed_at` | TIMESTAMPTZ | DEFAULT NOW() | |
| `ip_address` | INET | | |
| `session_info` | TEXT | | |

**Indexes:**
- `idx_audit_table` — table_name
- `idx_audit_record` — (table_name, record_id)
- `idx_audit_date` — performed_at
- `idx_audit_action` — action
- `idx_audit_year` — expression index on `DATE_PART('year', performed_at AT TIME ZONE 'UTC')`

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS audit_log (
    id              BIGSERIAL    PRIMARY KEY,
    table_name      VARCHAR(50)  NOT NULL,
    record_id       INTEGER      NOT NULL,
    action          VARCHAR(10)  NOT NULL CHECK (action IN ('INSERT','UPDATE','DELETE')),
    changed_fields  JSONB,
    old_values      JSONB,
    new_values      JSONB,
    performed_by    INTEGER      REFERENCES officers(officer_id),
    performed_at    TIMESTAMPTZ  DEFAULT NOW(),
    ip_address      INET,
    session_info    TEXT
);
```

---

### 18. `document_chunks` — auto-populated (RAG)

pgvector table for DSSY knowledge base. Populated at app startup from `documents/dssy_knowledge_base.md` + web sources.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `id` | SERIAL | **PK** | |
| `doc_name` | VARCHAR(200) | NOT NULL | e.g. `'DSSY_Knowledge_Base'`, `'WEB_DSSY_Official_Page'` |
| `chunk_index` | INTEGER | NOT NULL | Position within document |
| `chunk_text` | TEXT | NOT NULL | The actual text chunk |
| `embedding` | vector(768) | | Gemini embedding-001 vector |
| `metadata` | JSONB | DEFAULT `'{}'` | |
| `search_vector` | TSVECTOR | GENERATED ALWAYS AS STORED | Auto: `to_tsvector('english', chunk_text)` for BM25 keyword search |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | |

**Unique Constraint:** (doc_name, chunk_index)

**Indexes:**
- `doc_chunks_emb_idx` — IVFFlat on embedding (vector_cosine_ops, lists=50)
- `doc_chunks_search_idx` — GIN on search_vector
- `doc_chunks_doc_idx` — doc_name

**DDL:**

```sql
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

CREATE INDEX IF NOT EXISTS doc_chunks_emb_idx
    ON document_chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 50);
CREATE INDEX IF NOT EXISTS doc_chunks_search_idx
    ON document_chunks USING GIN (search_vector);
```

---

### 19. `conversation_context` — auto-populated

Multi-turn chat memory. L1 cache is in-memory; this is the L2 persistent store.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `id` | SERIAL | **PK** | |
| `session_id` | TEXT | NOT NULL | Browser session ID |
| `question` | TEXT | | Original user question |
| `resolved_question` | TEXT | | Standalone resolved version |
| `answer` | TEXT | | AI response |
| `intent` | TEXT | | `'SQL'`, `'RAG'`, `'EDGE'` |
| `sql_data` | TEXT | | JSON-encoded query results |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | |

**Indexes:**
- `idx_conv_session` — (session_id, created_at DESC)

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS conversation_context (
    id                  SERIAL PRIMARY KEY,
    session_id          TEXT         NOT NULL,
    question            TEXT,
    resolved_question   TEXT,
    answer              Text,
    intent              TEXT,
    sql_data            TEXT,
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_conv_session
    ON conversation_context(session_id, created_at DESC);
```

---

### 20. `analytics_query_log` — 0 rows (placeholder)

Traceability for NL queries: what was asked, how it was routed, what SQL was generated.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `id` | BIGSERIAL | **PK** | |
| `session_id` | TEXT | | |
| `user_question` | TEXT | NOT NULL | |
| `resolved_question` | TEXT | | |
| `intent` | VARCHAR(20) | | `'SQL'`, `'RAG'`, `'EDGE'`, `'FORECAST'` |
| `generated_sql` | TEXT | | |
| `chart_type` | VARCHAR(20) | | |
| `route_status` | VARCHAR(20) | | `'success'`, `'fallback'`, `'failed'` |
| `row_count` | INTEGER | DEFAULT 0 | |
| `execution_time_ms` | INTEGER | | |
| `confidence_label` | VARCHAR(20) | | |
| `result_preview` | JSONB | | |
| `error_message` | TEXT | | |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | |

**Indexes:**
- `idx_aql_session` — (session_id, created_at DESC)
- `idx_aql_intent` — (intent, created_at DESC)
- `idx_aql_status` — (route_status, created_at DESC)

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS analytics_query_log (
    id                  BIGSERIAL PRIMARY KEY,
    session_id          TEXT,
    user_question       TEXT         NOT NULL,
    resolved_question   TEXT,
    intent              VARCHAR(20),
    generated_sql       TEXT,
    chart_type          VARCHAR(20),
    route_status        VARCHAR(20),
    row_count           INTEGER      DEFAULT 0,
    execution_time_ms   INTEGER,
    confidence_label    VARCHAR(20),
    result_preview      JSONB,
    error_message       TEXT,
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);
```

---

### 21. `dashboard_views` — 0 rows (placeholder)

Saved dynamic dashboard presets so officers can save/re-open chart configurations.

| Column | Type | Constraint | Description |
|--------|------|------------|-------------|
| `dashboard_view_id` | SERIAL | **PK** | |
| `view_name` | VARCHAR(150) | NOT NULL | |
| `view_scope` | VARCHAR(30) | DEFAULT `'department'` | `'department'`, `'officer'`, `'minister'` |
| `created_by` | INTEGER | **FK** -> officers | |
| `nl_prompt` | TEXT | | The NL question that generated this view |
| `generated_sql` | TEXT | | |
| `chart_type` | VARCHAR(20) | | |
| `layout_config` | JSONB | DEFAULT `'{}'` | |
| `filter_config` | JSONB | DEFAULT `'{}'` | |
| `is_default` | BOOLEAN | DEFAULT FALSE | |
| `is_active` | BOOLEAN | DEFAULT TRUE | |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | |
| `updated_at` | TIMESTAMPTZ | DEFAULT NOW() | Auto-updated via trigger |

**Indexes:**
- `idx_dv_scope` — (view_scope, is_active)
- `idx_dv_created_by` — created_by

**DDL:**

```sql
CREATE TABLE IF NOT EXISTS dashboard_views (
    dashboard_view_id    SERIAL PRIMARY KEY,
    view_name            VARCHAR(150) NOT NULL,
    view_scope           VARCHAR(30)  DEFAULT 'department',
    created_by           INTEGER      REFERENCES officers(officer_id),
    nl_prompt            TEXT,
    generated_sql        Text,
    chart_type           VARCHAR(20),
    layout_config        JSONB        DEFAULT '{}'::jsonb,
    filter_config        JSONB        DEFAULT '{}'::jsonb,
    is_default           BOOLEAN      DEFAULT FALSE,
    is_active            BOOLEAN      DEFAULT TRUE,
    created_at           TIMESTAMPTZ  DEFAULT NOW(),
    updated_at           TIMESTAMPTZ  DEFAULT NOW()
);
```

---

## Materialized Views

### `payment_summary_monthly`

Auto-aggregates raw `payments` table by year/month/district/category. Cross-check source for `payment_summary`.

| Column | Type | Description |
|--------|------|-------------|
| `payment_year` | SMALLINT | Year from payment_date |
| `payment_month` | SMALLINT | Month from payment_date |
| `year_month` | TEXT | `'YYYY-MM'` formatted |
| `district_id` | INTEGER | FK -> districts |
| `district_name` | VARCHAR(100) | Denormalized |
| `category_id` | INTEGER | FK -> categories |
| `category_name` | VARCHAR(100) | Denormalized |
| `payment_count` | BIGINT | Total payments |
| `paid_count` | BIGINT | Paid status count |
| `pending_count` | BIGINT | Pending status count |
| `failed_count` | BIGINT | Failed status count |
| `total_amount` | NUMERIC | Sum of all amounts |
| `paid_amount` | NUMERIC | Sum of paid amounts |
| `pending_amount` | NUMERIC | Sum of pending amounts |
| `unique_beneficiaries` | BIGINT | Distinct beneficiary count |
| `compliance_pct` | NUMERIC | `paid / total * 100` |

**Unique Index:** `idx_psm_unique` — (payment_year, payment_month, district_id, category_id)

**DDL:**

```sql
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
        COUNT(*) FILTER (WHERE p.status = 'paid') * 100.0 / NULLIF(COUNT(*), 0), 2
    ) AS compliance_pct
FROM payments p
JOIN beneficiaries b ON p.beneficiary_id = b.beneficiary_id
JOIN districts     d ON b.district_id    = d.district_id
JOIN categories    c ON b.category_id    = c.category_id
GROUP BY
    p.payment_year, p.payment_month,
    DATE_TRUNC('month', p.payment_date),
    b.district_id, d.district_name,
    b.category_id, c.category_name;
```

---

### `beneficiary_summary_current`

Current-state aggregate for dashboard charts. No time dimension — just current snapshot.

| Column | Type | Description |
|--------|------|-------------|
| `status` | VARCHAR(20) | Active/Inactive/Deceased |
| `gender` | VARCHAR(10) | Male/Female/Other |
| `district_id` | INTEGER | FK -> districts |
| `district_name` | VARCHAR(100) | Denormalized |
| `taluka_id` | INTEGER | FK -> talukas |
| `taluka_name` | VARCHAR(100) | Denormalized |
| `category_id` | INTEGER | FK -> categories |
| `category_name` | VARCHAR(100) | Denormalized |
| `beneficiary_count` | BIGINT | Count in this slice |
| `avg_age` | NUMERIC(10,2) | Average age |
| `total_monthly_liability` | NUMERIC | Sum of monthly amounts |

**Indexes:** status, district_id, taluka_id, category_id

**DDL:**

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS beneficiary_summary_current AS
SELECT
    b.status, b.gender,
    b.district_id, d.district_name,
    b.taluka_id, t.taluka_name,
    b.category_id, c.category_name,
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
```

---

### `beneficiary_registration_summary_monthly`

Registration trend view for time-series charts and YoY comparisons.

| Column | Type | Description |
|--------|------|-------------|
| `registration_year` | INTEGER | Year from registration_date |
| `registration_month` | INTEGER | Month from registration_date |
| `year_month` | TEXT | `'YYYY-MM'` formatted |
| `district_id` | INTEGER | FK -> districts |
| `district_name` | VARCHAR(100) | Denormalized |
| `category_id` | INTEGER | FK -> categories |
| `category_name` | VARCHAR(100) | Denormalized |
| `status` | VARCHAR(20) | Beneficiary status at time of query |
| `registrations` | BIGINT | Count of registrations |

**Indexes:** registration_year, (registration_year, registration_month), district_id, category_id

**DDL:**

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS beneficiary_registration_summary_monthly AS
SELECT
    EXTRACT(YEAR FROM b.registration_date)::INTEGER  AS registration_year,
    EXTRACT(MONTH FROM b.registration_date)::INTEGER AS registration_month,
    TO_CHAR(DATE_TRUNC('month', b.registration_date), 'YYYY-MM') AS year_month,
    b.district_id, d.district_name,
    b.category_id, c.category_name,
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
```

---

## Relationships (ER Diagram — Text)

```
districts (2)
  +-- talukas (12)                    FK: talukas.district_id -> districts.district_id
  |     +-- villages (121)            FK: villages.taluka_id -> talukas.taluka_id
  +-- beneficiaries (300k)            FK: beneficiaries.district_id -> districts.district_id
  +-- payment_summary (1.7k)          FK: payment_summary.district_id -> districts.district_id
  +-- payment_forecasts               FK: payment_forecasts.district_id -> districts.district_id
  +-- officers (20)                   FK: officers.district_id -> districts.district_id

categories (12)
  +-- beneficiaries (300k)            FK: beneficiaries.category_id -> categories.category_id
  +-- payment_summary (1.7k)          FK: payment_summary.category_id -> categories.category_id
  +-- scheme_enrollments (90k)        FK: scheme_enrollments.category_id -> categories.category_id
  +-- category_amount_history (20)    FK: category_amount_history.category_id -> categories.category_id
  +-- payment_forecasts               FK: payment_forecasts.category_id -> categories.category_id

beneficiaries (300k)
  +-- payments (35k)                  FK: payments.beneficiary_id -> beneficiaries.beneficiary_id
  +-- life_certificates (130k)        FK: life_certificates.beneficiary_id -> beneficiaries.beneficiary_id
  +-- scheme_enrollments (90k)        FK: scheme_enrollments.beneficiary_id -> beneficiaries.beneficiary_id
  +-- beneficiary_status_history (318k) FK: ...beneficiary_id -> beneficiaries.beneficiary_id

banks (10)
  +-- beneficiaries (300k)            FK: beneficiaries.bank_id -> banks.bank_id
  +-- payments (35k)                  FK: payments.bank_id -> banks.bank_id

payment_batches (72)
  +-- payments (35k)                  FK: payments.batch_id -> payment_batches.batch_id

fiscal_periods (52)
  +-- payments (35k)                  FK: payments.fiscal_period_id -> fiscal_periods.fiscal_period_id

officers (20)
  +-- beneficiaries.registered_by
  +-- beneficiaries.status_changed_by
  +-- scheme_enrollments.approved_by
  +-- life_certificates.verified_by
  +-- payment_batches.initiated_by
  +-- payments.processed_by
  +-- audit_log.performed_by
  +-- dashboard_views.created_by
```

---

## Triggers & Functions

### PL/pgSQL Functions

| Function | Purpose |
|----------|---------|
| `set_updated_at()` | Sets `NEW.updated_at = NOW()` on row update |
| `sync_beneficiary_monthly_amount()` | Copies `current_monthly_amount` from category if NULL on beneficiary |
| `assign_payment_fiscal_period()` | Auto-assigns `fiscal_period_id` by looking up `payment_date` in `fiscal_periods` |
| `log_beneficiary_status_change()` | Inserts into `beneficiary_status_history` on INSERT or status UPDATE |
| `audit_beneficiaries()` | Inserts full row JSON into `audit_log` on any INSERT/UPDATE/DELETE |

### Trigger Assignments

| Trigger | Table | Event | Function |
|---------|-------|-------|----------|
| `trg_categories_updated_at` | categories | BEFORE UPDATE | `set_updated_at()` |
| `trg_beneficiaries_updated_at` | beneficiaries | BEFORE UPDATE | `set_updated_at()` |
| `trg_dashboard_views_updated_at` | dashboard_views | BEFORE UPDATE | `set_updated_at()` |
| `trg_beneficiaries_sync_amount` | beneficiaries | BEFORE INSERT OR UPDATE OF category_id, current_monthly_amount | `sync_beneficiary_monthly_amount()` |
| `trg_payments_assign_fiscal_period` | payments | BEFORE INSERT OR UPDATE OF payment_date | `assign_payment_fiscal_period()` |
| `trg_beneficiaries_status_history_insert` | beneficiaries | AFTER INSERT | `log_beneficiary_status_change()` |
| `trg_beneficiaries_status_history_update` | beneficiaries | AFTER UPDATE OF status, status_changed_at, status_changed_by, inactivation_reason | `log_beneficiary_status_change()` |
| `trg_beneficiaries_audit_insert` | beneficiaries | AFTER INSERT | `audit_beneficiaries()` |
| `trg_beneficiaries_audit_update` | beneficiaries | AFTER UPDATE | `audit_beneficiaries()` |
| `trg_beneficiaries_audit_delete` | beneficiaries | AFTER DELETE | `audit_beneficiaries()` |

### Function DDL

```sql
-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Sync beneficiary monthly amount from category
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

-- Auto-assign fiscal period from payment date
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

-- Log beneficiary status changes to history table
CREATE OR REPLACE FUNCTION log_beneficiary_status_change()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO beneficiary_status_history (
            beneficiary_id, old_status, new_status, changed_at, changed_by, reason, remarks
        ) VALUES (
            NEW.beneficiary_id, NULL, NEW.status,
            COALESCE(NEW.status_changed_at, NOW()),
            NEW.status_changed_by, NEW.inactivation_reason,
            'Initial beneficiary status'
        );
        RETURN NEW;
    END IF;
    IF OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO beneficiary_status_history (
            beneficiary_id, old_status, new_status, changed_at, changed_by, reason, remarks
        ) VALUES (
            NEW.beneficiary_id, OLD.status, NEW.status,
            COALESCE(NEW.status_changed_at, NOW()),
            NEW.status_changed_by, NEW.inactivation_reason,
            'Status changed through beneficiaries table'
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Full audit trail for beneficiaries table
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
        INSERT INTO audit_log (table_name, record_id, action, new_values, performed_by, session_info)
        VALUES ('beneficiaries', NEW.beneficiary_id, 'INSERT', to_jsonb(NEW), actor_id, 'schema_trigger');
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log (table_name, record_id, action, old_values, new_values, changed_fields, performed_by, session_info)
        VALUES ('beneficiaries', NEW.beneficiary_id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW),
            jsonb_build_object(
                'status', jsonb_build_array(OLD.status, NEW.status),
                'category_id', jsonb_build_array(OLD.category_id, NEW.category_id),
                'district_id', jsonb_build_array(OLD.district_id, NEW.district_id),
                'taluka_id', jsonb_build_array(OLD.taluka_id, NEW.taluka_id)
            ), actor_id, 'schema_trigger');
        RETURN NEW;
    ELSE
        INSERT INTO audit_log (table_name, record_id, action, old_values, performed_by, session_info)
        VALUES ('beneficiaries', OLD.beneficiary_id, 'DELETE', to_jsonb(OLD), actor_id, 'schema_trigger');
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;
```

---

## Materialized View Refresh

```sql
REFRESH MATERIALIZED VIEW payment_summary_monthly;
REFRESH MATERIALIZED VIEW beneficiary_summary_current;
REFRESH MATERIALIZED VIEW beneficiary_registration_summary_monthly;
```

> Views should be refreshed after bulk data loads (e.g. after running `seed_dssy.py`).

---

## Status Value Reference

| Table | Column | Valid Values |
|-------|--------|-------------|
| `beneficiaries` | `status` | `'Active'`, `'Inactive'`, `'Deceased'` |
| `payments` | `status` | `'Paid'`, `'Pending'`, `'Failed'`, `'Reversed'` |
| `payment_batches` | `batch_status` | `'Draft'`, `'Processing'`, `'Completed'`, `'Failed'`, `'Reversed'` |
| `beneficiaries` | `gender` | `'Male'`, `'Female'`, `'Other'` |
| `life_certificates` | `issued_by_type` | `'Bank_Manager'`, `'Gazetted_Officer'`, `'Aadhaar_eKYC'`, `'Post_Office'`, `'Tahsildar'`, `'Other'` |

> All status values are **Title Case**. Never use lowercase in WHERE clauses.

---

## PII Columns (Never SELECT in AI Queries)

| Table | Column | Reason |
|-------|--------|--------|
| `beneficiaries` | `aadhaar_number` | 12-digit national ID |
| `beneficiaries` | `phone_number` | Personal mobile |
| `beneficiaries` | `address` | Home address |
| `beneficiaries` | `account_number` | Bank account |

---

## UNIQUE Constraints Summary

| Table | Columns | Type |
|-------|---------|------|
| `beneficiaries` | `beneficiary_code` | UNIQUE |
| `beneficiaries` | `aadhaar_number` | UNIQUE |
| `districts` | `district_code` | UNIQUE |
| `talukas` | `taluka_code` | UNIQUE |
| `villages` | `village_code` | UNIQUE |
| `categories` | `category_code` | UNIQUE |
| `officers` | `officer_code` | UNIQUE |
| `payments` | `transaction_id` | UNIQUE |
| `payment_batches` | `batch_reference` | UNIQUE |
| `payment_batches` | `(payment_year, payment_month)` | UNIQUE composite |
| `life_certificates` | `(beneficiary_id, due_year)` | UNIQUE composite |
| `scheme_enrollments` | `(beneficiary_id)` WHERE is_current=TRUE | UNIQUE partial |
| `document_chunks` | `(doc_name, chunk_index)` | UNIQUE composite |
| `category_amount_history` | `(category_id, effective_from)` | UNIQUE composite |
| `fiscal_periods` | `(fiscal_year, quarter)` | UNIQUE composite |

---

## CHECK Constraints Summary

| Table | Column | Constraint |
|-------|--------|-----------|
| `beneficiaries` | `gender` | `IN ('Male','Female','Other')` |
| `beneficiaries` | `status` | `IN ('Active','Inactive','Deceased')` |
| `payments` | `status` | `IN ('Paid','Pending','Failed','Reversed')` |
| `payment_batches` | `batch_status` | `IN ('Draft','Processing','Completed','Failed','Reversed')` |
| `payment_batches` | `payment_month` | `BETWEEN 1 AND 12` |
| `fiscal_periods` | `quarter` | `BETWEEN 1 AND 4` |
| `life_certificates` | `issued_by_type` | `IN ('Bank_Manager','Gazetted_Officer','Aadhaar_eKYC','Post_Office','Tahsildar','Other')` |
| `audit_log` | `action` | `IN ('INSERT','UPDATE','DELETE')` |

---

## Complete Index Reference

### beneficiaries (15 indexes)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_ben_status` | status | B-tree |
| `idx_ben_district` | district_id | B-tree |
| `idx_ben_taluka` | taluka_id | B-tree |
| `idx_ben_village` | village_id | B-tree |
| `idx_ben_category` | category_id | B-tree |
| `idx_ben_gender` | gender | B-tree |
| `idx_ben_reg_year` | registration_year | B-tree |
| `idx_ben_reg_date` | registration_date | B-tree |
| `idx_ben_dob` | date_of_birth | B-tree |
| `idx_ben_age` | age | B-tree |
| `idx_ben_status_dist` | (status, district_id) | B-tree composite |
| `idx_ben_status_cat` | (status, category_id) | B-tree composite |
| `idx_ben_status_tal` | (status, taluka_id) | B-tree composite |
| `idx_ben_fname_trgm` | first_name | GIN trigram |
| `idx_ben_lname_trgm` | last_name | GIN trigram |

### payments (9 indexes)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_pay_ben_id` | beneficiary_id | B-tree |
| `idx_pay_date` | payment_date | B-tree |
| `idx_pay_year` | payment_year | B-tree |
| `idx_pay_month` | payment_month | B-tree |
| `idx_pay_year_month` | (payment_year, payment_month) | B-tree composite |
| `idx_pay_status` | status | B-tree |
| `idx_pay_status_year` | (status, payment_year) | B-tree composite |
| `idx_pay_fiscal` | fiscal_period_id | B-tree |
| `idx_pay_batch` | batch_id | B-tree |

### payment_summary (6 indexes)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_ps_year` | payment_year | B-tree |
| `idx_ps_year_month` | (payment_year, payment_month) | B-tree composite |
| `idx_ps_district` | district_id | B-tree |
| `idx_ps_category` | category_id | B-tree |
| `idx_ps_taluka` | taluka_id | B-tree |
| `idx_ps_composite` | (payment_year, payment_month, district_id, category_id) | B-tree composite |

### payment_batches (4 indexes)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_pb_year` | payment_year | B-tree |
| `idx_pb_year_month` | (payment_year, payment_month) | B-tree composite |
| `idx_pb_status` | batch_status | B-tree |
| `idx_pb_fiscal` | fiscal_year | B-tree |

### life_certificates (6 indexes)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_lc_ben` | beneficiary_id | B-tree |
| `idx_lc_due_year` | due_year | B-tree |
| `idx_lc_sub_date` | submission_date | B-tree |
| `idx_lc_suspended` | payment_suspended | B-tree partial (WHERE TRUE) |
| `idx_lc_late` | is_late_submission | B-tree partial (WHERE TRUE) |
| `idx_lc_year_ben` | (due_year, beneficiary_id) | B-tree composite |

### scheme_enrollments (5 indexes + 1 unique partial)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_enroll_ben` | beneficiary_id | B-tree |
| `idx_enroll_cat` | category_id | B-tree |
| `idx_enroll_date` | enrollment_date | B-tree |
| `idx_enroll_year` | enrollment_year | B-tree |
| `idx_enroll_current` | is_current | B-tree partial (WHERE TRUE) |
| `uq_enroll_one_current` | beneficiary_id | UNIQUE partial (WHERE is_current = TRUE) |

### beneficiary_status_history (4 indexes)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_bsh_ben` | beneficiary_id | B-tree |
| `idx_bsh_date` | changed_at | B-tree |
| `idx_bsh_status` | new_status | B-tree |
| `idx_bsh_year` | `DATE_PART('year', changed_at)` | B-tree expression |

### category_amount_history (2 indexes + 1 unique)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_cat_amt_hist_cat` | category_id | B-tree |
| `idx_cat_amt_hist_date` | effective_from | B-tree |
| `uq_cat_amt_hist_effective` | (category_id, effective_from) | UNIQUE |

### fiscal_periods (2 indexes)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_fp_start` | period_start | B-tree |
| `idx_fp_end` | period_end | B-tree |

### audit_log (5 indexes)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_audit_table` | table_name | B-tree |
| `idx_audit_record` | (table_name, record_id) | B-tree composite |
| `idx_audit_date` | performed_at | B-tree |
| `idx_audit_action` | action | B-tree |
| `idx_audit_year` | `DATE_PART('year', performed_at)` | B-tree expression |

### payment_forecasts (4 indexes)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_pf_type_period` | (forecast_type, forecast_period_start) | B-tree composite |
| `idx_pf_district` | district_id | B-tree |
| `idx_pf_taluka` | taluka_id | B-tree |
| `idx_pf_category` | category_id | B-tree |

### document_chunks (3 indexes)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `doc_chunks_emb_idx` | embedding | IVFFlat (cosine, lists=50) |
| `doc_chunks_search_idx` | search_vector | GIN |
| `doc_chunks_doc_idx` | doc_name | B-tree |

### conversation_context (1 index)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_conv_session` | (session_id, created_at DESC) | B-tree composite |

### analytics_query_log (3 indexes)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_aql_session` | (session_id, created_at DESC) | B-tree composite |
| `idx_aql_intent` | (intent, created_at DESC) | B-tree composite |
| `idx_aql_status` | (route_status, created_at DESC) | B-tree composite |

### dashboard_views (2 indexes)
| Index Name | Column(s) | Type |
|------------|-----------|------|
| `idx_dv_scope` | (view_scope, is_active) | B-tree composite |
| `idx_dv_created_by` | created_by | B-tree |

### Materialized View Indexes
| Index Name | View | Column(s) | Type |
|------------|------|-----------|------|
| `idx_psm_unique` | payment_summary_monthly | (payment_year, payment_month, district_id, category_id) | UNIQUE |
| `idx_psm_year` | payment_summary_monthly | payment_year | B-tree |
| `idx_psm_year_month` | payment_summary_monthly | (payment_year, payment_month) | B-tree |
| `idx_psm_district` | payment_summary_monthly | district_id | B-tree |
| `idx_psm_category` | payment_summary_monthly | category_id | B-tree |
| `idx_bsc_status` | beneficiary_summary_current | status | B-tree |
| `idx_bsc_district` | beneficiary_summary_current | district_id | B-tree |
| `idx_bsc_taluka` | beneficiary_summary_current | taluka_id | B-tree |
| `idx_bsc_category` | beneficiary_summary_current | category_id | B-tree |
| `idx_brsm_year` | beneficiary_registration_summary_monthly | registration_year | B-tree |
| `idx_brsm_year_month` | beneficiary_registration_summary_monthly | (registration_year, registration_month) | B-tree |
| `idx_brsm_district` | beneficiary_registration_summary_monthly | district_id | B-tree |
| `idx_brsm_category` | beneficiary_registration_summary_monthly | category_id | B-tree |

---

## Common Query Patterns

```sql
-- Category breakdown
SELECT c.category_name, COUNT(*) AS count
FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id
WHERE b.status = 'Active' GROUP BY c.category_name ORDER BY count DESC;

-- District comparison
SELECT d.district_name, COUNT(*) AS count
FROM beneficiaries b JOIN districts d ON b.district_id = d.district_id
WHERE b.status = 'Active' GROUP BY d.district_name;

-- YoY payments (use payment_summary, NOT payments)
SELECT ps.payment_year AS year, SUM(ps.total_net_amount) AS total_paid
FROM payment_summary ps GROUP BY ps.payment_year ORDER BY ps.payment_year;

-- Life cert compliance by taluka
SELECT t.taluka_name, COUNT(b.beneficiary_id) AS total,
       COUNT(lc.cert_id) AS submitted,
       ROUND(COUNT(lc.cert_id) * 100.0 / NULLIF(COUNT(b.beneficiary_id), 0), 2) AS pct
FROM beneficiaries b
JOIN talukas t ON b.taluka_id = t.taluka_id
LEFT JOIN life_certificates lc ON lc.beneficiary_id = b.beneficiary_id AND lc.due_year = 2025
WHERE b.status = 'Active' GROUP BY t.taluka_name ORDER BY pct ASC;

-- Status history (who became inactive in 2024)
SELECT COUNT(*) FROM beneficiary_status_history
WHERE new_status = 'Inactive' AND DATE_PART('year', changed_at) = 2024;

-- Fuzzy name search (uses trigram GIN index)
SELECT beneficiary_code, first_name, last_name
FROM beneficiaries
WHERE first_name ILIKE '%raj%' OR last_name ILIKE '%raj%'
LIMIT 20;

-- Monthly payment trend by category (last 3 fiscal years)
SELECT ps.fiscal_year_label, c.category_name,
       SUM(ps.total_net_amount) AS total_paid,
       SUM(ps.paid_count) AS total_paid_count
FROM payment_summary ps
JOIN categories c ON ps.category_id = c.category_id
WHERE ps.fiscal_year >= 2022
GROUP BY ps.fiscal_year_label, c.category_name
ORDER BY ps.fiscal_year_label, total_paid DESC;

-- Enrollment transfers (beneficiaries who changed category)
SELECT b.beneficiary_code, c_old.category_name AS from_category,
       c_new.category_name AS to_category, se.enrollment_date
FROM scheme_enrollments se
JOIN beneficiaries b ON se.beneficiary_id = b.beneficiary_id
JOIN categories c_old ON se.category_id = c_old.category_id
JOIN scheme_enrollments se_new ON se_new.beneficiary_id = se.beneficiary_id
    AND se_new.is_current = TRUE AND se.is_current = FALSE
JOIN categories c_new ON se_new.category_id = c_new.category_id
WHERE se.end_date IS NOT NULL
LIMIT 20;
```

---

## Seed Data Summary

| Data | Rows | Source |
|------|------|--------|
| districts | 2 | North Goa, South Goa |
| talukas | 12 | 6 per district (real Goa talukas) |
| villages | 121 | Real Goa villages with pincodes |
| banks | 10 | SBI, BoB, Corp, Canara, Union, BoI, CBI, IOB, PNB, GSCB |
| categories | 12 | SC, WD, SW, DIS-40/80/90, HIV, LEPROSY, DEAF, CANCER, KIDNEY, SICKLE |
| officers | 5 (base) | DSWO, ASWO, DEO, AO, TSWO roles |
| fiscal_periods | 52 | FY 2018-2030, 4 quarters each |
| payment_batches | 72 | April 2020 - March 2026 (1 per month) |
| category_amount_history | 3+ | SC amount: Rs 1000 -> 1500 -> 2000 |
| beneficiaries | 300,000 | Random names, ages, genders, locations |
| scheme_enrollments | ~90,000 | 30% have prior categories |
| beneficiary_status_history | ~318,000 | Status transitions tracked |
| payments | ~35,000 | Last 6 months only |
| payment_summary | ~1,680 | Pre-aggregated 6 fiscal years |
| life_certificates | ~130,000 | 2022-2025, with compliance tracking |
| **TOTAL** | **~875,000** | Deterministic: `random.seed(42)` |

---

*Auto-generated from `scripts/create_neon_schema.py` and `scripts/seed_dssy.py`*
*Last updated: April 2026*
