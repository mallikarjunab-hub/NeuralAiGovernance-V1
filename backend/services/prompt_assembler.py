"""
Prompt Assembler — single source of truth for all LLM prompt construction.

Prompt types:
  build_question_resolver_prompt  — resolve follow-up questions to standalone
  build_intent_prompt             — classify SQL vs RAG
  build_sql_prompt                — PostgreSQL SQL generation
  build_nl_answer_prompt          — human-readable NL answer from SQL results
  build_rag_answer_prompt         — document-grounded answer from RAG chunks

Context injection (conversation history including raw data) is handled
transparently by each builder so callers don't need to format anything.
"""
import json
from backend.services.context_store import ConversationTurn

LANGS = {
    "en": "English", "hi": "Hindi", "te": "Telugu",
    "kn": "Kannada", "mr": "Marathi", "kok": "Konkani",
}

# ── Anti-hallucination counts guard ───────────────────────────────────────────
# These are the REAL expected counts in the database after seeding.
# They are injected into every SQL-generation prompt so the model never
# generates "LIMIT 7000" or "WHERE category_id IN (...)" that produces
# 4k rows when the correct answer is 174k.
#
# IMPORTANT: Update these after running seed_dssy.py if your actual data differs.

COUNTS_GUARD = """
IMPORTANT — EXPECTED DATABASE COUNTS (do NOT generate SQL that would return totals
wildly different from these without a clear filter reason):
  Total beneficiaries (all statuses)   : ~300,000
  Active beneficiaries                 : ~282,000
  Inactive beneficiaries               : ~12,600
  Deceased beneficiaries               :  ~6,000
  Category counts (Active only):
    Senior Citizen (SC)                : ~163,560  ← largest category by far
    Widow (WD)                         :  ~70,500
    Single Woman (SW)                  :  ~16,920
    Disabled 40% (DIS-40)              :  ~11,844
    Disabled 80% (DIS-80)              :   ~6,486
    Disabled 90% (DIS-90)              :   ~4,230  ← smallest (NOT ~7,000 or ~4,000)
    HIV/AIDS (HIV)                     :   ~8,460
  District split: North Goa ~47%, South Goa ~53%
  Payment batches                      :      72 (one per month, FY 2020-21 → 2025-26)
  Monthly payout (state total)         : ~Rs 65–75 crore per month
  Payment summary table has data for   : 6 years (FY 2020-21 through 2025-26)

OFFICIAL STATE OUTLAY — MARCH 2026 (authoritative government figure):
  Total disbursed (March 2026)         : ₹45.26 Crore
  Beneficiaries covered (March 2026)   : 2.24 Lakh+ (2,24,000+)
  Schemes covered                      : DSSS (Dayanand Social Security Scheme) & Griha Aadhar
  Talukas covered                      : 12 (all talukas across Goa)
  Source                               : Directorate of Social Welfare, Government of Goa
  → When asked "total amount disbursed", "total outlay", "how much was disbursed", or
    "total amount for this scheme", answer with: ₹45.26 Crore disbursed to over 2.24 lakh
    beneficiaries under DSSS and Griha Aadhar (March 2026).
  Life certificates                    : ~130,000 rows (years 2022-2025)

ANTI-HALLUCINATION RULES:
  1. NEVER add a LIMIT clause to a COUNT(*) query — COUNT returns one row, not thousands.
  2. NEVER confuse "Disabled 90%" (DIS-90) with "Disabled 40%" (DIS-40) — they are different category_codes (DIS-40, DIS-80, DIS-90).
  3. When asked "which category is lowest", run ORDER BY count ASC — the first row is the answer.
     The answer is Disabled 90%+ (~4,230), NOT Senior Citizen.
  4. "Last 3 years payments" or "compare payments" → query payment_summary table (NOT payments table).
     ALWAYS include: total_beneficiaries, paid_count, pending_count, failed_count, total_paid, success_rate_pct
     Use: SELECT ps.payment_year AS year, SUM(ps.total_beneficiaries) AS total_beneficiaries, SUM(ps.paid_count) AS paid_count, SUM(ps.pending_count) AS pending_count, SUM(ps.failed_count) AS failed_count, SUM(ps.total_net_amount) AS total_paid, ROUND(SUM(ps.paid_count)*100.0/NULLIF(SUM(ps.total_beneficiaries),0),2) AS success_rate_pct FROM payment_summary ps WHERE ps.payment_year >= EXTRACT(YEAR FROM CURRENT_DATE)::INT - 2 GROUP BY ps.payment_year ORDER BY ps.payment_year
     NEVER make up totals or hardcode amounts.
  5. Payment year filter: use the year column on payment_summary directly (it is pre-computed).
     Do NOT use EXTRACT(YEAR FROM payment_date) when year/payment_year column exists.
  6. Status values in beneficiaries table are: 'Active', 'Inactive', 'Deceased'  (Title Case).
     Payment status values are: 'Paid', 'Pending', 'Failed'  (Title Case).
  7. For "compare last 3 years payout" ALWAYS use payment_summary table, not payments table.
     The payments table only has last 6 months of individual records. payment_summary has 6 years of aggregated data.
  8. For YoY (year-over-year) comparisons, use payment_summary grouped by year.
  9. For batch-level analysis (which month had most failures), use payment_batches table.
  10. NEVER return made-up amounts like "Rs 50 crore" or "Rs 100 crore" — let the SQL return actual data.
  11. For fiscal year labels use fiscal_year_label column from payment_batches (e.g. '2024-25').
  12. Life certificate data spans 2022-2025 only. Do not query outside this range.
"""

# ── Neon PostgreSQL Schema ─────────────────────────────────────────────────────

SCHEMA = """
-- DATABASE: Neon PostgreSQL (standard SQL, no backticks, no project prefix)
-- All table names are lowercase, no schema prefix needed
-- STATUS VALUES ARE TITLE CASE: 'Active','Inactive','Deceased' for beneficiaries; 'Paid','Pending','Failed' for payments

-- CORE DIMENSION TABLES
CREATE TABLE districts (
    district_id SERIAL PRIMARY KEY,
    district_code VARCHAR(10) UNIQUE,
    district_name VARCHAR(100),  -- 'North Goa' (district_id=1), 'South Goa' (district_id=2)
    state VARCHAR(100) DEFAULT 'Goa'
);

CREATE TABLE talukas (
    taluka_id SERIAL PRIMARY KEY,
    taluka_code VARCHAR(10) UNIQUE,
    taluka_name VARCHAR(100),
    district_id INT REFERENCES districts(district_id)
);

CREATE TABLE villages (
    village_id SERIAL PRIMARY KEY,
    village_code VARCHAR(10) UNIQUE,
    village_name VARCHAR(150),
    taluka_id INT REFERENCES talukas(taluka_id),
    population INT,
    pincode VARCHAR(10)
);

CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_code VARCHAR(20) UNIQUE,  -- SC, WD, SW, DIS-40, DIS-80, DIS-90, HIV, LEPROSY, DEAF, CANCER, KIDNEY, SICKLE
    category_name VARCHAR(100),        -- 'Senior Citizen','Widow','Single Woman','Disabled 40%','Disabled 80%','Disabled 90%','HIV/AIDS','Leprosy','Deaf and Dumb','Cancer Patient','Kidney Failure','Sickle Cell'
    description TEXT,
    current_monthly_amount DECIMAL(10,2),  -- current Rs/month for this category
    disability_percentage INT,             -- NULL for non-disability; 40, 80, 90
    is_active BOOLEAN DEFAULT TRUE,
    introduced_year INT
);

CREATE TABLE banks (
    bank_id SERIAL PRIMARY KEY,
    bank_name VARCHAR(150),
    ifsc_prefix VARCHAR(10),
    branch_name VARCHAR(150),
    city VARCHAR(100)
);

-- MAIN BENEFICIARIES TABLE
CREATE TABLE beneficiaries (
    beneficiary_id SERIAL PRIMARY KEY,
    beneficiary_code VARCHAR(20) UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100),
    gender VARCHAR(10),           -- 'Male','Female','Other'
    date_of_birth DATE,
    age INT,
    district_id INT REFERENCES districts(district_id),
    taluka_id INT REFERENCES talukas(taluka_id),
    village_id INT REFERENCES villages(village_id),
    pincode VARCHAR(10),
    category_id INT REFERENCES categories(category_id),
    current_monthly_amount DECIMAL(10,2),  -- denormalised from category
    bank_id INT REFERENCES banks(bank_id),
    account_number VARCHAR(30),
    aadhaar_number VARCHAR(12) UNIQUE,     -- PII: never SELECT this
    phone_number VARCHAR(15),              -- PII: never SELECT this
    address TEXT,                           -- PII: never SELECT this
    registration_date DATE NOT NULL,
    registration_year INT,                 -- auto-derived from registration_date
    status VARCHAR(20) DEFAULT 'Active',   -- 'Active','Inactive','Deceased' (Title Case!)
    status_changed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- PAYMENTS TABLE (individual payment records — last 6 months only)
CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    beneficiary_id INT REFERENCES beneficiaries(beneficiary_id),
    payment_date DATE NOT NULL,
    payment_month SMALLINT,        -- GENERATED STORED from payment_date
    payment_year SMALLINT,         -- GENERATED STORED from payment_date
    fiscal_period_id INT REFERENCES fiscal_periods(fiscal_period_id),  -- auto-assigned via trigger
    amount DECIMAL(10,2) NOT NULL,
    expected_amount DECIMAL(10,2), -- for shortfall queries
    status VARCHAR(20),            -- 'Paid','Pending','Failed','Reversed' (Title Case!)
    payment_method VARCHAR(50),
    transaction_id VARCHAR(100),
    bank_id INT REFERENCES banks(bank_id),
    batch_id INT REFERENCES payment_batches(batch_id),  -- linked ECS batch
    processed_by INT REFERENCES officers(officer_id),
    failure_reason VARCHAR(200),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- PAYMENT SUMMARY (pre-aggregated by year/month/district/taluka/category — 6 years of data)
-- USE THIS for "last 3 years payments", "YoY comparison", "district-wise payout by year"
-- DO NOT use raw payments table for historical/yearly queries — it only has last 6 months
CREATE TABLE payment_summary (
    summary_id SERIAL PRIMARY KEY,
    payment_year INT NOT NULL,
    payment_month INT NOT NULL,
    month_name VARCHAR(15),            -- e.g. 'January'
    fiscal_year INT,
    fiscal_year_label VARCHAR(10),     -- e.g. '2024-25'
    quarter SMALLINT,                  -- 1-4
    district_id INT REFERENCES districts(district_id),
    taluka_id INT REFERENCES talukas(taluka_id),
    category_id INT REFERENCES categories(category_id),
    total_beneficiaries INT,
    paid_count INT,
    pending_count INT,
    failed_count INT,
    on_hold_count INT DEFAULT 0,
    total_base_amount DECIMAL(18,2),   -- expected total
    total_net_amount DECIMAL(18,2),    -- actually paid amount
    male_count INT,
    female_count INT
);

-- PAYMENT BATCHES (one row per monthly ECS batch run — 72 rows for 6 fiscal years)
-- Use for: "which month had most failures?", "total disbursed in April 2025 batch?"
CREATE TABLE payment_batches (
    batch_id            SERIAL PRIMARY KEY,
    batch_reference     VARCHAR(30) UNIQUE,             -- e.g. 'BATCH/2024/04'
    payment_month       SMALLINT,                       -- 1-12
    payment_year        SMALLINT,
    fiscal_year         INT,                            -- e.g. 2024 = FY 2024-25
    fiscal_year_label   VARCHAR(10),                    -- e.g. '2024-25'
    batch_status        VARCHAR(20),                    -- 'Completed','Failed','Processing'
    total_beneficiaries INT,
    total_amount        NUMERIC(18,2),
    paid_count          INT,
    failed_count        INT,
    pending_count       INT,
    initiated_at        TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ
);

-- LIFE CERTIFICATES (annual compliance — every active beneficiary submits in April/May)
-- payment_suspended = TRUE means payments stopped until cert is submitted
CREATE TABLE life_certificates (
    cert_id             SERIAL PRIMARY KEY,
    beneficiary_id      INT REFERENCES beneficiaries(beneficiary_id),
    submission_date     DATE,
    due_month           SMALLINT DEFAULT 4,             -- 4 = April
    due_year            SMALLINT,
    fiscal_year         INT,
    is_late_submission  BOOLEAN,                        -- TRUE if submitted after May 31
    days_late           INT DEFAULT 0,
    issued_by_type      VARCHAR(50),                    -- 'Bank_Manager','Gazetted_Officer','Aadhaar_eKYC'
    payment_suspended   BOOLEAN DEFAULT FALSE,          -- TRUE = payments stopped
    suspension_date     DATE,
    reinstatement_date  DATE
);

-- OFFICERS (administrative officers who process approvals)
CREATE TABLE officers (
    officer_id SERIAL PRIMARY KEY,
    officer_code VARCHAR(20) UNIQUE,
    full_name VARCHAR(150),
    designation VARCHAR(100),     -- 'DSWO','ASWO','DEO','AO','TSWO'
    department VARCHAR(100) DEFAULT 'Social Welfare',
    district_id INT REFERENCES districts(district_id),
    taluka_id INT REFERENCES talukas(taluka_id),
    is_active BOOLEAN DEFAULT TRUE
);

-- FISCAL PERIODS (April-March fiscal year quarters, 2018-2030)
CREATE TABLE fiscal_periods (
    fiscal_period_id SERIAL PRIMARY KEY,
    fiscal_year INT NOT NULL,          -- e.g. 2024 = FY 2024-25
    fiscal_year_label VARCHAR(10),     -- e.g. '2024-25'
    quarter SMALLINT CHECK (quarter BETWEEN 1 AND 4),  -- Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar
    quarter_label VARCHAR(10),         -- e.g. 'Q1 FY25'
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    is_current BOOLEAN DEFAULT FALSE,
    UNIQUE (fiscal_year, quarter)
);

-- SCHEME ENROLLMENTS (tracks category changes per beneficiary)
CREATE TABLE scheme_enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    beneficiary_id INT REFERENCES beneficiaries(beneficiary_id),
    category_id INT REFERENCES categories(category_id),
    enrollment_date DATE NOT NULL,
    enrollment_year INT,        -- GENERATED STORED from enrollment_date
    end_date DATE,              -- NULL means currently enrolled
    end_reason VARCHAR(200),
    monthly_amount_at_enrollment DECIMAL(10,2),
    approved_by INT REFERENCES officers(officer_id),
    is_current BOOLEAN DEFAULT TRUE
);

-- BENEFICIARY STATUS HISTORY (tracks Active->Inactive->Deceased transitions)
CREATE TABLE beneficiary_status_history (
    id SERIAL PRIMARY KEY,
    beneficiary_id INT REFERENCES beneficiaries(beneficiary_id),
    old_status VARCHAR(20),
    new_status VARCHAR(20),
    changed_at TIMESTAMPTZ DEFAULT NOW(),
    changed_by INT REFERENCES officers(officer_id),
    reason VARCHAR(200),
    remarks TEXT
);

-- CATEGORY AMOUNT HISTORY (tracks when monthly pension amounts changed)
CREATE TABLE category_amount_history (
    id SERIAL PRIMARY KEY,
    category_id INT REFERENCES categories(category_id),
    monthly_amount DECIMAL(10,2) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,           -- NULL = currently active rate
    reason TEXT
);

-- KEY RELATIONSHIPS FOR JOINS:
-- beneficiaries.category_id -> categories.category_id (get category_name, current_monthly_amount)
-- beneficiaries.district_id -> districts.district_id (get district_name)
-- beneficiaries.taluka_id -> talukas.taluka_id (get taluka_name)
-- beneficiaries.village_id -> villages.village_id (get village_name)
-- beneficiaries.bank_id -> banks.bank_id (get bank_name)
-- beneficiaries.registered_by -> officers.officer_id
-- payments.beneficiary_id -> beneficiaries.beneficiary_id
-- payments.batch_id -> payment_batches.batch_id
-- payments.fiscal_period_id -> fiscal_periods.fiscal_period_id
-- payment_summary: pre-aggregated by (payment_year, payment_month, district_id, taluka_id, category_id)
-- life_certificates.beneficiary_id -> beneficiaries.beneficiary_id
-- scheme_enrollments.beneficiary_id -> beneficiaries.beneficiary_id
-- beneficiary_status_history.beneficiary_id -> beneficiaries.beneficiary_id
-- category_amount_history.category_id -> categories.category_id

-- COMMON QUERY PATTERNS:
-- Beneficiary count by category: SELECT c.category_name, COUNT(*) FROM beneficiaries b JOIN categories c ON b.category_id=c.category_id WHERE b.status='Active' GROUP BY c.category_name
-- YoY payments: SELECT payment_year, SUM(total_net_amount) AS total_paid FROM payment_summary GROUP BY payment_year ORDER BY payment_year
-- District-wise active: SELECT d.district_name, COUNT(*) FROM beneficiaries b JOIN districts d ON b.district_id=d.district_id WHERE b.status='Active' GROUP BY d.district_name
-- Life cert compliance: SELECT t.taluka_name, COUNT(lc.cert_id) AS submitted, COUNT(b.beneficiary_id) AS total FROM beneficiaries b JOIN talukas t ON b.taluka_id=t.taluka_id LEFT JOIN life_certificates lc ON lc.beneficiary_id=b.beneficiary_id AND lc.due_year=2025 WHERE b.status='Active' GROUP BY t.taluka_name
-- Status history: SELECT new_status, COUNT(*) FROM beneficiary_status_history WHERE DATE_PART('year', changed_at)=2024 GROUP BY new_status
-- Category transfers: SELECT se.beneficiary_id, c.category_name, se.enrollment_date FROM scheme_enrollments se JOIN categories c ON se.category_id=c.category_id WHERE se.is_current=FALSE
"""

# ── Few-Shot Examples ──────────────────────────────────────────────────────────

SHOTS = """-- PATTERN: Simple counts (with/without status filter)
Q: total beneficiaries
SQL: SELECT COUNT(*) AS total FROM beneficiaries;

Q: active beneficiaries
SQL: SELECT COUNT(*) AS total FROM beneficiaries WHERE status='Active';

-- PATTERN: Group by dimension (district/taluka/village/category/gender/status)
Q: district wise count
SQL: SELECT d.district_name AS district, COUNT(*) AS count FROM beneficiaries b JOIN districts d ON b.district_id = d.district_id WHERE b.status='Active' GROUP BY d.district_name ORDER BY count DESC;

Q: category breakdown
SQL: SELECT c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY c.category_name ORDER BY count DESC;

Q: taluka wise active beneficiaries
SQL: SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS count FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id JOIN districts d ON b.district_id = d.district_id WHERE b.status='Active' GROUP BY t.taluka_name, d.district_name ORDER BY count DESC;

Q: talukawise breakdown beneficiaries
SQL: SELECT t.taluka_name AS taluka, c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY t.taluka_name, c.category_name ORDER BY t.taluka_name, count DESC;
-- NOTE: This returns all 7 categories (Senior Citizen, Widow, Single Woman, Disabled 40%, Disabled 80%, Disabled 90%, HIV/AIDS) for every taluka. Render as both a pivot table and a stacked bar chart with taluka on x-axis and category as stack.

Q: give talukawise breakdown beneficiaries
SQL: SELECT t.taluka_name AS taluka, c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY t.taluka_name, c.category_name ORDER BY t.taluka_name, count DESC;

Q: gender breakdown
SQL: SELECT gender, COUNT(*) AS count FROM beneficiaries WHERE status='Active' GROUP BY gender ORDER BY count DESC;

Q: status wise beneficiary count
SQL: SELECT status, COUNT(*) AS count FROM beneficiaries GROUP BY status ORDER BY count DESC;

-- PATTERN: Filter by specific category
Q: widow count
SQL: SELECT COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE c.category_name = 'Widow' AND b.status='Active';

Q: disabled 90 percent beneficiaries
SQL: SELECT c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' AND LOWER(c.category_name) LIKE '%disabled%90%' GROUP BY c.category_name;

-- PATTERN: Filter by district (district_id=1 is North Goa, district_id=2 is South Goa)
Q: taluka wise beneficiaries in north goa
SQL: SELECT t.taluka_name AS taluka, COUNT(*) AS count FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id WHERE b.district_id = 1 AND b.status='Active' GROUP BY t.taluka_name ORDER BY count DESC;

-- PATTERN: Cross-dimension breakdown (two GROUP BY columns)
Q: district and category cross breakdown
SQL: SELECT d.district_name AS district, c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN districts d ON b.district_id = d.district_id JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY d.district_name, c.category_name ORDER BY d.district_name, count DESC;

Q: category-wise male vs female count
SQL: SELECT c.category_name AS category, b.gender, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY c.category_name, b.gender ORDER BY c.category_name, count DESC;

-- PATTERN: Ranking (lowest/highest/top N)
Q: which category has the lowest beneficiaries
SQL: SELECT c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY c.category_name ORDER BY count ASC LIMIT 1;

Q: top 5 talukas by active beneficiaries
SQL: SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS count FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id JOIN districts d ON b.district_id = d.district_id WHERE b.status='Active' GROUP BY t.taluka_name, d.district_name ORDER BY count DESC LIMIT 5;

-- PATTERN: Age-based queries
Q: age distribution
SQL: SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age BETWEEN 40 AND 59 THEN '40-59' WHEN age BETWEEN 60 AND 69 THEN '60-69' WHEN age BETWEEN 70 AND 79 THEN '70-79' ELSE '80+' END AS age_group, COUNT(*) AS count FROM beneficiaries WHERE status='Active' GROUP BY age_group ORDER BY MIN(age);

Q: beneficiaries above 80
SQL: SELECT COUNT(*) AS count FROM beneficiaries WHERE age > 80 AND status='Active';

-- PATTERN: Payout/amount calculations
Q: total amount disbursed for this scheme
SQL: SELECT pb.payment_month, pb.payment_year, pb.total_amount AS total_disbursed, pb.paid_count AS beneficiaries_paid, pb.total_beneficiaries FROM payment_batches pb WHERE pb.payment_month = 3 AND pb.payment_year = 2026 AND pb.batch_status = 'Completed' LIMIT 1;
-- NOTE: If the above returns no data, report the official government figure: ₹45.26 Crore disbursed to 2.24 lakh+ beneficiaries under DSSS and Griha Aadhar for March 2026.

Q: total outlay march 2026
SQL: SELECT pb.payment_month, pb.payment_year, pb.total_amount AS total_disbursed, pb.paid_count AS beneficiaries_paid FROM payment_batches pb WHERE pb.payment_month = 3 AND pb.payment_year = 2026 AND pb.batch_status = 'Completed' LIMIT 1;

Q: total monthly payout
SQL: SELECT SUM(c.current_monthly_amount) AS total_monthly_payout FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active';

Q: category wise monthly payout
SQL: SELECT c.category_name AS category, COUNT(*) AS beneficiaries, SUM(c.current_monthly_amount) AS monthly_payout FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY c.category_name ORDER BY monthly_payout DESC;

-- PATTERN: Percentage calculations
Q: male vs female percentage
SQL: SELECT gender, COUNT(*) AS count, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage FROM beneficiaries WHERE status='Active' GROUP BY gender ORDER BY count DESC;

-- PATTERN: Payment summary (yearly/historical — ALWAYS use payment_summary table)
Q: year wise payment comparison
SQL: SELECT ps.payment_year AS year, SUM(ps.total_beneficiaries) AS total_beneficiaries, SUM(ps.paid_count) AS paid_count, SUM(ps.pending_count) AS pending_count, SUM(ps.failed_count) AS failed_count, SUM(ps.total_net_amount) AS total_paid, ROUND(SUM(ps.paid_count) * 100.0 / NULLIF(SUM(ps.total_beneficiaries), 0), 2) AS success_rate_pct FROM payment_summary ps GROUP BY ps.payment_year ORDER BY ps.payment_year;

Q: district wise payment comparison by year
SQL: SELECT d.district_name AS district, ps.payment_year, SUM(ps.total_net_amount) AS total_paid, SUM(ps.paid_count) AS paid_count FROM payment_summary ps JOIN districts d ON ps.district_id = d.district_id WHERE ps.payment_year >= EXTRACT(YEAR FROM CURRENT_DATE)::INT - 2 GROUP BY d.district_name, ps.payment_year ORDER BY d.district_name, ps.payment_year;

Q: category wise payment comparison last 3 years
SQL: SELECT c.category_name AS category, ps.payment_year, SUM(ps.total_net_amount) AS total_paid, SUM(ps.paid_count) AS paid_count FROM payment_summary ps JOIN categories c ON ps.category_id = c.category_id WHERE ps.payment_year >= EXTRACT(YEAR FROM CURRENT_DATE)::INT - 2 GROUP BY c.category_name, ps.payment_year ORDER BY c.category_name, ps.payment_year;

Q: year-on-year payment growth percentage
SQL: SELECT ps.payment_year AS year, SUM(ps.total_net_amount) AS total_paid, ROUND((SUM(ps.total_net_amount) - LAG(SUM(ps.total_net_amount)) OVER (ORDER BY ps.payment_year)) * 100.0 / NULLIF(LAG(SUM(ps.total_net_amount)) OVER (ORDER BY ps.payment_year), 0), 2) AS growth_pct FROM payment_summary ps GROUP BY ps.payment_year ORDER BY ps.payment_year;

-- PATTERN: Payment batches (monthly ECS batch data)
Q: payment batch summary
SQL: SELECT pb.batch_reference, pb.payment_month, pb.payment_year, pb.fiscal_year_label, pb.batch_status, pb.total_beneficiaries, pb.total_amount, pb.paid_count, pb.failed_count, pb.pending_count FROM payment_batches pb ORDER BY pb.payment_year DESC, pb.payment_month DESC LIMIT 12;

Q: total disbursed per fiscal year from batches
SQL: SELECT pb.fiscal_year_label, SUM(pb.total_amount) AS total_disbursed, SUM(pb.paid_count) AS total_paid, SUM(pb.failed_count) AS total_failed FROM payment_batches pb WHERE pb.batch_status = 'Completed' GROUP BY pb.fiscal_year_label ORDER BY pb.fiscal_year_label;

-- PATTERN: Individual payments table (recent data only — last 6 months)
Q: payment status summary
SQL: SELECT status, COUNT(*) AS count FROM payments GROUP BY status ORDER BY count DESC;

Q: average payment amount per beneficiary by category
SQL: SELECT c.category_name AS category, ROUND(AVG(p.amount), 2) AS avg_payment, COUNT(DISTINCT p.beneficiary_id) AS beneficiaries_paid FROM payments p JOIN beneficiaries b ON p.beneficiary_id = b.beneficiary_id JOIN categories c ON b.category_id = c.category_id WHERE p.status = 'Paid' GROUP BY c.category_name ORDER BY avg_payment DESC;

-- PATTERN: Life certificates (annual compliance)
Q: life certificate compliance rate by taluka
SQL: SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(b.beneficiary_id) AS total_active, COUNT(lc.cert_id) AS submitted, ROUND(COUNT(lc.cert_id) * 100.0 / NULLIF(COUNT(b.beneficiary_id), 0), 2) AS compliance_pct FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id JOIN districts d ON b.district_id = d.district_id LEFT JOIN life_certificates lc ON lc.beneficiary_id = b.beneficiary_id AND lc.due_year = EXTRACT(YEAR FROM CURRENT_DATE)::INT WHERE b.status = 'Active' GROUP BY t.taluka_name, d.district_name ORDER BY compliance_pct ASC;

Q: year wise life certificate submissions
SQL: SELECT lc.due_year AS year, COUNT(*) AS total_submitted, COUNT(*) FILTER (WHERE lc.is_late_submission = TRUE) AS late_submissions, COUNT(*) FILTER (WHERE lc.payment_suspended = TRUE) AS suspensions FROM life_certificates lc GROUP BY lc.due_year ORDER BY lc.due_year DESC;

-- PATTERN: Registration trends
Q: year wise registration trend
SQL: SELECT EXTRACT(YEAR FROM b.registration_date)::INT AS year, COUNT(*) AS registrations FROM beneficiaries b WHERE b.registration_date IS NOT NULL GROUP BY year ORDER BY year;

Q: year wise count for disabled 90 percent
SQL: SELECT EXTRACT(YEAR FROM b.registration_date)::INT AS year, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE LOWER(c.category_name) LIKE '%disabled%90%' AND b.registration_date IS NOT NULL GROUP BY year ORDER BY year;

Q: in which year was disabled 90 percent the lowest
SQL: SELECT EXTRACT(YEAR FROM b.registration_date)::INT AS year, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE LOWER(c.category_name) LIKE '%disabled%90%' AND b.registration_date IS NOT NULL GROUP BY year ORDER BY count ASC LIMIT 1;

Q: year wise category wise beneficiary count
SQL: SELECT EXTRACT(YEAR FROM b.registration_date)::INT AS year, c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.registration_date IS NOT NULL GROUP BY year, c.category_name ORDER BY year, count DESC;

Q: new beneficiaries registered in the last 6 months by category
SQL: SELECT c.category_name AS category, COUNT(*) AS new_registrations FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.registration_date >= CURRENT_DATE - INTERVAL '6 months' GROUP BY c.category_name ORDER BY new_registrations DESC;

-- PATTERN: Year-wise enrollment breakup
Q: year wise enrollment breakup of DSSS beneficiaries
SQL: SELECT se.enrollment_year AS year, COUNT(DISTINCT se.beneficiary_id) AS total_enrolled, COUNT(DISTINCT se.beneficiary_id) FILTER (WHERE se.is_current = TRUE) AS currently_active, COUNT(DISTINCT se.beneficiary_id) FILTER (WHERE se.is_current = FALSE) AS exited FROM scheme_enrollments se WHERE se.enrollment_year IS NOT NULL GROUP BY se.enrollment_year ORDER BY se.enrollment_year;

Q: year wise enrollment breakup by category
SQL: SELECT se.enrollment_year AS year, c.category_name AS category, COUNT(DISTINCT se.beneficiary_id) AS enrolled FROM scheme_enrollments se JOIN categories c ON se.category_id = c.category_id WHERE se.enrollment_year IS NOT NULL GROUP BY se.enrollment_year, c.category_name ORDER BY se.enrollment_year, enrolled DESC;

Q: total enrollments per year all years
SQL: SELECT se.enrollment_year AS year, COUNT(*) AS enrollments FROM scheme_enrollments se WHERE se.enrollment_year IS NOT NULL GROUP BY se.enrollment_year ORDER BY se.enrollment_year;

-- PATTERN: Year-wise enrollment — category breakdown (queries ii & iii)
-- Returns chart_type=stacked (year + category + count → 2 label cols, 1 numeric)
Q: year wise category breakdown of DSSS beneficiary enrollment
SQL: SELECT se.enrollment_year AS year, c.category_name AS category, COUNT(DISTINCT se.beneficiary_id) AS enrolled FROM scheme_enrollments se JOIN categories c ON se.category_id = c.category_id WHERE se.enrollment_year IS NOT NULL GROUP BY se.enrollment_year, c.category_name ORDER BY se.enrollment_year, enrolled DESC;

Q: year wise category breakdown of enrolled citizens under DSSS
SQL: SELECT se.enrollment_year AS year, c.category_name AS category, COUNT(DISTINCT se.beneficiary_id) AS enrolled FROM scheme_enrollments se JOIN categories c ON se.category_id = c.category_id WHERE se.enrollment_year IS NOT NULL GROUP BY se.enrollment_year, c.category_name ORDER BY se.enrollment_year, enrolled DESC;

Q: year wise category breakdown of enrolled citizens all years
SQL: SELECT se.enrollment_year AS year, c.category_name AS category, COUNT(DISTINCT se.beneficiary_id) AS enrolled FROM scheme_enrollments se JOIN categories c ON se.category_id = c.category_id WHERE se.enrollment_year IS NOT NULL GROUP BY se.enrollment_year, c.category_name ORDER BY se.enrollment_year, enrolled DESC;

-- PATTERN: Year-wise enrollment — gender breakdown (query iv)
-- Returns chart_type=stacked (year + gender + count → 2 label cols, 1 numeric)
Q: year wise gender breakdown of enrolled citizens under DSSS
SQL: SELECT se.enrollment_year AS year, b.gender, COUNT(DISTINCT se.beneficiary_id) AS enrolled FROM scheme_enrollments se JOIN beneficiaries b ON se.beneficiary_id = b.beneficiary_id WHERE se.enrollment_year IS NOT NULL GROUP BY se.enrollment_year, b.gender ORDER BY se.enrollment_year, enrolled DESC;

Q: year wise gender breakdown of DSSS beneficiary enrollment
SQL: SELECT se.enrollment_year AS year, b.gender, COUNT(DISTINCT se.beneficiary_id) AS enrolled FROM scheme_enrollments se JOIN beneficiaries b ON se.beneficiary_id = b.beneficiary_id WHERE se.enrollment_year IS NOT NULL GROUP BY se.enrollment_year, b.gender ORDER BY se.enrollment_year, enrolled DESC;

-- PATTERN: Year-wise enrollment trend (total per year — line chart for all years)
Q: year wise enrollment trend of DSSS beneficiaries
SQL: SELECT se.enrollment_year AS year, COUNT(DISTINCT se.beneficiary_id) AS enrollments FROM scheme_enrollments se WHERE se.enrollment_year IS NOT NULL GROUP BY se.enrollment_year ORDER BY se.enrollment_year;

Q: enrollment trend over all years
SQL: SELECT se.enrollment_year AS year, COUNT(DISTINCT se.beneficiary_id) AS enrollments FROM scheme_enrollments se WHERE se.enrollment_year IS NOT NULL GROUP BY se.enrollment_year ORDER BY se.enrollment_year;

-- PATTERN: Status history / enrollments
Q: status changes in 2024
SQL: SELECT new_status, COUNT(*) AS count FROM beneficiary_status_history WHERE DATE_PART('year', changed_at) = 2024 GROUP BY new_status ORDER BY count DESC;

Q: category transfers
SQL: SELECT b.beneficiary_code, c_old.category_name AS from_category, c_new.category_name AS to_category, se.end_date AS transfer_date FROM scheme_enrollments se JOIN beneficiaries b ON se.beneficiary_id = b.beneficiary_id JOIN categories c_old ON se.category_id = c_old.category_id JOIN scheme_enrollments se_new ON se_new.beneficiary_id = se.beneficiary_id AND se_new.is_current = TRUE AND se.is_current = FALSE JOIN categories c_new ON se_new.category_id = c_new.category_id WHERE se.end_date IS NOT NULL ORDER BY se.end_date DESC LIMIT 20;

-- PATTERN: Pension amount history
Q: pension amount history for senior citizens
SQL: SELECT cah.monthly_amount, cah.effective_from, cah.effective_to, cah.reason FROM category_amount_history cah JOIN categories c ON cah.category_id = c.category_id WHERE c.category_name = 'Senior Citizen' ORDER BY cah.effective_from;

-- PATTERN: Fiscal periods
Q: current fiscal year
SQL: SELECT fiscal_year, fiscal_year_label, quarter, quarter_label, period_start, period_end FROM fiscal_periods WHERE is_current = TRUE;
"""

# ── Follow-up signal detection (heuristic, no API call) ───────────────────────
#
# Three layers of signals, scored together. The goal is to know:
#   (a) Is this a follow-up at all? → is_followup()
#   (b) Can it be answered from prior data without fetching new SQL? → is_reason_question()
#
# Both functions are pure-Python heuristics. They are intentionally
# conservative — when unsure, return True for is_followup (cheap) and False
# for is_reason_question (so we still fetch fresh data when in doubt).

# Pronouns / deictic references — almost always need prior context to resolve.
_REFERENTIAL = frozenset([
    ' it ', " it's", ' its ', ' that ', ' this ', ' those ', ' these ',
    ' them ', ' their ', ' there ', ' such ', ' same ',
    'that one', 'this one', 'the same', 'the previous', 'the above',
])

# Continuation phrases — user is extending the prior turn.
_CONTINUATION = frozenset([
    'what about', 'how about', 'same for', 'and the', 'and what', 'and how',
    'now show', 'now what', 'also show', 'as well', 'and also',
    'and inactive', 'and active', 'and deceased', 'and female', 'and male',
    'and north', 'and south', 'and widow', 'and senior',
])

# Aggregation across prior turns — needs prior numbers, not new SQL.
_AGGREGATION = frozenset([
    'sum of', 'total of', 'combine', 'add both', 'add them', 'add up',
    'both of', 'all three', 'all of them', 'altogether',
])

# Reasoning / explanation — answerable from prior data alone.
_REASONING = frozenset([
    'why ', 'why?', 'why is', 'why are', 'why does', 'why did', 'why has',
    'explain', 'explanation', 'reason for', 'what caused', 'how come',
    'what does this mean', 'what does that mean', 'interpret',
    'summarize', 'summarise', 'summary',
    'tell me more', 'elaborate', 'in short', 'in summary',
    # Follow-up continuation triggers (when prior data exists)
    'what about', 'and what about', 'how about',
    'show me only', 'filter by',
    'break that down', 'break it down',
    'compare that', 'compare those',
])

# Reflective comparison/ranking against PRIOR data shown.
_REFLECTIVE = frozenset([
    'which is the highest', 'which is the lowest',
    'which one is highest', 'which one is lowest',
    'which is bigger', 'which is smaller', 'which has more', 'which has less',
    'which is better', 'which is worse',
    'what is the highest', 'what is the lowest',
    'biggest', 'smallest', 'difference between',
    'top one', 'bottom one', 'best one', 'worst one',
    # Dimensional "which" patterns — identify specific item from prior data
    'which year', 'which month', 'which taluka', 'which district', 'which category',
    'which one', 'which has', 'which had', 'which is',
    # Superlatives against prior results
    ' lowest', ' highest', ' most ', ' least ', ' best ', ' worst ',
    'the lowest', 'the highest', 'the most', 'the least', 'the best', 'the worst',
])

# Topic markers that pin a question to a specific data domain. If a follow-up
# introduces a topic that doesn't appear in any recent context, it's a fresh
# question and should NOT inherit prior filters.
_TOPIC_TERMS = {
    'beneficiary', 'beneficiaries', 'beneficiar',
    'category', 'categories', 'widow', 'senior', 'disabled', 'hiv',
    'single woman', 'leprosy', 'cancer', 'kidney', 'sickle',
    'district', 'goa', 'taluka', 'village', 'pincode',
    'payment', 'payout', 'paid', 'pending', 'failed', 'batch',
    'life certificate', 'enrollment', 'enrolment',
    'gender', 'male', 'female', 'age',
    'active', 'inactive', 'deceased',
    'year', 'month', 'fiscal', 'quarter',
    'eligibility', 'apply', 'application', 'document', 'documents',
}


def _norm(text: str) -> str:
    """Normalize text for matching: lowercase, padded with spaces, collapsed."""
    return f" {text.lower().strip()} "


def is_followup(question: str, context: list[ConversationTurn]) -> bool:
    """
    Decide if a question needs context-aware resolution.

    Strategy: any one strong signal → True. Otherwise check shared topic with
    the last analytical turn — if the new question is short AND shares topic
    terms with prior context, it's a follow-up.

    Errs toward True when context exists and the question is short, because
    the resolver is cheap and a wrong fresh-route loses the user's intent.
    """
    if not context:
        return False
    analytical = [t for t in context if t.intent != "EDGE"]
    if not analytical:
        return False

    q = _norm(question)
    n_words = len(q.split())

    # Strong signals — always a follow-up.
    if any(sig in q for sig in _REFERENTIAL):    return True
    if any(sig in q for sig in _CONTINUATION):   return True
    if any(sig in q for sig in _AGGREGATION):    return True
    if any(sig in q for sig in _REASONING):      return True
    if any(sig in q for sig in _REFLECTIVE):     return True

    # Topic-shift check: if the question mentions a topic NOT present in any
    # recent turn AND is long enough to stand alone, treat as fresh.
    q_topics  = {t for t in _TOPIC_TERMS if t in q}
    ctx_blob  = " ".join(_norm(t.resolved_question) for t in analytical[-3:])
    ctx_topics = {t for t in _TOPIC_TERMS if t in ctx_blob}

    shared_topics = q_topics & ctx_topics
    new_topics    = q_topics - ctx_topics

    # Long, self-contained, with new topics and no shared ones → fresh.
    if n_words >= 6 and new_topics and not shared_topics:
        return False

    # Short questions (≤7 words) with prior context → almost always follow-up.
    if n_words <= 7:
        return True

    # Medium-length question that shares topics with prior context → follow-up.
    if shared_topics:
        return True

    return False


def is_reason_question(question: str, context: list[ConversationTurn]) -> bool:
    """
    Decide if a question can be answered purely by REASONING over prior data,
    without fetching new SQL or RAG. This is the natural-conversation path.

    Returns True when:
      - Prior context exists AND has sql_data to reason about, AND
      - The question is reasoning/reflective (why, explain, which is highest,
        which year, lowest, highest, most, least, what about, etc.)

    Conservative on purpose: when unsure, return False so we fall back to SQL.
    """
    if not context:
        return False
    analytical = [t for t in context if t.intent != "EDGE"]
    if not analytical:
        return False

    # Need at least one prior turn with actual data rows to reason about.
    has_data = any(t.sql_data for t in analytical)
    if not has_data:
        return False

    q = _norm(question)

    # "why / explain / interpret / summarize / what about / compare that" — clearly reasoning.
    if any(sig in q for sig in _REASONING):
        return True

    # "which is highest/lowest/bigger / which year / which district / lowest / highest" —
    # reflective comparison/identification over prior data.
    # Only if we don't introduce brand-new topics the prior data wouldn't cover.
    if any(sig in q for sig in _REFLECTIVE):
        # Time-dimension guard: if the question asks "which year" or "which month",
        # only treat it as REASON if the prior sql_data actually contains a year/month
        # column. Otherwise the AI cannot answer from prior data and we must fetch SQL.
        _TIME_DIM_TRIGGERS = ('which year', 'in which year', 'which month', 'in which month')
        needs_time_col = any(t in q for t in _TIME_DIM_TRIGGERS)
        if needs_time_col:
            prior_data_cols: set[str] = set()
            for t in analytical:
                if t.sql_data:
                    for row in t.sql_data[:1]:
                        prior_data_cols.update(k.lower() for k in row.keys())
            _YEAR_COLS = {'year', 'payment_year', 'registration_year', 'fiscal_year', 'month', 'payment_month'}
            if not prior_data_cols.intersection(_YEAR_COLS):
                # Prior data has no time dimension — must fetch new SQL
                return False

        q_topics   = {t for t in _TOPIC_TERMS if t in q}
        ctx_blob   = " ".join(_norm(t.resolved_question) for t in analytical[-3:])
        ctx_topics = {t for t in _TOPIC_TERMS if t in ctx_blob}
        new_topics = q_topics - ctx_topics
        # Allow if the question only has 0–1 new topic terms (likely still about prior data)
        if len(new_topics) <= 1:
            return True

    return False


# ── Internal context formatter ─────────────────────────────────────────────────

def _row_headline(rows: list, max_rows: int = 6) -> str:
    """
    Compress a list of dict rows into a short, human-readable preview that
    keeps the headline numbers a model needs to reason about ("North Goa = 1.9M
    in 2025, 0.49M in 2026") without dumping verbose JSON into every prompt.
    """
    if not rows:
        return ""
    rows = rows[:max_rows]
    cols = list(rows[0].keys())
    # Find one label column (text) and one numeric column (the headline metric)
    def _is_num(v):
        try: float(str(v)); return True
        except (ValueError, TypeError): return False
    num_cols = [c for c in cols if all(_is_num(r.get(c)) for r in rows if r.get(c) is not None)]
    lbl_cols = [c for c in cols if c not in num_cols]
    if not num_cols:
        # No numeric column → just stringify each row briefly
        return "; ".join(", ".join(f"{k}={v}" for k, v in r.items()) for r in rows)
    metric = num_cols[0]
    parts = []
    for r in rows:
        label_bits = " | ".join(str(r.get(c, "")) for c in lbl_cols) or "row"
        parts.append(f"{label_bits} → {metric}={r.get(metric)}")
    return "; ".join(parts)


def _fmt_context(
    context: list[ConversationTurn],
    *,
    mode: str = "compact",
    max_turns: int = 4,
) -> str:
    """
    Render conversation history for injection into prompts.

    mode="compact"  — question + 1-line answer + headline numbers (default).
                      Used by SQL/intent/resolver prompts. Keeps tokens low.
    mode="full"     — question + full answer + full sql_data JSON.
                      Used by the REASON prompt where the model must reason
                      over actual rows.

    EDGE turns (greetings, etc.) are excluded — they add noise.
    Only the most recent `max_turns` analytical turns are included.
    """
    if not context:
        return ""
    analytical = [t for t in context if t.intent != "EDGE"]
    if not analytical:
        return ""
    analytical = analytical[-max_turns:]

    if mode == "full":
        lines = ["CONVERSATION HISTORY (use the actual data rows below to answer the question):"]
        for i, t in enumerate(analytical, 1):
            lines.append(f"[{i}] User asked: {t.resolved_question}")
            short_ans = (t.answer[:300] + "…") if len(t.answer) > 300 else t.answer
            lines.append(f"     Answer given: {short_ans}")
            if t.sql_data:
                lines.append(f"     Data rows ({len(t.sql_data)}): {json.dumps(t.sql_data, default=str)}")
        lines.append("")
        return "\n".join(lines) + "\n"

    # compact mode
    lines = ["CONVERSATION HISTORY (recent turns — use to resolve references and maintain continuity):"]
    for i, t in enumerate(analytical, 1):
        lines.append(f"[{i}] Q: {t.resolved_question}")
        short_ans = (t.answer[:200] + "…") if len(t.answer) > 200 else t.answer
        lines.append(f"     A: {short_ans}")
        if t.sql_data:
            headline = _row_headline(t.sql_data, max_rows=6)
            if headline:
                lines.append(f"     Key numbers: {headline}")
    lines.append("")
    return "\n".join(lines) + "\n"


# ── Question Resolver ─────────────────────────────────────────────────────────

def build_question_resolver_prompt(question: str, context: list[ConversationTurn]) -> str:
    """
    Rewrite a follow-up into a complete, standalone question while PRESERVING
    the user's meta-intent. The output is just the rewritten question — the
    caller decides how to route it (REASON / SQL / RAG) using is_reason_question.

    Key principle: do NOT strip "why", "explain", "which is highest", etc.
    Those words tell us how the user wants the answer shaped — losing them
    is exactly what makes multi-turn feel robotic.

    Fix 2b: Inject prior turn's question, answer AND actual data rows so the
    resolver can produce precise standalone questions referencing real numbers.
    """
    # Build a rich context block: compact history + most-recent-turn full data rows
    ctx_block = _fmt_context(context, mode="compact", max_turns=4).rstrip()

    # Append the most recent analytical turn's actual data rows
    analytical = [t for t in context if t.intent != "EDGE"]
    if analytical:
        last = analytical[-1]
        if last.sql_data:
            rows_preview = json.dumps(last.sql_data[:15], default=str)
            ctx_block += (
                f"\n\nMOST RECENT TURN DATA (use these actual rows to resolve references):\n"
                f"Question: {last.resolved_question}\n"
                f"Answer summary: {last.answer[:300]}\n"
                f"Data rows: {rows_preview}"
            )

    return f"""You rewrite follow-up questions into complete, standalone questions for the DSSS analytics assistant.

{ctx_block}

CURRENT USER MESSAGE: "{question}"

YOUR JOB:
Rewrite the current message into ONE complete, standalone question that makes sense without the history,
while keeping the user's tone and meta-intent intact.

PRESERVE THE USER'S INTENT WORDS:
- Keep "why", "explain", "summarize", "what does this mean" if the user used them — these tell us they want
  reasoning over the prior data, NOT a fresh data fetch.
- Keep "which is highest/lowest/biggest", "compare", "difference between" — they want a reflective comparison.
- Keep "show", "list", "give me", "draw a chart" — they want fresh data or a visualization.

RESOLUTION RULES:
1. Replace pronouns ("it", "that", "this", "those", "them") with the concrete subject from history.
2. Carry forward filters (district, category, status, year) UNLESS the user changes them.
3. If the user introduces a brand-new topic that doesn't appear in history, do NOT bolt prior filters on.
4. If the message is already standalone, return it unchanged.
5. Output ONLY the rewritten question. No quotes, no prefix, no explanation.

EXAMPLES:
History: "active beneficiaries by category" answered with category counts.
"what about inactive?"  →  "How many inactive beneficiaries are there by category?"

History: shows north/south goa beneficiary totals by year (2024–2026).
"why 2026 has lowest beneficiaries?"  →  "Why is the 2026 beneficiary count the lowest in the data shown?"
   (KEEP "why" — this is a reasoning question, not a SQL fetch.)

History: shows disabled 90% had lowest active count.
"and why?"  →  "Why does Disabled 90% have the lowest active beneficiary count?"

History: gender breakdown by category.
"which one is highest?"  →  "Which category-gender combination has the highest count in the prior breakdown?"

History: north goa widow count.
"what about south goa?"  →  "How many widow beneficiaries are there in South Goa?"

History: senior citizen count (active).
"draw a chart"  →  "Show senior citizen active beneficiary count for charting."

History: payment trend last 3 years.
"explain that"  →  "Explain the payment trend over the last 3 years shown above."

History: category breakdown.
"show me payment status for last 6 months"
   →  "Show payment status counts for the last 6 months."   (NEW topic — no inheritance.)

REWRITTEN QUESTION:"""


# ── Intent Classification ─────────────────────────────────────────────────────

def build_intent_prompt(question: str, context: list[ConversationTurn] = None) -> str:
    """
    Classify a question into one of three intents: SQL, RAG, or REASON.

    SQL    — fetch fresh numbers from the database.
    RAG    — answer from scheme documents (eligibility, rules, procedures).
    REASON — answer purely by reasoning over data already in conversation history
             (why, explain, summarize, which is highest among shown). No fetch.

    REASON is only allowed when prior context contains analytical turns with data.
    """
    ctx = _fmt_context(context or [], mode="compact", max_turns=3)
    has_prior_data = bool(context) and any(
        t.intent != "EDGE" and t.sql_data for t in (context or [])
    )
    reason_hint = (
        "REASON is AVAILABLE — prior turns have data rows you can reason about."
        if has_prior_data else
        "REASON is NOT AVAILABLE — there is no prior data to reason about. Choose SQL or RAG."
    )

    return f"""{ctx}Route this user message to ONE of: SQL, RAG, or REASON.
{reason_hint}

SQL    = run a fresh query against the LIVE DATABASE for numbers, counts, breakdowns, trends, lists, charts.
RAG    = answer from SCHEME DOCUMENTS — eligibility rules, procedures, history, official notifications.
REASON = answer using ONLY the prior conversation data shown above. No new fetch. Use this when the user is
         asking to reason ABOUT what was already shown — "why", "explain", "summarize", "interpret",
         "which one is highest among these", "what does that mean".

DECISION ORDER:
1. Reasoning verbs (why / explain / summarize / interpret / what does this mean / tell me more) over prior
   data → REASON.
2. Reflective comparison ("which is the highest?", "biggest one?", "compare those") that refers to data
   already shown in history → REASON.
3. Question about rules / eligibility / procedure / documents / policy → RAG.
4. Anything asking for fresh numbers, breakdowns, charts, lists, trends → SQL.
5. When in doubt between SQL and REASON, prefer SQL (fresh data is safer than stale).
6. When in doubt between SQL and RAG, prefer SQL.

EXAMPLES:
"How many active beneficiaries?"                                       → SQL
"District-wise breakdown"                                              → SQL
"Compare payments last 3 years"                                        → SQL
"Draw a chart of category counts"                                      → SQL
"Show talukas in north goa"                                            → SQL

(after a yearly trend is shown) "why is 2026 the lowest?"              → REASON
(after a category breakdown)    "explain that"                         → REASON
(after district counts shown)   "which district is the highest?"       → REASON
(after seeing payment trend)    "summarize the trend"                  → REASON
(after multiple categories)     "what does this tell us about widows?" → REASON

"Who is eligible for DSSS?"                                            → RAG
"What documents are needed for application?"                           → RAG
"What is the difference between DSSS and DDSSY?"                       → RAG
"How to apply?"                                                        → RAG

Reply with EXACTLY one word: SQL, RAG, or REASON.

Question: {question}
Answer:"""


# ── Reasoning over prior data (no fetch) ─────────────────────────────────────

def build_reason_prompt(
    question: str,
    context: list[ConversationTurn],
    language: str = "en",
) -> str:
    """
    Build a prompt that asks Gemini to answer the user's question PURELY by
    reasoning over data already present in conversation history. No SQL,
    no RAG, no web search. Used for "why", "explain", "which is highest",
    "summarize that", "which year", "lowest", etc.

    Fix 2c: Explicitly surfaces the most recent analytical turn's raw sql_data
    rows at the top so the model reasons over real numbers from that turn.
    """
    lang_name  = LANGS.get(language, "English")
    lang_instr = f"Respond in {lang_name}." if language != "en" else ""

    # Full conversation history (all analytical turns with their data rows)
    ctx = _fmt_context(context or [], mode="full", max_turns=4)

    # Surface the most-recent analytical turn's data prominently
    analytical = [t for t in (context or []) if t.intent != "EDGE" and t.sql_data]
    primary_data_block = ""
    if analytical:
        last = analytical[-1]
        rows_json = json.dumps(last.sql_data, default=str)
        primary_data_block = (
            f"PRIMARY DATA TO REASON ABOUT (most recent query results):\n"
            f"Question that produced this data: \"{last.resolved_question}\"\n"
            f"All data rows: {rows_json}\n"
        )

    return f"""You are the DSSS analytics assistant for the Government of Goa Department of Social Welfare.
The user is asking a follow-up that should be answered by REASONING over the data already shown in this
conversation. Do NOT invent new numbers. Do NOT pretend to query a database. Use only the rows below.

{primary_data_block}
FULL CONVERSATION HISTORY:
{ctx}
USER'S QUESTION: "{question}"

HOW TO ANSWER:
- Start with the PRIMARY DATA above — these are the actual rows from the most recent query.
- For "which year/month/taluka/district/category": scan the data rows and name the matching row + its value.
- For "which is the highest/lowest/most/least/best/worst": sort the rows by the relevant metric and name
  the top/bottom row with its exact value from the data.
- For "why is X the lowest/highest?": check if X is the current year (still in progress → partial data),
  or if X is the smallest category by nature (e.g., DIS-90 has fewest beneficiaries by design). State the
  most plausible reason based on what the data actually shows.
- For "explain that drop / spike / trend": describe what changed and over what time using real numbers.
- For "what about [item]?", "show me only [filter]", "break that down": identify the matching rows in the
  data, filter/highlight them, and present the relevant numbers.
- For "summarize": list the 2–3 most important numbers from the most recent turn.
- NULL/empty columns are "not recorded" — do NOT cite them as reasons for any value.
- If you genuinely cannot answer from the data, say so honestly and suggest a follow-up query.

FORMATTING:
- 2–4 sentences. Direct answer first, then the supporting numbers.
- Use Indian comma notation (1,40,000 not 140000). Use "Rs." for amounts.
- Do NOT mention SQL, queries, databases, or that this came from conversation history.
{lang_instr}

ANSWER:"""


# ── SQL Generation ────────────────────────────────────────────────────────────

def build_sql_prompt(question: str, context: list[ConversationTurn] = None) -> str:
    """Generate PostgreSQL SQL. Prior SQL context is passed as a follow-up hint."""
    prior_hint = ""
    if context:
        # Surface recent analytical turns as continuity context
        analytical = [t for t in context if t.intent in ("SQL", "RAG")]
        if analytical:
            lines = ["FOLLOW-UP CONTEXT (recent conversation — use to understand what the user is looking at):"]
            for t in analytical[-3:]:  # last 3 turns max
                lines.append(f"  User asked: \"{t.resolved_question}\"")
                lines.append(f"  Answer: {t.answer[:200]}")
                if t.sql_data:
                    lines.append(f"  Data returned: {json.dumps(t.sql_data[:10], default=str)}")
            lines.append("The current question may be a follow-up. Apply any implied filters, categories, or scope from above.\n")
            prior_hint = "\n".join(lines) + "\n"

    return f"""{SCHEMA}

{COUNTS_GUARD}

EXAMPLES:
{SHOTS}

{prior_hint}RULES:
- Output ONLY a valid PostgreSQL SQL SELECT or WITH statement
- Use standard PostgreSQL SQL syntax (NOT BigQuery syntax)
- Use plain table names without backticks or project prefix
- Table names: beneficiaries, categories, payments, payment_batches, life_certificates, districts, talukas, villages, banks, payment_summary, scheme_enrollments, beneficiary_status_history, fiscal_periods, officers, category_amount_history
- No markdown, no backticks, no explanation
- Never select PII columns (aadhaar_number, phone_number, address, account_number)
- Always use proper JOIN conditions with correct column names
- For "top N" questions use LIMIT N with ORDER BY DESC
- For percentage questions use ROUND(x * 100.0 / SUM(x) OVER(), 2)
- For age range questions use BETWEEN or CASE WHEN age groups
- Default status filter is 'Active' unless question asks for Inactive/Deceased/all
- Status values (beneficiaries): 'Active', 'Inactive', 'Deceased'  — Title Case EXACTLY
- Status values (payments): 'Paid', 'Pending', 'Failed'  — Title Case EXACTLY
- For combined totals (active + inactive) use WHERE status IN ('Active', 'Inactive')
- Always alias columns with readable names (AS district, AS count, etc.)
- For cross-tab/breakdown questions GROUP BY both dimensions
- For year extraction use EXTRACT(YEAR FROM column)::INT
- For date formatting use TO_CHAR(DATE_TRUNC('month', col), 'YYYY-MM')
- "Last 3 years payments" or "payout comparison" or "compare payments" → ALWAYS use payment_summary table (NOT payments table):
    SELECT ps.payment_year AS year, SUM(ps.total_beneficiaries) AS total_beneficiaries,
           SUM(ps.paid_count) AS paid_count, SUM(ps.pending_count) AS pending_count,
           SUM(ps.failed_count) AS failed_count, SUM(ps.total_net_amount) AS total_paid,
           ROUND(SUM(ps.paid_count)*100.0/NULLIF(SUM(ps.total_beneficiaries),0),2) AS success_rate_pct
    FROM payment_summary ps
    WHERE ps.payment_year >= EXTRACT(YEAR FROM CURRENT_DATE)::INT - 2
    GROUP BY ps.payment_year ORDER BY ps.payment_year;
- "Year-over-year payments" → same pattern with payment_summary GROUP BY payment_year
- The payments table has only ~35k records (last 6 months). For historical/yearly data use payment_summary (1,680 rows, 6 years)
- For batch-level data (monthly ECS batches) use payment_batches table
- "Year-wise enrollment breakup" or "enrollment by year" → use scheme_enrollments table grouped by enrollment_year (NOT beneficiaries.registration_date). Include all years with no LIMIT.
- NEVER add LIMIT to a COUNT(*)-only query (it returns exactly 1 row)
- NEVER confuse category codes: DIS-90='Disabled 90%', DIS-80='Disabled 80%', DIS-40='Disabled 40%'
- If the question cannot be answered from the schema, output exactly: CANNOT_ANSWER

Question: {question}
SQL:"""


# ── Natural Language Answer ───────────────────────────────────────────────────

def build_nl_answer_prompt(
    question: str, sql: str, results: list, row_count: int,
    language: str, context: list[ConversationTurn] = None,
) -> str:
    """
    Generate human-readable answer. Injects conversation history so the model
    can reference prior numbers (e.g., "active was 45,231, inactive is 12,453,
    so combined is 57,684") for a true multi-turn ChatGPT-like experience.
    """
    lang_name  = LANGS.get(language, "English")
    lang_instr = f"Respond in {lang_name}." if language != "en" else ""
    ctx        = _fmt_context(context or [], mode="compact", max_turns=3)

    return f"""You are a DSSS analytics assistant for the Department of Social Welfare, Government of Goa.
{ctx}{COUNTS_GUARD}
The user asked: "{question}"
Query context: {sql[:200]}
Database returned {row_count} rows: {json.dumps(results[:50], default=str)}

CRITICAL GROUNDING RULES — READ BEFORE ANSWERING:
- Use ONLY numbers that appear verbatim in the data rows shown above. NEVER invent, estimate, or use numbers from training knowledge.
- For ranking questions (highest/lowest/most/least/top/bottom/fewest/smallest): the row order from the SQL IS the answer.
  ORDER BY ... ASC → first row is LOWEST. ORDER BY ... DESC → first row is HIGHEST.
- Never write a number that is not present in the data above.
- If the data has only 1 row (e.g., LIMIT 1 query), that single row IS the direct answer — state it directly.

NULL / EMPTY COLUMN RULE (very important):
- If the data has a column whose values are all NULL or all empty (for example an "explanation",
  "remarks", or "note" column), DO NOT explain anything by referring to that emptiness.
  NULL means "not recorded", it does NOT mean "data anomaly", "problem", or "decline".
  Never write phrases like "this is attributed to a data anomaly because the explanation field is null."

CURRENT-PERIOD RULE:
- If the data spans multiple years/months and the most recent period has a noticeably lower value than
  prior periods, the first thing to consider is: is that period still in progress? If yes, say "the
  {{year}} figure reflects data so far this year and is expected to grow as more records are added."
- The current calendar year is the year in CURRENT_DATE. If a row's year equals or exceeds the current
  year, treat it as a partial period unless the data clearly shows otherwise.

FOLLOW-UP CONTINUITY:
- If this is a follow-up that combines/sums prior turns, reference the exact prior numbers from history
  and show the arithmetic.
- If the current question references "the previous", "that", "those" — use the most recent turn's data
  in the history block above as the antecedent.

FORMATTING RULES:
- Lead with the direct answer or most important finding.
- Highlight highest/lowest values by name with exact numbers.
- If many rows, mention the top 2–3 AND the bottom 1–2 by name with exact numbers.
- Format large numbers with Indian comma notation (e.g., 1,40,000 not 140000). Use "Rs." for amounts.
- 2–4 sentences. No mention of SQL, queries, databases, or technical pipeline.
- TREND RULE: If results span multiple years, months, or categories, end your answer with one sentence
  describing the overall trend or the single most notable insight (e.g., "Overall, registrations have
  grown steadily since 2021, with 2024 recording the highest annual intake." or "Senior Citizens
  consistently account for the largest share across all years.").

DISBURSEMENT / TOTAL OUTLAY FORMAT RULE:
- When answering questions about total amount disbursed, total outlay, or total amount for the scheme,
  ALWAYS format the response exactly like this example:

  Total amount disbursed under DSSS (Dayanand Social Security Scheme):

  ₹45.26 Crore

  This represents the total state outlay credited directly to the accounts of over 2.24 lakh
  beneficiaries enrolled under the Dayanand Social Security Scheme (DSSS) and Griha Aadhar
  as of March 2026, across 12 talukas in Goa.

- The amount must be ₹45.26 Crore (the official March 2026 government figure).
- Never replace this with a computed monthly payout or a raw database sum like "₹ 12,84,37,500".
{lang_instr}

Answer:"""


# ── RAG Answer ────────────────────────────────────────────────────────────────

def build_rag_answer_prompt(
    question: str, chunks: list[str], language: str,
    context: list[ConversationTurn] = None,
) -> str:
    """Answer from RAG chunks with conversation context for follow-up awareness."""
    lang_name  = LANGS.get(language, "English")
    lang_instr = f"Respond in {lang_name}." if language != "en" else ""
    ctx        = _fmt_context(context or [], mode="compact", max_turns=3)

    return f"""You are an expert assistant for the Dayanand Social Security Scheme (DSSS), Government of Goa.
{ctx}Instructions:
- Answer ONLY from the provided context. If the answer is not in the context, say: "This specific information is not available in the DSSS scheme documents."
- Be precise with Rs. amounts, age limits, eligibility criteria, and deadlines
- For procedure/process questions, use a numbered list (1. 2. 3.)
- For eligibility questions, clearly state who qualifies and who does not
- For document questions, list each document on a new line
- Format amounts as: Rs. 2,500/- per month
- Mention relevant amendment years if context includes them (e.g., 2013, 2016, 2021)
- Keep the answer focused and factual — no filler phrases
{lang_instr}

CONTEXT:
{"---".join(chunks)}

QUESTION: {question}
ANSWER:"""
