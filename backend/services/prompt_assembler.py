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

Q: new beneficiaries registered in the last 6 months by category
SQL: SELECT c.category_name AS category, COUNT(*) AS new_registrations FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.registration_date >= CURRENT_DATE - INTERVAL '6 months' GROUP BY c.category_name ORDER BY new_registrations DESC;

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

# Signals that strongly indicate a follow-up / reference to prior context
_FOLLOWUP_SIGNALS = frozenset([
    'what about', 'how about', 'same for', 'and the', 'and what',
    'now show', 'now what', 'also', 'as well',
    'sum of', 'total of', 'combine', 'add both', 'add them',
    'both of', 'all three', 'all of them',
    'similarly', 'compare with', 'versus', ' vs ', 'difference between',
    'for that', 'of that', 'in that case', 'then what', 'and inactive',
    'and active', 'and deceased', 'and female', 'and male',
    'that one', 'those', ' it ', 'its ', 'their ', 'them',
    'compare to all', 'all years', 'which year', 'year wise', 'year-wise',
    'this category', 'that category', 'the lowest', 'the highest',
    'has the lowest', 'has the highest', 'and why',
])


def is_followup(question: str, context: list[ConversationTurn]) -> bool:
    """
    Determines if a question needs context-aware resolution before routing.
    Leans towards True when there IS prior context — the resolver is cheap
    (one fast Gemini call) and wrong routing is expensive (wrong answer).
    """
    if not context:
        return False
    # Only consider non-EDGE turns as valid prior context
    analytical = [t for t in context if t.intent != "EDGE"]
    if not analytical:
        return False
    q = question.lower().strip()
    # Very short questions (≤8 words) with context almost always need resolution
    if len(q.split()) <= 8:
        return True
    # Explicit follow-up signals
    if any(sig in q for sig in _FOLLOWUP_SIGNALS):
        return True
    # If the question mentions subjects from recent context, it's likely a follow-up
    last = analytical[-1]
    last_q = last.resolved_question.lower()
    # Check for shared nouns (category names, district names, etc.)
    key_terms = {'disabled', 'widow', 'senior', 'hiv', 'single woman',
                 'north goa', 'south goa', 'active', 'inactive', 'deceased',
                 'category', 'district', 'taluka', 'payment', 'payout'}
    q_terms = {t for t in key_terms if t in q}
    ctx_terms = {t for t in key_terms if t in last_q}
    if q_terms & ctx_terms:  # shared terms = likely continuation
        return True
    return False


# ── Internal context formatter ─────────────────────────────────────────────────

def _fmt_context(context: list[ConversationTurn]) -> str:
    """
    Render conversation history into a compact block for injection into prompts.
    Includes raw sql_data when available so the model can reference actual numbers.
    EDGE turns (greetings, thanks, etc.) are excluded — they carry no analytical
    content and would confuse the question resolver and SQL generator.
    """
    if not context:
        return ""
    # Filter out EDGE turns — they add noise to SQL/RAG resolution
    analytical = [t for t in context if t.intent != "EDGE"]
    if not analytical:
        return ""
    lines = ["CONVERSATION HISTORY (for context — use to resolve references and maintain continuity):"]
    for i, t in enumerate(analytical, 1):
        lines.append(f"[{i}] User asked: {t.resolved_question}")
        lines.append(f"     Answer: {t.answer}")
        if t.sql_data:
            lines.append(f"     Data retrieved: {json.dumps(t.sql_data, default=str)}")
    lines.append("")
    return "\n".join(lines) + "\n"


# ── Question Resolver ─────────────────────────────────────────────────────────

def build_question_resolver_prompt(question: str, context: list[ConversationTurn]) -> str:
    """
    Prompt to rewrite a follow-up question into a complete standalone question.
    This is the core of the multi-turn chain — called BEFORE intent classification.

    Examples handled:
      "what about inactive?"            → "How many inactive beneficiaries are there?"
      "sum of active and inactive?"     → "What is the combined total of active and inactive beneficiaries?"
      "which is the highest?"           → "Which district/category has the highest beneficiary count?"
      "show females only"               → "Show female beneficiary count district-wise" (from prior district context)
      "what about North Goa?"           → "How many active beneficiaries are in North Goa?" (from prior breakdown)
    """
    # Filter out EDGE turns — they carry no analytical context for follow-up resolution
    analytical = [t for t in context if t.intent != "EDGE"]
    ctx_lines = []
    for i, t in enumerate(analytical, 1):
        ctx_lines.append(f"[{i}] User asked: {t.resolved_question}")
        ctx_lines.append(f"     System answered: {t.answer}")
        if t.sql_data:
            ctx_lines.append(f"     Actual data returned: {json.dumps(t.sql_data, default=str)}")
    ctx_block = "\n".join(ctx_lines)

    return f"""You are a query resolver for the DSSY (Dayanand Social Security Scheme) analytics system.

Your job: Rewrite the user's question into a COMPLETE, STANDALONE question using conversation history.
The rewritten question must make sense on its own — someone reading it without the history should understand exactly what data is being asked for.

CONVERSATION HISTORY:
{ctx_block}

CURRENT QUESTION: "{question}"

RULES:
1. Replace pronouns/references ("it", "that", "this category", "those") with the actual subject from history
2. Carry forward filters from context (district, category, status) unless the user explicitly changes them
3. If the user asks to visualize/graph/chart the SAME data from prior context, rewrite as the data query (e.g., "draw a graph" → repeat the prior data question)
4. If the user asks "why" about a data pattern, keep it as a data question — add the relevant breakdown/comparison so the SQL can provide the answer through data
5. If the user is confirming/correcting prior results ("X has the lowest right?"), rewrite as a comparative query that can verify the claim
6. If already self-contained, return unchanged
7. IMPORTANT: Preserve the user's analytical intent — if they want comparison, trend, breakdown, ranking, keep that in the rewrite

EXAMPLES:
"what about inactive?" (after active count) → "How many inactive beneficiaries are there?"
"sum of both?" (after active + inactive) → "What is the combined total of active and inactive beneficiaries?"
"which is highest?" (after district breakdown) → "Which district has the highest number of active beneficiaries?"
"draw a graph" (after category-wise count) → "Show category-wise active beneficiary count"
"compare to all years which year got disabled 90% lowest and why?" → "Show year-wise total beneficiaries count for Disabled 90% category ordered by count ascending"
"disabled 80% has the lowest right?" (after showing Disabled 90% was lowest) → "Show all categories with their active beneficiary count ordered by count ascending"
"show trend for this" (after Disabled 90% data) → "Show year-wise Disabled 90% beneficiary count trend"
"what about South Goa?" (after North Goa widow count) → "How many widow beneficiaries are there in South Goa?"

Output ONLY the rewritten question — no explanation, no prefix, no quotes.

REWRITTEN QUESTION:"""


# ── Intent Classification ─────────────────────────────────────────────────────

def build_intent_prompt(question: str, context: list[ConversationTurn] = None) -> str:
    """Classify question as SQL or RAG, with conversation history for follow-up awareness."""
    ctx = _fmt_context(context or [])
    return f"""{ctx}Route this question to SQL or RAG for the DSSY (Dayanand Social Security Scheme) analytics system.

SQL = query the LIVE DATABASE for numbers, counts, statistics, comparisons, breakdowns, trends, charts, lists.
RAG = search SCHEME DOCUMENTS for rules, policies, eligibility criteria, procedures, history, official notifications.

DECISION RULES (in priority order):
1. If conversation history has SQL data AND this question continues that conversation (drill-down, comparison, chart, "why", confirmation) → SQL
2. If the question asks for NUMBERS or STATISTICS about beneficiaries, payments, categories, districts → SQL
3. If the question asks about RULES, ELIGIBILITY, PROCEDURES, POLICY, DOCUMENTS, or SCHEME HISTORY → RAG
4. When in doubt, prefer SQL — the SQL generator can return CANNOT_ANSWER if it truly can't handle it.

EXAMPLES:
"How many active beneficiaries?" → SQL
"District-wise breakdown" → SQL
"Category with lowest count" → SQL
"Compare payments last 3 years" → SQL
"draw a graph" → SQL
"and why?" → SQL
"Who is eligible for DSSY?" → RAG
"What documents are needed?" → RAG
"How to apply for DSSY?" → RAG
"What is the difference between DSSY and DDSSY?" → RAG
"What did the CAG audit find?" → RAG
"Can a divorced woman apply?" → RAG

Reply ONLY with SQL or RAG.

Question: {question}
Answer:"""


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
    ctx        = _fmt_context(context or [])

    return f"""You are a DSSY analytics assistant for the Department of Social Welfare, Government of Goa.
{ctx}{COUNTS_GUARD}
The user asked: "{question}"
Query context: {sql[:200]}
Database returned {row_count} rows: {json.dumps(results[:50], default=str)}

CRITICAL GROUNDING RULES — READ BEFORE ANSWERING:
- Use ONLY numbers that appear verbatim in the data rows shown above. NEVER invent, estimate, or use numbers from training knowledge.
- For ranking questions (highest/lowest/most/least/top/bottom/lowest/fewest/smallest): you MUST look at the actual data rows. The first row in the data IS the answer when ORDER BY is used — trust the row order.
- IMPORTANT: If the SQL used ORDER BY count ASC (ascending), the FIRST ROW is the LOWEST/FEWEST. If it used ORDER BY count DESC (descending), the FIRST ROW is the HIGHEST/MOST.
- When you see "last 3 years" or "payment comparison by year" in data: read each year row and state the actual amounts per year. Never say "no context available".
- Never write a number that is not present in the data above.
- If the data has only 1 row (e.g., LIMIT 1 query), that single row IS the direct answer — state it directly.

FORMATTING RULES:
- Lead with the most important finding or direct answer
- If this is a follow-up (e.g., a sum or combination), reference the prior numbers from conversation history and show how they add up
- Highlight highest/lowest values: name the actual winner/loser from the data, with their exact count
- If multiple categories/districts, mention the top 2-3 AND the bottom 1-2 by name with exact numbers
- Do NOT mention SQL, databases, queries, or technical terms
- Format large numbers with Indian comma notation (e.g., 1,40,000 not 140000)
- Use Rs. prefix for monetary amounts
- Write 2-4 sentences maximum
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
    ctx        = _fmt_context(context or [])

    return f"""You are an expert assistant for the Dayanand Social Security Scheme (DSSY), Government of Goa.
{ctx}Instructions:
- Answer ONLY from the provided context. If the answer is not in the context, say: "This specific information is not available in the DSSY scheme documents."
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
