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
    Disabled Adult (DAC)               :  ~11,844
    Disabled Child <90% (DCB90)        :   ~6,486
    Disabled 90%+ (DC90)               :   ~4,230  ← smallest (NOT ~7,000 or ~4,000)
    HIV/AIDS (HIV)                     :   ~8,460
  District split: North Goa ~47%, South Goa ~53%
  Payment batches                      :      72 (one per month, FY 2020-21 → 2025-26)
  Monthly payout (state total)         : ~Rs 65–75 crore per month
  Payment summary table has data for   : 6 years (FY 2020-21 through 2025-26)
  Life certificates                    : ~130,000 rows (years 2022-2025)

ANTI-HALLUCINATION RULES:
  1. NEVER add a LIMIT clause to a COUNT(*) query — COUNT returns one row, not thousands.
  2. NEVER confuse "Disabled 90%+" with "Disabled Adult" — they are different category_codes.
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
    category_code VARCHAR(20) UNIQUE,  -- SC, WD, SW, DAC, DCB90, DC90, HIV
    category_name VARCHAR(100),        -- 'Senior Citizen','Widow','Single Woman','Disabled Adult','Disabled Child <90%','Disabled 90%+','HIV/AIDS'
    description TEXT,
    current_monthly_amount DECIMAL(10,2)  -- current Rs/month for this category
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
    payment_month SMALLINT,        -- auto-derived from payment_date
    payment_year SMALLINT,         -- auto-derived from payment_date
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20),            -- 'Paid','Pending','Failed' (Title Case!)
    payment_method VARCHAR(50),
    transaction_id VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- PAYMENT SUMMARY (pre-aggregated by year/month/district/taluka/category — 6 years of data)
-- USE THIS for "last 3 years payments", "YoY comparison", "district-wise payout by year"
-- DO NOT use raw payments table for historical/yearly queries — it only has last 6 months
CREATE TABLE payment_summary (
    summary_id SERIAL PRIMARY KEY,
    payment_year INT NOT NULL,
    payment_month INT NOT NULL,
    fiscal_year INT,
    district_id INT REFERENCES districts(district_id),
    taluka_id INT REFERENCES talukas(taluka_id),
    category_id INT REFERENCES categories(category_id),
    total_beneficiaries INT,
    paid_count INT,
    pending_count INT,
    failed_count INT,
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

-- SCHEME ENROLLMENTS (tracks category changes per beneficiary)
CREATE TABLE scheme_enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    beneficiary_id INT REFERENCES beneficiaries(beneficiary_id),
    category_id INT REFERENCES categories(category_id),
    enrollment_date DATE NOT NULL,
    end_date DATE,              -- NULL means currently enrolled
    is_current BOOLEAN DEFAULT TRUE
);

-- BENEFICIARY STATUS HISTORY (tracks Active->Inactive->Deceased transitions)
CREATE TABLE beneficiary_status_history (
    id SERIAL PRIMARY KEY,
    beneficiary_id INT REFERENCES beneficiaries(beneficiary_id),
    old_status VARCHAR(20),
    new_status VARCHAR(20),
    changed_at TIMESTAMPTZ DEFAULT NOW(),
    reason VARCHAR(200)
);

-- KEY RELATIONSHIPS FOR JOINS:
-- beneficiaries.category_id -> categories.category_id (get category_name, current_monthly_amount)
-- beneficiaries.district_id -> districts.district_id (get district_name)
-- beneficiaries.taluka_id -> talukas.taluka_id (get taluka_name)
-- beneficiaries.village_id -> villages.village_id (get village_name)
-- payments.beneficiary_id -> beneficiaries.beneficiary_id
-- payment_summary: pre-aggregated by (payment_year, payment_month, district_id, taluka_id, category_id)
-- life_certificates.beneficiary_id -> beneficiaries.beneficiary_id

-- COMMON QUERY PATTERNS:
-- Beneficiary count by category: SELECT c.category_name, COUNT(*) FROM beneficiaries b JOIN categories c ON b.category_id=c.category_id WHERE b.status='Active' GROUP BY c.category_name
-- YoY payments: SELECT payment_year, SUM(total_net_amount) AS total_paid FROM payment_summary GROUP BY payment_year ORDER BY payment_year
-- District-wise active: SELECT d.district_name, COUNT(*) FROM beneficiaries b JOIN districts d ON b.district_id=d.district_id WHERE b.status='Active' GROUP BY d.district_name
-- Life cert compliance: SELECT t.taluka_name, COUNT(lc.cert_id) AS submitted, COUNT(b.beneficiary_id) AS total FROM beneficiaries b JOIN talukas t ON b.taluka_id=t.taluka_id LEFT JOIN life_certificates lc ON lc.beneficiary_id=b.beneficiary_id AND lc.due_year=2025 WHERE b.status='Active' GROUP BY t.taluka_name
"""

# ── Few-Shot Examples ──────────────────────────────────────────────────────────

SHOTS = """Q: total beneficiaries
SQL: SELECT COUNT(*) AS total FROM beneficiaries;

Q: active beneficiaries
SQL: SELECT COUNT(*) AS total FROM beneficiaries WHERE status='Active';

Q: inactive beneficiaries count
SQL: SELECT COUNT(*) AS total FROM beneficiaries WHERE status='Inactive';

Q: deceased beneficiaries count
SQL: SELECT COUNT(*) AS total FROM beneficiaries WHERE status='Deceased';

Q: gender breakdown
SQL: SELECT gender, COUNT(*) AS count FROM beneficiaries WHERE status='Active' GROUP BY gender ORDER BY count DESC;

Q: district wise count
SQL: SELECT d.district_name AS district, COUNT(*) AS beneficiary_count FROM beneficiaries b JOIN districts d ON b.district_id = d.district_id GROUP BY d.district_name ORDER BY beneficiary_count DESC;

Q: compare north goa south goa
SQL: SELECT d.district_name AS district, COUNT(*) AS beneficiary_count FROM beneficiaries b JOIN districts d ON b.district_id = d.district_id WHERE b.status='Active' GROUP BY d.district_name ORDER BY beneficiary_count DESC;

Q: category breakdown
SQL: SELECT c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY c.category_name ORDER BY count DESC;

Q: total monthly payout
SQL: SELECT SUM(c.current_monthly_amount) AS total_monthly_payout FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active';

Q: category wise monthly payout
SQL: SELECT c.category_name AS category, COUNT(*) AS beneficiaries, SUM(c.current_monthly_amount) AS monthly_payout FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY c.category_name ORDER BY monthly_payout DESC;

Q: beneficiaries above 80
SQL: SELECT COUNT(*) AS count FROM beneficiaries WHERE age > 80 AND status='Active';

Q: taluka wise active beneficiaries
SQL: SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS active_count FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id JOIN districts d ON b.district_id = d.district_id WHERE b.status='Active' GROUP BY t.taluka_name, d.district_name ORDER BY active_count DESC;

Q: taluka wise beneficiaries in north goa
SQL: SELECT t.taluka_name AS taluka, COUNT(*) AS count FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id WHERE b.district_id = 1 AND b.status='Active' GROUP BY t.taluka_name ORDER BY count DESC;

Q: taluka wise beneficiaries in south goa
SQL: SELECT t.taluka_name AS taluka, COUNT(*) AS count FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id WHERE b.district_id = 2 AND b.status='Active' GROUP BY t.taluka_name ORDER BY count DESC;

Q: village wise beneficiaries
SQL: SELECT v.village_name AS village, t.taluka_name AS taluka, COUNT(*) AS count FROM beneficiaries b JOIN villages v ON b.village_id = v.village_id JOIN talukas t ON b.taluka_id = t.taluka_id WHERE b.status='Active' GROUP BY v.village_name, t.taluka_name ORDER BY count DESC LIMIT 20;

Q: widow count
SQL: SELECT COUNT(*) AS widow_count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE c.category_name = 'Widow' AND b.status='Active';

Q: senior citizen count
SQL: SELECT COUNT(*) AS senior_citizen_count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE c.category_name = 'Senior Citizen' AND b.status='Active';

Q: age distribution
SQL: SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age BETWEEN 40 AND 59 THEN '40-59' WHEN age BETWEEN 60 AND 69 THEN '60-69' WHEN age BETWEEN 70 AND 79 THEN '70-79' ELSE '80+' END AS age_group, COUNT(*) AS count FROM beneficiaries WHERE status='Active' GROUP BY age_group ORDER BY MIN(age);

Q: female beneficiaries by district
SQL: SELECT d.district_name AS district, COUNT(*) AS female_count FROM beneficiaries b JOIN districts d ON b.district_id = d.district_id WHERE b.gender='Female' AND b.status='Active' GROUP BY d.district_name ORDER BY female_count DESC;

Q: payment status summary
SQL: SELECT status, COUNT(*) AS count FROM payments GROUP BY status ORDER BY count DESC;

Q: total amount paid
SQL: SELECT SUM(amount) AS total_paid FROM payments WHERE status='Paid';

Q: payment compliance rate
SQL: SELECT status, COUNT(*) AS count, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage FROM payments GROUP BY status ORDER BY count DESC;

Q: top 5 talukas by active beneficiaries
SQL: SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS active_count FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id JOIN districts d ON b.district_id = d.district_id WHERE b.status='Active' GROUP BY t.taluka_name, d.district_name ORDER BY active_count DESC LIMIT 5;

Q: average age of beneficiaries by category
SQL: SELECT c.category_name AS category, ROUND(AVG(b.age), 1) AS average_age, COUNT(*) AS total FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY c.category_name ORDER BY average_age DESC;

Q: district and category cross breakdown
SQL: SELECT d.district_name AS district, c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN districts d ON b.district_id = d.district_id JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY d.district_name, c.category_name ORDER BY d.district_name, count DESC;

Q: taluka with most senior citizens
SQL: SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS senior_count FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id JOIN districts d ON b.district_id = d.district_id JOIN categories c ON b.category_id = c.category_id WHERE c.category_name='Senior Citizen' AND b.status='Active' GROUP BY t.taluka_name, d.district_name ORDER BY senior_count DESC LIMIT 1;

Q: disabled beneficiaries count
SQL: SELECT COUNT(*) AS disabled_count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE LOWER(c.category_name) LIKE '%disabled%' AND b.status='Active';

Q: HIV AIDS beneficiaries count
SQL: SELECT COUNT(*) AS hiv_aids_count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE c.category_name = 'HIV/AIDS' AND b.status='Active';

Q: beneficiaries above 60 years
SQL: SELECT COUNT(*) AS count FROM beneficiaries WHERE age >= 60 AND status='Active';

Q: beneficiaries between 60 and 70
SQL: SELECT COUNT(*) AS count FROM beneficiaries WHERE age BETWEEN 60 AND 70 AND status='Active';

Q: single woman count
SQL: SELECT COUNT(*) AS single_woman_count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE c.category_name = 'Single Woman' AND b.status='Active';

Q: total failed payments
SQL: SELECT COUNT(*) AS failed_count, SUM(amount) AS failed_amount FROM payments WHERE status='Failed';

Q: pending payments count
SQL: SELECT COUNT(*) AS pending_count FROM payments WHERE status='Pending';

Q: category wise average monthly amount
SQL: SELECT c.category_name AS category, c.current_monthly_amount AS monthly_amount_rs FROM categories c ORDER BY c.current_monthly_amount DESC;

Q: total beneficiaries per taluka in north goa with category breakdown
SQL: SELECT t.taluka_name AS taluka, c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id JOIN categories c ON b.category_id = c.category_id WHERE b.district_id = 1 AND b.status='Active' GROUP BY t.taluka_name, c.category_name ORDER BY t.taluka_name, count DESC;

Q: male vs female active beneficiaries percentage
SQL: SELECT gender, COUNT(*) AS count, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage FROM beneficiaries WHERE status='Active' GROUP BY gender ORDER BY count DESC;

Q: village wise top 10 beneficiaries
SQL: SELECT v.village_name AS village, t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS count FROM beneficiaries b JOIN villages v ON b.village_id = v.village_id JOIN talukas t ON b.taluka_id = t.taluka_id JOIN districts d ON b.district_id = d.district_id WHERE b.status='Active' GROUP BY v.village_name, t.taluka_name, d.district_name ORDER BY count DESC LIMIT 10;

Q: inactive beneficiaries by category
SQL: SELECT c.category_name AS category, COUNT(*) AS inactive_count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Inactive' GROUP BY c.category_name ORDER BY inactive_count DESC;

Q: deceased beneficiaries by district
SQL: SELECT d.district_name AS district, COUNT(*) AS deceased_count FROM beneficiaries b JOIN districts d ON b.district_id = d.district_id WHERE b.status='Deceased' GROUP BY d.district_name ORDER BY deceased_count DESC;

Q: combined total of active and inactive beneficiaries
SQL: SELECT COUNT(*) AS total FROM beneficiaries WHERE status IN ('Active', 'Inactive');

Q: combined total of active inactive and deceased
SQL: SELECT COUNT(*) AS total FROM beneficiaries;

Q: North Goa active beneficiaries
SQL: SELECT COUNT(*) AS count FROM beneficiaries WHERE district_id=1 AND status='Active';

Q: South Goa active beneficiaries
SQL: SELECT COUNT(*) AS count FROM beneficiaries WHERE district_id=2 AND status='Active';

Q: list all categories with beneficiary counts
SQL: SELECT c.category_name AS category, COUNT(*) AS count, c.current_monthly_amount AS monthly_amount_rs FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY c.category_name, c.current_monthly_amount ORDER BY count DESC;

Q: which category has the lowest beneficiaries / which category has the least beneficiaries
SQL: SELECT c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY c.category_name ORDER BY count ASC LIMIT 1;

Q: which category has the highest beneficiaries
SQL: SELECT c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY c.category_name ORDER BY count DESC LIMIT 1;

Q: all categories ranked by count / show all categories sorted by beneficiary count
SQL: SELECT c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY c.category_name ORDER BY count ASC;

Q: disabled 90 percent beneficiaries / disabled 90+ count
SQL: SELECT c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' AND LOWER(c.category_name) LIKE '%disabled%90%' GROUP BY c.category_name ORDER BY count DESC;

Q: year wise payment comparison / compare payments last 3 years / payment trend by year / compare payments 2023 vs 2024 vs 2025 / last 3 years payment comparison
SQL: SELECT ps.payment_year AS year, SUM(ps.total_beneficiaries) AS total_beneficiaries, SUM(ps.paid_count) AS paid_count, SUM(ps.pending_count) AS pending_count, SUM(ps.failed_count) AS failed_count, SUM(ps.total_base_amount) AS total_expected, SUM(ps.total_net_amount) AS total_paid, ROUND(SUM(ps.paid_count) * 100.0 / NULLIF(SUM(ps.total_beneficiaries), 0), 2) AS success_rate_pct FROM payment_summary ps WHERE ps.payment_year >= EXTRACT(YEAR FROM CURRENT_DATE)::INT - 2 GROUP BY ps.payment_year ORDER BY ps.payment_year;

Q: year wise registration trend / registrations by year
SQL: SELECT EXTRACT(YEAR FROM b.registration_date)::INT AS year, COUNT(*) AS registrations FROM beneficiaries b GROUP BY year ORDER BY year;

Q: monthly payment trend for 2024 / month wise payments 2024
SQL: SELECT ps.payment_month, SUM(ps.total_net_amount) AS total_paid, SUM(ps.paid_count) AS paid_count FROM payment_summary ps WHERE ps.payment_year = 2024 GROUP BY ps.payment_month ORDER BY ps.payment_month;

Q: year wise active beneficiary registrations by category
SQL: SELECT EXTRACT(YEAR FROM b.registration_date)::INT AS year, c.category_name AS category, COUNT(*) AS count FROM beneficiaries b JOIN categories c ON b.category_id = c.category_id WHERE b.status='Active' GROUP BY year, category ORDER BY year, count DESC;

Q: last 3 years payment summary / payment comparison across years / compare payout last 3 years
SQL: SELECT ps.payment_year AS year, SUM(ps.total_beneficiaries) AS total_beneficiaries, SUM(ps.paid_count) AS paid_count, SUM(ps.pending_count) AS pending_count, SUM(ps.failed_count) AS failed_count, SUM(ps.total_base_amount) AS total_expected, SUM(ps.total_net_amount) AS total_paid, ROUND(SUM(ps.paid_count) * 100.0 / NULLIF(SUM(ps.total_beneficiaries), 0), 2) AS success_rate_pct FROM payment_summary ps WHERE ps.payment_year >= EXTRACT(YEAR FROM CURRENT_DATE)::INT - 2 GROUP BY ps.payment_year ORDER BY ps.payment_year;

Q: last 3 years total payout amount
SQL: SELECT ps.payment_year AS year, SUM(ps.total_net_amount) AS total_annual_payout, SUM(ps.total_beneficiaries) AS beneficiaries_covered FROM payment_summary ps WHERE ps.payment_year >= EXTRACT(YEAR FROM CURRENT_DATE)::INT - 2 GROUP BY ps.payment_year ORDER BY ps.payment_year;

Q: year wise payment growth / payment trend by year / yearly payout comparison
SQL: SELECT ps.payment_year AS year, SUM(ps.total_beneficiaries) AS total_beneficiaries, SUM(ps.paid_count) AS paid_count, SUM(ps.failed_count) AS failed_count, SUM(ps.total_net_amount) AS total_paid, ROUND(SUM(ps.paid_count) * 100.0 / NULLIF(SUM(ps.total_beneficiaries), 0), 2) AS success_rate_pct FROM payment_summary ps GROUP BY ps.payment_year ORDER BY ps.payment_year;

Q: district wise payment comparison by year
SQL: SELECT d.district_name AS district, ps.payment_year, SUM(ps.total_net_amount) AS total_paid, SUM(ps.paid_count) AS paid_count FROM payment_summary ps JOIN districts d ON ps.district_id = d.district_id WHERE ps.payment_year >= EXTRACT(YEAR FROM CURRENT_DATE)::INT - 2 GROUP BY d.district_name, ps.payment_year ORDER BY d.district_name, ps.payment_year;

Q: category wise payment comparison last 3 years
SQL: SELECT c.category_name AS category, ps.payment_year, SUM(ps.total_net_amount) AS total_paid, SUM(ps.paid_count) AS paid_count FROM payment_summary ps JOIN categories c ON ps.category_id = c.category_id WHERE ps.payment_year >= EXTRACT(YEAR FROM CURRENT_DATE)::INT - 2 GROUP BY c.category_name, ps.payment_year ORDER BY c.category_name, ps.payment_year;

Q: pending payments by year
SQL: SELECT ps.payment_year, SUM(ps.pending_count) AS pending_count FROM payment_summary ps GROUP BY ps.payment_year ORDER BY ps.payment_year;

Q: payment batch summary / list all payment batches
SQL: SELECT pb.batch_reference, pb.payment_month, pb.payment_year, pb.fiscal_year_label, pb.batch_status, pb.total_beneficiaries, pb.total_amount, pb.paid_count, pb.failed_count, pb.pending_count FROM payment_batches pb ORDER BY pb.payment_year DESC, pb.payment_month DESC LIMIT 12;

Q: which batch had the most failures / batch with highest failed payments
SQL: SELECT pb.batch_reference, pb.payment_year, pb.payment_month, pb.failed_count, pb.total_beneficiaries, ROUND(pb.failed_count * 100.0 / NULLIF(pb.total_beneficiaries, 0), 2) AS failure_rate_pct FROM payment_batches pb ORDER BY pb.failed_count DESC LIMIT 5;

Q: total disbursed per fiscal year from batches / year wise batch total
SQL: SELECT pb.fiscal_year_label, SUM(pb.total_amount) AS total_disbursed, SUM(pb.paid_count) AS total_paid, SUM(pb.failed_count) AS total_failed FROM payment_batches pb WHERE pb.batch_status = 'Completed' GROUP BY pb.fiscal_year_label ORDER BY pb.fiscal_year_label;

Q: payment batch for april 2025 / batch details april 2025
SQL: SELECT pb.batch_reference, pb.batch_status, pb.total_beneficiaries, pb.total_amount, pb.paid_count, pb.failed_count, pb.initiated_at, pb.completed_at FROM payment_batches pb WHERE pb.payment_year = 2025 AND pb.payment_month = 4;

Q: life certificate compliance rate by taluka / which talukas have lowest compliance
SQL: SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(b.beneficiary_id) AS total_active, COUNT(lc.cert_id) AS submitted, COUNT(b.beneficiary_id) - COUNT(lc.cert_id) AS not_submitted, ROUND(COUNT(lc.cert_id) * 100.0 / NULLIF(COUNT(b.beneficiary_id), 0), 2) AS compliance_pct FROM beneficiaries b JOIN talukas t ON b.taluka_id = t.taluka_id JOIN districts d ON b.district_id = d.district_id LEFT JOIN life_certificates lc ON lc.beneficiary_id = b.beneficiary_id AND lc.due_year = EXTRACT(YEAR FROM CURRENT_DATE)::INT WHERE b.status = 'active' GROUP BY t.taluka_name, d.district_name ORDER BY compliance_pct ASC;

Q: how many beneficiaries have not submitted life certificate / pending life certificates 2025
SQL: SELECT COUNT(*) AS not_submitted FROM beneficiaries b WHERE b.status = 'active' AND NOT EXISTS (SELECT 1 FROM life_certificates lc WHERE lc.beneficiary_id = b.beneficiary_id AND lc.due_year = 2025);

Q: beneficiaries with payment suspended due to life certificate / suspended payments count
SQL: SELECT COUNT(DISTINCT lc.beneficiary_id) AS suspended_count FROM life_certificates lc WHERE lc.payment_suspended = TRUE;

Q: late life certificate submissions by category
SQL: SELECT c.category_name AS category, COUNT(lc.cert_id) AS late_submissions, ROUND(AVG(lc.days_late), 1) AS avg_days_late FROM life_certificates lc JOIN beneficiaries b ON lc.beneficiary_id = b.beneficiary_id JOIN categories c ON b.category_id = c.category_id WHERE lc.is_late_submission = TRUE GROUP BY c.category_name ORDER BY late_submissions DESC;

Q: life certificate compliance by district
SQL: SELECT d.district_name AS district, COUNT(b.beneficiary_id) AS total_active, COUNT(lc.cert_id) AS submitted, ROUND(COUNT(lc.cert_id) * 100.0 / NULLIF(COUNT(b.beneficiary_id), 0), 2) AS compliance_pct FROM beneficiaries b JOIN districts d ON b.district_id = d.district_id LEFT JOIN life_certificates lc ON lc.beneficiary_id = b.beneficiary_id AND lc.due_year = EXTRACT(YEAR FROM CURRENT_DATE)::INT WHERE b.status = 'active' GROUP BY d.district_name ORDER BY compliance_pct ASC;

Q: year wise life certificate submissions / how many life certs submitted each year
SQL: SELECT lc.due_year AS year, COUNT(*) AS total_submitted, COUNT(*) FILTER (WHERE lc.is_late_submission = TRUE) AS late_submissions, COUNT(*) FILTER (WHERE lc.payment_suspended = TRUE) AS suspensions FROM life_certificates lc GROUP BY lc.due_year ORDER BY lc.due_year DESC;
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
])


def is_followup(question: str, context: list[ConversationTurn]) -> bool:
    """
    Fast heuristic: returns True if the question is likely a follow-up
    that needs resolution before being routed. Skips Gemini call if False.
    """
    if not context:
        return False
    q = question.lower().strip()
    # Very short questions (≤5 words) are almost always follow-ups
    if len(q.split()) <= 5:
        return True
    return any(sig in q for sig in _FOLLOWUP_SIGNALS)


# ── Internal context formatter ─────────────────────────────────────────────────

def _fmt_context(context: list[ConversationTurn]) -> str:
    """
    Render conversation history into a compact block for injection into prompts.
    Includes raw sql_data when available so the model can reference actual numbers.
    """
    if not context:
        return ""
    lines = ["CONVERSATION HISTORY (for context — use to resolve references and maintain continuity):"]
    for i, t in enumerate(context, 1):
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
    ctx_lines = []
    for i, t in enumerate(context, 1):
        ctx_lines.append(f"[{i}] User asked: {t.resolved_question}")
        ctx_lines.append(f"     System answered: {t.answer}")
        if t.sql_data:
            ctx_lines.append(f"     Actual data returned: {json.dumps(t.sql_data, default=str)}")
    ctx_block = "\n".join(ctx_lines)

    return f"""You are a query resolver for the DSSY (Dayanand Social Security Scheme) analytics assistant.

Your job: Convert a follow-up question into a COMPLETE, STANDALONE question that can be answered independently, using the conversation history for context.

CONVERSATION HISTORY:
{ctx_block}

CURRENT QUESTION: "{question}"

RULES:
1. If the question references prior results ("what about X", "same for Y", "and Z?") → complete it fully
2. If the question asks for arithmetic on prior data ("sum of active and inactive", "add both") → write the full aggregation question with all terms named explicitly
3. If the question uses pronouns ("it", "that", "those", "them") → replace with the specific subject from history
4. If a filter from prior context applies ("in North Goa", "for widows") → carry it forward unless the user explicitly changes it
5. If the question is already self-contained → return it unchanged
6. ALWAYS stay within the DSSY scheme domain (beneficiaries, districts, categories, payments)

EXAMPLES:
History: active beneficiaries = 45,231 | Q: "what about inactive?" → "How many inactive beneficiaries are there?"
History: active = 45,231, inactive = 12,453 | Q: "sum of both?" → "What is the combined total of active and inactive beneficiaries?"
History: district-wise breakdown | Q: "which is highest?" → "Which district has the highest number of active beneficiaries?"
History: widow count in North Goa | Q: "what about South Goa?" → "How many widow beneficiaries are there in South Goa?"
History: category breakdown | Q: "now show females only" → "Show female beneficiary count by category"
History: active = 45,231, inactive = 12,453, deceased = 5,100 | Q: "total of all three?" → "What is the combined total of active, inactive, and deceased beneficiaries?"

Output ONLY the rewritten question — no explanation, no prefix, no quotes.

REWRITTEN QUESTION:"""


# ── Intent Classification ─────────────────────────────────────────────────────

def build_intent_prompt(question: str, context: list[ConversationTurn] = None) -> str:
    """Classify question as SQL or RAG, with conversation history for follow-up awareness."""
    ctx = _fmt_context(context or [])
    return f"""{ctx}Route this question to the correct handler for the DSSY government welfare analytics system.

SQL  — question wants COUNTS, STATISTICS, LISTS, or COMPARISONS from the beneficiary DATABASE
       Keywords: how many, count, total, show me, list, compare, district-wise, taluka-wise,
       active/inactive/deceased, payout, payment status, age distribution, top N, breakdown, trend,
       percentage, village-wise, gender, registration, female, male, category-wise, combined, sum,
       last 3 years, year-wise, year wise, monthly, 2023, 2024, 2025, payment trend, payment comparison,
       which is lowest, which is highest, which has least, which has most, rank, ranked, sort by,
       disabled 90, hiv, single woman, widow, senior citizen count, beneficiary count,
       life certificate compliance, life cert submitted, not submitted, payment suspended, suspended payments,
       batch, payment batch, ecs batch, batch status, batch failures, batch total, batch reference

RAG  — question wants RULES, POLICY, ELIGIBILITY, DOCUMENTS, AMOUNTS, HISTORY, or PROCEDURES
       from OFFICIAL SCHEME DOCUMENTS
       Keywords: who is eligible, what documents, how much pension, how to apply, what is DSSY,
       life certificate, cancellation, income limit, registration fee, amendment, launched, history,
       widow rules, disabled rules, grievance, which schemes merged, what is DDSSY, payment process,
       bank account, ECS, CAG audit, notification number, Griha Aadhar

EXAMPLES:
"How many active beneficiaries?" → SQL
"What documents are needed?" → RAG
"District-wise breakdown" → SQL
"Who is eligible for DSSY?" → RAG
"Total widow beneficiaries in North Goa" → SQL
"How much pension do disabled persons get?" → RAG
"Show me top 5 talukas" → SQL
"What is the income limit for DSSY?" → RAG
"Combined total of active and inactive beneficiaries" → SQL
"What about inactive beneficiaries?" → SQL

Reply ONLY with SQL or RAG.

Question: {question}
Answer:"""


# ── SQL Generation ────────────────────────────────────────────────────────────

def build_sql_prompt(question: str, context: list[ConversationTurn] = None) -> str:
    """Generate PostgreSQL SQL. Prior SQL context is passed as a follow-up hint."""
    prior_hint = ""
    if context:
        # Find the most recent SQL turn and surface it as continuity context
        sql_turns = [t for t in context if t.intent == "SQL"]
        if sql_turns:
            last = sql_turns[-1]
            data_hint = ""
            if last.sql_data:
                data_hint = f"\n      Previous data returned: {json.dumps(last.sql_data, default=str)}"
            prior_hint = (
                f"FOLLOW-UP CONTEXT: The user's previous question was: \"{last.resolved_question}\""
                f"{data_hint}\n"
                f"The current question may be a follow-up. Apply any implied filters or scope from above if needed.\n\n"
            )

    return f"""{SCHEMA}

{COUNTS_GUARD}

EXAMPLES:
{SHOTS}

{prior_hint}RULES:
- Output ONLY a valid PostgreSQL SQL SELECT or WITH statement
- Use standard PostgreSQL SQL syntax (NOT BigQuery syntax)
- Use plain table names without backticks or project prefix
- Table names: beneficiaries, categories, payments, payment_batches, life_certificates, districts, talukas, villages, banks, payment_summary, scheme_enrollments, beneficiary_status_history, fiscal_periods
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
- NEVER confuse category codes: DC90='Disabled 90%+', DCB90='Disabled Child <90%', DAC='Disabled Adult'
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
