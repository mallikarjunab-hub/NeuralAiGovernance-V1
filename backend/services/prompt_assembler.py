"""
Prompt Assembler — single source of truth for all LLM prompt construction.

Prompt types:
  build_question_resolver_prompt  — resolve follow-up questions to standalone
  build_intent_prompt             — classify SQL vs RAG
  build_sql_prompt                — BigQuery SQL generation
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

# ── BigQuery Schema ────────────────────────────────────────────────────────────

SCHEMA = """DATABASE: DSSY – Dayanand Social Security Scheme, Government of Goa
ENGINE: Google BigQuery (dataset = neural, project = edw-pilot)

TABLES (always use fully-qualified names: `edw-pilot.neural.table_name`):

  beneficiaries(
    beneficiary_key  INTEGER,   -- surrogate key, use for JOINs with payments
    beneficiary_id   STRING,    -- human-readable ID e.g. BEN-00001
    first_name       STRING,
    last_name        STRING,
    gender           STRING,    -- 'Male' | 'Female'
    age              INTEGER,
    dob              DATE,
    district_id      INTEGER,   -- FK → districts.district_id
    taluka_id        INTEGER,   -- FK → talukas.taluka_id
    village_id       INTEGER,   -- FK → villages.village_id
    category_id      INTEGER,   -- FK → categories.category_id
    bank_id          INTEGER,
    registration_date_id INTEGER,
    status           STRING     -- 'active' | 'inactive' | 'deceased'
  )

  districts(district_id INTEGER, district_name STRING)
    -- district_id=1 → 'North Goa', district_id=2 → 'South Goa'

  talukas(taluka_id INTEGER, taluka_name STRING, district_id INTEGER)

  villages(village_id INTEGER, village_name STRING, taluka_id INTEGER)

  categories(category_id INTEGER, category_name STRING, monthly_amount INTEGER)
    -- category_name: 'Senior Citizen' | 'Widow' | 'Single Woman' | 'Disabled' | 'HIV/AIDS'

  payments(
    beneficiary_key  INTEGER,   -- FK → beneficiaries.beneficiary_key
    date_id          INTEGER,
    amount           INTEGER,   -- payment amount in Rs.
    payment_status   STRING     -- 'paid' | 'pending' | 'failed'
  )

  banks(bank_id INTEGER, bank_name STRING)
  dates(date_id INTEGER, date DATE)
    -- date is the actual calendar date. Use for year/month filtering.
    -- For payments year filter: JOIN dates pd ON p.date_id = pd.date_id, then EXTRACT(YEAR FROM pd.date)
    -- For registration year filter: JOIN dates rd ON b.registration_date_id = rd.date_id, then EXTRACT(YEAR FROM rd.date)

CRITICAL JOIN RULES:
  - payments → beneficiaries : JOIN ON payments.beneficiary_key = beneficiaries.beneficiary_key
  - payments → dates         : JOIN `edw-pilot.neural.dates` pd ON p.date_id = pd.date_id
  - beneficiaries → dates    : JOIN `edw-pilot.neural.dates` rd ON b.registration_date_id = rd.date_id
  - beneficiaries has NO village column — always JOIN villages ON b.village_id = v.village_id
  - beneficiaries date column is 'dob', NOT 'date_of_birth'
  - payments amount column is 'amount', NOT 'payment_amount'
  - payments has NO payment_date column — use dates table JOIN for time filtering
  - categories table has more entries than the common 5 — always query it, never assume all category names
"""

# ── Few-Shot Examples ──────────────────────────────────────────────────────────

SHOTS = """Q: total beneficiaries
SQL: SELECT COUNT(*) AS total FROM `edw-pilot.neural.beneficiaries`;

Q: active beneficiaries
SQL: SELECT COUNT(*) AS total FROM `edw-pilot.neural.beneficiaries` WHERE status='active';

Q: inactive beneficiaries count
SQL: SELECT COUNT(*) AS total FROM `edw-pilot.neural.beneficiaries` WHERE status='inactive';

Q: deceased beneficiaries count
SQL: SELECT COUNT(*) AS total FROM `edw-pilot.neural.beneficiaries` WHERE status='deceased';

Q: gender breakdown
SQL: SELECT gender, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` WHERE status='active' GROUP BY gender ORDER BY count DESC;

Q: district wise count
SQL: SELECT d.district_name AS district, COUNT(*) AS beneficiary_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id GROUP BY d.district_name ORDER BY beneficiary_count DESC;

Q: compare north goa south goa
SQL: SELECT d.district_name AS district, COUNT(*) AS beneficiary_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id WHERE b.status='active' GROUP BY d.district_name ORDER BY beneficiary_count DESC;

Q: category breakdown
SQL: SELECT c.category_name AS category, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE b.status='active' GROUP BY c.category_name ORDER BY count DESC;

Q: total monthly payout
SQL: SELECT SUM(c.monthly_amount) AS total_monthly_payout FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE b.status='active';

Q: category wise monthly payout
SQL: SELECT c.category_name AS category, COUNT(*) AS beneficiaries, SUM(c.monthly_amount) AS monthly_payout FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE b.status='active' GROUP BY c.category_name ORDER BY monthly_payout DESC;

Q: beneficiaries above 80
SQL: SELECT COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` WHERE age > 80 AND status='active';

Q: taluka wise active beneficiaries
SQL: SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS active_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id WHERE b.status='active' GROUP BY t.taluka_name, d.district_name ORDER BY active_count DESC;

Q: taluka wise beneficiaries in north goa
SQL: SELECT t.taluka_name AS taluka, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id WHERE b.district_id = 1 AND b.status='active' GROUP BY t.taluka_name ORDER BY count DESC;

Q: taluka wise beneficiaries in south goa
SQL: SELECT t.taluka_name AS taluka, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id WHERE b.district_id = 2 AND b.status='active' GROUP BY t.taluka_name ORDER BY count DESC;

Q: village wise beneficiaries
SQL: SELECT v.village_name AS village, t.taluka_name AS taluka, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.villages` v ON b.village_id = v.village_id JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id WHERE b.status='active' GROUP BY v.village_name, t.taluka_name ORDER BY count DESC LIMIT 20;

Q: widow count
SQL: SELECT COUNT(*) AS widow_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE c.category_name = 'Widow' AND b.status='active';

Q: senior citizen count
SQL: SELECT COUNT(*) AS senior_citizen_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE c.category_name = 'Senior Citizen' AND b.status='active';

Q: age distribution
SQL: SELECT CASE WHEN age < 40 THEN 'Under 40' WHEN age BETWEEN 40 AND 59 THEN '40-59' WHEN age BETWEEN 60 AND 69 THEN '60-69' WHEN age BETWEEN 70 AND 79 THEN '70-79' ELSE '80+' END AS age_group, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` WHERE status='active' GROUP BY age_group ORDER BY MIN(age);

Q: female beneficiaries by district
SQL: SELECT d.district_name AS district, COUNT(*) AS female_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id WHERE b.gender='Female' AND b.status='active' GROUP BY d.district_name ORDER BY female_count DESC;

Q: payment status summary
SQL: SELECT payment_status, COUNT(*) AS count FROM `edw-pilot.neural.payments` GROUP BY payment_status ORDER BY count DESC;

Q: total amount paid
SQL: SELECT SUM(amount) AS total_paid FROM `edw-pilot.neural.payments` WHERE payment_status='paid';

Q: payment compliance rate
SQL: SELECT payment_status, COUNT(*) AS count, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage FROM `edw-pilot.neural.payments` GROUP BY payment_status ORDER BY count DESC;

Q: top 5 talukas by active beneficiaries
SQL: SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS active_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id WHERE b.status='active' GROUP BY t.taluka_name, d.district_name ORDER BY active_count DESC LIMIT 5;

Q: average age of beneficiaries by category
SQL: SELECT c.category_name AS category, ROUND(AVG(b.age), 1) AS average_age, COUNT(*) AS total FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE b.status='active' GROUP BY c.category_name ORDER BY average_age DESC;

Q: district and category cross breakdown
SQL: SELECT d.district_name AS district, c.category_name AS category, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE b.status='active' GROUP BY d.district_name, c.category_name ORDER BY d.district_name, count DESC;

Q: taluka with most senior citizens
SQL: SELECT t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS senior_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE c.category_name='Senior Citizen' AND b.status='active' GROUP BY t.taluka_name, d.district_name ORDER BY senior_count DESC LIMIT 1;

Q: disabled beneficiaries count
SQL: SELECT COUNT(*) AS disabled_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE c.category_name = 'Disabled' AND b.status='active';

Q: HIV AIDS beneficiaries count
SQL: SELECT COUNT(*) AS hiv_aids_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE c.category_name = 'HIV/AIDS' AND b.status='active';

Q: beneficiaries above 60 years
SQL: SELECT COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` WHERE age >= 60 AND status='active';

Q: beneficiaries between 60 and 70
SQL: SELECT COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` WHERE age BETWEEN 60 AND 70 AND status='active';

Q: single woman count
SQL: SELECT COUNT(*) AS single_woman_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE c.category_name = 'Single Woman' AND b.status='active';

Q: total failed payments
SQL: SELECT COUNT(*) AS failed_count, SUM(amount) AS failed_amount FROM `edw-pilot.neural.payments` WHERE payment_status='failed';

Q: pending payments count
SQL: SELECT COUNT(*) AS pending_count FROM `edw-pilot.neural.payments` WHERE payment_status='pending';

Q: category wise average monthly amount
SQL: SELECT c.category_name AS category, c.monthly_amount AS monthly_amount_rs FROM `edw-pilot.neural.categories` c ORDER BY c.monthly_amount DESC;

Q: total beneficiaries per taluka in north goa with category breakdown
SQL: SELECT t.taluka_name AS taluka, c.category_name AS category, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE b.district_id = 1 AND b.status='active' GROUP BY t.taluka_name, c.category_name ORDER BY t.taluka_name, count DESC;

Q: male vs female active beneficiaries percentage
SQL: SELECT gender, COUNT(*) AS count, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage FROM `edw-pilot.neural.beneficiaries` WHERE status='active' GROUP BY gender ORDER BY count DESC;

Q: village wise top 10 beneficiaries
SQL: SELECT v.village_name AS village, t.taluka_name AS taluka, d.district_name AS district, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.villages` v ON b.village_id = v.village_id JOIN `edw-pilot.neural.talukas` t ON b.taluka_id = t.taluka_id JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id WHERE b.status='active' GROUP BY v.village_name, t.taluka_name, d.district_name ORDER BY count DESC LIMIT 10;

Q: inactive beneficiaries by category
SQL: SELECT c.category_name AS category, COUNT(*) AS inactive_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE b.status='inactive' GROUP BY c.category_name ORDER BY inactive_count DESC;

Q: deceased beneficiaries by district
SQL: SELECT d.district_name AS district, COUNT(*) AS deceased_count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.districts` d ON b.district_id = d.district_id WHERE b.status='deceased' GROUP BY d.district_name ORDER BY deceased_count DESC;

Q: combined total of active and inactive beneficiaries
SQL: SELECT COUNT(*) AS total FROM `edw-pilot.neural.beneficiaries` WHERE status IN ('active', 'inactive');

Q: combined total of active inactive and deceased
SQL: SELECT COUNT(*) AS total FROM `edw-pilot.neural.beneficiaries`;

Q: North Goa active beneficiaries
SQL: SELECT COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` WHERE district_id=1 AND status='active';

Q: South Goa active beneficiaries
SQL: SELECT COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` WHERE district_id=2 AND status='active';

Q: list all categories with beneficiary counts
SQL: SELECT c.category_name AS category, COUNT(*) AS count, c.monthly_amount AS monthly_amount_rs FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE b.status='active' GROUP BY c.category_name, c.monthly_amount ORDER BY count DESC;

Q: which category has the lowest beneficiaries / which category has the least beneficiaries
SQL: SELECT c.category_name AS category, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE b.status='active' GROUP BY c.category_name ORDER BY count ASC LIMIT 1;

Q: which category has the highest beneficiaries
SQL: SELECT c.category_name AS category, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE b.status='active' GROUP BY c.category_name ORDER BY count DESC LIMIT 1;

Q: year wise payment comparison / compare payments last 3 years / payment trend by year
SQL: SELECT EXTRACT(YEAR FROM pd.date) AS year, COUNT(DISTINCT p.beneficiary_key) AS beneficiaries_paid, SUM(p.amount) AS total_amount, COUNT(*) AS payment_count FROM `edw-pilot.neural.payments` p JOIN `edw-pilot.neural.dates` pd ON p.date_id = pd.date_id WHERE p.payment_status = 'paid' GROUP BY year ORDER BY year;

Q: compare payments 2023 vs 2024 vs 2025
SQL: SELECT EXTRACT(YEAR FROM pd.date) AS year, SUM(p.amount) AS total_paid, COUNT(*) AS payment_count, COUNT(DISTINCT p.beneficiary_key) AS unique_beneficiaries FROM `edw-pilot.neural.payments` p JOIN `edw-pilot.neural.dates` pd ON p.date_id = pd.date_id WHERE p.payment_status = 'paid' AND EXTRACT(YEAR FROM pd.date) IN (2023, 2024, 2025) GROUP BY year ORDER BY year;

Q: year wise registration trend / registrations by year
SQL: SELECT EXTRACT(YEAR FROM rd.date) AS year, COUNT(*) AS registrations FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.dates` rd ON b.registration_date_id = rd.date_id GROUP BY year ORDER BY year;

Q: monthly payment trend for 2024 / month wise payments 2024
SQL: SELECT FORMAT_DATE('%Y-%m', pd.date) AS month, SUM(p.amount) AS total_paid, COUNT(*) AS payment_count FROM `edw-pilot.neural.payments` p JOIN `edw-pilot.neural.dates` pd ON p.date_id = pd.date_id WHERE p.payment_status = 'paid' AND EXTRACT(YEAR FROM pd.date) = 2024 GROUP BY month ORDER BY month;

Q: year wise active beneficiary registrations by category
SQL: SELECT EXTRACT(YEAR FROM rd.date) AS year, c.category_name AS category, COUNT(*) AS count FROM `edw-pilot.neural.beneficiaries` b JOIN `edw-pilot.neural.dates` rd ON b.registration_date_id = rd.date_id JOIN `edw-pilot.neural.categories` c ON b.category_id = c.category_id WHERE b.status='active' GROUP BY year, category ORDER BY year, count DESC;

Q: last 3 years payment summary / payment comparison across years
SQL: SELECT EXTRACT(YEAR FROM pd.date) AS year, p.payment_status, COUNT(*) AS count, SUM(p.amount) AS total_amount FROM `edw-pilot.neural.payments` p JOIN `edw-pilot.neural.dates` pd ON p.date_id = pd.date_id WHERE EXTRACT(YEAR FROM pd.date) >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)) GROUP BY year, p.payment_status ORDER BY year, p.payment_status;

Q: pending payments by year
SQL: SELECT EXTRACT(YEAR FROM pd.date) AS year, COUNT(*) AS pending_count, SUM(p.amount) AS pending_amount FROM `edw-pilot.neural.payments` p JOIN `edw-pilot.neural.dates` pd ON p.date_id = pd.date_id WHERE p.payment_status = 'pending' GROUP BY year ORDER BY year;
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
       percentage, village-wise, gender, registration, female, male, category-wise, combined, sum

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
    """Generate BigQuery SQL. Prior SQL context is passed as a follow-up hint."""
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

EXAMPLES:
{SHOTS}

{prior_hint}RULES:
- Output ONLY a valid BigQuery SQL SELECT or WITH statement
- No markdown, no backticks, no explanation
- Never select PII columns (aadhar, phone, address, bank_account)
- Always use proper JOIN conditions with correct column names
- Use BigQuery SQL dialect (not PostgreSQL)
- For "top N" questions use LIMIT N with ORDER BY DESC
- For percentage questions use ROUND(x * 100.0 / SUM(x) OVER(), 2)
- For age range questions use BETWEEN or CASE WHEN age groups
- Default status filter is 'active' unless question asks for inactive/deceased/all
- For combined totals (active + inactive) use WHERE status IN ('active', 'inactive')
- Always alias columns with readable names (AS district, AS count, etc.)
- For cross-tab/breakdown questions GROUP BY both dimensions
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
{ctx}The user asked: "{question}"
Query context: {sql[:200]}
Database returned {row_count} rows: {json.dumps(results[:50], default=str)}

CRITICAL GROUNDING RULES — READ BEFORE ANSWERING:
- Use ONLY numbers that appear verbatim in the data rows shown above. NEVER invent, estimate, or use numbers from training knowledge.
- For ranking questions (highest/lowest/most/least/top/bottom): mentally scan ALL provided rows and identify the actual maximum or minimum from the data. Do not guess.
- If all categories or districts are listed, scan every row to find the true min/max — do not assume any category is highest or lowest without checking.
- Never write a number that is not present in the data above.

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
