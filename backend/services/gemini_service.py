"""
Gemini Service — Google AI Studio
All AI: intent classify · SQL gen (BigQuery) · NL answer · embeddings · RAG answer
"""
import re, json, logging, httpx
from backend.config import settings

logger     = logging.getLogger(__name__)
BASE       = "https://generativelanguage.googleapis.com/v1beta"
CHAT       = "gemini-2.5-flash-lite"
EMBED      = "gemini-embedding-001"
EMBED_DIM  = 768
LANGS      = {"en":"English","hi":"Hindi","te":"Telugu","kn":"Kannada","mr":"Marathi","kok":"Konkani"}

# ── BigQuery Schema ──────────────────────────────────────────

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

  banks(bank_id INTEGER, ...)
  dates(date_id INTEGER, ...)

CRITICAL JOIN RULES:
  - payments → beneficiaries : JOIN ON payments.beneficiary_key = beneficiaries.beneficiary_key
  - beneficiaries has NO village column — always JOIN villages ON b.village_id = v.village_id
  - beneficiaries date column is 'dob', NOT 'date_of_birth'
  - payments amount column is 'amount', NOT 'payment_amount'
  - payments has NO payment_date column
"""

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
"""

FORBIDDEN = [r'\bINSERT\b',r'\bUPDATE\b',r'\bDELETE\b',r'\bDROP\b',r'\bCREATE\b',r'\bALTER\b',r'\bTRUNCATE\b',r'\bMERGE\b']


# ── Intent Classification ─────────────────────────────────────

async def classify_intent(question: str) -> str:
    """Returns 'SQL' or 'RAG'. Edge cases handled before this is called."""
    prompt = f"""Route this question to the correct handler for a government welfare analytics system.

SQL  — question wants COUNTS, STATISTICS, LISTS, or COMPARISONS from the beneficiary DATABASE
       Keywords: how many, count, total, show me, list, compare, district-wise, taluka-wise,
       active/inactive/deceased, payout, payment status, age distribution, top N, breakdown, trend,
       percentage, village-wise, gender, registration, female, male, category-wise

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

Reply ONLY with SQL or RAG.

Question: {question}
Answer:"""
    try:
        r = await _call(prompt, 0.0)
        i = r.strip().upper().split()[0] if r.strip() else "SQL"
        return i if i in ("SQL", "RAG") else "SQL"
    except:
        return "SQL"


# ── SQL Generation ────────────────────────────────────────────

async def generate_sql(question: str) -> tuple[str, float]:
    prompt = f"""{SCHEMA}

EXAMPLES:
{SHOTS}

RULES:
- Output ONLY a valid BigQuery SQL SELECT or WITH statement
- No markdown, no backticks, no explanation
- Never select PII columns (aadhar, phone, address, bank_account)
- Always use proper JOIN conditions with correct column names
- Use BigQuery SQL dialect (not PostgreSQL)
- For "top N" questions use LIMIT N with ORDER BY DESC
- For percentage questions use ROUND(x * 100.0 / SUM(x) OVER(), 2)
- For age range questions use BETWEEN or CASE WHEN age groups
- Default status filter is 'active' unless question asks for inactive/deceased/all
- Always alias columns with readable names (AS district, AS count, etc.)
- For cross-tab/breakdown questions GROUP BY both dimensions
- If the question cannot be answered from the schema, output exactly: CANNOT_ANSWER

Question: {question}
SQL:"""
    raw = await _call(prompt, 0.05)
    sql = _clean_sql(raw)
    conf = 0.0 if "CANNOT_ANSWER" in sql else 0.9
    logger.info(f"Generated SQL: {sql[:150]}")
    return sql, conf


# ── Natural Language Answer ───────────────────────────────────

async def generate_nl_answer(question: str, sql: str, results: list, row_count: int, language: str = "en") -> str:
    if row_count == 0:
        no_data = {
            "en": "No records found for this query. Please try a different filter or question.",
            "hi": "इस प्रश्न के लिए कोई रिकॉर्ड नहीं मिला। कृपया कोई अन्य प्रश्न आज़माएं।",
            "te": "ఈ ప్రశ్నకు రికార్డులు కనుగొనబడలేదు। దయచేసి వేరే ప్రశ్న ప్రయత్నించండి।",
        }
        return no_data.get(language, no_data["en"])

    lang_name = LANGS.get(language, "English")
    lang_instr = f"Respond in {lang_name}." if language != "en" else ""

    prompt = f"""You are a DSSY scheme analytics assistant for the Department of Social Welfare, Government of Goa.
The user asked: "{question}"
Query context: {sql[:200]}
Database returned {row_count} rows: {json.dumps(results[:15], default=str)}

Write a clear, insightful 2-4 sentence answer using the exact numbers from the data.
- Lead with the most important finding or direct answer to the question
- Highlight the highest/lowest values or notable patterns if present
- If there are multiple categories/districts, mention the top 2-3 by name with their numbers
- Do NOT address the user by any title — answer directly
- Do NOT mention SQL, databases, queries, or technical terms
- Format large numbers with Indian comma notation (e.g., 1,40,000 not 140000)
- Use Rs. prefix for monetary amounts
{lang_instr}

Answer:"""
    return await _call(prompt, 0.2)


# ── RAG Answer ────────────────────────────────────────────────

async def rag_answer(question: str, chunks: list[str], language: str = "en") -> str:
    if not chunks:
        return "This information is not available in the DSSY knowledge base. Please contact the Directorate of Social Welfare, Government of Goa."

    lang_name = LANGS.get(language, "English")
    lang_instr = f"Respond in {lang_name}." if language != "en" else ""

    prompt = f"""You are an expert assistant for the Dayanand Social Security Scheme (DSSY), Government of Goa.

Instructions:
- Answer ONLY from the provided context. If the answer is not in the context, say clearly: "This specific information is not available in the DSSY scheme documents."
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
    return await _call(prompt, 0.15)


# ── Embeddings ────────────────────────────────────────────────

async def embed_text(text: str) -> list[float]:
    url = f"{BASE}/models/{EMBED}:embedContent?key={settings.GEMINI_API_KEY}"
    payload = {
        "model": f"models/{EMBED}",
        "content": {"parts": [{"text": text[:2000]}]},
        "outputDimensionality": EMBED_DIM,
    }
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(url, json=payload)
    r.raise_for_status()
    return r.json()["embedding"]["values"]


# ── Health Check ──────────────────────────────────────────────

async def check_health() -> bool:
    try:
        return len(await _call("Say OK", 0.0)) > 0
    except:
        return False


# ── SQL Validation ────────────────────────────────────────────

def validate_sql(sql: str) -> tuple[bool, str]:
    u = sql.upper().strip()
    if not (u.startswith("SELECT") or u.startswith("WITH")):
        return False, "Only SELECT/WITH statements allowed"
    for p in FORBIDDEN:
        if re.search(p, sql, re.IGNORECASE):
            return False, f"Forbidden keyword detected"
    if u.count("(") != u.count(")"):
        return False, "Unbalanced parentheses"
    return True, "OK"


# ── Chart Suggestion ──────────────────────────────────────────

def suggest_chart(results: list) -> str | None:
    if not results or len(results) < 2:
        return None
    cols = list(results[0].keys())
    if len(cols) < 2:
        return None
    num = [c for c in cols if all(_is_num(r.get(c)) for r in results)]
    lbl = [c for c in cols if c not in num]
    if not num or not lbl:
        return None
    lc = lbl[0].lower()
    if any(k in lc for k in ["month", "year", "date", "period", "week", "quarter", "trend"]):
        return "line"
    if len(results) <= 6:
        return "doughnut"
    return "bar"


# ── Internal Helpers ──────────────────────────────────────────

def _is_num(v):
    try:
        float(str(v))
        return True
    except:
        return False

def _clean_sql(raw: str) -> str:
    sql = re.sub(r"```sql\s*", "", raw, flags=re.IGNORECASE)
    sql = re.sub(r"```\s*", "", sql).strip()
    if sql and not sql.endswith(";"):
        sql += ";"
    return sql

async def _call(prompt: str, temp: float, max_tokens: int = 1024) -> str:
    url = f"{BASE}/models/{CHAT}:generateContent?key={settings.GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temp, "maxOutputTokens": max_tokens},
    }
    async with httpx.AsyncClient(timeout=60.0) as c:
        r = await c.post(url, json=payload)
    r.raise_for_status()
    return r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()