# Neural AI Governance v3.0 -- DSSY Analytics

**AI-powered conversational analytics for the Dayanand Social Security Scheme (DSSY),
Department of Social Welfare, Government of Goa.**

Built by [Bharath Light House Software Solutions Pvt Ltd](https://www.bharathlighthouse.com)

---

## What It Does

Government officers ask questions in plain English (or Hindi/Telugu/Kannada/Marathi/Konkani)
and get instant answers with charts -- no SQL knowledge needed.

```
Officer: "How many active beneficiaries are in North Goa?"
System:  "There are 1,32,540 active beneficiaries in North Goa..."  + bar chart

Officer: "What about South Goa?"
System:  "South Goa has 1,49,460 active beneficiaries..."  (understands follow-up)

Officer: "Sum of both?"
System:  "The combined total of North Goa and South Goa is 2,82,000..."
```

---

## Architecture

```
                        User Question (text or voice)
                               |
                    +----------v-----------+
                    |   Edge Case Handler   |  <-- greetings, identity, off-topic
                    |   (regex, FREE)       |      zero API cost, instant response
                    +----------+-----------+
                               |  not edge case
                    +----------v-----------+
                    | Question Resolver     |  <-- multi-turn: "what about inactive?"
                    | (Gemini, context)     |      --> "How many inactive beneficiaries?"
                    +----------+-----------+
                               |
                    +----------v-----------+
                    |  Intent Classifier    |  <-- SQL or RAG?
                    |  (Gemini, zero-temp)  |
                    +---+-------------+----+
                        |             |
              +---------v---+   +----v----------+
              |  SQL Path   |   |   RAG Path    |
              |             |   |               |
              | Gemini      |   | pgvector      |
              | generates   |   | hybrid search |
              | PostgreSQL  |   | (vector 60%   |
              | SQL query   |   |  + keyword    |
              |      |      |   |    40%)       |
              |      v      |   |      |        |
              | Neon DB     |   |      v        |
              | executes    |   | Gemini        |
              | query       |   | answers from  |
              |      |      |   | doc chunks    |
              +------+------+   +------+--------+
                     |                 |
                     +--------+--------+
                              |
                    +---------v----------+
                    |  NL Answer + Chart  |  <-- anti-hallucination grounding
                    |  (Gemini)           |      numbers ONLY from query results
                    +--------------------+
```

### Smart Fallbacks

- SQL query fails --> try RAG
- RAG low confidence --> try SQL
- SQL returns 0 rows --> try RAG before saying "no data"
- Both fail --> helpful error with example questions

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Backend | Python 3.11+, FastAPI, Uvicorn |
| Database | Neon PostgreSQL (data + pgvector RAG) |
| AI Engine | Gemini 2.5 Flash Lite (chat) + Gemini Embedding 001 (vectors) |
| Frontend | Single-page HTML, Chart.js, vanilla JS |
| Cache | Redis (optional, degrades gracefully) |

---

## Database Schema

### Core Tables

| Table | Rows | Purpose |
|-------|------|---------|
| `beneficiaries` | ~300,000 | All DSSY beneficiaries with demographics |
| `categories` | 7 | SC, Widow, Single Woman, Disabled Adult, Disabled <90%, Disabled 90%+, HIV/AIDS |
| `districts` | 2 | North Goa, South Goa |
| `talukas` | 12 | All Goa talukas |
| `villages` | ~350 | Village-level geography |
| `banks` | 10 | Payment banks |

### Analytics Tables

| Table | Rows | Purpose |
|-------|------|---------|
| `payment_summary` | ~1,680 | Pre-aggregated by year/month/district/category (6 years, FY 2020-26) |
| `payment_batches` | 72 | Monthly ECS batch records |
| `payments` | ~35,000 | Individual payment records (last 6 months) |
| `life_certificates` | ~130,000 | Annual compliance submissions (2022-2025) |
| `scheme_enrollments` | ~90,000 | Category enrollment history |
| `beneficiary_status_history` | ~318,000 | Active/Inactive/Deceased transitions |

### Supporting Tables

| Table | Purpose |
|-------|---------|
| `fiscal_periods` | April-March fiscal year mapping |
| `officers` | Administrative staff (who approved/processed) |
| `audit_log` | Every INSERT/UPDATE on beneficiaries |
| `category_amount_history` | Monthly amount changes over years |
| `payment_forecasts` | Forecast outputs for minister dashboards |
| `analytics_query_log` | NL question -> SQL trace for debugging |
| `dashboard_views` | Saved dynamic dashboard presets |
| `document_chunks` | pgvector RAG chunks (DSSY knowledge base) |
| `conversation_context` | Multi-turn session persistence |

### Status Values (Title Case -- critical for queries)

- Beneficiaries: `'Active'`, `'Inactive'`, `'Deceased'`
- Payments: `'Paid'`, `'Pending'`, `'Failed'`

---

## Project Structure

```
NAG_V3/
|-- backend/
|   |-- main.py                  # FastAPI app, lifespan, health check
|   |-- config.py                # Pydantic settings from .env
|   |-- database.py              # Neon PostgreSQL engine, session, SQL execution
|   |-- schemas.py               # Pydantic request/response models
|   |-- routers/
|   |   |-- query.py             # /api/query -- main 3-way routing endpoint
|   |   |-- analytics.py         # /api/analytics/dashboard -- KPI cards + charts
|   |   |-- beneficiaries.py     # /api/beneficiaries -- paginated listing
|   |   |-- rag.py               # /api/rag/status -- ingestion status
|   |-- services/
|   |   |-- ai_service.py        # Gemini HTTP transport (retry, circuit breaker, dedup)
|   |   |-- gemini_service.py    # Domain logic (classify, generate SQL, NL answer)
|   |   |-- prompt_assembler.py  # All LLM prompts, schema, few-shots, COUNTS_GUARD
|   |   |-- rag_service.py       # pgvector + keyword hybrid search, chunking, ingestion
|   |   |-- context_store.py     # Multi-turn conversation persistence (L1 cache + Neon)
|   |   |-- edge_handler.py      # Regex-based edge case detection (greetings, off-topic)
|   |   |-- cache.py             # Redis cache wrapper (optional)
|-- frontend/
|   |-- index.html               # Single-page dashboard + chat UI
|-- documents/
|   |-- dssy_knowledge_base.md   # DSSY scheme docs (ingested into RAG)
|-- scripts/
|   |-- create_neon_schema.py    # One-time DB schema setup (35+ tables/views/indexes)
|   |-- seed_dssy.py             # Generate 300k synthetic beneficiaries + payment history
|   |-- keep_warm.py             # Ping /health every 4 min to prevent Neon sleep
|-- requirements.txt
|-- .env                         # Environment variables (not committed)
```

---

## Setup & Run

### Prerequisites

- Python 3.11+
- Neon PostgreSQL account (free tier works)
- Gemini API key from [Google AI Studio](https://aistudio.google.com/apikey)

### 1. Install dependencies

```bash
cd NAG_V3
pip install -r requirements.txt
```

### 2. Configure environment

Create a `.env` file in the project root:

```env
NEON_DATABASE_URL=postgresql://user:pass@ep-xxx.neon.tech/neondb?sslmode=require
GEMINI_API_KEY=your-gemini-api-key
ENVIRONMENT=development
DEBUG=false
LOG_LEVEL=INFO
SECRET_KEY=your-secret-key
CORS_ORIGINS=http://localhost:8000,http://localhost:3000
```

### 3. Create database schema (first time only)

```bash
python scripts/create_neon_schema.py
```

This creates all tables, indexes, materialized views, and seeds reference data
(districts, talukas, categories, fiscal periods).

### 4. Seed synthetic data (first time only)

```bash
pip install faker
python scripts/seed_dssy.py
```

Generates ~875,000 rows: 300k beneficiaries, 6 years of payment history,
life certificates, enrollment history. Takes 5-10 minutes.

### 5. Run the application

```bash
python -m backend.main
```

The app starts at **http://localhost:8000**

### 6. Keep Neon warm (optional, separate terminal)

```bash
python scripts/keep_warm.py
```

Pings `/health` every 4 minutes to prevent Neon free tier auto-suspend.

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Frontend dashboard |
| `/health` | GET | Neon + Gemini + Redis status |
| `/api/query` | POST | Main query (3-way auto-routing) |
| `/api/query/transcribe` | POST | Voice-to-text (audio upload) |
| `/api/query/suggestions` | GET | Categorized example questions |
| `/api/analytics/dashboard` | GET | Dashboard KPIs, charts, trends |
| `/api/beneficiaries` | GET | Paginated beneficiary list with filters |
| `/api/rag/status` | GET | RAG document ingestion status |
| `/docs` | GET | Swagger UI (dev only) |

### Query Request

```json
POST /api/query
{
  "question": "How many active beneficiaries are in North Goa?",
  "language": "en",
  "session_id": "browser-session-uuid",
  "include_sql": false
}
```

### Query Response

```json
{
  "question": "How many active beneficiaries are in North Goa?",
  "answer": "There are 1,32,540 active beneficiaries in North Goa...",
  "intent": "SQL",
  "data": [{"district": "North Goa", "count": 132540}],
  "row_count": 1,
  "execution_time_ms": 1250,
  "confidence": "high",
  "chart_type": "bar"
}
```

---

## Anti-Hallucination Safeguards

1. **COUNTS_GUARD** -- expected database counts injected into every SQL prompt so the model never generates wildly wrong numbers
2. **60+ few-shot examples** -- covers all common query patterns with correct SQL
3. **SQL validation** -- only SELECT/WITH allowed, no DDL/DML, balanced parens, no multi-statement
4. **NL answer grounding** -- "use ONLY numbers from the data rows, NEVER invent"
5. **RAG grounding** -- "answer ONLY from provided context, say 'not available' otherwise"
6. **PII protection** -- prompt forbids selecting aadhaar, phone, address, account_number

---

## Multi-Turn Conversation

The system maintains conversation context per browser session:

| Turn | User Says | System Understands |
|------|-----------|-------------------|
| 1 | "How many active beneficiaries?" | Direct question -> SQL |
| 2 | "What about inactive?" | Resolves to: "How many inactive beneficiaries?" |
| 3 | "Sum of both?" | Resolves to: "Combined total of active and inactive" |
| 4 | "Which district has more?" | Resolves to: "Which district has more beneficiaries?" |

Context is stored in:
- **L1 cache** (in-memory) -- instant reads within same process
- **L2 Neon** (persistent) -- survives server restarts, shared across instances

---

## Edge Case Handling (Zero API Cost)

| Input | Type | Response |
|-------|------|----------|
| "Hi" / "Namaste" | Greeting | Welcome + capabilities list |
| "Who are you?" | Identity | DSSY assistant description |
| "Tell me a joke" | Silly | Polite redirect to DSSY queries |
| "Cricket score?" | Off-topic | Redirect with example DSSY questions |
| "Thanks" / "Bye" | Thanks/Goodbye | Polite acknowledgment |
| "Help" / "?" | Confused | Query examples and guidance |
| Profanity | Profanity | Calm redirect |

---

## Supported Languages

| Code | Language |
|------|----------|
| `en` | English |
| `hi` | Hindi |
| `te` | Telugu |
| `kn` | Kannada |
| `mr` | Marathi |
| `kok` | Konkani |

---

## AI Service Architecture

```
gemini_service.py          (domain logic: classify, generate SQL, NL answer)
       |
  ai_service.py            (transport layer)
       |-- Retry with exponential backoff (3 attempts)
       |-- Circuit breaker (CLOSED -> OPEN after 5 failures, recovers after 60s)
       |-- In-flight deduplication (same prompt = shared response)
       |-- Token budget guard (rejects prompts > 120k chars)
       |-- Structured logging (latency, status, attempt count)
       |
  httpx --> Gemini API
```

---

## Key Design Decisions

| Decision | Why |
|----------|-----|
| `payment_summary` table instead of querying raw payments | Raw `payments` only has 6 months; summary has 6 years pre-aggregated. Prevents "no data" for YoY queries |
| Synthetic direct-answer RAG chunks | Guarantees retrieval of critical facts (eligibility, amounts, documents) that section-chunking might miss |
| Hybrid search (vector 60% + keyword 40%) with RRF merge | Vector alone misses exact terms ("Rs.2,500"); keyword alone misses semantic similarity |
| Question resolver before intent classification | "What about inactive?" must become a standalone question before routing, otherwise intent classifier can't work |
| COUNTS_GUARD in every SQL prompt | Without expected counts, the model sometimes generates LIMIT 7000 or confuses category codes |
| Circuit breaker in ai_service.py | Prevents 300 pending requests when Gemini is down; fails fast after 5 consecutive errors |
| Title Case status values | PostgreSQL is case-sensitive; ensures consistency between schema, seed data, prompts, and few-shots |

---

## Deployment

### Render / Railway / Fly.io

```bash
# Start command
python -m backend.main
```

Environment variables to set:
- `NEON_DATABASE_URL`
- `GEMINI_API_KEY`
- `ENVIRONMENT=production`
- `SECRET_KEY` (random string)

### Docker (optional)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["python", "-m", "backend.main"]
```

---

## License

Proprietary -- Bharath Light House Software Solutions Private Limited
