# Neural AI Governance v3.0 — DSSY Analytics

## Architecture

```
Officer Question
       │
       ▼
┌─────────────────────┐
│  Edge Case Handler  │ ← Greetings, identity, silly, off-topic (FREE, no API)
│  (regex patterns)   │
└────────┬────────────┘
         │ Not edge case
         ▼
┌─────────────────────┐
│  Gemini Classifier  │ ← Routes: SQL or RAG
│  (intent detection)  │
└───┬─────────┬───────┘
    │         │
    ▼         ▼
┌────────┐ ┌──────────┐
│BigQuery│ │Neon PG   │
│SQL Path│ │RAG Path  │
│(data)  │ │(scheme)  │
└───┬────┘ └────┬─────┘
    │           │
    ▼           ▼
┌─────────────────────┐
│  Unified Response   │ ← Answer + Data + Chart
└─────────────────────┘
```

## Dual Database Setup

| Database | Purpose | Tables |
|----------|---------|--------|
| **BigQuery** | Beneficiary data, analytics, SQL queries | dim_beneficiary, dim_district, dim_taluka, dim_category, fact_table |
| **Neon PostgreSQL** | RAG document chunks only | document_chunks (pgvector) |

## 3-Way Query Routing

1. **EDGE** — Greetings, identity ("who are you"), silly questions, off-topic → Instant canned response, zero API cost
2. **SQL** — Data queries → Gemini generates BigQuery SQL → Execute → NL answer + Chart.js
3. **RAG** — Scheme knowledge → Neon pgvector hybrid search (vector 65% + keyword 35%) → Gemini answer

## Setup

### 1. Environment Variables
```bash
cp .env.example .env
# Edit .env with your credentials
```

### 2. Required credentials:
- `DATABASE_URL` — BigQuery connection string (e.g., `bigquery://project/dataset`)
- `NEON_DATABASE_URL` — Neon PostgreSQL URL (e.g., `postgresql://user:pass@ep-xxx.neon.tech/neondb?sslmode=require`)
- `GEMINI_API_KEY` — Google AI Studio API key
- BigQuery service account JSON in `credentials/`

### 3. Install & Run
```bash
pip install -r backend/requirements.txt
python -m backend.main
```

### 4. First Run
On startup, the app will:
1. Connect to BigQuery (data queries)
2. Connect to Neon PostgreSQL (RAG)
3. Create `document_chunks` table with pgvector extension
4. Ingest `documents/dssy_knowledge_base.md` into vector chunks
5. Start serving on `http://localhost:8000`

## Files Changed (v2 → v3)

| File | Change |
|------|--------|
| `config.py` | Added `NEON_DATABASE_URL`, `NEON_POOL_SIZE` |
| `database.py` | **Rewritten** — dual connections: `execute_bq_query()` + `neon_session_context()` |
| `main.py` | Dual DB startup, both health checks |
| `schemas.py` | Added `edge_type` field |
| `services/edge_handler.py` | **NEW** — regex-based edge case detection |
| `services/gemini_service.py` | BigQuery SQL dialect, expanded examples |
| `services/rag_service.py` | Uses Neon only (not BigQuery) |
| `routers/query.py` | 3-way routing (Edge→SQL→RAG), categorized suggestions |
| `routers/analytics.py` | Date range param, all-talukas, category payout |
| `routers/beneficiaries.py` | Uses `execute_bq_query` |
| `routers/rag.py` | Uses `neon_session_context` |
| `frontend/index.html` | Categorized sidebar, edge badge, dual health status |

## Edge Case Examples

| Input | Response Type |
|-------|--------------|
| "Hi" / "Hello" / "Namaste" | Greeting with capabilities list |
| "Who are you?" / "What can you do?" | Identity + capability description |
| "Tell me a joke" / "Sing a song" | Polite redirect to DSSY queries |
| "What's the weather?" / "Cricket score" | Off-topic notice |
| "Thanks" / "Bye" | Polite acknowledgment |
| "Help" / "I'm confused" | Query examples and guidance |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | BigQuery + Neon + Gemini status |
| `/api/query` | POST | Main query endpoint (auto-routes) |
| `/api/query/suggestions` | GET | Categorized standard queries |
| `/api/analytics/dashboard` | GET | Dashboard KPIs and charts |
| `/api/beneficiaries` | GET | Paginated beneficiary list |
| `/api/rag/status` | GET | RAG ingestion status |
