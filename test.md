# NAG V3 — Comprehensive Test Cases

> **Purpose:** Manual and automated test cases for the DSSY AI Analytics system.
> Covers: Multi-turn Conversation, RAG Engine, SQL Engine, Dynamic Dashboard, AI Query Routing, Edge Handling.
> **Date:** 2026-04-06

---

## Table of Contents

1. [Multi-turn Conversation Tests](#1-multi-turn-conversation-tests)
2. [SQL Engine Tests](#2-sql-engine-tests)
3. [RAG Engine Tests](#3-rag-engine-tests)
4. [Dynamic Dashboard Tests](#4-dynamic-dashboard-tests)
5. [AI Query Routing Tests](#5-ai-query-routing-tests)
6. [Edge Case Handler Tests](#6-edge-case-handler-tests)
7. [Fallback Chain Tests](#7-fallback-chain-tests)
8. [Context Store Tests](#8-context-store-tests)
9. [Security & Validation Tests](#9-security--validation-tests)
10. [Performance & Resilience Tests](#10-performance--resilience-tests)
11. [Proposal Alignment Tests](#11-proposal-alignment-tests)
12. [Web Search Source Removal Tests](#12-web-search-source-removal-tests)
13. [Suggestion Query Routing Tests](#13-suggestion-query-routing-tests)

---

## 1. Multi-turn Conversation Tests

### 1.1 Basic Follow-up Resolution

| # | Turn | User Input | Expected Resolved Question | Expected Intent | Pass? |
|---|------|-----------|---------------------------|-----------------|-------|
| 1 | T1 | "How many active beneficiaries are there?" | (same — standalone) | SQL | |
| 2 | T2 | "what about inactive?" | "How many inactive beneficiaries are there?" | SQL | |
| 3 | T3 | "and deceased?" | "How many deceased beneficiaries are there?" | SQL | |
| 4 | T4 | "sum of all three?" | "What is the combined total of active, inactive, and deceased beneficiaries?" | SQL | |

### 1.2 Arithmetic Follow-ups

| # | Turn | User Input | Expected Behavior | Pass? |
|---|------|-----------|-------------------|-------|
| 1 | T1 | "How many active beneficiaries?" | Returns count (e.g., ~282,000) | |
| 2 | T2 | "How many inactive?" | Returns count (e.g., ~12,600) | |
| 3 | T3 | "add both" | Should compute sum of active + inactive from prior sql_data, answer ~294,600 | |
| 4 | T4 | "what percentage is inactive out of total?" | Should reference prior data to compute percentage | |

### 1.3 Filter Carry-Forward

| # | Turn | User Input | Expected Resolved Question | Pass? |
|---|------|-----------|---------------------------|-------|
| 1 | T1 | "Show taluka-wise beneficiaries in North Goa" | (standalone) | |
| 2 | T2 | "what about South Goa?" | "Show taluka-wise beneficiaries in South Goa" | |
| 3 | T3 | "which taluka is highest?" | "Which taluka has the highest beneficiaries in South Goa?" (carries South Goa filter) | |

### 1.4 Category Context Switch

| # | Turn | User Input | Expected Behavior | Pass? |
|---|------|-----------|-------------------|-------|
| 1 | T1 | "How many widow beneficiaries?" | SQL — returns widow count | |
| 2 | T2 | "same for senior citizens" | Resolves to "How many senior citizen beneficiaries are there?" | |
| 3 | T3 | "compare both" | Should reference widow and senior citizen counts from prior turns | |

### 1.5 Cross-Intent Follow-up (SQL then RAG)

| # | Turn | User Input | Expected Behavior | Pass? |
|---|------|-----------|-------------------|-------|
| 1 | T1 | "How many widow beneficiaries are there?" | SQL — returns count | |
| 2 | T2 | "how much pension do they receive?" | RAG — should resolve "they" to "widows", answer Rs. 2,500/month | |
| 3 | T3 | "what documents do they need to apply?" | RAG — should resolve "they" to "widows", list required documents | |

### 1.6 EDGE Turn Does NOT Pollute Context

| # | Turn | User Input | Expected Behavior | Pass? |
|---|------|-----------|-------------------|-------|
| 1 | T1 | "How many active beneficiaries?" | SQL — returns count | |
| 2 | T2 | "thank you" | EDGE — thanks response | |
| 3 | T3 | "what about inactive?" | Should resolve using T1 context (NOT T2 greeting), returns inactive count | |

### 1.7 Pronoun Resolution

| # | Turn | User Input | Expected Resolved Question | Pass? |
|---|------|-----------|---------------------------|-------|
| 1 | T1 | "Show category-wise breakdown" | (standalone) | |
| 2 | T2 | "which one is the lowest?" | "Which category has the lowest number of beneficiaries?" | |
| 3 | T3 | "show it by district" | "Show [lowest category] beneficiaries by district" | |

### 1.8 Long Conversation (5-Turn Window)

| # | Turn | User Input | Expected Behavior | Pass? |
|---|------|-----------|-------------------|-------|
| 1 | T1 | "Active beneficiaries count" | SQL | |
| 2 | T2 | "Inactive count" | SQL | |
| 3 | T3 | "Deceased count" | SQL | |
| 4 | T4 | "District-wise breakdown" | SQL | |
| 5 | T5 | "Category-wise breakdown" | SQL | |
| 6 | T6 | "go back to the first question — what was active count?" | Should recall T1 result from context (T1 is at edge of 5-turn window) | |
| 7 | T7 | "what was the active count?" | T1 may have been evicted (only 5 turns kept). Verify graceful handling — should re-query, not error | |

### 1.9 Session Isolation

| # | Step | Action | Expected Behavior | Pass? |
|---|------|--------|-------------------|-------|
| 1 | S1 | Session A: "How many active beneficiaries?" | Returns count | |
| 2 | S2 | Session B: "what about inactive?" | Should NOT resolve using Session A context — treated as standalone | |
| 3 | S3 | Session A: "what about inactive?" | SHOULD resolve using Session A context | |

### 1.10 Ambiguous Follow-up

| # | Turn | User Input | Expected Behavior | Pass? |
|---|------|-----------|-------------------|-------|
| 1 | T1 | "Show district-wise and category-wise breakdown" | SQL — multi-dimension query | |
| 2 | T2 | "which is highest?" | Ambiguous — should pick a reasonable interpretation (likely district or category with highest count) | |

### 1.11 Meta-Conversation Questions

| # | Turn | User Input | Expected Behavior | Pass? |
|---|------|-----------|-------------------|-------|
| 1 | T1 | "How many active beneficiaries?" | SQL — returns count | |
| 2 | T2 | "What did I just ask?" | Should pass through edge handler (meta-conversation passthrough) and reach resolve_question | |
| 3 | T3 | "Summarize our conversation" | Should pass through edge handler and summarize prior turns | |

---

## 2. SQL Engine Tests

### 2.1 Basic Count Queries

| # | Question | Expected SQL Pattern | Expected Result Shape | Pass? |
|---|----------|---------------------|-----------------------|-------|
| 1 | "How many total beneficiaries?" | `SELECT COUNT(*) ... FROM beneficiaries` | Single row, ~300,000 | |
| 2 | "How many active beneficiaries?" | `WHERE status='Active'` | ~282,000 | |
| 3 | "How many inactive beneficiaries?" | `WHERE status='Inactive'` | ~12,600 | |
| 4 | "How many deceased beneficiaries?" | `WHERE status='Deceased'` | ~6,000 | |

### 2.2 Category Queries

| # | Question | Expected Behavior | Pass? |
|---|----------|-------------------|-------|
| 1 | "Category-wise beneficiary breakdown" | GROUP BY category_name, 7 categories returned | |
| 2 | "How many widow beneficiaries?" | JOIN categories, filter 'Widow', ~70,500 | |
| 3 | "Senior citizen count" | Filter 'Senior Citizen', ~163,560 | |
| 4 | "Disabled 90% count" | Filter LIKE '%disabled%90%', ~4,230 | |
| 5 | "Which category has the lowest?" | ORDER BY count ASC LIMIT 1, answer: Disabled 90% | |
| 6 | "Which category has the highest?" | ORDER BY count DESC LIMIT 1, answer: Senior Citizen | |
| 7 | "HIV AIDS beneficiaries count" | Filter 'HIV/AIDS', ~8,460 | |

### 2.3 Geography Queries

| # | Question | Expected Behavior | Pass? |
|---|----------|-------------------|-------|
| 1 | "District-wise active beneficiaries" | 2 rows: North Goa (~47%), South Goa (~53%) | |
| 2 | "Compare North Goa vs South Goa" | 2 rows with counts | |
| 3 | "Taluka-wise active count" | ~12 talukas with counts | |
| 4 | "Top 5 talukas by active beneficiaries" | LIMIT 5, ORDER BY DESC | |
| 5 | "Taluka-wise in North Goa" | district_id = 1 filter | |
| 6 | "Village-wise beneficiaries" | JOIN villages, LIMIT 20 | |

### 2.4 Payment & Financial Queries

| # | Question | Expected SQL Table | Expected Behavior | Pass? |
|---|----------|--------------------|-------------------|-------|
| 1 | "Compare payments last 3 years" | payment_summary (NOT payments) | 3 rows with year, paid_count, failed_count, total_paid, success_rate_pct | |
| 2 | "Monthly payment trend for 2024" | payment_summary | 12 rows, grouped by month | |
| 3 | "Total monthly payout" | beneficiaries JOIN categories | SUM(current_monthly_amount) for active | |
| 4 | "Payment status summary" | payments | 3 rows: Paid, Pending, Failed | |
| 5 | "Which batch had the most failures?" | payment_batches | ORDER BY failed_count DESC LIMIT 5 | |
| 6 | "Total disbursed per fiscal year" | payment_batches | GROUP BY fiscal_year_label | |
| 7 | "Payment batch for April 2025" | payment_batches | WHERE payment_year=2025 AND payment_month=4 | |

### 2.5 Life Certificate Queries

| # | Question | Expected Behavior | Pass? |
|---|----------|-------------------|-------|
| 1 | "Life certificate compliance rate by taluka" | JOIN life_certificates, GROUP BY taluka | |
| 2 | "How many have not submitted life certificate?" | NOT EXISTS subquery on life_certificates | |
| 3 | "Suspended payments count" | WHERE payment_suspended = TRUE | |
| 4 | "Late submissions by category" | WHERE is_late_submission = TRUE | |
| 5 | "Year-wise life certificate submissions" | GROUP BY due_year | |

### 2.6 Cross-Dimension Queries

| # | Question | Expected Behavior | Pass? |
|---|----------|-------------------|-------|
| 1 | "District and category cross breakdown" | GROUP BY district_name, category_name | |
| 2 | "Category-wise male vs female" | GROUP BY category, gender | |
| 3 | "Taluka breakdown with category in North Goa" | 3-way JOIN, district_id=1 | |
| 4 | "Deceased by category and district" | GROUP BY 2 dimensions | |

### 2.7 Anti-Hallucination Verification

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | "How many active beneficiaries?" | Answer must say ~282,000 (not hallucinated number) | |
| 2 | "Which category is lowest?" | Must say Disabled 90% (~4,230), NOT any other category | |
| 3 | "Last 3 years total payout" | Must use payment_summary table, NOT payments table | |
| 4 | "Total monthly payout" | Must return Rs. 65-75 crore range, NOT hallucinated amount | |
| 5 | Verify no LIMIT on COUNT(*) queries | COUNT(*) queries must NOT have LIMIT clause | |

### 2.8 Status Value Case Sensitivity

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | SQL uses `status='Active'` | Title Case, not 'active' or 'ACTIVE' | |
| 2 | SQL uses `status='Inactive'` | Title Case | |
| 3 | SQL uses `status='Deceased'` | Title Case | |
| 4 | Payment SQL uses `status='Paid'` | Title Case | |
| 5 | Payment SQL uses `status='Failed'` | Title Case | |

---

## 3. RAG Engine Tests

### 3.1 Eligibility & Rules Questions

| # | Question | Expected Source | Key Facts in Answer | Pass? |
|---|----------|----------------|---------------------|-------|
| 1 | "Who is eligible for DSSY?" | RAG — synthetic chunk | Senior citizens 60+, single women 18+, disabled, HIV/AIDS, 15-year residency | |
| 2 | "What documents are required to apply?" | RAG — synthetic chunk | Birth cert, income cert, residence cert, medical cert (disabled), Aadhaar, election card, Rs.200 fee | |
| 3 | "How much pension do widows receive?" | RAG — synthetic chunk | Rs. 2,500/month (post-2021 amendment) | |
| 4 | "What is the Life Certificate requirement?" | RAG — synthetic chunk | Annual in April/May, bank manager or gazetted officer, failure = stopped | |
| 5 | "What are the cancellation rules?" | RAG — synthetic chunk | Begging or income exceeds ceiling | |
| 6 | "When was DSSY launched?" | RAG — synthetic chunk | 2nd October 2001, effective 1st January 2002 | |

### 3.2 Comparison & Difference Questions

| # | Question | Expected Answer Contains | Pass? |
|---|----------|--------------------------|-------|
| 1 | "What is the difference between DSSY and DDSSY?" | DSSY = pension, DDSSY = health insurance, separate schemes | |
| 2 | "What is Griha Aadhar and how is it related to DSSY?" | Separate scheme, Rs. 1,500 to homemakers, cannot receive both | |
| 3 | "Can both husband and wife receive DSSY?" | No — only one at a time | |

### 3.3 Process & Procedure Questions

| # | Question | Expected Answer Format | Pass? |
|---|----------|------------------------|-------|
| 1 | "How to apply for DSSY?" | Numbered steps (1-7) | |
| 2 | "Who approves DSSY applications?" | Committee headed by Chief Minister | |
| 3 | "How is payment made?" | ECS, bank account, monthly | |

### 3.4 Historical & Amendment Questions

| # | Question | Expected Answer Contains | Pass? |
|---|----------|--------------------------|-------|
| 1 | "What did the CAG audit find?" | 2008 audit, beneficiary growth 5,720 to 97,282 | |
| 2 | "Which schemes were amalgamated into DSSY?" | Old Age Pension, Widow Pension, Disability Pension | |
| 3 | "What are the DSSY amendment changes in 2021?" | Widow pension increased from Rs. 2,000 to Rs. 2,500 | |
| 4 | "What is the notification number?" | 50/354/02-03/HC/Part-I/4247 | |

### 3.5 Specific Category Questions

| # | Question | Expected Answer | Pass? |
|---|----------|-----------------|-------|
| 1 | "Can a divorced woman apply for DSSY?" | Yes — requires Divorce Decree from court | |
| 2 | "What is the income limit?" | Annual family per capita income less than annual assistance amount | |
| 3 | "Can a disabled person continue after marriage?" | Should answer from RAG knowledge | |
| 4 | "What is the residency requirement?" | 15 years domicile or born in Goa | |
| 5 | "How much can disabled claim for aids?" | Up to Rs. 1,00,000 once in five years | |

### 3.6 RAG Confidence & Fallback

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | Question with high RAG match (similarity > 0.60) | confidence: "high" | |
| 2 | Question with medium RAG match (0.22-0.60) | confidence: "medium" | |
| 3 | Question with very low RAG match (< 0.18) | Falls back to web search | |
| 4 | RAG returns "not available" + low similarity | Falls back to web search | |

### 3.7 Synonym Expansion

| # | Input Term | Should Expand To Include | Pass? |
|---|-----------|--------------------------|-------|
| 1 | "cancel" | stop, discontinue, terminate, cancellation | |
| 2 | "eligible" | qualify, entitled, criteria, conditions | |
| 3 | "document" | certificate, proof, paperwork, required | |
| 4 | "launch" | started, began, 2001, history, established | |
| 5 | "life certificate" | annual certificate, april may, bank manager | |

---

## 4. Dynamic Dashboard Tests

### 4.1 Static Dashboard KPIs

| # | KPI Card | Expected Data | Pass? |
|---|----------|---------------|-------|
| 1 | Total Beneficiaries | ~300,000 | |
| 2 | Active Beneficiaries | ~282,000 | |
| 3 | Inactive Beneficiaries | ~12,600 | |
| 4 | Deceased Beneficiaries | ~6,000 | |
| 5 | Monthly Payout | Rs. 65-75 crore | |
| 6 | Payment Compliance % | Paid/(Paid+Pending+Failed) | |

### 4.2 Static Dashboard Charts

| # | Chart | Type | Expected Data | Pass? |
|---|-------|------|---------------|-------|
| 1 | Category Distribution | Pie | 7 categories | |
| 2 | Gender Split | Doughnut | Male, Female, Other | |
| 3 | District Comparison | Bar | North Goa, South Goa | |
| 4 | Registration Trend | Line | Monthly time series | |
| 5 | Age Distribution | Horizontal Bar | Under 40, 40-59, 60-69, 70-79, 80+ | |
| 6 | Top 5 Talukas | Table | 5 rows with progress bars | |

### 4.3 Date Range Filtering

| # | Range | Expected Behavior | Pass? |
|---|-------|-------------------|-------|
| 1 | "all" | No date filter, full dataset | |
| 2 | "7d" | registration_date >= CURRENT_DATE - 7 days | |
| 3 | "30d" | registration_date >= CURRENT_DATE - 30 days | |
| 4 | "90d" | registration_date >= CURRENT_DATE - 90 days | |
| 5 | "6m" | registration_date >= CURRENT_DATE - 6 months | |
| 6 | "1y" | registration_date >= CURRENT_DATE - 1 year | |
| 7 | "custom" with valid dates | BETWEEN date_from AND date_to | |
| 8 | "custom" with invalid dates | Should not crash, fallback to no filter | |

### 4.4 Dynamic Dashboard NLP Chips (Non-Overlap Verification)

Each chip should produce a **distinct** query and result set. Verify no two chips return identical data:

| # | Chip Label | NLP Query | Distinct Dimension | Pass? |
|---|-----------|-----------|-------------------|-------|
| 1 | Category breakdown | "Category-wise beneficiary breakdown" | category x count | |
| 2 | District comparison | "District-wise active beneficiaries" | district x count | |
| 3 | North Goa talukas | "Taluka-wise active beneficiaries in North Goa" | taluka x count (North only) | |
| 4 | South Goa talukas | "Taluka-wise active beneficiaries in South Goa" | taluka x count (South only) | |
| 5 | Age distribution | "Age group distribution of active beneficiaries" | age_group x count | |
| 6 | Gender split | "Gender breakdown of active beneficiaries" | gender x count | |
| 7 | Payment trend 3yr | "Compare payments 2023 vs 2024 vs 2025 with paid failed pending count and total amount" | year x multiple metrics | |
| 8 | Monthly payments 2024 | "Monthly payment trend for 2024" | month x payment amount | |
| 9 | Lowest category | "Which category has the lowest number of beneficiaries" | single row result | |
| 10 | Inactive by category | "Inactive beneficiaries by category" | category x inactive_count | |
| 11 | Registration trend | "Year wise registration trend" | year x registrations | |
| 12 | Active vs Inactive vs Deceased | "Total active vs inactive vs deceased beneficiaries" | 3 status rows | |
| 13 | Senior citizens 80+ | "Senior citizen beneficiaries above age 80" | single count | |
| 14 | Widows by taluka | "Widow beneficiaries by taluka" | taluka x widow_count | |
| 15 | New registrations (6m) | "New beneficiaries registered in the last 6 months by category" | category x recent_count | |
| 16 | Avg age by category | "Category-wise average age of beneficiaries" | category x avg_age | |
| 17 | Deceased by category & district | "Deceased beneficiaries count by category and district" | category x district x count | |
| 18 | Male vs Female by category | "Category-wise male vs female count" | category x gender x count | |
| 19 | Total FY disbursement | "Total disbursement amount this fiscal year" | single amount | |
| 20 | Category-wise payout | "Category-wise monthly payout amount" | category x payout | |
| 21 | District-wise disbursement | "District-wise total payment disbursement last year" | district x payout | |
| 22 | Monthly failure rate | "Payment failure rate by month for last 12 months" | month x failure_rate | |
| 23 | Pending payments | "Pending payments count and amount this month" | single count + amount | |
| 24 | Avg payment per beneficiary | "Average payment amount per beneficiary by category" | category x avg_amount | |
| 25 | YoY payment growth | "Year-on-year payment growth percentage" | year x growth_pct | |
| 26 | Failed payments by district | "Failed payments by district" | district x failed_count | |
| 27 | Batch summary (6m) | "Payment batch summary last 6 months" | batch x stats | |
| 28 | Life cert compliance | "Life certificate compliance rate by year" | year x compliance_pct | |
| 29 | Late submissions by taluka | "Late life certificate submissions by taluka" | taluka x late_count | |
| 30 | North vs South Goa | "Compare North Goa vs South Goa beneficiary count and payout" | district x count + payout | |
| 31 | Taluka failure rates | "Taluka-wise payment failure rate" | taluka x failure_rate | |
| 32 | District x Category | "District-wise category breakdown" | district x category x count | |

### 4.5 Overlap Matrix — Chips That Could Seem Similar

Verify these pairs return **different** result sets:

| # | Chip A | Chip B | Why They're Different | Pass? |
|---|--------|--------|----------------------|-------|
| 1 | Category breakdown (count) | Category-wise payout (Rs.) | Different metric: count vs payout amount | |
| 2 | District comparison (count) | District-wise disbursement (Rs.) | Different metric: beneficiary count vs payment amount | |
| 3 | District comparison | North vs South Goa | Same dimension but NvS includes payout column too | |
| 4 | Payment trend 3yr | YoY payment growth | 3yr = absolute numbers, YoY = growth percentage | |
| 5 | Payment trend 3yr | Monthly payments 2024 | 3yr = yearly granularity, Monthly = month granularity | |
| 6 | Inactive by category | Category breakdown | Different status filter: Inactive vs Active | |
| 7 | North Goa talukas | South Goa talukas | Different district_id filter (1 vs 2) | |
| 8 | Life cert compliance (by year) | Late submissions by taluka | Different dimension: year vs taluka, different metric | |
| 9 | Failed payments by district | Taluka failure rates | Different geography granularity: district vs taluka | |
| 10 | Batch summary (6m) | Monthly failure rate | Different source: payment_batches vs payment_summary, different metric | |

### 4.6 Dynamic Chart Type Auto-Detection

| # | Query | Expected Chart Type | Reason | Pass? |
|---|-------|--------------------|---------| ------|
| 1 | "Year wise registration trend" | line | Time-series (has "year" in label column) | |
| 2 | "Gender breakdown" | doughnut | Few rows (2-3) | |
| 3 | "District-wise active" | doughnut | 2 rows (North/South) | |
| 4 | "Taluka-wise active" | bar | Many rows (12+) | |
| 5 | "Category-wise breakdown" | bar | 7 rows (>6 threshold) | |
| 6 | "Monthly payments 2024" | line | Has "month" in label | |
| 7 | Single aggregate (e.g., total count) | null (no chart) | Only 1 row | |

### 4.7 Chart Toggle (Bar / Donut / Line)

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | Click "Bar" on a doughnut chart | Chart re-renders as bar | |
| 2 | Click "Donut" on a bar chart | Chart re-renders as doughnut | |
| 3 | Click "Line" on a bar chart | Chart re-renders as line | |
| 4 | Toggle preserves data | Same labels and values after toggle | |

---

## 5. AI Query Routing Tests

### 5.1 Intent Classification

| # | Question | Expected Intent | Pass? |
|---|----------|-----------------|-------|
| 1 | "How many active beneficiaries?" | SQL | |
| 2 | "What documents are needed to apply?" | RAG | |
| 3 | "District-wise breakdown" | SQL | |
| 4 | "Who is eligible for DSSY?" | RAG | |
| 5 | "Total widow beneficiaries in North Goa" | SQL | |
| 6 | "How much pension do disabled persons get?" | RAG | |
| 7 | "Show top 5 talukas" | SQL | |
| 8 | "What is the income limit for DSSY?" | RAG | |
| 9 | "Life certificate compliance by taluka" | SQL | |
| 10 | "What is the Life Certificate requirement?" | RAG | |
| 11 | "Payment batch for April 2025" | SQL | |
| 12 | "How to apply for DSSY?" | RAG | |
| 13 | "Dynamic dashboard for North Goa" | SQL | |
| 14 | "What are the cancellation rules?" | RAG | |
| 15 | "When did pension amount change for senior citizens?" | SQL | |

### 5.2 Routing Pipeline Order

| # | Test | Expected Flow | Pass? |
|---|------|---------------|-------|
| 1 | "Hello" | EDGE (greeting) — no API call | |
| 2 | "How many active?" | EDGE skip → resolve → classify(SQL) → generate_sql → execute → NL answer | |
| 3 | "What is DSSY eligibility?" | EDGE skip → resolve → classify(RAG) → rag_search → rag_answer | |
| 4 | Question with CANNOT_ANSWER SQL | SQL → CANNOT_ANSWER → fallback to RAG → fallback to Web | |

---

## 6. Edge Case Handler Tests

### 6.1 Greeting Detection

| # | Input | Expected Edge Type | Pass? |
|---|-------|--------------------|-------|
| 1 | "hi" | greeting | |
| 2 | "Hello!" | greeting | |
| 3 | "Good morning" | greeting | |
| 4 | "namaste" | greeting | |
| 5 | "hey" | greeting | |
| 6 | "Hi, how many beneficiaries?" | NOT edge (has DSSY keyword "beneficiaries") | |

### 6.2 Identity Questions

| # | Input | Expected Edge Type | Pass? |
|---|-------|--------------------|-------|
| 1 | "Who are you?" | identity | |
| 2 | "What can you do?" | identity | |
| 3 | "Are you AI or human?" | identity | |
| 4 | "Which AI model are you?" | identity | |

### 6.3 Thanks & Goodbye

| # | Input | Expected Edge Type | Pass? |
|---|-------|--------------------|-------|
| 1 | "Thank you" | thanks | |
| 2 | "Thanks!" | thanks | |
| 3 | "Bye" | goodbye | |
| 4 | "Have a good day" | goodbye | |

### 6.4 Silly / Off-Topic Blocking

| # | Input | Expected Edge Type | Pass? |
|---|-------|--------------------|-------|
| 1 | "Tell me a joke" | silly | |
| 2 | "What is the capital of France?" | silly | |
| 3 | "IPL score today" | off_topic | |
| 4 | "Best restaurants in Goa" | off_topic | |
| 5 | "Stock market prediction" | off_topic | |

### 6.5 DSSY Strong Signal Passthrough

| # | Input | Expected Behavior | Pass? |
|---|-------|-------------------|-------|
| 1 | "active beneficiaries" | NOT edge — has DSSY keyword | |
| 2 | "dashboard for taluka" | NOT edge — has "dashboard" and "taluka" | |
| 3 | "payment batch status" | NOT edge — has "payment" and "batch" | |
| 4 | "life certificate compliance" | NOT edge — has "life cert" and "compliance" | |
| 5 | "show enrollment trend" | NOT edge — has "enrollment" | |

### 6.6 Follow-up Passthrough

| # | Input | Expected Behavior | Pass? |
|---|-------|-------------------|-------|
| 1 | "what about both?" | NOT edge — follow-up passthrough | |
| 2 | "sum of them" | NOT edge — arithmetic follow-up | |
| 3 | "which is highest?" | NOT edge — superlative follow-up | |
| 4 | "add both together" | NOT edge — combine follow-up | |
| 5 | "what about inactive?" | NOT edge — "what about" pattern | |

### 6.7 Confused / Help

| # | Input | Expected Edge Type | Pass? |
|---|-------|--------------------|-------|
| 1 | "?" | confused | |
| 2 | "help" | confused | |
| 3 | "hmm" | confused | |
| 4 | "I don't understand" | confused | |
| 5 | "ok" | confused | |

---

## 7. Fallback Chain Tests

### 7.1 SQL → RAG Fallback

| # | Scenario | Expected Behavior | Pass? |
|---|----------|-------------------|-------|
| 1 | SQL returns CANNOT_ANSWER | Falls back to RAG search | |
| 2 | SQL execution throws error | Falls back to RAG, then Web | |
| 3 | SQL returns 0 rows | Falls back to RAG, then Web | |

### 7.2 RAG → Web Fallback

| # | Scenario | Expected Behavior | Pass? |
|---|----------|-------------------|-------|
| 1 | RAG returns no relevant chunks | Falls back to web search | |
| 2 | RAG answer contains "not available" + low similarity | Falls back to web search | |
| 3 | RAG search throws exception | Falls back to web search | |

### 7.3 Complete Fallback Chain

| # | Scenario | Expected Behavior | Pass? |
|---|----------|-------------------|-------|
| 1 | SQL CANNOT_ANSWER → RAG no match → Web no result | Returns graceful "unable to find" message with contact info | |
| 2 | Intent=RAG → RAG fails → Web fails | Returns "not available in knowledge base or web search" message | |

---

## 8. Context Store Tests

### 8.1 L1 Cache Operations

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | First query for new session | Creates L1 entry, schedules Neon write | |
| 2 | Second query same session | L1 cache hit, instant context return | |
| 3 | 6th turn added to session | Oldest turn evicted (MAX_TURNS=5) | |
| 4 | 501st session created | Oldest session evicted from L1 (MAX_SESSIONS=500) | |

### 8.2 L2 Neon Persistence

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | Server restart, then query with existing session_id | L1 miss → Neon read → context restored | |
| 2 | Neon read takes >2 seconds | Timeout → returns empty context (graceful degradation) | |
| 3 | Neon write fails | L1 still valid, warning logged, no query failure | |

### 8.3 Turn Data Integrity

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | SQL turn stores sql_data | sql_data contains result rows (up to 50) | |
| 2 | RAG turn has no sql_data | sql_data is None | |
| 3 | EDGE turn stored | Stored but filtered from analytical context | |
| 4 | Answer truncated at 800 chars | Long answers truncated with "..." | |

### 8.4 TTL & Cleanup

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | Query context after 30 minutes | Old turns excluded (created_at > NOW() - 30 min) | |
| 2 | Probabilistic global purge | 1-in-20 writes trigger cleanup of expired rows | |

---

## 9. Security & Validation Tests

### 9.1 SQL Injection Prevention

| # | Input | Expected Behavior | Pass? |
|---|-------|-------------------|-------|
| 1 | "'; DROP TABLE beneficiaries; --" | SQL validation rejects (forbidden keyword DROP) | |
| 2 | "1; DELETE FROM payments" | SQL validation rejects (forbidden DELETE) | |
| 3 | "SELECT * FROM beneficiaries; INSERT INTO..." | Multiple statements rejected (semicolon check) | |
| 4 | "UPDATE beneficiaries SET status='Active'" | Forbidden keyword UPDATE | |

### 9.2 SQL Validation Rules

| # | Test | Expected Result | Pass? |
|---|------|-----------------|-------|
| 1 | Valid SELECT statement | (True, "OK") | |
| 2 | Valid WITH/CTE statement | (True, "OK") | |
| 3 | Statement starting with INSERT | (False, "Only SELECT/WITH allowed") | |
| 4 | Unbalanced parentheses | (False, "Unbalanced parentheses") | |
| 5 | GRANT or REVOKE | (False, "Forbidden keyword") | |
| 6 | EXEC or EXECUTE | (False, "Forbidden keyword") | |

### 9.3 PII Protection

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | "Show all beneficiary aadhaar numbers" | SQL should NOT select aadhaar_number | |
| 2 | "List phone numbers of beneficiaries" | SQL should NOT select phone_number | |
| 3 | "Show beneficiary addresses" | SQL should NOT select address | |
| 4 | "Show account numbers" | SQL should NOT select account_number | |

### 9.4 Input Validation

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | Empty question ("") | Rejected by Pydantic (min_length=2) | |
| 2 | Single character "a" | Rejected by Pydantic (min_length=2) | |
| 3 | Question >500 chars | Rejected by Pydantic (max_length=500) | |
| 4 | Valid 2-char question "hi" | Accepted, handled as edge case | |

### 9.5 Audio Transcription Security

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | Audio file >5MB | Rejected with 413 error | |
| 2 | Invalid MIME type | Falls back to "audio/webm" | |
| 3 | Valid audio file | Transcription returned | |

---

## 10. Performance & Resilience Tests

### 10.1 Response Time Targets

| # | Operation | Target | Pass? |
|---|-----------|--------|-------|
| 1 | Edge case detection | < 5ms (no API call) | |
| 2 | Simple SQL count query | < 3000ms end-to-end | |
| 3 | RAG search + answer | < 5000ms end-to-end | |
| 4 | Dashboard KPI load | < 3000ms (parallel queries) | |
| 5 | Cache hit (same question) | < 500ms | |

### 10.2 Circuit Breaker (ai_service.py)

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | Gemini API returns 500 error | Retry with exponential backoff (3 attempts) | |
| 2 | 5 consecutive failures | Circuit breaker opens, fast-fail subsequent calls | |
| 3 | Circuit breaker half-open | Allows 1 test request through | |
| 4 | Test request succeeds | Circuit breaker closes, normal operation resumes | |

### 10.3 Caching

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | Same resolved question asked twice | Second request served from Redis cache | |
| 2 | Different question, same resolved form | Cache hit (e.g., "what about inactive?" resolved to same as "How many inactive?") | |
| 3 | Redis unavailable | Graceful degradation, queries still work without cache | |

### 10.4 Concurrent Requests

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | 10 simultaneous /api/query requests | All return valid responses, no deadlocks | |
| 2 | Dashboard + query simultaneously | Both return correctly | |
| 3 | Same question from 2 sessions | In-flight deduplication in ai_service.py | |

### 10.5 Neon Cold Start

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | First query after Neon sleep | Connection established, query succeeds (may be slower) | |
| 2 | keep_warm.py running | Prevents cold starts | |

---

## Multi-turn Conversation Stress Scenarios

### Scenario A: Full Data Exploration Flow

```
T1: "How many total beneficiaries are there?"
    → SQL, ~300,000
T2: "break it down by status"
    → Resolves to "Show beneficiary count by status (Active, Inactive, Deceased)"
T3: "now show active ones by category"
    → Resolves to "Show active beneficiary count by category"
T4: "which category is lowest?"
    → Resolves to "Which category has the lowest active beneficiary count?"
    → Answer: Disabled 90% (~4,230)
T5: "show that category by district"
    → Resolves to "Show Disabled 90% beneficiaries by district"
```

### Scenario B: Payment Analysis Flow

```
T1: "Compare payments last 3 years"
    → SQL using payment_summary, 3 rows
T2: "which year had the most failures?"
    → Resolves to "Which year had the most payment failures in the last 3 years?"
T3: "show that year's monthly breakdown"
    → Resolves to "Show monthly payment breakdown for [year with most failures]"
T4: "what about the pending ones?"
    → Resolves to "Show monthly pending payment count for [same year]"
```

### Scenario C: Mixed SQL + RAG Flow

```
T1: "How many widow beneficiaries are there?"
    → SQL, ~70,500
T2: "how much do they receive per month?"
    → RAG, Rs. 2,500/month
T3: "what documents do they need?"
    → RAG, lists documents for widows
T4: "now show widows by taluka"
    → SQL, taluka-wise widow count
T5: "which taluka has the most?"
    → Resolves to "Which taluka has the most widow beneficiaries?"
```

### Scenario D: Greeting Interruption Flow

```
T1: "How many active beneficiaries?"
    → SQL, ~282,000
T2: "thanks"
    → EDGE (thanks)
T3: "what about inactive?"
    → Should correctly resolve from T1 (EDGE T2 filtered out)
T4: "hello"
    → EDGE (greeting)
T5: "sum of active and inactive?"
    → Should correctly reference T1 and T3 data
```

### Scenario E: District Deep-Dive Flow

```
T1: "Compare North Goa vs South Goa"
    → SQL, 2 rows
T2: "show talukas in North Goa"
    → Resolves to "Show taluka-wise active beneficiaries in North Goa"
T3: "and South Goa?"
    → Resolves to "Show taluka-wise active beneficiaries in South Goa"
T4: "which taluka overall has the most beneficiaries?"
    → Resolves to "Which taluka has the most active beneficiaries across all districts?"
T5: "show that taluka's category breakdown"
    → Resolves to "Show category-wise breakdown for [top taluka]"
```

### Scenario F: Life Certificate Compliance Flow

```
T1: "Life certificate compliance rate by taluka"
    → SQL, taluka-wise compliance data
T2: "which taluka has the lowest?"
    → Resolves to "Which taluka has the lowest life certificate compliance rate?"
T3: "how many are suspended there?"
    → Resolves to "How many beneficiaries have suspended payments in [lowest taluka]?"
T4: "what happens if you don't submit life certificate?"
    → RAG, explains payment suspension
```

### Scenario G: Arithmetic Chain

```
T1: "How many senior citizen beneficiaries?"
    → SQL, ~163,560
T2: "How many widows?"
    → SQL, ~70,500
T3: "How many single women?"
    → SQL, ~16,920
T4: "total of all three?"
    → Should compute: 163,560 + 70,500 + 16,920 = ~250,980
T5: "what percentage is widows out of that total?"
    → Should compute: 70,500 / 250,980 * 100 = ~28.1%
```

---

## How to Execute These Tests

### Manual Testing (via Frontend Chat)

1. Open `frontend/index.html` in browser
2. Use the chat interface to type each question
3. Verify answers match expected behavior
4. Use the same session for multi-turn scenarios
5. Open a new incognito/private window for session isolation tests

### Manual Testing (via API)

```bash
# Single query
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{"question": "How many active beneficiaries?", "session_id": "test-session-1", "include_sql": true}'

# Multi-turn (same session_id)
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{"question": "what about inactive?", "session_id": "test-session-1", "include_sql": true}'

# Dashboard
curl http://localhost:8000/api/analytics/dashboard?range=all

# Dashboard with date filter
curl "http://localhost:8000/api/analytics/dashboard?range=custom&date_from=2024-01-01&date_to=2024-12-31"

# RAG status
curl http://localhost:8000/api/rag/status

# Health check
curl http://localhost:8000/health

# Suggestions
curl http://localhost:8000/api/query/suggestions
```

### Automated Testing (Python)

```python
import httpx, asyncio

BASE = "http://localhost:8000"

async def test_multiturn():
    async with httpx.AsyncClient(timeout=30) as c:
        # T1
        r1 = await c.post(f"{BASE}/api/query", json={
            "question": "How many active beneficiaries?",
            "session_id": "pytest-mt-1", "include_sql": True
        })
        d1 = r1.json()
        assert d1["intent"] == "SQL"
        assert d1["row_count"] >= 1

        # T2 — follow-up
        r2 = await c.post(f"{BASE}/api/query", json={
            "question": "what about inactive?",
            "session_id": "pytest-mt-1", "include_sql": True
        })
        d2 = r2.json()
        assert d2["intent"] == "SQL"
        assert "inactive" in d2["answer"].lower() or "Inactive" in d2["answer"]

        # T3 — arithmetic follow-up
        r3 = await c.post(f"{BASE}/api/query", json={
            "question": "sum of both?",
            "session_id": "pytest-mt-1", "include_sql": True
        })
        d3 = r3.json()
        assert d3["intent"] == "SQL"

        print("Multi-turn test PASSED")

asyncio.run(test_multiturn())
```

---

## 11. Proposal Alignment Tests

These tests verify the system handles all example queries from the Neural AI Governance PoC proposal document.

### 11.1 Proposal Example Queries (Page 4)

| # | Proposal Query | Expected Intent | Expected Behavior | Pass? |
|---|---------------|-----------------|-------------------|-------|
| 1 | "How many DSSY beneficiaries are above the age of 60 in North Goa?" | SQL | Filter age >= 60, district_id=1 | |
| 2 | "What is the district-wise distribution of beneficiaries?" | SQL | GROUP BY district, 2 rows | |
| 3 | "How many beneficiaries were added after 2020 digitization?" | SQL | WHERE registration_date >= '2020-01-01' | |
| 4 | "What is the gender-wise breakdown of DSSY beneficiaries?" | SQL | GROUP BY gender | |
| 5 | "Which talukas show the highest increase in beneficiaries over the past five years?" | SQL | Recent registrations by taluka, ORDER BY DESC | |

### 11.2 Proposal Example Queries (Page 7)

| # | Proposal Query | Expected Intent | Expected Behavior | Pass? |
|---|---------------|-----------------|-------------------|-------|
| 1 | "Show beneficiary distribution by district" | SQL | Same as district-wise count | |
| 2 | "How many new beneficiaries were added after 2022?" | SQL | WHERE registration_date >= '2022-01-01' | |
| 3 | "What is the gender-wise distribution of DSSY beneficiaries?" | SQL | GROUP BY gender | |

### 11.3 Proposal PoC Capabilities

| # | Capability | Test Query | Expected Result | Pass? |
|---|-----------|-----------|-----------------|-------|
| 1 | Statistical analysis | "Category-wise beneficiary distribution" | 7 categories with counts | |
| 2 | Dynamic reporting | Click any NLP chip in dashboard | Chart + table rendered | |
| 3 | Permutation-combination queries | "District and category cross breakdown" | Multi-dimension result | |
| 4 | Automated insights | "Compare North Goa vs South Goa" | NL answer with comparison | |
| 5 | Trend analysis | "Year wise registration trend" | Time-series data + line chart | |
| 6 | Scheme performance | "Payment compliance status summary" | Paid/Pending/Failed counts | |
| 7 | Multi-language | Send query with language: "hi" | Hindi response | |
| 8 | Voice input | Upload audio via /api/query/transcribe | Transcription returned | |

---

## 12. Web Search Source Removal Tests

Verify that web search fallback answers do NOT include source links.

| # | Test | Expected Behavior | Pass? |
|---|------|-------------------|-------|
| 1 | Query that falls to web search | Answer has NO "Sources:" section | |
| 2 | Query that falls to web search | Answer has NO markdown links like [title](url) | |
| 3 | RAG answer from local knowledge base | No source links (was never an issue) | |
| 4 | SQL answer from database | No source links (was never an issue) | |

---

## 13. Suggestion Query Routing Tests

Every query in the `/api/query/suggestions` endpoint must route correctly.

### 13.1 Data Queries (must ALL route to SQL)

| # | Suggestion Query | Expected Intent | Has Few-Shot | Pass? |
|---|-----------------|-----------------|-------------|-------|
| 1 | "How many total beneficiaries are there in DSSY?" | SQL | YES | |
| 2 | "Show taluka-wise active beneficiary count" | SQL | YES | |
| 3 | "Compare North Goa vs South Goa beneficiaries" | SQL | YES | |
| 4 | "What is the gender-wise breakdown of beneficiaries?" | SQL | YES | |
| 5 | "Which taluka has the most Senior Citizen beneficiaries?" | SQL | YES | |
| 6 | "What is the total monthly payout for active beneficiaries?" | SQL | YES | |
| 7 | "Show category-wise beneficiary distribution" | SQL | YES | |
| 8 | "How many beneficiaries are above 80 years old?" | SQL | YES | |
| 9 | "List inactive beneficiaries by district" | SQL | YES | |
| 10 | "Show age group distribution of beneficiaries" | SQL | YES | |
| 11 | "Female beneficiaries count by district" | SQL | YES | |
| 12 | "Payment compliance status summary" | SQL | YES | |
| 13 | "How many widow beneficiaries are there?" | SQL | YES | |
| 14 | "How many deceased beneficiaries are recorded?" | SQL | YES | |

### 13.2 Scheme Information (must ALL route to RAG)

| # | Suggestion Query | Expected Intent | Has RAG Chunk | Pass? |
|---|-----------------|-----------------|--------------|-------|
| 1 | "Who is eligible for DSSY benefits?" | RAG | YES (synthetic) | |
| 2 | "What documents are required to apply for DSSY?" | RAG | YES (synthetic) | |
| 3 | "How much pension do widows receive under DSSY?" | RAG | YES (synthetic) | |
| 4 | "What is the financial assistance for disabled persons?" | RAG | YES (synthetic) | |
| 5 | "What is the Life Certificate requirement?" | RAG | YES (synthetic) | |
| 6 | "When was DSSY launched?" | RAG | YES (synthetic) | |
| 7 | "What is the difference between DSSY and DDSSY?" | RAG | YES (synthetic) | |
| 8 | "Can both husband and wife receive DSSY?" | RAG | YES (synthetic) | |
| 9 | "What are the cancellation rules for DSSY?" | RAG | YES (synthetic) | |
| 10 | "What is the registration fee for DSSY?" | RAG | YES (synthetic) | |
| 11 | "How is DSSY payment made to beneficiaries?" | RAG | YES (synthetic) | |
| 12 | "What happens if Life Certificate is not submitted?" | RAG | YES (synthetic) | |
| 13 | "What are the DSSY amendment changes in 2021?" | RAG | YES (synthetic) | |
| 14 | "What is the residency requirement for DSSY?" | RAG | YES (synthetic) | |
| 15 | "Can a divorced woman apply for DSSY?" | RAG | YES (synthetic) | |
| 16 | "What is the income limit to qualify for DSSY?" | RAG | YES (synthetic) | |
| 17 | "Who approves DSSY applications?" | RAG | YES (synthetic) | |
| 18 | "Can a disabled person continue DSSY after marriage?" | RAG | YES | |
| 19 | "What is the medical assistance for senior citizens?" | RAG | YES | |
| 20 | "How much can a disabled person claim for aids and appliances?" | RAG | YES (synthetic) | |
| 21 | "What happened to DSSY payments before ECS was introduced?" | RAG | YES (synthetic) | |
| 22 | "What did the CAG audit find about DSSY in 2008?" | RAG | YES (synthetic) | |
| 23 | "Which schemes were amalgamated into DSSY?" | RAG | YES (synthetic) | |
| 24 | "What is the Griha Aadhar scheme and how is it related to DSSY?" | RAG | YES (synthetic) | |
| 25 | "Can children receive DSSY if parents are already beneficiaries?" | RAG | YES | |
| 26 | "What is the notification number of DSSY scheme?" | RAG | YES (synthetic) | |
| 27 | "How many widows benefited from the 2021 amendment?" | RAG | YES (synthetic) | |
| 28 | "Where is the Department of Social Welfare located?" | RAG | YES (synthetic) | |
| 29 | "What was the original pension amount when DSSY started?" | RAG | YES (synthetic) | |

### 13.3 Verify NO Sources in RAG/WEB Answers

| # | Query | Expected | Pass? |
|---|-------|----------|-------|
| 1 | "What was the original pension amount when DSSY started?" | RAG answer, NO "Sources:" section | |
| 2 | "Where is the Department of Social Welfare located?" | RAG answer, NO "Sources:" section | |
| 3 | "How many widows benefited from the 2021 amendment?" | RAG answer with "35,145", NO sources | |

---

## Test Results Summary

| Component | Total Tests | Passed | Failed | Notes |
|-----------|------------|--------|--------|-------|
| Multi-turn Conversation | 40+ | | | |
| SQL Engine | 35+ | | | |
| RAG Engine | 25+ | | | |
| Dynamic Dashboard | 50+ | | | |
| AI Query Routing | 20+ | | | |
| Edge Case Handler | 30+ | | | |
| Fallback Chain | 7 | | | |
| Context Store | 10+ | | | |
| Security & Validation | 15+ | | | |
| Performance & Resilience | 12+ | | | |
| Proposal Alignment | 16 | | | |
| Web Search Sources | 4 | | | |
| Suggestion Routing | 46 | | | |
| **TOTAL** | **~300+** | | | |
