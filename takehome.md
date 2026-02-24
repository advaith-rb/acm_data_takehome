# Analytics Engineer Take-Home Challenge

## Customer Transaction Data Platform

**Version:** February 2026

---

## Context

This take-home uses **transaction data** and **sentiment/perception data** to estimate whether a customer will attend a soccer match. You will build a prediction model that takes customer features and match context as inputs and outputs an attendance probability with reasoning.

---

## Timebox

Please spend **~3-4 hours total** across the 24 hours you are given with the assignment. 
Use AI tools freely. We're evaluating your approach to data integration, pipeline design, schema governance, and engineering judgment. While we expect you to generate code using AI tooling (Cursor, Claude Code, etc.), we do expect you to have an understanding of the system implemented and describe design decisions, even if you didn't write the majority of the code. 

If you cut scope, keep the golden path working and explain tradeoffs in the README.

---

## Goal

Build a **data pipeline and platform** that:

1. **Ingests** the three provided datasets with resilience to bad data
2. **Validates and cleans** messy inputs (duplicates, missing values, inconsistent formats, schema violations)
3. **Transforms** raw data into normalized, well-structured data models (fact tables, dimensions, etc.)
4. **Enforces data governance** — schema contracts, row count validations, data quality checks, lineage awareness
5. **Provides queryable outputs** — cleaned tables ready for analytics, BI dashboards, or reporting

You can choose any stack: Python + SQL (PostgreSQL, DuckDB), dbt, or a combination. You may bring in visualization tools if desired.

---

## Provided Data (Fixtures)

You will receive a `data/` folder containing:

- `transactions.csv` — transaction history across customer spend categories
- `sentiment.json` — social media posts (unstructured) with sentiment scores and engagement metrics
- `customers.csv` — customer demographics, membership, and preferences

Treat these as **external, messy datasets**. They contain:
- Missing/null values
- Duplicates (exact and near-duplicates)
- Inconsistent formats (dates, timestamps, currency, casing)
- Orphan records (references to non-existent customers)
- Invalid or out-of-range values

You should **not need external API keys or live data sources.**

---

## A) Data Ingestion & Validation

Your system must be able to:

- **Load** the three datasets without crashing on missing/invalid data
- **Detect and log** data quality issues (missing rates, duplicates, schema violations, orphan keys)
- **Describe** key characteristics of each source (row counts, unique values, null patterns)

Your approach should be **resilient**: bad rows should not cause the pipeline to fail; instead, you should log issues and decide on a handling strategy (skip, impute, flag for review).

### Required Outputs

Produce **validation/quality reports** that show:
- Row counts (before & after cleaning)
- Null rates by column
- Duplicate detection (exact and fuzzy matches)
- Orphan key detection (e.g., transactions with non-existent customer_id)
- Data type mismatches (e.g., non-numeric amounts, unparseable dates)

---

## B) Data Transformation & Normalization

Transform the raw data into a **clean, queryable data model**. You have full flexibility in schema design, but consider:

### Suggested approach (not mandatory)

- **Dimension tables**: `customers`, `merchants`, `categories` (lookup tables)
- **Fact table**: `transactions` (normalized, deduplicated, timestamped)
- **Derived table**: `customer_profile` (aggregated features: total spend, transaction frequency, preferred categories, etc.)
- **Sentiment table**: `sentiment_posts` (normalized, deduplicated) or `sentiment_daily_summary` (aggregated by topic/date)

### Key responsibilities

- **Deduplication**: Remove exact duplicates and handle near-duplicates (e.g., off-by-one errors in amounts)
- **Normalization**: Standardize formats (dates, currencies, casing, units)
- **Key management**: Enforce referential integrity (all customer_ids exist, etc.)
- **Type safety**: Ensure columns have correct types and valid ranges
- **Lineage**: Track which raw rows contributed to each clean row (for debugging)

---

## C) Data Governance & Quality

Implement a **data contract** and **ongoing validation layer**:

### Schema Contracts

Define expected column names, types, nullability, and constraints for each output table. Document why each column exists.

### Data Quality Checks

Implement checks that can be run **on pipeline output**:
- Row count thresholds (alert if transactions drop unexpectedly)
- Null rate limits (fail if key columns exceed X% null)
- Uniqueness constraints (customer_id should be unique in dimension table)
- Value range checks (transaction amounts >= 0, except refunds; dates within reasonable range)
- Referential integrity (no orphan foreign keys)
- Freshness checks (data should be from the last N days)

### Lineage & Debugging

Be able to **trace back** from any clean row to its source(s):
- Which raw rows created this output row?
- What transformations were applied?
- Were any rows skipped or flagged?

---

## D) Technology & Implementation Choices

You are **free to choose your approach**. Some options:

### Option 1: Python + SQL (PostgreSQL or DuckDB)
- Use Python for orchestration and data loading
- SQL for transformation (CREATE TABLE AS, normalizations)
- Python for validation logic (pandas, polars, or raw SQL)
- Simple SQLite or DuckDB for local development

### Option 2: dbt + SQL
- Use dbt for transformation DAG and documentation
- SQL for all normalization logic
- dbt tests for data quality
- dbt can run on top of Postgres, DuckDB, Snowflake (local dev friendly)

### Option 3: Hybrid
- Use dbt for transformations + tests
- Python for non-standard validation (fuzzy dedup, custom data quality)
- Orchestrate with simple shell scripts or Python

### Visualization (Optional)

If you want to surface insights from the clean data, you may use any tool:
- Metabase / Superset (open source, local)
- Streamlit / Plotly (Python-based dashboards)
- Looker / Tableau / Sigma (if you have access)
- Simple SQL + CSV export + Excel

The goal is **readability and debugging**, not production BI.

---

## E) Deliverables

You should produce:

1. **Runnable pipeline code** that:
   - Loads raw data
   - Validates and cleans
   - Outputs normalized tables
   - Logs data quality issues

2. **Schema documentation** describing:
   - Each output table (purpose, columns, constraints)
   - Transformation logic (how raw data becomes clean data)
   - Lineage model (how to trace issues)

3. **Data quality reports** showing:
   - Validation results (pass/fail on key checks)
   - Data profile (nulls, duplicates, value ranges)
   - Recommendations (e.g., "rows with null age should be handled as follows...")

4. **README** with:
   - How to run the pipeline
   - Architecture overview & design decisions
   - Data model diagram or description
   - Known data quality issues & how you handled them
   - Tradeoffs made (scope cuts, assumptions, workarounds)
   - What you'd do next with more time (scalability, observability, etc.)

---

## Evaluation Criteria

We care about:

- **Data Architecture**: Is your schema design sound? Are fact/dimension tables well-separated? Can it scale to more data sources?
- **Data Quality & Governance**: Do you validate inputs? Are quality checks automated? Can you debug issues end-to-end?
- **Practical Problem Solving**: Did you handle duplicates, missing values, and inconsistent formats gracefully? Did you log decisions?
- **Code & Documentation**: Is your code readable and modular? Are transformation steps clear? Can someone else understand your choices?
- **Communication**: Can you explain tradeoffs, assumptions, and risks? Do you articulate why your design is maintainable?

We do **not** care about:
- Perfect model accuracy or insights (this is a generated dataset)
- Production-grade scalability or optimization
- Fancy UI or visualization (if you include it, keep it simple)
- External integrations or APIs

---

## Debrief (1 hour)

In the debrief we'll:

- Walk through your pipeline and data model
- Review data quality checks and governance approach
- Deep-dive on a tricky data quality issue (how did you handle it?)
- Discuss how you'd scale the pipeline (new data sources, higher volume, real-time feeds)
- Talk through architectural decisions and what you'd do differently with more time

---

## What to Submit

- Repo link (or zip) containing:
  - All code (Python, SQL, dbt, etc.)
  - README as described above
  - Sample outputs (e.g., exported clean tables, validation reports)
  - Any documentation or architecture diagrams
- Any notes or assumptions you want us to know (optional)
