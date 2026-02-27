# Reference Solution — Analytics Engineer Take-Home

**DO NOT SHARE WITH CANDIDATES**

---

## Architecture Overview

This reference solution demonstrates a clean, maintainable data pipeline for ingesting, validating, and transforming the three messy datasets into a queryable data platform.

### Tech Stack

- **Python 3.9+** for orchestration, loading, and validation
- **DuckDB** (in-process SQL database, no setup required)
- **Pandas** for data manipulation and validation
- **Pydantic** for schema contracts
- **SQLAlchemy** for SQL abstraction

### Why This Approach?

1. **No infrastructure** — DuckDB is embedded, no Postgres installation needed
2. **Python-friendly** — most analytics engineers know Python well
3. **Testable** — each layer has clear inputs/outputs
4. **Debuggable** — SQL is transparent, can inspect tables at any stage
5. **Extensible** — easy to add new data sources or validation rules

---

## Pipeline Stages

### 1. Ingestion (`pipeline.py`)

```
Raw CSV/JSON files
    ↓
Read with Pandas (lenient parsing)
    ↓
Basic type detection & validation
    ↓
Write to DuckDB staging tables (raw_customers, raw_transactions, raw_sentiment)
    ↓
Log issues (parsing errors, duplicates, orphans, nulls)
```

**Key design:**
- Load raw data as-is, then clean it step-by-step
- Use `try/except` to handle parsing errors row-by-row
- Track lineage: `_source_row_id` in staging tables
- Generate validation report before transforming

### 2. Validation (`validation.py`)

Run data quality checks on raw staging tables:

| Check | What | Result |
|-------|------|--------|
| Row counts | How many rows loaded for each source? | Report |
| Null rates | What % of each column is null? | Report + recommendations |
| Duplicates | Exact or fuzzy match detection | Flag rows, suggest dedup strategy |
| Orphan keys | customer_id in transactions not in customers | Report & decide (skip or flag) |
| Type violations | Non-numeric amounts, unparseable dates | Report & count |
| Value range | Transaction amounts >= -1000 and <= 10000? | Report violations |
| Freshness | Are dates within reasonable range (2020-2026)? | Report outliers |

**Output:** `validation_report.json` with structure:

```json
{
  "timestamp": "2026-02-24T10:30:00Z",
  "raw_data": {
    "customers": {
      "rows_loaded": 204,
      "nulls_by_column": { "email": 12, "age": 23, ... },
      "duplicates": 4,
      "issues": ["4 customer_id duplicates found", ...]
    },
    "transactions": { ... },
    "sentiment": { ... }
  },
  "recommendations": [
    "Remove 5 exact duplicate transactions",
    "Skip 5 transactions with orphan customer_id",
    ...
  ]
}
```

### 3. Transformation (`transforms.py`)

Clean and normalize data using SQL:

#### Stage 1: Deduplicate

```sql
-- Customers: keep first occurrence of each customer_id, standardize fields
WITH deduped AS (
  SELECT DISTINCT ON (customer_id) *
  FROM raw_customers
  ORDER BY customer_id, _load_timestamp
)
SELECT
  customer_id,
  LOWER(TRIM(name)) as name,
  LOWER(TRIM(email)) as email,
  CAST(age AS INTEGER) as age,
  LOWER(TRIM(city)) as city,
  ...
FROM deduped
WHERE customer_id IS NOT NULL
```

#### Stage 2: Normalize & Type Coerce

```sql
-- Transactions: parse dates, standardize currency, validate amounts
SELECT
  transaction_id,
  customer_id,
  CAST(parse_timestamp(timestamp) AS TIMESTAMP) as ts,
  TRY_CAST(amount AS DECIMAL(10, 2)) as amount_eur,  -- handle missing
  UPPER(TRIM(currency)) as currency_code,
  LOWER(TRIM(category)) as category,
  ...
FROM raw_transactions
WHERE customer_id IN (SELECT customer_id FROM dim_customers)
```

#### Stage 3: Build Dimensions

```sql
-- dim_customers: one row per unique customer
CREATE TABLE dim_customers AS
SELECT
  customer_id,
  name,
  email,
  age,
  city,
  country,
  favorite_team,
  UPPER(membership_tier) as tier,
  signup_date,
  CURRENT_TIMESTAMP as dbt_loaded_at
FROM cleaned_customers
WHERE customer_id IS NOT NULL
```

#### Stage 4: Build Facts

```sql
-- fact_transactions: normalized fact table with FK references
CREATE TABLE fact_transactions AS
SELECT
  transaction_id,
  customer_id,
  ts as transaction_date,
  amount_eur,
  category,
  merchant,
  _source_row_id,  -- lineage: which raw row did this come from?
  CURRENT_TIMESTAMP as dbt_loaded_at
FROM cleaned_transactions
WHERE customer_id IS NOT NULL
  AND amount_eur IS NOT NULL
  AND amount_eur > -1000 AND amount_eur < 50000
```

#### Stage 5: Build Aggregations

```sql
-- customer_profile: pre-computed aggregations for easy analytics
CREATE TABLE customer_profile AS
SELECT
  c.customer_id,
  COUNT(DISTINCT t.transaction_id) as txn_count,
  ROUND(SUM(t.amount_eur), 2) as total_spend,
  ROUND(AVG(t.amount_eur), 2) as avg_txn,
  MAX(t.transaction_date) as last_txn_date,
  COUNT(DISTINCT CASE WHEN t.category = 'match_tickets' THEN t.transaction_id END) as match_ticket_count,
  ...
FROM dim_customers c
LEFT JOIN fact_transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_id
```

### 4. Validation Output (`validation.py`)

Run post-transformation checks:

```
fact_transactions has no nulls in customer_id? ✓
fact_transactions.customer_id all exist in dim_customers? ✓
dim_customers.customer_id is unique? ✓
Row counts match expectation (within 10%)? ✓
No stale data (all dates > 2020-01-01)? ✓
```

---

## Schema Design

### Fact Tables

**fact_transactions**
```sql
CREATE TABLE fact_transactions (
  transaction_id VARCHAR PRIMARY KEY,
  customer_id VARCHAR NOT NULL REFERENCES dim_customers(customer_id),
  transaction_date TIMESTAMP NOT NULL,
  amount_eur DECIMAL(10, 2) NOT NULL,
  category VARCHAR NOT NULL,
  merchant VARCHAR,
  _source_row_id INTEGER NOT NULL,  -- links back to raw data for debugging
  _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**fact_sentiment**
```sql
CREATE TABLE fact_sentiment (
  post_id VARCHAR PRIMARY KEY,
  user_name VARCHAR,
  topic VARCHAR,
  sentiment_score DECIMAL(3, 2),
  engagement INT,
  published_at TIMESTAMP,
  _source_row_id INTEGER,
  _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Dimension Tables

**dim_customers**
```sql
CREATE TABLE dim_customers (
  customer_id VARCHAR PRIMARY KEY,
  name VARCHAR,
  email VARCHAR,
  age INTEGER CHECK (age >= 0 AND age <= 150),
  city VARCHAR,
  country VARCHAR,
  favorite_team VARCHAR,
  membership_tier VARCHAR CHECK (membership_tier IN ('gold', 'silver', 'bronze', 'free')),
  signup_date DATE,
  _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Aggregate Tables

**customer_profile** (for fast querying)
```sql
CREATE TABLE customer_profile (
  customer_id VARCHAR PRIMARY KEY,
  txn_count INT,
  total_spend DECIMAL(10, 2),
  avg_txn DECIMAL(10, 2),
  last_txn_date DATE,
  match_ticket_count INT,
  sports_spend_ratio DECIMAL(3, 2),
  avg_days_between_txns DECIMAL(5, 1),
  _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Data Quality Governance

### Pre-Transform Validation

Before cleaning, we validate the input:

1. **Row Counts**: Log how many rows loaded for each source
2. **Null Analysis**: Flag columns with >50% nulls
3. **Duplicate Detection**: Find exact + fuzzy matches
4. **Orphan Keys**: Identify transactions with non-existent customers
5. **Type Checking**: Try to parse dates/numbers, log failures

### Post-Transform Validation

After cleaning, we enforce contracts:

```python
# schema_contracts.py
from pydantic import BaseModel, Field

class CustomerContract(BaseModel):
    customer_id: str
    name: str
    age: Optional[int] = Field(None, ge=0, le=150)
    membership_tier: str
    
    class Config:
        table_name = "dim_customers"
        row_count_threshold = 190  # expect ~200 rows, warns if < 190

class TransactionContract(BaseModel):
    transaction_id: str
    customer_id: str
    amount_eur: Decimal = Field(..., ge=Decimal("-1000"), le=Decimal("50000"))
    transaction_date: datetime
    
    class Config:
        table_name = "fact_transactions"
        allow_orphan_fk = False  # all customer_ids must exist
```

### Validation Report

After each run, generate:

```json
{
  "pipeline_run_id": "2026-02-24-001",
  "timestamp": "2026-02-24T10:30:00Z",
  "stage_results": {
    "ingestion": { "status": "success", "rows_loaded": 476 },
    "validation": { "status": "warning", "issues": ["4 duplicates found", ...] },
    "transformation": { "status": "success", "rows_output": 470 },
    "post_validation": { "status": "success", "all_checks_pass": true }
  },
  "data_quality": {
    "dim_customers": { "rows": 200, "nulls": {...} },
    "fact_transactions": { "rows": 2505, "referential_integrity": "ok" },
    "customer_profile": { "rows": 200, "completeness": 1.0 }
  }
}
```

---

## Running the Pipeline

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline
python main.py

# Or run individual stages
python main.py --stage ingestion
python main.py --stage validation
python main.py --stage transform
python main.py --stage post_validate

# Query the results
python -c "
import duckdb
conn = duckdb.connect('output/duckdb.db')
print(conn.execute('SELECT * FROM dim_customers LIMIT 5').df())
"
```

---

## Key Design Decisions

### 1. DuckDB vs PostgreSQL

**Why DuckDB:**
- No installation required
- File-based (easy to version, backup, share)
- SQL support and OLAP optimization
- Great pandas integration

**Tradeoff:** Single-process, not suitable for concurrent access. For multi-user, would migrate to Postgres.

### 2. Staging Tables

We keep `raw_*` tables alongside clean tables for:
- **Debugging** — can trace issues back to source
- **Replayability** — don't lose original data
- **Lineage** — `_source_row_id` links fact rows to raw rows

**Tradeoff:** Doubles storage. In production, might archive raw data separately.

### 3. Aggregate Tables

Pre-computing `customer_profile` avoids repeatedly joining large fact table to dimensions.

**Tradeoff:** Slight staleness (refresh on schedule). Could use views instead if data is small enough.

### 4. Silent vs Loud Failures

We **log issues but don't crash**:
- Missing emails → skip that row, log count
- Non-numeric amounts → set to NULL, report
- Orphan customer_id → exclude from fact table, log

This allows partial success. In production, could make stricter.

### 5. Deduplication Strategy

For customers, we keep the first occurrence of each ID (by load timestamp).

**Alternative:** Merge conflicting fields (e.g., prefer non-null age). This reference solution takes the simpler approach.

---

## Expected Candidate Variations

### Python + SQL (like this solution)
- Pros: Clear separation, SQL is readable, easy to test
- Cons: More boilerplate

### dbt
- Pros: Great for documentation, versioning, lineage
- Cons: New tool, might have steeper curve

### Pandas Only
- Pros: Quick to write
- Cons: Hard to scale, less readable for SQL practitioners

### Spark
- Pros: Distributed
- Cons: Overkill for this dataset, adds complexity

All approaches are valid. We evaluate on **clarity, governance, and explanation** — not tool choice.

---

## Debrief Questions

When reviewing a candidate solution, ask:

1. **Data Model**: "Walk me through your fact/dimension design. Why did you separate X from Y?"
2. **Deduplication**: "How did you handle the 4 duplicate customers? What approach would you take with 1M records?"
3. **Validation**: "How would you know if the pipeline broke? Walk me through your quality checks."
4. **Lineage**: "If I find a wrong value in the output, how would you trace it back to the raw source?"
5. **Scaling**: "How would this design change if we had 100M transactions/day coming in real-time?"
6. **Tradeoffs**: "What did you cut? What would you do differently with more time?"

---

## Running This Solution

```bash
cd internal/reference_solution
python main.py
```

This will:
1. Load raw data from `../../data/`
2. Run validation and generate `output/validation_report.json`
3. Transform and load clean tables into `output/duckdb.db`
4. Generate `output/pipeline_report.json`
5. Display summary stats

Candidate should be able to do something similar, though their implementation may look quite different.
