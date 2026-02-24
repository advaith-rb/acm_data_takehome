# Reference Solution — Analytics Engineer Take-Home

**DO NOT SHARE WITH CANDIDATES**

This directory contains a working reference implementation of the data pipeline challenge. Use this to:

1. **Evaluate candidate submissions** — compare their approach to this one
2. **Verify the problem is solvable** in the 3-4 hour window
3. **Debrief on design choices** — ask candidates why they made different decisions

## Running the Solution

```bash
# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python main.py
```

This will:
1. Load raw data from `../../data/`
2. Create DuckDB database in `output/duckdb.db`
3. Validate raw data, log issues in `output/validation_report.json`
4. Transform and load clean tables
5. Validate output and log results
6. Write `output/pipeline_report.json`

## Output Files

- `output/duckdb.db` — SQLite-compatible database with clean tables
- `output/validation_report.json` — Raw data quality issues
- `output/pipeline_report.json` — Pipeline execution summary

## Architecture

See `solution_overview.md` for:
- Design decisions and tradeoffs
- Schema diagrams and table descriptions
- Data quality governance approach
- Why this design was chosen

## Key Files

- `main.py` — Entry point, orchestrates pipeline
- `pipeline.py` — Data loading/ingestion
- `validation.py` — Data quality checks
- `transforms.py` — SQL-based transformations
- `schema.py` — Table definitions and Pydantic contracts
- `config.py` — Configuration and constants

## What Makes This a Good Solution

1. **Resilient Ingestion** — doesn't crash on bad data, logs issues
2. **Clear Separation** — ingestion → validation → transformation → validation
3. **Debuggable** — can trace any output row back to raw source (`_source_row_id`)
4. **Documented** — schema contracts, meaningful column names, comments
5. **Testable** — each stage has clear inputs/outputs
6. **Scalable** — could swap DuckDB for Postgres or Snowflake without code changes

## Comparing to Candidate Solutions

### dbt Approach
- **Pros**: Great docs, lineage, version control
- **Cons**: Extra tooling, might be unfamiliar
- Still should have same tables, same validations

### Pandas-Only Approach
- **Pros**: Quick to prototype
- **Cons**: Harder to debug, less SQL-aware
- Should still have clear transformations and validation

### Spark Approach
- **Pros**: Distributed
- **Cons**: Overkill for this dataset
- Same principles apply

## Evaluation Checklist

When reviewing a candidate solution, look for:

- [ ] **Ingestion**: Does it handle CSV/JSON without crashing? Are parsing errors logged?
- [ ] **Validation**: Can you see what data quality issues were found?
- [ ] **Transformation**: Are raw and clean tables separate? Is deduplication explicit?
- [ ] **Schema**: Are there fact/dimension separations? Are constraints clear?
- [ ] **Lineage**: Can you trace a clean row back to its source?
- [ ] **Documentation**: Is the README clear? Can they explain the design?
- [ ] **Code Quality**: Is it modular and readable? Are tradeoffs documented?

## Debrief Questions

1. **Walk me through your pipeline** — how data flows from raw to clean
2. **How did you handle the 4 duplicate customers?** What would you do at scale?
3. **Show me a data quality check** — how would you know if the pipeline broke?
4. **Lineage** — trace a value from output back to raw source
5. **Scaling** — how would this change with 100M daily rows?
6. **Tradeoffs** — what did you cut? Why?

## Notes

- This solution prioritizes **clarity over performance**
- DuckDB is chosen for **simplicity** — no server to manage
- Staging tables kept for **debuggability** — can inspect transformations
- Aggregate tables pre-computed for **fast analytics**
- All validation results logged to JSON for **auditability**

Candidates may make different choices. That's fine — evaluate based on their **reasoning** and **execution**, not exact matching.
