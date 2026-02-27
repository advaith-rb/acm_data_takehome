# Analytics Engineer Take-Home — Internal Guide

**Internal** — do not share with candidates.

---

## Repo Structure

```
takehome.md                  # Candidate-facing assignment brief
data/
  customers.csv              # 204 rows (200 unique customers)
  transactions.csv           # 2,510 rows (2,505 unique transactions)
  sentiment.json             # 269 social media posts
internal/
  DATA_OVERVIEW.md           # Data quality issues and embedded patterns
  README.md                  # How to run reference solution
  SOLUTION_OVERVIEW.md       # Architecture and design decisions
  main.py, pipeline.py, etc. # Reference solution code
  requirements.txt           # Python dependencies
```

## What Candidates Receive

- `takehome.md` — the assignment brief
- `data/` — the three fixture files

Candidates do **not** receive `internal/` or this README.

## Sending to a Candidate

1. Create a new private repo or zip containing only `takehome.md` and `data/`
2. Share the repo/zip with the candidate

## Regenerating Data

The data is deterministic (seeded). To regenerate after making changes:

```bash
cd internal
python3 generate_fixtures.py
# Output lands in a fixtures/ directory next to the script
# Copy the files to data/
cp fixtures/*.csv fixtures/*.json ../data/
```

## Embedded Patterns (What We're Looking For)

The data has intentional demographic correlations that a strong candidate should discover through EDA. See `internal/DATA_OVERVIEW.md` for full details. Quick summary:

| Signal | Pattern | Why It Matters |
|--------|---------|----------------|
| Favorite team | AC Milan fans: 74% sports spend vs no-team: 54% | Sports affinity is the strongest attendance predictor |
| Membership tier | Gold avg EUR 122, 1031 txns vs Free avg EUR 58, 244 txns | Tier proxies for engagement and price sensitivity |
| City | Milan: 28% match tickets vs Dubai: 11% | Proximity to San Siro drives in-person attendance |
| Country | International fans: 22-25% streaming vs Italy: 11% | Remote fans consume digitally, not in-person |
| Age | 50+: 25% match tickets vs under-30: 15% | Older fans attend more; younger fans are more price-elastic |

## Data Quality Issues (Tests Cleaning Skills)

- **Customers**: 4 duplicate IDs (slight field variations), mixed date formats, inconsistent casing on city/gender, empty strings for nulls
- **Transactions**: 5 exact duplicates, 5 orphan customer IDs, mixed timestamp formats, ~7% missing amounts, ~2% refunds, whitespace in categories, inconsistent currency (`EUR`/`eur`/`€`/`USD`)
- **Sentiment**: ~5 duplicate IDs, 30 null sentiment scores, 27 null engagement, 13 empty texts, mixed topic casing

## Evaluation Notes

A strong candidate will:

1. **Load & validate reliably** — ingestion handles missing/invalid rows, parsing errors are logged, pipeline doesn't crash on bad data
2. **Design clear data models** — staging → cleaned dim/fact tables, schema contracts, and lineage (`_source_row_id`) exist
3. **Automate quality checks** — validation reports, null-rate/row-count thresholds, orphan/key checks, and freshness tests
4. **Engineer useful features** — derive customer/context features (recency, sports affinity, tier, ticket history) and surface them in `customer_profile`
5. **Document & communicate** — README explains how to run, design tradeoffs, and how to reproduce results

A weaker candidate will: crash on bad rows, mix ingestion with prediction logic, lack lineage, or omit automated validation.
