# Founding Engineer Take-Home — Internal Guide

**RedBird Internal** — do not share with candidates.

---

## Repo Structure

```
takehome.md                  # Candidate-facing assignment brief
data/
  customers.csv              # 204 rows (200 unique customers)
  transactions.csv           # 2,510 rows (2,505 unique transactions)
  sentiment.json             # 269 social media posts
rb_internal/
  generate_fixtures.py       # Deterministic data generator (seed=42)
  DATA_OVERVIEW.md           # Full dataset documentation + embedded patterns
  handoff.md                 # Changelog for the fixture generator
```

## What Candidates Receive

- `takehome.md` — the assignment brief
- `data/` — the three fixture files

Candidates do **not** receive `rb_internal/` or this README.

## Sending to a Candidate

1. Create a new private repo or zip containing only `takehome.md` and `data/`
2. Share the repo/zip with the candidate

## Regenerating Data

The data is deterministic (seeded). To regenerate after making changes:

```bash
cd rb_internal
python3 generate_fixtures.py
# Output lands in a fixtures/ directory next to the script
# Copy the files to data/
cp fixtures/*.csv fixtures/*.json ../data/
```

## Embedded Patterns (What We're Looking For)

The data has intentional demographic correlations that a strong candidate should discover through EDA. See `rb_internal/DATA_OVERVIEW.md` for full details. Quick summary:

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

1. **Clean the data** — handle all the messiness without crashing
2. **Discover the patterns** — compute features like sports affinity ratio, avg spend by tier, and incorporate them into the prediction model
3. **Build a clear model boundary** — `ModelClient.predict()` that takes structured features and returns probability + reasoning
4. **Wire up a usable UI** — customer selection, match config, results with provenance
5. **Explain tradeoffs** — what they cut, what they'd do with more time

A weaker candidate will treat all customers identically and miss the demographic signals entirely.
