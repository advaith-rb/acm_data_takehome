# Handoff: Demographic Purchasing Patterns

## What changed

`generate_fixtures.py` was updated so that transaction data correlates with customer demographics. Previously, each transaction picked a random customer and random category with zero correlation — now there are realistic, discoverable patterns.

## File modified

`/acm_data_takehome/internal/generate_fixtures.py`

## Changes

### 1. Full customer records passed to `generate_transactions()`
- `generate_customers()` now returns the full `list[dict]` instead of just IDs.
- `generate_transactions()` accepts `customers: list[dict]`, builds a deduplicated lookup dict `cust_by_id`.

### 2. Tier-weighted transaction volume
- Gold/silver customers appear in more transactions via `random.choices` with weights.
- `TIER_TXN_WEIGHTS`: gold=3, silver=2, bronze=1.5, free=1.
- **Result:** Gold customers have ~4x more transactions than free-tier.

### 3. Team-based sports spending probability
- AC Milan fans: 55% chance of sports category per transaction.
- Other team fans: 45%.
- No team: 18%.
- **Result:** Clear sports spend gap between fans and non-fans.

### 4. City-weighted sports sub-categories
- Milan residents: heavily weighted toward match tickets (weight 4 vs 1 for streaming).
- Other Italian cities: balanced with slight ticket preference.
- International: heavily weighted toward streaming (weight 3 vs 1 for tickets).
- Uses `SPORTS_CATEGORIES_LIST` (fixed-order list) for deterministic weighted selection.
- **Result:** Milan residents buy ~2.5x more match tickets than international fans.

### 5. Tier-based amount scaling
- `TIER_AMOUNT_MULT`: gold=1.4x, silver=1.15x, bronze=1.0x, free=0.75x, empty=0.9x.
- Applied to the base amount before messiness injection (missing amounts, refunds).
- **Result:** Gold avg transaction ~$122 vs free ~$58 (~2.1x ratio).

### 6. Age-based category preferences (non-sports)
- Under 30: weighted toward streaming, dining, entertainment.
- 50+: weighted toward groceries, match tickets.
- 30-49: uniform random (baseline).
- **Result:** Distinct spending profiles by age group.

## Discoverable patterns summary

| Segment | Pattern |
|---------|--------|
| Favorite team | AC Milan fans: 74% sports spend vs no team: 54% |
| Membership tier (volume) | Gold: ~1031 txns vs Free: ~244 txns |
| Membership tier (amount) | Gold avg: $122 vs Free avg: $58 |
| City | Milan: 28% match tickets vs Dubai: 11% |
| Country | International fans favor streaming over tickets |
| Age | Under-30: streaming/entertainment; 50+: groceries/tickets |

## Key constants added

```python
SPORTS_CATEGORIES_LIST = ["match_tickets", "sports_merchandise", "sports_bar", "streaming"]
TIER_TXN_WEIGHTS = {"gold": 3, "silver": 2, "bronze": 1.5, "free": 1, "": 1}
TIER_AMOUNT_MULT = {"gold": 1.4, "silver": 1.15, "bronze": 1.0, "free": 0.75, "": 0.9}
```

## Verification

```bash
cd /acm_data_takehome/internal
python3 generate_fixtures.py
```

Regenerated fixtures are in `fixtures/` with all patterns baked in.