# Data Overview

This document describes the three fixture datasets and the patterns embedded in them. The data is intentionally messy (missing values, duplicates, inconsistent formats) to test data-cleaning skills, but it also contains **realistic demographic correlations** that reward exploratory analysis — and that directly inform the attendance prediction task.

---

## Datasets at a Glance

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `customers.csv` | CSV | 204 (200 unique) | Customer demographics, membership, favorite team |
| `transactions.csv` | CSV | 2,510 (2,505 unique) | Purchase history across 9 categories |
| `sentiment.json` | JSON | 269 (264 unique) | Social media posts with sentiment scores and engagement |

---

## 1. `customers.csv`

### Schema

| Column | Type | Notes |
|--------|------|-------|
| `customer_id` | string | `CUST-NNNN`, 200 unique IDs |
| `name` | string | Italian + international names |
| `email` | string | ~12 missing |
| `age` | int/empty | 17–72, ~23 missing |
| `city` | string | 8 cities, inconsistent casing (lower/upper/title) |
| `country` | string | Italy, UK, USA, UAE |
| `signup_date` | string | Mixed formats: `YYYY-MM-DD`, `MM/DD/YYYY`, `DD Mon YYYY` |
| `favorite_team` | string | AC Milan (heavy), Inter, Juventus, Napoli, Roma, or empty |
| `membership_tier` | string | gold/silver/bronze/free, ~3 missing |
| `gender` | string | male/female/non-binary, inconsistent casing, ~6 missing |

### Distributions

- **City**: Milan is heavily weighted (~36%), then London/Turin/Rome/Florence (~10% each), Naples/Dubai/New York (~7-9% each)
- **Favorite team**: AC Milan ~42%, Juventus ~20%, Inter ~15%, none ~10%, Roma ~7%, Napoli ~6%
- **Tier**: Roughly even across gold (57), bronze (58), silver (46), free (40)
- **Age groups**: Under 30 (39), 30–49 (64), 50+ (78)

### Data quality issues to clean
- 4 duplicate customer IDs with slight field variations (email, city casing, age off-by-one)
- Mixed date formats in `signup_date`
- Inconsistent casing on `city` and `gender`
- Empty strings instead of nulls for missing `favorite_team`, `membership_tier`

---

## 2. `transactions.csv`

### Schema

| Column | Type | Notes |
|--------|------|-------|
| `transaction_id` | string | `TXN-NNNNNN` |
| `customer_id` | string | References customers (+ 5 orphan IDs) |
| `timestamp` | string | Mixed: ISO 8601, `YYYY-MM-DD HH:MM`, `MM/DD/YYYY`, `DD-Mon-YYYY` |
| `amount` | float/empty | ~184 missing, ~59 negative (refunds) |
| `currency` | string | Mostly `EUR`, some `eur`, `€`, rare `USD` |
| `category` | string | 9 categories, some with leading/trailing whitespace |
| `merchant` | string | Category-specific merchant names |
| `description` | string | `{merchant} - {category title}` |

### Categories

| Category | Count | % | Avg amount range |
|----------|-------|---|------------------|
| match_tickets | 533 | 21% | EUR 25–350 |
| sports_merchandise | 424 | 17% | EUR 15–200 |
| sports_bar | 404 | 16% | EUR 5–150 |
| streaming | 355 | 14% | EUR 8–40 |
| groceries | 187 | 7% | EUR 5–120 |
| transport | 180 | 7% | EUR 1.50–45 |
| entertainment | 148 | 6% | EUR 5–150 |
| dining | 140 | 6% | EUR 8–80 |
| retail | 139 | 6% | EUR 5–150 |

### Data quality issues to clean
- 5 exact duplicate rows
- 5 orphan `customer_id` values (CUST-0309, 0332, 0334, 0358, 0383) with no matching customer
- Mixed timestamp formats
- Missing amounts (~7%), negative refunds (~2%)
- Inconsistent currency representations
- Whitespace padding in some category values

---

## 3. `sentiment.json`

### Schema

Each item has: `id`, `user`, `source`, `text`, `published_at`, `topic`, `tags`, `sentiment_score`, `engagement`

### Distributions

- **115 unique users**, each posting 2–3 times
- **Topics**: AC Milan (36), F1 Racing (30), Random (29), Tech News (28), Weather (25), Inter Milan (24), Serie A (20), Champions League (19), Fashion Week (19), Roma (15), Napoli (10), Transfer Rumors (8), Juventus (6)
- **Sources**: twitter, reddit, news, blog, forum
- **Sentiment**: mean 0.204, range -0.5 to 0.9 (football posts skew positive)

### Data quality issues to clean
- ~5 duplicate IDs
- 30 null sentiment scores, 27 null engagement objects, 13 empty text fields
- Mixed topic casing (lowercase, title case)
- Mixed `published_at` formats

---

## Embedded Demographic Patterns

These are the correlations a candidate should discover through EDA. Each is directly relevant to building a better attendance predictor.

### Favorite Team → Sports Spending

| Team | Sports % of transactions |
|------|--------------------------|
| AC Milan | 74% |
| Juventus | 69% |
| Roma | 68% |
| Napoli | 66% |
| Inter Milan | 65% |
| No team | 54% |

**Relevance to task**: A customer's favorite team is a strong signal for sports engagement. AC Milan fans spend disproportionately on sports — they're the most likely match attenders.

### Membership Tier → Spend Amount and Volume

| Tier | Avg transaction | Transaction count |
|------|----------------|-------------------|
| Gold | EUR 122 | 1,031 |
| Silver | EUR 106 | 561 |
| Bronze | EUR 84 | 470 |
| Free | EUR 58 | 244 |

**Relevance to task**: Gold members spend ~2x more per transaction and transact ~4x more often. Higher-tier customers are more engaged and less price-sensitive — ticket price should matter less in their attendance prediction.

### City → Match Ticket Purchases

| City | Match ticket % |
|------|---------------|
| Milan | 28% |
| Turin | 22% |
| Florence | 22% |
| Naples | 18% |
| Rome | 17% |
| New York | 16% |
| London | 14% |
| Dubai | 11% |

**Relevance to task**: Milan residents buy match tickets at ~2.5x the rate of international fans. Proximity to San Siro is a strong attendance driver — a candidate should weight city/locality in their prediction model.

### Country → Streaming vs Tickets

| Country | Streaming % |
|---------|-------------|
| UK | 25% |
| UAE | 22% |
| USA | 22% |
| Italy | 11% |

**Relevance to task**: International fans consume via streaming rather than in-person attendance. This is a meaningful signal — an international customer's sports spending doesn't indicate they'll physically attend a match.

### Age → Category Preferences

| Age group | Top non-sports category | Notable pattern |
|-----------|------------------------|-----------------|
| Under 30 | Streaming (18%), Entertainment (11%) | Digital-first, experience-oriented |
| 30–49 | Transport (7%), Dining (6%) | Balanced, baseline |
| 50+ | Match tickets (25%), Groceries (9%) | Highest in-person match attendance |

**Relevance to task**: Older customers already attend matches at higher rates (25% match ticket share vs 15% for under-30s). Age should factor into attendance probability, and the model should consider that younger fans might be more price-elastic.

---

## How the Patterns Connect to the Take-Home

A strong candidate doing the Attendance Prediction Challenge should:

1. **In data integration** — clean the messy fields (dates, casing, whitespace, duplicates, orphans) and compute per-customer features like sports affinity, average spend, and recency
2. **In feature engineering** — discover and leverage the demographic correlations above:
   - Sports affinity ratio as a feature (varies by team)
   - Tier as a proxy for engagement and price sensitivity
   - City/country to distinguish local vs remote fans
   - Age-based attendance propensity
3. **In the prediction model** — incorporate these derived features as inputs so the model can weight them appropriately (e.g., "This gold-tier AC Milan fan in Milan spends 74% on sports and has attended 12 matches")
4. **In the UI** — customer profiles segmented by these dimensions make the tool more useful and the results more interpretable

The sentiment data adds a contextual layer: recent AC Milan sentiment, weather conditions, and overall football buzz should modulate the base attendance probability derived from transaction patterns.
