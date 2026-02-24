#!/usr/bin/env python3
"""
Generate fixture data for the Attendance Prediction Challenge take-home.

Produces:
  - fixtures/customers.csv   (~200 customers)
  - fixtures/transactions.csv (~2500 transactions)
  - fixtures/sentiment.json   (~300 items)

Data is deterministic (seeded) and intentionally messy to test data-cleaning skills.
"""

import csv
import json
import os
import random
import string
from datetime import datetime, timedelta

SEED = 42
random.seed(SEED)

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
os.makedirs(OUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def format_date_messy(dt: datetime) -> str:
    """Return a date string in one of several formats."""
    fmt = random.choice([
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d %b %Y",
    ])
    return dt.strftime(fmt)


def format_timestamp_messy(dt: datetime) -> str:
    """Return a datetime string in one of several formats."""
    fmt = random.choice([
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y",
        "%d-%b-%Y",
    ])
    return dt.strftime(fmt)


FIRST_NAMES = [
    "Marco", "Luca", "Alessandro", "Andrea", "Francesco", "Matteo", "Lorenzo",
    "Davide", "Giuseppe", "Antonio", "Stefano", "Roberto", "Giovanni", "Paolo",
    "Simone", "Fabio", "Riccardo", "Daniele", "Tommaso", "Filippo",
    "Giulia", "Sara", "Chiara", "Anna", "Martina", "Valentina", "Elisa",
    "Francesca", "Alessia", "Federica", "Laura", "Silvia", "Elena", "Ilaria",
    "Claudia", "Maria", "Paola", "Roberta", "Monica", "Cristina",
    "James", "Oliver", "William", "Thomas", "David", "Mohammed", "Ahmed",
    "Sophie", "Emma", "Charlotte", "Emily", "Amelia", "Sarah", "Fatima",
]

LAST_NAMES = [
    "Rossi", "Russo", "Ferrari", "Esposito", "Bianchi", "Romano", "Colombo",
    "Ricci", "Marino", "Greco", "Bruno", "Gallo", "Conti", "De Luca",
    "Mancini", "Costa", "Giordano", "Rizzo", "Lombardi", "Moretti",
    "Barbieri", "Fontana", "Santoro", "Mariani", "Rinaldi", "Caruso",
    "Ferrara", "Gatti", "Pellegrini", "Palumbo", "Sartori", "Marchetti",
    "Smith", "Johnson", "Brown", "Williams", "Jones", "Khan", "Ali",
    "Taylor", "Wilson", "Davis", "Clark", "Hall", "Martin", "Adams",
]

CITIES_CANONICAL = ["Milan", "Rome", "Turin", "Naples", "Florence", "London", "New York", "Dubai"]
CITY_WEIGHTS = [3, 1, 1, 1, 1, 1, 1, 1]  # Milan weighted 3x

CITY_COUNTRY = {
    "Milan": "Italy", "Rome": "Italy", "Turin": "Italy", "Naples": "Italy",
    "Florence": "Italy", "London": "UK", "New York": "USA", "Dubai": "UAE",
}

TEAMS = [
    "AC Milan", "AC Milan", "AC Milan", "AC Milan", "AC Milan",  # heavy weight
    "Inter Milan", "Inter Milan",
    "Juventus", "Juventus",
    "Napoli",
    "Roma",
]

TIERS = ["gold", "silver", "bronze", "free"]

GENDERS = ["male", "female", "non-binary"]
GENDER_WEIGHTS = [45, 45, 10]

DOMAINS = ["gmail.com", "outlook.com", "yahoo.it", "libero.it", "icloud.com", "hotmail.com"]


def make_email(first: str, last: str) -> str:
    sep = random.choice([".", "_", ""])
    num = random.choice(["", str(random.randint(1, 99))])
    domain = random.choice(DOMAINS)
    return f"{first.lower()}{sep}{last.lower()}{num}@{domain}"


# ---------------------------------------------------------------------------
# 1. Customers
# ---------------------------------------------------------------------------

def generate_customers():
    customers = []
    signup_start = datetime(2020, 1, 1)
    signup_end = datetime(2025, 7, 31)

    for i in range(1, 201):
        cid = f"CUST-{i:04d}"
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        name = f"{first} {last}"
        email = make_email(first, last)
        age = random.randint(18, 72)
        city_canonical = random.choices(CITIES_CANONICAL, weights=CITY_WEIGHTS, k=1)[0]
        country = CITY_COUNTRY[city_canonical]
        signup = rand_date(signup_start, signup_end)
        team = random.choice(TEAMS + [None])  # occasional None
        tier = random.choice(TIERS)
        gender = random.choices(GENDERS, weights=GENDER_WEIGHTS, k=1)[0]

        # --- Messiness ---

        # ~10% missing age
        if random.random() < 0.10:
            age = ""

        # ~5% missing email
        if random.random() < 0.05:
            email = ""

        # Inconsistent city casing
        r = random.random()
        if r < 0.10:
            city = city_canonical.lower()
        elif r < 0.18:
            city = city_canonical.upper()
        else:
            city = city_canonical

        # Mixed signup_date formats
        signup_str = format_date_messy(signup)

        # ~5% missing tier (empty string instead of null)
        if random.random() < 0.05:
            tier = ""

        # ~5% missing gender
        if random.random() < 0.05:
            gender = ""
        # ~8% inconsistent casing
        elif random.random() < 0.08:
            gender = random.choice([gender.capitalize(), gender.upper()])

        # Some empty-string favorite_team instead of proper null
        if team is None:
            team = random.choice(["", ""])  # keep as empty string

        customers.append({
            "customer_id": cid,
            "name": name,
            "email": email,
            "age": age,
            "city": city,
            "country": country,
            "signup_date": signup_str,
            "favorite_team": team,
            "membership_tier": tier,
            "gender": gender,
        })

    # Inject 4 duplicate rows with slight variations
    dup_indices = random.sample(range(len(customers)), 4)
    for idx in dup_indices:
        orig = dict(customers[idx])
        # Slightly alter one field
        mutation = random.choice(["email", "city", "age"])
        if mutation == "email":
            parts = orig["name"].split()
            orig["email"] = make_email(parts[0], parts[1]) if len(parts) > 1 else orig["email"]
        elif mutation == "city":
            orig["city"] = orig["city"].upper() if orig["city"] == orig["city"].lower() else orig["city"].lower()
        elif mutation == "age" and orig["age"] != "":
            orig["age"] = int(orig["age"]) + random.choice([-1, 1])
        customers.append(orig)

    random.shuffle(customers)

    path = os.path.join(OUT_DIR, "customers.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "customer_id", "name", "email", "age", "city", "country",
            "signup_date", "favorite_team", "membership_tier", "gender",
        ])
        writer.writeheader()
        writer.writerows(customers)

    print(f"  customers.csv  -> {len(customers)} rows written to {path}")
    return customers


# ---------------------------------------------------------------------------
# 2. Transactions
# ---------------------------------------------------------------------------

CATEGORIES = [
    "match_tickets", "sports_merchandise", "sports_bar", "streaming",
    "groceries", "dining", "transport", "retail", "entertainment",
]

SPORTS_CATEGORIES_LIST = ["match_tickets", "sports_merchandise", "sports_bar", "streaming"]
SPORTS_CATEGORIES = set(SPORTS_CATEGORIES_LIST)

MERCHANTS = {
    "match_tickets": ["San Siro Box Office", "Ticketone", "Viagogo", "StubHub Italia"],
    "sports_merchandise": ["AC Milan Store", "Adidas Milan", "Nike Football Store", "Inter Store Milano"],
    "sports_bar": ["Bar Sport Milano", "The Red & Black Pub", "Birrificio Lambrate", "Old English Pub"],
    "streaming": ["DAZN", "Sky Sport", "Amazon Prime Video", "Paramount+"],
    "groceries": ["Esselunga", "Carrefour", "Coop", "Lidl Italia", "Pam Supermarket"],
    "dining": ["Pizzeria da Michele", "Ristorante Cracco", "McDonald's", "Poke House", "Spontini"],
    "transport": ["Uber", "ATM Milano", "Trenitalia", "Enjoy Car Sharing", "Lime"],
    "retail": ["Zara", "H&M", "IKEA Milano", "MediaWorld", "Feltrinelli"],
    "entertainment": ["Cinema Arcadia", "Teatro alla Scala", "Escape Room Milano", "Bowling Bicocca"],
}


TIER_TXN_WEIGHTS = {"gold": 3, "silver": 2, "bronze": 1.5, "free": 1, "": 1}
TIER_AMOUNT_MULT = {"gold": 1.4, "silver": 1.15, "bronze": 1.0, "free": 0.75, "": 0.9}


def generate_transactions(customers: list[dict]):
    # Build lookup and deduplicate on customer_id (keep first occurrence)
    cust_by_id = {}
    for c in customers:
        cid = c["customer_id"]
        if cid not in cust_by_id:
            cust_by_id[cid] = c
    unique_ids = list(cust_by_id.keys())

    # Tier-based weights so gold/silver customers get more transactions
    weights = [TIER_TXN_WEIGHTS.get(cust_by_id[cid].get("membership_tier", "").lower(), 1) for cid in unique_ids]

    txn_start = datetime(2025, 8, 1)
    txn_end = datetime(2026, 2, 15)
    transactions = []

    for i in range(1, 2501):
        txn_id = f"TXN-{i:06d}"
        cid = random.choices(unique_ids, weights=weights, k=1)[0]
        cust = cust_by_id[cid]

        # --- Demographic-driven category selection ---

        # Sports probability based on favorite team
        team = (cust.get("favorite_team") or "").strip()
        if team == "AC Milan":
            sports_prob = 0.55
        elif team:
            sports_prob = 0.45
        else:
            sports_prob = 0.18

        if random.random() < sports_prob:
            # Weight sports sub-categories by city
            city = (cust.get("city") or "").strip().lower()
            if city == "milan":
                cat = random.choices(
                    SPORTS_CATEGORIES_LIST,
                    weights=[4, 2, 2, 1],
                    k=1,
                )[0]
            elif cust.get("country") == "Italy":
                cat = random.choices(
                    SPORTS_CATEGORIES_LIST,
                    weights=[2, 2, 2, 1],
                    k=1,
                )[0]
            else:
                cat = random.choices(
                    SPORTS_CATEGORIES_LIST,
                    weights=[1, 2, 1, 3],
                    k=1,
                )[0]
        else:
            # Age-based non-sports category preferences
            age = cust.get("age")
            if age and age != "":
                age = int(age)
                if age < 30:
                    cat = random.choices(CATEGORIES, weights=[
                        1, 2, 2, 3,
                        1, 3, 2, 2, 3,
                    ], k=1)[0]
                elif age >= 50:
                    cat = random.choices(CATEGORIES, weights=[
                        3, 1, 1, 1,
                        3, 2, 2, 2, 1,
                    ], k=1)[0]
                else:
                    cat = random.choice(CATEGORIES)
            else:
                cat = random.choice(CATEGORIES)

        merchant = random.choice(MERCHANTS[cat])
        ts = rand_date(txn_start, txn_end)
        ts_str = format_timestamp_messy(ts)

        # Amount: sports tickets higher, groceries moderate, etc.
        if cat == "match_tickets":
            amount = round(random.uniform(25, 350), 2)
        elif cat == "sports_merchandise":
            amount = round(random.uniform(15, 200), 2)
        elif cat in ("streaming",):
            amount = round(random.uniform(7.99, 39.99), 2)
        elif cat == "groceries":
            amount = round(random.uniform(5, 120), 2)
        elif cat == "dining":
            amount = round(random.uniform(8, 80), 2)
        elif cat == "transport":
            amount = round(random.uniform(1.50, 45), 2)
        else:
            amount = round(random.uniform(5, 150), 2)

        # Scale amount by membership tier
        tier = (cust.get("membership_tier") or "").strip().lower()
        amount = round(amount * TIER_AMOUNT_MULT.get(tier, 1.0), 2)

        currency = "EUR"
        description = f"{merchant} - {cat.replace('_', ' ').title()}"

        # --- Messiness ---

        # ~8% missing amount
        if random.random() < 0.08:
            amount = ""

        # ~3% refunds (negative amounts)
        elif random.random() < 0.03:
            amount = -abs(amount)

        # Mixed currency representations
        r = random.random()
        if r < 0.05:
            currency = "eur"
        elif r < 0.08:
            currency = "€"
        elif r < 0.09:
            currency = "USD"

        # Extra whitespace in some category values (~5%)
        if random.random() < 0.025:
            cat = " " + cat
        elif random.random() < 0.025:
            cat = cat + " "

        transactions.append({
            "transaction_id": txn_id,
            "customer_id": cid,
            "timestamp": ts_str,
            "amount": amount,
            "currency": currency,
            "category": cat,
            "merchant": merchant,
            "description": description,
        })

    # Inject 5 exact duplicate rows
    dup_indices = random.sample(range(len(transactions)), 5)
    for idx in dup_indices:
        transactions.append(dict(transactions[idx]))

    # Inject 5 orphan customer_ids
    for _ in range(5):
        orphan_id = f"CUST-{random.randint(300, 400):04d}"
        cat = random.choice(CATEGORIES)
        transactions.append({
            "transaction_id": f"TXN-{random.randint(90000, 99999):06d}",
            "customer_id": orphan_id,
            "timestamp": format_timestamp_messy(rand_date(txn_start, txn_end)),
            "amount": round(random.uniform(10, 100), 2),
            "currency": "EUR",
            "category": cat,
            "merchant": random.choice(MERCHANTS[cat]),
            "description": f"Orphan txn",
        })

    random.shuffle(transactions)

    path = os.path.join(OUT_DIR, "transactions.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "transaction_id", "customer_id", "timestamp", "amount",
            "currency", "category", "merchant", "description",
        ])
        writer.writeheader()
        writer.writerows(transactions)

    print(f"  transactions.csv -> {len(transactions)} rows written to {path}")


# ---------------------------------------------------------------------------
# 3. Sentiment
# ---------------------------------------------------------------------------

TOPICS = [
    "AC Milan", "AC Milan", "AC Milan", "AC Milan",  # heavy weight
    "Inter Milan", "Inter Milan", "Juventus", "Serie A", "Serie A",
    "Champions League", "Transfer Rumors",
    "Napoli", "Roma",
    "Weather", "Weather",
    "F1 Racing", "Fashion Week", "Tech News", "Random",
]

SOURCES = ["twitter", "reddit", "news", "blog", "forum"]

# Twitter-style handles: mix of Italian football fans, general sports, noise accounts
HANDLE_PARTS_PREFIX = [
    "rossonero", "milanista", "calcio", "seriea", "ultras", "curva",
    "futbol", "golazo", "tifoso", "azzurri", "forza", "curvaSud",
    "ilDiavolo", "acm", "san_siro", "meazza", "campione", "maldini",
    "baresi", "gattuso", "pirlo", "kaka", "sheva", "inzaghi",
    "nesta", "seedorf", "rino", "boban", "leao_fan", "theo_stan",
    "pulisic", "tomori", "maignan", "bennacer", "tonali",
    "interista", "juve_fan", "napoli_ultra", "roma_here",
    "f1addict", "ferrari_fan", "modaMilano", "tech_bro",
    "espresso_lover", "la_dolce_vita", "roma_boy", "torino_girl",
]

HANDLE_PARTS_SUFFIX = [
    "99", "2k", "07", "1899", "1908", "2024", "ita", "milano",
    "_real", "_official", "szn", "x", "", "", "", "", "_",
    "fan", "hd", "daily", "takes", "vibes",
]

TWEETS_AC_MILAN = [
    "Forza Milan! What a game at San Siro tonight 🔴⚫",
    "Leao is genuinely world class when he turns up. No debate.",
    "San Siro atmosphere was ELECTRIC tonight. This is what football is about.",
    "Theo Hernandez bombing down the left flank gives me life",
    "The rebuild under RedBird is actually working? Color me surprised.",
    "San Siro renovation plans look sick but will miss the old girl 😢",
    "Watching Milan play like this makes all those dark years worth it",
    "That Pulisic goal was absolutely filthy. On repeat all night.",
    "Champions League nights at the Meazza hit different 🌙",
    "Derby della Madonnina tickets are highway robbery at these prices 💸",
    "Milan DNA is real. Even the youth team plays beautiful football.",
    "Another clean sheet. Maignan is a wall. 🧤",
    "That pass from Bennacer was pure filth. Midfield maestro.",
    "Hot take: this Milan squad is better than the 2007 CL team. Fight me.",
]

TWEETS_INTER = [
    "Lautaro Martinez is the best striker in Serie A, no question 🐂",
    "Nerazzurri on fire tonight! Forza Inter! 🖤💙",
    "Inter dominating the midfield, Barella is a machine",
    "San Siro is OUR home. Derby della Madonnina belongs to us.",
    "Inzaghi masterclass tonight, tactical perfection",
    "Second star on the chest and we're not stopping here ⭐⭐",
]

TWEETS_JUVENTUS = [
    "Fino alla fine! Juve never gives up 🤍🖤",
    "Vlahovic finally showing his worth. What a striker.",
    "Allianz Stadium rocking tonight, Juve faithful always show up",
    "La Vecchia Signora is back. Scudetto race is ON.",
    "Juve DNA runs deep. We always find a way to win.",
]

TWEETS_NAPOLI = [
    "Forza Napoli sempre! Maradona is watching over us 💙",
    "Stadio Diego Armando Maradona erupting tonight. Incredible atmosphere.",
    "Napoli playing the best football in Italy right now, no debate.",
    "Partenopei on a roll! Scudetto defense looking strong 🏆",
    "Kvara is magic. Napoli have found their next legend.",
]

TWEETS_ROMA = [
    "Daje Roma! The Olimpico is bouncing tonight 🟡🔴",
    "Roma fighting for every ball, this is what Romanisti want to see.",
    "Curva Sud in full voice, nobody matches our passion.",
    "AS Roma till I die. Through thick and thin. Forza Roma!",
    "The Eternal City deserves a trophy. This could be our year.",
]

TWEETS_FOOTBALL = [
    "3 points! We keep climbing. Scudetto is not a dream anymore 🏆",
    "VAR ruining football yet again. That was a clear penalty, embarrassing.",
    "Primavera kids looking incredible. Future is bright 🌟",
    "Can someone explain that offside call to me? Genuinely confused.",
    "Serie A title race is absolutely insane this year. 4 teams in it.",
    "Transfer rumors heating up... we NEED a proper striker in January",
    "Match thread: who's watching? Drop your predictions below ⬇️",
    "DAZN stream buffering at the worst possible moment. Every. Single. Time.",
    "The away section was bouncing today. Best fans in Italy and it's not close.",
    "Half time thoughts: we're dominating possession but need to be clinical",
    "Full time whistle. 3 points in the bag. Avanti così! 💪",
    "Anyone else think the ref was shocking tonight or just me?",
    "Just watched the tactical breakdown of our 4-2-3-1. Chef's kiss.",
    "Genuinely can't focus on work after that late winner last night",
    "Watching highlights for the 5th time and still getting chills",
    "New kit just dropped and it's actually gorgeous for once 😍",
]

TWEETS_WEATHER = [
    "Rain forecast for match day, typical Milano weather ☔",
    "Beautiful sunny day in Milan, perfect for football ☀️",
    "Freezing cold at San Siro tonight, bring layers 🥶",
    "Fog rolling in across the pitch, classic Lombardy winter",
    "Hailstorm delay at the stadium, fans soaked but still singing",
    "Clear skies for the derby, couldn't ask for better conditions",
    "Wind making every long ball unpredictable tonight 💨",
    "Snowing in Milano, San Siro looks magical under the floodlights ❄️",
]

TWEETS_NOISE = [
    "Ferrari's new car looks absolutely mental. Can't wait for the season 🏎️",
    "Milan Fashion Week never disappoints. The Prada show was stunning.",
    "Another day, another AI startup raising millions. When does the bubble pop?",
    "This espresso from the bar downstairs might be the best I've ever had ☕",
    "Snow in Milan tomorrow apparently. Time to break out the big coat 🥶",
    "Just tried that new restaurant in Navigli. Overpriced but decent pasta.",
    "The commute on ATM today was absolutely brutal. 40 mins for 3 stops.",
    "Remote work is great until your wifi dies during the all-hands 💀",
    "Italian cinema is having a moment and nobody is talking about it",
    "Scooter almost took me out on Via Torino. Classic Milano.",
    "Venice Film Festival lineup looks incredible this year",
    "GDP numbers looking up. Maybe we can afford those match tickets after all 😅",
]


def _generate_handles(n: int) -> list[str]:
    """Create n unique-ish Twitter handles."""
    handles = set()
    while len(handles) < n:
        prefix = random.choice(HANDLE_PARTS_PREFIX)
        suffix = random.choice(HANDLE_PARTS_SUFFIX)
        handle = f"@{prefix}{suffix}"
        handles.add(handle)
    return list(handles)


def generate_sentiment():
    items = []
    ts_start = datetime(2025, 8, 1)
    ts_end = datetime(2026, 2, 15)

    # Generate ~110 random users, each tweeting 2-3 times -> ~275-330 tweets
    num_users = 115
    handles = _generate_handles(num_users)

    tweet_id_counter = 1
    for handle in handles:
        num_tweets = random.choice([2, 2, 2, 3, 3])  # 2-3 tweets per user

        # Pick a loose "vibe" for this user — mostly football, some noise
        user_is_football_fan = random.random() < 0.75

        for _ in range(num_tweets):
            # Generate id, occasionally duplicate
            if random.random() < 0.03 and len(items) > 10:
                item_id = random.choice(items[-10:])["id"]
            else:
                item_id = f"SENT-{tweet_id_counter:05d}"
            tweet_id_counter += 1

            # Topic: football fans mostly tweet about football, but not always
            if user_is_football_fan:
                topic = random.choice(TOPICS) if random.random() < 0.85 else random.choice(
                    ["F1 Racing", "Fashion Week", "Tech News", "Random"])
            else:
                topic = random.choice(
                    ["F1 Racing", "Fashion Week", "Tech News", "Random"]) if random.random() < 0.7 else random.choice(TOPICS)

            source = random.choice(SOURCES)
            ts = rand_date(ts_start, ts_end)

            is_football = topic not in ("F1 Racing", "Fashion Week", "Tech News", "Random", "Weather")
            is_weather = topic == "Weather"

            if is_weather:
                tweet_text = random.choice(TWEETS_WEATHER)
            elif topic == "AC Milan":
                tweet_text = random.choice(TWEETS_AC_MILAN)
            elif topic == "Inter Milan":
                tweet_text = random.choice(TWEETS_INTER)
            elif topic == "Juventus":
                tweet_text = random.choice(TWEETS_JUVENTUS)
            elif topic == "Napoli":
                tweet_text = random.choice(TWEETS_NAPOLI)
            elif topic == "Roma":
                tweet_text = random.choice(TWEETS_ROMA)
            elif is_football:
                tweet_text = random.choice(TWEETS_FOOTBALL)
            else:
                tweet_text = random.choice(TWEETS_NOISE)

            # Sentiment score
            if is_weather:
                sentiment = round(random.uniform(-0.5, 0.8), 3)
            elif is_football:
                sentiment = round(random.uniform(-0.5, 0.9), 3)
            else:
                sentiment = round(random.uniform(-0.3, 0.7), 3)

            # Tags
            tags = []
            if is_weather:
                tags.append("weather")
            elif is_football:
                tags.append("football")
                if topic == "AC Milan":
                    tags.append("ac_milan")
                elif topic == "Inter Milan":
                    tags.append("inter")
                if topic == "Juventus":
                    tags.append("juventus")
                if "Serie A" in topic:
                    tags.append("serie_a")
                if "Champions" in topic:
                    tags.append("ucl")
                if "Transfer" in topic:
                    tags.append("transfers")
                if topic == "Napoli":
                    tags.append("napoli")
                if topic == "Roma":
                    tags.append("roma")
            else:
                tags.append(topic.lower().replace(" ", "_"))

            # Engagement
            engagement = {
                "likes": random.randint(0, 5000),
                "shares": random.randint(0, 1200),
                "comments": random.randint(0, 800),
            }

            # --- Messiness ---

            # ~10% missing sentiment_score
            if random.random() < 0.10:
                sentiment = None

            # ~8% engagement is null
            if random.random() < 0.08:
                engagement = None

            # Inconsistent published_at formats
            published_at = format_timestamp_messy(ts)

            # Mixed casing in topic (~15%)
            r = random.random()
            if r < 0.07:
                topic = topic.lower()
            elif r < 0.12:
                topic = topic.title()

            # ~5% empty tweet text
            if random.random() < 0.05:
                tweet_text = ""

            items.append({
                "id": item_id,
                "user": handle,
                "source": source,
                "text": tweet_text,
                "published_at": published_at,
                "topic": topic,
                "tags": tags,
                "sentiment_score": sentiment,
                "engagement": engagement,
            })

    random.shuffle(items)

    path = os.path.join(OUT_DIR, "sentiment.json")
    with open(path, "w") as f:
        json.dump(items, f, indent=2)

    print(f"  sentiment.json  -> {len(items)} items ({num_users} users) written to {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Generating fixture data...")
    customers = generate_customers()
    generate_transactions(customers)
    generate_sentiment()
    print("Done!")
