"""
Configuration and constants for the reference solution.
"""

import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "../data"
OUTPUT_DIR = BASE_DIR / "output"
QUERIES_DIR = BASE_DIR / "queries"

OUTPUT_DIR.mkdir(exist_ok=True)

# Database
DB_PATH = OUTPUT_DIR / "duckdb.db"

# Data sources
CUSTOMERS_FILE = DATA_DIR / "customers.csv"
TRANSACTIONS_FILE = DATA_DIR / "transactions.csv"
SENTIMENT_FILE = DATA_DIR / "sentiment.json"

# Validation thresholds
NULL_RATE_WARNING = 0.3  # Warn if > 30% nulls
MIN_EXPECTED_CUSTOMERS = 190  # Expect ~200, warn if < 190
MIN_EXPECTED_TRANSACTIONS = 2400  # Expect ~2500, warn if < 2400
MAX_TRANSACTION_AMOUNT = 50000
MIN_TRANSACTION_AMOUNT = -1000

# Date ranges for freshness checks
MIN_DATE = "2020-01-01"
MAX_DATE = "2026-12-31"
