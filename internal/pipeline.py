"""
Data ingestion from raw CSV/JSON files into staging tables.
"""

import json
import pandas as pd
import duckdb
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import logging

from config import (
    CUSTOMERS_FILE,
    TRANSACTIONS_FILE,
    SENTIMENT_FILE,
    DB_PATH,
)
from schema import (
    CREATE_STAGING_CUSTOMERS,
    CREATE_STAGING_TRANSACTIONS,
    CREATE_STAGING_SENTIMENT,
)

logger = logging.getLogger(__name__)


class DataIngestor:
    """Handles loading raw data into DuckDB staging tables."""

    def __init__(self):
        self.conn = duckdb.connect(str(DB_PATH))
        self.issues = {"customers": [], "transactions": [], "sentiment": []}
        self.row_counts = {"customers": 0, "transactions": 0, "sentiment": 0}

    def ingest_all(self) -> Dict:
        """Load all data sources and return summary."""
        logger.info("Starting data ingestion...")

        # Create staging tables
        self.conn.execute(CREATE_STAGING_CUSTOMERS)
        self.conn.execute(CREATE_STAGING_TRANSACTIONS)
        self.conn.execute(CREATE_STAGING_SENTIMENT)

        # Ingest each source
        self.ingest_customers()
        self.ingest_transactions()
        self.ingest_sentiment()

        logger.info("Ingestion complete")
        return self.get_summary()

    def ingest_customers(self):
        """Load customers.csv into raw_customers."""
        logger.info(f"Loading {CUSTOMERS_FILE}...")

        try:
            df = pd.read_csv(CUSTOMERS_FILE)
            logger.info(f"  Loaded {len(df)} rows")

            # Add row IDs
            df.insert(0, "_row_id", range(len(df)))

            # Select only columns that match the schema
            cols = [c for c in df.columns if c in [
                "_row_id", "customer_id", "name", "email", "age", 
                "city", "country", "signup_date", "favorite_team", "membership_tier", "gender"
            ]]
            
            # Insert into DuckDB
            self.conn.execute(f"INSERT INTO raw_customers ({', '.join(cols)}) SELECT {', '.join(cols)} FROM df")
            self.row_counts["customers"] = len(df)

        except Exception as e:
            logger.error(f"Error loading customers: {e}")
            self.issues["customers"].append(f"Failed to load: {str(e)}")

    def ingest_transactions(self):
        """Load transactions.csv into raw_transactions."""
        logger.info(f"Loading {TRANSACTIONS_FILE}...")

        try:
            df = pd.read_csv(TRANSACTIONS_FILE)
            logger.info(f"  Loaded {len(df)} rows")

            # Add row IDs
            df.insert(0, "_row_id", range(len(df)))

            # Select only columns that match the schema
            cols = [c for c in df.columns if c in [
                "_row_id", "transaction_id", "customer_id", "timestamp", "amount",
                "currency", "category", "merchant", "description"
            ]]
            
            # Insert into DuckDB
            self.conn.execute(f"INSERT INTO raw_transactions ({', '.join(cols)}) SELECT {', '.join(cols)} FROM df")
            self.row_counts["transactions"] = len(df)

        except Exception as e:
            logger.error(f"Error loading transactions: {e}")
            self.issues["transactions"].append(f"Failed to load: {str(e)}")

    def ingest_sentiment(self):
        """Load sentiment.json into raw_sentiment."""
        logger.info(f"Loading {SENTIMENT_FILE}...")

        try:
            with open(SENTIMENT_FILE, "r") as f:
                data = json.load(f)

            # Convert to DataFrame
            records = []
            for i, item in enumerate(data):
                records.append({
                    "_row_id": i,
                    "id": item.get("id"),
                    "user": item.get("user"),
                    "source": item.get("source"),
                    "text": item.get("text"),
                    "published_at": item.get("published_at"),
                    "topic": item.get("topic"),
                    "tags": json.dumps(item.get("tags", [])),
                    "sentiment_score": item.get("sentiment_score"),
                    "engagement": item.get("engagement"),
                })

            df = pd.DataFrame(records)
            logger.info(f"  Loaded {len(df)} records")

            # Select only columns that match the schema
            cols = [c for c in df.columns if c in [
                "_row_id", "id", "user", "source", "text", "published_at",
                "topic", "tags", "sentiment_score", "engagement"
            ]]
            
            # Insert into DuckDB
            self.conn.execute(f"INSERT INTO raw_sentiment ({', '.join(cols)}) SELECT {', '.join(cols)} FROM df")
            self.row_counts["sentiment"] = len(df)

        except Exception as e:
            logger.error(f"Error loading sentiment: {e}")
            self.issues["sentiment"].append(f"Failed to load: {str(e)}")

    def get_summary(self) -> Dict:
        """Return ingestion summary."""
        return {
            "timestamp": datetime.now().isoformat(),
            "status": "success",
            "row_counts": self.row_counts,
            "issues": self.issues,
        }

    def close(self):
        """Close database connection."""
        self.conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingestor = DataIngestor()
    summary = ingestor.ingest_all()
    ingestor.close()
    print(json.dumps(summary, indent=2))
