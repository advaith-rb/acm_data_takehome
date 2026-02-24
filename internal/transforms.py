"""
Data transformation from raw staging tables to clean fact/dimension tables.
"""

import duckdb
from datetime import datetime
from typing import Dict
import logging

from config import DB_PATH
from schema import (
    CREATE_DIM_CUSTOMERS,
    CREATE_FACT_TRANSACTIONS,
    CREATE_FACT_SENTIMENT,
    CREATE_CUSTOMER_PROFILE,
)

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms raw data into clean fact and dimension tables."""

    def __init__(self):
        self.conn = duckdb.connect(str(DB_PATH))
        self.report = {"timestamp": datetime.now().isoformat(), "steps": []}

    def transform_all(self) -> Dict:
        """Run the full transformation pipeline."""
        logger.info("Starting data transformation...")

        self.create_tables()
        self.load_dim_customers()
        self.load_fact_transactions()
        self.load_fact_sentiment()
        self.load_customer_profile()

        logger.info("Transformation complete")
        return self.report

    def create_tables(self):
        """Create target table schemas."""
        logger.info("Creating target tables...")
        self.conn.execute(CREATE_DIM_CUSTOMERS)
        self.conn.execute(CREATE_FACT_TRANSACTIONS)
        self.conn.execute(CREATE_FACT_SENTIMENT)
        self.conn.execute(CREATE_CUSTOMER_PROFILE)
        self.report["steps"].append("Tables created")

    def load_dim_customers(self):
        """Load and deduplicate customers into dim_customers."""
        logger.info("Loading dim_customers...")

        # Dedup and normalize
        sql = """
        INSERT INTO dim_customers
        WITH deduped AS (
          SELECT DISTINCT ON (customer_id) *
          FROM raw_customers
          WHERE customer_id IS NOT NULL
          ORDER BY customer_id, _load_timestamp
        )
        SELECT
          customer_id,
          LOWER(TRIM(COALESCE(name, 'Unknown'))) as name,
          LOWER(TRIM(email)) as email,
          TRY_CAST(age AS INTEGER) as age,
          LOWER(TRIM(city)) as city,
          country,
          LOWER(TRIM(favorite_team)) as favorite_team,
          LOWER(TRIM(membership_tier)) as membership_tier,
          TRY_CAST(signup_date AS DATE) as signup_date,
          CURRENT_TIMESTAMP
        FROM deduped;
        """

        try:
            self.conn.execute(sql)
            row_count = self.conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
            self.report["steps"].append(f"dim_customers: {row_count} rows")
            logger.info(f"  Inserted {row_count} rows")
        except Exception as e:
            logger.error(f"Error loading dim_customers: {e}")
            self.report["steps"].append(f"ERROR in dim_customers: {str(e)}")

    def load_fact_transactions(self):
        """Load and clean transactions into fact_transactions."""
        logger.info("Loading fact_transactions...")

        sql = """
        INSERT INTO fact_transactions
        WITH deduped AS (
          SELECT DISTINCT ON (transaction_id) *
          FROM raw_transactions
          WHERE transaction_id IS NOT NULL
          ORDER BY transaction_id, _row_id
        )
        SELECT
          transaction_id,
          customer_id,
          COALESCE(
            TRY_CAST(timestamp AS TIMESTAMP),
            CURRENT_TIMESTAMP
          ) as transaction_date,
          TRY_CAST(REPLACE(amount, ',', '.') AS DECIMAL(10, 2)) as amount_eur,
          LOWER(TRIM(category)) as category,
          merchant,
          _row_id,
          CURRENT_TIMESTAMP
        FROM deduped
        WHERE customer_id IS NOT NULL
          AND customer_id IN (SELECT customer_id FROM dim_customers)
          AND TRY_CAST(amount AS DECIMAL(10, 2)) IS NOT NULL
          AND TRY_CAST(amount AS DECIMAL(10, 2)) > -1000
          AND TRY_CAST(amount AS DECIMAL(10, 2)) < 50000;
        """

        try:
            self.conn.execute(sql)
            row_count = self.conn.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
            self.report["steps"].append(f"fact_transactions: {row_count} rows")
            logger.info(f"  Inserted {row_count} rows")
        except Exception as e:
            logger.error(f"Error loading fact_transactions: {e}")
            self.report["steps"].append(f"ERROR in fact_transactions: {str(e)}")

    def load_fact_sentiment(self):
        """Load and clean sentiment posts into fact_sentiment."""
        logger.info("Loading fact_sentiment...")

        sql = """
        INSERT INTO fact_sentiment
        WITH deduped AS (
          SELECT DISTINCT ON (id) *
          FROM raw_sentiment
          WHERE id IS NOT NULL
          ORDER BY id, _load_timestamp
        )
        SELECT
          id as post_id,
          LOWER(TRIM(user)) as user_name,
          LOWER(TRIM(topic)) as topic,
          TRY_CAST(sentiment_score AS DECIMAL(3, 2)) as sentiment_score,
          TRY_CAST(engagement AS INTEGER) as engagement,
          TRY_CAST(published_at AS TIMESTAMP) as published_at,
          _row_id,
          CURRENT_TIMESTAMP
        FROM deduped
        WHERE id IS NOT NULL;
        """

        try:
            self.conn.execute(sql)
            row_count = self.conn.execute("SELECT COUNT(*) FROM fact_sentiment").fetchone()[0]
            self.report["steps"].append(f"fact_sentiment: {row_count} rows")
            logger.info(f"  Inserted {row_count} rows")
        except Exception as e:
            logger.error(f"Error loading fact_sentiment: {e}")
            self.report["steps"].append(f"ERROR in fact_sentiment: {str(e)}")

    def load_customer_profile(self):
        """Load pre-aggregated customer profiles."""
        logger.info("Loading customer_profile...")

        sql = """
        INSERT INTO customer_profile
        SELECT
          c.customer_id,
          COUNT(DISTINCT t.transaction_id) as txn_count,
          ROUND(SUM(t.amount_eur), 2) as total_spend,
          ROUND(AVG(t.amount_eur), 2) as avg_txn,
          MAX(DATE(t.transaction_date)) as last_txn_date,
          COUNT(DISTINCT CASE WHEN t.category = 'match_tickets' THEN t.transaction_id END) as match_ticket_count,
          ROUND(
            CAST(
              COUNT(DISTINCT CASE WHEN t.category LIKE '%sports%' OR t.category = 'match_tickets' THEN t.transaction_id END)
              AS DECIMAL
            ) / NULLIF(COUNT(DISTINCT t.transaction_id), 0),
            2
          ) as sports_affinity_ratio,
          ROUND(
            CAST(
              (MAX(DATE(t.transaction_date)) - MIN(DATE(t.transaction_date)))
              AS DECIMAL
            ) / NULLIF(COUNT(DISTINCT t.transaction_id) - 1, 0),
            1
          ) as avg_days_between_txns,
          CURRENT_TIMESTAMP
        FROM dim_customers c
        LEFT JOIN fact_transactions t ON c.customer_id = t.customer_id
        GROUP BY c.customer_id;
        """

        try:
            self.conn.execute(sql)
            row_count = self.conn.execute("SELECT COUNT(*) FROM customer_profile").fetchone()[0]
            self.report["steps"].append(f"customer_profile: {row_count} rows")
            logger.info(f"  Inserted {row_count} rows")
        except Exception as e:
            logger.error(f"Error loading customer_profile: {e}")
            self.report["steps"].append(f"ERROR in customer_profile: {str(e)}")

    def close(self):
        """Close database connection."""
        self.conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    transformer = DataTransformer()
    report = transformer.transform_all()
    transformer.close()
    print(report)
