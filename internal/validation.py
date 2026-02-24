"""
Data quality validation and reporting.
"""

import json
import duckdb
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import logging

from config import DB_PATH, NULL_RATE_WARNING, OUTPUT_DIR

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates raw and transformed data."""

    def __init__(self):
        self.conn = duckdb.connect(str(DB_PATH))
        self.report = {
            "timestamp": datetime.now().isoformat(),
            "stages": {},
            "issues": [],
        }

    def validate_raw_data(self) -> Dict:
        """Validate raw staging tables."""
        logger.info("Validating raw data...")

        validation = {
            "customers": self._validate_table("raw_customers"),
            "transactions": self._validate_table("raw_transactions"),
            "sentiment": self._validate_table("raw_sentiment"),
        }

        # Check for duplicates
        validation["customers"]["duplicates"] = self._find_duplicates(
            "raw_customers", "customer_id"
        )
        validation["transactions"]["duplicates"] = self._find_duplicates(
            "raw_transactions", "transaction_id"
        )
        validation["sentiment"]["duplicates"] = self._find_duplicates(
            "raw_sentiment", "id"
        )

        # Check for orphan keys
        validation["transactions"]["orphan_keys"] = self._find_orphan_keys()

        self.report["raw_data"] = validation
        return validation

    def _validate_table(self, table_name: str) -> Dict:
        """Validate a single table: row count, null rates, types."""
        try:
            # Row count
            row_count = self.conn.execute(
                f"SELECT COUNT(*) as cnt FROM {table_name}"
            ).fetchone()[0]

            # Null rates
            result = self.conn.execute(
                f"SELECT * FROM {table_name} LIMIT 1"
            ).description
            columns = [desc[0] for desc in result]

            null_rates = {}
            for col in columns:
                if col.startswith("_"):
                    continue
                null_count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL"
                ).fetchone()[0]
                null_pct = null_count / row_count if row_count > 0 else 0
                if null_pct > NULL_RATE_WARNING:
                    null_rates[col] = {
                        "null_count": null_count,
                        "null_rate": null_pct,
                        "warning": f"High null rate: {null_pct:.1%}",
                    }

            return {
                "row_count": row_count,
                "columns": columns,
                "high_null_columns": null_rates,
            }

        except Exception as e:
            logger.error(f"Error validating {table_name}: {e}")
            return {"error": str(e)}

    def _find_duplicates(self, table_name: str, key_col: str) -> Dict:
        """Find exact duplicate rows by key column."""
        try:
            result = self.conn.execute(
                f"""
                SELECT {key_col}, COUNT(*) as cnt
                FROM {table_name}
                WHERE {key_col} IS NOT NULL
                GROUP BY {key_col}
                HAVING COUNT(*) > 1
                ORDER BY cnt DESC
                """
            ).fetchall()

            if result:
                return {
                    "found": True,
                    "count": len(result),
                    "duplicates": [{"key": r[0], "occurrences": r[1]} for r in result],
                }
            else:
                return {"found": False, "count": 0}

        except Exception as e:
            logger.error(f"Error finding duplicates in {table_name}: {e}")
            return {"error": str(e)}

    def _find_orphan_keys(self) -> Dict:
        """Find transactions with non-existent customer_id."""
        try:
            orphans = self.conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM raw_transactions t
                WHERE customer_id IS NOT NULL
                  AND customer_id NOT IN (SELECT customer_id FROM raw_customers)
                """
            ).fetchone()[0]

            return {
                "found": orphans > 0,
                "count": orphans,
                "note": "Transactions with non-existent customer_id" if orphans > 0 else "None",
            }

        except Exception as e:
            logger.error(f"Error finding orphan keys: {e}")
            return {"error": str(e)}

    def validate_transformed_data(self) -> Dict:
        """Validate clean output tables."""
        logger.info("Validating transformed data...")

        validation = {
            "dim_customers": self._validate_table("dim_customers"),
            "fact_transactions": self._validate_table("fact_transactions"),
            "customer_profile": self._validate_table("customer_profile"),
        }

        # Check referential integrity
        validation["fact_transactions"]["referential_integrity"] = self._check_foreign_keys()

        # Check uniqueness
        validation["dim_customers"]["customer_id_unique"] = (
            self.conn.execute(
                "SELECT COUNT(*) FROM dim_customers"
            ).fetchone()[0]
            == self.conn.execute(
                "SELECT COUNT(DISTINCT customer_id) FROM dim_customers"
            ).fetchone()[0]
        )

        self.report["transformed_data"] = validation
        return validation

    def _check_foreign_keys(self) -> Dict:
        """Check that all fact table FKs point to valid dimensions."""
        try:
            orphans = self.conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM fact_transactions t
                WHERE customer_id NOT IN (SELECT customer_id FROM dim_customers)
                """
            ).fetchone()[0]

            return {
                "valid": orphans == 0,
                "orphan_count": orphans,
                "note": "All foreign keys valid" if orphans == 0 else f"{orphans} orphan rows",
            }

        except Exception as e:
            return {"error": str(e)}

    def generate_report(self) -> str:
        """Write validation report to JSON file."""
        report_path = OUTPUT_DIR / "validation_report.json"
        with open(report_path, "w") as f:
            json.dump(self.report, f, indent=2, default=str)
        logger.info(f"Validation report written to {report_path}")
        return str(report_path)

    def close(self):
        """Close database connection."""
        self.conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    validator = DataValidator()
    validator.validate_raw_data()
    validator.validate_transformed_data()
    validator.generate_report()
    validator.close()
