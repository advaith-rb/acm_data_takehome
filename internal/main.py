"""
Main orchestration script: runs the full pipeline.
"""

import logging
import json
import sys
from pathlib import Path
from datetime import datetime

from config import OUTPUT_DIR
from pipeline import DataIngestor
from validation import DataValidator
from transforms import DataTransformer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Run the complete data pipeline."""
    logger.info("=" * 80)
    logger.info("ANALYTICS DATA PIPELINE - CUSTOMER TRANSACTION PLATFORM")
    logger.info("=" * 80)

    pipeline_report = {
        "pipeline_run_id": f"run-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "timestamp": datetime.now().isoformat(),
        "stages": {},
    }

    try:
        # Stage 1: Ingestion
        logger.info("\n[STAGE 1] Data Ingestion")
        logger.info("-" * 80)
        ingestor = DataIngestor()
        ingest_summary = ingestor.ingest_all()
        ingestor.close()

        pipeline_report["stages"]["ingestion"] = ingest_summary
        logger.info(f"✓ Ingestion complete: {ingest_summary['row_counts']}")

        # Stage 2: Raw Data Validation
        logger.info("\n[STAGE 2] Raw Data Validation")
        logger.info("-" * 80)
        validator1 = DataValidator()
        raw_validation = validator1.validate_raw_data()
        validator1.close()

        pipeline_report["stages"]["raw_validation"] = raw_validation
        logger.info("✓ Raw data validation complete")

        # Stage 3: Transformation
        logger.info("\n[STAGE 3] Data Transformation")
        logger.info("-" * 80)
        transformer = DataTransformer()
        transform_report = transformer.transform_all()
        transformer.close()

        pipeline_report["stages"]["transformation"] = transform_report
        logger.info("✓ Transformation complete")

        # Stage 4: Transformed Data Validation
        logger.info("\n[STAGE 4] Transformed Data Validation")
        logger.info("-" * 80)
        validator2 = DataValidator()
        transformed_validation = validator2.validate_transformed_data()
        validator2.generate_report()
        validator2.close()

        pipeline_report["stages"]["transformed_validation"] = transformed_validation
        logger.info("✓ Transformed data validation complete")

        # Write pipeline report
        report_path = OUTPUT_DIR / "pipeline_report.json"
        with open(report_path, "w") as f:
            json.dump(pipeline_report, f, indent=2, default=str)

        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE EXECUTION SUCCESSFUL")
        logger.info("=" * 80)
        logger.info(f"Database: {OUTPUT_DIR / 'duckdb.db'}")
        logger.info(f"Reports: {OUTPUT_DIR / 'validation_report.json'}")
        logger.info(f"         {OUTPUT_DIR / 'pipeline_report.json'}")
        logger.info("\nTo query the data:")
        logger.info("  python -c \"import duckdb; conn = duckdb.connect('output/duckdb.db');")
        logger.info("             print(conn.execute('SELECT * FROM dim_customers LIMIT 5').df())\"")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error(f"\n✗ Pipeline failed: {e}", exc_info=True)
        pipeline_report["status"] = "failed"
        pipeline_report["error"] = str(e)

        report_path = OUTPUT_DIR / "pipeline_report.json"
        with open(report_path, "w") as f:
            json.dump(pipeline_report, f, indent=2, default=str)

        return 1


if __name__ == "__main__":
    sys.exit(main())
