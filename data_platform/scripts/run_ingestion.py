"""Run CSV-to-PostgreSQL ingestion using schema-driven loader."""

import os
import logging
from data_platform.clients.postgres_client import PostgresClient
from data_platform.ingestion.postgres_ingestion_csv import (
    PostgresIngestionCSV,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

ingestion_data_path_dict = {
    "parking_violation_codes": {
        "schema_path": "data_platform/data/schemas/parking_violation_codes.json",
        "csv_path": "data_platform/data/clean_data/dof_parking_violation_codes.csv",
    },
    "parking_violations_issued": {
        "schema_path": "data_platform/data/schemas/parking_violations_issued.json",
        "csv_path": "data_platform/data/clean_data/parking_violations_issued_fiscal_year_2023_sample.csv",
    }
}


def run_ingestions():
    """Run ingestion for all configured datasets."""
    for table_name, paths in ingestion_data_path_dict.items():
        schema_path = paths["schema_path"]
        csv_path = paths["csv_path"]

        if not os.path.exists(schema_path) or not os.path.exists(csv_path):
            logger.error(
                f"Missing input for {table_name} "
                f"(schema: {schema_path}, csv: {csv_path})"
            )
            raise SystemExit(2)

        logger.info(f"Starting ingestion for {table_name}")
        try:
            PostgresIngestionCSV().ingest(
                csv_path=csv_path,
                schema_path=schema_path,
            )
            logger.info(f"Completed ingestion for {table_name}")
        except Exception:
            logger.exception(f"Ingestion failed for {table_name}")
            raise SystemExit(1)


def run_postgres_check():
    """Run the provided PostgreSQL query and log the DataFrame result."""
    try:
        pg = PostgresClient()
        df = pg.execute_query(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
            """,
            return_pd_dataframe=True,
        )
        logger.info(f"Found {len(df)} public tables")
        logger.info(f"Public tables dataframe:\n{df.to_string(index=False)}")
    except Exception:
        logger.exception("PostgreSQL post-check failed")
        raise SystemExit(1)


def main():
    run_ingestions()
    run_postgres_check()


if __name__ == "__main__":
    main()
