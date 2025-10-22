"""Run combined ETL (Postgres→MinIO) and ELT (MinIO→DuckDB)."""

import os
import logging
from data_platform.etl.batch_postgres_minio_etl import (
    BatchPostgresMinioETL,
)
from data_platform.etl.batch_minio_duckdb_elt import (
    BatchMinioDuckdbELT,
)
from data_platform.clients.minio_client import MinioClient
from data_platform.clients.duckdb_client import DuckdbClient

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SCHEMA_DIR = "data_platform/data/schemas"
MINIO_BUCKET = "raw-data"
ROOT_PREFIX = "postgres-exports"
ELT_TABLES = [
    "parking_violation_codes",
    "parking_violations_issued"
]


def run_postgres_to_minio_etl():
    """Run Postgres→MinIO ETL with schema dir validation."""
    if not os.path.isdir(SCHEMA_DIR):
        logger.error(f"Schema dir not found: {SCHEMA_DIR}")
        raise SystemExit(2)

    try:
        logger.info(
            f"Starting ETL: Postgres→MinIO (bucket={MINIO_BUCKET})"
        )
        etl = BatchPostgresMinioETL(schema_directory=SCHEMA_DIR)
        etl.run_etl(bucket_name=MINIO_BUCKET)
        logger.info("Completed ETL: Postgres→MinIO")

        minio_client = MinioClient()
        object_keys = sorted(minio_client.list_objects(MINIO_BUCKET))
        for object_key in object_keys:
            logger.info(f"minio:{MINIO_BUCKET}/{object_key}")
    except Exception:
        logger.exception("ETL failed")
        raise SystemExit(1)


def run_minio_to_duckdb_elt():
    """Run MinIO→DuckDB ELT for configured tables."""
    try:
        elt = BatchMinioDuckdbELT()
        for table in ELT_TABLES:
            meta = elt.extract(table_name=table, root_prefix=ROOT_PREFIX)
            elt.load(extract_meta=meta, target_table=table)
            logger.info(f"Completed ELT for table={table}")

        # DuckDB staging table check
        with DuckdbClient(
            db_path="data_platform/lakehouse/lakehouse.db"
        ) as duck:
            df = duck.execute_query(
                """
                    SELECT
                        database,
                        schema,
                        name
                    FROM
                        (
                            SHOW ALL TABLES
                        )
                """,
                return_pd_dataframe=True,
            )
            logger.info(f"DuckDB staging tables:\n{df}")
    except Exception:
        logger.exception("ELT failed")
        raise SystemExit(1)


def main():
    run_postgres_to_minio_etl()
    run_minio_to_duckdb_elt()


if __name__ == "__main__":
    main()
