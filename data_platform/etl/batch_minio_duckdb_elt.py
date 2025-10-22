import logging
from typing import Optional, Dict
from data_platform.clients.minio_client import MinioClient
from data_platform.clients.duckdb_client import DuckdbClient


class BatchMinioDuckdbELT:
    """
    Extract the latest Avro object for a logical table from MinIO, then Load
    into DuckDB.
    """

    def __init__(
            self,
            db_path: str = "data_platform/lakehouse/lakehouse.db",
            bucket_name: str = "raw-data"
    ):
        self.database_path = db_path
        self.bucket_name = bucket_name
        self.minio = MinioClient()
        self.logger = logging.getLogger(__name__)

    def extract(self, table_name: str, root_prefix: str) -> Dict[str, str]:
        """
        Find latest Avro object for table under given root prefix.

        Args:
            table_name: logical table name
            root_prefix: root path (e.g., "postgres-exports")

        Returns:
            dict with source_table, latest_object_key, latest_s3_uri
        """
        self.logger.info(
            f"Extracting latest Avro for table '{table_name}' "
            f"from '{root_prefix}'"
            )

        all_object_keys = self.minio.list_objects(self.bucket_name)
        prefix = f"{root_prefix}/{table_name}/"
        candidate_keys = [
            k for k in all_object_keys
            if k.startswith(prefix) and k.endswith(".avro")
        ]

        if not candidate_keys:
            raise RuntimeError(
                f"No .avro files for table '{table_name}' under "
                f"'{root_prefix}' in bucket '{self.bucket_name}'"
                )

        latest_key = max(candidate_keys)
        self.logger.info(
            f"Found {len(candidate_keys)} candidates, latest: {latest_key}"
            )

        return {
            "source_table": table_name,
            "latest_object_key": latest_key,
            "latest_s3_uri": f"s3://{self.bucket_name}/{latest_key}",
        }

    def load(
            self,
            extract_meta: Dict[str, str],
            target_table: Optional[str] = None
            ):
        """
        Load latest Avro into DuckDB as staging.<target_table>.

        Args:
            extract_meta: dict from extract()
            target_table: target table name (defaults to source_table)
        """
        source_table = extract_meta["source_table"]
        latest_s3_uri = extract_meta["latest_s3_uri"]
        target_table_name = target_table or source_table

        self.logger.info(
            f"Loading '{latest_s3_uri}' into staging.{target_table_name}"
            )

        with DuckdbClient(db_path=self.database_path) as duck:
            self.logger.debug("Creating staging schema")
            duck.execute_query("CREATE SCHEMA IF NOT EXISTS staging;")

            self.logger.info(f"Creating table staging.{target_table_name}")
            duck.execute_query(f"""
                CREATE OR REPLACE TABLE staging.{target_table_name} AS
                SELECT * FROM read_avro('{latest_s3_uri}');
            """)

            row_count = duck.execute_query(
                f"SELECT COUNT(*) AS count FROM staging.{target_table_name};",
                return_pd_dataframe=True
            )["count"].iloc[0]

            self.logger.info(
                f"Successfully loaded {row_count} rows into "
                f"staging.{target_table_name}"
                )
