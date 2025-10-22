import fastavro
import json
import logging
import os

from data_platform.etl.batch_postgres_minio_etl import BatchPostgresMinioETL


class BatchPostgresMinioWriteAuditPublishETL:
    def __init__(self, bucket_name: str, schema_directory: str):
        self.logger = logging.getLogger(__name__)
        self.bucket_name = bucket_name
        self.schema_directory = schema_directory
        self.etl = BatchPostgresMinioETL(schema_directory)

    def run_wap(self):
        tables = self.etl._get_public_tables()
        self.logger.info(f"Starting Write-Audit-Publish for {len(tables)} tables: {tables}")

        for table in tables:
            self.logger.info(f"Preparing staging and quarantine tables for '{table}'")
            self._create_quarantine_table(table)
            self._create_staging_table(table)

            data, columns = self.etl.extract(table)
            schema = self.etl._infer_avro_schema(table)
            primary_key = self._get_primary_key(table)

            self.logger.info(f"Extracted {len(data)} rows from '{table}'")

            for row in data:
                record = dict(zip(columns, row))
                record = self.etl._convert_timestamps_to_millis(record)
                try:
                    fastavro.validate(record, schema)
                except Exception:
                    self._insert_into_quarantine(table, record, primary_key)

            self.logger.info(f"Populating staging table: '{table}'")
            self._populate_staging_excluding_quarantine(table, primary_key)
            self.logger.info(f"Finished '{table}'")

        self.logger.info("Write-Audit-Publish run complete")
    
    def run_etl(self):
        tables = self._get_staging_tables()
        self.logger.info(f"Starting ETL for {len(tables)} tables: {tables}")

        for table in tables:
            self.logger.info(f"Processing table: {table}")
            data, columns = self.etl.extract(table)

            # use base-table schema for transform
            logical_name = table
            for prefix in ("staging_", "quarantine_"):
                if logical_name.startswith(prefix):
                    logical_name = logical_name[len(prefix):]
                    break

            transformed_data = self.etl.transform(data, columns, logical_name)

            # keep MinIO object name as the staging/quarantine table
            self.etl.load(transformed_data, self.bucket_name, table)
            self.logger.info(f"Successfully processed table: {table}")

        self.logger.info("ETL run complete")
    
    # def run(self):
    #     self.run_etl()
    #     self.run_wap()
    
    # Quality Checks
    def avro_compatibility_check(self, table_name, record, schema, primary_key):
        try:
            fastavro.validate(record, schema)
        except Exception:
            self._insert_into_quarantine(table_name, record, primary_key)
    
    #########################################
    # ADD MORE QUALITY CHECK FUNCTIONS HERE #
    #########################################

    # Utility Functions
    def _get_primary_key(self, table_name):
        schema_file = os.path.join(self.schema_directory, f"{table_name}.json")
        with open(schema_file, 'r') as f:
            schema = json.load(f)
        
        # Look through the columns array to find the one with primary_key: true
        for column in schema.get('columns', []):
            if column.get('primary_key', False):
                return column['name']
        
        return None  # No primary key found

    def _create_quarantine_table(self, table_name):
        primary_key = self._get_primary_key(table_name)
        query = f"""
        CREATE TABLE IF NOT EXISTS quarantine_{table_name} (
            LIKE {table_name} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_quarantine_{table_name}_{primary_key}
            ON quarantine_{table_name} ({primary_key});
        """
        self.etl.postgres.execute_query(query)

    def _create_staging_table(self, table_name):
        primary_key = self._get_primary_key(table_name)
        query = f"""
        CREATE TABLE IF NOT EXISTS staging_{table_name} (
            LIKE {table_name} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_staging_{table_name}_{primary_key}
            ON staging_{table_name} ({primary_key});
        """
        self.etl.postgres.execute_query(query)

    def _insert_into_quarantine(self, table_name, record, primary_key):
        primary_value = record[primary_key]
        
        query = f"""
        INSERT INTO quarantine_{table_name} 
        SELECT * FROM {table_name} WHERE {primary_key} = '{primary_value}'
        ON CONFLICT ({primary_key}) DO NOTHING
        """
        self.etl.postgres.execute_query(query)

    def _populate_staging_excluding_quarantine(self, table_name, primary_key):
        query = f"""
        INSERT INTO staging_{table_name}
        SELECT {table_name}.*
        FROM {table_name}
        LEFT JOIN quarantine_{table_name}
        ON quarantine_{table_name}.{primary_key} = {table_name}.{primary_key}
        WHERE quarantine_{table_name}.{primary_key} IS NULL
        ON CONFLICT ({primary_key}) DO NOTHING;
        """
        self.etl.postgres.execute_query(query)
    
    def _get_staging_tables(self):
        self.logger.info("Querying PostgreSQL for staging tables")
        query = """
        SELECT
            table_name
        FROM
            information_schema.tables
        WHERE
            table_schema = 'public' AND
            table_name LIKE 'staging_%'
        """
        data, _ = self.etl.postgres.execute_query(query)

        if not data:
            raise ValueError("No tables found in postgres")

        table_names = [row[0] for row in data]
        return table_names
