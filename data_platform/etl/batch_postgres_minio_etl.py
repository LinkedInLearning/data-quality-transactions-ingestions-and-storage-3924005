import fastavro
import json
import os
import logging
from datetime import datetime
from data_platform.clients.postgres_client import PostgresClient
from data_platform.clients.minio_client import MinioClient


class BatchPostgresMinioETL:
    """
    Batch ETL pipeline for extracting data from PostgreSQL and loading to
    MinIO.

    This class provides functionality to extract data from all tables in the
    PostgreSQL public schema, transform it to Avro format with proper type
    mapping, and load it to MinIO with hierarchical path structure.

    Attributes:
        postgres (PostgresClient): PostgreSQL database client
        minio (MinioClient): MinIO object storage client
        schema_directory (str): Path to directory containing JSON schema files
        pg_timestamp_fields (list): List of PostgreSQL timestamp field names
        postgres_to_avro_mapping (dict): Mapping of PostgreSQL types to Avro
        types
    """
    def __init__(self, schema_directory: str):
        """
        Initialize the Batch ETL pipeline.

        Args:
            schema_directory (str): Path to directory containing JSON schema
                files for each table. Schema files should be named
                {table_name}.json and contain column definitions.
        """
        self.postgres = PostgresClient()
        self.minio = MinioClient()
        self.schema_directory = schema_directory
        self.pg_timestamp_fields = ['pg_created_at', 'pg_updated_at']
        self.logger = logging.getLogger(__name__)

        # PostgreSQL to Avro type mapping
        self.postgres_to_avro_mapping = {
            'INTEGER': 'long',
            'TEXT': 'string',
            'VARCHAR': 'string',
            'BOOLEAN': 'boolean',
            'FLOAT': 'double',
            'DOUBLE': 'double',
            'BIGINT': 'long',
            'DATE': {'type': 'int', 'logicalType': 'date'},
            'TIMESTAMP': {'type': 'long', 'logicalType': 'timestamp-millis'}
        }

    # ETL PIPELINE METHODS
    def extract(self, table_name):
        """
        Extract data from a PostgreSQL table.

        Args:
            table_name (str): Name of the table to extract data from

        Returns:
            tuple: (data, columns) where data is list of rows and columns is
                list of column names

        Raises:
            ValueError: If no data is found in the specified table
        """
        self.logger.info(f"Extracting data from table: {table_name}")
        query = f"SELECT * FROM {table_name}"
        data, columns = self.postgres.execute_query(query)

        if not data:
            raise ValueError(f"No data found in table {table_name}")

        self.logger.info(
            f"Successfully extracted {len(data)} rows from {table_name}"
            )
        return data, columns

    def transform(self, data, columns, table_name: str):
        """
        Transform PostgreSQL data to Avro format.

        Args:
            data (list): Raw data rows from PostgreSQL
            columns (list): Column names from PostgreSQL
            table_name (str): Name of the table being transformed

        Returns:
            tuple: (avro_records, schema) where avro_records is list of Avro
            records and schema is the Avro schema dictionary

        Raises:
            ValueError: If schema file is not found for the table
        """
        self.logger.info(
            f"Transforming {len(data)} records to Avro format for "
            f"table: {table_name}"
            )
        avro_records = []
        for row in data:
            record = dict(zip(columns, row))

            # Convert timestamp fields
            record = self._convert_timestamps_to_millis(record)

            avro_records.append(record)

        schema = self._infer_avro_schema(table_name)
        self.logger.info(
            f"Successfully transformed {len(avro_records)} records to"
            f"Avro format"
            )
        return avro_records, schema

    def load(self, data, bucket_name, object_name):
        """
        Load Avro data to MinIO object storage.

        Args:
            data (tuple): (avro_records, schema) from transform method
            bucket_name (str): MinIO bucket name to store the data
            object_name (str): Base name for the object (typically table name)
        """
        records, schema = data
        self.logger.info(
            f"Loading {len(records)} records to MinIO bucket: {bucket_name}"
            )

        # Create hierarchical path structure
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_path = datetime.now().strftime("%Y/%m/%d")

        object_key = (
            f"postgres-exports/{object_name}/{date_path}/"
            f"{object_name}_{timestamp}.avro"
        )
        avro_file = f"/tmp/{object_name}_{timestamp}.avro"

        try:
            self.logger.debug(f"Writing Avro file: {avro_file}")
            with open(avro_file, 'wb') as out:
                fastavro.writer(out, schema, records)

            self.logger.debug(f"Uploading to MinIO: {object_key}")
            self.minio.upload_file(bucket_name, object_key, avro_file)
            self.logger.info(
                f"Successfully uploaded {len(records)} records to {object_key}"
                )
        finally:
            # Clean up temporary file
            if os.path.exists(avro_file):
                os.remove(avro_file)
                self.logger.debug(f"Cleaned up temporary file: {avro_file}")

    def run_etl(self, bucket_name):
        """
        Run the complete ETL pipeline for all public tables.

        Args:
            bucket_name (str): MinIO bucket name to store the processed data

        Raises:
            ValueError: If no tables are found in the public schema
        """
        self.logger.info(f"Starting ETL pipeline for bucket: {bucket_name}")
        tables = self._get_public_tables()
        self.logger.info(f"Found {len(tables)} tables to process: {tables}")

        for table in tables:
            try:
                self.logger.info(f"Processing table: {table}")
                data, columns = self.extract(table)
                transformed_data = self.transform(data, columns, table)
                self.load(transformed_data, bucket_name, table)
                self.logger.info(f"Successfully processed table: {table}")
            except Exception as e:
                self.logger.error(f"Failed to process table {table}: {str(e)}")
                raise

        self.logger.info("ETL pipeline completed successfully")

    # SCHEMA METHODS
    def _load_table_schema(self, table_name: str):
        """
        Load table schema from JSON file.

        Args:
            table_name (str): Name of the table to load schema for

        Returns:
            dict or None: Schema dictionary if file exists, None otherwise
        """
        schema_file = os.path.join(self.schema_directory, f"{table_name}.json")
        self.logger.debug(f"Loading schema from: {schema_file}")

        if os.path.exists(schema_file):
            with open(schema_file, 'r') as f:
                schema = json.load(f)
            self.logger.debug(f"Successfully loaded schema for {table_name}")
            return schema
        else:
            self.logger.warning(f"Schema file not found: {schema_file}")
            return None

    def _infer_avro_schema(self, table_name: str):
        """
        Infer Avro schema from table definition and add timestamp fields.

        Args:
            table_name (str): Name of the table to create schema for

        Returns:
            dict: Avro schema dictionary

        Raises:
            ValueError: If schema file is not found for the table
        """
        self.logger.debug(f"Inferring Avro schema for table: {table_name}")
        table_schema = self._load_table_schema(table_name)

        if not table_schema:
            self.logger.error(
                f"Schema file not found for table '{table_name}'"
                )
            raise ValueError(
                f"Schema file not found for table '{table_name}'."
                f"Expected: {self.schema_directory}/{table_name}.json"
                )

        fields = []
        for col_def in table_schema['columns']:
            avro_type = self.postgres_to_avro_mapping.get(
                col_def['type'].upper(), 'string'
            )
            fields.append({'name': col_def['name'], 'type': avro_type})

        # Add automatic timestamp columns using the mapping
        timestamp_type = self.postgres_to_avro_mapping['TIMESTAMP']
        for field_name in self.pg_timestamp_fields:
            fields.append({'name': field_name, 'type': timestamp_type})

        schema = {'type': 'record', 'name': table_name, 'fields': fields}
        self.logger.debug(
            f"Generated Avro schema with {len(fields)} fields for {table_name}"
            )
        return schema

    # UTILITY METHODS
    def _get_public_tables(self):
        """
        Get all table names from the PostgreSQL public schema.

        Returns:
            list: List of table names in the public schema

        Raises:
            ValueError: If no tables are found in the public schema
        """
        self.logger.info("Querying PostgreSQL for public tables")
        query = """
        SELECT
            table_name
        FROM
            information_schema.tables
        WHERE
            table_schema = 'public' AND
            table_name NOT LIKE 'staging_%' AND
            table_name NOT LIKE 'quarantine_%'
        """
        data, _ = self.postgres.execute_query(query)

        if not data:
            raise ValueError("No tables found in postgres")

        table_names = [row[0] for row in data]
        return table_names

    def _convert_timestamps_to_millis(self, record):
        """
        Convert PostgreSQL timestamp fields to milliseconds since epoch for
        Avro.

        Args:
            record (dict): Record dictionary containing timestamp fields

        Returns:
            dict: Record with timestamp fields converted to milliseconds
        """
        for field in self.pg_timestamp_fields:
            record[field] = int(record[field].timestamp() * 1000)

        return record
