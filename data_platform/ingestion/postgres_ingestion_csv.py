import pandas as pd
import json
import logging
from typing import Dict, Any, List
from data_platform.clients.postgres_client import PostgresClient
from data_platform.config.postgres_config import PostgresConfig


class PostgresIngestionCSV:
    """PostgreSQL CSV ingestion with schema-driven table creation and UPSERT
    operations."""

    def __init__(self) -> None:
        """Initialize with PostgreSQL client and configuration."""
        self.postgres = PostgresClient()
        self.postgres_config = PostgresConfig()
        self.logger = logging.getLogger(__name__)

    # PUBLIC METHODS
    def ingest(self, csv_path: str, schema_path: str) -> None:
        """Ingest CSV data into PostgreSQL using JSON schema definition.

        Args:
            csv_path: Path to CSV file to ingest
            schema_path: Path to JSON schema file defining table structure

        Raises:
            FileNotFoundError: If CSV or schema file not found
            psycopg.Error: For database operations errors
        """
        self.logger.info(f"Starting ingestion: {csv_path} -> PostgreSQL")

        schema = self._load_schema(schema_path)
        self.logger.info(f"Loaded schema for table: {schema['table_name']}")

        df = pd.read_csv(csv_path)
        df = df.dropna(how='all')

        original_rows = len(df)
        cleaned_rows = len(df)
        if original_rows != cleaned_rows:
            self.logger.info(
                f"Removed {original_rows - cleaned_rows} empty rows"
                )

        df_col_cleaned = self._clean_csv_columns(df)
        self.logger.info(f"Processing {cleaned_rows} rows for ingestion")

        self._create_table(schema)
        self._insert_data(df_col_cleaned, schema)

        self.logger.info(
            f"Successfully completed ingestion of {cleaned_rows} rows"
            )

    # SCHEMA & DATA LOADING
    def _load_schema(self, schema_path: str) -> Dict[str, Any]:
        """Load table schema from JSON file.

        Returns:
            Dictionary containing table name, columns, and metadata
        """
        self.logger.debug(f"Loading schema from: {schema_path}")
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        return schema

    def _clean_csv_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean CSV column names to match database naming conventions.

        Transforms column names to lowercase with underscores, removes special
        characters.

        Returns:
            DataFrame with standardized column names
        """
        original_columns = df.columns.tolist()
        df.columns = (
            df.columns.str.lower()
            .str.replace(r'[^a-z0-9 ]', '', regex=True)
            .str.replace(r'\s+', '_', regex=True)
            .str.replace(r'_+', '_', regex=True)
            .str.strip('_')
        )
        self.logger.debug(
            f"Cleaned column names: {original_columns} "
            f"-> {df.columns.tolist()}"
            )
        return df

    # TABLE CREATION METHODS
    def _create_table(self, schema: Dict[str, Any]) -> None:
        """Create PostgreSQL table with columns, timestamps, and update
        triggers."""
        table_name = schema['table_name']
        self.logger.info(
            f"Creating table structure and triggers for: {table_name}"
            )

        self._create_table_structure(schema)
        self._create_update_trigger(schema['table_name'])

        self.logger.info(f"Table setup completed for: {table_name}")

    def _create_table_structure(self, schema: Dict[str, Any]) -> None:
        """Create table with columns from schema plus audit timestamps."""
        table_name = schema['table_name']
        column_count = len(schema['columns'])
        self.logger.debug(
            f"Creating table {table_name} with {column_count} columns"
            )

        columns = []
        for col in schema['columns']:
            column_def = f"{col['name']} {col['type']}"
            if col.get('primary_key'):
                column_def += " PRIMARY KEY"
            columns.append(column_def)

        # Automatically add timestamp columns
        columns.append("pg_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        columns.append("pg_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

        create_query = f"""
            CREATE TABLE IF NOT EXISTS {schema['table_name']} (
                {', '.join(columns)}
            );
        """
        self.logger.debug(
            f"Generated SQL create table query for {table_name}:\n"
            f"{create_query}"
            )

        self.postgres.execute_query(create_query)

    def _create_update_trigger(self, table_name: str) -> None:
        """Create trigger to automatically update pg_updated_at on row
        changes."""
        self.logger.debug(f"Setting up update trigger for table: {table_name}")
        self._create_update_trigger_function()
        self._create_table_trigger(table_name)

    def _create_update_trigger_function(self) -> None:
        """Create reusable trigger function for updating pg_updated_at
        timestamp."""
        trigger_function_query = """
            CREATE OR REPLACE FUNCTION update_pg_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.pg_updated_at = NOW();
                RETURN NEW;
            END;
            $$ language 'plpgsql';
        """
        self.postgres.execute_query(trigger_function_query)

    def _create_table_trigger(self, table_name: str) -> None:
        """Create table-specific trigger that calls update function on row
        updates."""
        self.logger.debug(f"Creating update trigger for table: {table_name}")
        trigger_query = f"""
            DROP TRIGGER IF EXISTS update_pg_updated_at ON {table_name};
            CREATE TRIGGER update_pg_updated_at
                BEFORE UPDATE ON {table_name}
                FOR EACH ROW EXECUTE FUNCTION update_pg_updated_at_column();
        """
        self.postgres.execute_query(trigger_query)

    # DATA INSERTION METHODS
    def _insert_data(self, df: pd.DataFrame, schema: Dict[str, Any]) -> None:
        """Insert DataFrame data using bulk UPSERT operations.

        Uses ON CONFLICT to handle duplicate primary keys by updating existing
        records. Automatically manages pg_created_at and pg_updated_at
        timestamps.
        """
        table_name = schema['table_name']
        row_count = len(df)
        self.logger.info(f"Inserting {row_count} rows into {table_name}")

        connection = self.postgres_config.connect_postgres()

        try:
            with connection.cursor() as cursor:
                # Use only the business columns from schema (timestamps are
                # auto-managed)
                csv_columns = [
                    col for col in schema['columns']
                    if col['name'] in df.columns
                ]

                matched_columns = [col['name'] for col in csv_columns]
                self.logger.debug(
                    f"Matched columns for insertion: {matched_columns}"
                    )

                column_names = ', '.join([col['name'] for col in csv_columns])
                placeholders = ', '.join(['%s'] * len(csv_columns))

                primary_key_cols = self._find_primary_keys(schema)
                primary_key_str = ', '.join(primary_key_cols)
                update_clause = self._build_update_clause(csv_columns)

                self.logger.debug(f"Primary keys: {primary_key_cols}")

                upsert_query = f"""
                    INSERT INTO {schema['table_name']} ({column_names})
                    VALUES ({placeholders})
                    ON CONFLICT ({primary_key_str})
                    DO UPDATE SET {update_clause}
                """

                # Ensure DataFrame column order matches csv_columns
                df_ordered = df[[col['name'] for col in csv_columns]]
                values_list = [
                    tuple(row) for row in df_ordered.itertuples(index=False)
                ]

                # Use executemany for bulk UPSERT (psycopg v3 optimized)
                cursor.executemany(upsert_query, values_list)
                connection.commit()

                self.logger.info(
                    f"Successfully inserted/updated {row_count} rows "
                    f"in {table_name}"
                    )
        finally:
            connection.close()

    def _find_primary_keys(self, schema: Dict[str, Any]) -> List[str]:
        """Extract primary key column names from schema definition.

        Returns:
            List of primary key column names
        """
        primary_key_cols = []
        for col in schema['columns']:
            if col.get('primary_key'):
                primary_key_cols.append(col['name'])
        return primary_key_cols

    def _build_update_clause(self, csv_columns: List[Dict[str, Any]]) -> str:
        """Build SQL UPDATE clause for non-primary key columns.

        Returns:
            Comma-separated UPDATE assignments using EXCLUDED values
        """
        update_parts = []
        for col in csv_columns:
            if not col.get('primary_key'):
                update_parts.append(f"{col['name']} = EXCLUDED.{col['name']}")
        return ', '.join(update_parts)
