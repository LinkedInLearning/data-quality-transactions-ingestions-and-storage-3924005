from data_platform.config.postgres_config import PostgresConfig
import pandas as pd
from typing import Optional, Union, List, Tuple


class PostgresClient:
    """PostgreSQL client for executing database queries."""

    def __init__(self):
        """Initialize PostgreSQL client with configuration."""
        self.config = PostgresConfig()

    def execute_query(
            self,
            query: str,
            return_pd_dataframe: bool = False
            ) -> Optional[Union[Tuple[List[Tuple], List[str]], pd.DataFrame]]:
        """Execute a SQL query and return results as raw data or DataFrame.

        Args:
            query: SQL query string to execute
            return_pd_dataframe: If True, return DataFrame. If False, return
            (rows, columns).

        Returns:
            (rows, columns) tuple with raw results, or DataFrame if
            return_pd_dataframe=True, or None for DDL operations

        Raises:
            ValueError: If query is empty or None
            psycopg.Error: For database connection or execution errors
        """
        if not query or query.strip() == "":
            raise ValueError("Query cannot be empty or None")

        connection = self.config.connect_postgres()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)

                # Check if query returns results
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    results = cursor.fetchall()

                    if return_pd_dataframe:
                        return pd.DataFrame(results, columns=columns)
                    else:
                        return results, columns
                else:
                    # DDL operation - commit and return None
                    connection.commit()
                    return None
        finally:
            connection.close()
