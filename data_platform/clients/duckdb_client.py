import duckdb
from data_platform.config.duckdb_config import DuckdbConfig


class DuckdbClient:
    def __init__(
            self,
            db_path: str = "data_platform/lakehouse/lakehouse.db"
            ):
        self.config = DuckdbConfig()
        self.connection = duckdb.connect(database=db_path)
        self.connection.execute("INSTALL httpfs;")
        self.connection.execute("LOAD httpfs;")
        self.connection.execute("INSTALL avro;")
        self.connection.execute("LOAD avro;")
        for stmt in self.config.connection_string.strip().split(';'):
            if stmt.strip():
                self.connection.execute(stmt)

    def execute_query(self, query: str, return_pd_dataframe: bool = False):
        if return_pd_dataframe:
            return self.connection.execute(query).df()
        return self.connection.execute(query).fetchall()

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
