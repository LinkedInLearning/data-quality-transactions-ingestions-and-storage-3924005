import psycopg


class PostgresConfig:
    """Configuration and connection management for PostgreSQL"""

    def __init__(self):
        """Initialize database configuration with connection parameters."""
        self.postgres_config = {
            'host': 'postgres',
            'port': 5432,
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'postgres'
        }

    def connect_postgres(self):
        """Create and return a PostgreSQL connection.

        Returns:
            psycopg.Connection: PostgreSQL connection object
        """
        connection = psycopg.connect(
            host=self.postgres_config['host'],
            port=self.postgres_config['port'],
            dbname=self.postgres_config['dbname'],
            user=self.postgres_config['user'],
            password=self.postgres_config['password']
        )
        return connection
