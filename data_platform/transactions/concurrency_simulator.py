import logging
import threading
import time

from data_platform.clients.postgres_client import PostgresClient
from data_platform.config.postgres_config import PostgresConfig


class TransactionSimulator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.postgres = PostgresClient()
        self.postgres_config = PostgresConfig()

    def _setup_demo_table(self):
        """Create the demo table and initialize the counter to 0."""
        self.logger.info("Setting up transaction demo table")
        query = """
            CREATE TABLE IF NOT EXISTS transaction_simulation (
                id INTEGER PRIMARY KEY,
                counter INTEGER NOT NULL
            )
        """
        self.postgres.execute_query(query)
        self._reset_counter(0)
        self.logger.info("Demo table setup complete")

    def _reset_counter(self, value=0):
        """Reset the counter to the specified value."""
        self.logger.info(f"Resetting counter to {value}")
        query = f"""
            INSERT INTO transaction_simulation (id, counter)
            VALUES (1, {value})
            ON CONFLICT (id) DO UPDATE SET counter = EXCLUDED.counter
        """
        self.postgres.execute_query(query)
        self.logger.info(f"Counter reset to {value}")

    def _get_counter(self):
        """Get the current counter value from the database."""
        query = "SELECT counter FROM transaction_simulation WHERE id = 1"
        self.logger.debug(f"Executing: {query}")
        df = self.postgres.execute_query(query, return_pd_dataframe=True)
        if df.empty:
            raise ValueError("No counter found in database")
        counter_value = int(df["counter"][0])
        self.logger.debug(f"Current counter value: {counter_value}")
        return counter_value

    def _set_isolation_level(self, cursor, connection, isolation_level):
        """Set the transaction isolation level for the current session."""
        if not isolation_level:
            return

        set_session = (
            f"SET SESSION CHARACTERISTICS AS TRANSACTION "
            f"ISOLATION LEVEL {isolation_level}"
        )
        self.logger.info(f"Executing: {set_session}")
        cursor.execute(set_session)
        connection.commit()

    def _read_counter(self, cursor):
        """Read the current counter value."""
        select_query = (
            "SELECT counter FROM transaction_simulation WHERE id = 1"
        )
        cursor.execute(select_query)
        current_value = cursor.fetchone()[0]
        return current_value

    def _update_counter(self, cursor, connection, new_value):
        """Update the counter to the new value."""
        update_query = f"UPDATE transaction_simulation SET counter = {new_value} WHERE id = 1"
        cursor.execute(update_query)
        connection.commit()

    def _try_transaction(self, cursor, connection, increment, barrier=None):
        """Execute a transaction without retry logic."""
        current_value = self._read_counter(cursor)

        if barrier:
            self.logger.info("Waiting at barrier")
            barrier.wait()
            self.logger.info("Passed barrier")

        new_value = current_value + increment
        self.logger.info(f"Calculated new value: {new_value}")

        time.sleep(1.0)
        self.logger.info("Sleeping for 1.0 seconds")

        self._update_counter(cursor, connection, new_value)
        self.logger.info(f"Updated counter to {new_value}")
        return new_value

    def _handle_transaction_with_retry(
        self, cursor, connection, increment, isolation_level, barrier
    ):
        """Handle a transaction with retry logic for serialization failures."""
        # First attempt with barrier synchronization
        try:
            self._try_transaction(cursor, connection, increment, barrier)
            return True
        except Exception as e:
            connection.rollback()
            error_str = str(e).lower()
            self.logger.warning(f"Transaction failed: {e}")

            # Only retry serialization failures when using isolation
            if not isolation_level or not (
                "serialization" in error_str or "serialize" in error_str
            ):
                self.logger.error("Non-serialization error; not retrying")
                raise

            # Retry logic - without barrier
            return self._retry_transaction(cursor, connection, increment)

    def _retry_transaction(self, cursor, connection, increment):
        """Retry a transaction after serialization failure."""
        max_retries = 4
        for retry in range(1, max_retries + 1):
            try:
                self.logger.info(f"Retry attempt {retry}/{max_retries}")
                self._try_transaction(cursor, connection, increment)
                return True
            except Exception as retry_error:
                connection.rollback()
                error_str = str(retry_error).lower()
                if "serialization" in error_str or "serialize" in error_str:
                    self.logger.warning(
                        f"Serialization failure on retry {retry}: {retry_error}"
                    )
                    if retry < max_retries:
                        backoff = 0.2 * retry
                        self.logger.info(f"Backing off for {backoff:.1f}s")
                        time.sleep(backoff)
                    else:
                        self.logger.error("Max retries reached; giving up")
                        return False
                else:
                    self.logger.error(
                        f"Retry failed with non-serialization error: {retry_error}"
                    )
                    raise
        return False

    def _default_worker(self, increment, barrier, isolation_level=None):
        """Worker function that runs in a separate thread."""
        with (
            self.postgres_config.connect_postgres() as connection,
            connection.cursor() as cursor,
        ):
            # Set transaction isolation level if specified
            self._set_isolation_level(cursor, connection, isolation_level)

            # Execute transaction with retry logic
            self._handle_transaction_with_retry(
                cursor, connection, increment, isolation_level, barrier
            )

    def _create_worker_threads(
        self, increment_a, increment_b, barrier, isolation_level
    ):
        """Create worker threads for an iteration."""
        threads = [
            threading.Thread(
                target=self._default_worker,
                args=(
                    increment_a,
                    barrier,
                    isolation_level,
                ),
            ),
            threading.Thread(
                target=self._default_worker,
                args=(
                    increment_b,
                    barrier,
                    isolation_level,
                ),
            ),
        ]
        return threads

    def simulate(
        self,
        increment_a=1,
        increment_b=2,
        iterations=5,
        isolation_level=None,
    ):
        """Run the concurrency simulation."""
        self.logger.info(
            f"Starting simulation with {iterations} iterations, "
            f"increments {increment_a} & {increment_b}"
            + (f", isolation: {isolation_level}" if isolation_level else "")
        )
        self._setup_demo_table()

        for iteration in range(iterations):
            self.logger.info(f"--- Iteration {iteration + 1}/{iterations} ---")
            barrier = threading.Barrier(2)
            threads = self._create_worker_threads(
                increment_a, increment_b, barrier, isolation_level
            )

            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            final_value = self._get_counter()
            self.logger.info(
                f"Counter after iteration {iteration + 1}: {final_value}"
            )

            table_df = self.postgres.execute_query(
                "SELECT id, counter FROM transaction_simulation ORDER BY id",
                return_pd_dataframe=True,
            )
            self.logger.info(
                f"\ntransaction_simulation after iteration {iteration + 1}:\n{table_df.to_string(index=False)}"
            )
            self.logger.info(f"Iteration {iteration + 1} complete")
