"""
databricks_replay_manager.py

Manages replay ID persistence using Databricks Delta tables for service recovery.
Queries the existing Databricks table to find the latest replay_id for resuming streams.
"""

import logging
import os
import re
import time
from typing import Optional, Tuple

import requests


class DatabricksReplayManager:
    """Manages replay ID persistence using Databricks Delta tables."""

    def __init__(
        self,
        table_name=None,
        object_name=None,
        workspace_url=None,
        client_id=None,
        client_secret=None,
        sql_endpoint=None,
    ):
        self.object_name = object_name or "unknown"
        self.logger = logging.getLogger(f"{__name__}.{self.object_name}")

        self.table_name = table_name or os.getenv("DATABRICKS_TABLE_NAME")

        if not self.table_name:
            raise ValueError(
                "table_name parameter or DATABRICKS_TABLE_NAME environment variable is required"
            )

        self.workspace_url = workspace_url or os.getenv("DATABRICKS_WORKSPACE_URL")
        self.client_id = client_id or os.getenv("DATABRICKS_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("DATABRICKS_CLIENT_SECRET")
        self.sql_endpoint = sql_endpoint or os.getenv("DATABRICKS_SQL_ENDPOINT")

        # Use main workspace URL for all operations
        self.sql_workspace_url = self.workspace_url

        if not all([self.workspace_url, self.client_id, self.client_secret, self.sql_endpoint]):
            raise ValueError("workspace_url, client_id, client_secret, and sql_endpoint are required")

        # Extract warehouse_id from sql_endpoint format: /sql/1.0/warehouses/{warehouse_id}
        match = re.search(r"/sql/1\.0/warehouses/([a-zA-Z0-9]+)", self.sql_endpoint)
        if not match:
            raise ValueError(
                f"Invalid sql_endpoint format: {self.sql_endpoint}. Expected: /sql/1.0/warehouses/{{warehouse_id}}"
            )

        self.warehouse_id = match.group(1)

        # OAuth token management
        self._oauth_token = None
        self._oauth_token_expires_at = 0

        # Base headers (authorization header will be added dynamically)
        self._base_headers = {
            "Content-Type": "application/json",
        }

        # Cache for replay_id to avoid blocking main thread later
        self._cached_replay_id = None
        self._replay_id_fetched = False

        self.logger.info("Databricks replay manager initialized successfully")

    def _generate_oauth_token(self) -> str:
        """
        Generate OAuth access token using Service Principal credentials.

        Returns:
            str: OAuth access token

        Raises:
            Exception: If token generation fails
        """
        try:
            oauth_url = f"{self.sql_workspace_url.rstrip('/')}/oidc/v1/token"

            payload = {
                "grant_type": "client_credentials",
                "scope": "all-apis"
            }

            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }

            self.logger.debug(f"Requesting OAuth token from: {oauth_url}")

            response = requests.post(
                oauth_url,
                data=payload,
                headers=headers,
                auth=(self.client_id, self.client_secret),
                timeout=30
            )

            response.raise_for_status()
            token_data = response.json()

            access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 3600)  # Default 1 hour

            if not access_token:
                raise ValueError("No access_token in OAuth response")

            # Cache token with expiration (subtract 5 minutes for buffer)
            self._oauth_token = access_token
            self._oauth_token_expires_at = time.time() + expires_in - 300

            self.logger.debug(f"OAuth token generated successfully, expires in {expires_in} seconds")
            return access_token

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to generate OAuth token: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_details = e.response.json()
                    self.logger.error(f"OAuth error response: {error_details}")
                except:
                    self.logger.error(f"OAuth error response text: {e.response.text}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error generating OAuth token: {e}")
            raise

    def _get_auth_headers(self) -> dict:
        """
        Get headers with valid OAuth token, refreshing if necessary.

        Returns:
            dict: Headers including Authorization with Bearer token
        """
        # Check if token needs refresh
        if (not self._oauth_token or
            time.time() >= self._oauth_token_expires_at):
            self.logger.debug("OAuth token expired or missing, generating new token")
            self._generate_oauth_token()

        headers = self._base_headers.copy()
        headers["Authorization"] = f"Bearer {self._oauth_token}"
        return headers

    def execute_sql_query(self, query: str, timeout: int = 30) -> Optional[dict]:
        """
        Execute SQL query using Databricks REST API.

        Args:
            query: SQL query to execute
            timeout: Timeout in seconds for the request

        Returns:
            dict: Query result data or None if no results
        """
        try:
            url = f"{self.sql_workspace_url.rstrip('/')}/api/2.0/sql/statements"

            payload = {
                "warehouse_id": self.warehouse_id,
                "statement": query,
                "wait_timeout": f"{min(timeout, 50)}s",  # Max 50s per API docs
                "disposition": "INLINE",  # Get results immediately
            }

            self.logger.debug(f"Executing SQL query: {query}")
            self.logger.debug(f"Using SQL workspace URL: {self.sql_workspace_url}")
            self.logger.debug(f"Using warehouse_id: {self.warehouse_id}")

            response = requests.post(
                url,
                headers=self._get_auth_headers(),
                json=payload,
                timeout=timeout + 5,  # Add buffer to HTTP timeout
            )

            response.raise_for_status()
            result = response.json()

            # Check if query completed successfully
            if result.get("status", {}).get("state") == "SUCCEEDED":
                manifest = result.get("manifest", {})
                if manifest.get("total_row_count", 0) > 0:
                    return result
                else:
                    self.logger.debug("Query completed but returned no rows")
                    return None
            else:
                state = result.get("status", {}).get("state", "UNKNOWN")
                error = result.get("status", {}).get("error", {})
                self.logger.warning(f"Query failed with state: {state}, error: {error}")
                return None

        except requests.exceptions.Timeout:
            self.logger.error(f"SQL query timed out after {timeout} seconds")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP error executing SQL query: {e}")
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_details = e.response.json()
                    self.logger.error(f"Error response details: {error_details}")
                except:
                    self.logger.error(f"Error response text: {e.response.text}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error executing SQL query: {e}")
            return None

    def initialize_replay_recovery(self):
        """Pre-fetch replay_id during initialization to avoid blocking main thread later."""
        if not self._replay_id_fetched:
            self._cached_replay_id = self.get_last_replay_id()
            self._replay_id_fetched = True

    def get_last_replay_id(self) -> Optional[str]:
        """
        Query the Databricks Delta table to get the latest replay_id using REST API.

        Returns:
            str: The latest replay_id if found, None if table is empty or doesn't exist
        """
        try:
            self.logger.info(f"Querying latest replay_id from {self.table_name}")

            query = f"""
                SELECT replay_id 
                FROM {self.table_name} 
                ORDER BY timestamp DESC 
                LIMIT 1
            """

            result = self.execute_sql_query(query, timeout=30)

            if result is None:
                self.logger.info(
                    f"No events found in {self.table_name} - starting fresh"
                )
                return None

            result_data = result.get("result", {})
            data_array = result_data.get("data_array", [])

            if data_array and len(data_array) > 0:
                replay_id = data_array[0][0] if data_array[0] else None
                if replay_id:
                    self.logger.info(f"Found latest replay_id: {replay_id}")
                    return str(replay_id)

            self.logger.info(f"No events found in {self.table_name} - starting fresh")
            return None

        except Exception as e:
            self.logger.warning(f"Failed to query replay_id from Databricks: {e}")
            self.logger.info("Will fallback to LATEST subscription mode")
            return None

    def table_exists(self) -> bool:
        """
        Check if the target Databricks table exists using REST API.

        Returns:
            bool: True if table exists and is accessible, False otherwise
        """
        try:
            query = f"DESCRIBE {self.table_name}"
            result = self.execute_sql_query(query, timeout=10)

            if result is not None:
                self.logger.debug(f"Table {self.table_name} exists")
                return True
            else:
                self.logger.debug(f"Table {self.table_name} does not exist")
                return False

        except Exception as e:
            self.logger.debug(
                f"Table existence check failed for {self.table_name}: {e}"
            )
            return False

    def create_table_if_not_exists(self) -> bool:
        """
        Create the Databricks table with proper CDC schema if it doesn't exist.

        Returns:
            bool: True if table was created or already exists, False if creation failed
        """
        try:
            # Extract catalog and schema from table name for proper partitioning
            table_parts = self.table_name.split(".")
            if len(table_parts) != 3:
                self.logger.error(
                    f"Invalid table name format: {self.table_name}. Expected: catalog.schema.table"
                )
                return False

            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    event_id STRING COMMENT 'Unique identifier for this event',
                    schema_id STRING COMMENT 'Avro schema identifier',
                    replay_id STRING COMMENT 'Salesforce replay ID for ordering and recovery',
                    timestamp BIGINT COMMENT 'Event timestamp from Salesforce (epoch milliseconds)',
                    change_type STRING COMMENT 'Type of change: CREATE, UPDATE, DELETE, UNDELETE',
                    entity_name STRING COMMENT 'Salesforce object name (e.g., Account)',
                    change_origin STRING COMMENT 'Source of the change event',
                    record_ids ARRAY<STRING> COMMENT 'Array of Salesforce record IDs that changed',
                    changed_fields ARRAY<STRING> COMMENT 'List of fields that were modified',
                    nulled_fields ARRAY<STRING> COMMENT 'List of fields that were set to null',
                    diff_fields ARRAY<STRING> COMMENT 'List of fields with differences',
                    record_data_json STRING COMMENT 'Complete CDC event data as JSON',
                    payload_binary BINARY COMMENT 'Raw Avro binary payload from Salesforce for schema-based parsing',
                    schema_json STRING COMMENT 'Avro schema JSON string for parsing binary payload',
                    org_id STRING COMMENT 'Salesforce organization ID',
                    processed_timestamp BIGINT COMMENT 'When the event was processed (epoch milliseconds)'
                )
                USING DELTA
                TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true',
                    'delta.enableRowTracking' = 'false'
                )
                COMMENT 'Real-time Salesforce Change Data Capture events'
            """

            self.logger.info(f"Creating Databricks table: {self.table_name}")
            result = self.execute_sql_query(create_table_sql, timeout=60)

            # Check if the SQL execution failed (returns None)
            # For CREATE TABLE, successful execution should return a result dict (even if empty)
            # But we also need to verify the table actually exists
            if self.table_exists():
                self.logger.info(f"Successfully created table: {self.table_name}")
                return True
            else:
                self.logger.error(
                    f"Failed to create table: {self.table_name} - table does not exist after creation attempt"
                )
                return False

        except Exception as e:
            self.logger.error(f"Error creating table {self.table_name}: {e}")
            return False

    def get_subscription_params(
        self, auto_create_table: bool = True, backfill_historical: bool = True
    ) -> Tuple[str, str]:
        """
        Get the appropriate subscription parameters based on table existence and replay state.

        Args:
            auto_create_table: If True, create table if it doesn't exist
            backfill_historical: If True, start from EARLIEST for new tables

        Returns:
            Tuple[str, str]: (replay_type, replay_id) for PubSub subscription
        """
        # First check if table exists
        if not self.table_exists():
            if auto_create_table:
                self.logger.info(
                    f"Table {self.table_name} doesn't exist - creating and configuring for historical backfill"
                )
                table_created = self.create_table_if_not_exists()

                if table_created and backfill_historical:
                    self.logger.info(
                        "Starting historical backfill from EARLIEST (this may take time for large orgs)"
                    )
                    return ("EARLIEST", "")
                elif table_created:
                    self.logger.info("Table created - starting from LATEST")
                    return ("LATEST", "")
                else:
                    self.logger.warning(
                        "Failed to create table - falling back to LATEST mode"
                    )
                    return ("LATEST", "")
            else:
                self.logger.warning(
                    f"Table {self.table_name} doesn't exist and auto_create_table=False - falling back to LATEST"
                )
                return ("LATEST", "")

        if self._replay_id_fetched:
            last_replay_id = self._cached_replay_id
        else:
            last_replay_id = self.get_last_replay_id()

        if last_replay_id:
            self.logger.info(f"Resuming subscription from replay_id: {last_replay_id}")
            return ("CUSTOM", last_replay_id)
        else:
            if backfill_historical:
                self.logger.info(
                    "Table exists but is empty - starting historical backfill from EARLIEST"
                )
                return ("EARLIEST", "")
            else:
                self.logger.info("Table exists but is empty - starting from LATEST")
                return ("LATEST", "")

    def validate_table_exists(self) -> bool:
        """
        Check if the Databricks table exists and is accessible.

        Returns:
            bool: True if table exists and is accessible, False otherwise
        """
        try:
            query = f"DESCRIBE {self.table_name}"

            # Try to describe the table
            for row in self.sql_executor(query):
                # If we get any results, table exists
                self.logger.info(f"Validated table {self.table_name} exists")
                return True

            return False

        except Exception as e:
            self.logger.warning(f"Table validation failed for {self.table_name}: {e}")
            return False

    def get_table_stats(self) -> Optional[dict]:
        """
        Get basic statistics about the replay table for monitoring.

        Returns:
            dict: Table statistics or None if query fails
        """
        try:
            query = f"""
                SELECT 
                    COUNT(*) as total_events,
                    MIN(timestamp) as earliest_event,
                    MAX(timestamp) as latest_event,
                    MAX(replay_id) as latest_replay_id
                FROM {self.table_name}
            """

            for row in self.sql_executor(query):
                stats = {
                    "total_events": row[0],
                    "earliest_event": row[1],
                    "latest_event": row[2],
                    "latest_replay_id": row[3],
                }

                self.logger.info(
                    f"Table stats: {stats['total_events']} events, latest: {stats['latest_replay_id']}"
                )
                return stats

            return None

        except Exception as e:
            self.logger.warning(f"Failed to get table statistics: {e}")
            return None


def create_replay_manager_from_env(
    table_name=None, object_name=None
) -> DatabricksReplayManager:
    """
    Create and initialize a DatabricksReplayManager from environment variables.

    Args:
        table_name: Optional table name override
        object_name: Optional object name for logging context

    Returns:
        DatabricksReplayManager: Initialized replay manager

    Raises:
        ValueError: If required environment variables are missing
        Exception: If initialization fails

    Environment Variables:
        Required:
        - DATABRICKS_WORKSPACE_URL: Databricks workspace URL
        - DATABRICKS_CLIENT_ID: OAuth Service Principal client ID
        - DATABRICKS_CLIENT_SECRET: OAuth Service Principal client secret
        - DATABRICKS_SQL_ENDPOINT: SQL warehouse endpoint
        - DATABRICKS_TABLE_NAME: Target table name (if table_name param not provided)

    """
    return DatabricksReplayManager(table_name=table_name, object_name=object_name)
