"""
databricks_forwarder.py

Handles forwarding Salesforce events to Databricks Delta tables using the Zerobus API.
"""

import json
import logging
import os
import time

from zerobus_sdk import (
    StreamConfigurationOptions,
    StreamState,
    TableProperties,
    ZerobusException,
)
from zerobus_sdk.aio import ZerobusSdk

from ..pubsub.proto import salesforce_events_pb2


class DatabricksForwarder:
    """
    Forwards Salesforce Change Data Capture events to Databricks Delta tables.

    Leverages the Zerobus SDK's automatic recovery capabilities for maximum reliability.
    The SDK handles stream recreation, error recovery, and unacknowledged record replay.
    """

    def __init__(
        self,
        ingest_endpoint: str,
        workspace_url: str,
        api_token: str,
        table_name: str,
        stream_config_options: dict = None,
    ):
        """
        Initialize the Databricks forwarder.

        Args:
            ingest_endpoint: Databricks ingest endpoint
            workspace_url: Databricks workspace URL
            api_token: Databricks API token
            table_name: Target Delta table name
            stream_config_options: Optional dict of ZerobusSdk stream configuration options
                Available options (recovery is always enabled):
                - max_inflight_records (int): Max records in flight (default: 50,000)
                - recovery_retries (int): Number of recovery attempts (default: 5)
                - recovery_timeout_ms (int): Recovery timeout per attempt (default: 30,000ms)
                - recovery_backoff_ms (int): Backoff between attempts (default: 5,000ms)
                - server_lack_of_ack_timeout_ms (int): Server unresponsive timeout (default: 60,000ms)
                - flush_timeout_ms (int): Stream flush timeout (default: 300,000ms)
                - ack_callback (callable): Acknowledgment callback function (default: debug logging)
                Note: recovery is always True for maximum reliability
        """
        self.ingest_endpoint = ingest_endpoint
        self.workspace_url = workspace_url
        self.api_token = api_token
        self.table_name = table_name

        self.sdk = ZerobusSdk(ingest_endpoint, workspace_url, api_token)

        self.table_properties = TableProperties(
            table_name, salesforce_events_pb2.SalesforceEvent.DESCRIPTOR
        )

        # Configure stream options with enhanced recovery settings
        # Default to production-ready recovery configuration
        default_config = {
            "recovery": True,  # Always enable recovery
            "recovery_retries": 5,  # Increased for production resilience
            "recovery_timout_ms": 30000,  # 30 seconds per attempt
            "recovery_backoff_ms": 5000,  # 5 second backoff
            "server_lack_of_ack_timeout_ms": 60000,  # 60 seconds server timeout
            "max_inflight_records": 50000,  # Default batch size
        }

        # Merge user-provided options with defaults (user options take precedence)
        final_config = {**default_config, **(stream_config_options or {})}

        # Force recovery to always be True as per plan
        final_config["recovery"] = True

        # Add optional acknowledgment callback for monitoring (if not provided)
        if "ack_callback" not in final_config:
            final_config["ack_callback"] = self._default_ack_callback

        self.stream_config = StreamConfigurationOptions(**final_config)

        self.stream = None
        self.logger = logging.getLogger(__name__)

    def _default_ack_callback(self, response):
        """Default acknowledgment callback for monitoring ingestion progress."""
        offset_id = response.durability_ack_up_to_offset
        self.logger.debug(f"Zerobus ack received up to offset: {offset_id}")

    def get_stream_health(self) -> dict:
        """Get comprehensive stream health information."""
        if not self.stream:
            return {"status": "no_stream", "healthy": False, "stream_id": None}

        try:
            state = self.stream.get_state()
            return {
                "status": state.name.lower(),
                "healthy": state in [StreamState.OPENED, StreamState.RECOVERING],
                "stream_id": getattr(self.stream, "stream_id", None),
                "state_code": state.value,
            }
        except Exception as e:
            return {"status": "error", "healthy": False, "error": str(e)}

    async def initialize_stream(self):
        """Create the ingest stream to the Delta table."""
        try:
            self.stream = await self.sdk.create_stream(
                self.table_properties, self.stream_config
            )
            self.logger.info(f"Initialized Zerobus stream to table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Zerobus stream: {e}")
            self.stream = None
            raise

    async def forward_event(
        self,
        salesforce_event_data: dict,
        org_id: str,
        payload_binary: bytes = None,
        schema_json: str = None,
    ):
        """
        Convert Salesforce CDC event to protobuf and forward to Databricks.

        The Zerobus SDK handles automatic recovery, stream recreation, and error handling.
        This method focuses purely on data transformation and ingestion.

        Args:
            salesforce_event_data: Decoded Salesforce event data
            org_id: Salesforce organization ID
            payload_binary: Raw Avro binary payload from Salesforce (optional)
            schema_json: Avro schema JSON string for parsing (optional)
        """
        # Initialize stream if not already created
        if not self.stream:
            await self.initialize_stream()

        # Extract event data
        event_id = salesforce_event_data.get("event_id", "")
        schema_id = salesforce_event_data.get("schema_id", "")
        replay_id = salesforce_event_data.get("replay_id", "")
        change_header = salesforce_event_data.get("ChangeEventHeader", {})
        change_type = change_header.get("changeType", "UNKNOWN")
        entity_name = change_header.get("entityName", "UNKNOWN")
        change_origin = change_header.get("changeOrigin", "")
        record_ids = change_header.get("recordIds", [])
        changed_fields = salesforce_event_data.get("converted_changed_fields", [])
        nulled_fields = salesforce_event_data.get("converted_nulled_fields", [])
        diff_fields = salesforce_event_data.get("converted_diff_fields", [])

        # Extract record data (exclude metadata fields)
        excluded_keys = [
            "ChangeEventHeader",
            "event_id",
            "schema_id",
            "replay_id",
            "converted_changed_fields",
            "converted_nulled_fields",
            "converted_diff_fields",
        ]
        record_data = {
            k: v for k, v in salesforce_event_data.items() if k not in excluded_keys
        }
        record_data_json = json.dumps(record_data)

        # Create protobuf message
        pb_event = salesforce_events_pb2.SalesforceEvent(
            event_id=event_id,
            schema_id=schema_id,
            replay_id=replay_id,
            timestamp=int(time.time() * 1000),
            change_type=change_type,
            entity_name=entity_name,
            change_origin=change_origin,
            record_ids=record_ids,
            changed_fields=changed_fields,
            nulled_fields=nulled_fields,
            diff_fields=diff_fields,
            record_data_json=record_data_json,
            payload_binary=payload_binary if payload_binary is not None else b"",
            schema_json=schema_json if schema_json is not None else "",
            org_id=org_id,
            processed_timestamp=int(time.time() * 1000),
        )

        try:
            # Ingest the record - SDK handles all recovery automatically
            await self.stream.ingest_record(pb_event)

            # Log successful ingestion
            record_id = record_ids[0] if record_ids else "unknown"
            self.logger.info(
                f"Ingested to Databricks: {self.table_name} - {entity_name} {change_type} {record_id}"
            )

        except ZerobusException as e:
            # Only handle non-recoverable errors - SDK handles recoverable ones automatically
            self.logger.error(f"Non-recoverable Zerobus error: {e}")
            # For non-recoverable errors, we may need to recreate the stream manually
            if (
                "schema" in str(e).lower()
                or "permission" in str(e).lower()
                or "authentication" in str(e).lower()
            ):
                self.logger.warning(
                    "Non-recoverable error detected - may require stream recreation"
                )
                self.stream = None
            raise

        except Exception as e:
            # Handle unexpected errors
            self.logger.error(f"Unexpected error forwarding event to Databricks: {e}")
            raise

    async def flush(self):
        """Flush any pending records to ensure they're written."""
        if self.stream:
            try:
                await self.stream.flush()
                self.logger.debug("Flushed pending records to Databricks")
            except Exception as e:
                self.logger.error(f"Failed to flush records: {e}")
                raise

    async def close(self):
        """Close the stream and clean up resources."""
        if self.stream:
            try:
                await self.stream.close()
                self.logger.info("Successfully closed Zerobus stream")
            except Exception as e:
                self.logger.warning(f"Error while closing stream: {e}")
            finally:
                self.stream = None


def create_forwarder_from_env(table_name=None) -> DatabricksForwarder:
    """
    Create a DatabricksForwarder instance from environment variables.

    Args:
        table_name: Optional table name override. If not provided, uses DATABRICKS_TABLE_NAME env var.

    Returns:
        Configured DatabricksForwarder instance

    Raises:
        ValueError: If required environment variables are missing

    Environment Variables:
        Required:
        - DATABRICKS_INGEST_ENDPOINT: Databricks ingest endpoint
        - DATABRICKS_WORKSPACE_URL: Databricks workspace URL
        - DATABRICKS_API_TOKEN: Databricks API token
        - DATABRICKS_TABLE_NAME: Target table name (if table_name param not provided)

        Optional ZerobusSdk Stream Configuration (recovery always enabled):
        - ZEROBUS_MAX_INFLIGHT_RECORDS: Max records in flight (default: 50000)
        - ZEROBUS_RECOVERY_RETRIES: Recovery attempt count (default: 5)
        - ZEROBUS_RECOVERY_TIMEOUT_MS: Recovery timeout per attempt (default: 30000)
        - ZEROBUS_RECOVERY_BACKOFF_MS: Backoff between attempts (default: 5000)
        - ZEROBUS_SERVER_ACK_TIMEOUT_MS: Server unresponsive timeout (default: 60000)
        - ZEROBUS_FLUSH_TIMEOUT_MS: Stream flush timeout (default: 300000)
    """
    # Use provided table name or fallback to environment variable
    target_table_name = table_name or os.getenv("DATABRICKS_TABLE_NAME")

    required_vars = [
        "DATABRICKS_INGEST_ENDPOINT",
        "DATABRICKS_WORKSPACE_URL",
        "DATABRICKS_API_TOKEN",
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    if not target_table_name:
        raise ValueError(
            "table_name parameter or DATABRICKS_TABLE_NAME environment variable is required"
        )

    # Build stream configuration from environment variables
    stream_config = {}

    if os.getenv("ZEROBUS_MAX_INFLIGHT_RECORDS"):
        stream_config["max_inflight_records"] = int(
            os.getenv("ZEROBUS_MAX_INFLIGHT_RECORDS")
        )

    if os.getenv("ZEROBUS_RECOVERY_RETRIES"):
        stream_config["recovery_retries"] = int(os.getenv("ZEROBUS_RECOVERY_RETRIES"))

    if os.getenv("ZEROBUS_RECOVERY_TIMOUT_MS"):
        stream_config["recovery_timout_ms"] = int(
            os.getenv("ZEROBUS_RECOVERY_TIMOUT_MS")
        )

    if os.getenv("ZEROBUS_RECOVERY_BACKOFF_MS"):
        stream_config["recovery_backoff_ms"] = int(
            os.getenv("ZEROBUS_RECOVERY_BACKOFF_MS")
        )

    if os.getenv("ZEROBUS_SERVER_ACK_TIMEOUT_MS"):
        stream_config["server_lack_of_ack_timeout_ms"] = int(
            os.getenv("ZEROBUS_SERVER_ACK_TIMEOUT_MS")
        )

    if os.getenv("ZEROBUS_FLUSH_TIMEOUT_MS"):
        stream_config["flush_timeout_ms"] = int(os.getenv("ZEROBUS_FLUSH_TIMEOUT_MS"))

    return DatabricksForwarder(
        ingest_endpoint=os.getenv("DATABRICKS_INGEST_ENDPOINT"),
        workspace_url=os.getenv("DATABRICKS_WORKSPACE_URL"),
        api_token=os.getenv("DATABRICKS_API_TOKEN"),
        table_name=target_table_name,
        stream_config_options=stream_config if stream_config else None,
    )
