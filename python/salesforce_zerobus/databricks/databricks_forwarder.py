"""
databricks_forwarder.py

Handles forwarding Salesforce events to Databricks Delta tables using the Zerobus API.
"""

import json
import logging
import os
import time

from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import (
    AckCallback,
    StreamConfigurationOptions,
    TableProperties,
    ZerobusException,
)

from ..pubsub.proto import salesforce_events_pb2


class _DefaultAckCallback(AckCallback):
    """AckCallback subclass that logs ack offsets at debug level."""

    def __init__(self, logger):
        self._logger = logger

    def on_ack(self, offset: int):
        self._logger.debug(f"Zerobus ack received up to offset: {offset}")

    def on_error(self, offset: int, error_message: str):
        self._logger.warning(f"Zerobus ack error at offset {offset}: {error_message}")


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
        client_id: str,
        client_secret: str,
        table_name: str,
        stream_config_options: dict = None,
        sdk=None,
    ):
        """
        Initialize the Databricks forwarder.

        Args:
            ingest_endpoint: Databricks ingest endpoint
            workspace_url: Databricks workspace URL (Unity Catalog URL)
            client_id: OAuth 2.0 Service Principal client ID
            client_secret: OAuth 2.0 Service Principal client secret
            table_name: Target Delta table name (fully qualified: catalog.schema.table)
            stream_config_options: Optional dict of ZerobusSdk stream configuration options
                Available options (recovery is always enabled):
                - max_inflight_records (int): Max records in flight (default: 50,000)
                - recovery_retries (int): Number of recovery attempts (default: 5)
                - recovery_timeout_ms (int): Recovery timeout per attempt (default: 15,000ms)
                - recovery_backoff_ms (int): Backoff between attempts (default: 5,000ms)
                - server_lack_of_ack_timeout_ms (int): Server unresponsive timeout (default: 180,000ms)
                - flush_timeout_ms (int): Stream flush timeout (default: 300,000ms)
                - ack_callback (callable): Acknowledgment callback function (default: debug logging)
                Note: recovery is always True for maximum reliability
                Note: OAuth credentials are passed directly to create_stream(), not via token_factory
        """
        # The Zerobus SDK requires an https:// scheme on the ingest endpoint.
        if not ingest_endpoint.startswith("https://"):
            ingest_endpoint = f"https://{ingest_endpoint}"

        self.ingest_endpoint = ingest_endpoint
        self.workspace_url = workspace_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.table_name = table_name

        # ZerobusSdk requires server_endpoint and unity_catalog_url as a named parameter.
        # An `sdk` may be injected for testing (fake in-memory SDK/stream).
        self.sdk = sdk or ZerobusSdk(ingest_endpoint, unity_catalog_url=workspace_url)

        self.table_properties = TableProperties(
            table_name, salesforce_events_pb2.SalesforceEvent.DESCRIPTOR
        )

        # Configure stream options with enhanced recovery settings
        # Default to production-ready recovery configuration aligned with documentation
        default_config = {
            "recovery": True,  # Always enable recovery
            "recovery_retries": 5,  # Increased for production resilience
            "recovery_timeout_ms": 15000,  # 15 seconds per recovery attempt (faster than default 30s)
            "recovery_backoff_ms": 5000,  # 5 second backoff
            "server_lack_of_ack_timeout_ms": 180000,  # 3 minutes (increased from 1 minute)
            "max_inflight_records": 50000,  # Conservative for reliability (reduced from 50k)
            "flush_timeout_ms": 300000,  # 5 minutes for batch operations
        }

        # Merge user-provided options with defaults (user options take precedence)
        final_config = {**default_config, **(stream_config_options or {})}

        # Force recovery to always be True as per plan
        final_config["recovery"] = True

        self.stream = None
        self.logger = logging.getLogger(__name__)

        # Add optional acknowledgment callback for monitoring (if not provided)
        if "ack_callback" not in final_config:
            final_config["ack_callback"] = _DefaultAckCallback(self.logger)

        # Note: OAuth credentials (client_id, client_secret) are passed directly to
        # create_stream() method, not via token_factory in the configuration

        self.stream_config = StreamConfigurationOptions(**final_config)

    def get_stream_health(self) -> dict:
        """Get stream health information."""
        if not self.stream:
            return {"status": "no_stream", "healthy": False, "stream_id": None}

        return {
            "status": "active",
            "healthy": True,
            "stream_id": getattr(self.stream, "stream_id", None),
        }

    async def initialize_stream(self):
        """Create the ingest stream to the Delta table."""
        try:
            # Pass OAuth credentials directly to create_stream (per SDK best practices)
            self.stream = await self.sdk.create_stream(
                self.client_id, self.client_secret, self.table_properties, self.stream_config
            )
            self.logger.info(f"Initialized Zerobus stream to table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Zerobus stream: {e}")
            self.stream = None
            raise

    def build_record(
        self,
        salesforce_event_data: dict,
        org_id: str,
        payload_binary: bytes = None,
        schema_json: str = None,
    ) -> "salesforce_events_pb2.SalesforceEvent":
        """
        Convert a decoded Salesforce CDC event into a SalesforceEvent protobuf message.

        Pure transform (no I/O) so it is trivially unit-testable and can be batched by the
        caller before ingestion.

        Args:
            salesforce_event_data: Decoded Salesforce event data
            org_id: Salesforce organization ID
            payload_binary: Raw Avro binary payload from Salesforce (optional)
            schema_json: Avro schema JSON string for parsing (optional)
        """
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
        record_data = {k: v for k, v in salesforce_event_data.items() if k not in excluded_keys}
        record_data_json = json.dumps(record_data)

        # Use the Salesforce commit timestamp (epoch ms) from the ChangeEventHeader as the
        # event `timestamp`, not the forwarder's wall clock. commitTimestamp is the true
        # event time and is monotonic with replay order, so downstream sequencing and any
        # time-based reasoning are accurate. `processed_timestamp` remains the wall clock.
        commit_ts = change_header.get("commitTimestamp")
        event_timestamp = int(commit_ts) if commit_ts is not None else int(time.time() * 1000)

        return salesforce_events_pb2.SalesforceEvent(
            event_id=event_id,
            schema_id=schema_id,
            replay_id=replay_id,
            timestamp=event_timestamp,
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

    async def recreate(self):
        """Recreate the underlying Zerobus stream after a permanent failure."""
        self.stream = await self.sdk.recreate_stream(self.stream)
        self.logger.info("Successfully recreated Zerobus stream")

    async def ingest_batch(self, records: list) -> int:
        """
        Submit a batch of protobuf records and return the batch's final offset.

        Does NOT wait for durability — call ``wait_durable(offset)`` for that. Raises
        ``ZerobusException`` on permanent stream failure; the caller is responsible for
        ``recreate()`` + retry (so it can keep its checkpoint pinned until durable).
        """
        if not self.stream:
            await self.initialize_stream()
        return await self.stream.ingest_records_offset(records)

    async def ingest_batch_nowait(self, records: list):
        """Submit a batch fire-and-forget (no offset, no durability wait).

        Durability is handled asynchronously by the SDK's background sender; use
        ``flush()`` (e.g. on shutdown) to ensure buffered records are sent.
        """
        if not self.stream:
            await self.initialize_stream()
        self.stream.ingest_records_nowait(records)

    async def wait_durable(self, offset: int):
        """Block until all records up to ``offset`` are durably written to Delta."""
        await self.stream.wait_for_offset(offset)

    async def forward_event(
        self,
        salesforce_event_data: dict,
        org_id: str,
        payload_binary: bytes = None,
        schema_json: str = None,
    ):
        """
        Single-record convenience path: transform, ingest, and block until durable.

        The batched consumer in ``core.py`` uses ``build_record`` + ``ingest_batch`` +
        ``wait_durable`` directly; this wrapper is kept for backward compatibility and
        now provides a real durability barrier (logs success only after the record is
        durably written).
        """
        pb_event = self.build_record(salesforce_event_data, org_id, payload_binary, schema_json)
        record_ids = pb_event.record_ids
        record_id = record_ids[0] if record_ids else "unknown"

        try:
            offset = await self.ingest_batch([pb_event])
            await self.wait_durable(offset)
        except ZerobusException as e:
            # Permanent stream failure after all SDK recovery attempts: recreate and retry
            # the same record so nothing is dropped.
            self.logger.warning(
                f"Stream permanently failed after SDK recovery attempts, recreating. Error: {e}."
            )
            try:
                await self.recreate()
                offset = await self.stream.ingest_records_offset([pb_event])
                await self.wait_durable(offset)
            except Exception as retry_error:
                self.logger.error(f"Failed to recreate stream or retry ingestion: {retry_error}")
                self.stream = None  # Force reinitialization on next attempt
                raise

        # Log only AFTER durability is confirmed.
        self.logger.info(
            f"Durably ingested: {self.table_name} - {pb_event.entity_name} "
            f"{pb_event.change_type} {record_id}"
        )

    async def flush(self):
        """Flush any pending records to ensure they're written."""
        if self.stream:
            try:
                await self.stream.flush()
                self.logger.debug("Flushed pending records to Databricks")
            except ZerobusException as e:
                # Per Zerobus docs: recreate stream if flush fails with ZerobusException
                self.logger.warning(f"Flush failed, stream permanently failed, recreating: {e}")
                self.stream = await self.sdk.recreate_stream(self.stream)
                self.logger.info("Successfully recreated stream after flush failure")
                # Retry flush with new stream
                await self.stream.flush()
                self.logger.debug("Flush retry successful after stream recreation")
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
        - DATABRICKS_CLIENT_ID: OAuth Service Principal client ID
        - DATABRICKS_CLIENT_SECRET: OAuth Service Principal client secret
        - DATABRICKS_TABLE_NAME: Target table name (if table_name param not provided)

        Optional ZerobusSdk Stream Configuration (recovery always enabled):
        - ZEROBUS_MAX_INFLIGHT_RECORDS: Max records in flight (default: 50000)
        - ZEROBUS_RECOVERY_RETRIES: Recovery attempt count (default: 5)
        - ZEROBUS_RECOVERY_TIMEOUT_MS: Recovery timeout per attempt (default: 15000)
        - ZEROBUS_RECOVERY_BACKOFF_MS: Backoff between attempts (default: 5000)
        - ZEROBUS_SERVER_ACK_TIMEOUT_MS: Server unresponsive timeout (default: 180000)
        - ZEROBUS_FLUSH_TIMEOUT_MS: Stream flush timeout (default: 300000)
    """
    # Use provided table name or fallback to environment variable
    target_table_name = table_name or os.getenv("DATABRICKS_TABLE_NAME")

    required_vars = [
        "DATABRICKS_INGEST_ENDPOINT",
        "DATABRICKS_WORKSPACE_URL",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
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
        stream_config["max_inflight_records"] = int(os.getenv("ZEROBUS_MAX_INFLIGHT_RECORDS"))

    if os.getenv("ZEROBUS_RECOVERY_RETRIES"):
        stream_config["recovery_retries"] = int(os.getenv("ZEROBUS_RECOVERY_RETRIES"))

    if os.getenv("ZEROBUS_RECOVERY_TIMEOUT_MS"):
        stream_config["recovery_timeout_ms"] = int(os.getenv("ZEROBUS_RECOVERY_TIMEOUT_MS"))

    if os.getenv("ZEROBUS_RECOVERY_BACKOFF_MS"):
        stream_config["recovery_backoff_ms"] = int(os.getenv("ZEROBUS_RECOVERY_BACKOFF_MS"))

    if os.getenv("ZEROBUS_SERVER_ACK_TIMEOUT_MS"):
        stream_config["server_lack_of_ack_timeout_ms"] = int(
            os.getenv("ZEROBUS_SERVER_ACK_TIMEOUT_MS")
        )

    if os.getenv("ZEROBUS_FLUSH_TIMEOUT_MS"):
        stream_config["flush_timeout_ms"] = int(os.getenv("ZEROBUS_FLUSH_TIMEOUT_MS"))

    return DatabricksForwarder(
        ingest_endpoint=os.getenv("DATABRICKS_INGEST_ENDPOINT"),
        workspace_url=os.getenv("DATABRICKS_WORKSPACE_URL"),
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
        table_name=target_table_name,
        stream_config_options=stream_config if stream_config else None,
    )
