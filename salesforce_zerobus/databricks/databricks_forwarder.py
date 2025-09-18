"""
databricks_forwarder.py

Handles forwarding Salesforce events to Databricks Delta tables using the Zerobus API.
"""

import json
import logging
import os
import time

from ..pubsub.proto import salesforce_events_pb2
from zerobus_sdk import TableProperties
from zerobus_sdk.aio import ZerobusSdk


class DatabricksForwarder:
    """
    Forwards Salesforce Change Data Capture events to Databricks Delta tables.
    """

    def __init__(
        self, ingest_endpoint: str, workspace_url: str, api_token: str, table_name: str
    ):
        """
        Initialize the Databricks forwarder.

        Args:
            ingest_endpoint: Databricks ingest endpoint
            workspace_url: Databricks workspace URL
            api_token: Databricks API token
            table_name: Target Delta table name
        """
        self.ingest_endpoint = ingest_endpoint
        self.workspace_url = workspace_url
        self.api_token = api_token
        self.table_name = table_name

        self.sdk = ZerobusSdk(ingest_endpoint, workspace_url, api_token)

        self.table_properties = TableProperties(
            table_name, salesforce_events_pb2.SalesforceEvent.DESCRIPTOR
        )

        self.stream = None
        self.logger = logging.getLogger(__name__)

    async def initialize_stream(self):
        """Create the ingest stream to the Delta table."""
        try:
            self.stream = await self.sdk.create_stream(self.table_properties)
            self.logger.info(f"Initialized stream to table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to initialize stream: {e}")
            raise

    async def forward_event(self, salesforce_event_data: dict, org_id: str, payload_binary: bytes = None, schema_json: str = None):
        """
        Convert Salesforce CDC event to protobuf and forward to Databricks.

        Args:
            salesforce_event_data: Decoded Salesforce event data
            org_id: Salesforce organization ID
            payload_binary: Raw Avro binary payload from Salesforce (optional)
            schema_json: Avro schema JSON string for parsing (optional)
        """
        if not self.stream:
            await self.initialize_stream()

        try:
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
                timestamp=int(time.time() * 1000),  # Current timestamp in milliseconds
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

            # Ingest the record
            await self.stream.ingest_record(pb_event)

            # Log successful forward
            record_id = record_ids[0] if record_ids else "unknown"
            self.logger.info(
                f"Written to Databricks: {self.table_name} - {entity_name} {change_type} {record_id}"
            )

        except Exception as e:
            self.logger.error(f"Failed to forward event to Databricks: {e}")
            # Re-raise to allow caller to handle
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
                self.logger.info("Closed Databricks stream")
            except Exception as e:
                self.logger.error(f"Failed to close stream: {e}")
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

    return DatabricksForwarder(
        ingest_endpoint=os.getenv("DATABRICKS_INGEST_ENDPOINT"),
        workspace_url=os.getenv("DATABRICKS_WORKSPACE_URL"),
        api_token=os.getenv("DATABRICKS_API_TOKEN"),
        table_name=target_table_name,
    )
