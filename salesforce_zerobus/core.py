"""
Main SalesforceZerobus API class providing simple interface for streaming
Salesforce Change Data Capture events to Databricks Delta tables.
"""

import asyncio
import logging
import threading
import time
from queue import Queue
from typing import Any, Dict, Optional

import avro.schema

from .databricks import DatabricksForwarder, DatabricksReplayManager
from .pubsub import PubSub
from .utils import FlowController, process_bitmap


class SalesforceZerobus:
    """
    Simple interface for streaming Salesforce CDC events to Databricks.

    Example:
        # Standard objects
        streamer = SalesforceZerobus(
            sf_object_channel="AccountChangeEvent",
            databricks_table="catalog.schema.account_events",
            salesforce_auth={
                "username": "user@company.com",
                "password": "password+token",
                "instance_url": "https://company.salesforce.com"
            },
            databricks_auth={
                "workspace_url": "https://workspace.cloud.databricks.com",
                "api_token": "dapi...",
                "ingest_endpoint": "workspace-id.ingest.cloud.databricks.com"
            }
        )

        # Custom objects
        streamer = SalesforceZerobus(
            sf_object_channel="CustomObject__cChangeEvent",
            databricks_table="catalog.schema.custom_events",
            # ... same auth dicts
        )

        # Backward compatibility (deprecated)
        streamer = SalesforceZerobus(
            sf_object="Account",  # Auto-converts to "AccountChangeEvent"
            # ... rest of config
        )

        # Synchronous (blocking)
        streamer.start()

        # Or asynchronous
        async with streamer:
            await streamer.stream_forever()
    """

    def __init__(
        self,
        sf_object_channel: Optional[str] = None,
        databricks_table: str = None,
        salesforce_auth: Dict[str, str] = None,
        databricks_auth: Dict[str, str] = None,
        batch_size: int = 10,
        enable_replay_recovery: bool = True,
        timeout_seconds: float = 300.0,
        max_timeouts: int = 3,
        grpc_host: str = "api.pubsub.salesforce.com",
        grpc_port: int = 7443,
        api_version: str = "57.0",
        # New table management parameters
        auto_create_table: bool = True,
        backfill_historical: bool = True,
        # Backward compatibility
        sf_object: Optional[str] = None,
    ):
        """
        Initialize SalesforceZerobus streamer.

        Args:
            sf_object_channel: CDC channel name (e.g., "AccountChangeEvent", "CustomObject__cChangeEvent")
            databricks_table: Target Databricks table name (catalog.schema.table)
            salesforce_auth: Dict with keys: username, password, instance_url
            databricks_auth: Dict with keys: workspace_url, api_token, ingest_endpoint
            batch_size: Number of events to fetch per request (default: 10)
            enable_replay_recovery: Enable zero-data-loss replay recovery (default: True)
            timeout_seconds: Timeout for semaphore operations (default: 300.0)
            max_timeouts: Max consecutive timeouts before recovery (default: 3)
            grpc_host: Salesforce gRPC host (default: api.pubsub.salesforce.com)
            grpc_port: Salesforce gRPC port (default: 7443)
            api_version: Salesforce API version (default: 57.0)
            auto_create_table: Auto-create Databricks table if it doesn't exist (default: True)
            backfill_historical: Start from EARLIEST for new tables to get historical data (default: True)
            sf_object: [DEPRECATED] Use sf_object_channel instead
        """
        # Handle backward compatibility and new parameter
        if sf_object_channel and sf_object:
            raise ValueError(
                "Cannot specify both sf_object_channel and sf_object. Use sf_object_channel."
            )

        if sf_object and not sf_object_channel:
            # Backward compatibility - auto-generate channel name
            sf_object_channel = f"{sf_object}ChangeEvent"
            self.sf_object = sf_object  # For logging compatibility
        elif sf_object_channel:
            # New preferred method - extract object name for logging
            if sf_object_channel.endswith("ChangeEvent"):
                self.sf_object = sf_object_channel[:-11]  # Remove "ChangeEvent"
            else:
                self.sf_object = sf_object_channel
        else:
            raise ValueError(
                "Either sf_object_channel or sf_object parameter is required"
            )

        # Validate required parameters
        self._validate_config(
            sf_object_channel, databricks_table, salesforce_auth, databricks_auth
        )

        # Store configuration
        self.sf_object_channel = sf_object_channel
        self.databricks_table = databricks_table
        self.salesforce_auth = salesforce_auth.copy()
        self.databricks_auth = databricks_auth.copy()
        self.batch_size = batch_size
        self.enable_replay_recovery = enable_replay_recovery
        self.timeout_seconds = timeout_seconds
        self.max_timeouts = max_timeouts
        self.grpc_host = grpc_host
        self.grpc_port = grpc_port
        self.api_version = api_version
        self.auto_create_table = auto_create_table
        self.backfill_historical = backfill_historical

        # Use the channel name directly for topic
        self.topic = f"/data/{sf_object_channel}"

        # Runtime state
        self.running = False
        self.event_queue = Queue()
        self.org_id = None

        # Components (lazy initialized)
        self._pubsub_client = None
        self._databricks_forwarder = None
        self._replay_manager = None
        self._flow_controller = None

        # Setup logging
        self.logger = logging.getLogger(f"{__name__}.{sf_object}")

    def _validate_config(
        self,
        sf_object_channel: str,
        databricks_table: str,
        salesforce_auth: Dict[str, str],
        databricks_auth: Dict[str, str],
    ):
        """Validate required configuration parameters."""
        if not sf_object_channel:
            raise ValueError("sf_object_channel parameter is required")

        if not databricks_table:
            raise ValueError("databricks_table parameter is required")

        # Validate Salesforce auth
        required_sf_keys = ["username", "password", "instance_url"]
        missing_sf = [
            k
            for k in required_sf_keys
            if k not in salesforce_auth or not salesforce_auth[k]
        ]
        if missing_sf:
            raise ValueError(f"Missing required Salesforce auth keys: {missing_sf}")

        # Validate Databricks auth
        required_db_keys = [
            "workspace_url",
            "api_token",
            "ingest_endpoint",
            "sql_endpoint",
        ]
        missing_db = [
            k
            for k in required_db_keys
            if k not in databricks_auth or not databricks_auth[k]
        ]
        if missing_db:
            raise ValueError(f"Missing required Databricks auth keys: {missing_db}")

    def _initialize_components(self):
        """Initialize all components for streaming."""
        self.logger.info(f"Initializing components for {self.sf_object} streaming")

        # Initialize flow controller
        self._flow_controller = FlowController(
            semaphore_count=1,
            acquire_timeout=self.timeout_seconds,
            max_consecutive_timeouts=self.max_timeouts,
            logger=self.logger,
        )

        # Initialize PubSub client
        pubsub_args = {
            "url": self.salesforce_auth["instance_url"],
            "username": self.salesforce_auth["username"],
            "password": self.salesforce_auth["password"],
            "grpcHost": self.grpc_host,
            "grpcPort": str(self.grpc_port),
            "apiVersion": self.api_version,
            "topic": self.topic,
            "batchSize": str(self.batch_size),
        }

        self._pubsub_client = PubSub(pubsub_args)
        self._pubsub_client.set_flow_controller(self._flow_controller)

        # Initialize Databricks forwarder
        self._databricks_forwarder = DatabricksForwarder(
            ingest_endpoint=self.databricks_auth["ingest_endpoint"],
            workspace_url=self.databricks_auth["workspace_url"],
            api_token=self.databricks_auth["api_token"],
            table_name=self.databricks_table,
        )

        # Initialize replay manager if enabled
        if self.enable_replay_recovery:
            try:
                self._replay_manager = DatabricksReplayManager(
                    table_name=self.databricks_table,
                    object_name=self.sf_object,
                    workspace_url=self.databricks_auth["workspace_url"],
                    api_token=self.databricks_auth["api_token"],
                    sql_endpoint=self.databricks_auth["sql_endpoint"],
                    sql_workspace_url=self.databricks_auth.get("sql_workspace_url"),
                    sql_api_token=self.databricks_auth.get("sql_api_token"),
                )
                self.logger.info(
                    "Replay recovery enabled - will resume from last position"
                )
            except Exception as e:
                self.logger.warning(f"Failed to initialize replay manager: {e}")
                self._replay_manager = None
        else:
            self.logger.info("Replay recovery disabled - starting from LATEST")

        self.logger.info("Components initialized successfully")

    async def _initialize_databricks_async(self):
        """Initialize Databricks components for async operation."""
        # First, ensure table exists and get subscription params (triggers table creation if needed)
        if self._replay_manager:
            # This call will create the table if it doesn't exist
            replay_type, replay_id = self._replay_manager.get_subscription_params(
                auto_create_table=self.auto_create_table,
                backfill_historical=self.backfill_historical,
            )
            self.logger.debug(
                f"Table initialization complete, replay mode: {replay_type}"
            )

            # Initialize replay recovery (pre-fetch replay_id to avoid blocking later)
            self._replay_manager.initialize_replay_recovery()

        # Now initialize the stream (table should exist at this point)
        if self._databricks_forwarder:
            await self._databricks_forwarder.initialize_stream()
            self.logger.info("Databricks stream initialized")

    def _get_subscription_params(self):
        """Get replay parameters for subscription (table should already exist from async init)."""
        if self._replay_manager:
            try:
                # Table should already exist from _initialize_databricks_async, so just get the params
                replay_type, replay_id = self._replay_manager.get_subscription_params(
                    auto_create_table=False,  # Don't create table again
                    backfill_historical=self.backfill_historical,
                )
                if replay_type == "CUSTOM":
                    self.logger.info(f"Resuming from replay_id: {replay_id}")
                elif replay_type == "EARLIEST":
                    self.logger.info("Starting historical backfill from EARLIEST")
                else:
                    self.logger.info("Starting fresh subscription from LATEST")
                return replay_type, replay_id
            except Exception as e:
                self.logger.warning(f"Replay manager failed, using LATEST: {e}")

        return "LATEST", ""

    def _salesforce_event_callback(self, event, pubsub):
        """Callback for processing Salesforce events."""
        try:
            if event.events:
                # Store org_id from first successful connection
                if not self.org_id:
                    self.org_id = pubsub.tenant_id

                # Process each event
                for evt in event.events:
                    try:
                        # Decode event
                        payload_bytes = evt.event.payload
                        schema_id = evt.event.schema_id
                        json_schema = pubsub.get_schema_json(schema_id)
                        decoded_event = pubsub.decode(json_schema, payload_bytes)

                        # Add metadata
                        decoded_event["event_id"] = evt.event.id
                        decoded_event["schema_id"] = schema_id
                        decoded_event["replay_id"] = evt.replay_id.hex()

                        # Process CDC bitmap fields
                        if "ChangeEventHeader" in decoded_event:
                            header = decoded_event["ChangeEventHeader"]

                            # Parse schema once for all bitmap processing
                            try:
                                parsed_schema = avro.schema.parse(json_schema)
                            except Exception as e:
                                self.logger.warning(
                                    f"Could not parse Avro schema for bitmap processing: {e}"
                                )
                                parsed_schema = None

                            # Convert changedFields bitmap to readable names
                            changed_fields = header.get("changedFields", [])
                            if changed_fields and parsed_schema:
                                try:
                                    converted_fields = process_bitmap(
                                        parsed_schema, changed_fields
                                    )
                                    decoded_event["converted_changed_fields"] = (
                                        converted_fields
                                    )
                                except Exception as e:
                                    self.logger.warning(
                                        f"Could not convert changedFields bitmap: {e}"
                                    )
                                    decoded_event["converted_changed_fields"] = []
                            else:
                                decoded_event["converted_changed_fields"] = []

                            # Convert nulledFields bitmap to readable names
                            nulled_fields = header.get("nulledFields", [])
                            if nulled_fields and parsed_schema:
                                try:
                                    converted_nulled_fields = process_bitmap(
                                        parsed_schema, nulled_fields
                                    )
                                    decoded_event["converted_nulled_fields"] = (
                                        converted_nulled_fields
                                    )
                                except Exception as e:
                                    self.logger.warning(
                                        f"Could not convert nulledFields bitmap: {e}"
                                    )
                                    decoded_event["converted_nulled_fields"] = []
                            else:
                                decoded_event["converted_nulled_fields"] = []

                            # Convert diffFields bitmap to readable names
                            diff_fields = header.get("diffFields", [])
                            if diff_fields and parsed_schema:
                                try:
                                    converted_diff_fields = process_bitmap(
                                        parsed_schema, diff_fields
                                    )
                                    decoded_event["converted_diff_fields"] = (
                                        converted_diff_fields
                                    )
                                except Exception as e:
                                    self.logger.warning(
                                        f"Could not convert diffFields bitmap: {e}"
                                    )
                                    decoded_event["converted_diff_fields"] = []
                            else:
                                decoded_event["converted_diff_fields"] = []

                            # Log received event
                            entity_name = header.get("entityName", "Unknown")
                            change_type = header.get("changeType", "Unknown")
                            record_ids = header.get("recordIds", ["unknown"])
                            record_id = record_ids[0] if record_ids else "unknown"

                            self.logger.info(
                                f"Received {entity_name} {change_type} {record_id}"
                            )

                        # Queue for async processing with binary payload and schema for Avro parsing
                        event_package = {
                            "decoded_event": decoded_event,
                            "payload_binary": payload_bytes,
                            "schema_json": json_schema
                        }
                        self.event_queue.put(event_package)

                    except Exception as e:
                        self.logger.error(f"Error processing event: {e}")
            else:
                self.logger.debug("Keepalive message received")

        except Exception as e:
            self.logger.error(f"Critical error in event callback: {e}")
        finally:
            # Release semaphore for flow control
            if event.events and event.pending_num_requested == 0:
                if self._flow_controller.release():
                    self.logger.debug("Released semaphore for batch completion")
            elif not event.events:
                if self._flow_controller.release():
                    self.logger.debug("Released semaphore for keepalive")

    async def _process_event_queue(self):
        """Process queued events and forward to Databricks."""
        while self.running:
            try:
                if not self.event_queue.empty():
                    event_package = self.event_queue.get_nowait()
                    await self._databricks_forwarder.forward_event(
                        event_package["decoded_event"],
                        self.org_id,
                        event_package["payload_binary"],
                        event_package["schema_json"]
                    )
                    self.event_queue.task_done()
                else:
                    await asyncio.sleep(0.1)
            except Exception as e:
                self.logger.error(f"Error processing event queue: {e}")
                await asyncio.sleep(1)

    async def _health_monitor(self):
        """Monitor health and log statistics."""
        last_report = time.time()
        report_interval = 300  # 5 minutes

        while self.running:
            try:
                current_time = time.time()
                if current_time - last_report >= report_interval:
                    # Log health statistics
                    self._flow_controller.log_health_report()

                    # Check queue health
                    queue_size = self.event_queue.qsize()
                    if queue_size > 100:
                        self.logger.warning(
                            f"Event queue backing up: {queue_size} events"
                        )
                    elif queue_size > 0:
                        self.logger.info(f"Queue status: {queue_size} events pending")

                    last_report = current_time

                await asyncio.sleep(30)  # Check every 30 seconds

            except Exception as e:
                self.logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(60)

    def start(self):
        """
        Start synchronous streaming (blocking).
        This method will run until interrupted with Ctrl+C.
        """
        self.logger.info(f"Starting SalesforceZerobus streaming for {self.sf_object}")
        self._initialize_components()
        self.running = True

        try:
            self.logger.info("Authenticating with Salesforce...")
            self._pubsub_client.auth()
            self.logger.info("Authentication successful!")
            self.logger.info("Initializing Databricks connection...")
            self.background_loop = asyncio.new_event_loop()

            def run_async():
                asyncio.set_event_loop(self.background_loop)
                self.background_loop.run_until_complete(
                    self._initialize_databricks_async()
                )
                self.background_loop.run_until_complete(self._run_background_tasks())

            self.async_thread = threading.Thread(target=run_async, daemon=True)
            self.async_thread.start()

            import time

            time.sleep(2)
            self.logger.info("Databricks connection initialized")

            replay_type, replay_id = self._get_subscription_params()

            self.logger.info(f"Starting subscription to {self.topic}")
            self.logger.info(f"Batch size: {self.batch_size}, Mode: {replay_type}")

            # Start streaming with robust error handling and retry logic
            # The updated subscribe method now handles RST_STREAM and other gRPC errors automatically
            self._pubsub_client.subscribe(
                self.topic,
                replay_type,
                replay_id,
                self.batch_size,
                self._salesforce_event_callback,
            )

        except KeyboardInterrupt:
            self.logger.info("Shutting down gracefully...")
        except Exception as e:
            # Enhanced error logging with more context
            import traceback
            self.logger.error(f"Critical streaming error: {e}")
            self.logger.error(f"Error type: {type(e).__name__}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")

            # Check if this is a gRPC error and log additional details
            if hasattr(e, 'code') and hasattr(e, 'details'):
                self.logger.error(f"gRPC Status Code: {e.code()}")
                self.logger.error(f"gRPC Details: {e.details()}")

            # Re-raise to allow higher-level error handling
            raise
        finally:
            self.running = False
            self.logger.info("Cleaning up resources...")

            # Close gRPC channel properly
            if hasattr(self, "_pubsub_client") and hasattr(self._pubsub_client, "channel"):
                try:
                    self._pubsub_client.channel.close()
                    self.logger.info("gRPC channel closed successfully")
                except Exception as e:
                    self.logger.warning(f"Error closing gRPC channel: {e}")

            # Stop background loop and wait for async thread
            if hasattr(self, "background_loop"):
                try:
                    self.background_loop.call_soon_threadsafe(self.background_loop.stop)
                except RuntimeError:
                    # Loop may already be stopped
                    pass
            if hasattr(self, "async_thread"):
                self.async_thread.join(timeout=5)
                if self.async_thread.is_alive():
                    self.logger.warning("Background thread did not stop within timeout")

            self.logger.info("Cleanup completed")

    async def _run_background_tasks(self):
        """Run background async tasks (event processing and health monitoring only)."""
        try:
            tasks = [
                asyncio.create_task(self._process_event_queue()),
                asyncio.create_task(self._health_monitor()),
            ]

            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()

        except Exception as e:
            self.logger.error(f"Background tasks error: {e}")
        finally:
            if self._databricks_forwarder:
                await self._databricks_forwarder.close()

    async def _run_async_tasks(self):
        """Run async tasks (Databricks forwarding and health monitoring)."""
        try:
            await self._initialize_databricks_async()

            tasks = [
                asyncio.create_task(self._process_event_queue()),
                asyncio.create_task(self._health_monitor()),
            ]

            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()

        except Exception as e:
            self.logger.error(f"Async tasks error: {e}")
        finally:
            if self._databricks_forwarder:
                await self._databricks_forwarder.close()

    async def stream_forever(self):
        """
        Start asynchronous streaming.
        Use this method in async contexts.
        """
        if not self._pubsub_client:
            raise RuntimeError(
                "Must initialize components first - use async with statement"
            )

        self.logger.info(f"Starting async streaming for {self.sf_object}")
        self.running = True

        try:
            replay_type, replay_id = self._get_subscription_params()

            self.logger.info(f"Starting subscription to {self.topic}")
            self.logger.info(f"Batch size: {self.batch_size}, Mode: {replay_type}")

            async_task = asyncio.create_task(self._run_async_tasks())

            subscription_task = asyncio.create_task(
                asyncio.to_thread(
                    self._pubsub_client.subscribe,
                    self.topic,
                    replay_type,
                    replay_id,
                    self.batch_size,
                    self._salesforce_event_callback,
                )
            )

            done, pending = await asyncio.wait(
                [async_task, subscription_task], return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        finally:
            self.running = False

    async def __aenter__(self):
        """Async context manager entry."""
        self._initialize_components()

        self.logger.info("Authenticating with Salesforce...")
        self._pubsub_client.auth()
        self.logger.info("Authentication successful!")

        await self._initialize_databricks_async()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        self.running = False
        if self._databricks_forwarder:
            await self._databricks_forwarder.close()

        self.logger.info("SalesforceZerobus streaming stopped")
        return False

    def get_stats(self) -> Dict[str, Any]:
        """Get current streaming statistics."""
        stats = {
            "sf_object_channel": self.sf_object_channel,
            "sf_object": self.sf_object,
            "topic": self.topic,
            "databricks_table": self.databricks_table,
            "running": self.running,
            "queue_size": self.event_queue.qsize(),
            "org_id": self.org_id,
        }

        if self._flow_controller:
            stats.update(self._flow_controller.get_health_status())

        return stats
