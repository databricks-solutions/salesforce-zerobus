"""
Main SalesforceZerobus API class providing simple interface for streaming
Salesforce Change Data Capture events to Databricks Delta tables.
"""

import asyncio
import contextlib
import logging
import signal
from typing import Any, Dict, Optional

import avro.schema
from zerobus.sdk.shared import ZerobusException

from .databricks import DatabricksForwarder, DatabricksReplayManager
from .pubsub import PubSub
from .utils import process_bitmap

# Sentinel pushed onto the internal queue to tell the consumer the producer has stopped
# and it should drain any remaining records and exit.
_SHUTDOWN_SENTINEL = object()


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
                "client_id": "your-service-principal-client-id",
                "client_secret": "your-service-principal-client-secret",
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
        timeout_seconds: float = 50.0,
        max_timeouts: int = 3,
        grpc_host: str = "api.pubsub.salesforce.com",
        grpc_port: int = 7443,
        api_version: str = "57.0",
        # New table management parameters
        auto_create_table: bool = True,
        backfill_historical: bool = True,
        # Zerobus SDK recovery configuration
        zerobus_max_inflight_records: int = 50000,
        zerobus_recovery_retries: int = 5,
        zerobus_recovery_timeout_ms: int = 30000,
        zerobus_recovery_backoff_ms: int = 5000,
        zerobus_server_ack_timeout_ms: int = 60000,
        zerobus_flush_timeout_ms: int = 300000,
        # Async pipeline tuning
        queue_maxsize: int = 2000,
        ingest_batch_size: int = 100,
        flush_interval_seconds: float = 5.0,
        max_batch_retries: int = 10,
        wait_for_durability: bool = True,
        # Backward compatibility
        sf_object: Optional[str] = None,
    ):
        """
        Initialize SalesforceZerobus streamer.

        Args:
            sf_object_channel: CDC channel name (e.g., "AccountChangeEvent", "CustomObject__cChangeEvent")
            databricks_table: Target Databricks table name (catalog.schema.table)
            salesforce_auth: Dict with keys: username, password, instance_url
            databricks_auth: Dict with keys: workspace_url, client_id, client_secret, ingest_endpoint
            batch_size: Number of events to fetch per request (default: 10)
            enable_replay_recovery: Enable zero-data-loss replay recovery (default: True)
            timeout_seconds: Idle read timeout; a keepalive FetchRequest is sent to
                Salesforce after this many seconds of no response (default: 50.0)
            max_timeouts: Max consecutive timeouts before recovery (default: 3)
            grpc_host: Salesforce gRPC host (default: api.pubsub.salesforce.com)
            grpc_port: Salesforce gRPC port (default: 7443)
            api_version: Salesforce API version (default: 57.0)
            auto_create_table: Auto-create Databricks table if it doesn't exist (default: True)
            backfill_historical: Start from EARLIEST for new tables to get historical data (default: True)
            zerobus_max_inflight_records: Max records in flight for Databricks ingestion (default: 50000)
            zerobus_recovery_retries: Number of Zerobus recovery attempts (default: 5)
            zerobus_recovery_timeout_ms: Zerobus recovery timeout per attempt in ms (default: 30000)
            zerobus_recovery_backoff_ms: Zerobus recovery backoff between attempts in ms (default: 5000)
            zerobus_server_ack_timeout_ms: Zerobus server unresponsive timeout in ms (default: 60000)
            zerobus_flush_timeout_ms: Zerobus stream flush timeout in ms (default: 300000)
            queue_maxsize: Max buffered events between the Salesforce reader and the
                Databricks writer; when full it applies backpressure to Salesforce (default: 2000)
            ingest_batch_size: Max events written to Zerobus per batch (default: 100)
            flush_interval_seconds: Max time a partial batch waits before being written (default: 5.0)
            max_batch_retries: Attempts per batch before failing loudly (default: 10)
            wait_for_durability: If True (default), block each batch until Zerobus durably
                acks before advancing (strongest guarantee, adds commit latency). If False,
                fire-and-forget for lower latency/higher throughput — still at-least-once via
                the SDK background sender, flush-on-shutdown, and table-based restart recovery.
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
            raise ValueError("Either sf_object_channel or sf_object parameter is required")

        # Validate required parameters
        self._validate_config(sf_object_channel, databricks_table, salesforce_auth, databricks_auth)

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

        # Async pipeline tuning
        self.queue_maxsize = queue_maxsize
        self.ingest_batch_size = ingest_batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.max_batch_retries = max_batch_retries
        # True  -> block each batch until Zerobus durably acks (strongest guarantee).
        # False -> fire-and-forget (lower latency/higher throughput; still at-least-once
        #          via the SDK background sender + flush-on-shutdown + table recovery).
        self.wait_for_durability = wait_for_durability

        # Store Zerobus SDK recovery configuration
        self.zerobus_config = {
            "max_inflight_records": zerobus_max_inflight_records,
            "recovery_retries": zerobus_recovery_retries,
            "recovery_timeout_ms": zerobus_recovery_timeout_ms,
            "recovery_backoff_ms": zerobus_recovery_backoff_ms,
            "server_lack_of_ack_timeout_ms": zerobus_server_ack_timeout_ms,
            "flush_timeout_ms": zerobus_flush_timeout_ms,
        }

        # Use the channel name directly for topic
        self.topic = f"/data/{sf_object_channel}"

        # Runtime state
        self.running = False
        self.org_id = None
        self._stopping = False
        self._queue = None
        self._producer_task = None
        # Last replay id whose batch has been durably written to Delta. Advanced only
        # after Zerobus acks, so it is a safe resume point for at-least-once delivery.
        self._last_durable_replay_id = None

        # Components (lazy initialized)
        self._pubsub_client = None
        self._databricks_forwarder = None
        self._replay_manager = None

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

        # Validate Salesforce auth - support both OAuth and SOAP authentication
        has_oauth = (
            "client_id" in salesforce_auth
            and "client_secret" in salesforce_auth
            and salesforce_auth.get("client_id")
            and salesforce_auth.get("client_secret")
        )

        has_soap = (
            "username" in salesforce_auth
            and "password" in salesforce_auth
            and salesforce_auth.get("username")
            and salesforce_auth.get("password")
        )

        has_instance = "instance_url" in salesforce_auth and salesforce_auth.get("instance_url")

        if not has_instance:
            raise ValueError("salesforce_auth must include 'instance_url'")

        if not (has_oauth or has_soap):
            raise ValueError(
                "salesforce_auth must include either:\n"
                "  - OAuth: 'client_id' and 'client_secret'\n"
                "  - SOAP: 'username' and 'password'"
            )

        # Validate Databricks auth
        required_db_keys = [
            "workspace_url",
            "client_id",
            "client_secret",
            "ingest_endpoint",
            "sql_endpoint",
        ]
        missing_db = [
            k for k in required_db_keys if k not in databricks_auth or not databricks_auth[k]
        ]
        if missing_db:
            raise ValueError(f"Missing required Databricks auth keys: {missing_db}")

    def _initialize_components(self):
        """Initialize all components for streaming."""
        self.logger.info(f"Initializing components for {self.sf_object} streaming")

        # Initialize PubSub client
        pubsub_args = {
            "url": self.salesforce_auth["instance_url"],
            "grpcHost": self.grpc_host,
            "grpcPort": str(self.grpc_port),
            "apiVersion": self.api_version,
            "topic": self.topic,
            "batchSize": str(self.batch_size),
            "timeout_seconds": self.timeout_seconds,
        }

        # Add OAuth credentials if present
        if "client_id" in self.salesforce_auth:
            pubsub_args["client_id"] = self.salesforce_auth["client_id"]
            pubsub_args["client_secret"] = self.salesforce_auth["client_secret"]

        # Add SOAP credentials if present
        if "username" in self.salesforce_auth:
            pubsub_args["username"] = self.salesforce_auth["username"]
            pubsub_args["password"] = self.salesforce_auth["password"]

        self._pubsub_client = PubSub(pubsub_args)

        # Initialize Databricks forwarder with Zerobus recovery configuration
        self._databricks_forwarder = DatabricksForwarder(
            ingest_endpoint=self.databricks_auth["ingest_endpoint"],
            workspace_url=self.databricks_auth["workspace_url"],
            client_id=self.databricks_auth["client_id"],
            client_secret=self.databricks_auth["client_secret"],
            table_name=self.databricks_table,
            stream_config_options=self.zerobus_config,
        )

        # Initialize replay manager if enabled
        if self.enable_replay_recovery:
            try:
                self._replay_manager = DatabricksReplayManager(
                    table_name=self.databricks_table,
                    object_name=self.sf_object,
                    workspace_url=self.databricks_auth["workspace_url"],
                    client_id=self.databricks_auth["client_id"],
                    client_secret=self.databricks_auth["client_secret"],
                    sql_endpoint=self.databricks_auth["sql_endpoint"],
                )
                self.logger.info("Replay recovery enabled - will resume from last position")
            except Exception as e:
                self.logger.warning(f"Failed to initialize replay manager: {e}")
                self._replay_manager = None
        else:
            self.logger.info("Replay recovery disabled - starting from LATEST")

        self.logger.info("Components initialized successfully")

    def _resolve_replay_params(self):
        """Resolve the subscription replay position (blocking; call via asyncio.to_thread).

        Creates the target table if needed and derives the replay decision in a single
        sequential step, replacing the old thread + threading.Event choreography that
        raced table creation and could silently fall back to LATEST (skipping backfill).
        """
        if not self._replay_manager:
            self.logger.info("Replay recovery disabled - starting from LATEST")
            return ("LATEST", "")

        try:
            replay_type, replay_id = self._replay_manager.get_subscription_params(
                auto_create_table=self.auto_create_table,
                backfill_historical=self.backfill_historical,
            )
            # Pre-fetch/cache the replay id so we don't re-query later.
            self._replay_manager.initialize_replay_recovery()
        except Exception as e:
            self.logger.warning(f"Replay manager failed, using LATEST: {e}")
            return ("LATEST", "")

        if replay_type == "CUSTOM":
            self.logger.info(f"Resuming from replay_id: {replay_id}")
        elif replay_type == "EARLIEST":
            self.logger.info("Starting historical backfill from EARLIEST")
        else:
            self.logger.info("Starting fresh subscription from LATEST")
        return replay_type, replay_id

    def _convert_bitmap(self, parsed_schema, header, field_key):
        """Convert a single CDC bitmap field to readable names (empty list on failure)."""
        raw = header.get(field_key, [])
        if not raw or not parsed_schema:
            return []
        try:
            return process_bitmap(parsed_schema, raw)
        except Exception as e:
            self.logger.warning(f"Could not convert {field_key} bitmap: {e}")
            return []

    async def _decode_event(self, evt):
        """Decode one Salesforce ConsumerEvent into a queue package.

        Raises on Avro decode failure (a poison event) rather than silently dropping it,
        so a schema problem is loud and never becomes a silent gap.
        """
        payload_bytes = evt.event.payload
        schema_id = evt.event.schema_id
        json_schema = await self._pubsub_client.get_schema_json(schema_id)
        decoded_event = self._pubsub_client.decode(json_schema, payload_bytes)

        # Add metadata (replay_id stored as fixed-position hex; see replay manager).
        decoded_event["event_id"] = evt.event.id
        decoded_event["schema_id"] = schema_id
        decoded_event["replay_id"] = evt.replay_id.hex()

        # Process CDC bitmap fields (best-effort; the raw payload is always preserved).
        if "ChangeEventHeader" in decoded_event:
            header = decoded_event["ChangeEventHeader"]
            try:
                parsed_schema = avro.schema.parse(json_schema)
            except Exception as e:
                self.logger.warning(f"Could not parse Avro schema for bitmap processing: {e}")
                parsed_schema = None

            decoded_event["converted_changed_fields"] = self._convert_bitmap(
                parsed_schema, header, "changedFields"
            )
            decoded_event["converted_nulled_fields"] = self._convert_bitmap(
                parsed_schema, header, "nulledFields"
            )
            decoded_event["converted_diff_fields"] = self._convert_bitmap(
                parsed_schema, header, "diffFields"
            )

            record_ids = header.get("recordIds", ["unknown"])
            self.logger.info(
                f"Received {header.get('entityName', 'Unknown')} "
                f"{header.get('changeType', 'Unknown')} "
                f"{record_ids[0] if record_ids else 'unknown'}"
            )

        return {
            "decoded_event": decoded_event,
            "payload_binary": payload_bytes,
            "schema_json": json_schema,
        }

    async def _produce(self, queue, replay_type, replay_id):
        """Subscribe to Salesforce and feed decoded events into the bounded queue.

        ``queue.put`` blocks when the queue is full, which suspends this coroutine and,
        via the events() async generator, stops sending FetchRequests — natural
        backpressure onto Salesforce when Zerobus ingestion lags.
        """
        try:
            async for evt in self._pubsub_client.events(
                self.topic, replay_type, replay_id, self.batch_size
            ):
                if not self.org_id:
                    self.org_id = self._pubsub_client.tenant_id
                package = await self._decode_event(evt)
                await queue.put(package)  # backpressure point
        except asyncio.CancelledError:
            self.logger.info("Producer cancelled (shutdown requested)")
            raise
        self.logger.warning("Salesforce event stream ended")

    async def _consume(self, queue):
        """Drain the queue, batch records, and ingest with a durability barrier.

        Advances the durable checkpoint only after Zerobus acks, and never drops a batch
        on failure (retries the same batch after recreating the stream). ``flush_interval``
        bounds latency when traffic is sparse; ``ingest_batch_size`` bounds it under load.
        """
        batch = []
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=self.flush_interval_seconds)
            except asyncio.TimeoutError:
                # Idle flush: write whatever has accumulated so latency stays bounded.
                if batch:
                    await self._flush_batch(batch)
                    batch = []
                continue

            if item is _SHUTDOWN_SENTINEL:
                queue.task_done()
                if batch:
                    await self._flush_batch(batch)
                    batch = []
                self.logger.info("Consumer drained queue; stopping")
                return

            batch.append(item)
            queue.task_done()
            if len(batch) >= self.ingest_batch_size:
                await self._flush_batch(batch)
                batch = []

    async def _flush_batch(self, batch):
        """Ingest a batch, block until durable, then advance the checkpoint.

        Retries the same batch (recreating the stream on ZerobusException) so nothing is
        dropped. After ``max_batch_retries`` the error is raised, failing the service
        loudly rather than skipping records — a crash resumes from the last durable
        replay id (at-least-once), whereas a silent skip would be a permanent gap.
        """
        if not batch:
            return

        forwarder = self._databricks_forwarder
        records = [
            forwarder.build_record(
                p["decoded_event"], self.org_id, p["payload_binary"], p["schema_json"]
            )
            for p in batch
        ]

        delay = 1
        for attempt in range(1, self.max_batch_retries + 1):
            try:
                if self.wait_for_durability:
                    # Durable: block until Zerobus acks the batch (strongest guarantee,
                    # but adds the server's ~seconds commit latency per batch).
                    offset = await forwarder.ingest_batch(records)
                    await forwarder.wait_durable(offset)
                else:
                    # Fire-and-forget: submit and move on. The SDK's background sender
                    # (recovery=True) writes to Delta, flush() on shutdown drains it, and
                    # restart recovery reads the table's max replay id — so this stays
                    # at-least-once, just without the per-batch durability wait.
                    await forwarder.ingest_batch_nowait(records)
                break
            except ZerobusException as e:
                self.logger.warning(
                    f"Batch ingest failed (attempt {attempt}/{self.max_batch_retries}): "
                    f"{e}. Recreating stream and retrying the same batch."
                )
                with contextlib.suppress(Exception):
                    await forwarder.recreate()
            except Exception as e:
                self.logger.error(
                    f"Unexpected batch ingest error (attempt {attempt}/"
                    f"{self.max_batch_retries}): {e}. Retrying the same batch."
                )
            if attempt >= self.max_batch_retries:
                self.logger.error(
                    "Exhausted batch retries; failing so the service restarts and "
                    "resumes from the last durable replay id (no silent gap)."
                )
                raise
            await asyncio.sleep(min(delay, 30))
            delay *= 2

        # FIFO queue + ordered batch ingest ⇒ the last event is the batch's max replay id.
        self._last_durable_replay_id = batch[-1]["decoded_event"].get("replay_id")
        verb = "Durably ingested" if self.wait_for_durability else "Submitted"
        self.logger.info(
            f"{verb} {len(records)} record(s) through replay " f"{self._last_durable_replay_id}"
        )

    def _request_stop(self):
        """Signal-handler callback: begin graceful drain-and-flush shutdown."""
        self.logger.info("Shutdown signal received; draining queue and flushing...")
        self._stopping = True
        if self._producer_task and not self._producer_task.done():
            self._producer_task.cancel()

    async def _run_pipeline(self, install_signals):
        """Run the producer/consumer pipeline until shutdown, then flush and close."""
        self.running = True
        self._stopping = False

        if install_signals:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, self._request_stop)
                except NotImplementedError:
                    # add_signal_handler is POSIX-only; on Windows rely on KeyboardInterrupt.
                    pass

        try:
            replay_type, replay_id = await asyncio.to_thread(self._resolve_replay_params)
            await self._databricks_forwarder.initialize_stream()
            if replay_type == "CUSTOM":
                self._last_durable_replay_id = replay_id

            self.logger.info(
                f"Starting subscription to {self.topic} "
                f"(mode={replay_type}, fetch_size={self.batch_size}, "
                f"ingest_batch={self.ingest_batch_size})"
            )

            queue = asyncio.Queue(maxsize=self.queue_maxsize)
            self._queue = queue
            producer = asyncio.create_task(self._produce(queue, replay_type, replay_id))
            consumer = asyncio.create_task(self._consume(queue))
            self._producer_task = producer

            done, _pending = await asyncio.wait(
                {producer, consumer}, return_when=asyncio.FIRST_COMPLETED
            )
            self._stopping = True

            if consumer in done:
                # Consumer exited first (fatal). Stop the producer and surface the error.
                producer.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await producer
                exc = consumer.exception()
                if exc is not None:
                    raise exc
            else:
                # Producer finished first (stream ended, fatal, or cancelled by signal).
                # Tell the consumer to drain the remainder and flush before exiting.
                await queue.put(_SHUTDOWN_SENTINEL)
                await consumer
                if not producer.cancelled():
                    exc = producer.exception()
                    if exc is not None:
                        raise exc
        finally:
            self.running = False
            await self._shutdown()

    async def _shutdown(self):
        """Flush and close the Zerobus stream and the Salesforce channel."""
        if self._databricks_forwarder:
            with contextlib.suppress(Exception):
                await self._databricks_forwarder.flush()
            with contextlib.suppress(Exception):
                await self._databricks_forwarder.close()
        channel = getattr(self._pubsub_client, "channel", None)
        if channel is not None:
            with contextlib.suppress(Exception):
                await channel.close()
        self.logger.info("Shutdown complete")

    async def _run(self):
        """Full lifecycle: init components, authenticate, run the pipeline."""
        self._initialize_components()
        self.logger.info("Authenticating with Salesforce...")
        await self._pubsub_client.authenticate()
        self.logger.info("Authentication successful!")
        await self._run_pipeline(install_signals=True)

    def start(self):
        """
        Start synchronous streaming (blocking) until interrupted (Ctrl+C / SIGTERM).

        Thin wrapper over the single-loop async pipeline; kept for backward compatibility.
        """
        self.logger.info(f"Starting SalesforceZerobus streaming for {self.sf_object}")
        try:
            asyncio.run(self._run())
        except KeyboardInterrupt:
            # POSIX installs signal handlers for a graceful drain; this covers platforms
            # (e.g. Windows) where loop.add_signal_handler is unavailable.
            self.logger.info("Interrupted; shutting down")

    async def stream_forever(self):
        """
        Start asynchronous streaming. Use inside an ``async with`` block.

        Components and authentication are set up by ``__aenter__``; this runs the
        producer/consumer pipeline until the stream ends or the block is exited.
        """
        if not self._pubsub_client:
            raise RuntimeError("Must initialize components first - use an 'async with' statement")
        self.logger.info(f"Starting async streaming for {self.sf_object}")
        # No OS signal handlers here: lifecycle is owned by the async-with caller.
        await self._run_pipeline(install_signals=False)

    async def __aenter__(self):
        """Async context manager entry: initialize components and authenticate."""
        self._initialize_components()
        self.logger.info("Authenticating with Salesforce...")
        await self._pubsub_client.authenticate()
        self.logger.info("Authentication successful!")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit: flush and close (idempotent)."""
        self.running = False
        await self._shutdown()
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
            "queue_size": self._queue.qsize() if self._queue is not None else 0,
            "org_id": self.org_id,
            "last_durable_replay_id": self._last_durable_replay_id,
        }

        # Basic Zerobus stream status (detailed health requires async)
        if self._databricks_forwarder:
            stats["zerobus_stream_active"] = self._databricks_forwarder.stream is not None

        # Zerobus configuration
        stats["zerobus_config"] = self.zerobus_config.copy()

        return stats
