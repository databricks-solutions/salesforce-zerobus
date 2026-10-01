"""
PubSub.py

This file defines the class `PubSub`, which contains functionality for
subscriber clients to connect to Salesforce Pub/Sub API.
"""

import asyncio
import contextlib
import io
import logging
import os
import threading
import time
import uuid
import xml.etree.ElementTree as et
from urllib.parse import urlparse

import avro.io
import avro.schema
import certifi
import grpc
import grpc.aio
import requests
from dotenv import load_dotenv

from .proto import pubsub_api_pb2 as pb2
from .proto import pubsub_api_pb2_grpc as pb2_grpc

load_dotenv()

with open(certifi.where(), "rb") as f:
    secure_channel_credentials = grpc.ssl_channel_credentials(f.read())


def get_argument(key, argument_dict):
    """Get configuration value from command line args or environment variables."""
    if key in argument_dict and argument_dict[key] is not None:
        return argument_dict[key]

    env_map = {
        "url": "SALESFORCE_URL",
        "username": "SALESFORCE_USERNAME",
        "password": "SALESFORCE_PASSWORD",
        "client_id": "SALESFORCE_CLIENT_ID",
        "client_secret": "SALESFORCE_CLIENT_SECRET",
        "grpcHost": "GRPC_HOST",
        "grpcPort": "GRPC_PORT",
        "apiVersion": "API_VERSION",
        "topic": "TOPIC",
        "batchSize": "PUBSUB_BATCH_SIZE",
    }

    env_var = env_map.get(key, key.upper())
    return os.getenv(env_var)


class PubSub(object):
    """
    Class with helpers to use the Salesforce Pub/Sub API.
    """

    json_schema_dict = {}

    def __init__(self, argument_dict):
        # Initialize logger first
        self.logger = logging.getLogger(__name__)

        self.url = get_argument("url", argument_dict)
        self.username = get_argument("username", argument_dict)
        self.password = get_argument("password", argument_dict)
        self.client_id = get_argument("client_id", argument_dict)
        self.client_secret = get_argument("client_secret", argument_dict)
        self.metadata = None
        grpc_host = get_argument("grpcHost", argument_dict)
        grpc_port = get_argument("grpcPort", argument_dict)
        pubsub_url = grpc_host + ":" + grpc_port

        # Store original gRPC connection details for reconnection
        self._grpc_host = grpc_host
        self._grpc_port = grpc_port

        # Store timeout configuration (centralized from core.py)
        self.timeout_seconds = float(argument_dict.get("timeout_seconds", 50.0))

        # Connection resilience settings
        self.max_retries = 5
        self.base_retry_delay = 1.0
        self.max_retry_delay = 60.0

        # Configure gRPC channel with keepalive settings optimized for long-lived streaming
        channel_options = [
            (
                "grpc.keepalive_time_ms",
                60000,
            ),  # Send keepalive every 60 seconds (aligns with Salesforce 60s requirement)
            (
                "grpc.keepalive_timeout_ms",
                10000,
            ),  # Wait 10 seconds for keepalive response
            (
                "grpc.keepalive_permit_without_calls",
                True,
            ),  # Allow keepalive on idle connections
            ("grpc.http2.max_pings_without_data", 0),  # Unlimited pings without data
            (
                "grpc.http2.min_time_between_pings_ms",
                30000,
            ),  # Min 30 seconds between pings
            (
                "grpc.http2.min_ping_interval_without_data_ms",
                30000,
            ),  # 30 seconds without data before ping
            (
                "grpc.max_connection_idle_ms",
                7200000,
            ),  # Keep connection for 2 hours when idle
        ]

        # Create an async (grpc.aio) channel. The client runs inside a single asyncio
        # event loop. The x-client-trace-id that used to be injected via a synchronous
        # interceptor is now attached to the per-call metadata in authenticate() instead
        # (sync interceptors do not attach to aio channels).
        self.channel = grpc.aio.secure_channel(
            pubsub_url, secure_channel_credentials, options=channel_options
        )
        self.stub = pb2_grpc.PubSubStub(self.channel)
        self.session_id = None
        self.pb2 = pb2
        self.topic_name = get_argument("topic", argument_dict)
        # If the API version is not provided as an argument, use a default value
        if get_argument("apiVersion", argument_dict) is None:
            self.apiVersion = "57.0"
        else:
            # Otherwise, get the version from the argument
            self.apiVersion = get_argument("apiVersion", argument_dict)
        # Event deduplication as per Salesforce recommendations. Access is guarded by a
        # lock so the dedup cache stays correct even though auth runs in a worker thread.
        self._processed_event_ids = set()
        self._max_event_ids_cache = 10000  # Limit cache size to prevent memory issues
        self._event_ids_lock = threading.Lock()

        # Store last RPC ID for support troubleshooting
        self._last_rpc_id = None

    def _is_event_duplicate(self, event_id):
        """
        Check if an event has already been processed using the system-generated ID field.
        Implements Salesforce-recommended deduplication logic.
        """
        with self._event_ids_lock:
            return event_id in self._processed_event_ids

    def _mark_event_processed(self, event_id):
        """
        Mark an event as processed for deduplication.
        Maintains cache size to prevent memory issues.
        """
        with self._event_ids_lock:
            # If cache is full, remove oldest entries (FIFO)
            if len(self._processed_event_ids) >= self._max_event_ids_cache:
                # Convert to list, remove first half, convert back to set
                event_ids_list = list(self._processed_event_ids)
                self._processed_event_ids = set(event_ids_list[len(event_ids_list) // 2 :])
                self.logger.debug(
                    f"Event ID cache trimmed to {len(self._processed_event_ids)} entries"
                )

            self._processed_event_ids.add(event_id)
            self.logger.debug(f"Marked event {event_id} as processed")

    def get_last_rpc_id(self):
        """
        Get the last RPC ID from error responses for support troubleshooting.
        Returns the RPC ID as recommended by Salesforce documentation.
        """
        return self._last_rpc_id

    def _validate_auth_headers(self):
        """
        Validate authentication headers format per Salesforce documentation.
        Ensures compliance with required header formats.
        """
        if not self.session_id:
            raise ValueError("Session ID (accesstoken) is required")

        if not self.url:
            raise ValueError("Instance URL is required")

        if not self.tenant_id:
            raise ValueError("Tenant ID (Organization ID) is required")

        # Validate URL format
        if not (self.url.startswith("https://") or self.url.startswith("http://")):
            self.logger.warning(f"Instance URL should use HTTPS: {self.url}")

        # Validate Org ID format (15 or 18 character Salesforce ID)
        if len(self.tenant_id) not in [15, 18]:
            self.logger.warning(
                f"Tenant ID format may be invalid (should be 15 or 18 chars): {self.tenant_id}"
            )

        # Log successful validation
        self.logger.debug(f"Authentication headers validated: org={self.tenant_id}, url={self.url}")

    def _is_retryable_error(self, e):
        """
        Determine if a gRPC error is retryable.
        """
        if isinstance(e, grpc.RpcError):
            status_code = e.code()
            # Retry on connection issues, timeouts, and temporary failures
            retryable_codes = [
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.INTERNAL,
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                grpc.StatusCode.ABORTED,
                grpc.StatusCode.UNAUTHENTICATED,  # For authentication refresh
            ]
            return status_code in retryable_codes
        return False

    def _determine_retry_strategy(
        self, error, last_processed_replay_id, original_replay_type, original_replay_id
    ):
        """
        Determine the appropriate retry strategy based on Salesforce documentation.
        Returns tuple of (replay_type, replay_id) or None if non-retryable.
        """
        if not isinstance(error, grpc.RpcError):
            return None

        status_code = error.code()
        error_details = error.details() if hasattr(error, "details") else ""

        # Convert bytes replay ID to hex for logging
        replay_id_display = last_processed_replay_id.hex() if last_processed_replay_id else "None"
        self.logger.info(
            f"Determining retry strategy for error: {status_code}, details: {error_details}, last_replay_id: {replay_id_display}"
        )

        # Non-retryable errors
        non_retryable_codes = [
            grpc.StatusCode.INVALID_ARGUMENT,
            grpc.StatusCode.NOT_FOUND,
            grpc.StatusCode.ALREADY_EXISTS,
            grpc.StatusCode.PERMISSION_DENIED,
            grpc.StatusCode.FAILED_PRECONDITION,
            grpc.StatusCode.OUT_OF_RANGE,
            grpc.StatusCode.UNIMPLEMENTED,
            grpc.StatusCode.DATA_LOSS,
        ]

        # Special case: expired/invalid replay ID should fall back to LATEST
        if status_code == grpc.StatusCode.INVALID_ARGUMENT and "Replay ID" in error_details:
            self.logger.warning(
                "Replay ID is invalid or expired - falling back to LATEST replay strategy"
            )
            return ("LATEST", None)

        if status_code in non_retryable_codes:
            return None

        # Authentication errors - retry with original parameters after refresh
        if status_code == grpc.StatusCode.UNAUTHENTICATED:
            self.logger.info(
                "Authentication error - will refresh token and retry with original parameters"
            )
            return (original_replay_type, original_replay_id)

        # Temporary server errors - use CUSTOM with last processed replay ID if available
        if status_code in [
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.ABORTED,
        ]:
            if last_processed_replay_id:
                replay_display = (
                    last_processed_replay_id.hex()
                    if isinstance(last_processed_replay_id, bytes)
                    else last_processed_replay_id
                )
                self.logger.info(
                    f"Temporary server error - retrying with CUSTOM replay from last processed: {replay_display}"
                )
                return ("CUSTOM", last_processed_replay_id)
            else:
                self.logger.info(
                    "Temporary server error - no replay ID available, using original parameters"
                )
                return (original_replay_type, original_replay_id)

        # Resource exhaustion - use CUSTOM with last processed replay ID with longer backoff
        if status_code == grpc.StatusCode.RESOURCE_EXHAUSTED:
            if last_processed_replay_id:
                replay_display = (
                    last_processed_replay_id.hex()
                    if isinstance(last_processed_replay_id, bytes)
                    else last_processed_replay_id
                )
                self.logger.info(
                    f"Resource exhausted - retrying with CUSTOM replay from: {replay_display}"
                )
                return ("CUSTOM", last_processed_replay_id)
            else:
                self.logger.info("Resource exhausted - using LATEST to reduce load")
                return ("LATEST", "")

        # Timeout errors - continue from where we left off if possible
        if status_code == grpc.StatusCode.DEADLINE_EXCEEDED:
            if last_processed_replay_id:
                replay_display = (
                    last_processed_replay_id.hex()
                    if isinstance(last_processed_replay_id, bytes)
                    else last_processed_replay_id
                )
                self.logger.info(
                    f"Deadline exceeded - resuming with CUSTOM replay from: {replay_display}"
                )
                return ("CUSTOM", last_processed_replay_id)
            else:
                self.logger.info(
                    "Deadline exceeded - no processed events, retrying with original parameters"
                )
                return (original_replay_type, original_replay_id)

        # Handle corrupted replay ID scenarios (look for specific error messages)
        if "replay" in error_details.lower() and (
            "invalid" in error_details.lower() or "corrupt" in error_details.lower()
        ):
            self.logger.warning("Detected corrupted replay ID error - switching to LATEST")
            return ("LATEST", "")

        # Default retry strategy for other retryable errors
        if self._is_retryable_error(error):
            if last_processed_replay_id:
                return ("CUSTOM", last_processed_replay_id)
            else:
                return (original_replay_type, original_replay_id)

        # Non-retryable
        return None

    def _get_retry_delay(self, attempt):
        """
        Calculate exponential backoff delay with jitter following Salesforce recommendations.
        Salesforce recommends increasing time between calls to prevent repeated errors.
        """
        # Base delay increases more aggressively per Salesforce guidance
        base_delay = self.base_retry_delay * (2**attempt)

        # Cap at maximum delay
        delay = min(base_delay, self.max_retry_delay)

        # Add jitter (10-20% variance) to prevent thundering herd
        import random

        jitter_factor = 0.1 + (random.random() * 0.1)  # 10-20% jitter
        jitter = delay * jitter_factor

        final_delay = delay + jitter

        self.logger.debug(
            f"Retry attempt {attempt + 1}: base_delay={base_delay:.2f}s, final_delay={final_delay:.2f}s"
        )
        return final_delay

    def _log_grpc_error(self, e, context=""):
        """
        Log gRPC errors with detailed information including custom error codes from trailers.
        Follows Salesforce documentation for comprehensive error handling.
        """
        if isinstance(e, grpc.RpcError):
            # Extract basic error information
            status_code = e.code()
            details = e.details()
            debug_string = getattr(e, "debug_error_string", lambda: "N/A")()

            # Extract custom error codes and RPC ID from trailers per Salesforce docs
            custom_error_code = None
            rpc_id = None

            try:
                # Get trailers from the exception
                trailers = e.trailing_metadata()
                if trailers:
                    for key, value in trailers:
                        if key == "sfdc-error-code":
                            custom_error_code = value
                        elif key == "x-rpc-id":
                            rpc_id = value
                        elif key.startswith("sfdc-"):
                            # Log other Salesforce-specific trailer keys
                            self.logger.debug(f"Salesforce trailer {key}: {value}")
            except Exception as trailer_error:
                self.logger.debug(f"Could not extract trailers: {trailer_error}")

            # Comprehensive error logging
            error_msg = (
                f"gRPC error {context}: "
                f"code={status_code}, "
                f"details={details}, "
                f"debug_error_string={debug_string}"
            )

            if custom_error_code:
                error_msg += f", custom_error_code={custom_error_code}"

            if rpc_id:
                error_msg += f", rpc_id={rpc_id}"
                # Store RPC ID for potential support troubleshooting
                self._last_rpc_id = rpc_id

            self.logger.error(error_msg)

        else:
            self.logger.error(f"Non-gRPC error {context}: {e}")

    async def _recreate_channel_and_stub(self):
        """
        Recreate the gRPC channel and stub to recover from connection issues.
        """
        try:
            self.logger.info("Recreating gRPC channel and stub for connection recovery")

            # Close existing channel if it exists
            if hasattr(self, "channel") and self.channel:
                await self.channel.close()

            # Recreate channel with original configuration
            # Use stored grpc_host and grpc_port from initialization
            grpc_host = getattr(self, "_grpc_host", "api.pubsub.salesforce.com")
            grpc_port = getattr(self, "_grpc_port", "7443")
            pubsub_url = f"{grpc_host}:{grpc_port}"

            channel_options = [
                (
                    "grpc.keepalive_time_ms",
                    60000,
                ),  # Send keepalive every 60 seconds (aligns with Salesforce 60s requirement)
                (
                    "grpc.keepalive_timeout_ms",
                    10000,
                ),  # Wait 10 seconds for keepalive response
                (
                    "grpc.keepalive_permit_without_calls",
                    True,
                ),  # Allow keepalive on idle connections
                (
                    "grpc.http2.max_pings_without_data",
                    0,
                ),  # Unlimited pings without data
                (
                    "grpc.http2.min_time_between_pings_ms",
                    30000,
                ),  # Min 30 seconds between pings
                (
                    "grpc.http2.min_ping_interval_without_data_ms",
                    30000,
                ),  # 30 seconds without data before ping
                (
                    "grpc.max_connection_idle_ms",
                    7200000,
                ),  # Keep connection for 2 hours when idle
            ]

            self.channel = grpc.aio.secure_channel(
                pubsub_url, secure_channel_credentials, options=channel_options
            )
            self.stub = pb2_grpc.PubSubStub(self.channel)

            # Re-authenticate to get fresh session token
            await self.authenticate()

            self.logger.info("Successfully recreated gRPC channel and authenticated")
            return True

        except Exception as e:
            self.logger.error(f"Failed to recreate channel and stub: {e}")
            return False

    def auth(self):
        """
        Sends a login request to the Salesforce SOAP API to retrieve a session
        token. The session token is bundled with other identifying information
        to create a tuple of metadata headers, which are needed for every RPC
        call.
        """
        url_suffix = "/services/Soap/u/" + self.apiVersion + "/"
        headers = {"content-type": "text/xml", "SOAPAction": "Login"}
        xml = (
            "<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' "
            + "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
            + "xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>"
            + "<urn:login><urn:username><![CDATA["
            + self.username
            + "]]></urn:username><urn:password><![CDATA["
            + self.password
            + "]]></urn:password></urn:login></soapenv:Body></soapenv:Envelope>"
        )
        res = requests.post(self.url + url_suffix, data=xml, headers=headers)

        if res.status_code != 200:
            raise Exception(f"SOAP request failed with status {res.status_code}")

        res_xml = et.fromstring(res.content.decode("utf-8"))[0][0][0]

        try:
            url_parts = urlparse(res_xml[3].text)
            self.url = "{}://{}".format(url_parts.scheme, url_parts.netloc)
            self.session_id = res_xml[4].text
        except IndexError:
            self.logger.error("An exception occurred. Check the response XML: %s", res.__dict__)

        # Get org ID from UserInfo
        uinfo = res_xml[6]
        # Org ID
        self.tenant_id = uinfo[8].text

        # Set metadata headers per Salesforce documentation (trace id replaces the
        # removed sync gRPC interceptor).
        self.metadata = (
            ("accesstoken", self.session_id),
            ("instanceurl", self.url),
            ("tenantid", self.tenant_id),
            ("x-client-trace-id", str(uuid.uuid4())),
        )

        # Validate header formats per Salesforce requirements
        self._validate_auth_headers()

    def auth_soap(self):
        """
        Authenticate using SOAP Login (legacy method).

        Uses username + password + security token to obtain a session token.
        This is the traditional authentication method for Salesforce integrations.
        """
        # This is the original auth() method - just call it
        self.auth()

    def auth_oauth(self):
        """
        Authenticate using OAuth 2.0 Client Credentials flow.

        More secure than SOAP login - recommended for production deployments.
        Does not require user credentials, only connected app credentials.
        Requires a Salesforce connected app with Client Credentials flow enabled.
        """
        token_url = f"{self.url}/services/oauth2/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        try:
            self.logger.debug(f"Requesting OAuth token from {token_url}")
            response = requests.post(token_url, data=payload, headers=headers, timeout=30)
            response.raise_for_status()

            data = response.json()

            # Extract token and metadata
            self.session_id = data.get("access_token")
            # Update instance URL if provided (may differ from initial URL)
            if data.get("instance_url"):
                url_parts = urlparse(data.get("instance_url"))
                self.url = f"{url_parts.scheme}://{url_parts.netloc}"

            # OAuth tokens don't include org ID in response, need to fetch separately
            self.tenant_id = self._fetch_org_id()

            # Set metadata headers same format as SOAP (trace id replaces the removed
            # sync gRPC interceptor).
            self.metadata = (
                ("accesstoken", self.session_id),
                ("instanceurl", self.url),
                ("tenantid", self.tenant_id),
                ("x-client-trace-id", str(uuid.uuid4())),
            )

            self._validate_auth_headers()

            self.logger.info("OAuth authentication successful")

        except requests.exceptions.HTTPError as e:
            error_msg = f"OAuth authentication failed with status {e.response.status_code}"
            if e.response.text:
                try:
                    error_data = e.response.json()
                    error_msg += f": {error_data.get('error', 'unknown')} - {error_data.get('error_description', 'no description')}"
                except:
                    error_msg += f": {e.response.text[:200]}"
            raise Exception(error_msg)
        except Exception as e:
            raise Exception(f"OAuth authentication failed: {e}")

    def _fetch_org_id(self) -> str:
        """
        Fetch organization ID using current access token.

        OAuth tokens don't include the org ID in the token response,
        so we need to query it separately using the REST API.

        Returns:
            Organization ID (tenant ID)
        """
        try:
            # Use identity URL from token response if available, or construct from instance URL
            identity_url = f"{self.url}/services/oauth2/userinfo"

            headers = {
                "Authorization": f"Bearer {self.session_id}",
                "Content-Type": "application/json",
            }

            response = requests.get(identity_url, headers=headers, timeout=30)
            response.raise_for_status()

            user_info = response.json()
            org_id = user_info.get("organization_id")

            if not org_id:
                raise Exception("organization_id not found in userinfo response")

            self.logger.debug(f"Fetched organization ID: {org_id}")
            return org_id

        except Exception as e:
            self.logger.error(f"Failed to fetch organization ID: {e}")
            raise Exception(f"Could not retrieve organization ID: {e}")

    async def authenticate(self):
        """
        Authenticate with Salesforce (async wrapper).

        The underlying auth methods use the blocking `requests` library, so they run in
        a worker thread to avoid stalling the event loop (and the FetchRequest cadence).
        """
        await asyncio.to_thread(self._authenticate_sync)

    def _authenticate_sync(self):
        """
        Authenticate with Salesforce using available credentials.

        Auto-detects authentication method based on provided credentials:
        - If client_id + client_secret provided: OAuth 2.0 Client Credentials
        - If username + password provided: SOAP Login

        OAuth is preferred if both credential sets are provided.
        """
        has_oauth = self.client_id and self.client_secret
        has_soap = self.username and self.password

        if has_oauth:
            self.logger.info("Authenticating with OAuth 2.0 Client Credentials")
            self.auth_oauth()
        elif has_soap:
            self.logger.info("Authenticating with SOAP Login")
            self.auth_soap()
        else:
            raise ValueError(
                "No valid credentials provided for authentication. "
                "Provide either (client_id, client_secret) for OAuth "
                "or (username, password) for SOAP login."
            )

    def make_fetch_request(self, topic, replay_type, replay_id, num_requested):
        """
        Creates a FetchRequest per the proto file.
        """
        replay_preset = None
        match replay_type:
            case "LATEST":
                replay_preset = pb2.ReplayPreset.LATEST
            case "EARLIEST":
                replay_preset = pb2.ReplayPreset.EARLIEST
            case "CUSTOM":
                replay_preset = pb2.ReplayPreset.CUSTOM
            case _:
                raise ValueError("Invalid Replay Type " + replay_type)

        # Handle replay_id type conversion - it can be bytes or hex string
        if isinstance(replay_id, bytes):
            replay_id_bytes = replay_id
        elif isinstance(replay_id, str):
            replay_id_bytes = bytes.fromhex(replay_id)
        else:
            replay_id_bytes = b""  # Empty bytes for LATEST/EARLIEST

        return pb2.FetchRequest(
            topic_name=topic,
            replay_preset=replay_preset,
            replay_id=replay_id_bytes,
            num_requested=num_requested,
        )

    def decode(self, schema, payload):
        """
        Uses Avro and the event schema to decode a serialized payload.
        """
        schema = avro.schema.parse(schema)
        buf = io.BytesIO(payload)
        decoder = avro.io.BinaryDecoder(buf)
        reader = avro.io.DatumReader(schema)
        ret = reader.read(decoder)
        return ret

    async def get_topic(self, topic_name):
        return await self.stub.GetTopic(
            pb2.TopicRequest(topic_name=topic_name), metadata=self.metadata
        )

    async def get_schema_json(self, schema_id):
        """
        Uses GetSchema RPC to retrieve schema given a schema ID.
        """
        # If the schema is not found in the dictionary, get the schema and store it in the dictionary
        if schema_id not in self.json_schema_dict or self.json_schema_dict[schema_id] is None:
            res = await self.stub.GetSchema(
                pb2.SchemaRequest(schema_id=schema_id), metadata=self.metadata
            )
            self.json_schema_dict[schema_id] = res.schema_json

        return self.json_schema_dict[schema_id]

    async def events(self, topic, replay_type, replay_id, num_requested):
        """
        Async generator over Salesforce Pub/Sub events for a subscription.

        Yields individual ConsumerEvent objects (already deduplicated). Backpressure is
        automatic: while the consumer is not pulling, this generator is suspended at the
        ``yield`` and stops sending FetchRequests, so Salesforce throttles delivery.

        Implements Salesforce-compliant retry with replay-ID tracking, token refresh, and
        channel recreation. The initial replay position is provided by the caller (derived
        from the durable checkpoint on restart); mid-stream reconnects resume from the last
        received replay id — in-flight events remain safely buffered downstream, so this is
        at-least-once with no gap.
        """
        attempt = 0
        consecutive_failures = 0
        max_consecutive_failures = 3

        # Track replay state for proper retry behavior
        current_replay_type = replay_type
        current_replay_id = replay_id
        last_processed_replay_id = None
        original_replay_type = replay_type
        original_replay_id = replay_id

        while attempt <= self.max_retries:
            try:
                replay_id_display = (
                    current_replay_id.hex()
                    if isinstance(current_replay_id, bytes)
                    else current_replay_id
                )
                self.logger.info(
                    f"Starting subscription to {topic} (attempt {attempt + 1}); "
                    f"strategy={current_replay_type}, replay_id={replay_id_display}"
                )

                # Open a bidirectional stream and prime it with the initial FetchRequest.
                call = self.stub.Subscribe(metadata=self.metadata)
                await call.write(
                    self.make_fetch_request(
                        topic, current_replay_type, current_replay_id, num_requested
                    )
                )
                self.logger.info(f"Successfully subscribed to {topic}")
                consecutive_failures = 0

                # Read responses. We wait on each read with a timeout so we can send a
                # periodic keepalive FetchRequest during idle periods, but we must NOT
                # cancel the in-flight read — cancelling call.read() finishes the RPC.
                # So the pending read task is preserved across idle timeouts and the
                # keepalive is written concurrently (read and write are independent
                # directions of the bidi stream).
                read_task = None
                try:
                    while True:
                        if read_task is None:
                            read_task = asyncio.ensure_future(call.read())
                        done, _ = await asyncio.wait({read_task}, timeout=self.timeout_seconds)
                        if not done:
                            self.logger.debug(
                                "Idle %.0fs; sending keepalive FetchRequest",
                                self.timeout_seconds,
                            )
                            await call.write(
                                self.make_fetch_request(
                                    topic,
                                    current_replay_type,
                                    current_replay_id,
                                    num_requested,
                                )
                            )
                            continue

                        response = read_task.result()  # may raise AioRpcError
                        read_task = None

                        if response == grpc.aio.EOF:
                            self.logger.warning(f"Subscription stream to {topic} ended normally")
                            return

                        # Track the latest replay id advertised by the server (bytes).
                        if getattr(response, "latest_replay_id", None):
                            last_processed_replay_id = response.latest_replay_id

                        for individual_event in response.events:
                            event_id = getattr(individual_event.event, "id", None)
                            if event_id and self._is_event_duplicate(event_id):
                                self.logger.debug(f"Skipping duplicate event: {event_id}")
                                continue
                            if event_id:
                                self._mark_event_processed(event_id)
                            else:
                                self.logger.warning(
                                    "Event missing system-generated ID, cannot deduplicate"
                                )
                            if getattr(individual_event, "replay_id", None):
                                last_processed_replay_id = individual_event.replay_id
                            # Backpressure point: suspends here until the consumer pulls.
                            yield individual_event

                        # Top up credit once the server has exhausted what it will send.
                        if getattr(response, "pending_num_requested", 0) == 0:
                            await call.write(
                                self.make_fetch_request(
                                    topic,
                                    current_replay_type,
                                    current_replay_id,
                                    num_requested,
                                )
                            )
                finally:
                    if read_task is not None:
                        read_task.cancel()
                        with contextlib.suppress(BaseException):
                            await read_task

            except grpc.aio.AioRpcError as e:
                consecutive_failures += 1
                self._log_grpc_error(e, f"during subscription to {topic}")

                # Determine retry strategy based on error type and Salesforce best practices
                retry_strategy = self._determine_retry_strategy(
                    e,
                    last_processed_replay_id,
                    original_replay_type,
                    original_replay_id,
                )

                if retry_strategy is None:
                    self.logger.error(f"Non-retryable gRPC error: {e.code()}")
                    raise

                current_replay_type, current_replay_id = retry_strategy
                replay_id_display = (
                    current_replay_id.hex()
                    if isinstance(current_replay_id, bytes)
                    else current_replay_id
                )
                self.logger.info(
                    f"Next retry will use: {current_replay_type}, replay_id: {replay_id_display}"
                )

                # Handle authentication errors with token refresh
                if e.code() == grpc.StatusCode.UNAUTHENTICATED:
                    self.logger.info("Authentication token expired, refreshing...")
                    try:
                        await self.authenticate()
                        self.logger.info("Authentication refreshed successfully")
                    except Exception as auth_error:
                        self.logger.error(f"Failed to refresh authentication: {auth_error}")
                        consecutive_failures += 1

                # Recreate the channel after too many consecutive failures
                if consecutive_failures >= max_consecutive_failures:
                    self.logger.error(
                        f"Too many consecutive failures ({consecutive_failures}), "
                        f"attempting channel recreation"
                    )
                    if not await self._recreate_channel_and_stub():
                        self.logger.error("Failed to recreate channel, giving up")
                        raise
                    consecutive_failures = 0

            except Exception as e:
                self.logger.error(f"Unexpected error during subscription: {e}")
                consecutive_failures += 1

            # Calculate retry delay and wait (only if we haven't exceeded max retries)
            if attempt < self.max_retries:
                retry_delay = self._get_retry_delay(attempt)
                self.logger.info(f"Retrying subscription in {retry_delay:.2f} seconds...")
                await asyncio.sleep(retry_delay)

            attempt += 1

        error_msg = (
            f"Failed to establish stable subscription to {topic} "
            f"after {self.max_retries} retries"
        )
        self.logger.error(error_msg)
        raise Exception(error_msg)
