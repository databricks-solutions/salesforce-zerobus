"""
PubSub.py

This file defines the class `PubSub`, which contains functionality for
subscriber clients to connect to Salesforce Pub/Sub API.
"""

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
import requests
from dotenv import load_dotenv

from .proto import pubsub_api_pb2 as pb2
from .proto import pubsub_api_pb2_grpc as pb2_grpc

load_dotenv()

with open(certifi.where(), "rb") as f:
    secure_channel_credentials = grpc.ssl_channel_credentials(f.read())


class ClientTraceInterceptor(
    grpc.UnaryUnaryClientInterceptor, grpc.StreamStreamClientInterceptor
):
    """
    gRPC interceptor to add client trace ID for debugging as per Salesforce documentation.
    """

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def _add_trace_id(self, client_call_details, request):
        """Add unique trace ID to request metadata."""
        trace_id = str(uuid.uuid4())
        metadata = list(client_call_details.metadata or [])
        metadata.append(("x-client-trace-id", trace_id))

        new_call_details = client_call_details._replace(metadata=metadata)

        self.logger.debug(
            f"Request start - Trace ID: {trace_id}, Method: {client_call_details.method}"
        )
        return new_call_details, trace_id

    def intercept_unary_unary(self, continuation, client_call_details, request):
        """Intercept unary-unary calls to add trace ID."""
        new_call_details, trace_id = self._add_trace_id(client_call_details, request)

        try:
            response = continuation(new_call_details, request)
            self.logger.debug(f"Request completed - Trace ID: {trace_id}")
            return response
        except Exception as e:
            self.logger.error(f"Request failed - Trace ID: {trace_id}, Error: {e}")
            raise

    def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        """Intercept stream-stream calls to add trace ID."""
        new_call_details, trace_id = self._add_trace_id(
            client_call_details, request_iterator
        )

        try:
            response_iterator = continuation(new_call_details, request_iterator)
            self.logger.debug(f"Streaming request started - Trace ID: {trace_id}")
            return response_iterator
        except Exception as e:
            self.logger.error(
                f"Streaming request failed - Trace ID: {trace_id}, Error: {e}"
            )
            raise


def get_argument(key, argument_dict):
    """Get configuration value from command line args or environment variables."""
    if key in argument_dict and argument_dict[key] is not None:
        return argument_dict[key]

    env_map = {
        "url": "SALESFORCE_URL",
        "username": "SALESFORCE_USERNAME",
        "password": "SALESFORCE_PASSWORD",
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
        self.metadata = None
        grpc_host = get_argument("grpcHost", argument_dict)
        grpc_port = get_argument("grpcPort", argument_dict)
        pubsub_url = grpc_host + ":" + grpc_port

        # Store original gRPC connection details for reconnection
        self._grpc_host = grpc_host
        self._grpc_port = grpc_port

        # Store timeout configuration (centralized from core.py)
        self.timeout_seconds = float(argument_dict.get("timeout_seconds", 45.0))

        # Connection resilience settings
        self.max_retries = 5
        self.base_retry_delay = 1.0
        self.max_retry_delay = 60.0

        # Configure gRPC channel with keepalive settings optimized for long-lived streaming
        channel_options = [
            (
                "grpc.keepalive_time_ms",
                60000,
            ),  # Send keepalive every 60 seconds (less aggressive)
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
                300000,
            ),  # 5 minutes without data before ping
            (
                "grpc.max_connection_idle_ms",
                1800000,
            ),  # Keep connection for 30 minutes when idle
        ]

        # Create channel with trace interceptor
        channel = grpc.secure_channel(
            pubsub_url, secure_channel_credentials, options=channel_options
        )

        # Add client trace ID interceptor as per Salesforce documentation
        trace_interceptor = ClientTraceInterceptor(self.logger)
        intercepted_channel = grpc.intercept_channel(channel, trace_interceptor)

        self.channel = channel
        self.stub = pb2_grpc.PubSubStub(intercepted_channel)
        self.session_id = None
        self.pb2 = pb2
        self.topic_name = get_argument("topic", argument_dict)
        # If the API version is not provided as an argument, use a default value
        if get_argument("apiVersion", argument_dict) is None:
            self.apiVersion = "57.0"
        else:
            # Otherwise, get the version from the argument
            self.apiVersion = get_argument("apiVersion", argument_dict)
        """
        Semaphore used for subscriptions. This keeps the subscription stream open
        to receive events and to notify when to send the next FetchRequest.
        See Python Quick Start for more information.
        https://developer.salesforce.com/docs/platform/pub-sub-api/guide/qs-python-quick-start.html
        There is probably a better way to do this. This is only sample code. Please
        use your own discretion when writing your production Pub/Sub API client.
        Make sure to use only one semaphore per subscribe call if you are planning
        to share the same instance of PubSub.
        """
        self.semaphore = threading.Semaphore(1)
        self.flow_controller = None  # Can be injected for enhanced flow control

        # Event deduplication as per Salesforce recommendations
        self._processed_event_ids = set()
        self._max_event_ids_cache = 10000  # Limit cache size to prevent memory issues
        self._event_ids_lock = threading.Lock()

        # Store last RPC ID for support troubleshooting
        self._last_rpc_id = None

    def set_flow_controller(self, flow_controller):
        """
        Inject a flow controller for enhanced semaphore management.
        When set, the flow controller will be used instead of the basic semaphore.
        """
        self.flow_controller = flow_controller

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
                self._processed_event_ids = set(
                    event_ids_list[len(event_ids_list) // 2 :]
                )
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
        self.logger.debug(
            f"Authentication headers validated: org={self.tenant_id}, url={self.url}"
        )

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
        replay_id_display = (
            last_processed_replay_id.hex() if last_processed_replay_id else "None"
        )
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
            self.logger.warning(
                "Detected corrupted replay ID error - switching to LATEST"
            )
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

    def _recreate_channel_and_stub(self):
        """
        Recreate the gRPC channel and stub to recover from connection issues.
        """
        try:
            self.logger.info("Recreating gRPC channel and stub for connection recovery")

            # Close existing channel if it exists
            if hasattr(self, "channel") and self.channel:
                self.channel.close()

            # Recreate channel with original configuration
            # Use stored grpc_host and grpc_port from initialization
            grpc_host = getattr(self, "_grpc_host", "api.pubsub.salesforce.com")
            grpc_port = getattr(self, "_grpc_port", "7443")
            pubsub_url = f"{grpc_host}:{grpc_port}"

            channel_options = [
                (
                    "grpc.keepalive_time_ms",
                    60000,
                ),  # Send keepalive every 60 seconds (less aggressive)
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
                    300000,
                ),  # 5 minutes without data before ping
                (
                    "grpc.max_connection_idle_ms",
                    1800000,
                ),  # Keep connection for 30 minutes when idle
            ]

            channel = grpc.secure_channel(
                pubsub_url, secure_channel_credentials, options=channel_options
            )

            # Add client trace ID interceptor
            trace_interceptor = ClientTraceInterceptor(self.logger)
            intercepted_channel = grpc.intercept_channel(channel, trace_interceptor)

            self.channel = channel
            self.stub = pb2_grpc.PubSubStub(intercepted_channel)

            # Re-authenticate to get fresh session token
            self.auth()

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
            self.logger.error(
                "An exception occurred. Check the response XML: %s", res.__dict__
            )

        # Get org ID from UserInfo
        uinfo = res_xml[6]
        # Org ID
        self.tenant_id = uinfo[8].text

        # Set metadata headers per Salesforce documentation
        self.metadata = (
            ("accesstoken", self.session_id),
            ("instanceurl", self.url),
            ("tenantid", self.tenant_id),
        )

        # Validate header formats per Salesforce requirements
        self._validate_auth_headers()

    def release_subscription_semaphore(self):
        """
        Release semaphore so FetchRequest can be sent.
        Uses flow controller if available for enhanced safety.
        """
        if self.flow_controller:
            success = self.flow_controller.release()
            if success:
                self.logger.debug(
                    "Flow controller released semaphore successfully - ready for next fetch"
                )
            else:
                self.logger.warning("Flow controller failed to release semaphore")
        else:
            try:
                self.semaphore.release()
                self.logger.debug(
                    "Semaphore released successfully - ready for next fetch"
                )
            except ValueError as e:
                self.logger.warning(
                    "Attempted to release semaphore beyond maximum value: %s", e
                )
            except Exception as e:
                self.logger.error("Error releasing semaphore: %s", e)

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

    def fetch_req_stream(self, topic, replay_type, replay_id, num_requested):
        """
        Returns a FetchRequest stream for the Subscribe RPC.
        Implements Salesforce flow control requirements: new FetchRequest within 60 seconds.
        """
        consecutive_timeouts = 0
        max_consecutive_timeouts = 3
        last_response_time = time.time()

        while True:
            # Salesforce requirement: send new FetchRequest within 60 seconds of last response
            time_since_last_response = time.time() - last_response_time

            # Only send FetchRequest when needed. Semaphore release indicates need for new FetchRequest
            # Use flow controller if available, otherwise fall back to basic semaphore
            if self.flow_controller:
                acquired = self.flow_controller.acquire(timeout=self.timeout_seconds)
            else:
                acquired = self.semaphore.acquire(timeout=self.timeout_seconds)

            if not acquired:
                consecutive_timeouts += 1

                # Check if we're approaching Salesforce's 60-second limit
                if time_since_last_response >= self.timeout_seconds:
                    self.logger.debug(
                        "Approaching Salesforce 60-second limit (%.1fs). Sending compliance FetchRequest.",
                        time_since_last_response,
                    )
                    # Force a FetchRequest to maintain compliance
                    consecutive_timeouts = 0
                    last_response_time = time.time()
                    self.logger.debug(
                        "Sending compliance FetchRequest for %d events",
                        num_requested,
                    )
                    yield self.make_fetch_request(
                        topic, replay_type, replay_id, num_requested
                    )
                    continue

                # Only warn if we have multiple consecutive timeouts (indicating real issues)
                if consecutive_timeouts >= 2:
                    self.logger.warning(
                        "Multiple semaphore timeouts (#%d). Stream may be experiencing issues. Time since last response: %.1fs",
                        consecutive_timeouts,
                        time_since_last_response,
                    )
                else:
                    self.logger.debug(
                        "Semaphore acquire timeout #%d (normal for idle streams). Time since last response: %.1fs",
                        consecutive_timeouts,
                        time_since_last_response,
                    )

                if consecutive_timeouts >= max_consecutive_timeouts:
                    self.logger.error(
                        "%d consecutive timeouts. Stream appears to be deadlocked.",
                        max_consecutive_timeouts,
                    )
                    # Force release to attempt recovery
                    try:
                        if self.flow_controller:
                            self.flow_controller.release()
                        else:
                            self.semaphore.release()
                        self.logger.info("Attempted recovery by releasing semaphore")
                        consecutive_timeouts = 0
                    except ValueError:
                        self.logger.error(
                            "Recovery failed: semaphore was already at maximum value"
                        )
                        break

                # Continue loop to retry
                continue
            else:
                # Successfully acquired semaphore
                consecutive_timeouts = 0
                last_response_time = time.time()
                self.logger.debug(
                    "Semaphore acquired successfully. Sending Fetch Request for %d events",
                    num_requested,
                )
                yield self.make_fetch_request(
                    topic, replay_type, replay_id, num_requested
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

    def get_topic(self, topic_name):
        return self.stub.GetTopic(
            pb2.TopicRequest(topic_name=topic_name), metadata=self.metadata
        )

    def get_schema_json(self, schema_id):
        """
        Uses GetSchema RPC to retrieve schema given a schema ID.
        """
        # If the schema is not found in the dictionary, get the schema and store it in the dictionary
        if (
            schema_id not in self.json_schema_dict
            or self.json_schema_dict[schema_id] is None
        ):
            res = self.stub.GetSchema(
                pb2.SchemaRequest(schema_id=schema_id), metadata=self.metadata
            )
            self.json_schema_dict[schema_id] = res.schema_json

        return self.json_schema_dict[schema_id]

    def subscribe(self, topic, replay_type, replay_id, num_requested, callback):
        """
        Calls the Subscribe RPC defined in the proto file and accepts a
        client-defined callback to handle any events that are returned by the
        API. Implements Salesforce-compliant retry logic with proper replay ID tracking.
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
                self.logger.info(
                    f"Starting subscription to {topic} (attempt {attempt + 1})"
                )
                # Display replay ID properly for logging
                replay_id_display = (
                    current_replay_id.hex()
                    if isinstance(current_replay_id, bytes)
                    else current_replay_id
                )
                self.logger.info(
                    f"Using replay strategy: {current_replay_type}, replay_id: {replay_id_display}"
                )

                # Create enhanced callback that tracks replay IDs and implements deduplication
                def replay_tracking_callback(event, client):
                    nonlocal last_processed_replay_id
                    try:
                        # Track latest replay ID from the response (store as bytes per Salesforce docs)
                        if (
                            hasattr(event, "latest_replay_id")
                            and event.latest_replay_id
                        ):
                            last_processed_replay_id = (
                                event.latest_replay_id
                            )  # Keep as bytes
                            self.logger.debug(
                                f"Updated last processed replay ID: {event.latest_replay_id.hex()}"
                            )

                        # Process individual events with deduplication and replay ID tracking
                        if hasattr(event, "events") and event.events:
                            deduplicated_events = []
                            for individual_event in event.events:
                                # Check for duplicate using system-generated ID
                                event_id = getattr(individual_event.event, "id", None)
                                if event_id:
                                    if self._is_event_duplicate(event_id):
                                        self.logger.debug(
                                            f"Skipping duplicate event: {event_id}"
                                        )
                                        continue
                                    else:
                                        self._mark_event_processed(event_id)
                                        deduplicated_events.append(individual_event)
                                else:
                                    # No event ID available, process anyway but log warning
                                    self.logger.warning(
                                        "Event missing system-generated ID, cannot deduplicate"
                                    )
                                    deduplicated_events.append(individual_event)

                                # Track replay ID for last processed event (store as bytes per Salesforce docs)
                                if (
                                    hasattr(individual_event, "replay_id")
                                    and individual_event.replay_id
                                ):
                                    last_processed_replay_id = (
                                        individual_event.replay_id
                                    )  # Keep as bytes
                                    self.logger.debug(
                                        f"Processing event with replay ID: {individual_event.replay_id.hex()}"
                                    )

                            # Create a new event object with deduplicated events for the callback
                            if deduplicated_events:
                                # Create a copy of the event with only non-duplicate events
                                deduplicated_event = type(event)(
                                    events=deduplicated_events,
                                    latest_replay_id=event.latest_replay_id,
                                    rpc_id=getattr(event, "rpc_id", ""),
                                    pending_num_requested=getattr(
                                        event, "pending_num_requested", 0
                                    ),
                                )
                                callback(deduplicated_event, client)
                            elif event.events:
                                self.logger.info(
                                    f"All {len(event.events)} events in batch were duplicates, skipping callback"
                                )
                            else:
                                # No events in batch (keepalive), call callback anyway
                                callback(event, client)
                        else:
                            # No events in response (keepalive message)
                            callback(event, client)

                    except Exception as callback_error:
                        self.logger.error(f"Error in event callback: {callback_error}")
                        # Don't re-raise callback errors to avoid breaking the stream

                sub_stream = self.stub.Subscribe(
                    self.fetch_req_stream(
                        topic, current_replay_type, current_replay_id, num_requested
                    ),
                    metadata=self.metadata,
                )

                self.logger.info(f"Successfully subscribed to {topic}")
                consecutive_failures = (
                    0  # Reset failure counter on successful connection
                )

                # Process events from the stream
                for event in sub_stream:
                    replay_tracking_callback(event, self)

                # If we reach here, the stream ended normally
                self.logger.warning(f"Subscription stream to {topic} ended normally")
                break

            except grpc.RpcError as e:
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
                # Display replay ID properly for logging
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
                        self.auth()  # Re-authenticate
                        self.logger.info("Authentication refreshed successfully")
                    except Exception as auth_error:
                        self.logger.error(
                            f"Failed to refresh authentication: {auth_error}"
                        )
                        consecutive_failures += 1

                # Check if we've had too many consecutive failures
                if consecutive_failures >= max_consecutive_failures:
                    self.logger.error(
                        f"Too many consecutive failures ({consecutive_failures}), attempting channel recreation"
                    )
                    if not self._recreate_channel_and_stub():
                        self.logger.error("Failed to recreate channel, giving up")
                        raise
                    consecutive_failures = 0

            except Exception as e:
                self.logger.error(f"Unexpected error during subscription: {e}")
                consecutive_failures += 1

            # Calculate retry delay and wait (only if we haven't exceeded max retries)
            if attempt < self.max_retries:
                retry_delay = self._get_retry_delay(attempt)
                self.logger.info(
                    f"Retrying subscription in {retry_delay:.2f} seconds..."
                )
                time.sleep(retry_delay)

            attempt += 1

        if attempt > self.max_retries:
            error_msg = f"Failed to establish stable subscription to {topic} after {self.max_retries} retries"
            self.logger.error(error_msg)
            raise Exception(error_msg)
