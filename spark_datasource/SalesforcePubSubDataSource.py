"""
SalesforcePubSubDataSource.py

Spark custom data source for bidirectional streaming with Salesforce Pub/Sub API.
Supports both reading from Salesforce (Change Data Capture, Platform Events) and 
writing to Salesforce (Platform Events, Custom Events) via gRPC streaming.

This implementation follows the Databricks PySpark DataSource pattern with full
support for Spark Structured Streaming, checkpointing, and fault tolerance.
"""

import os
import sys
import json
import time
import threading
import uuid
from typing import Iterator, Tuple
import io
import avro.io

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, DataSourceStreamWriter, InputPartition, WriterCommitMessage
from pyspark.sql.types import StructType, StructField, StringType, LongType, BinaryType

# Import bitmap decoding utility
from .util import process_bitmap


class SalesforcePartition(InputPartition):
    """Input partition for Salesforce events."""
    
    def __init__(self, start_replay_id, end_replay_id, events):
        self.start_replay_id = start_replay_id
        self.end_replay_id = end_replay_id
        self.events = events


class SalesforceCommitMessage(WriterCommitMessage):
    """
    Commit message for tracking Salesforce PubSub publish results.
    Contains information about published events and any errors.
    Inherits from WriterCommitMessage to satisfy Spark's interface requirements.
    """
    
    def __init__(self, partition_id: int, published_count: int, failed_count: int, 
                 replay_ids: list = None, errors: list = None):
        super().__init__()  # Initialize parent WriterCommitMessage
        self.partition_id = partition_id
        self.published_count = published_count
        self.failed_count = failed_count
        self.replay_ids = replay_ids or []
        self.errors = errors or []
        self.total_events = published_count + failed_count
    
    def __repr__(self):
        return (f"SalesforceCommitMessage(partition={self.partition_id}, "
                f"published={self.published_count}, failed={self.failed_count})")


class SalesforcePubSubDataSource(DataSource):
    """
    Spark custom data source for bidirectional streaming with Salesforce Pub/Sub API.
    
    Supports both reading from and writing to Salesforce topics including:
    - Change Data Capture (CDC) events (read-only)
    - Platform Events (read/write)
    - Custom Events (read/write)
    
    Reading Usage:
        df = spark.readStream.format("salesforce_pubsub") \
            .option("username", "your-username@example.com") \
            .option("password", "your-password-and-token") \
            .option("topic", "/data/AccountChangeEvent") \
            .option("replayPreset", "EARLIEST") \
            .load()
    
    Writing Usage:
        df.writeStream.format("salesforce_pubsub") \
            .option("username", "your-username@example.com") \
            .option("password", "your-password-and-token") \
            .option("topic", "/event/MyPlatformEvent__e") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start()
    
    Common Options:
        - username: Salesforce username (required)
        - password: Salesforce password + security token (required)
        - topic: Salesforce topic path (required)
        - loginUrl: Salesforce login URL (default: https://login.salesforce.com)
        - grpcHost: PubSub API host (default: api.pubsub.salesforce.com)
        - grpcPort: PubSub API port (default: 7443)
    
    Reader-Specific Options:
        - replayPreset: "EARLIEST" or "LATEST" (default: LATEST)
        - replayId: Specific replay ID to start from (optional, overrides replayPreset)
    
    Writer-Specific Options:
        - batchSize: Events per publish batch (default: 100)
        - eventIdField: Field to use as event ID (optional, generates UUID if not specified)
    """

    @classmethod
    def name(cls):
        return "salesforce_pubsub"

    def schema(self):
        """
        Define the schema for Salesforce events when reading from Salesforce.
        
        This schema is used by the reader to structure incoming events from
        Change Data Capture, Platform Events, and Custom Events.
        
        Returns:
            StructType: Spark schema with standardized fields for all event types
        """
        return StructType([
            StructField("replay_id", StringType(), True),
            StructField("event_payload", BinaryType(), True),
            StructField("schema_id", StringType(), True),
            StructField("topic_name", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("decoded_event", StringType(), True)  # JSON string of decoded event
        ])

    def streamReader(self, schema: StructType):
        return SalesforcePubSubStreamReader(schema, self.options)
    
    def streamWriter(self, schema: StructType, overwrite: bool):
        return SalesforcePubSubStreamWriter(schema, self.options, overwrite)


class SalesforcePubSubStreamReader(DataSourceStreamReader):
    """
    Stream reader for consuming events from Salesforce Pub/Sub API.
    
    Handles subscription to Salesforce topics and converts incoming events
    to Spark streaming format with proper checkpointing and fault tolerance.
    
    Supports:
    - Change Data Capture (CDC) events from Salesforce objects
    - Platform Events published to Salesforce
    - Custom Events with user-defined schemas
    - Automatic Avro decoding and bitmap field processing
    - Replay ID-based checkpointing for exactly-once processing
    - Multi-partition processing for high-throughput scenarios
    """
    
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
        
        # Required options
        self.username = options.get("username")
        self.password = options.get("password") 
        self.topic = options.get("topic", "/data/AccountChangeEvent")
        
        # Optional options
        self.login_url = options.get("loginUrl", "https://login.salesforce.com")
        self.grpc_host = options.get("grpcHost", "api.pubsub.salesforce.com")
        self.grpc_port = int(options.get("grpcPort", "7443"))
        
        # Replay configuration
        self.replay_preset = self._parse_replay_preset(options.get("replayPreset", "LATEST"))
        self.replay_id = options.get("replayId")  # Optional specific replay ID
        
        # Validate required options
        if not self.username or not self.password:
            raise ValueError("Username and password are required options")
        
        # Client state (initialized lazily on worker nodes to avoid serialization issues)
        self.client = None
        self.event_buffer = []
        self.current_offset = 0
        self.subscription_thread = None
        self._client_initialized = False
        
        # Current replay ID (tracked automatically by Spark checkpointing)
        # We store the original binary replay ID as base64 for exact format preservation
        self.current_replay_id = None
        self.current_replay_id_binary = None  # Original binary format
    
    def _ensure_client_initialized(self):
        """Lazily initialize and authenticate the PubSub client to avoid serialization issues."""
        if not self._client_initialized:
            try:
                # Import client creation function locally to avoid serialization issues
                from spark_datasource.PubSubAPIClient import create_client_from_options
                
                # Create client from options - this handles authentication automatically
                self.client = create_client_from_options(self.options)
                self._client_initialized = True
                
            except Exception as e:
                raise RuntimeError(f"Failed to initialize Salesforce client: {e}")
    

    
    def _event_callback(self, received_event):
        """Callback function to handle incoming events from PubSub."""
        try:
            # Ensure client is initialized (should already be, but safety check)
            self._ensure_client_initialized()
            
            # Get topic info for schema
            topic_info = self.client.get_topic(self.topic)
            schema_id = topic_info.schema_id if topic_info else "unknown"
            
            # Store original binary replay ID and create base64 version for JSON storage
            replay_id_binary = received_event.replay_id
            replay_id = self._encode_replay_id_for_storage(replay_id_binary)
            
            # Decode the event payload
            decoded_event = None
            if topic_info:
                try:
                    schema = self.client.get_schema(schema_id)
                    if schema:
                        reader = avro.io.DatumReader(schema)
                        binary_data = io.BytesIO(received_event.event.payload)
                        decoder = avro.io.BinaryDecoder(binary_data)
                        decoded_event = reader.read(decoder)
                        
                        # Decode bitmap fields to human-readable field names
                        if decoded_event and 'ChangeEventHeader' in decoded_event:
                            header = decoded_event['ChangeEventHeader']
                            
                            # Decode changedFields bitmap
                            if 'changedFields' in header and header['changedFields']:
                                changed_field_names = process_bitmap(schema, header['changedFields'])
                                header['changedFieldNames'] = changed_field_names
                            
                            # Decode nulledFields bitmap
                            if 'nulledFields' in header and header['nulledFields']:
                                nulled_field_names = process_bitmap(schema, header['nulledFields'])
                                header['nulledFieldNames'] = nulled_field_names
                            
                            # Decode diffFields bitmap
                            if 'diffFields' in header and header['diffFields']:
                                diff_field_names = process_bitmap(schema, header['diffFields'])
                                header['diffFieldNames'] = diff_field_names
                                
                except Exception as decode_error:
                    # Still continue processing, just without decoded event
                    pass
            
            # Create event record
            event_record = {
                'replay_id': replay_id,
                'event_payload': received_event.event.payload,
                'schema_id': schema_id,
                'topic_name': self.topic,
                'timestamp': int(time.time() * 1000),  # milliseconds
                'decoded_event': json.dumps(decoded_event) if decoded_event else None
            }
            
            # Add to buffer
            self.event_buffer.append(event_record)
            
        except Exception as e:
            print(f"Error processing event: {e}")
    
    def _encode_replay_id_for_storage(self, replay_id_bytes):
        """
        Encode binary replay ID for JSON storage in Spark checkpoints.
        
        We use base64 encoding because Spark checkpoints are stored as JSON,
        and JSON cannot store raw binary data. This preserves the exact 
        binary format that Salesforce expects.
        """
        if not replay_id_bytes:
            return None
        
        import base64
        return base64.b64encode(replay_id_bytes).decode('ascii')
    
    def _decode_replay_id_from_storage(self, replay_id_b64):
        """
        Decode base64-encoded replay ID back to original binary format.
        """
        if not replay_id_b64:
            return None
            
        import base64
        return base64.b64decode(replay_id_b64)
    
    def _parse_replay_preset(self, replay_preset_str):
        """Parse replay preset string to protobuf constant."""
        from .proto import pubsub_api_pb2 as pb2
        
        preset_str = replay_preset_str.upper().strip()
        
        if preset_str == "EARLIEST":
            return pb2.ReplayPreset.EARLIEST
        elif preset_str == "LATEST":
            return pb2.ReplayPreset.LATEST
        else:
            # Default to LATEST if unrecognized
            return pb2.ReplayPreset.LATEST
    

    
    def _start_subscription(self):
        """Start the PubSub subscription in a background thread."""
        if self.subscription_thread and self.subscription_thread.is_alive():
            return
        
        # Initialize client on worker node before starting subscription
        self._ensure_client_initialized()
        
        # Use binary replay ID for Salesforce communication
        subscription_replay_id_binary = self.current_replay_id_binary
        
        self.subscription_thread = self.client.subscribe(
            topic_name=self.topic,
            callback=self._event_callback,
            replay_preset=self.replay_preset,
            replay_id_binary=subscription_replay_id_binary  # Pass binary directly
        )
        
        # Give subscription time to start
        time.sleep(2)
    
    def _get_events_between_replay_ids(self, start_replay_id, end_replay_id):
        """
        Get events from buffer that are in range (start_replay_id, end_replay_id].
        
        This ensures each micro-batch processes only events in its designated range,
        preventing duplicates in multi-node clusters.
        
        Args:
            start_replay_id: Start boundary (exclusive) - base64 encoded replay ID
            end_replay_id: End boundary (inclusive) - base64 encoded replay ID
            
        Returns:
            List of events in the specified range
        """
        if not self.event_buffer:
            return []
        
        # Convert base64 boundaries to binary for proper comparison
        start_binary = self._decode_replay_id_from_storage(start_replay_id) if start_replay_id else None
        end_binary = self._decode_replay_id_from_storage(end_replay_id) if end_replay_id else None
        
        if not start_binary:
            # First batch - return events up to end_replay_id
            if not end_binary:
                return list(self.event_buffer)
            
            # Return all events <= end_binary
            range_events = []
            for event in self.event_buffer:
                event_replay_id_b64 = event.get('replay_id')
                if event_replay_id_b64:
                    event_binary = self._decode_replay_id_from_storage(event_replay_id_b64)
                    if event_binary and event_binary <= end_binary:
                        range_events.append(event)
            
            return range_events
        
        # Return events in range (start_binary, end_binary]
        range_events = []
        for event in self.event_buffer:
            event_replay_id_b64 = event.get('replay_id')
            if event_replay_id_b64:
                event_binary = self._decode_replay_id_from_storage(event_replay_id_b64)
                if event_binary:
                    # Event must be > start_binary (exclusive)
                    if event_binary > start_binary:
                        # If end_binary is specified, event must also be <= end_binary (inclusive)
                        if not end_binary or event_binary <= end_binary:
                            range_events.append(event)
        
        return range_events
    
    def __getstate__(self):
        """Custom pickle behavior to exclude non-serializable objects."""
        state = self.__dict__.copy()
        # Remove non-serializable objects
        state['client'] = None
        state['subscription_thread'] = None
        state['_client_initialized'] = False
        # Note: Keep current_replay_id (base64 string) and current_replay_id_binary (bytes)
        # as they are serializable and needed for checkpoint recovery
        return state
    
    def __setstate__(self, state):
        """Custom unpickle behavior to restore serializable state."""
        self.__dict__.update(state)
        # Client and subscription will be lazily initialized when needed
    
    def initialOffset(self) -> dict:
        """
        Returns the initial start offset of the reader.
        
        For Salesforce PubSub, we use replay IDs as offsets instead of integers.
        This is only called on the very first run (no checkpoint exists).
        """
        # Determine initial replay ID from configuration
        if self.replay_id:
            # replay_id should be a base64-encoded string representing binary data
            initial_replay_id = self.replay_id
            self.current_replay_id_binary = self._decode_replay_id_from_storage(self.replay_id)
        else:
            # Use None to indicate we should use replay preset
            initial_replay_id = None
            self.current_replay_id_binary = None
        
        self.current_replay_id = initial_replay_id
        
        # Start subscription with initial replay settings
        self._start_subscription()
        
        return {"replay_id": initial_replay_id}
    
    def latestOffset(self) -> dict:
        """
        Returns the current latest offset that the next microbatch will read to.
        
        For Salesforce PubSub, this returns the latest replay ID from processed events.
        Spark automatically persists this to checkpoint and restores it on restart.
        """
        # Find the latest replay ID from events in buffer
        if self.event_buffer:
            latest_event = self.event_buffer[-1]
            latest_replay_id = latest_event.get('replay_id')
            
            if latest_replay_id and latest_replay_id != self.current_replay_id:
                self.current_replay_id = latest_replay_id
                # Decode to binary for Salesforce communication
                self.current_replay_id_binary = self._decode_replay_id_from_storage(latest_replay_id)
        
        return {"replay_id": self.current_replay_id}
    
    def partitions(self, start: dict, end: dict):
        """
        Plans the partitioning of the current microbatch defined by start and end offset.
        Returns a sequence of InputPartition objects.
        """
        start_replay_id = start.get("replay_id")
        end_replay_id = end.get("replay_id")
        
        # Handle checkpoint recovery - update subscription if needed
        if start_replay_id and start_replay_id != self.current_replay_id:
            self.current_replay_id = start_replay_id
            # Decode the checkpointed replay ID to binary format for Salesforce
            self.current_replay_id_binary = self._decode_replay_id_from_storage(start_replay_id)
            # Restart subscription from the checkpointed replay ID if needed
            if not self.subscription_thread or not self.subscription_thread.is_alive():
                self._start_subscription()
        
        # Get events from buffer that are newer than start_replay_id
        batch_events = self._get_events_between_replay_ids(start_replay_id, end_replay_id)
        
        # For simplicity, create a single partition per batch
        return [SalesforcePartition(start_replay_id, end_replay_id, batch_events)]
    
    def commit(self, end: dict):
        """
        Invoked when the query has finished processing data before end offset.
        
        Spark automatically persists the 'end' offset to checkpoint directory.
        We remove processed events from buffer to prevent duplicates.
        """
        committed_replay_id = end.get("replay_id")
        
        # Remove all processed events (replay_id <= committed_replay_id) from buffer
        if committed_replay_id:
            committed_binary = self._decode_replay_id_from_storage(committed_replay_id)
            if committed_binary:
                self.event_buffer = [
                    event for event in self.event_buffer
                    if self._decode_replay_id_from_storage(event.get('replay_id', '')) > committed_binary
                ]
    
    def read(self, partition) -> Iterator[Tuple]:
        """
        Takes a partition as an input and reads an iterator of tuples from the data source.
        """
        for event in partition.events:
            # Convert event to tuple matching our schema
            yield (
                event['replay_id'],           # replay_id
                event['event_payload'],       # event_payload  
                event['schema_id'],           # schema_id
                event['topic_name'],          # topic_name
                event['timestamp'],           # timestamp
                event['decoded_event']        # decoded_event
            )


class SalesforcePubSubStreamWriter(DataSourceStreamWriter):
    """
    Stream writer for publishing data to Salesforce Pub/Sub API.
    
    Converts Spark streaming data into Salesforce events and publishes them
    to Platform Events or Custom Event topics with proper error handling,
    batching, and replay ID tracking.
    
    Features:
    - Dynamic schema mapping to Salesforce event fields
    - Automatic Avro encoding for platform events
    - Batch publishing for high throughput
    - Error handling with detailed failure reporting
    - Support for both structured and unstructured payloads
    - Custom field mapping with __c suffix handling
    - Replay ID collection for downstream processing
    
    Supported Event Types:
    - Platform Events (e.g., /event/MyEvent__e)
    - Custom Events with user-defined schemas
    - Forwarding of CDC events (event replay scenarios)
    
    Configuration Options:
        Authentication:
        - username: Salesforce username (required)
        - password: Salesforce password + security token (required) 
        - loginUrl: Salesforce login URL (default: https://login.salesforce.com)
        - grpcHost: PubSub API host (default: api.pubsub.salesforce.com)
        - grpcPort: PubSub API port (default: 7443)
        
        Publishing:
        - topic: Salesforce topic to publish to (required)
        - batchSize: Events per publish batch (default: 100)
        
        Data Mapping:
        - eventIdField: Field to use as event ID (optional, generates UUID if not specified)
    """
    
    def __init__(self, schema: StructType, options: dict, overwrite: bool):
        self.schema = schema
        self.options = options
        self.overwrite = overwrite
        
        # Required options
        self.username = options.get("username")
        self.password = options.get("password")
        self.topic = options.get("topic")
        
        # Optional options
        self.login_url = options.get("loginUrl", "https://login.salesforce.com")
        self.grpc_host = options.get("grpcHost", "api.pubsub.salesforce.com")
        self.grpc_port = int(options.get("grpcPort", "7443"))
        
        # Publishing options
        self.batch_size = int(options.get("batchSize", "100"))
        self.event_id_field = options.get("eventIdField")
        
        # Validation
        if not self.username or not self.password:
            raise ValueError("Username and password are required options")
        if not self.topic:
            raise ValueError("Topic is required option")
        
        # Client state (initialized per partition to avoid serialization)
        self.client = None
        self._client_initialized = False
        self.topic_schema = None
        self.topic_info = None
    
    def _ensure_client_initialized(self):
        """Initialize and authenticate the PubSub client."""
        if not self._client_initialized:
            try:
                from spark_datasource.PubSubAPIClient import create_client_from_options
                
                self.client = create_client_from_options(self.options)
                self._client_initialized = True
                
                # Get topic information and schema
                self.topic_info = self.client.get_topic(self.topic)
                if not self.topic_info:
                    raise RuntimeError(f"Topic {self.topic} not found or not accessible")
                
                # Check if topic allows publishing
                if not self.topic_info.can_publish:
                    raise RuntimeError(f"Topic {self.topic} does not allow publishing")
                
                # Get schema if available
                if self.topic_info.schema_id:
                    self.topic_schema = self.client.get_schema(self.topic_info.schema_id)
                
                print(f"✅ Salesforce PubSub writer initialized for topic: {self.topic}")
                
            except Exception as e:
                print(f"❌ Failed to initialize writer client: {e}")
                raise RuntimeError(f"Failed to initialize Salesforce writer client: {e}")
    
    def write(self, iterator):
        """
        Write data from iterator to Salesforce PubSub.
        Returns commit message with publish results.
        """
        from pyspark import TaskContext
        
        # Get partition context
        context = TaskContext.get()
        partition_id = context.partitionId() if context else 0
        
        # Initialize client
        self._ensure_client_initialized()
        
        # Collect events from iterator
        events_to_publish = []
        row_count = 0
        failed_count = 0
        published_replay_ids = []
        errors = []
        
        for row in iterator:
            try:
                event = self._convert_row_to_event(row)
                events_to_publish.append(event)
                row_count += 1
                
                # Publish in batches if we hit batch size
                if len(events_to_publish) >= self.batch_size:
                    batch_result = self._publish_batch(events_to_publish)
                    if batch_result:
                        published_replay_ids.extend(batch_result.get('replay_ids', []))
                        failed_count += batch_result.get('failed_count', 0)
                        errors.extend(batch_result.get('errors', []))
                    events_to_publish = []
                    
            except Exception as e:
                error_msg = f"Error converting row to event: {e}"
                print(f"❌ {error_msg}")
                errors.append(error_msg)
                failed_count += 1
                # Continue processing other rows
        
        # Publish remaining events
        if events_to_publish:
            batch_result = self._publish_batch(events_to_publish)
            if batch_result:
                published_replay_ids.extend(batch_result.get('replay_ids', []))
                failed_count += batch_result.get('failed_count', 0)
                errors.extend(batch_result.get('errors', []))
        
        successful_count = row_count - failed_count
        if row_count > 0:
            print(f"✅ Partition {partition_id}: {successful_count}/{row_count} events published successfully")
        
        # Return commit message
        return SalesforceCommitMessage(
            partition_id=partition_id,
            published_count=successful_count,
            failed_count=failed_count,
            replay_ids=published_replay_ids,
            errors=errors
        )
    
    def _convert_row_to_event(self, row):
        """Convert Spark row to Salesforce event format."""
        try:
            # Always use entire row as payload data
            payload_data = row.asDict()
            
            # Determine event ID
            event_id = str(uuid.uuid4())
            if self.event_id_field and hasattr(row, self.event_id_field):
                event_id = str(getattr(row, self.event_id_field))
            
            # Use topic's schema ID (auto-detected from Salesforce)
            schema_id = self.topic_info.schema_id if self.topic_info else ''
            
            # For platform events, create Avro record with system fields + user data
            if self.topic_schema:
                # Start with user's row data
                platform_event_data = dict(payload_data)
                
                # Add required system fields for platform events (Salesforce will override these)
                schema_fields = {field.name for field in self.topic_schema.fields}
                import time
                if 'CreatedDate' in schema_fields:
                    platform_event_data['CreatedDate'] = int(time.time() * 1000)  # Current timestamp in milliseconds
                if 'CreatedById' in schema_fields:
                    platform_event_data['CreatedById'] = '005000000000000AAA'  # Placeholder - Salesforce will override
                
                # Encode to Avro using the platform event schema
                payload = self.client.encode_event_to_avro(platform_event_data, self.topic_schema)
            else:
                # Fallback: create JSON structure without schema
                payload = json.dumps(payload_data).encode('utf-8')
            
            return {
                'id': event_id,
                'schema_id': schema_id,
                'payload': payload,
                'headers': {}
            }
        except Exception as e:
            error_msg = f"Failed to convert row to event: {e}"
            print(f"❌ {error_msg}")
            raise RuntimeError(error_msg)
    
    def _publish_batch(self, events):
        """Publish a batch of events to Salesforce."""
        batch_size = len(events)
        
        try:
            response = self.client.publish_event(self.topic, events)
            
            # Process results
            replay_ids = []
            failed_count = 0
            errors = []
            
            for i, result in enumerate(response.results):
                if result.error and result.error.code != 0:
                    error_msg = f"Event {i} publish failed: {result.error.msg}"
                    print(f"❌ {error_msg}")
                    errors.append(error_msg)
                    failed_count += 1
                else:
                    # Successful publish
                    if result.replay_id:
                        try:
                            # Try to decode as UTF-8 first
                            if isinstance(result.replay_id, bytes):
                                replay_id_str = result.replay_id.decode('utf-8')
                            else:
                                replay_id_str = str(result.replay_id)
                        except UnicodeDecodeError:
                            # If UTF-8 fails, use base64 encoding for binary data
                            import base64
                            if isinstance(result.replay_id, bytes):
                                replay_id_str = base64.b64encode(result.replay_id).decode('ascii')
                            else:
                                replay_id_str = str(result.replay_id)
                        
                        replay_ids.append(replay_id_str)
            
            successful_count = batch_size - failed_count
            
            return {
                'replay_ids': replay_ids,
                'failed_count': failed_count,
                'errors': errors,
                'successful_count': successful_count
            }
                    
        except Exception as e:
            error_msg = f"Failed to publish batch: {e}"
            print(f"❌ {error_msg}")
            return {
                'replay_ids': [],
                'failed_count': batch_size,
                'errors': [error_msg],
                'successful_count': 0
            }
    
    def commit(self, messages, batchId) -> None:
        """
        Handle successful completion of all partition writes.
        """
        total_published = sum(msg.published_count for msg in messages)
        total_failed = sum(msg.failed_count for msg in messages)
        
        if total_published > 0:
            print(f"✅ Batch {batchId}: {total_published} events published to Salesforce")
        
        # Log errors if any
        for msg in messages:
            if msg.errors:
                for error in msg.errors:
                    print(f"❌ Partition {msg.partition_id} error: {error}")
    
    def abort(self, messages, batchId) -> None:
        """
        Handle failed batch - some partitions succeeded, others failed.
        """
        total_published = sum(msg.published_count for msg in messages)
        total_failed = sum(msg.failed_count for msg in messages)
        
        print(f"💥 Batch {batchId} aborted: {total_published} published, {total_failed} failed")
        
        # Log errors
        for msg in messages:
            for error in msg.errors:
                print(f"❌ Partition {msg.partition_id} error: {error}")
    
    def __getstate__(self):
        """Custom pickle behavior to exclude non-serializable objects."""
        state = self.__dict__.copy()
        # Remove non-serializable objects
        state['client'] = None
        state['_client_initialized'] = False
        state['topic_schema'] = None
        state['topic_info'] = None
        return state
    
    def __setstate__(self, state):
        """Custom unpickle behavior to restore serializable state."""
        self.__dict__.update(state)
        # Client will be lazily initialized when needed


# Registration helper function
def register_salesforce_data_source(spark):
    """
    Register the Salesforce PubSub data source with Spark for bidirectional streaming.
    
    This enables both reading from and writing to Salesforce Pub/Sub API using
    the "salesforce_pubsub" format in readStream and writeStream operations.
    
    Args:
        spark: SparkSession instance
    
    Usage:
        from spark_datasource import register_data_source
        register_data_source(spark)
        
        # Then use in streaming:
        # Reading:
        df = spark.readStream.format("salesforce_pubsub").option(...).load()
        
        # Writing:
        df.writeStream.format("salesforce_pubsub").option(...).start()
    """
    spark.dataSource.register(SalesforcePubSubDataSource)

