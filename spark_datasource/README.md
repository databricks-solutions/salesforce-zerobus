# Salesforce PubSub Spark Data Source

A comprehensive Spark data source for **bidirectional streaming** with Salesforce Platform Events and Change Data Capture (CDC) via the PubSub API.

## Features

### 📖 **Reader Capabilities**
- **Real-time streaming** from Salesforce Platform Events and CDC
- **Change Data Capture (CDC)** support with automatic bitmap field decoding
- **Configurable replay** (EARLIEST/LATEST) with automatic resume
- **Avro schema decoding** with automatic schema management
- **Exactly-once processing** with Spark checkpointing
- **Automatic field name decoding** from hex bitmaps (changedFields, nulledFields, diffFields)

### ✍️ **Writer Capabilities** (NEW!)
- **Publish streaming data** to Salesforce Platform Events
- **Event forwarding** between Salesforce topics with transformations
- **Custom data publishing** from any Spark streaming source
- **Batch optimization** for high-volume scenarios
- **Transactional semantics** with commit/abort handling
- **Avro encoding** for proper Salesforce event format
- **Error handling** with detailed logging and recovery

## Installation on Databricks

### Step 1: Build the Wheel Package

```bash
python build_wheel.py
```

This creates: `dist/sfpubsub-1.0.0-py3-none-any.whl`

### Step 2: Upload Wheel File to Databricks

**Option A: Upload to Volume (Recommended)**
Upload the wheel file to a Unity Catalog volume.

**Option B: Upload to Workspace Directory**
Upload the wheel file to your workspace directory.

### Step 3: Install the Package

**From Volume:**
```python
%pip install /Volumes/path/wheels/sfpubsub-1.0.0-py3-none-any.whl
dbutils.library.restartPython()
```

**From Workspace:**
```python
%pip install /Workspace/Users/your_email/wheels/sfpubsub-1.0.0-py3-none-any.whl
dbutils.library.restartPython()
```

### Step 4: Set Up Streaming

#### 📖 Reading from Salesforce (Subscription)

```python
from sfpubsub import register_data_source
from pyspark.sql.functions import col, current_timestamp

# Set your credentials
USERNAME = "your-username@example.com"
PASSWORD = "your-password-and-security-token"

# Register the data source
register_data_source(spark)

# Create streaming DataFrame
df = spark.readStream.format("salesforce_pubsub") \
    .option("username", USERNAME) \
    .option("password", PASSWORD) \
    .option("topic", "/data/AccountChangeEvent") \
    .option("replayPreset", "EARLIEST") \
    .load()

# Transform and display data
display_df = df.select(
    col("replay_id"),
    col("topic_name"), 
    col("schema_id"),
    col("timestamp"),
    col("decoded_event")
).withColumn("processed_at", current_timestamp())

# Stream to Delta table
delta_query = display_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/Volumes/users/harsha_pasala/checkpoints/1/") \
    .trigger(processingTime="15 seconds") \
    .toTable("users.harsha_pasala.salesforce_account_updates")

# Start the stream
delta_query.start()
```

#### ✍️ Writing to Salesforce (Publishing)

```python
from pyspark.sql.functions import struct, to_json, lit

# Create custom events to publish
custom_events = your_stream \
    .withColumn("event_data", to_json(struct(
        col("customer_id"),
        col("action"),
        col("timestamp"),
        col("metadata")
    )))

# Publish to Salesforce Platform Event
publish_query = custom_events.writeStream \
    .format("salesforce_pubsub") \
    .option("username", USERNAME) \
    .option("password", PASSWORD) \
    .option("topic", "/data/CustomEvent__e") \
    .option("batchSize", "100") \
    .option("checkpointLocation", "/Volumes/users/harsha_pasala/checkpoints/publish/") \
    .outputMode("append") \
    .start()
```

#### 🔄 Event Forwarding (Read + Transform + Write)

```python
# Read from one topic, transform, and forward to another
source_stream = spark.readStream.format("salesforce_pubsub") \
    .option("username", USERNAME) \
    .option("password", PASSWORD) \
    .option("topic", "/data/AccountChangeEvent") \
    .load()

# Transform events
transformed = source_stream \
    .filter(col("decoded_event").isNotNull()) \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("source_topic", lit("/data/AccountChangeEvent"))

# Forward to another topic
forward_query = transformed.writeStream \
    .format("salesforce_pubsub") \
    .option("username", USERNAME) \
    .option("password", PASSWORD) \
    .option("topic", "/data/ProcessedEvent__e") \
    .option("checkpointLocation", "/Volumes/users/harsha_pasala/checkpoints/forward/") \
    .start()
```

## Enhanced Output with Bitmap Decoding

The data source automatically decodes CDC bitmap fields into human-readable field names:

```json
{
  "ChangeEventHeader": {
    "entityName": "Account",
    "changeType": "UPDATE",
    "changedFields": ["0x400002"],
    "changedFieldNames": ["Name", "LastModifiedDate"],    // ← Decoded!
    "nulledFields": ["0x8"],
    "nulledFieldNames": ["Phone"],                        // ← Decoded!
    "diffFields": [],
    "diffFieldNames": []                                  // ← Decoded!
  },
  "Name": "Acme Corp Updated",
  "LastModifiedDate": 1753117917000
}
```

## Configuration Options

### 📖 Reader Configuration (readStream)

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `username` | Yes | - | Salesforce username |
| `password` | Yes | - | Password + security token |
| `topic` | No | `/data/AccountChangeEvent` | Platform Event topic |
| `replayPreset` | No | `LATEST` | Replay preset (`EARLIEST`/`LATEST`) |
| `replayId` | No | - | Specific replay ID to start from |
| `loginUrl` | No | `https://login.salesforce.com` | Salesforce login URL |
| `grpcHost` | No | `api.pubsub.salesforce.com` | PubSub API host |
| `grpcPort` | No | `7443` | PubSub API port |

### ✍️ Writer Configuration (writeStream)

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `username` | Yes | - | Salesforce username |
| `password` | Yes | - | Password + security token |
| `topic` | Yes | - | Platform Event topic to publish to |
| `batchSize` | No | `100` | Events per publish request |
| `eventIdField` | No | - | Field to use as event ID (generates UUID if not specified) |
| `loginUrl` | No | `https://login.salesforce.com` | Salesforce login URL |
| `grpcHost` | No | `api.pubsub.salesforce.com` | PubSub API host |
| `grpcPort` | No | `7443` | PubSub API port |

## Automatic Replay Management

The data source provides **exactly-once processing** guarantees:

- ✅ **First Run**: Uses your configured `replayPreset` or `replayId`
- ✅ **Subsequent Runs**: Automatically resumes from last processed replay ID
- ✅ **No Duplicates**: Never reprocesses the same messages
- ✅ **No Data Loss**: Always continues from where it left off
- ✅ **Fault Tolerant**: Survives application crashes and restarts

## Output Schema

```python
StructType([
    StructField("replay_id", StringType(), True),       # Event replay ID
    StructField("event_payload", BinaryType(), True),   # Raw Avro payload
    StructField("schema_id", StringType(), True),       # Avro schema ID  
    StructField("topic_name", StringType(), True),      # CDC topic name
    StructField("timestamp", LongType(), True),         # Processing timestamp
    StructField("decoded_event", StringType(), True)    # JSON decoded event
])
```

## Example: Advanced Usage

```python
# Advanced filtering and processing
processed_df = df.select(
    col("replay_id"),
    col("topic_name"),
    get_json_object(col("decoded_event"), "$.ChangeEventHeader.entityName").alias("entity"),
    get_json_object(col("decoded_event"), "$.ChangeEventHeader.changeType").alias("change_type"),
    get_json_object(col("decoded_event"), "$.ChangeEventHeader.changedFieldNames").alias("changed_fields"),
    get_json_object(col("decoded_event"), "$.Name").alias("account_name"),
    get_json_object(col("decoded_event"), "$.Id").alias("account_id")
).filter(
    # Only process updates to Name field
    col("changed_fields").contains("Name")
)

# Stream to console for monitoring
console_query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()
```

## Dependencies

- `grpcio>=1.50.0`
- `grpcio-tools>=1.50.0`
- `protobuf>=4.21.0`
- `certifi>=2022.0.0`
- `avro-python3>=1.10.0`
- `requests>=2.28.0`
- `bitstring>=4.0.0`
- `pyspark>=3.4.0`

## Troubleshooting

### Installation Issues

**Package not found:**
```python
# Check if package is installed
%pip list | grep salesforce

# Restart Python after installation
dbutils.library.restartPython()
```

**Import errors:**
```python
# Verify installation
try:
    from sfpubsub import register_data_source
    print("✅ Package installed correctly")
except ImportError as e:
    print(f"❌ Package not found: {e}")
```

### Authentication Issues

- Ensure security token is concatenated with password: `password = "mypassword" + "securitytoken"`
- Check username format (usually email address)
- Verify Salesforce org has Pub/Sub API enabled
- Test credentials with Salesforce login

### Performance Tuning

```python
# Adjust processing interval based on event volume
.trigger(processingTime="15 seconds")  # Lower for real-time, higher for batch processing

# Use multiple workers for high-volume topics
spark.conf.set("spark.sql.streaming.numReaderPartitions", "4")
```

### Checkpoint Management

```python
# Use volume paths for durability
.option("checkpointLocation", "/Volumes/catalog/schema/volume/checkpoints/stream_name/")

# For recovery from specific replay ID, delete checkpoint and restart
%fs rm -r /Volumes/catalog/schema/volume/checkpoints/stream_name/
```

## Documentation

- **[Writer Guide](WRITER_GUIDE.md)** - Comprehensive guide for publishing data to Salesforce
- **[Example Usage](example_writer_usage.py)** - Complete working examples for all scenarios
- **[Salesforce Pub/Sub API](https://github.com/forcedotcom/pub-sub-api)** - Official Salesforce documentation

## Writer Use Cases

The new writer capability enables powerful integration patterns:

- **🔄 Event Processing Pipelines**: Read from Salesforce, transform with Spark, write back to Salesforce
- **📊 Analytics Publishing**: Stream analytics results to Salesforce for real-time dashboards
- **🔗 System Integration**: Bridge external systems with Salesforce via Platform Events
- **⚡ Real-time Notifications**: Publish alerts and notifications to Salesforce workflows
- **🎯 Event Routing**: Forward and filter events between different Salesforce topics

## Performance Guidelines

### For High-Volume Scenarios
- Use batch sizes of 200-500 events
- Set processing triggers to 30-60 seconds
- Monitor Salesforce API limits
- Implement proper error handling and retries

### For Low-Latency Scenarios  
- Use smaller batch sizes (10-50 events)
- Set processing triggers to 5-10 seconds
- Monitor memory usage with frequent commits
- Use checkpointing for fault tolerance

## License

See [LICENSE](LICENSE) file for details.
