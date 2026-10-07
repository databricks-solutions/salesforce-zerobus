"""Salesforce CDC (variant layout) -> current state and SCD2 history, for every org.

Reads the Zerobus table written by go_salesforce_zerobus with
SFZB_PAYLOAD_FORMAT=variant (or both), where each row's `record` VARIANT holds
the decoded event. No Avro schema is needed, so new orgs, new objects, and new
fields flow through without restarting the pipeline. Typed, per-org views are
generated separately from the Lakebase schema registry (generate_views.py).

Tables (in the pipeline's catalog and schema):

  salesforce_current        One row per (org_id, entity_name, Id): `fields`
                            VARIANT is the record's merged current state. A
                            Delta table the pipeline doesn't manage (so a full
                            refresh never resets it), with Change Data Feed,
                            MERGEd by a foreach_batch_sink (sparse updates need the
                            prior state, which AUTO CDC can't express). Deletes
                            are soft (`_is_deleted`) so UNDELETE and late
                            redeliveries resolve correctly.
  salesforce_history        SCD2 history from salesforce_current's Change Data
                            Feed. `fields_json` is the merged state as JSON text
                            (VARIANT values can't be compared, which SCD2
                            change detection needs); query it with parse_json.
  salesforce_decode_errors  Events the service could not decode; the raw Avro
                            payload and schema are kept in the source table.

Event semantics (sparse updates, compound fields, unified diffs, ordering and
redelivery) live in cdc_fold.py, which is unit-tested without Spark.

Like the original pipeline, history lags current state by one update when the
pipeline runs triggered (the CDF reader snapshots salesforce_current before
that update's MERGE commits); under continuous mode the lag is one micro-batch.
"""

import sys

from delta.tables import DeltaTable
from pyspark import cloudpickle
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    VariantType,
)

spark = SparkSession.getActiveSession()

# The pipeline's root (resources/pipeline.yml sets root_path) holds cdc_fold.py.
sys.path.append(spark.conf.get("bundle.sourcePath", "."))
import cdc_fold  # noqa: E402

# Ship cdc_fold with the UDF instead of requiring it on every executor.
cloudpickle.register_pickle_by_value(cdc_fold)

CATALOG = spark.conf.get("catalog")
SCHEMA = spark.conf.get("schema")
SOURCE = spark.conf.get("source_table")  # the Zerobus target table
CURRENT = spark.conf.get("current_table", f"{CATALOG}.{SCHEMA}.salesforce_current")

KEYS = ["org_id", "entity_name", "Id"]

CURRENT_SCHEMA = StructType(
    [
        StructField("org_id", StringType()),
        StructField("entity_name", StringType()),
        StructField("Id", StringType()),
        StructField("fields", VariantType()),
        StructField("_is_deleted", BooleanType()),
        StructField("_last_change_type", StringType()),
        # Watermark of the last applied event (see cdc_fold.seq_key).
        StructField("_last_commit_ts", LongType()),
        StructField("_last_commit_number", LongType()),
        StructField("_last_sequence_number", IntegerType()),
        StructField("_last_replay_id", StringType()),
        StructField("_updated_at", TimestampType()),
    ]
)

FOLD_RESULT = StructType(
    [
        StructField("fields_json", StringType()),
        StructField("is_deleted", BooleanType()),
        StructField("change_type", StringType()),
        StructField("commit_ts", LongType()),
        StructField("commit_number", LongType()),
        StructField("sequence_number", IntegerType()),
        StructField("replay_id", StringType()),
    ]
)


@F.udf(returnType=FOLD_RESULT)
def _fold(prior_json, prior_deleted, wm_ts, wm_number, wm_seq, wm_replay, events):
    """Fold one record's events for this micro-batch onto its stored state."""
    prior_seq = None if wm_replay is None else cdc_fold.seq_key(wm_ts, wm_number, wm_seq, wm_replay)
    evs = [
        {
            "change_type": e["change_type"],
            "record": cdc_fold.loads(e["record_json"]),
            "changed_fields": e["changed_fields"],
            "nulled_fields": e["nulled_fields"],
            "diff_fields": e["diff_fields"],
            "seq": cdc_fold.seq_key(e["commit_ts"], e["commit_number"], e["sequence_number"], e["replay_id"]),
        }
        for e in events
    ]
    out = cdc_fold.fold(cdc_fold.loads(prior_json), prior_deleted, prior_seq, evs)
    if out is None:
        return None  # nothing newer than what is stored
    ts, number, seq, replay = out["seq"]
    return (cdc_fold.dumps(out["fields"]), out["deleted"], out["change_type"], ts, number, seq, replay)


def _ensure_current_table():
    """Create salesforce_current (CDF on) during graph evaluation, so the
    history flow's CDF reader can be analyzed. spark.sql CREATE is not allowed
    while the graph is evaluated; the DeltaTable builder is."""
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(CURRENT)
        .addColumns(CURRENT_SCHEMA)
        .clusterBy("org_id", "entity_name")
        .property("delta.enableChangeDataFeed", "true")
        .property("delta.enableVariantShredding", "true")
        .execute()
    )


def _merge_batch(batch_df, batch_id):
    """MERGE one micro-batch of events into salesforce_current."""
    s = batch_df.sparkSession
    events = batch_df.dropDuplicates(["event_id", "Id"])
    grouped = events.groupBy(*KEYS).agg(
        F.collect_list(
            F.struct(
                "change_type", "record_json", "changed_fields", "nulled_fields", "diff_fields",
                "commit_ts", "commit_number", "sequence_number", "replay_id",
            )
        ).alias("events")
    )
    prior = s.read.table(CURRENT).select(
        *KEYS,
        F.expr("to_json(fields)").alias("prior_json"),
        "_is_deleted", "_last_commit_ts", "_last_commit_number", "_last_sequence_number", "_last_replay_id",
    )
    folded = (
        grouped.join(prior, KEYS, "left")
        .select(
            *KEYS,
            _fold(
                "prior_json", "_is_deleted", "_last_commit_ts", "_last_commit_number",
                "_last_sequence_number", "_last_replay_id", "events",
            ).alias("r"),
        )
        .where("r IS NOT NULL")
        .select(*KEYS, "r.*")
    )
    values = {
        "fields": "parse_json(s.fields_json)",
        "_is_deleted": "s.is_deleted",
        "_last_change_type": "s.change_type",
        "_last_commit_ts": "s.commit_ts",
        "_last_commit_number": "s.commit_number",
        "_last_sequence_number": "s.sequence_number",
        "_last_replay_id": "s.replay_id",
        "_updated_at": "current_timestamp()",
    }
    (
        DeltaTable.forName(s, CURRENT)
        .alias("t")
        .merge(folded.alias("s"), " AND ".join(f"t.{k} = s.{k}" for k in KEYS))
        .whenMatchedUpdate(set=values)
        .whenNotMatchedInsert(values={**{k: f"s.{k}" for k in KEYS}, **values})
        .execute()
    )


_ensure_current_table()


# ---- Layer 1: events -> salesforce_current (MERGE) ----

@dp.foreach_batch_sink(name="salesforce_current_sink")
def _current_sink(df, batch_id):
    _merge_batch(df, batch_id)


@dp.append_flow(target="salesforce_current_sink", name="salesforce_current_flow")
def _events():
    return (
        spark.readStream.table(SOURCE)
        .where("record IS NOT NULL")
        .select(
            "org_id",
            "entity_name",
            # One event can carry the same change for many records.
            F.explode("record_ids").alias("Id"),
            "change_type",
            "changed_fields",
            "nulled_fields",
            "diff_fields",
            F.expr("to_json(record)").alias("record_json"),
            F.expr("record:ChangeEventHeader.commitTimestamp::bigint").alias("commit_ts"),
            "commit_number",
            "sequence_number",
            "replay_id",
            "event_id",
        )
    )


# ---- Layer 2: SCD2 history from salesforce_current's Change Data Feed ----

@dp.temporary_view(name="salesforce_current_cdf")
def _current_cdf():
    # startingVersion=0 backfills the full history on the first run; ignored
    # once the flow has a checkpoint.
    return (
        spark.readStream.option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .table(CURRENT)
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .withColumn("fields_json", F.expr("to_json(fields)"))
        .drop("fields")
    )


dp.create_streaming_table(
    name="salesforce_history",
    comment="SCD2 history of Salesforce records across orgs; fields_json is the record's merged state (parse_json to query).",
    cluster_by=["org_id", "entity_name"],
)
dp.create_auto_cdc_flow(
    target="salesforce_history",
    source="salesforce_current_cdf",
    keys=KEYS,
    sequence_by=F.col("_commit_version"),
    stored_as_scd_type=2,
    apply_as_deletes=F.expr("_is_deleted"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
)


# ---- Events the service could not decode ----

@dp.table(
    name="salesforce_decode_errors",
    comment="CDC events the service could not decode; payload_binary and schema_json in the source table hold the raw event.",
)
def _decode_errors():
    return (
        spark.readStream.table(SOURCE)
        .where("record IS NULL AND decode_error IS NOT NULL")
        .select("event_id", "org_id", "tenant_key", "topic", "entity_name", "change_type", "schema_id", "replay_id", "decode_error", "timestamp")
    )
