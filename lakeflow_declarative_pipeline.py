"""Salesforce CDC -> accurate current state (Type 1) + SCD2 history, in one SDP pipeline.

Salesforce Change Data Capture sends *sparse* update events: only changed fields
are meaningful, and because the payload is Avro (every field is always present),
an unchanged field and a field genuinely changed-to-null both decode as `null`.
The only authoritative signal for what changed is the change mask carried in the
bronze columns `changed_fields` / `nulled_fields` / `diff_fields`.

A plain @dp.table cannot produce accurate final state (it returns a DataFrame and
cannot express "keep the existing value for fields not in the change mask"), and
create_auto_cdc_flow's `ignore_null_updates` keys off the value being null so it
can't tell unchanged-null from changed-to-null. The correct primitive is a
conditional MERGE, run inside the pipeline via a ForEachBatch sink.

Per Salesforce object this builds:
  Layer 1  salesforce_current_<obj>  - Type-1 accurate current state
           (@dp.append_flow parses Avro inline -> @dp.foreach_batch_sink MERGE).
           External Delta table with Change Data Feed enabled.
  Layer 2  salesforce_history_<obj>  - SCD2 history
           (@dp.table reads the current table's CDF -> create_auto_cdc_flow).
           Lags Layer 1 by one update under a TRIGGERED cadence: Layer 2's CDF
           reader is a streaming source that snapshots the current table at the
           start of each update, before Layer 1's MERGE in that same update
           commits. So a change written to the current table in update N first
           appears in history in update N+1. Harmless under continuous:true (the
           lag is one micro-batch and self-heals); only visible when triggered
           updates complete and the pipeline goes IDLE between them. When testing
           after a source change, expect history to reflect it one update later.

NOTE: salesforce_current_<obj> is an *external* Delta table (not a pipeline-managed
node), so it is not reset by full refresh. The sink creates it (CDF on) on its
first micro-batch and merges into it; SDP forbids CREATE TABLE during graph
evaluation but allows it inside the sink's runtime micro-batch. On a brand new
object the Layer 2 CDF reader can fail once at startup (table not yet created) and
recovers on SDP's flow retry. Run the pipeline (not a full refresh) when the Avro
schema evolves; new fields are only picked up after the current table is recreated.
"""

import hashlib
import re

from delta.tables import DeltaTable
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import array_contains, array_union, col, desc, expr, lit, max_by
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

spark = SparkSession.getActiveSession()

# Bronze table written by the salesforce-zerobus streamer.
zerobus_table = "alexn.salesforce.zb_contact"
CATALOG, SCHEMA, _ = zerobus_table.split(".")

# Bronze columns that are CDC metadata, not Salesforce business fields. Everything
# else after Avro parsing (minus the ChangeEventHeader struct) is a business field.
META_COLS = {
    "event_id", "schema_id", "replay_id", "timestamp", "change_type", "entity_name",
    "change_origin", "record_ids", "changed_fields", "nulled_fields", "diff_fields",
    "record_data_json", "payload_binary", "schema_json", "org_id", "processed_timestamp",
    "ChangeEventHeader",
}

# change_type values that carry a full image with an empty change mask; for these,
# treat every populated field as "changed".
CREATE_LIKE = ("CREATE", "UNDELETE", "GAP_CREATE", "GAP_UNDELETE", "GAP_OVERFLOW")


def _safe(obj: str) -> str:
    """Object name -> safe identifier suffix for table/flow/sink names."""
    return re.sub(r"\W+", "_", obj)


def _latest_schema_json(obj: str):
    """Most recent Avro schema for an object, or None if it has no payload yet."""
    row = (
        dp.read(zerobus_table)
        .filter(
            (col("entity_name") == obj)
            & col("payload_binary").isNotNull()
            & col("schema_json").isNotNull()
        )
        .orderBy(desc("timestamp"))
        .select("schema_json")
        .first()
    )
    return row[0] if row else None


def _parse_avro(df, schema_json):
    """Flatten the Avro payload into typed business columns alongside the metadata."""
    return (
        df.select("*", from_avro(col("payload_binary"), schema_json, {"mode": "PERMISSIVE"}).alias("p"))
        .select("*", "p.*")
        .drop("p")
    )


_HUNK_RE = re.compile(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


def _parse_unified_diff(diff_lines):
    """Split raw diff lines into (expected_hash, hunks).

    `expected_hash` is the SHA-256 carried in the `+++` header (or None). Each hunk is
    a (old_start, body_lines) pair, where old_start is the 1-based line in the previous
    value where the hunk begins and body_lines are its `' '`/`'-'`/`'+'` content lines.

    Returns None if a hunk header is malformed (the caller then keeps the prior value)."""
    expected_hash = None
    hunks = []
    pos, total = 0, len(diff_lines)
    while pos < total:
        line = diff_lines[pos]
        if line.startswith("+++ "):
            expected_hash = line[4:].strip() or None
            pos += 1
        elif line.startswith("@@"):
            match = _HUNK_RE.match(line)
            if not match:
                return None
            old_start = int(match.group(1))
            pos += 1
            body = []
            while pos < total:
                body_line = diff_lines[pos]
                if body_line[:2] == "@@" or body_line[:4] in ("--- ", "+++ "):
                    break  # start of the next hunk/header
                if body_line == "" and pos == total - 1:
                    pos += 1  # trailing artifact from split("\n")
                    continue
                body.append(body_line)
                pos += 1
            hunks.append((old_start, body))
        else:
            pos += 1  # "--- " header or any line outside a hunk
    return expected_hash, hunks


def _rebuild_from_hunks(prev_lines, hunks):
    """Apply parsed hunks to the previous value's lines, returning the new lines.

    Returns None on an unrecognized body line (the caller then keeps the prior value)."""
    result = []
    prev_idx = 0  # 0-based cursor into prev_lines
    for old_start, body in hunks:
        # Copy the unchanged lines that precede this hunk verbatim.
        while prev_idx < old_start - 1 and prev_idx < len(prev_lines):
            result.append(prev_lines[prev_idx])
            prev_idx += 1
        for body_line in body:
            tag = body_line[0] if body_line else " "
            if tag == " ":  # context: keep the prior line (fall back to diff text)
                result.append(prev_lines[prev_idx] if prev_idx < len(prev_lines) else body_line[1:])
                prev_idx += 1
            elif tag == "-":  # removal: drop the prior line
                prev_idx += 1
            elif tag == "+":  # addition: emit the new line
                result.append(body_line[1:])
            else:
                return None
    # Copy any unchanged lines after the last hunk.
    while prev_idx < len(prev_lines):
        result.append(prev_lines[prev_idx])
        prev_idx += 1
    return result


def _apply_unified_diff(prev, diff):
    """Reconstruct a field's full value by applying a Salesforce CDC unified diff to
    its previous value. Large text fields (>= 1000 chars) are sent as a unified diff
    (named in diff_fields) instead of the full value.

    Returns `prev` unchanged if the diff is missing/unparseable, or if the SHA-256 in
    the `+++` header doesn't match the reconstructed value (so we never store corrupt
    text — worst case we keep the last full value)."""
    if diff is None:
        return prev
    # The field value may use CRLF line endings while the diff body uses LF. Split the
    # prior value on its own terminator so line content matches the diff, and rejoin
    # with the same terminator (the SHA-256 in the +++ header is over the CRLF form).
    term = "\r\n" if (prev and "\r\n" in prev) else "\n"
    prev_lines = [] if prev is None else prev.split(term)
    try:
        parsed = _parse_unified_diff(diff.split("\n"))
        if parsed is None:
            return prev
        expected_hash, hunks = parsed
        result_lines = _rebuild_from_hunks(prev_lines, hunks)
        if result_lines is None:
            return prev
        result = term.join(result_lines)
    except Exception:
        return prev
    if expected_hash and hashlib.sha256(result.encode("utf-8")).hexdigest() != expected_hash:
        return prev
    return result


def _resolve_chain(prior, chain):
    """Fold a record's per-batch change chain for one string column, in sequence
    order, starting from the prior committed value. Each item is a full value (d=False)
    or a unified diff to apply (d=True). Empty chain -> carry forward `prior`."""
    if not chain:
        return prior
    items = sorted(chain, key=lambda r: (r["s"]["ts"], r["s"]["rid"] or ""))
    cur = prior
    for it in items:
        cur = _apply_unified_diff(cur, it["v"]) if it["d"] else it["v"]
    return cur


def _collapse_batch(batch_df, business_cols, string_cols):
    """Fold all events for a key within one micro-batch into a single combined delta.

    Each event is itself a sparse delta, so we cannot just keep the latest event:
    fields changed by earlier events in the same batch would be lost. Per column,
    take the value from the latest event in which the column was actually touched.

    String columns instead emit an ordered `__chain__c` of touching events (value +
    is-diff flag) so the sink can fold full-value and unified-diff events in sequence
    (a diff must apply to the prior value, which may be set earlier in the same batch).
    """
    mask = array_union(
        array_union(col("changed_fields"), col("nulled_fields")), col("diff_fields")
    )
    seq = F.struct(col("timestamp").alias("ts"), col("replay_id").alias("rid"))
    is_create_like = col("change_type").isin(*CREATE_LIKE)

    d = (
        batch_df.withColumn("Id", col("record_ids")[0])
        .withColumn("_seq", seq)
        .withColumn("_mask", mask)
    )

    aggs = []
    for c in business_cols:
        # A row "touches" c when c is named in the change mask, or it's a create-like
        # event with a populated value (creates carry a full image, empty mask).
        touched = array_contains(col("_mask"), lit(c)) | (is_create_like & col(f"`{c}`").isNotNull())
        seq_c = F.when(touched, col("_seq"))  # null on rows that don't touch c
        # Did c get touched at all in this batch?
        aggs.append(F.max(seq_c).isNotNull().alias(f"__chg__{c}"))
        if c in string_cols:
            # ordered chain of touching events (value + whether it's a unified diff)
            item = F.when(
                touched,
                F.struct(
                    col("_seq").alias("s"),
                    col(f"`{c}`").alias("v"),
                    array_contains(col("diff_fields"), lit(c)).alias("d"),
                ),
            )
            aggs.append(F.collect_list(item).alias(f"__chain__{c}"))
        else:
            # Latest value among touched rows (null here = a genuine change-to-null).
            aggs.append(max_by(col(f"`{c}`"), seq_c).alias(c))

    aggs += [
        max_by(col("change_type"), col("_seq")).alias("_last_change_type"),
        max_by(col("replay_id"), col("_seq")).alias("_last_replay_id"),
        F.max(col("timestamp")).alias("_last_event_ts"),
    ]
    return d.groupBy("Id").agg(*aggs)


def _ensure_current_table(current_fqn: str, obj: str, schema_json: str):
    """Create the Type-1 current-state table (CDF on) if it doesn't exist.

    Called during graph evaluation so the table exists before Layer 2's CDF reader
    is *analyzed* (SDP resolves all flows up front; a streaming read of a missing
    table fails analysis, and the sink creating it later is too late). spark.sql
    CREATE is forbidden during eval, but the DeltaTable builder API is allowed."""
    # Resolve parsed Avro types without scanning data (from_avro types come from schema).
    parsed_schema = _parse_avro(
        dp.read(zerobus_table).filter(col("entity_name") == obj).limit(0), schema_json
    ).schema
    business_fields = [f for f in parsed_schema.fields if f.name not in META_COLS]
    table_schema = StructType(
        [StructField("Id", StringType())]
        + business_fields
        + [
            StructField("_last_change_type", StringType()),
            StructField("_last_replay_id", StringType()),
            StructField("_last_event_ts", LongType()),
            StructField("_updated_at", TimestampType()),
        ]
    )
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(current_fqn)
        .addColumns(table_schema)
        .property("delta.enableChangeDataFeed", "true")
        .execute()
    )


def _merge_fn(current_fqn: str):
    """Build the foreachBatch function that MERGEs each micro-batch into the
    current-state table (created during graph eval by _ensure_current_table)."""

    def _fn(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        spark_s = batch_df.sparkSession
        spark_s.udf.register("resolve_chain", _resolve_chain, StringType())
        # Only merge columns that exist in the target (guards against schema drift).
        target_cols = set(spark_s.read.table(current_fqn).columns)
        string_cols = {
            f.name for f in batch_df.schema.fields if isinstance(f.dataType, StringType)
        }
        business_cols = [
            c for c in batch_df.columns if c not in META_COLS and c in target_cols
        ]
        combined = _collapse_batch(
            batch_df, business_cols, {c for c in business_cols if c in string_cols}
        )

        tracking = {
            "_last_change_type": "s._last_change_type",
            "_last_replay_id": "s._last_replay_id",
            "_last_event_ts": "s._last_event_ts",
            "_updated_at": "current_timestamp()",
        }
        update_set, insert_values = {}, {"Id": "s.Id"}
        for c in business_cols:
            if c in string_cols:
                # fold the change chain over the prior value (handles unified diffs);
                # empty chain -> carry forward on update, NULL on insert
                update_set[c] = f"resolve_chain(t.`{c}`, s.`__chain__{c}`)"
                insert_values[c] = f"resolve_chain(CAST(NULL AS STRING), s.`__chain__{c}`)"
            else:
                # only overwrite when actually touched this batch
                update_set[c] = f"CASE WHEN s.`__chg__{c}` THEN s.`{c}` ELSE t.`{c}` END"
                insert_values[c] = f"CASE WHEN s.`__chg__{c}` THEN s.`{c}` ELSE NULL END"
        update_set.update(tracking)
        insert_values.update(tracking)

        (
            DeltaTable.forName(spark_s, current_fqn)
            .alias("t")
            .merge(combined.alias("s"), "t.Id = s.Id")
            .whenMatchedDelete(condition="s._last_change_type = 'DELETE'")
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(condition="s._last_change_type <> 'DELETE'", values=insert_values)
            .execute()
        )

    return _fn


def create_pipeline(salesforce_object: str):
    schema_json = _latest_schema_json(salesforce_object)
    if schema_json is None:
        return  # no payload for this object yet; nothing to build

    safe = _safe(salesforce_object)
    current_fqn = f"{CATALOG}.{SCHEMA}.salesforce_current_{safe}"
    sink_name = f"current_sink_{safe}"
    cdf_name = f"current_cdf_{safe}"
    history_name = f"salesforce_history_{safe}"

    # Create the current-state table now (graph eval) so Layer 2's CDF reader can be
    # analyzed; the sink then MERGEs into it and Layer 2 streams its Change Data Feed.
    _ensure_current_table(current_fqn, salesforce_object, schema_json)

    # ---- Layer 1: parse Avro inline, MERGE into the Type-1 current-state table ----
    # (defined inside create_pipeline, so each closes over this call's variables)
    sink_merge = _merge_fn(current_fqn)

    @dp.foreach_batch_sink(name=sink_name)
    def _sink(df, batch_id):
        sink_merge(df, batch_id)

    @dp.append_flow(target=sink_name, name=f"{sink_name}_flow")
    def _flow():
        stream = dp.readStream(zerobus_table).filter(col("entity_name") == salesforce_object)
        return _parse_avro(stream, schema_json)

    # ---- Layer 2: SCD2 history from the current table's Change Data Feed ----
    # temporary_view: pipeline-scoped, not materialized (no extra physical table)
    @dp.temporary_view(name=cdf_name)
    def _cdf():
        # startingVersion=0 so the first run captures the full history of the current
        # table (a streaming CDF read otherwise starts at the latest version and
        # skips the initial backfill). Ignored once a checkpoint exists.
        return (
            spark.readStream.option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .table(current_fqn)
            .filter(col("_change_type").isin("insert", "update_postimage", "delete"))
        )

    dp.create_streaming_table(name=history_name)
    dp.create_auto_cdc_flow(
        target=history_name,
        source=cdf_name,
        keys=["Id"],
        sequence_by=col("_commit_timestamp"),
        stored_as_scd_type=2,
        apply_as_deletes=expr("_change_type = 'delete'"),
        except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
        ignore_null_updates=False,
    )


# One set of flows/tables per Salesforce object present in bronze.
salesforce_objects = [
    row.entity_name
    for row in dp.read(zerobus_table).select("entity_name").distinct().collect()
]
for salesforce_object in salesforce_objects:
    create_pipeline(salesforce_object)
