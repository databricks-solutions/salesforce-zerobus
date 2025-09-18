from pyspark import pipelines as dp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, desc


@dp.table(name="<table_name_here>")
def ingest_salesforce():
    table = "<your_zerobus_table_here>"

    df = dp.readStream(table)

    latest_schema = (
        dp.read(table)
        .filter(col("payload_binary").isNotNull() & col("schema_json").isNotNull())
        .orderBy(desc("timestamp"))
        .select("schema_json")
        .first()[0]
    )

    df = df.select(
        "*",
        from_avro(col("payload_binary"), latest_schema, {"mode": "PERMISSIVE"}).alias(
            "parsed_data"
        ),
    )
    return df.select("*", "parsed_data.*").drop("parsed_data")
