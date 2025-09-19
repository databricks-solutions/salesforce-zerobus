from pyspark import pipelines as dp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, desc

# .collects() are generally not recommended in LDP, but becuase the list of Salesforce objects is small it is okay here.


def create_pipeline(salesforce_object):
    @dp.table(name=f"salesforce_parsed_{salesforce_object}")
    def parse_salesforce_stream():
        df = dp.readStream(zerobus_table).filter(
            col("entity_name") == salesforce_object
        )

        latest_schema = (
            dp.read(zerobus_table)
            .filter(
                (col("entity_name") == salesforce_object)
                & (col("payload_binary").isNotNull())
                & (col("schema_json").isNotNull())
            )
            .orderBy(desc("timestamp"))
            .select("schema_json")
            .first()[0]
        )

        df = df.select(
            "*",
            from_avro(
                col("payload_binary"), latest_schema, {"mode": "PERMISSIVE"}
            ).alias("parsed_data"),
        )
        return df.select("*", "parsed_data.*").drop("parsed_data")


zerobus_table = "<your_zerobus_table_name>"
salesforce_objects = [
    sf_object.entity_name
    for sf_object in dp.read(zerobus_table).select("entity_name").distinct().collect()
]
for salesforce_object in salesforce_objects:
    create_pipeline(salesforce_object)
