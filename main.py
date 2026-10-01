import logging
import os

from dotenv import load_dotenv

from salesforce_zerobus import SalesforceZerobus

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logging.getLogger("zerobus_sdk").setLevel(logging.WARNING)

streamer = SalesforceZerobus(
    sf_object_channel=os.getenv(
        "SALESFORCE_CHANGE_EVENT_CHANNEL"
    ),  # Use "ChangeEvents" to get change events for ALL objects, otherwise specify a specific object like "AccountChangeEvent"
    databricks_table=os.getenv("DATABRICKS_ZEROBUS_TARGET_TABLE"),
    salesforce_auth={
        # "username": os.getenv("SALESFORCE_USERNAME"),
        # "password": os.getenv("SALESFORCE_PASSWORD"),
        "instance_url": os.getenv("SALESFORCE_INSTANCE_URL"),
        "client_id": os.getenv("SALESFORCE_CLIENT_ID"),
        "client_secret": os.getenv("SALESFORCE_CLIENT_SECRET"),
    },
    databricks_auth={
        "workspace_url": os.getenv("DATABRICKS_WORKSPACE_URL"),
        "client_id": os.getenv("DATABRICKS_CLIENT_ID"),
        "client_secret": os.getenv("DATABRICKS_CLIENT_SECRET"),
        "ingest_endpoint": os.getenv("DATABRICKS_INGEST_ENDPOINT"),
        "sql_endpoint": os.getenv("DATABRICKS_SQL_ENDPOINT"),
    },
    # Durability vs. latency:
    #   True  (default) — block each batch until Zerobus durably acks it. Strongest
    #                     guarantee, but adds the server's per-batch commit latency.
    #   False           — fire-and-forget: submit and move on for lower latency / higher
    #                     throughput. Still at-least-once (SDK background sender + flush on
    #                     shutdown + resume from the table's last replay id on restart).
    wait_for_durability=True,
)

print("Starting Salesforce to Databricks streaming...")
print(
    f"Monitoring Channel:{streamer.sf_object_channel} → Databricks Table:{streamer.databricks_table}"
)

if __name__ == "__main__":
    streamer.start()
