# This file is for local testing of the Salesforce Zerobus functionality, to run in a databricks job, use notebook_task.py

import logging
import os

from dotenv import load_dotenv

from salesforce_zerobus import SalesforceZerobus

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logging.getLogger("zerobus_sdk").setLevel(logging.INFO)

streamer = SalesforceZerobus(
    auto_create_table=True,
    sf_object_channel=os.getenv(
        "SALESFORCE_CHANGE_EVENT_CHANNEL"
    ),  # Use "ChangeEvents" to get change events for ALL objects, otherwise specify a specific object like "AccountChangeEvent"
    databricks_table=os.getenv("DATABRICKS_ZEROBUS_TARGET_TABLE"),
    salesforce_auth={
        "username": os.getenv("SALESFORCE_USERNAME"),
        "password": os.getenv("SALESFORCE_PASSWORD"),
        "instance_url": os.getenv("SALESFORCE_INSTANCE_URL"),
    },
    databricks_auth={
        "workspace_url": os.getenv("DATABRICKS_WORKSPACE_URL"),
        "client_id": os.getenv("DATABRICKS_CLIENT_ID"),
        "client_secret": os.getenv("DATABRICKS_CLIENT_SECRET"),
        "ingest_endpoint": os.getenv("DATABRICKS_INGEST_ENDPOINT"),
        "sql_endpoint": os.getenv("DATABRICKS_SQL_ENDPOINT"),
    },
)

print("Starting Salesforce to Databricks streaming...")
print(
    f"Monitoring Channel:{streamer.sf_object_channel} → Databricks Table:{streamer.databricks_table}"
)

if __name__ == "__main__":
    streamer.start()
