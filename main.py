# This file is for local testing of the Salesforce Zerobus functionality, to run in a databricks job, use notebook_task.py

import logging
import os

from dotenv import load_dotenv

from salesforce_zerobus import SalesforceZerobus

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

streamer = SalesforceZerobus(
    sf_object_channel="ChangeEvents",  # Use "ChangeEvents" to get change events for ALL objects, otherwise specify a specific object like "AccountChangeEvent"
    databricks_table="catalog.schema.salesforce_change_events",
    salesforce_auth={
        "username": os.getenv("SALESFORCE_USERNAME"),
        "password": os.getenv("SALESFORCE_PASSWORD"),
        "instance_url": os.getenv("SALESFORCE_INSTANCE_URL"),
    },
    databricks_auth={
        "workspace_url": os.getenv("DATABRICKS_WORKSPACE_URL"),
        "api_token": os.getenv("DATABRICKS_API_TOKEN"),
        "ingest_endpoint": os.getenv("DATABRICKS_INGEST_ENDPOINT"),
        "sql_endpoint": os.getenv("DATABRICKS_SQL_ENDPOINT"),
        "sql_workspace_url": os.getenv("DATABRICKS_SQL_WORKSPACE_URL"),
        "sql_api_token": os.getenv("DATABRICKS_SQL_API_TOKEN"),
    },
)

print("Starting Salesforce to Databricks streaming...")
print(
    f"Monitoring Channel:{streamer.sf_object_channel} → Databricks Table:{streamer.databricks_table}"
)

streamer.start()
