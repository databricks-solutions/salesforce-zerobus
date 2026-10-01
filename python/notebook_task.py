# Databricks notebook source
# This file is to run the Salesforce-Zerobus service as a databricks job, to run the service locally, use main.py

import json
import logging

from databricks.sdk import WorkspaceClient, dbutils

from salesforce_zerobus import SalesforceZerobus

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)
logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)

# COMMAND ----------
# You can remove this cell after the secrets have been created once.

w = WorkspaceClient()

salesforce_auth = {
    "username": "<your_salesforce_username>",
    "password": "<your_salesforce_password+security_token>",
    "instance_url": "<your_salesforce_instance_url>",
    "client_id": "<your_salesforce_client_id>",
    "client_secret": "<your_salesforce_client_secret>",
}
databricks_auth = {
    "workspace_url": "<your_databricks_workspace_url>",
    "api_token": "<your_databricks_api_token>",
    "ingest_endpoint": "<your_databricks_ingest_endpoint>",
    "sql_endpoint": "/sql/1.0/warehouses/<your_sql_endpoint_id>",
    "sql_workspace_url": "<your_databricks_sql_workspace_url>",
    "sql_api_token": "<your_databricks_sql_api_token>",
}

secret_scope_name = "<your-secret-scope-name>"

w.secrets.put_secret(
    scope=secret_scope_name,
    key="salesforce_auth",
    string_value=json.dumps(salesforce_auth),
)
w.secrets.put_secret(
    scope=secret_scope_name,
    key="databricks_auth",
    string_value=json.dumps(databricks_auth),
)

# COMMAND ----------
salesforce_object = dbutils.widgets.get("salesforce_object")

salesforce_auth = json.loads(
    dbutils.secrets.get(scope=secret_scope_name, key="salesforce_auth")
)
databricks_auth = json.loads(
    dbutils.secrets.get(scope=secret_scope_name, key="databricks_auth")
)
streamer = SalesforceZerobus(
    sf_object_channel=f"{salesforce_object}",
    databricks_table=f"catalog.schema.{salesforce_object.lower()}",
    salesforce_auth=salesforce_auth,
    databricks_auth=databricks_auth,
)

print("Starting Salesforce to Databricks streaming...")
print(
    f"Monitoring Channel:{streamer.sf_object_channel} → Databricks Table:{streamer.databricks_table}"
)

streamer.start()
