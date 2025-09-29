"""
SalesforceZerobus - Simple Python library for streaming Salesforce Change Data Capture 
events to Databricks Delta tables in real-time.

Example:
    from salesforce_zerobus import SalesforceZerobus

    streamer = SalesforceZerobus(
        sf_object="Account",
        databricks_table="catalog.schema.account_events",
        salesforce_auth={
            "username": "user@company.com",
            "password": "password+token",
            "instance_url": "https://company.salesforce.com"
        },
        databricks_auth={
            "workspace_url": "https://workspace.cloud.databricks.com",
            "client_id": "your-service-principal-client-id",
            "client_secret": "your-service-principal-client-secret",
            "ingest_endpoint": "workspace-id.ingest.cloud.databricks.com"
        }
    )
    
    # Start streaming (blocking)
    streamer.start()
    
    # Or use async context manager
    async with streamer:
        await streamer.stream_forever()
"""

from .core import SalesforceZerobus

# Version info
__version__ = "1.0.0"
__author__ = "Grant Doyle"
__description__ = "Simple Python library for streaming Salesforce CDC events to Databricks"

# Main export
__all__ = ["SalesforceZerobus"]