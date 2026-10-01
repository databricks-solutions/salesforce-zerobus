"""
Salesforce PubSub Spark Package

Salesforce PubSub API custom data source for Spark Structured Streaming.
"""

from spark_datasource.SalesforcePubSubDataSource import SalesforcePubSubDataSource, register_salesforce_data_source
from spark_datasource.PubSubAPIClient import PubSubAPIClient, create_client_from_options, create_client

# Provide both function names for backward compatibility
register_data_source = register_salesforce_data_source

__version__ = "1.0.0"
__all__ = [
    "SalesforcePubSubDataSource",
    "register_salesforce_data_source",  # Original name
    "register_data_source",             # Shorter alias
    "PubSubAPIClient", 
    "create_client_from_options",
    "create_client"
] 