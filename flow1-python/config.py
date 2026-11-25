import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "postgres.public.users"
KAFKA_GROUP_ID = "flow1-bronze-consumer"

# Databricks Configuration
DATABRICKS_SERVER_HOSTNAME = os.environ[
    "DATABRICKS_SERVER_HOSTNAME"
]

DATABRICKS_HTTP_PATH = os.environ[
    "DATABRICKS_HTTP_PATH"
]

DATABRICKS_CATALOG = "workspace"
DATABRICKS_SCHEMA = "bronze"
DATABRICKS_TABLE = "workspace.bronze.users_cdc"
