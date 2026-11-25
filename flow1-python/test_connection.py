import os
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

print("Connecting to Databricks...")

with sql.connect(
    server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    auth_type="databricks-oauth",
    catalog="workspace",
    schema="bronze"
) as connection:

    with connection.cursor() as cursor:

        cursor.execute("SELECT 1")
        print("Connection test:", cursor.fetchone()[0])

        cursor.execute("""
            SELECT COUNT(*)
            FROM workspace.bronze.users_cdc
        """)

        print("Bronze records:", cursor.fetchone()[0])

print("DATABRICKS CONNECTION SUCCESSFUL")
