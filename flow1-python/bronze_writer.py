from databricks import sql
from config import (
    DATABRICKS_SERVER_HOSTNAME,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_CATALOG,
    DATABRICKS_SCHEMA,
    DATABRICKS_TABLE
)


class BronzeWriter:

    def __init__(self):

        self.connection = sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            auth_type="databricks-oauth",
            catalog=DATABRICKS_CATALOG,
            schema=DATABRICKS_SCHEMA
        )

        print("Bronze Writer connected.")

    def write(self, record):

        query = f"""
            MERGE INTO {DATABRICKS_TABLE} AS target

            USING (
                SELECT
                    CAST(? AS INT) AS id,
                    CAST(? AS STRING) AS name,
                    CAST(? AS STRING) AS email,
                    CAST(? AS INT) AS age,
                    CAST(? AS STRING) AS operation,
                    CAST(? AS STRING) AS source_table,
                    CAST(? AS STRING) AS source_database,
                    CAST(? AS BIGINT) AS event_timestamp_ms,
                    CAST(? AS INT) AS kafka_partition,
                    CAST(? AS BIGINT) AS kafka_offset,
                    CAST(? AS TIMESTAMP) AS processed_at,
                    CAST(? AS STRING) AS kafka_topic
            ) AS source

            ON target.kafka_topic = source.kafka_topic
            AND target.kafka_partition = source.kafka_partition
            AND target.kafka_offset = source.kafka_offset

            WHEN NOT MATCHED THEN
                INSERT *
        """

        values = (
            record["id"],
            record["name"],
            record["email"],
            record["age"],
            record["operation"],
            record["source_table"],
            record["source_database"],
            record["event_timestamp_ms"],
            record["kafka_partition"],
            record["kafka_offset"],
            record["processed_at"],
            record["kafka_topic"]
        )

        with self.connection.cursor() as cursor:
            cursor.execute(query, values)

        print(
            f"WRITE SUCCESS | "
            f"OP={record['operation']} | "
            f"ID={record['id']} | "
            f"OFFSET={record['kafka_offset']}"
        )

    def close(self):
        self.connection.close()
        print("Bronze Writer closed.")
