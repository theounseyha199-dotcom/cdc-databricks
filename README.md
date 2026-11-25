# PostgreSQL CDC to Databricks

Both flows read the same schema-less Debezium JSON from
`lakehouse.public.customers`. The local Python consumer writes to
`cdc_bronze.bronze_python` through a Databricks SQL warehouse. Databricks
Structured Streaming uses its Kafka connector to write `cdc_bronze.bronze_spark`
and applies changes to the Delta table `cdc_silver.customer_state`.
`cdc_silver.silver_customers` exposes the active rows.

## Prerequisites

- Docker with Compose v2.24.4 or later, Python 3.11+, and a Databricks workspace
  with a SQL warehouse, serverless notebooks, and a writable Unity Catalog
  catalog and volume.
- For both flows, a Kafka broker reachable from Docker Connect, the Python
  consumer, and Databricks serverless compute, using SASL/PLAIN over TLS with a
  trusted certificate. A local-only broker is included for the Flow 1 demo.
- Keep the demo topic on **one partition** for its lifetime. Kafka offsets order
  changes within that partition. Configure the topic with delete retention
  rather than compaction so the Bronze comparison can replay the
  original history. Retain records for the duration of the demo.

Free Edition restricts outbound networking by default. Check the
[Free Edition limits](https://docs.databricks.com/gcp/en/getting-started/free-edition-limitations)
and confirm that Databricks can read from the broker, including the advertised
broker addresses. If the workspace blocks external Kafka, use a workspace with
Kafka connectivity for Flow 2.

## Run

1. Copy `.env.example` to `.env`. Fill in the SQL warehouse's server hostname
   (without `https://`) and HTTP path from **Connection details**, and set
   `DATABRICKS_CATALOG` to a writable catalog. Configure Databricks
   [unified authentication](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth)
   separately. For interactive use, run
   `databricks auth login --host https://<workspace-host> --profile lakehouse`
   and export `DATABRICKS_CONFIG_PROFILE=lakehouse`.
2. Select that catalog in Databricks and run `databricks/00_setup.sql`. For the
   remote broker, create a Databricks secret scope named `lakehouse` with keys
   `kafka-username` and `kafka-password`.
3. Start PostgreSQL and Kafka Connect. For the local Flow 1 demo, run
   `docker compose up -d --wait`. For both flows, set the remote Kafka fields
   in `.env` and run
   `docker compose -f compose.yaml -f compose.remote.yaml up -d --wait connect`.
   The Connect worker needs access to create or use its three internal topics
   and to produce to the CDC topic. If your provider requires pre-created
   topics, create the CDC topic with one partition and create the Connect
   config, offset, and status topics with compaction. The config topic must
   have exactly one partition. The topic names are in `compose.yaml`.
4. Register the connector once Connect's HTTP API responds. Confirm the
   connector and its task report `RUNNING`.

   ```bash
   curl --fail --retry 12 --retry-connrefused --retry-delay 2 \
     -X PUT -H 'Content-Type: application/json' \
     --data-binary @infra/debezium/customers.json \
     http://localhost:18083/connectors/customers/config
   curl --fail http://localhost:18083/connectors/customers/status
   ```

5. Install and start Flow 1 from the repository root. Run one consumer at a
   time for the Bronze table.

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   python -m pip install -e ./flow1
   set -a
   source .env
   set +a
   lakehouse-consumer
   ```

6. Import both Python notebooks into Databricks. In
   `01_kafka_to_bronze.py` set the catalog, broker endpoint, and secret scope
   widgets. Run it on serverless compute to verify the authenticated Kafka
   read. Run `02_bronze_to_silver.py` with the same catalog. Both notebooks
   use `AvailableNow`, so re-run them in order after producing additional
   events. Keep one active run per notebook and checkpoint.

## Verify inserts, updates, deletes, and retries

After the connector starts, generate changes and capture the PostgreSQL result.

```bash
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U cdc -d lakehouse \
  < demo/changes.sql
```

Wait for Flow 1 to catch up, run both notebooks in order, then run
`demo/verify.sql` in Databricks. The duplicate and parity queries should return
zero rows. The active Silver rows should match the PostgreSQL output, with
customer 100 updated and customer 101 absent. The seeded row should produce a
snapshot event if it was present when the connector took its initial snapshot.

Restart the consumer and rerun the notebooks without generating new changes.
The queries should return the same results. To exercise a Flow 1 replay, stop
the existing consumer and launch it once with a fresh group ID
(`KAFKA_GROUP_ID=lakehouse-python-replay lakehouse-consumer`). Stop the replay
consumer after it catches up and repeat the duplicate check.

Both Bronze tables retain the raw key and payload, Kafka coordinates,
operation, and row images. Tombstones remain in Bronze and are excluded from
the Silver merge. The Silver Delta table keeps the newest offset for every ID,
including deleted IDs, so an older replay cannot resurrect a deleted customer.
Use fresh tables, checkpoints, and a new consumer group for a newly created
Kafka topic with reused offset numbers.

Run the local unit tests with
`PYTHONPATH=flow1/src python3 -m unittest discover -s flow1/tests -v`.
