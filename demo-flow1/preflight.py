"""Read-only probes for the PostgreSQL to Databricks pipeline."""
import json
import logging
import os
import re
import shutil
import subprocess
import time
from collections import Counter
from pathlib import Path
from urllib.request import urlopen

from databricks import sql
from dotenv import load_dotenv, dotenv_values
from kafka import KafkaConsumer, TopicPartition

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
APP = ROOT / "flow1-python"
TOPIC = "postgres.public.users"
GROUP = "flow1-bronze-consumer"
BROKER = "localhost:29092"
WAREHOUSE = "workspace.bronze.users_cdc"
KAFBAT = "http://localhost:8080"
CONNECTOR = "http://localhost:8083/connectors/postgres-connector/status"
QUERIES = dict(re.findall(
    r"^-- (baseline|duplicates|snapshot|history)(?:[^\n]*)\n(SELECT[^;]+);",
    (HERE / "demo_queries.sql").read_text(), re.M,
))


def configure():
    load_dotenv(HERE / ".env")
    logging.getLogger("kafka").setLevel(logging.CRITICAL)
    logging.getLogger("databricks").setLevel(logging.CRITICAL)
    missing = [tool for tool in ("obs", "node", "ffmpeg", "ffprobe", "psql", "docker")
               if shutil.which(tool) is None]
    if missing:
        raise RuntimeError("Missing tools: " + ", ".join(missing))


def command(args, *, cwd=ROOT, env=None, content=None, timeout=25, label=None):
    output = subprocess.run(args, cwd=cwd, env=env, input=content, capture_output=True,
                            timeout=timeout, text=True)
    if output.returncode:
        message = output.stderr.splitlines()
        if label == "PostgreSQL":
            # Diagnostic SQLSTATEs can be shown. Full diagnostics can contain row values.
            safe = re.search(r"(?:ERROR|FATAL):\s+([0-9A-Z]{5}):", output.stderr)
            message = [f"SQLSTATE {safe[1]}"] if safe else []
        raise RuntimeError(f"{label or args[0]} exited {output.returncode}" +
                           (f": {message[0]}" if message else ""))
    return output.stdout.strip()


def http_json(url):
    with urlopen(url, timeout=8) as response:
        return json.load(response)


def postgres(statement):
    args = ["psql", "-X", "-w", "-At", "-F", "|", "-v", "ON_ERROR_STOP=1",
            "-v", "VERBOSITY=verbose"]
    settings = json.loads((ROOT / "docker/postgres-connector.json").read_text())
    settings = settings.get("config", settings)
    env = {**os.environ, "PGCONNECT_TIMEOUT": "6",
           "PGPASSWORD": settings["database.password"],
           "PGOPTIONS": "-c default_transaction_read_only=on"}
    args += ["-h", "localhost", "-p", "5432", "-U", settings["database.user"],
             "-d", "company_db"]
    return command(args, env=env, content=statement, label="PostgreSQL", timeout=16)


def databricks():
    settings = {**dotenv_values(APP / ".env"), **os.environ}
    try:
        return sql.connect(server_hostname=settings["DATABRICKS_SERVER_HOSTNAME"],
                           http_path=settings["DATABRICKS_HTTP_PATH"],
                           auth_type="databricks-oauth", catalog="workspace",
                           schema="bronze", _socket_timeout=45)
    except Exception as error:
        raise RuntimeError(f"Databricks SQL session failed ({type(error).__name__})") from None


def query(db, statement, parameters=None):
    try:
        with db.cursor() as cursor:
            cursor.execute(statement, parameters)
            return [tuple(row) for row in cursor.fetchall()]
    except Exception as error:
        raise RuntimeError(f"Databricks SQL query failed ({type(error).__name__})") from None


def group_status():
    output = command(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                      "--bootstrap-server", "kafka:9092", "--describe", "--group", GROUP])
    rows = {}
    for line in output.splitlines():
        cols = line.split()
        if len(cols) >= 7 and cols[:2] == [GROUP, TOPIC]:
            partition = int(cols[2])
            rows[partition] = {
                "committed": int(cols[3]) if cols[3] != "-" else None,
                "end": int(cols[4]),
                "lag": int(cols[5]) if cols[5] != "-" else None,
                "active": cols[6] != "-",
            }
    if not rows:
        raise RuntimeError("No topic offsets found for the existing consumer group")
    return rows


def observer():
    return KafkaConsumer(bootstrap_servers=[BROKER], group_id=None,
                         enable_auto_commit=False, request_timeout_ms=8000,
                         session_timeout_ms=6000,
                         auto_offset_reset="earliest")


def envelope(message):
    if message.value is None:
        return None
    return json.loads(message.value)["payload"]


def retained_events():
    counts = Counter()
    snapshots = []
    with observer() as consumer:
        partitions = consumer.partitions_for_topic(TOPIC)
        if not partitions:
            raise RuntimeError(f"Kafka topic {TOPIC} is unavailable")
        targets = [TopicPartition(TOPIC, index) for index in sorted(partitions)]
        consumer.assign(targets)
        end = consumer.end_offsets(targets)
        start = consumer.beginning_offsets(targets)
        for partition in targets:
            consumer.seek(partition, start[partition])
        deadline = time.monotonic() + 12
        while any(consumer.position(p) < end[p] for p in targets):
            if time.monotonic() >= deadline:
                raise TimeoutError("Kafka retained-event read timed out")
            for partition, messages in consumer.poll(timeout_ms=800).items():
                for message in messages:
                    if message.offset >= end[partition]:
                        continue
                    value = envelope(message)
                    if value is None:
                        counts["tombstone"] += 1
                    elif value["op"] == "r":
                        counts["r"] += 1
                        if len(snapshots) < 8:
                            snapshots.append((message.partition, message.offset,
                                              value["source"]["snapshot"]))
                    else:
                        counts[value["op"]] += 1
    return counts, snapshots


def infrastructure():
    compose = command(["docker", "compose", "-f", "docker/docker-compose.yml", "ps",
                       "--format", "json"])
    services = {entry["Service"]: entry["State"] for entry in
                (json.loads(line) for line in compose.splitlines())}
    if any(services.get(service) != "running" for service in ("kafka", "connect", "kafbat")):
        raise RuntimeError(f"Missing infrastructure service: {services}")
    status = http_json(CONNECTOR)
    tasks = [{"id": task["id"], "state": task["state"]} for task in status["tasks"]]
    if status["connector"]["state"] != "RUNNING" or not tasks or any(
            task["state"] != "RUNNING" for task in tasks):
        raise RuntimeError(f"Connector/task status check failed: {[task['state'] for task in tasks]}")
    cluster = next((item["name"] for item in http_json(f"{KAFBAT}/api/clusters")
                    if item["status"] == "ONLINE"), None)
    if not cluster:
        raise RuntimeError("No online Kafbat cluster")
    info = http_json(f"{KAFBAT}/api/clusters/{cluster}/topics/{TOPIC}")
    if info["name"] != TOPIC:
        raise RuntimeError("Kafbat topic verification failed")
    source = postgres("""
        SELECT current_setting('server_version'), current_setting('wal_level'),
               to_regclass('public.users');
    """)
    counts, snapshots = retained_events()
    return {"services": services, "tasks": tasks, "cluster": cluster, "topic": info,
            "postgres": source, "counts": counts, "snapshots": snapshots, "group": group_status()}


if __name__ == "__main__":
    configure()
    print(json.dumps(infrastructure(), indent=2))
    with databricks() as database:
        print("Warehouse returned", query(database, "SELECT 1")[0][0])
        print("Bronze records:", query(database, QUERIES["baseline"])[0][0])
        print("Duplicate Kafka event keys:", len(query(database, QUERIES["duplicates"])))
