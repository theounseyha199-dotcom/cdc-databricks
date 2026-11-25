from datetime import datetime, timezone


def transform_cdc_event(event, kafka_offset, kafka_partition=0):

    if event is None:
        return None

    payload = event.get("payload") or {}
    operation = payload.get("op")

    if operation in ("r", "c", "u"):
        row = payload.get("after")
    elif operation == "d":
        row = payload.get("before")
    else:
        return None

    if not row:
        return None

    source = payload.get("source") or {}

    return {
        "id": row.get("id"),
        "name": row.get("name"),
        "email": row.get("email"),
        "age": row.get("age"),
        "operation": operation,
        "source_table": source.get("table"),
        "source_database": source.get("db"),
        "event_timestamp_ms": payload.get("ts_ms"),
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
        "processed_at": datetime.now(timezone.utc).isoformat()
    }
