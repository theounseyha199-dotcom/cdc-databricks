import json

from kafka import KafkaConsumer
from transform import transform_cdc_event
from bronze_writer import BronzeWriter
from config import (
    KAFKA_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS
)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id="flow1-bronze-writer-test",
    enable_auto_commit=False,
    consumer_timeout_ms=15000
)

writer = None

try:
    writer = BronzeWriter()

    for message in consumer:

        if message.value is None:
            continue

        event = json.loads(
            message.value.decode("utf-8")
        )

        record = transform_cdc_event(
            event=event,
            kafka_offset=message.offset,
            kafka_partition=message.partition
        )

        if record is None:
            continue

        record["kafka_topic"] = message.topic

        print(json.dumps(record, indent=4))

        writer.write(record)

        print("BRONZE WRITER TEST PASSED")
        break

    else:
        print("No valid Kafka event received.")

finally:
    consumer.close()

    if writer is not None:
        writer.close()
