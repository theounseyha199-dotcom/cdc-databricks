import json

from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata

from transform import transform_cdc_event
from bronze_writer import BronzeWriter

from config import (
    KAFKA_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID
)


def commit_message(consumer, message):

    partition = TopicPartition(
        message.topic,
        message.partition
    )

    consumer.commit({
        partition: OffsetAndMetadata(
            message.offset + 1,
            "",
            -1
        )
    })


def main():

    consumer = None
    writer = None

    try:

        writer = BronzeWriter()

        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=1,
            max_poll_interval_ms=900000
        )

        print("\n=== FLOW 1: FULL CDC PIPELINE ===")
        print("PostgreSQL -> Kafka -> Databricks")
        print("Listening for CDC events...\n")

        for message in consumer:

            if message.value is None:

                print(
                    f"TOMBSTONE SKIPPED | "
                    f"OFFSET={message.offset}"
                )

                commit_message(consumer, message)
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
                raise ValueError(
                    f"Invalid CDC event at offset "
                    f"{message.offset}"
                )

            record["kafka_topic"] = message.topic

            # Write to Databricks Bronze
            writer.write(record)

            # Commit only after successful write
            commit_message(consumer, message)

            print(
                f"OFFSET COMMITTED | "
                f"PARTITION={message.partition} | "
                f"NEXT_OFFSET={message.offset + 1}",
                flush=True
            )

    except KeyboardInterrupt:
        print("\nPipeline stopped by user.")

    except Exception as error:
        print(f"\nPIPELINE ERROR: {error}")
        raise

    finally:

        if consumer is not None:
            consumer.close()

        if writer is not None:
            writer.close()

        print("Pipeline closed.")


if __name__ == "__main__":
    main()
