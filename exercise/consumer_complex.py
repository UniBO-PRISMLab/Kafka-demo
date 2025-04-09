import json
import logging

from kafka import KafkaConsumer, TopicPartition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka")
logger.setLevel(logging.INFO)


def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


BOOTSTRAP_SERVERS = ["localhost:9095"]
TOPICS = ["temperature", "air_quality", "motion"]
my_room_ids = [0, 1, 2]
GROUP_ID = "IoT:01"

def main():
    """
    If group_id is set, consumers with same ID will share the load.
    If group_id is None, consumer will read all messages independently.
    """

    # ---------- CONSUMER CONFIGURATION ----------
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Deserialization function
        value_deserializer=json_deserializer,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        # Enable automatic commits or not
        enable_auto_commit=True,  # Change to False to manage commits manually
        auto_commit_interval_ms=5000,  # How often to commit offsets (if auto)
        auto_offset_reset="earliest", #or "latest",
        # Group ID determines consumer group behavior
        group_id=GROUP_ID,  # None → independent consumer; same ID → shared load
    )
    partitions = [TopicPartition(topic, p) for topic in TOPICS for p in my_room_ids]
    consumer.assign(partitions)
    print(f" Kafka consumer started | Group: {GROUP_ID or 'INDEPENDENT'}\n")

    try:
        while True: 
            records = consumer.poll(timeout_ms=1000)  # long-polling

            for messages in records.items():
                for msg in messages:
                    print(f"[RECEIVED] topic={msg.topic}, partition={msg.partition}, offset={msg.offset}")
                    print(f"           key={msg.key}, value={msg.value}, ts={msg.timestamp}")
                    print("-" * 60)


    except KeyboardInterrupt:
        print("\n Consumer stopped by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
