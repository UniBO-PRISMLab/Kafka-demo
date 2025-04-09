import json
import random
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


BOOTSTRAP_SERVERS = ["localhost:9095"]
REPLICATION_FACTOR = 2
NUM_PARTITIONS = 2


def json_serializer(data) -> bytes:
    return json.dumps(data).encode("utf-8")


def create_topics(topics) -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    new_topics = [
        NewTopic(name=topic, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR) for topic in topics
    ]

    try:
        admin.create_topics(new_topics)
        print("Topics created...")
    except TopicAlreadyExistsError:
        print("Topics already exist — skipping creation.")
    except Exception as e:
        print(f"Error creating topics: {e}")
    finally:
        admin.close()


def run_example() -> None:
    print("running...")
    topics = ["first_topic", "second_topic"]

    create_topics(topics)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=str.encode,
        value_serializer=json_serializer,
    )

    try:
        while True:
            topic = random.choice(topics)
            key = str(random.randint(0, 5))
            payload = {"value": round(random.uniform(15, 35), 2), "unit": "Celsius", "timestamp": time.time()}
            producer.send(topic=topic, key=key, value=payload)
            print(f"[SEND] topic={topic} | key={key} | value={payload}")
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
    except Exception as e:
        print(f"Producer error: {e}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run_example()
