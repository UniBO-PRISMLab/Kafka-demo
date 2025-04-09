import json
import random
import time
import math
import logging
from collections import defaultdict
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions
from kafka.errors import TopicAlreadyExistsError, KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka")
logger.setLevel(logging.INFO)

NUMBER_OF_ROOMS = 4
BOOTSTRAP_SERVERS = ["localhost:9095"]
REPLICATION_FACTOR = 3
DEVICE_TYPES = {"temperature": "temperature", "air_quality": "air_quality", "motion": "motion"}


def create_devices(num_rooms: int):
    device_list = []
    device_count_by_type = defaultdict(int)
    room_assignment = {}

    for room_id in range(num_rooms):
        for device_type in DEVICE_TYPES.keys():
            idx = device_count_by_type[device_type]
            device_id = f"{device_type}-{idx:03d}"

            device_list.append((device_id, device_type))
            room_assignment[device_id] = room_id
            device_count_by_type[device_type] += 1

    return device_list, device_count_by_type, room_assignment


def generate_device_data(device_type: str):
    if device_type == "temperature":
        return {"value": round(random.uniform(15, 35), 2), "unit": "Celsius"}
    elif device_type == "air_quality":
        return {"pm25": round(random.uniform(0, 100), 2), "pm10": round(random.uniform(0, 200), 2)}
    elif device_type == "motion":
        return {"motion_detected": random.choice([True, False])}
    else:
        return {}


def json_serializer(data) -> bytes:
    return json.dumps(data).encode("utf-8")


def create_topics(device_distribution) -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    topics = []
    for device_type, count in device_distribution.items():
        topic = DEVICE_TYPES[device_type]
        num_partitions = math.ceil(count / 5)
        topics.append(NewTopic(name=topic, num_partitions=num_partitions, replication_factor=REPLICATION_FACTOR))
    try:
        admin.create_topics(topics)
        print("Topics created...")
    except TopicAlreadyExistsError:
        print("Topics already exist — skipping creation.")
    finally:
        admin.close()


def check_partitions(device_distribution) -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    topic_names = [DEVICE_TYPES[dt] for dt in device_distribution]
    try:
        topics = admin.describe_topics(topics=topic_names)
        for topic_metadata in topics:
            topic_name = topic_metadata["topic"]
            if topic_name not in topic_names:
                continue
            current_partitions = len(topic_metadata["partitions"])
            desired_partitions = math.ceil(device_distribution[topic_name] / 5)
            if current_partitions < desired_partitions:
                try:
                    admin.create_partitions({topic_name: NewPartitions(total_count=desired_partitions)})
                    logging.info(f"Partitions for topic '{topic_name}' updated to {desired_partitions}.")
                except KafkaError as e:
                    logging.warning(f"Could not update partitions for topic '{topic_name}': {e}")
    finally:
        admin.close()


def simulate_devices() -> None:
    device_list, device_count_by_type, room_assignment = create_devices(NUMBER_OF_ROOMS)

    create_topics(device_count_by_type)
    check_partitions(device_count_by_type)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=str.encode,
        value_serializer=json_serializer,
        acks="all",  # Wait for all ISR to ack
        compression_type="gzip",  # Compress payload
    )

    try:
        while True:
            for device_id, device_type in device_list:
                topic = DEVICE_TYPES[device_type]
                payload = generate_device_data(device_type)
                room = room_assignment[device_id]
                payload.update({"device_id": device_id, "room": room_assignment[device_id], "timestamp": time.time()})

                producer.send(topic=topic, key=device_id, value=payload, partition=room)
                print(f"[SEND] topic={topic} | key={device_id} | value={payload}")
                time.sleep(5)

    except KeyboardInterrupt:
        print("\n Producer stopped by user.")
    finally:
        producer.close()


if __name__ == "__main__":
    simulate_devices()
