from kafka import KafkaConsumer

consumer = KafkaConsumer("first_topic", bootstrap_servers=["localhost:9094"])
for message in consumer:
    print(
        "%s:%d:%d: key=%s value=%s timestamp=%s"
        % (message.topic, message.partition, message.offset, message.key, message.value, message.timestamp)
    )
