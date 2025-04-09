from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=["localhost:9094"])

producer.send("topic_name", value="Hello, World!".encode("utf-8"))
producer.flush()