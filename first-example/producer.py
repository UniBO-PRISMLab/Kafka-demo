from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=["137.204.143.89:9094"])

producer.send("topic_name", value="Hello, World!".encode("utf-8"))
producer.flush()