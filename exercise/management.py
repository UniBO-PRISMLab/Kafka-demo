from kafka.admin import KafkaAdminClient
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = ["localhost:9094", "localhost:9095", "localhost:9096"]

admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
cluster_metadata = consumer._client.cluster

brokers = cluster_metadata.brokers()
print(f"Number of brokers in the cluster: {len(brokers)}")
for broker in brokers:
    print(f"Broker ID: {broker.nodeId}, Host: {broker.host}, Port: {broker.port}")

topics = admin_client.describe_topics()
print(f"\nNumber of topics in the cluster: {len(topics)}")

for topic_metadata in topics:
    print(f"\nTopic: {topic_metadata["topic"]}")
    print(f"  Number of partitions: {len(topic_metadata["partitions"])}")

    for partition in topic_metadata["partitions"]:
        print(f"    Partition ID: {partition["partition"]}")
        print(f"      Leader: Broker ID {partition["leader"]}")
        print(f"      Replicas: {partition["replicas"]}")
        print(f"      ISR: {partition["isr"]}")

print("\nConsumer Groups:")
groups = admin_client.list_consumer_groups()
for group_info in groups:
    group_id = group_info[0]
    print(f"\n- Group ID: {group_id}")
    try:
        desc = admin_client.describe_consumer_groups([group_id])[0]
        print(f"  State: {desc.state}")
        print(f"  Protocol: {desc.protocol_type}")
        for member in desc.members:
            print(f"    Member ID: {member.member_id}")
            print(f"    Client ID: {member.client_id}")
            print(f"    Client Host: {member.client_host}")
    except Exception as e:
        print(f"  Could not describe group: {e}")
