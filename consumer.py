from kafka import KafkaConsumer
import json
from pymongo import MongoClient
from kafka.admin import KafkaAdminClient, NewTopic

# Kafka Configuration
KAFKA_TOPIC = "amazon-products"
KAFKA_SERVER = "localhost:9092"
GROUP_ID = "amazon_product_consumers"

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "amazon"

# Set up Kafka Admin Client to manage topics
admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)

# Delete the topic if it exists (optional, for cleanup)
try:
    admin_client.delete_topics(topics=[KAFKA_TOPIC])
except Exception as e:
    print(f"Error deleting topic: {e}")

# Create the topic (optional, if not already created)
try:
    admin_client.create_topics(new_topics=[NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)])
except Exception as e:
    print(f"Error creating topic: {e}")

# Set up MongoDB client
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[DATABASE_NAME]

# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    max_poll_records=100,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda m: m.decode('utf-8') if m else None,  # Handle None key gracefully
    auto_offset_reset='earliest',
    group_id=GROUP_ID
)

if __name__ == "__main__":
    for message in consumer:
        key = message.key
        product_info = message.value

        if key is not None and isinstance(product_info, list):
            # Determine collection name from key (product name)
            collection_name = key.replace(' ', '_').lower()  # Ensure valid collection name format
            mongo_collection = mongo_db[collection_name]

            # Insert product info into the respective collection
            mongo_collection.insert_many(product_info)
            print(f"Inserted product info into collection '{collection_name}'")
        else:
            print(f"Ignored message due to missing key or unexpected data format: key={key}, value={product_info}")
