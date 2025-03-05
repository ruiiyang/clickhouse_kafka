import json
import uuid
import random
import time
import requests
from confluent_kafka import Producer, Consumer

# ClickHouse Server Info
CLICKHOUSE_URL = "http://localhost:8123"  # Change if needed
CLICKHOUSE_DB = "default"
CLICKHOUSE_TABLE = "photo_events"

# Kafka Config
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "photo.tag.bdr.events"

producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "python-producer"
}
consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "clickhouse-consumer",
    "auto.offset.reset": "earliest"
}

producer = Producer(producer_conf)
consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])


# Function to generate fake data
def generate_fake_data(num_messages=1):
    """
    Generate fake photo data with tags in JSON format.

    Args:
        num_messages (int): Number of photo messages to generate. to simplify, it is 1. But in theory, it should be randome number of message that is being generated per second. 

    Returns:
        list: A list of dictionaries, each representing a photo with tags.
    """
    all_photos = []
    
    for _ in range(num_messages):
        tags = []
        for _ in range(2):  # Generate exactly 2 tags per photo
            tags.append({
                "id": str(uuid.uuid4()),  # Generate a unique ID for each tag
                "raw": f"tag_{random.randint(1, 100)}"  # Generate a random tag name
            })
        
        # Construct the photo JSON structure
        photo_json = {
            "photo": {
                "tags": {
                    "tag": tags  # Add the list of tags
                }
            }
        }
        all_photos.append(photo_json)
    
    return all_photos


# Send data to Kafka (JSON format)
def produce_to_kafka(json_data):
    try:
        producer.produce(KAFKA_TOPIC, value=json.dumps(json_data).encode('utf-8'))
        producer.flush()
    except Exception as e:
        print(f"❌ Kafka Produce Error: {e}")



# Insert data into ClickHouse
def insert_into_clickhouse(id, raw):
    query = f"INSERT INTO {CLICKHOUSE_TABLE} (id, raw) VALUES ('{id}', '{raw}')"
    try:
        response = requests.post(f"{CLICKHOUSE_URL}/?query={query}")
        if response.status_code == 200:
            print(f"✅ Inserted into ClickHouse: id={id}, raw={raw}")
        else:
            print(f"❌ ClickHouse Insert Error: {response.text}")
    except requests.RequestException as e:
        print(f"❌ Error inserting into ClickHouse: {e}")


# Kafka Consumer - Read & Insert into ClickHouse
def consume_from_kafka():
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"❌ Kafka Error: {msg.error()}")
            continue

        try:
            # Directly parse JSON message
            message = json.loads(msg.value().decode('utf-8'))  
            tags = message["photo"]["tags"]["tag"]
            
            for tag in tags:
                insert_into_clickhouse(tag["id"], tag["raw"])
                print(tag["id"], tag["raw"])
                print(f"✅ Inserted into ClickHouse: id={tag['id']}, raw={tag['raw']}")
        
        except Exception as e:
            print(f"❌ JSON Decode Error: {e}")


# Start Producer
def start_producer():
    while True:
        num_messages = 1#random.randint(1, 5)
        fake_data = generate_fake_data(num_messages=num_messages)

        for message in fake_data:
            produce_to_kafka(message)
            print(f" Sent: {message}")
        print(f"🚀 Sent {num_messages} messages")
        time.sleep(5)


# Start Consumer
if __name__ == "__main__":
    from multiprocessing import Process
    
    producer_process = Process(target=start_producer)
    consumer_process = Process(target=consume_from_kafka)
    
    producer_process.start()
    consumer_process.start()
    
    producer_process.join()
    consumer_process.join()
