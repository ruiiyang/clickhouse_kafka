from typing import Dict, Any
import json
from kafka import KafkaConsumer
from clickhouse_driver import Client
from ..config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    CLICKHOUSE_URL,
    CLICKHOUSE_TABLE
)

def consume_from_kafka() -> None:
    """Consume data from Kafka topic and insert into ClickHouse"""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    client = Client.from_url(CLICKHOUSE_URL)
    
    for message in consumer:
        insert_into_clickhouse(message.value, client)

def insert_into_clickhouse(data: Dict[str, Any], client: Client) -> None:
    """
    Insert data into ClickHouse
    
    Args:
        data: Dictionary containing data to be inserted
        client: ClickHouse client instance
    """
   
    pass 