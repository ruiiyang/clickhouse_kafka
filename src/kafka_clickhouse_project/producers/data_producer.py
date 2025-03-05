from typing import Dict, Any
import json
from kafka import KafkaProducer
from ..config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

def produce_to_kafka(data: Dict[str, Any]) -> None:
    """
    Produce data to Kafka topic
    
    Args:
        data: Dictionary containing data to be sent to Kafka
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    producer.send(KAFKA_TOPIC, data)
    producer.flush() 