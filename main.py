from src.kafka_clickhouse_project.utils.fake_data import generate_fake_data
from src.kafka_clickhouse_project.producers.data_producer import produce_to_kafka
from src.kafka_clickhouse_project.consumers.data_consumer import consume_from_kafka
from src.kafka_clickhouse_project.config import CLICKHOUSE_URL, CLICKHOUSE_TABLE
from src.kafka_clickhouse_project.consumers.data_consumer import insert_into_clickhouse

def main():
    try:
       
        print("Starting making data...")
        fake_data = generate_fake_data()
        print(f"Already {len(fake_data)} data generated")

        
        print("Sending data to Kafka...")
        produce_to_kafka(fake_data)
        print("Data already sent to Kafka")

        
        print("Consuming data from Kafka...")
        consumed_data = consume_from_kafka()
        print("Data already consumed from Kafka")

        
        print("Inserting data into ClickHouse...")
        insert_into_clickhouse(consumed_data)
        print(f"Data already inserted into ClickHouse table {CLICKHOUSE_TABLE}")

    except Exception as e:
        print(f" Error: {str(e)}")

if __name__ == "__main__":
    print(f"Starting data processing pipeline")
    print(f"ClickHouse connection URL: {CLICKHOUSE_URL}")
    print(f"Target table: {CLICKHOUSE_TABLE}")
    main()