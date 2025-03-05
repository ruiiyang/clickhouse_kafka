# Data Processing Pipeline

This project implements a data processing pipeline that generates fake data, processes it through Kafka, and stores it in ClickHouse.

## Features

- Fake data generation
- Kafka producer and consumer implementation
- ClickHouse data insertion
- Complete data processing pipeline

## Prerequisites

- Python 3.7+
- Apache Kafka
- Zookeeper
- ClickHouse Database

## Project Structure
```
BDR/
├── src/
│   └── kafka_clickhouse_project/
│       ├── __init__.py
│       ├── producers/
│       │   ├── __init__.py
│       │   └── data_producer.py
│       └── utils/
│           ├── __init__.py
│           └── fake_data.py
└── tests/
    └── test_kafka_clickhouse/
        ├── __init__.py
        └── test_producer.py
```

## testing using produce.py or landing_api.ipynb

produce.py is the producer that generates fake data and sends it to Kafka.consumes the data from Kafka and stores it in ClickHouse.
landing_api.ipynb has various format of the data format to show there are many ways of the data format.
    multiple tags, multiple photos, multiple users, multiple events, etc.

## Run the project
currently produce.py is the main file to run the data generation and processing pipeline.

there is not a finished project, it is a work in progress. still issues to be solved with Clickhouse.
 1. pending items:
 using Avro format to send data to clickhouse 
 load clickhouse data in (some issue with parsing)







