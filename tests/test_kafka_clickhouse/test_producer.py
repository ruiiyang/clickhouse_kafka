import pytest
from src.kafka_clickhouse_project.utils.fake_data import generate_fake_data
from src.kafka_clickhouse_project.producers.data_producer import produce_to_kafka

def test_generate_fake_data():
    """
    Test the generate_fake_data function to ensure it generates the correct data structure.
    """
    # Generate fake data
    data = generate_fake_data(num_messages=2)
    
    # Print generated data for debugging
    print("Generated data:", data)
    
    # Assertions
    assert len(data) == 2  # Ensure 2 messages are generated
    for photo in data:
        assert "photo" in photo  # Ensure "photo" key exists
        assert "tags" in photo["photo"]  # Ensure "tags" key exists
        assert len(photo["photo"]["tags"]["tag"]) == 2  # Ensure 2 tags per photo

def test_produce_to_kafka():
    """
    Test the produce_to_kafka function to ensure it can send data to Kafka without errors.
    """
    # Generate fake data
    data = generate_fake_data(num_messages=1)
    
    # Attempt to produce data to Kafka
    try:
        produce_to_kafka(data)
    except Exception as e:
        pytest.fail(f"Failed to produce to Kafka: {str(e)}")

if __name__ == "__main__":
    pytest.main()