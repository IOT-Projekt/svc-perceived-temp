import os
import logging
from typing import List
from kafka import KafkaConsumer, KafkaProducer
import sys
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class KafkaConfig:
    """Encapsulates Kafka configuration using Singleton pattern."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KafkaConfig, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize configuration values."""
        self.bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )  # Kafka broker(s)
        self.group_id = os.getenv("KAFKA_GROUP_ID", "fake-consumer-group")
        self.validate()

    def validate(self) -> bool:
        """Validate the required configuration fields."""
        if not self.bootstrap_servers:
            raise ValueError(
                "Environment variable KAFKA_BOOTSTRAP_SERVERS is missing or empty."
            )
        if not self.group_id:
            raise ValueError("Environment variable KAFKA_GROUP_ID is missing or empty.")


def setup_kafka_consumer(config: KafkaConfig, topics: List[str]) -> KafkaConsumer:
    """Sets up the Kafka consumer with appropriate settings."""
    consumer = KafkaConsumer(
        *topics,
        group_id=config.group_id,
        bootstrap_servers=config.bootstrap_servers,
        value_deserializer=json_deserializer,
    )
    return consumer


def setup_kafka_producer(config: KafkaConfig) -> KafkaProducer:
    """Sets up the Kafka producer with appropriate settings."""
    producer = KafkaProducer(
        bootstrap_servers=config.bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8"),
    )
    return producer


def json_deserializer(message):
    """Deserialize JSON message."""
    try:
        return json.loads(message.decode("utf-8"))
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON message: {e}")
        return None


def on_message_print(msg):
    """Process and print the received Kafka message."""
    logging.info(f"Received message: {msg.topic} -> {msg.value.decode('utf-8')}")


def close_consumer(consumer: KafkaConsumer):
    """Method to close the kafka consumer connection."""
    logging.info("Closing Kafka consumer")
    consumer.close()
    sys.exit(0)


def close_producer(producer: KafkaProducer):
    """Method to close the kafka producer connection."""
    logging.info("Closing Kafka producer")
    producer.close()
    sys.exit(0)


def send_kafka_message(producer: KafkaProducer, topic: str, payload: str):
    """Method to send a message to a Kafka topic."""
    
    # use always this json layout in order to work with the mqtt-kafka-bridge 
    json_content = { "message": 
                        {   
                            "source": "kafka", 
                            "payload": payload 
                        }
                   }

    producer.send(topic, json.dumps(json_content))
    logging.info(f"Sent message to Kafka topic: {topic}")
