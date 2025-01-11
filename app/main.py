import json
import threading
from perceived_temp import calculate_perceived_temperature
from kafka_handler import KafkaConsumer, KafkaProducer, KafkaConfig, send_kafka_message, setup_kafka_consumer, setup_kafka_producer
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "perceived_temperature")

def consume_temperature_messages(
    temp_consumer: KafkaConsumer, shared_data: dict, lock: threading.Lock
):
    for temp_msg in temp_consumer:
        temp = json.loads(temp_msg.value.get("message"))["temperature_c"]
        logging.info(f"Received temperature: {temp}")
        with lock:
            shared_data["temperature"] = temp
            if shared_data["humidity"] is not None:
                perceived_temp = calculate_perceived_temperature(
                    temp, shared_data["humidity"]
                )
                send_kafka_message(
                    shared_data["producer"],
                    KAFKA_TOPIC,
                    perceived_temp,
                )
                logging.info(f"Sent perceived temperature: {perceived_temp}")
                shared_data["humidity"] = None


def consume_humidity_messages(
    humidity_consumer: KafkaConsumer,
    shared_data: dict,
    lock: threading.Lock,
):
    for humidity_msg in humidity_consumer:
        humidity = json.loads(humidity_msg.value.get("message"))["humidity"]
        logging.info(f"Received humidity: {humidity}")
        with lock:
            shared_data["humidity"] = humidity
            if shared_data["temperature"] is not None:
                perceived_temp = calculate_perceived_temperature(
                    shared_data["temperature"], humidity
                )
                send_kafka_message(
                    shared_data["producer"],
                    KAFKA_TOPIC,
                    perceived_temp,
                )
                logging.info(f"Sent perceived temperature: {perceived_temp}")
                shared_data["temperature"] = None


def main():
    # Set up Kafka
    kafka_config = KafkaConfig()
    temp_consumer = setup_kafka_consumer(kafka_config, ["temperatures"])
    humidity_consumer = setup_kafka_consumer(kafka_config, ["humidity"])
    perceived_temp_producer = setup_kafka_producer(kafka_config)

    shared_data = {
        "temperature": None,
        "humidity": None,
        "producer": perceived_temp_producer,
    }
    lock = threading.Lock()

    temp_thread = threading.Thread(
        target=consume_temperature_messages, args=(temp_consumer, shared_data, lock)
    )
    humidity_thread = threading.Thread(
        target=consume_humidity_messages, args=(humidity_consumer, shared_data, lock)
    )

    temp_thread.start()
    humidity_thread.start()

    temp_thread.join()
    humidity_thread.join()


if __name__ == "__main__":
    while True:
        main()
