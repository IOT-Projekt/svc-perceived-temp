import json
import threading
from perceived_temp import calculate_perceived_temperature
from kafka_handler import (
    KafkaConsumer,
    KafkaConfig,
    send_kafka_message,
    setup_kafka_consumer,
    setup_kafka_producer,
)
import logging
import os

# Set up basic logging
logging.basicConfig(level=logging.INFO)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "perceived_temperature")


def consume_temperature_messages(
    temperature_consumer: KafkaConsumer, 
    shared_data: dict, 
    lock: threading.Lock
) -> None:
    for temp_msg in temperature_consumer:
        # Get temperature and timestamp from mesasge
        temp = json.loads(temp_msg.value.get("message"))["temperature_c"]
        timestamp = json.loads(temp_msg.value.get("message"))["timestamp"]
        logging.info(f"Received temperature: {temp}")
        
        # calculate perceived temperature and update shared data
        with lock:
            shared_data["temperature"] = temp
            # if there is humidity data, calculate perceived temperature
            if shared_data["humidity"] is not None:
                perceived_temp = calculate_perceived_temperature(temp, shared_data["humidity"])
                
                # create payload with timestamp and perceived temperature and send it to kafka
                create_payload_and_send(shared_data, timestamp, perceived_temp)
                shared_data["humidity"] = None # reset humidity data for next set of data


def consume_humidity_messages(
    humidity_consumer: KafkaConsumer,
    shared_data: dict,
    lock: threading.Lock,
) -> None:
    for humidity_msg in humidity_consumer:
        # Get humidity and timestamp from message
        humidity = json.loads(humidity_msg.value.get("message"))["humidity"]
        timestamp = json.loads(humidity_msg.value.get("message"))["timestamp"]
        logging.info(f"Received humidity: {humidity}")
        
        # update shared data with humidity and calculate perceived temperature
        with lock:
            shared_data["humidity"] = humidity
            # if there is temperature data, calculate perceived temperature
            if shared_data["temperature"] is not None:
                perceived_temp = calculate_perceived_temperature( shared_data["temperature"], humidity)
                
                # create payload with timestamp and perceived temperature and send it to kafka
                create_payload_and_send(shared_data, timestamp, perceived_temp) 
                shared_data["temperature"] = None # reset temperature data for next set of data


def create_payload_and_send(
    shared_data: dict, timestamp: float, perceived_temp: float
) -> None:
    payload = {
        "timestamp": timestamp,
        "perceived_temperature": perceived_temp,
    }
    send_kafka_message(
        shared_data["producer"],
        KAFKA_TOPIC,
        payload,
    )
    logging.info(f"Sent perceived temperature: {perceived_temp}")


def main():
    # Set up Kafka
    kafka_config = KafkaConfig()
    temp_consumer = setup_kafka_consumer(kafka_config, ["temperatures"])
    humidity_consumer = setup_kafka_consumer(kafka_config, ["humidity"])
    perceived_temp_producer = setup_kafka_producer(kafka_config)

    # Shared data between threads
    shared_data = {
        "temperature": None,
        "humidity": None,
        "producer": perceived_temp_producer,
    }
    lock = threading.Lock()

    # Create the threads
    temp_thread = threading.Thread(
        target=consume_temperature_messages, args=(temp_consumer, shared_data, lock)
    )
    humidity_thread = threading.Thread(
        target=consume_humidity_messages, args=(humidity_consumer, shared_data, lock)
    )

    # Start the threads and wait for them to finish
    temp_thread.start()
    humidity_thread.start()
    temp_thread.join()
    humidity_thread.join()


if __name__ == "__main__":
    while True:
        main()
