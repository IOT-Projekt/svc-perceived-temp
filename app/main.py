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

# Kafka topics from environment variables
KAFKA_PRODUCER_TOPIC = os.getenv("KAFKA_PRODUCER_TOPIC", "perceived_temperature")
KAFKA_TEMP_TOPIC = os.getenv("KAFKA_TEMP_TOPIC", "temperatures")
KAFKA_HUMIDITY_TOPIC = os.getenv("KAFKA_HUMIDITY_TOPIC", "humidity")

def consume_temperature_messages(
    temperature_consumer: KafkaConsumer, 
    shared_data: dict, 
    lock: threading.Lock
) -> None:
    """Consume temperature messages from kafka and calculate the perceived temperature, if humidity data is available"""
    for temp_msg in temperature_consumer:        
        # Get temperature and timestamp from mesasge
        temp = temp_msg.value["temperature_c"]
        timestamp = temp_msg.value["timestamp"]

        # log received temperature        
        logging.info(f"Received temperature: {temp}") 

        
        # calculate perceived temperature and update shared data
        # Use lock to prevent both threads from updating/reading shared data at the same time  
        with lock:
            # update shared data temperature with received temperature
            shared_data["temperature"] = temp 
            
            # if there is humidity data, calculate perceived temperature
            if shared_data["humidity"] is not None:
                perceived_temp = calculate_perceived_temperature(shared_data["temperature"], shared_data["humidity"])
                
                # create payload with timestamp and perceived temperature. Afterwards send it to kafka
                create_payload_and_send(shared_data, timestamp, perceived_temp)
                shared_data["humidity"] = None # reset humidity data for next set of data


def consume_humidity_messages(
    humidity_consumer: KafkaConsumer,
    shared_data: dict,
    lock: threading.Lock,
) -> None:
    """Consume humidity messages from kafka and calculate the perceived temperature, if temperature data is available"""
    for humidity_msg in humidity_consumer:        
        # Get humidity and timestamp from message
        humidity = humidity_msg.value["humidity"]
        timestamp = humidity_msg.value["timestamp"]
        
        # log received humidity
        logging.info(f"Received humidity: {humidity}")
        
        # update shared data with humidity and calculate perceived temperature
        # Use lock to prevent both threads from updating/reading shared data at the same time
        with lock:
            # update shared data humidity with received humidity
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
    """Create payload with perceived temperature and timestamp and send it to kafka afterwards"""
    payload = {
        "timestamp": timestamp,
        "perceived_temperature": perceived_temp,
    }
    send_kafka_message(
        shared_data["producer"],
        KAFKA_PRODUCER_TOPIC,
        payload,
    )
    logging.info(f"Sent perceived temperature: {perceived_temp}")


def main():
    # Set up Kafka consumers and producer
    kafka_config = KafkaConfig()
    temp_consumer = setup_kafka_consumer(kafka_config, [KAFKA_TEMP_TOPIC])
    humidity_consumer = setup_kafka_consumer(kafka_config, [KAFKA_HUMIDITY_TOPIC])
    perceived_temp_producer = setup_kafka_producer(kafka_config)

    # Create a dict of shared data between threads
    shared_data = {
        "temperature": None,
        "humidity": None,
        "producer": perceived_temp_producer,
    }
    # Create a lock for thread safety
    lock = threading.Lock()

    # Create two threads for the two consume functions 
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
    # Run the main function in a loop to always calculate perceived temperature and send it to kafka
    while True:
        main()
