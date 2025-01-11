import json
from perceived_temp import calculate_perceived_temperature
import kafka_handler
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def consume_messages():
    # Set up Kafka
    kafka_config = kafka_handler.KafkaConfig()
    temp_consumer = kafka_handler.setup_kafka_consumer(kafka_config, ['temperatures'])
    humidity_consumer = kafka_handler.setup_kafka_consumer(kafka_config, ['humidity'])
    perceived_temp_producer = kafka_handler.setup_kafka_producer(kafka_config)

    temp = None
    humidity = None

    for temp_msg in temp_consumer:
        temp = json.loads(temp_msg.value.decode('utf-8'))['temperature_c']
        logging.info(f"Received temperature: {temp}")
        if humidity is not None:
            perceived_temp = calculate_perceived_temperature(temp, humidity)
            perceived_temp_producer.send('perceived_temperature', json.dumps({'perceived_temp': perceived_temp}).encode('utf-8'))
            logging.info(f"Sent perceived temperature: {perceived_temp}")
            humidity = None

    for humidity_msg in humidity_consumer:
        humidity = json.loads(humidity_msg.value.decode('utf-8'))['humidity']
        logging.info(f"Received humidity: {humidity}")
        if temp is not None:
            perceived_temp = calculate_perceived_temperature(temp, humidity)
            perceived_temp_producer.send('perceived_temperature', json.dumps({'perceived_temp': perceived_temp}).encode('utf-8'))
            logging.info(f"Sent perceived temperature: {perceived_temp}")
            temp = None
    
if __name__ == "__main__":
    while True:
        consume_messages()