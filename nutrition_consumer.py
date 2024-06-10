"""
    This program listens for nutritional data messages continuously and processes them.
    Author: Bambee Garfield
    Date: June 10th, 2024
"""

import pika
import sys
import time
from datetime import datetime
from collections import deque

# Configure Logging
from util_logger import setup_logger
logger, logname = setup_logger(__file__)

def connect_rabbitmq(host):
    """Connect to RabbitMQ and return connection and channel."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        channel = connection.channel()
        return connection, channel
    except Exception as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)

def create_and_declare_queues(channel, queues):
    """Declare queues."""
    for queue_name in queues:
        channel.queue_declare(queue=queue_name, durable=True)
        logger.info(f"Queue '{queue_name}' declared.")

# Initialize deques for processing nutritional data
nutrition_data = {
    "Protein (g)": deque(maxlen=1),
    "Fat (g)": deque(maxlen=1),
    "Carbohydrates (g)": deque(maxlen=1)
}

def process_nutrition_data(nutrient, value):
    """Process nutritional data and log alerts."""
    nutrition_data[nutrient].append(value)
    total_value = sum(nutrition_data[nutrient])

    if nutrient == "Protein (g)" and total_value < 120:
        logger.warning(f"Alert! Total protein for the day is below 120g: {total_value}g")
    elif nutrient == "Fat (g)" and total_value > 75:
        logger.warning(f"Alert! Total fat for the day is above 75g: {total_value}g")
    elif nutrient == "Carbohydrates (g)" and total_value > 450:
        logger.warning(f"Alert! Total carbohydrates for the day are above 450g: {total_value}g")


def process_message(queue_name, body):
    """Process message received from the queue."""
    data = body.decode().split(',')
    date = data[0].split(': ')[1]
    value = float(data[1].split(': ')[1])
    value = round(value, 2)  # Round to two decimal points
    logger.info(f"Processing {queue_name} message: {date}, {value}")
    
    if queue_name in nutrition_data:
        process_nutrition_data(queue_name, value)
    else:
        logger.warning(f"Received message from unknown queue: {queue_name}")


def callback(ch, method, properties, body):
    """Callback function to process received messages."""
    queue_name = method.routing_key
    logger.info(f"Received message from queue: {queue_name}")
    process_message(queue_name, body)

def main(host: str = "localhost", queues: list = ["Protein (g)", "Fat (g)", "Carbohydrates (g)"]):
    """Main function to consume messages from RabbitMQ."""
    try:
        connection, channel = connect_rabbitmq(host)
        create_and_declare_queues(channel, queues)

        for queue_name in queues:
            channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

        logger.info(" [*] Ready for work. To exit press CTRL+C")
        channel.start_consuming()

    except Exception as e:
        logger.error("ERROR: Something went wrong.")
        logger.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.error(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.error("\nClosing connection. Goodbye.\n")
        connection.close()

if __name__ == "__main__":
    main()
