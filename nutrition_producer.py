"""
    This program sends a message to a queue on the RabbitMQ server with aggregated nutritional data.
    Author: Bambee Garfield
    Date: June 10th, 2024
"""

import pika
import sys
import csv
from collections import defaultdict
import time
import os

# Configure Logging
from util_logger import setup_logger
logger, logname = setup_logger(__file__)

RABBITMQ_HOST = "localhost"

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
    """Delete existing queues and declare new ones."""
    for queue_name in queues:
        channel.queue_delete(queue=queue_name)
        channel.queue_declare(queue=queue_name, durable=True)
        logger.info(f"Queue '{queue_name}' declared.")

def aggregate_nutrition_data(file_path):
    """Aggregate nutrition data by date."""
    columns_to_aggregate = ["Protein (g)", "Fat (g)", "Carbohydrates (g)", "Sodium (mg)", "Fiber"]
    aggregated_data = defaultdict(lambda: {col: 0.0 for col in columns_to_aggregate})

    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            date = row['Date']
            for col in columns_to_aggregate:
                try:
                    aggregated_data[date][col] += float(row[col])
                except ValueError:
                    continue  # Skip rows with invalid numeric values

    return aggregated_data

def send_aggregated_data_to_queues(host, aggregated_data, queues):
    """Send aggregated data to RabbitMQ queues."""
    for date, totals in aggregated_data.items():
        for queue_name in queues:
            if queue_name in totals:
                message = f"Date: {date}, {queue_name}: {totals[queue_name]}"
                send_message(host, queue_name, message)
            else:
                logger.warning(f"Queue name '{queue_name}' not found in aggregated data for date {date}")

def send_message(host: str, queue_name: str, message: str):
    """Send a message to the RabbitMQ queue."""
    connection, channel = connect_rabbitmq(host)
    try:
        channel.basic_publish(
            exchange='', 
            routing_key=queue_name, 
            body=message.encode(), 
            properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
        )
        logger.info(f" [x] Sent '{message}' to queue '{queue_name}'.")
    except Exception as e:
        logger.error(f"Error sending message to queue '{queue_name}'.")
        logger.error(f"The error says: {e}")
    finally:
        connection.close()

if __name__ == "__main__":  
    file_name = 'Nutrition-Summary.csv'  # Adjust this to the correct relative path
    host = "localhost"
    queues = ["Protein (g)", "Fat (g)", "Carbohydrates (g)", "Sodium (mg)", "Fiber"]
    
    if not os.path.isfile(file_name):
        logger.error(f"File not found: {file_name}")
        sys.exit(1)
    
    connection, channel = connect_rabbitmq(host)
    create_and_declare_queues(channel, queues)
    connection.close()  # Close the initial connection after declaring queues
    
    aggregated_data = aggregate_nutrition_data(file_name)
    send_aggregated_data_to_queues(host, aggregated_data, queues)
