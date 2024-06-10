"""44671 Streaming Data Final Project
    Bambee Garfield
    June 13th, 2024 """

import pika
import csv
import time
from collections import defaultdict

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
        raise

def create_and_declare_queue(channel, queue_name):
    """Declare a queue."""
    channel.queue_declare(queue=queue_name, durable=True)
    logger.info(f"Queue '{queue_name}' declared.")

def send_message(channel, queue_name, message):
    """Send a message to the RabbitMQ queue."""
    try:
        channel.basic_publish(
            exchange='', 
            routing_key=queue_name, 
            body=message,
            properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
        )
        logger.info(f" [x] Sent '{message}' to queue '{queue_name}'.")
    except Exception as e:
        logger.error(f"Error sending message to queue '{queue_name}': {e}")
        raise

def aggregate_nutrition_data(file_path):
    """Aggregate nutrition data by date."""
    aggregated_data = defaultdict(lambda: defaultdict(float))

    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            date = row['Date']
            aggregated_data[date]['Protein'] += float(row['Protein (g)'])
            aggregated_data[date]['Carbohydrates'] += float(row['Carbohydrates (g)'])
            aggregated_data[date]['Fat'] += float(row['Fat (g)'])
            aggregated_data[date]['Sodium'] += float(row['Sodium (mg)'])
            # Adjust for the column name discrepancy
            aggregated_data[date]['Fiber'] += float(row['Fiber'])
            # Consider other nutrients if needed

    return aggregated_data

def main(host: str = "localhost", input_file: str = "nutrition-summary.csv", queue_name: str = "nutrition_data"):
    """Main function to read nutrition data from file and send to RabbitMQ."""
    try:
        # Connect to RabbitMQ
        connection, channel = connect_rabbitmq(host)
        create_and_declare_queue(channel, queue_name)

        # Aggregate nutrition data
        aggregated_data = aggregate_nutrition_data(input_file)

        # Send messages to RabbitMQ
        for date, data in aggregated_data.items():
            # Truncate numbers to one decimal point
            protein = round(data['Protein'], 1)
            carbohydrates = round(data['Carbohydrates'], 1)
            fat = round(data['Fat'], 1)
            sodium = round(data['Sodium'], 1)
            fiber = round(data['Fiber'], 1)

            # Construct message
            message = f"Date: {date}, Protein: {protein}, Carbohydrates: {carbohydrates}, Fat: {fat}, Sodium: {sodium}, Fiber: {fiber}"
            send_message(channel, queue_name, message)

            # Introduce a delay of 3 seconds
            time.sleep(.5)

        # Close connection
        connection.close()
    except Exception as e:
        logger.error("Error in the producer:")
        logger.error(str(e))

if __name__ == "__main__":
    main()
