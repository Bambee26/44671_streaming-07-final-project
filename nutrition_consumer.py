import pika
import sys
import csv
from collections import deque

# Configure Logging
from util_logger import setup_logger
logger, logname = setup_logger(__file__)

# Initialize deques for processing nutritional data
protein_data = deque(maxlen=1)
carbohydrates_data = deque(maxlen=1)
fat_data = deque(maxlen=1)
sodium_data = deque(maxlen=1)
fiber_data = deque(maxlen=1)

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

def process_message(channel, method, properties, body, output_writer):
    """Process message received from the queue."""
    # Parse message
    try:
        data = body.decode().split(', ')
        date = data[0].split(': ')[1]
        nutrients = {nutrient.split(': ')[0]: float(nutrient.split(': ')[1]) for nutrient in data[1:]}
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
        return

    # Update deques
    protein_data.append(nutrients['Protein'])
    carbohydrates_data.append(nutrients['Carbohydrates'])
    fat_data.append(nutrients['Fat'])
    sodium_data.append(nutrients['Sodium'])
    fiber_data.append(nutrients['Fiber'])

    # Calculate total calories
    total_calories = nutrients['Protein'] * 4 + nutrients['Carbohydrates'] * 4 + nutrients['Fat'] * 9

    # Check nutrient levels
    if nutrients['Protein'] < 120:
        logger.warning(f"Alert! Total protein for {date} is below 120g: {nutrients['Protein']}g")
    if nutrients['Carbohydrates'] > 500:
        logger.warning(f"Alert! Total carbohydrates for {date} are above 500g: {nutrients['Carbohydrates']}g")
    if nutrients['Fat'] > 75:
        logger.warning(f"Alert! Total fat for {date} is above 75g: {nutrients['Fat']}g")
    if total_calories > 3000:
        logger.warning(f"Alert! Total calories for {date} are over 3000: {total_calories}")

    # Write to output file with rounded values
    output_writer.writerow([date, '', round(protein_data[-1], 1), round(carbohydrates_data[-1], 1), round(fat_data[-1], 1), round(total_calories, 1), '', '', round(sodium_data[-1], 1), round(fiber_data[-1], 1)])

def callback(channel, method, properties, body, output_writer):
    """Callback function to process received messages."""
    logger.info(f"Received message: {body}")
    process_message(channel, method, properties, body, output_writer)

def main(host: str = "localhost", queue_name: str = "nutrition_data"):
    """Main function to consume messages from RabbitMQ and generate output file."""
    try:
        # Connect to RabbitMQ
        connection, channel = connect_rabbitmq(host)
        create_and_declare_queue(channel, queue_name)

        # Open output file
        with open("output.csv", 'w', newline='') as csvfile:
            output_writer = csv.writer(csvfile)

            # Write header
            output_writer.writerow(['Date', 'Weight', 'Protein', 'Carbohydrates', 'Fats', 'Total Calories', 'Water', 'Caffeine', 'Sodium', 'Fiber'])

            # Set up consumer
            channel.basic_consume(queue=queue_name, on_message_callback=lambda ch, method, properties, body: callback(ch, method, properties, body, output_writer), auto_ack=True)

            # Start consuming messages
            logger.info(" [*] Waiting for messages. To exit press CTRL+C")
            channel.start_consuming()
    except Exception as e:
        logger.error("Error in the consumer:")
        logger.error(str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("User interrupted. Closing connection.")
        connection.close()

if __name__ == "__main__":
    main()
