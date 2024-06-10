import pika
import sys

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

def process_message(body):
    """Process message received from the queue."""
    # Parse message
    try:
        data = body.decode().split(', ')
        date = data[0].split(': ')[1]
        nutrients = {nutrient.split(': ')[0]: round(float(nutrient.split(': ')[1]), 1) for nutrient in data[1:]}
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
        return

    # Check nutrient levels
    if nutrients['Protein'] < 120:
        logger.warning(f"Alert! Total protein for {date} is below 120g: {nutrients['Protein']}g")
    if nutrients['Carbohydrates'] > 500:
        logger.warning(f"Alert! Total carbohydrates for {date} are above 500g: {nutrients['Carbohydrates']}g")
    if nutrients['Fat'] > 75:
        logger.warning(f"Alert! Total fat for {date} is above 75g: {nutrients['Fat']}g")


def callback(ch, method, properties, body):
    """Callback function to process received messages."""
    logger.info(f"Received message: {body}")
    process_message(body)

def main(host: str = "localhost", queue_name: str = "nutrition_data"):
    """Main function to consume messages from RabbitMQ."""
    try:
        # Connect to RabbitMQ
        connection, channel = connect_rabbitmq(host)
        create_and_declare_queue(channel, queue_name)

        # Set up consumer
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

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
