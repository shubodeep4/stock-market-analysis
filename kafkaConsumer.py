from kafka import KafkaConsumer
from time import sleep
from json import dumps, loads
import json
import boto3
from s3fs import S3FileSystem

key = ""
secret = ""

#removed

# Kafka Consumer Setup
consumer = KafkaConsumer(
    'demo_test',  # Kafka topic to consume from
    bootstrap_servers=['18.191.214.157:9092'],  # Change to your Kafka broker IP
    value_deserializer=lambda x: loads(x.decode('utf-8'))  # Deserialize JSON messages
)

# S3 FileSystem Setup with AWS credentials
s3 = S3FileSystem(
    key=key,
    secret=secret
)

# Initialize a counter for sequential file naming
counter = 0

# Infinite loop to consume messages
try:
    for message in consumer:
        # Create a unique filename using a counter
        unique_filename = f"stock_market_{counter}.json"
        counter += 1  # Increment the counter for the next file
        
        # Upload message to S3
        with s3.open(f"s3://stock-market-kafka-suvadeep/{unique_filename}", 'w') as file:
            json.dump(message.value, file)
        
        # Monitor the uploaded file
        print(f"Uploaded message to S3: {unique_filename}")
        
        # Optional: Sleep to control consumption speed
        sleep(1)

except KeyboardInterrupt:
    print("Consumer stopped manually.")

finally:
    print("Consumer process completed.")
