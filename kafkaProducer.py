import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=['18.191.214.157:9092'],  # Change to your Kafka broker IP
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# Read the CSV file into a pandas DataFrame
df = pd.read_csv("D:/kafka-v2/New folder/indexProcessed.csv")  # Update your CSV path here
print(df.head())  # Check the first few rows of the CSV

# Infinite loop to send data
try:
    while True:
        # Sample one random record from the CSV DataFrame
        dict_stock = df.sample(1).to_dict(orient="records")[0]
        
        # Send the sampled record to Kafka
        producer.send('demo_test', value=dict_stock)
        
        # Optional: print to monitor what's being sent
        print(f"Sending: {dict_stock}")
        
        # Sleep to control the message flow
        sleep(1)

except KeyboardInterrupt:
    print("Producer stopped manually.")

finally:
    # Flush any remaining data to Kafka before closing
    producer.flush()
    print("Producer flushed and closed.")
