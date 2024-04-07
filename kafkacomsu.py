from kafka import KafkaConsumer
import json

# Initialize consumer
consumer = KafkaConsumer(
    'ecommerce-transactions',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Process messages
for message in consumer:
    transaction = message.value
    print(f"Processing transaction: {transaction}")
    # Process and analyze data here
