from cassandra.cluster import Cluster

cluster = Cluster(['cassandra-node1', 'cassandra-node2'])
session = cluster.connect('ecommerce')

# Assuming a table schema for transactions (id UUID PRIMARY KEY, amount decimal, ...)
query = "INSERT INTO transactions (id, amount, ...) VALUES (%s, %s, ...)"
session.execute(query, (uuid.uuid1(), 250.00, ...))


from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i in range(100):
    message = {'order_id': i, 'amount': 100 + i}
    producer.send('ecommerce-data', value=message)
    time.sleep(1)

producer.flush()
