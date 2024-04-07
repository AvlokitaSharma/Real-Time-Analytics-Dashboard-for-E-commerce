from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
session = cluster.connect('ecommerce')

for message in consumer:
    order_id = message.value['order_id']
    amount = message.value['amount']
    session.execute(
        "INSERT INTO transactions (order_id, amount) VALUES (%s, %s)",
        (order_id, amount)
    )
