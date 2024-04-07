from flask import Flask, jsonify
from cassandra.cluster import Cluster

app = Flask(__name__)
cluster = Cluster(['localhost'])
session = cluster.connect('ecommerce')

@app.route('/data')
def get_data():
    rows = session.execute("SELECT * FROM transactions")
    data = [{"order_id": row.order_id, "amount": row.amount} for row in rows]
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
