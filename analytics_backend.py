import json,sys,six

if sys.version_info >= (3, 12, 0): sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaConsumer


consumer = KafkaConsumer('order_confirmed',bootstrap_servers='localhost:29092')


print("Listening for confirmed orders")

while True:
    print("waiting for orders")
    for confirmed_orders in consumer:
        order = json.loads(confirmed_orders.value.decode('utf-8'))
        print(f"Order with is {order['order_id']} has been received for analysis.")
        