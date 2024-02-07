import json,sys,six

if sys.version_info >= (3, 12, 0): sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaConsumer



consumer = KafkaConsumer('order_confirmed',bootstrap_servers='localhost:29092')


print("Listening for confirmed orders")
while True:
    for message in consumer:
        order = json.loads(message.value.decode('utf-8'))
        print(f"order email has been sent to {order['user_email']}")