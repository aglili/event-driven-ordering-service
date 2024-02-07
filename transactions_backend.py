import json,sys,six




if sys.version_info >= (3, 12, 0): sys.modules['kafka.vendor.six.moves'] = six.moves


from kafka import KafkaProducer,KafkaConsumer


KAFKA_TOPIC_READ = "order_details"
KAFKA_TOPIC_WRITE = "order_confirmed"



producer = KafkaProducer(bootstrap_servers='localhost:29092')
consumer = KafkaConsumer(KAFKA_TOPIC_READ,bootstrap_servers='localhost:29092')



print("Listening for orders")


while True:
    for message in consumer:
        print("working on orders...../")
        consumed_message = json.loads(message.value.decode('utf-8'))
        print(consumed_message)

        producer.send(
            KAFKA_TOPIC_WRITE,
            json.dumps(consumed_message).encode('utf-8')
        )
        print("Order confirmed")





    
        