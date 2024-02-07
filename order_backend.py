import json,time,sys,six
from faker import Faker


if sys.version_info >= (3, 12, 0): sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer,producer





KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 20



producer = KafkaProducer(bootstrap_servers='localhost:29092')
faker = Faker()





print("Producer started")
print("Generating orders")

for i in range(1,ORDER_LIMIT):
    data = {
        "order_id": faker.uuid4(),
        "user_id": faker.uuid4(),
        "user_email": faker.email(),
        "order_date": str(faker.date_time_this_month()),
        "order_total": faker.random_number(2),
        "order_items": [
            {
                "item_id": faker.uuid4(),
                "item_name": faker.word(),
                "item_price": faker.random_number(2),
                "item_quantity": faker.random_number(2),
            },
            {
                "item_id": faker.uuid4(),
                "item_name": faker.word(),
                "item_price": faker.random_number(2),
                "item_quantity": faker.random_number(2),
            },
            {
                "item_id": faker.uuid4(),
                "item_name": faker.word(),
                "item_price": faker.random_number(2),
                "item_quantity": faker.random_number(2),
            }
        ],

    }

    producer.send(
        KAFKA_TOPIC,
        json.dumps(data).encode('utf-8')
    )

    print(f"done sending order {i} of {ORDER_LIMIT}")
    time.sleep(10)





