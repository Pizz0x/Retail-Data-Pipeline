import random
import json
import time

from confluent_kafka import Producer
from faker import Faker

conf = {'bootstrap.servers': 'localhost:9092'} # we just need get the address of the bootstrap server among the cluster of the Kafka servers
producer = Producer(conf)
fake = Faker('')

def generate_transaction():
    transaction = {
        "transaction_id": fake.uuid4(),
        "card_id": f"CARD_{random.randint(1000, 1050)}",
        "amount": round(random.uniform(0.0,500.0),2),
        "location": fake.city()
    }
    return transaction

def delivery_check(err, msg):
    if err is not None:
        print(f"Error in the transaction deliver: {err}")
    else:
        print(f"correclty deliver to Kafka : {msg.value().decode('utf-8')}")

try:
    while True:
        trans_data = generate_transaction()
        trans_json = json.dumps(trans_data)

        producer.produce(
            topic = "bank_transactions", # to which kafka pipeline send the data
            value = trans_json.encode('utf-8'), # encode data that has to be sent
            callback = delivery_check # tell us what happened when data are delivered (since kafka is asyncronous)
        )
        producer.poll(0) # check if the buffer still has data that we are sure are delivered correctly and remove them

        time.sleep(random.uniform(0.2, 0.5))
except KeyboardInterrupt:
    print("The generator has stopped")
finally:
    producer.flush() # ensure that the program doesn't stop until all data in buffer are delivered