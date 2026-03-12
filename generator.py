import random, json, time, os
import argparse # used to configure arguments passed when executing the script
from confluent_kafka import Producer


### CONFIGURATION OF THE ARGUMENTS
parser = argparse.ArgumentParser(description="Checkout Simulator")

# in the execution of the script we can specify the store, in this way we can run the script simultaneously multiple times, one for each store
parser.add_argument('--store', type=str, required=True, help='Store location (ex. Milan)')

# we also want to specify the checkout number, indeed we can have different checkout in a single store where each of them compute receipts independently
parser.add_argument('--checkout', type=str, required=True, help='Checkout number (ex. 01)')

args = parser.parse_args()
store_loc = args.store 
checkout_n = args.checkout


### STATE MANAGER FOR THE CHECKOUTS
state_file = f"./data/{store_loc}_{checkout_n}.txt"

def get_last_receipt():
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            return int(f.read().strip())
    return 0

def save_receipt(n):
    with open(state_file, 'w') as f:
        f.write(str(n))

current_receipt = get_last_receipt()


### KAFKA CONFIGURATION

conf = {'bootstrap.servers': 'localhost:9092', # we just need get the address of the bootstrap server among the cluster of the Kafka servers
        'client.id': f'{store_loc}_{checkout_n}'
        }
producer = Producer(conf)
topic = "bank_transactions"

# possible configurations of each article

def generate_receipt():
    receipt = {
        "receipt_id": fake.uuid4(),
        "card_id": f"CARD_{random.randint(1000, 1050)}",
        "amount": round(random.uniform(0.0,500.0),2),
    }
    return receipt

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
            topic = topic, # to which kafka pipeline send the data
            value = trans_json.encode('utf-8'), # encode data that has to be sent
            callback = delivery_check # tell us what happened when data are delivered (since kafka is asyncronous)
        )
        producer.poll(0) # check if the buffer still has data that we are sure are delivered correctly and remove them

        time.sleep(random.uniform(0.2, 0.5))
except KeyboardInterrupt:
    print("The generator has stopped")
finally:
    producer.flush() # ensure that the program doesn't stop until all data in buffer are delivered