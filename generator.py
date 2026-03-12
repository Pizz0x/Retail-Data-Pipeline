import random, json, time, os
import argparse # used to configure arguments passed when executing the script
from confluent_kafka import Producer
from datetime import datetime


### CONFIGURATION OF THE ARGUMENTS
parser = argparse.ArgumentParser(description="Checkout Simulator")

# in the execution of the script we can specify the store, in this way we can run the script simultaneously multiple times, one for each store
parser.add_argument('--store', type=str, required=True, help='Store location (ex. Milan)')

# we also want to specify the checkout number, indeed we can have different checkout in a single store where each of them compute receipts independently
parser.add_argument('--checkout', type=str, required=True, help='Checkout number (ex. 3)')

args = parser.parse_args()
store_loc = args.store 
checkout_n = int(args.checkout)


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
catalogue = {
    'Jeans': {
        'Skinny': 49.99, 'Slim': 59.99, 'Straight': 69.99, 'Baggy': 79.99
    },
    'T-Shirt': {
        'Basic': 14.99, 'Graphic Print': 24.99, 'Oversize': 29.99, 'Polo': 34.99
    },
    'Sweater': {
        'Crewneck': 39.99, 'Hoodie': 49.99, 'Zip-Up': 54.99
    },
    'Jacker': {
        'Denim': 79.99, 'Bomber': 99.99, 'Puffer': 129.99, 'Leather': 199.99
    },
    'Shoes': {
        'Canvas': 59.99, 'Running': 89.99, 'Chunky': 119.99, 'High-Top': 139.99
    },
    'Sockets': {
        'Ankle (3-pack)': 9.99, 'Crew': 5.99, 'Sport': 12.99
    }
}
size = ['XS', 'S', 'M', 'L', 'XL', '2XL']
sex = ['F', 'M']


def generate_receipt():
    global current_receipt
    current_receipt += 1
    save_receipt(current_receipt)

    store_prefix = store_loc[:3].upper()
    receipt_id = f"{store_prefix}-{checkout_n:02d}-{current_receipt:06d}"

    n_items = int(random.triangular(1, 20, 1))
    items = []
    total_amount = 0.0

    for _ in range(n_items):
        category = random.choice(list(catalogue.keys()))
        model = random.choice(list(catalogue[category].keys()))
        price = catalogue[category][model]

        items.append({"category": category, "model": model, "price": price, "sex": random.choice(sex), "size": random.choice(size)})
        total_amount += price

    
    receipt = {
        "receipt_id": receipt_id,
        "store": store_loc,
        "checkout": checkout_n,
        "timestamp": datetime.now().isoformat(),
        "total_price": round(total_amount, 2),
        "items": items
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