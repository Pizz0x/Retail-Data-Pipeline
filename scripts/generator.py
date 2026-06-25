import argparse
import json
import os
import random
import time
from datetime import datetime
from pathlib import Path
# type specifications
from typing import Any, Dict, List, TypedDict
# libraries for Kafka
from confluent_kafka import KafkaError, Message, Producer


class ItemDict(TypedDict):
    category: str
    model: str
    price: float
    sex: str
    size: str
    quantity: int


class ReceiptDict(TypedDict):
    receipt_id: str
    store: str
    checkout: str
    timestamp: str
    total_amount: float
    payment: str
    test: bool
    items: List[ItemDict]


CatalogueType = Dict[str, Dict[str, float]]
SIZE: List[str] = ['XS', 'S', 'M', 'L', 'XL', '2XL']
SEX: List[str] = ['F', 'M']
PAYMENTS: List[str] = ['card', 'cash', 'gift card']

catalogue: CatalogueType = {
    'Jeans': {
        'Skinny': 49.99, 'Slim': 59.99, 'Straight': 69.99, 'Baggy': 79.99
    },
    'T-Shirt': {
        'Basic': 14.99, 'Graphic Print': 24.99, 'Oversize': 29.99, 'Polo': 34.99
    },
    'Sweater': {
        'Crewneck': 39.99, 'Hoodie': 49.99, 'Zip-Up': 54.99
    },
    'Jacket': {
        'Denim': 79.99, 'Bomber': 99.99, 'Puffer': 129.99, 'Leather': 199.99
    },
    'Shoes': {
        'Canvas': 59.99, 'Running': 89.99, 'Chunky': 119.99, 'High-Top': 139.99
    },
    'Socks': {
        'Ankle (3-pack)': 9.99, 'Crew': 5.99, 'Sport': 12.99
    }
}

TOPIC = 'receipts_flow'
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9094')


def parse_args() -> tuple[str, int]:
    parser = argparse.ArgumentParser(description='Checkout Simulator')
    # in the execution of the script we can specify the store, in this way we can run the script simultaneously multiple times, one for each store
    parser.add_argument('--store', type=str, required=True, help='Store location (ex. Milan)')
    # we also want to specify the checkout number, indeed we can have different checkout in a single store where each of them compute receipts independently
    parser.add_argument('--checkout', type=str, required=True, help='Checkout number (ex. 3)')

    args = parser.parse_args()
    return args.store, int(args.checkout)


def state_file_path(store_loc: str, checkout_n: int) -> Path:
    return Path('data') / f'{store_loc}_{checkout_n}.txt'


def get_last_receipt(state_path: Path) -> int:
    if state_path.exists():
        with state_path.open('r') as handle:
            return int(handle.read().strip())
    return 0


def save_receipt(state_path: Path, n: int) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with state_path.open('w') as handle:
        handle.write(str(n))


def create_producer(client_id: str) -> Producer:
    return Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': client_id,
    })


def generate_receipt(store_loc: str, checkout_n: int, current_receipt: int, is_test: bool) -> tuple[ReceiptDict, int]:
    current_receipt += 1
    save_receipt(state_file_path(store_loc, checkout_n), current_receipt)

    store_prefix = store_loc[:3].upper()
    receipt_id = f'{store_prefix}-{checkout_n:02d}-{current_receipt:06d}'

    items: List[ItemDict] = []
    total_price = 0.0
    n_items = int(1 + (random.random() ** 2 * 19))

    for _ in range(n_items):
        category = random.choice(list(catalogue.keys()))
        model = random.choice(list(catalogue[category].keys()))
        chance = random.random() * 100
        discount = 0.0

        if chance < 5:
            discount = 0.40
        elif chance < 10:
            discount = 0.30
        elif chance < 20:
            discount = 0.20

        if random.random() > 0.95:
            multiplier = -1
        else:
            multiplier = 1

        price = round((catalogue[category][model] * multiplier) * (1 - discount), 2)
        quantity = int(1 + (random.random() ** 2 * 5))

        item: ItemDict = {
            'category': category,
            'model': model,
            'price': price,
            'sex': random.choice(SEX),
            'size': random.choice(SIZE),
            'quantity': quantity,
        }
        items.append(item)
        total_price += price * quantity

    receipt: ReceiptDict = {
        'receipt_id': receipt_id,
        'store': store_loc,
        'checkout': str(checkout_n),
        'timestamp': datetime.now().isoformat(),
        'total_amount': round(total_price, 2),
        'payment': random.choice(PAYMENTS),
        'test': is_test,
        'items': items,
    }

    return receipt, current_receipt


def delivery_check(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        print(f'Error in the receipt delivery: {err}')
    else:
        raw = msg.value() if msg is not None else None
        value = raw.decode('utf-8') if raw is not None else '<no message>'
        print(f'Receipt correctly delivered to Kafka: {value}')


def main() -> None:
    store_loc, checkout_n = parse_args()
    state_path = state_file_path(store_loc, checkout_n)
    current_receipt = get_last_receipt(state_path)
    producer = create_producer(f'{store_loc}_{checkout_n}')

    print(f'Store: {store_loc} | Checkout: {checkout_n} | Last receipt: {current_receipt}')

    is_test = True

    try:
        while True:
            receipt, current_receipt = generate_receipt(store_loc, checkout_n, current_receipt, is_test)
            is_test = False
            json_rec = json.dumps(receipt)
            message_key = f'{store_loc}_{checkout_n}'

            producer.produce(
                topic=TOPIC,
                key=message_key.encode('utf-8'),
                value=json_rec.encode('utf-8'),
                callback=delivery_check,
            )
            producer.poll(0)
            time.sleep(random.uniform(1.0, 2.0))
    except KeyboardInterrupt:
        print('The Checkout has closed')
    finally:
        producer.flush()


if __name__ == '__main__':
    main()
