import json, os
from confluent_kafka import Consumer
import psycopg2

kafka_server = "localhost:9092"
topic = "receipts_flow"

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "inventory_db")
DB_USER = os.environ.get("DB_USER", "admin")
DB_PASS = os.environ.get("DB_PASS", "password")
DB_PORT = os.environ.get("DB_PORT", "5432")

# Consumer Connection
consumer = Consumer({
    "bootstrap.servers": kafka_server,
    "auto.offset.reset": "latest"
})
consumer.subscribe([topic])


def main():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()
        print("Connected to the database successfully.")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer Error: {msg.error()}")
                continue
            
            try:
                receipt_data = json.loads(msg.value().decode('utf-8'))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue
            
            print(receipt_data)
    except KeyboardInterrupt:
        pass
    finally: 
        consumer.close()
        cursor.close()
        conn.close()
        print("Consumer closed and database connection closed.")

if __name__ == "__main__":
    main()
