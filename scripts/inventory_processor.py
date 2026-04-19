import json, os
from confluent_kafka import Consumer
import psycopg2
from dotenv import load_dotenv, find_dotenv


kafka_server = "localhost:9092"
topic = "receipts_flow"

# search for the .env file and load the variables in the script
load_dotenv(find_dotenv())

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "inventory_db")
DB_USER = os.environ.get("DB_USER", "admin")
DB_PASS = os.environ.get("DB_PASSWORD", "password")
DB_PORT = os.environ.get("DB_PORT", "5432")

# Consumer Connection
consumer = Consumer({
    "bootstrap.servers": kafka_server,
    'group.id': "inventory_processor",
    "auto.offset.reset": "earliest"
})
consumer.subscribe([topic])


def main():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
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
            store = receipt_data.get("store")
            items = receipt_data.get("items", [])
            for item in items:
                category = item.get("category")
                model = item.get("model")
                quantity = item.get("quantity", 1)
                update = """
                    UPDATE inventory
                    SET quantity = quantity - %s
                    WHERE store = %s and category = %s and model = %s
                    RETURNING quantity;
                """
                try:
                    cursor.execute(update, (quantity, store, category, model))
                    result = cursor.fetchone()
                    
                except Exception as e:
                    print(f"Error in the database update: {e}")
                    conn.rollback()
                    continue
            conn.commit()

    except KeyboardInterrupt:
        pass
    finally: 
        consumer.close()
        cursor.close()
        conn.close()
        print("Consumer closed and database connection closed.")

if __name__ == "__main__":
    main()
