import json
import time

import requests
from confluent_kafka import Producer

API_URL = "http://127.0.0.1:8000/orders/live"
TOPIC = "api_orders"

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "acks": "all",
    "enable.idempotence": True,
})


def on_delivery(error, message):
    if error:
        print(f"Failed to send: {error}")
    else:
        print(f"Sent order {message.key().decode()} to partition {message.partition()}, offset {message.offset()}")


def main():
    print("Producer started. Press Ctrl+C to stop.")
    try:
        while True:
            try:
                orders = requests.get(API_URL, timeout=10).json()["data"]
            except requests.RequestException as error:
                print(f"API error: {error}")
                time.sleep(5)
                continue

            for order in orders:
                key = str(order["order_id"]).split(".")[0]
                producer.produce(TOPIC, key=key, value=json.dumps(order), callback=on_delivery)

            producer.poll(0)
            time.sleep(2)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()