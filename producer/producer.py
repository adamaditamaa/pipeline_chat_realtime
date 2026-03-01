from fastapi import FastAPI
from kafka import KafkaProducer
import time
import json
import os

app = FastAPI()

producer = None

def get_producer():
    global producer
    if producer is None:
        while True:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                print("Connected to Kafka")
                break
            except Exception as e:
                print("Kafka not ready, retrying in 5 seconds")
                time.sleep(5)
    return producer


@app.post("/send")
def send_message(message: dict):
    kafka_producer = get_producer()
    kafka_producer.send("chat_topic", message)
    kafka_producer.flush()
    return {"status": "sent"}