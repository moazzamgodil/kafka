import json

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"

producer: AIOKafkaProducer | None = None


@app.on_event("startup")
async def startup():
    global producer

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
    )

    await producer.start()


@app.on_event("shutdown")
async def shutdown():
    if producer:
        await producer.stop()


@app.post("/orders")
async def create_order(order: dict):

    message = json.dumps(order).encode("utf-8")

    await producer.send_and_wait(
        TOPIC,
        message
    )

    return {
        "message": "Order published",
        "order": order
    }