import asyncio
import json

from aiokafka import AIOKafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
GROUP_ID = "order-service"


async def consume():

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
    )

    await consumer.start()

    try:

        print("Consumer started...")
        print("Waiting for orders...\n")

        async for message in consumer:

            order = json.loads(
                message.value.decode("utf-8")
            )

            print("New order received:")
            print(order)

            print(
                f"Processing order {order['id']}..."
            )

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume())