import asyncio
import json

from aiokafka import AIOKafkaConsumer


async def main():

    topic = "notifications"

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        group_id="notification-service",
        auto_offset_reset="earliest"
    )

    await consumer.start()

    print("Notification Service started...")

    try:

        async for message in consumer:

            order = json.loads(
                message.value.decode()
            )

            print("\nNOTIFICATION SERVICE")
            print("Received order:")
            print(order)

            print(
                f"Sending email for order "
                f"{order['order_id']}"
            )

    finally:

        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())