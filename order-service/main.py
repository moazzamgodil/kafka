import asyncio
import json

from aiokafka import (
    AIOKafkaConsumer,
    AIOKafkaProducer
)


async def main():

    topic = "orders"
    topic_notifications = "notifications"

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        group_id="order-service",
        auto_offset_reset="earliest"
    )

    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092"
    )

    await consumer.start()
    await producer.start()

    print("Order Service started...")

    try:

        async for message in consumer:

            order = json.loads(
                message.value.decode()
            )

            print("\nORDER SERVICE")
            print("Received:")
            print(order)

            # Create notification message

            notification = {
                "type": "SEND_EMAIL",
                "to": "moazzam@example.com",
                "subject": "Order confirmed",
                "order_id": order["order_id"]
            }

            # Send to notifications topic

            await producer.send_and_wait(
                topic_notifications,
                json.dumps(notification).encode()
            )

            print("Notification event published!")

    finally:

        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())