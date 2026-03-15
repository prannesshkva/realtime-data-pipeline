import asyncio
import json
import aiohttp
from aiokafka import AIOKafkaProducer

async def produce():

    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

    await producer.start()

    async with aiohttp.ClientSession() as session:

        try:
            while True:

                async with session.get("https://api.github.com/events") as response:

                    events = await response.json()

                    if isinstance(events, list):

                        for event in events:

                            event_type = event.get("type", "other")
                            topic = f"gh_{event_type.lower().replace('event','')}"

                            payload = {
                                "id": event.get("id"),
                                "repo": event.get("repo", {}).get("name"),
                                "user": event.get("actor", {}).get("login"),
                                "action": event_type,
                                "ts": event.get("created_at")
                            }

                            await producer.send_and_wait(topic, payload)

                        print(f"Sent {len(events)} events")

                await asyncio.sleep(60)

        finally:
            await producer.stop()

asyncio.run(produce())