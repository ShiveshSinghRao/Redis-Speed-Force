"""
Chapter 3: Pub/Sub — Fire-and-Forget Messaging.

Pub/Sub is NOT stored. If no subscriber is listening when a message
is published, it's lost forever. For reliable queuing use Streams.

This file contains:
  1. Synchronous pub/sub (threaded subscriber + publisher)
  2. Async pub/sub using redis.asyncio
"""
import redis
import threading
import time
import asyncio
import redis.asyncio as aioredis


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Part 1: Synchronous pub/sub
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def sync_subscriber():
    r = redis.Redis()
    pubsub = r.pubsub()
    pubsub.subscribe("dubbing:events")
    print("[Subscriber] Listening for events...")

    for message in pubsub.listen():
        if message["type"] == "message":
            channel = message["channel"].decode()
            data = message["data"].decode()
            print(f"[Subscriber] [{channel}] {data}")
            if data == "stop":
                break

    pubsub.unsubscribe()
    pubsub.close()


def sync_publisher():
    r = redis.Redis()
    time.sleep(0.5)  # let subscriber connect first

    for event in ["job_123:started", "job_123:stt_complete", "job_123:completed"]:
        r.publish("dubbing:events", event)
        print(f"[Publisher] Sent: {event}")
        time.sleep(0.3)

    r.publish("dubbing:events", "stop")


def run_sync_demo():
    print("=== Sync Pub/Sub Demo ===")
    sub_t = threading.Thread(target=sync_subscriber, daemon=True)
    pub_t = threading.Thread(target=sync_publisher)
    sub_t.start()
    pub_t.start()
    pub_t.join()
    sub_t.join(timeout=2)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Part 2: Async pub/sub
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def async_subscriber(stop_event: asyncio.Event):
    r = aioredis.Redis()
    pubsub = r.pubsub()
    await pubsub.subscribe("dubbing:events:async")

    async for message in pubsub.listen():
        if message["type"] == "message":
            data = message["data"].decode()
            print(f"[AsyncSub] Got: {data}")
            if data == "stop":
                break

    await pubsub.unsubscribe()
    await pubsub.close()
    await r.close()
    stop_event.set()


async def async_publisher(stop_event: asyncio.Event):
    r = aioredis.Redis()
    await asyncio.sleep(0.5)

    for i in range(5):
        await r.publish("dubbing:events:async", f"event_{i}")
        await asyncio.sleep(0.3)

    await r.publish("dubbing:events:async", "stop")
    await r.close()
    await stop_event.wait()


async def run_async_demo():
    print("\n=== Async Pub/Sub Demo ===")
    stop = asyncio.Event()
    await asyncio.gather(async_subscriber(stop), async_publisher(stop))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    run_sync_demo()
    asyncio.run(run_async_demo())
