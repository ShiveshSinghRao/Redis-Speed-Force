import asyncio
import redis.asyncio as redis

async def main():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # Add messages
    await r.xadd("mystream", {"name": "shivesh", "task": "learn"})
    await r.xadd("mystream", {"name": "dev", "task": "build"})

    # Read messages
    messages = await r.xrange("mystream", "-", "+")
    
    print("Messages in stream:")
    for msg_id, data in messages:
        print(msg_id, data)

    await r.aclose()

asyncio.run(main())