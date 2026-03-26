"""
Chapter 12, Q8: Minimal Reliable Task Queue using Redis Streams.

Combines everything: consumer groups, XREADGROUP, XACK, PEL recovery
via XPENDING + XCLAIM. A production-ready pattern in ~50 lines.
"""
import redis
import time
import json
import threading

r = redis.Redis()
STREAM = "tasks"
GROUP = "workers"


def setup():
    r.delete(STREAM)
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def enqueue(task_type: str, data: dict):
    msg_id = r.xadd(STREAM, {
        "task_type": task_type,
        "data": json.dumps(data),
        "enqueued_at": str(time.time()),
    })
    return msg_id


def process_and_ack(name, msg_id, fields):
    task_type = fields[b"task_type"].decode()
    data = json.loads(fields[b"data"])
    print(f"[{name}] Processing {task_type}: {data}")
    time.sleep(0.5)
    r.xack(STREAM, GROUP, msg_id)
    print(f"[{name}] ACK'd {task_type}")


def worker(name: str):
    while True:
        # Step 1: recover stuck messages in my PEL
        pending = r.xpending_range(STREAM, GROUP, consumername=name,
                                   min="-", max="+", count=5)
        stuck = [p["message_id"] for p in pending
                 if p["time_since_delivered"] > 30000]

        if stuck:
            claimed = r.xclaim(STREAM, GROUP, name,
                               min_idle_time=30000, message_ids=stuck)
            for msg_id, fields in claimed:
                process_and_ack(name, msg_id, fields)

        # Step 2: read new messages
        entries = r.xreadgroup(GROUP, name, {STREAM: ">"}, block=2000, count=1)
        if not entries:
            break
        for _, messages in entries:
            for msg_id, fields in messages:
                process_and_ack(name, msg_id, fields)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    print("=== Minimal Reliable Task Queue ===\n")
    setup()

    for i in range(6):
        enqueue(f"task_{i}", {"value": i * 10})
        print(f"[Main] Enqueued task_{i}")

    threads = [
        threading.Thread(target=worker, args=("W1",)),
        threading.Thread(target=worker, args=("W2",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    summary = r.xpending(STREAM, GROUP)
    print(f"\nPending after run: {summary['pending']}")
    r.delete(STREAM)
