"""
Chapter 5: Redis Streams — The Core Data Structure.

Covers: XADD, XREAD (blocking + historical), XRANGE, XREVRANGE,
XTRIM, XDEL, XINFO, and a threaded producer/consumer demo.

Key insight: reading from a stream does NOT delete entries.
Streams are append-only logs — fundamentally different from Lists.
"""
import redis
import time
import threading
import json

r = redis.Redis()
STREAM = "demo:tasks"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Part 1: Basic stream operations
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def basic_ops():
    print("=== Stream Basic Operations ===")
    r.delete("demo:ops")

    # XADD — append entries ('*' = auto-generate ID, which is the default)
    id1 = r.xadd("demo:ops", {"task_type": "validate_video", "job_id": "abc-123",
                               "payload": '{"video_url": "https://..."}'})
    id2 = r.xadd("demo:ops", {"task_type": "split_chunks", "job_id": "abc-123"})
    id3 = r.xadd("demo:ops", {"task_type": "validate_video", "job_id": "def-456"})
    print(f"Added 3 entries. Last ID: {id3}")
    print(f"Stream length: {r.xlen('demo:ops')}")

    # XRANGE — read all entries (oldest to newest)
    print("\nXRANGE (all):")
    for msg_id, fields in r.xrange("demo:ops", "-", "+"):
        print(f"  {msg_id.decode()} -> {fields}")

    # XRANGE with count limit
    print("\nXRANGE (first 2):")
    for msg_id, fields in r.xrange("demo:ops", "-", "+", count=2):
        print(f"  {msg_id.decode()}")

    # XREVRANGE — newest first
    print("\nXREVRANGE (latest 1):")
    latest = r.xrevrange("demo:ops", "+", "-", count=1)
    print(f"  {latest[0][0].decode()} -> {latest[0][1]}")

    # XREAD — read all from beginning
    print("\nXREAD (from 0-0):")
    entries = r.xread({"demo:ops": "0-0"})
    for stream_name, messages in entries:
        for msg_id, fields in messages:
            print(f"  {msg_id.decode()}")

    # XREAD — read only entries after a given ID
    print(f"\nXREAD (after {id1.decode()}):")
    entries = r.xread({"demo:ops": id1})
    for stream_name, messages in entries:
        for msg_id, fields in messages:
            print(f"  {msg_id.decode()}")

    # XTRIM — cap stream size
    r.xtrim("demo:ops", maxlen=2)
    print(f"\nAfter XTRIM maxlen=2: length = {r.xlen('demo:ops')}")

    # XINFO STREAM — metadata
    info = r.xinfo_stream("demo:ops")
    print(f"Stream info: length={info['length']}, first={info['first-entry'][0].decode()}")

    # XDEL — delete a specific entry by ID
    r.xdel("demo:ops", id3)
    print(f"After XDEL: length = {r.xlen('demo:ops')}")

    r.delete("demo:ops")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Part 2: Threaded Producer / Consumer using XREAD (no consumer groups)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def producer():
    """Adds 5 tasks to the stream, one per second."""
    for i in range(5):
        task = {"task_type": f"task_{i}", "data": json.dumps({"value": i * 10})}
        msg_id = r.xadd(STREAM, task)
        print(f"[Producer] Added {task['task_type']} -> ID: {msg_id.decode()}")
        time.sleep(1)
    print("[Producer] Done.")


def consumer():
    """Reads new tasks using XREAD with blocking."""
    last_id = "0-0"
    print("[Consumer] Waiting for tasks...")

    while True:
        entries = r.xread({STREAM: last_id}, block=2000, count=1)
        if not entries:
            print("[Consumer] No new tasks, exiting.")
            break

        for stream_name, messages in entries:
            for msg_id, fields in messages:
                task_type = fields[b"task_type"].decode()
                print(f"[Consumer] Processing {task_type} (ID: {msg_id.decode()})")
                time.sleep(0.5)
                last_id = msg_id


def run_producer_consumer():
    print("\n=== Threaded Producer / Consumer (XREAD) ===")
    r.delete(STREAM)
    t1 = threading.Thread(target=producer)
    t2 = threading.Thread(target=consumer, daemon=True)
    t2.start()
    t1.start()
    t1.join()
    time.sleep(3)
    print(f"\nStream still has all entries: length = {r.xlen(STREAM)}")
    r.delete(STREAM)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    basic_ops()
    run_producer_consumer()
