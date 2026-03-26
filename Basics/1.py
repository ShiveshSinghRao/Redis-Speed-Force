import redis
import time
import json

r = redis.Redis()
STREAM = "demo:reliable"
GROUP = "reliable-workers"


def setup():
    r.delete(STREAM)
    r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    # Enqueue 5 tasks
    for i in range(5):
        r.xadd(STREAM, {"task": f"task_{i}"})


def buggy_worker():
    """Simulates a worker that crashes after reading 3 messages."""
    for _ in range(3):
        entries = r.xreadgroup(
            GROUP, "buggy-worker", {STREAM: ">"}, count=1, block=1000
        )
        if entries:
            for _, messages in entries:
                for msg_id, fields in messages:
                    task = fields[b"task"].decode()
                    print(f"[Buggy] Got {task} (ID: {msg_id.decode()})")
                    # DOES NOT call XACK -- simulates crash
            print("[Buggy] CRASHED! (3 unacknowledged messages in PEL)")


def check_pel():
    """Inspect the PEL to see what's stuck."""
    print("\n--- PEL Status ---")
    summary = r.xpending(STREAM, GROUP)
    print(f"Total pending: {summary['pending']}")
    pending = r.xpending_range(STREAM, GROUP, min="-", max="+", count=10)
    for entry in pending:
        print(
            f" ID: {entry['message_id'].decode()}, "
            f"consumer: {entry['consumer'].decode()}, "
            f"idle: {entry['time_since_delivered']}ms, "
            f"delivered: {entry['times_delivered']}x"
        )


def recovery_worker():
    """Claims and processes stuck messages from crashed consumers."""
    print("\n--- Recovery Worker ---")
    pending = r.xpending_range(
        STREAM,
        GROUP,
        consumername="buggy-worker",
        min="-",
        max="+",
        count=10,
    )
    stuck_ids = [
        entry["message_id"] for entry in pending if entry["time_since_delivered"] > 1000
    ]
    if not stuck_ids:
        print("No stuck messages found.")
        return
    claimed = r.xclaim(
        STREAM,
        GROUP,
        "recovery-worker",
        min_idle_time=1000,
        message_ids=stuck_ids,
    )
    for msg_id, fields in claimed:
        task = fields[b"task"].decode()
        print(f"[Recovery] Reclaimed + processing {task}")
        r.xack(STREAM, GROUP, msg_id)
        print(f"[Recovery] ACK'd {task}")
    while True:
        entries = r.xreadgroup(
            GROUP, "recovery-worker", {STREAM: ">"}, count=1, block=1000
        )
        if not entries:
            break
        for _, messages in entries:
            for msg_id, fields in messages:
                task = fields[b"task"].decode()
                print(f"[Recovery] Processing new {task}")
                r.xack(STREAM, GROUP, msg_id)


# Run the demo
setup()
buggy_worker()
time.sleep(2)  # Let idle time build up
check_pel()
recovery_worker()
print("\n--- Final PEL Status ---")
summary = r.xpending(STREAM, GROUP)
print(f"Total pending: {summary['pending']}")  # Should be 0
