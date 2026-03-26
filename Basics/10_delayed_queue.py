"""
Chapter 12, Q9: Delayed Task Queue with Redis.

Uses a Sorted Set (score = execution timestamp) as a staging area.
A poller moves due tasks from the sorted set into a Stream for
immediate processing by consumer-group workers.
"""
import redis
import time
import json
import threading

r = redis.Redis()
DELAYED_KEY = "delayed_tasks"
STREAM = "ready_tasks"
GROUP = "workers"


def setup():
    r.delete(DELAYED_KEY, STREAM)
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def enqueue_delayed(task: dict, delay_seconds: int):
    """Add task to sorted set with score = execution timestamp."""
    execute_at = time.time() + delay_seconds
    r.zadd(DELAYED_KEY, {json.dumps(task): execute_at})
    print(f"[Scheduler] Queued {task['task_type']} to run in {delay_seconds}s")


def poll_delayed_tasks(stop_event: threading.Event):
    """Move due tasks from sorted set to stream."""
    while not stop_event.is_set():
        now = time.time()
        due_tasks = r.zrangebyscore(DELAYED_KEY, "-inf", now, start=0, num=10)

        for task_data in due_tasks:
            if r.zrem(DELAYED_KEY, task_data):
                task = json.loads(task_data)
                r.xadd(STREAM, task)
                print(f"[Poller] Moved to stream: {task['task_type']}")

        time.sleep(0.5)


def worker(name: str, stop_event: threading.Event):
    """Consume ready tasks from the stream."""
    while not stop_event.is_set():
        entries = r.xreadgroup(GROUP, name, {STREAM: ">"}, block=1000, count=1)
        if not entries:
            continue
        for _, messages in entries:
            for msg_id, fields in messages:
                task_type = fields[b"task_type"].decode()
                print(f"[{name}] Processing {task_type}")
                time.sleep(0.3)
                r.xack(STREAM, GROUP, msg_id)
                print(f"[{name}] Done + ACK'd {task_type}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    print("=== Delayed Task Queue ===\n")
    setup()

    enqueue_delayed({"task_type": "immediate_task", "id": "1"}, delay_seconds=0)
    enqueue_delayed({"task_type": "short_delay_task", "id": "2"}, delay_seconds=2)
    enqueue_delayed({"task_type": "medium_delay_task", "id": "3"}, delay_seconds=4)

    stop = threading.Event()
    threads = [
        threading.Thread(target=poll_delayed_tasks, args=(stop,), daemon=True),
        threading.Thread(target=worker, args=("W1", stop), daemon=True),
    ]
    for t in threads:
        t.start()

    time.sleep(6)
    stop.set()
    for t in threads:
        t.join(timeout=2)

    remaining = r.zcard(DELAYED_KEY)
    print(f"\nDelayed set remaining: {remaining}")
    print(f"Stream length: {r.xlen(STREAM)}")
    r.delete(DELAYED_KEY, STREAM)
