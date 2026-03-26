"""
Chapter 6: Consumer Groups — Distributed Stream Processing.

Without consumer groups (XREAD), every reader sees ALL messages (broadcast).
With consumer groups (XREADGROUP), each message goes to exactly ONE consumer
in the group (load balancing).

Covers: XGROUP CREATE, XREADGROUP, XACK, XINFO GROUPS, XINFO CONSUMERS.
"""
import redis
import time
import threading

r = redis.Redis()
STREAM = "demo:work"
GROUP = "workers"


def setup():
    r.delete(STREAM)
    r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)


def producer():
    for i in range(9):
        r.xadd(STREAM, {"task": f"task_{i}", "data": str(i)})
        print(f"[Producer] Enqueued task_{i}")
        time.sleep(0.3)


def worker(name: str):
    while True:
        entries = r.xreadgroup(
            groupname=GROUP,
            consumername=name,
            streams={STREAM: ">"},
            block=2000,
            count=1,
        )
        if not entries:
            break

        for _, messages in entries:
            for msg_id, fields in messages:
                task = fields[b"task"].decode()
                print(f"  [{name}] Processing {task}...")
                time.sleep(0.5)
                r.xack(STREAM, GROUP, msg_id)
                print(f"  [{name}] Done + ACK'd {task}")


def inspect():
    """Show consumer group and per-consumer state."""
    print("\n--- XINFO GROUPS ---")
    for group in r.xinfo_groups(STREAM):
        print(f"Group: {group['name'].decode()}")
        print(f"  Consumers: {group['consumers']}")
        print(f"  Pending:   {group['pending']}")
        print(f"  Last ID:   {group['last-delivered-id'].decode()}")

    print("\n--- XINFO CONSUMERS ---")
    for c in r.xinfo_consumers(STREAM, GROUP):
        print(f"Consumer: {c['name'].decode()}")
        print(f"  Pending: {c['pending']}")
        print(f"  Idle:    {c['idle']}ms")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    print("=== Consumer Groups: 3 Workers Sharing a Stream ===\n")
    setup()

    threads = [
        threading.Thread(target=producer),
        threading.Thread(target=worker, args=("W1",)),
        threading.Thread(target=worker, args=("W2",)),
        threading.Thread(target=worker, args=("W3",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    inspect()
    r.delete(STREAM)
