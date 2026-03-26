"""
Chapter 7 (continued): Dead Letter Detection.

When a message has been delivered too many times (delivery count >=
MAX_DELIVERY_COUNT), it's likely a "poison pill" that always crashes
the consumer. Move it to a dead-letter stream and ACK it to stop retries.

See 1.py for the PEL recovery demo (also from Chapter 7).
"""
import redis
import time

r = redis.Redis()
STREAM = "demo:deadletter"
GROUP = "dl-workers"
DEAD_LETTER_STREAM = "dead_letters"
MAX_DELIVERY_COUNT = 3


def setup():
    r.delete(STREAM, DEAD_LETTER_STREAM)
    r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    for i in range(3):
        r.xadd(STREAM, {"task": f"task_{i}"})


def crashing_worker():
    """Reads messages but never ACKs — simulates repeated crashes."""
    for _ in range(3):
        entries = r.xreadgroup(GROUP, "crashy", {STREAM: ">"}, count=1, block=1000)
        if entries:
            for _, messages in entries:
                for msg_id, fields in messages:
                    print(f"[Crashy] Got {fields[b'task'].decode()} "
                          f"(ID: {msg_id.decode()}) — crashing!")

    # Simulate re-delivery by XCLAIMing each message multiple times
    pending = r.xpending_range(STREAM, GROUP, min="-", max="+", count=10)
    for entry in pending:
        msg_id = entry["message_id"]
        for _ in range(MAX_DELIVERY_COUNT - 1):
            r.xclaim(STREAM, GROUP, "crashy", min_idle_time=0,
                     message_ids=[msg_id])


def recovery_with_dead_letter():
    """Checks pending messages and dead-letters any over the threshold."""
    print("\n--- Recovery with Dead-Letter Detection ---")
    pending = r.xpending_range(STREAM, GROUP, min="-", max="+", count=100)

    for entry in pending:
        msg_id = entry["message_id"]
        deliveries = entry["times_delivered"]

        if deliveries >= MAX_DELIVERY_COUNT:
            print(f"Dead-lettering {msg_id.decode()} ({deliveries} deliveries)")

            msg_data = r.xrange(STREAM, min=msg_id, max=msg_id)
            if msg_data:
                r.xadd(DEAD_LETTER_STREAM, msg_data[0][1])

            r.xack(STREAM, GROUP, msg_id)
        else:
            r.xclaim(STREAM, GROUP, "recovery-worker", min_idle_time=0,
                     message_ids=[msg_id])
            claimed = r.xrange(STREAM, min=msg_id, max=msg_id)
            if claimed:
                task = claimed[0][1][b"task"].decode()
                print(f"[Recovery] Reclaiming {task} (delivery #{deliveries})")
                r.xack(STREAM, GROUP, msg_id)


def show_dead_letters():
    print("\n--- Dead Letter Stream ---")
    entries = r.xrange(DEAD_LETTER_STREAM, "-", "+")
    if not entries:
        print("  (empty)")
    for msg_id, fields in entries:
        print(f"  {msg_id.decode()} -> {fields}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    setup()
    crashing_worker()
    time.sleep(1)
    recovery_with_dead_letter()
    show_dead_letters()

    print(f"\nPEL after recovery: {r.xpending(STREAM, GROUP)['pending']} pending")
    r.delete(STREAM, DEAD_LETTER_STREAM)
