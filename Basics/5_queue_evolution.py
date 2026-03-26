"""
Chapter 4: Redis as a Queue — The Evolution.

Generation 1: LPUSH / BRPOP  (simple, but message loss on crash)
Generation 2: RPOPLPUSH      (reliable, but manual bookkeeping)
Generation 3: Streams         (covered in 6_streams.py onward)
"""
import redis
import time
import threading

r = redis.Redis()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Generation 1: LPUSH / BRPOP (The Simple Queue)
# Problem: if the worker crashes after BRPOP but before finishing,
# the message is gone forever.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def enqueue_task(task_data: str):
    r.lpush("task_queue", task_data)
    print(f"[Producer] Enqueued: {task_data}")


def simple_worker():
    print("[Worker] Waiting for tasks...")
    while True:
        result = r.brpop("task_queue", timeout=2)
        if result is None:
            print("[Worker] No more tasks.")
            break
        _, task_data = result
        print(f"[Worker] Processing: {task_data.decode()}")
        time.sleep(0.3)
        print(f"[Worker] Done: {task_data.decode()}")


def run_gen1_demo():
    print("=== Generation 1: Simple LPUSH / BRPOP Queue ===")
    r.delete("task_queue")
    for i in range(4):
        enqueue_task(f"task_{i}")
    simple_worker()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Generation 2: RPOPLPUSH (The Reliable Queue)
# Atomically moves from main queue to a processing queue.
# On success → remove from processing queue.
# On failure → move back to main queue.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process(task: bytes):
    print(f"  Processing {task.decode()}...")
    time.sleep(0.3)


def reliable_worker():
    print("[Reliable] Waiting for tasks...")
    while True:
        task = r.rpoplpush("rq:main", "rq:processing")
        if task is None:
            print("[Reliable] No more tasks.")
            break
        try:
            process(task)
            r.lrem("rq:processing", 1, task)
            print(f"  Done + removed from processing list: {task.decode()}")
        except Exception:
            r.rpoplpush("rq:processing", "rq:main")
            print(f"  Failed — moved back to main queue: {task.decode()}")


def run_gen2_demo():
    print("\n=== Generation 2: RPOPLPUSH Reliable Queue ===")
    r.delete("rq:main", "rq:processing")
    for i in range(4):
        r.lpush("rq:main", f"task_{i}")
        print(f"[Producer] Enqueued task_{i}")
    reliable_worker()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    run_gen1_demo()
    run_gen2_demo()
    print("\nGeneration 3 (Streams) → see 6_streams.py")
