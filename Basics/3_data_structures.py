"""
Chapter 2: Redis Data Structures — Strings, Hashes, Lists, Sets,
Sorted Sets, and a brief Streams taste.
"""
import redis
import time

r = redis.Redis()


# ── 1. Strings ──────────────────────────────────────────────────
print("=== Strings ===")

r.set("name", "Shivesh")
print(r.get("name"))            # b'Shivesh'

# Atomic increment (no race condition even with concurrent clients)
r.set("counter", 0)
r.incr("counter")               # 1
r.incr("counter")               # 2
r.incrby("counter", 5)          # 7
print(f"counter = {r.get('counter').decode()}")

# TTL — key auto-deletes after 60 seconds
r.set("session:abc", "user_data", ex=60)
print(f"TTL remaining: {r.ttl('session:abc')}s")

# SETNX — set only if key does NOT exist (distributed lock primitive)
print(r.setnx("lock:job_123", "worker_1"))  # True  (acquired)
print(r.setnx("lock:job_123", "worker_2"))  # False (already held)
r.delete("lock:job_123")


# ── 2. Hashes ───────────────────────────────────────────────────
print("\n=== Hashes ===")

r.hset("job:123", mapping={
    "status": "in_progress",
    "src_lang": "English",
    "target_lang": "Hindi",
    "created_at": "2025-01-15T10:30:00",
})
print(r.hget("job:123", "status"))    # b'in_progress'
print(r.hgetall("job:123"))           # {b'status': b'in_progress', ...}

r.hset("job:123", "status", "completed")
r.hincrby("job:123", "retry_count", 1)
r.delete("job:123")


# ── 3. Lists ────────────────────────────────────────────────────
print("\n=== Lists ===")

r.delete("task_queue")
r.rpush("task_queue", "task_a", "task_b", "task_c")
# Queue: [task_a, task_b, task_c]

print(r.lpop("task_queue"))           # b'task_a'  (FIFO)
print(r.llen("task_queue"))           # 2
print(r.lrange("task_queue", 0, -1)) # [b'task_b', b'task_c']

# BRPOP — blocking pop (waits up to 2s for an item)
print(r.brpop("task_queue", timeout=2))  # (b'task_queue', b'task_c')
r.delete("task_queue")


# ── 4. Sets ─────────────────────────────────────────────────────
print("\n=== Sets ===")

r.sadd("processed_jobs", "job_1", "job_2", "job_3")
print(r.sismember("processed_jobs", "job_1"))   # True
print(r.sismember("processed_jobs", "job_99"))  # False

r.sadd("set_a", "1", "2", "3")
r.sadd("set_b", "2", "3", "4")
print(f"intersection: {r.sinter('set_a', 'set_b')}")  # {b'2', b'3'}
print(f"union:        {r.sunion('set_a', 'set_b')}")  # {b'1',b'2',b'3',b'4'}
r.delete("processed_jobs", "set_a", "set_b")


# ── 5. Sorted Sets (ZSets) ─────────────────────────────────────
print("\n=== Sorted Sets ===")

r.zadd("leaderboard", {"alice": 100, "bob": 95, "charlie": 110})
print(r.zrange("leaderboard", 0, -1, withscores=True))
# [(b'bob', 95.0), (b'alice', 100.0), (b'charlie', 110.0)]

print(f"Top scorer: {r.zrevrange('leaderboard', 0, 0)}")  # [b'charlie']

# Delayed-job pattern: score = timestamp when job should run
r.zadd("delayed_tasks", {"retry_job_123": time.time() + 300})
r.delete("leaderboard", "delayed_tasks")


# ── 6. Streams (brief taste — Ch5 covers in depth) ─────────────
print("\n=== Streams (quick demo) ===")

r.delete("demo:stream")
msg_id = r.xadd("demo:stream", {
    "task_type": "validate_video",
    "job_id": "abc-123",
})
print(f"Added entry: {msg_id}")
print(f"Stream length: {r.xlen('demo:stream')}")
print(r.xrange("demo:stream", "-", "+"))
r.delete("demo:stream")
