"""
Chapter 1: Redis Basics — Connection, SET/GET, bytes encoding.

Redis stores everything as bytes internally. The Python client
returns bytes by default; decode with .decode() or pass
decode_responses=True to the Redis constructor.
"""
import redis

r = redis.Redis(host='localhost', port=6379, db=0)

# --- SET / GET ---
r.set("greeting", "hello")
value = r.get("greeting")
print(value)            # b'hello' (bytes, not string!)
print(value.decode())   # 'hello'

# --- Auto-decode variant ---
r2 = redis.Redis(decode_responses=True)
r2.set("greeting", "hello")
print(r2.get("greeting"))  # 'hello' (already a str)
