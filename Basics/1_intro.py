import redis

# Step 1: Connect to Redis
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

try:
    r.ping()
    print("✅ Redis is running\n")
except redis.ConnectionError:
    print("❌ Redis is NOT running. Start Redis first.")
    exit()

print("✅ Connected to Redis\n")

# -------------------------------
# Step 2: SET & GET
# -------------------------------
print("🔹 SET & GET")
r.set("name", "Shivesh")
print("Name:", r.get("name"))

# -------------------------------
# Step 3: Store Multiple Values
# -------------------------------
print("\n🔹 Store Name & Age")
r.set("age", 24)

print("Name:", r.get("name"))
print("Age:", r.get("age"))

# -------------------------------
# Step 4: Overwrite Value
# -------------------------------
print("\n🔹 Overwrite Value")
r.set("name", "Rao")
print("Updated Name:", r.get("name"))

# -------------------------------
# Step 5: Key Not Found
# -------------------------------
print("\n🔹 Key Not Found")
print("Unknown:", r.get("unknown"))  # None

# -------------------------------
# Step 6: Delete Key
# -------------------------------
print("\n🔹 Delete Key")
r.delete("age")
print("Age after delete:", r.get("age"))  # None

# -------------------------------
# Step 7: Check if Key Exists
# -------------------------------
print("\n🔹 Check Key Exists")
exists = r.exists("name")
print("Does 'name' exist?", bool(exists))

# -------------------------------
# Step 8: Clean up
# -------------------------------
r.delete("name")
print("\n🧹 Cleanup done")

print("\n🎉 Lesson 1 Completed!")