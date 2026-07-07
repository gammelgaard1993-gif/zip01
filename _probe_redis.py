import redis

r = redis.from_url("redis://localhost:6379/0", protocol=2)

try:
    info = r.execute_command("INFO", "server")
    text = info.decode() if isinstance(info, (bytes, bytearray)) else str(info)
    for line in text.splitlines():
        if "version" in line.lower() or "server_name" in line.lower():
            print("INFO:", line.strip())
except Exception as e:
    print("INFO failed:", e)

r.delete("t:probe")
try:
    r.execute_command("HSET", "t:probe", "a", "1", "b", "2")
    print("variadic HSET: OK")
except Exception as e:
    print("variadic HSET: FAILS ->", e)
finally:
    r.delete("t:probe")

try:
    r.execute_command("HSET", "t:probe", "a", "1")
    print("single HSET: OK")
except Exception as e:
    print("single HSET: FAILS ->", e)
finally:
    r.delete("t:probe")
