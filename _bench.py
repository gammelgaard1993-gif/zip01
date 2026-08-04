import time
import json
import urllib.request

data = json.dumps({
    "device_id": "dev_bench",
    "room_id": "room_bench",
    "type": "heartbeat",
    "ts": "2026-08-04T18:00:00.000Z",
    "seq": 1,
}).encode()

n = 200
start = time.perf_counter()
for _ in range(n):
    req = urllib.request.Request(
        "http://localhost:8080/events",
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        resp.read()
elapsed = time.perf_counter() - start
print(f"{n} requests in {elapsed:.3f}s = {n / elapsed:.1f} req/s, avg {elapsed / n * 1000:.2f}ms")
