import json
import time
from urllib.request import Request, urlopen

URL = "http://localhost:8080/events"
N = 30

def post(i):
    body = json.dumps({
        "device_id": "dev_9999",
        "room_id": "room_999",
        "type": "heartbeat",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + ".000Z",
        "seq": i,
    }).encode()
    req = Request(URL, data=body, method="POST",
                  headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    with urlopen(req, timeout=10) as resp:
        resp.read()
    return (time.perf_counter() - t0) * 1000

# warm up
post(0)
lat = [post(i) for i in range(1, N + 1)]
lat.sort()
total = sum(lat)
print(f"n={N}  min={lat[0]:.1f}ms  p50={lat[len(lat)//2]:.1f}ms  "
      f"p95={lat[int(len(lat)*0.95)]:.1f}ms  max={lat[-1]:.1f}ms")
print(f"avg={total/len(lat):.1f}ms  -> max sequential throughput ~= {1000/(total/len(lat)):.1f} events/sec")
