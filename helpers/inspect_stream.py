import json
import threading
import time
import urllib.request
from urllib.request import Request, urlopen

base = 'http://127.0.0.1:8081'
room = 'room_debug'

chunks = []
stop_evt = threading.Event()


def reader() -> None:
    req = Request(f'{base}/alarms/stream?room_id={room}', headers={'Accept': 'text/event-stream'})
    with urlopen(req, timeout=10) as resp:
        while not stop_evt.is_set():
            chunk = resp.read(1024)
            if not chunk:
                break
            chunks.append(chunk.decode('utf-8', errors='replace'))
            if len(chunks) >= 5:
                break

thread = threading.Thread(target=reader, daemon=True)
thread.start()
time.sleep(1.0)

payload = {
    'device_id': 'dev_debug',
    'room_id': room,
    'type': 'fall_warn',
    'ts': '2026-08-10T20:25:00.000Z',
    'seq': 1,
    'confidence': 0.95,
}
req = Request(
    f'{base}/events',
    data=json.dumps(payload).encode(),
    method='POST',
    headers={'Content-Type': 'application/json'},
)
with urlopen(req, timeout=10) as resp:
    print('POST status', resp.status)
    print(resp.read().decode())

time.sleep(3.0)
stop_evt.set()
thread.join(timeout=5)
print('--- chunks ---')
for c in chunks:
    print(c)
