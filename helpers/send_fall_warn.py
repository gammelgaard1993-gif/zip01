import json
import os
import urllib.request
from datetime import datetime, timezone

payload = {
    'device_id': 'diag',
    'room_id': 'room_000',
    'type': 'fall_warn',
    'ts': datetime.now(timezone.utc).isoformat(),
    'seq': 1,
    'confidence': 0.95,
}
req = urllib.request.Request(
    'http://127.0.0.1:8080/events',
    data=json.dumps(payload).encode(),
    method='POST',
    headers={'Content-Type': 'application/json'},
)
with urllib.request.urlopen(req, timeout=10) as resp:
    print('post', resp.status, resp.read().decode())
