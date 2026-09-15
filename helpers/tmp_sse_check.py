import json, threading, time, http.client

base = ('127.0.0.1', 8080)


def send_event():
    time.sleep(0.5)
    conn = http.client.HTTPConnection(*base, timeout=5)
    payload = json.dumps({
        'device_id': 'dev_test2',
        'room_id': 'room_test',
        'type': 'fall_warn',
        'ts': '2026-08-10T12:00:00.000Z',
        'seq': 2,
        'confidence': 0.95,
    }).encode()
    conn.request('POST', '/events', body=payload, headers={'Content-Type': 'application/json'})
    resp = conn.getresponse()
    print('post status', resp.status, resp.read().decode())

thread = threading.Thread(target=send_event, daemon=True)
thread.start()

conn = http.client.HTTPConnection(*base, timeout=5)
conn.request('GET', '/alarms/stream?room_id=room_test', headers={'Accept': 'text/event-stream'})
resp = conn.getresponse()
print('stream status', resp.status)
print('first read start')
chunk = resp.read(2048)
print('chunk', chunk)
