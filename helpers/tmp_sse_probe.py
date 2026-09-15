import http.client

conn = http.client.HTTPConnection('127.0.0.1', 8080, timeout=5)
conn.request('GET', '/alarms/stream?room_id=room-live')
resp = conn.getresponse()
print('status', resp.status)
print('headers', dict(resp.getheaders()))
for i in range(6):
    line = resp.readline().decode('utf-8', 'replace')
    print('line', i, repr(line))
    if not line:
        break
