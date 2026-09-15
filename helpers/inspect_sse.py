from urllib.request import Request, urlopen

room = 'room_000'
url = f'http://127.0.0.1:8080/alarms/stream?room_id={room}'
req = Request(url, headers={'Accept': 'text/event-stream'})
with urlopen(req, timeout=5) as resp:
    print('status', resp.status)
    for i in range(3):
        chunk = resp.read(1024)
        print('chunk', i, chunk[:200])
        if not chunk:
            break
