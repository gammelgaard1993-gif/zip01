"""
Concurrent load test for the Teton streaming backend.

What it measures:
  - Throughput (requests/sec actually delivered)
  - Alarm delivery latency: time from fall_warn POST 200 OK → SSE receipt (p50/p95/p99)
  - Error rate

No extra dependencies — uses only stdlib (threading + urllib).

Usage:
    # Start service first: python main.py
    python _loadtest.py                          # 200 devices, 60s, baseline ~1 ev/dev/s
    python _loadtest.py --devices 500 --duration 120 --concurrency 32
    python _loadtest.py --burst --devices 200    # 10x burst for 30s mid-run
"""

import argparse
import json
import queue
import random
import statistics
import sys
import threading
import time
from collections import deque
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# Use 127.0.0.1 to bypass Windows IPv6 dual-stack resolution delay for localhost.
TARGET = "http://127.0.0.1:8080"


# ---------------------------------------------------------------------------
# SSE listener — one thread per room, records alarm receipt times
# ---------------------------------------------------------------------------

class RoomListener(threading.Thread):
    """Subscribes to /alarms/stream?room_id=<room> and records receipt times."""

    def __init__(self, target: str, room_id: str, received: dict, lock: threading.Lock):
        super().__init__(daemon=True)
        self.url = f"{target.rstrip('/')}/alarms/stream?room_id={room_id}"
        self.received = received  # shared: composite_key -> monotonic receipt time
        self._lock = lock
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                req = Request(self.url, headers={"Accept": "text/event-stream"})
                with urlopen(req, timeout=30) as resp:
                    if resp.status != 200:
                        print(f"  [SSE] {self.url} → HTTP {resp.status}")
                        time.sleep(1.0)
                        continue
                    buf = b""
                    while not self._stop.is_set():
                        chunk = resp.read(512)
                        if not chunk:
                            break
                        buf += chunk
                        while b"\n\n" in buf:
                            block, buf = buf.split(b"\n\n", 1)
                            self._parse_block(block)
            except Exception as exc:
                if not self._stop.is_set():
                    print(f"  [SSE] {self.url} error: {exc!r}")
                    time.sleep(0.5)

    def _parse_block(self, block: bytes) -> None:
        for line in block.decode(errors="replace").splitlines():
            if line.startswith("data:"):
                raw = line[5:].strip()
                try:
                    obj = json.loads(raw)
                    device_id = obj.get("device_id", "")
                    room_id = obj.get("room_id", "")
                    ts = (obj.get("ts") or "")[:19]  # truncate to second for dedup key match
                    key = f"{device_id}:{room_id}:{ts}"
                    with self._lock:
                        if key not in self.received:
                            self.received[key] = time.monotonic()
                except (json.JSONDecodeError, AttributeError):
                    pass

    def stop(self) -> None:
        self._stop.set()


class AlarmListener:
    """Manages per-room SSE listeners for a sample of rooms."""

    def __init__(self, target: str, rooms: list[str]):
        self._received: dict[str, float] = {}
        self._lock = threading.Lock()
        self._listeners = [RoomListener(target, r, self._received, self._lock) for r in rooms]

    def start(self) -> None:
        for l in self._listeners:
            l.start()

    def stop(self) -> None:
        for l in self._listeners:
            l.stop()

    def get_received(self) -> dict[str, float]:
        with self._lock:
            return dict(self._received)


# ---------------------------------------------------------------------------
# Worker — sends events concurrently from a shared job queue
# ---------------------------------------------------------------------------

class Stats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.sent = 0
        self.failed = 0
        self.latencies_ms: list[float] = []   # POST round-trip ms
        # fall_warn tracking: event_id -> send completion time (monotonic)
        self.fall_sends: dict[str, float] = {}

    def record_ok(self, latency_ms: float) -> None:
        with self._lock:
            self.sent += 1
            self.latencies_ms.append(latency_ms)

    def record_fail(self) -> None:
        with self._lock:
            self.failed += 1

    def record_fall(self, event_id: str, sent_at: float) -> None:
        with self._lock:
            self.fall_sends[event_id] = sent_at


def worker(job_queue: queue.Queue, stats: Stats, target: str) -> None:
    url = target.rstrip("/") + "/events"
    while True:
        item = job_queue.get()
        if item is None:
            job_queue.task_done()
            break
        event = item
        data = json.dumps(event).encode()
        req = Request(url, data=data, method="POST",
                      headers={"Content-Type": "application/json"})
        t0 = time.monotonic()
        try:
            with urlopen(req, timeout=10) as resp:
                resp.read()
            elapsed_ms = (time.monotonic() - t0) * 1000
            stats.record_ok(elapsed_ms)
            if event.get("type") == "fall_warn":
                # Key matches the SSE listener's composite key (ts truncated to second)
                    stats.record_fall(
                        f"{event['device_id']}:{event['room_id']}:{event['ts'][:19]}",
                        time.monotonic(),
                    )
        except (URLError, HTTPError, OSError):
            stats.record_fail()
        finally:
            job_queue.task_done()


# ---------------------------------------------------------------------------
# Event generation helpers
# ---------------------------------------------------------------------------

def make_event(device_id: str, room_id: str, etype: str, seq: int) -> dict:
    ts_unix = time.time()
    ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts_unix)) + \
         f".{int(ts_unix * 1000) % 1000:03d}Z"
    e: dict = {"device_id": device_id, "room_id": room_id, "type": etype, "ts": ts, "seq": seq}
    if etype == "presence":
        e["in_room"] = random.choice([True, False])
    elif etype == "motion":
        e["magnitude"] = round(random.random(), 2)
    elif etype == "fall_warn":
        e["confidence"] = round(random.uniform(0.7, 0.99), 2)
    elif etype == "sleep_state":
        e["state"] = random.choice(["asleep", "awake", "unknown"])
    elif etype == "net_status":
        e["rssi"] = random.randint(-90, -50)
    return e


EVENT_TYPES = ["heartbeat"] * 50 + ["motion"] * 15 + ["presence"] * 4 + \
              ["sleep_state"] * 3 + ["net_status"] * 3 + ["fall_warn"] * 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _probe_baseline(target: str) -> None:
    """Send 5 sequential requests to measure single-threaded server latency."""
    url = target.rstrip("/") + "/events"
    event = {"device_id": "dev_probe", "room_id": "room_probe",
             "type": "heartbeat", "ts": "", "seq": 0}
    lats = []
    for i in range(5):
        event["seq"] = i + 1
        event["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + ".000Z"
        data = json.dumps(event).encode()
        req = Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
        t0 = time.monotonic()
        try:
            with urlopen(req, timeout=10) as resp:
                resp.read()
            lats.append((time.monotonic() - t0) * 1000)
        except Exception as e:
            print(f"  probe failed: {e}")
    if lats:
        print(f"Probe  : {len(lats)}/5 sequential requests ok, "
              f"avg {sum(lats)/len(lats):.0f}ms, min {min(lats):.0f}ms, max {max(lats):.0f}ms")
        if min(lats) > 500:
            print("  ⚠ Single-threaded latency > 500ms — server bottleneck, not client concurrency")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--target", default=TARGET)
    p.add_argument("--devices", type=int, default=200)
    p.add_argument("--duration", type=float, default=60.0)
    p.add_argument("--rps", type=float, default=1.0,
                   help="Events per device per second (baseline)")
    p.add_argument("--concurrency", type=int, default=16,
                   help="Concurrent HTTP worker threads (keep low on Windows: urllib has no conn pooling)")
    p.add_argument("--burst", action="store_true",
                   help="Do a 10x burst from t=30s to t=60s")
    args = p.parse_args()

    print(f"Target : {args.target}")
    print(f"Devices: {args.devices}  Duration: {args.duration}s  "
          f"Workers: {args.concurrency}  Baseline: {args.rps} ev/dev/s")
    if args.burst:
        print("Burst  : 10x from t=30s to t=60s")

    # Single-threaded baseline probe so you can see server-only latency before concurrent load.
    _probe_baseline(args.target)

    # Subscribe to a representative sample of rooms (one SSE connection each).
    rooms_to_watch = [f"room_{i:03d}" for i in range(min(10, args.devices // 2))]
    print(f"SSE    : watching {len(rooms_to_watch)} rooms")
    print()

    listener = AlarmListener(args.target, rooms_to_watch)
    listener.start()

    # Start worker threads
    job_queue: queue.Queue = queue.Queue(maxsize=args.concurrency * 4)
    stats = Stats()
    workers = []
    for _ in range(args.concurrency):
        t = threading.Thread(target=worker, args=(job_queue, stats, args.target), daemon=True)
        t.start()
        workers.append(t)

    # Build device list
    devices = [
        {"device_id": f"dev_{i:04d}", "room_id": f"room_{i // 2:03d}", "seq": 0}
        for i in range(args.devices)
    ]

    # Emit loop
    start = time.monotonic()
    end = start + args.duration
    next_tick = [start + random.random() / args.rps for _ in devices]  # stagger
    progress_at = start + 10.0
    sent_snapshot = 0

    while True:
        now = time.monotonic()
        if now >= end:
            break

        burst_active = args.burst and 30 <= (now - start) < 60
        rate = args.rps * (10.0 if burst_active else 1.0)

        for i, d in enumerate(devices):
            if now < next_tick[i]:
                continue
            d["seq"] += 1
            etype = random.choice(EVENT_TYPES)
            ev = make_event(d["device_id"], d["room_id"], etype, d["seq"])
            # Fall jitter: emit 1-3 copies so dedup is exercised
            copies = random.randint(1, 3) if etype == "fall_warn" else 1
            for c in range(copies):
                if c > 0:
                    d["seq"] += 1
                    ev = dict(ev, seq=d["seq"])
                try:
                    job_queue.put(ev, timeout=0.1)
                except queue.Full:
                    stats.record_fail()  # client-side backpressure; don't block
            next_tick[i] = now + 1.0 / rate

        if now >= progress_at:
            elapsed = now - start
            with stats._lock:
                delta = stats.sent - sent_snapshot
                sent_snapshot = stats.sent
                err = stats.failed
            print(f"  t={elapsed:5.0f}s  sent={stats.sent:7d}  "
                  f"+{delta:5d}/10s  err={err}  "
                  f"{'[BURST]' if burst_active else ''}")
            progress_at += 10.0

        time.sleep(0.002)

    # Drain workers
    for _ in workers:
        job_queue.put(None)
    job_queue.join()

    # Give SSE a moment to catch up
    print("\nWaiting 3s for SSE delivery to settle…")
    time.sleep(3)
    listener.stop()

    # Poll /alarms to verify fall_warns reached the DB, independent of SSE matching.
    try:
        with urlopen(f"{args.target}/alarms?since=0", timeout=5) as r:
            body = json.loads(r.read())
        db_alarms = body.get("alarms", [])
        print(f"  /alarms?since=0 returned {len(db_alarms)} alarms in DB")
    except Exception as exc:
        print(f"  /alarms poll failed: {exc}")
        db_alarms = []

    # ---------------------------------------------------------------------------
    # Report
    # ---------------------------------------------------------------------------
    elapsed_total = time.monotonic() - start
    received = listener.get_received()
    fall_sends = {k: v for k, v in stats.fall_sends.items()}

    # Alarm delivery latencies
    alarm_latencies_ms: list[float] = []
    for eid, sent_at in fall_sends.items():
        recv_at = received.get(eid)
        if recv_at is not None:
            alarm_latencies_ms.append((recv_at - sent_at) * 1000)

    print()
    print("=" * 52)
    print(f"  Duration         {elapsed_total:.1f}s")
    print(f"  Events sent OK   {stats.sent:,}")
    print(f"  Events failed    {stats.failed:,}")
    print(f"  Throughput       {stats.sent / elapsed_total:.0f} req/s")

    if stats.latencies_ms:
        lats = sorted(stats.latencies_ms)
        print(f"\n  POST round-trip latency (ms)")
        print(f"    p50  {statistics.median(lats):.1f}")
        print(f"    p95  {lats[int(len(lats) * 0.95)]:.1f}")
        print(f"    p99  {lats[int(len(lats) * 0.99)]:.1f}")
        print(f"    max  {lats[-1]:.1f}")

    print(f"\n  Fall warns sent  {len(fall_sends):,}")
    print(f"  SSE alarms recv  {len(received):,}")
    if alarm_latencies_ms:
        al = sorted(alarm_latencies_ms)
        print(f"\n  Alarm delivery latency (send→SSE receipt, ms)")
        print(f"    p50  {statistics.median(al):.0f}   {'✓' if statistics.median(al) < 500 else '✗'}")
        p95 = al[int(len(al) * 0.95)]
        print(f"    p95  {p95:.0f}   {'✓ <1000ms' if p95 < 1000 else '✗ >1000ms  ← scoring risk'}")
        print(f"    p99  {al[int(len(al) * 0.99)]:.0f}")
        print(f"    max  {al[-1]:.0f}")
        matched_pct = len(alarm_latencies_ms) / max(len(fall_sends), 1) * 100
        print(f"    matched {matched_pct:.0f}% of fall sends to SSE events")
    elif fall_sends:
        print("  ✗ No fall_warn events matched in SSE stream — check /alarms/stream")
    else:
        print("  (no fall_warn events emitted — increase --duration or --devices)")
    print("=" * 52)


if __name__ == "__main__":
    main()
