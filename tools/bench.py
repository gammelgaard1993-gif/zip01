"""
Concurrent load benchmark for POST /events.

Unlike event_generator/generate.py (single-threaded, one blocking POST at a
time), this fires from N worker threads, each holding a persistent keep-alive
HTTP connection, sending as fast as the server will accept for a fixed
duration. It reports achieved throughput and per-request latency percentiles.

Purpose: measure the *server's* true ingest ceiling under concurrency, so we
can establish a baseline before deciding whether async-Redis / async-writer
changes are worth it.

Run:
    python tools/bench.py --concurrency 32 --duration 8
    python tools/bench.py --sweep            # runs 1,8,32,64,128 back to back
"""
from __future__ import annotations

import argparse
import http.client
import json
import random
import threading
import time
from urllib.parse import urlparse


def make_body(seq: int) -> bytes:
    r = random.random()
    dev = random.randint(0, 4999)
    ev = {
        "device_id": f"dev_{dev:04d}",
        "room_id": f"room_{dev // 2:03d}",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{random.randint(0, 999):03d}Z",
        "seq": seq,
    }
    if r < 0.02:
        ev["type"] = "fall_warn"
        ev["confidence"] = round(random.uniform(0.7, 0.99), 2)
    elif r < 0.30:
        ev["type"] = "presence"
        ev["in_room"] = random.choice([True, False])
    else:
        ev["type"] = "heartbeat"
    return json.dumps(ev).encode()


class Worker(threading.Thread):
    def __init__(self, host: str, port: int, stop_at: float):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.stop_at = stop_at
        self.latencies: list[float] = []
        self.ok = 0
        self.errors = 0

    def run(self) -> None:
        conn = http.client.HTTPConnection(self.host, self.port, timeout=30)
        headers = {"Content-Type": "application/json"}
        seq = 0
        while time.perf_counter() < self.stop_at:
            seq += 1
            body = make_body(seq)
            t0 = time.perf_counter()
            try:
                conn.request("POST", "/events", body=body, headers=headers)
                resp = conn.getresponse()
                resp.read()
                dt = (time.perf_counter() - t0) * 1000.0
                if resp.status in (200, 202):
                    self.ok += 1
                    self.latencies.append(dt)
                else:
                    self.errors += 1
            except Exception:
                self.errors += 1
                try:
                    conn.close()
                except Exception:
                    pass
                conn = http.client.HTTPConnection(self.host, self.port, timeout=30)
        conn.close()


def pct(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    idx = min(len(sorted_vals) - 1, int(len(sorted_vals) * p))
    return sorted_vals[idx]


def run_once(host: str, port: int, concurrency: int, duration: float) -> None:
    stop_at = time.perf_counter() + duration
    workers = [Worker(host, port, stop_at) for _ in range(concurrency)]
    t_start = time.perf_counter()
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    elapsed = time.perf_counter() - t_start

    lat: list[float] = []
    ok = errors = 0
    for w in workers:
        lat.extend(w.latencies)
        ok += w.ok
        errors += w.errors
    lat.sort()
    thr = ok / elapsed if elapsed else 0.0
    print(
        f"conc={concurrency:>4}  ok={ok:>7}  err={errors:>5}  "
        f"thr={thr:>9.1f} ev/s  "
        f"p50={pct(lat, 0.50):>7.1f}ms  p95={pct(lat, 0.95):>7.1f}ms  "
        f"p99={pct(lat, 0.99):>7.1f}ms  max={ (lat[-1] if lat else float('nan')):>7.1f}ms"
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--target", default="http://localhost:8080")
    p.add_argument("--concurrency", type=int, default=32)
    p.add_argument("--duration", type=float, default=8.0)
    p.add_argument("--sweep", action="store_true",
                   help="run a concurrency sweep instead of a single level")
    args = p.parse_args()

    u = urlparse(args.target)
    host = u.hostname or "localhost"
    port = u.port or 8080

    print(f"target={args.target}  duration={args.duration}s per level")
    if args.sweep:
        for c in (1, 8, 32, 64, 128):
            run_once(host, port, c, args.duration)
    else:
        run_once(host, port, args.concurrency, args.duration)


if __name__ == "__main__":
    main()
