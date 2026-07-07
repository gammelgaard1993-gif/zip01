"""Device simulator / load harness for the Teton backend.

Publishes synthetic sensor events to the MQTT broker on
``{topic_prefix}/{device_id}/events`` (matching ``config.MQTT_TOPIC``) so the
service can be exercised end-to-end without real hardware.

Modes
-----
steady   : ~`rate` events/sec/device for `duration` seconds (baseline load).
burst    : a fixed total `rate` events/sec across all devices for `duration`
           seconds (used by `make burst` to validate backpressure + p95 latency).
offline  : one device replays `events` buffered transitions whose timestamps are
           `offline-minutes` in the past (late-event / occupancy-backfill check).

Examples
--------
    python tools/simulator.py steady  --devices 500 --duration 30
    python tools/simulator.py burst   --devices 500 --duration 30 --rate 50000
    python tools/simulator.py offline --offline-minutes 20 --events 1200
"""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt

EVENT_TYPES_NORMAL = ["heartbeat", "presence", "motion", "sleep_state", "net_status"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_payload(event_type: str) -> dict[str, object]:
    if event_type == "presence":
        return {"in_room": random.random() < 0.5}
    if event_type == "fall_warn":
        return {"confidence": round(random.uniform(0.6, 0.99), 2)}
    if event_type == "net_status":
        return {"online": True, "rssi": random.randint(-90, -40)}
    if event_type == "sleep_state":
        return {"state": random.choice(["awake", "light", "deep"])}
    if event_type == "motion":
        return {"detected": random.random() < 0.3}
    return {}


def _build_event(device_index: int, *, event_type: str, ts: str | None = None) -> dict[str, object]:
    device_id = f"dev_{device_index:04d}"
    room_id = f"room_{device_index % 50:02d}"
    return {
        "device_id": device_id,
        "room_id": room_id,
        "type": event_type,
        "ts": ts or _now_iso(),
        "payload": _build_payload(event_type),
    }


def _publish(client: mqtt.Client, topic_prefix: str, event: dict[str, object]) -> None:
    topic = f"{topic_prefix}/{event['device_id']}/events"
    client.publish(topic, json.dumps(event), qos=1)


def _connect(host: str, port: int) -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"teton-sim-{random.randint(0, 1_000_000)}")
    client.connect(host, port)
    client.loop_start()
    return client


def _pick_normal_type() -> str:
    # Heartbeats dominate (~1/sec/device expected), with occasional fall warnings.
    if random.random() < 0.01:
        return "fall_warn"
    return random.choice(EVENT_TYPES_NORMAL)


def run_steady(client: mqtt.Client, args: argparse.Namespace) -> int:
    published = 0
    interval = 1.0 / max(args.rate, 1)
    deadline = time.monotonic() + args.duration
    while time.monotonic() < deadline:
        cycle_start = time.monotonic()
        for device_index in range(args.devices):
            _publish(client, args.topic_prefix, _build_event(device_index, event_type=_pick_normal_type()))
            published += 1
        elapsed = time.monotonic() - cycle_start
        sleep_for = interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)
    return published


def run_burst(client: mqtt.Client, args: argparse.Namespace) -> int:
    published = 0
    deadline = time.monotonic() + args.duration
    per_tick = max(1, args.rate // 100)  # ~100 batches/sec toward the target rate
    while time.monotonic() < deadline:
        tick_start = time.monotonic()
        for _ in range(per_tick):
            device_index = random.randint(0, args.devices - 1)
            event_type = "fall_warn" if random.random() < 0.05 else _pick_normal_type()
            _publish(client, args.topic_prefix, _build_event(device_index, event_type=event_type))
            published += 1
        elapsed = time.monotonic() - tick_start
        sleep_for = 0.01 - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)
    return published


def run_offline(client: mqtt.Client, args: argparse.Namespace) -> int:
    """Replay a backlog of late events with timestamps `offline-minutes` in the past."""
    published = 0
    base = datetime.now(timezone.utc) - timedelta(minutes=args.offline_minutes)
    device_index = 0
    in_room = True
    for i in range(args.events):
        ts = (base + timedelta(seconds=i * (args.offline_minutes * 60 / max(args.events, 1)))).isoformat()
        # Alternate presence transitions plus periodic heartbeats so occupancy and
        # availability windows are both backfilled.
        if i % 10 == 0:
            in_room = not in_room
            event = _build_event(device_index, event_type="presence", ts=ts)
            event["payload"] = {"in_room": in_room}
        else:
            event = _build_event(device_index, event_type="heartbeat", ts=ts)
        _publish(client, args.topic_prefix, event)
        published += 1
    return published


def main() -> None:
    parser = argparse.ArgumentParser(description="Teton backend MQTT device simulator")
    parser.add_argument("mode", choices=["steady", "burst", "offline"])
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic-prefix", default="teton/devices")
    parser.add_argument("--devices", type=int, default=500)
    parser.add_argument("--duration", type=int, default=30, help="seconds (steady/burst)")
    parser.add_argument("--rate", type=int, default=1, help="steady: events/sec/device; burst: total events/sec")
    parser.add_argument("--offline-minutes", type=int, default=20, help="offline: backlog age in minutes")
    parser.add_argument("--events", type=int, default=1200, help="offline: number of buffered events")
    args = parser.parse_args()

    client = _connect(args.host, args.port)
    start = time.monotonic()
    try:
        if args.mode == "steady":
            published = run_steady(client, args)
        elif args.mode == "burst":
            published = run_burst(client, args)
        else:
            published = run_offline(client, args)
    finally:
        client.loop_stop()
        client.disconnect()

    elapsed = time.monotonic() - start
    rate = published / elapsed if elapsed > 0 else 0.0
    print(
        json.dumps(
            {
                "mode": args.mode,
                "published": published,
                "elapsed_seconds": round(elapsed, 2),
                "effective_rate_per_sec": round(rate, 1),
            }
        )
    )


if __name__ == "__main__":
    main()
