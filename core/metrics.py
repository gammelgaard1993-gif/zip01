from __future__ import annotations

from collections import defaultdict, deque
from math import ceil
from threading import Lock
from typing import Deque, Dict

_counter_lock = Lock()
_counters: Dict[str, int] = defaultdict(int)
_latency_lock = Lock()
_MAX_LATENCY_SAMPLES = 5000
# maxlen makes each append O(1) and drops the oldest sample automatically. This runs on the alarm hot path (AlarmBus._dispatch_room)
_alarm_feed_latencies_ms: Deque[float] = deque(maxlen=_MAX_LATENCY_SAMPLES)


def increment_counter(name: str, amount: int = 1) -> None:
    with _counter_lock:
        _counters[name] += amount


def get_counters() -> Dict[str, int]:
    with _counter_lock:
        return dict(_counters)


def observe_alarm_feed_latency_ms(value_ms: float) -> None:
    bounded_value = max(0.0, value_ms)
    with _latency_lock:
        _alarm_feed_latencies_ms.append(bounded_value)


def get_alarm_feed_latency_ms_p95() -> int:
    with _latency_lock:
        if not _alarm_feed_latencies_ms:
            return 0
        sorted_values = sorted(_alarm_feed_latencies_ms)

    rank_index = max(0, ceil(0.95 * len(sorted_values)) - 1)
    return int(round(sorted_values[rank_index]))
